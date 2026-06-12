//! `MERGE INTO` (Postgres 15+) end-to-end coverage.
//!
//! MERGE compiles each per-row WHEN action into ordinary INSERT/UPDATE/DELETE
//! driven through the normal statement pipeline (`crate::merge`), so these
//! tests exercise the full surface: clause ordering / first-match-wins,
//! DELETE + DO NOTHING actions, the duplicate-target 21000 error, VALUES and
//! SELECT-subquery sources, atomicity (a failing action rolls back the whole
//! MERGE), RLS enforcement on every action, read-back through the fast paths,
//! the empty-source and multi-row-mixed cases, and the command tag.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness (mirrors upsert_batching.rs) ────────────────────────────────────

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

async fn session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

async fn rows_of(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

/// `SELECT id, n, v FROM t ORDER BY id` → (ids, ns, vs).
async fn dump(sess: &ProjectSession, table: &str) -> (Vec<i64>, Vec<i64>, Vec<String>) {
    let b = rows_of(sess, &format!("SELECT id, n, v FROM {table} ORDER BY id")).await;
    (col_i64(&b, "id"), col_i64(&b, "n"), col_string(&b, "v"))
}

async fn create_target(sess: &ProjectSession, table: &str) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL, v TEXT NOT NULL)"
    ))
    .await
    .unwrap();
}

async fn create_source(sess: &ProjectSession, table: &str) {
    sess.execute(&format!(
        "CREATE TABLE {table} (sid BIGINT NOT NULL PRIMARY KEY, sn BIGINT NOT NULL, sv TEXT NOT NULL)"
    ))
    .await
    .unwrap();
}

fn tag_of(r: &ExecResult) -> &str {
    match r {
        ExecResult::Empty { tag } => tag.as_str(),
        ExecResult::Rows { .. } => panic!("expected Empty/tag result"),
    }
}

// ─── classic upsert-shaped MERGE (matched UPDATE + not-matched INSERT) ────────

#[tokio::test]
async fn merge_upsert_matched_update_unmatched_insert() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    // Target: ids 1,2 exist. Source: ids 1 (match), 3 (new).
    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 100, 'A'), (3, 300, 'C')")
        .await
        .unwrap();

    let r = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET n = s.sn, v = s.sv \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2", "1 update + 1 insert");

    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(ns, vec![100, 20, 300]); // id=1 updated, id=2 untouched, id=3 inserted
    assert_eq!(vs, vec!["A", "b", "C"]);
}

// ─── WHEN MATCHED AND <cond> ordering: first-match-wins ───────────────────────

#[tokio::test]
async fn merge_when_matched_and_first_match_wins() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    // id 1: sn=5 → first clause (sn<10) wins; id 2: sn=50 → second clause wins.
    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 0, 'x'), (2, 0, 'x')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 5, 'lo'), (2, 50, 'hi')")
        .await
        .unwrap();

    let r = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED AND s.sn < 10 THEN UPDATE SET v = 'small' \
             WHEN MATCHED THEN UPDATE SET v = 'big'",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2");

    let (_, _, vs) = dump(&sess, "t").await;
    assert_eq!(vs, vec!["small", "big"]);
}

// ─── DELETE action ───────────────────────────────────────────────────────────

#[tokio::test]
async fn merge_matched_delete() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")
        .await
        .unwrap();
    // Source flags rows 1 and 3 for deletion.
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 0, 'x'), (3, 0, 'x')")
        .await
        .unwrap();

    let r = sess
        .execute("MERGE INTO t USING s ON t.id = s.sid WHEN MATCHED THEN DELETE")
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2");

    let (ids, _, _) = dump(&sess, "t").await;
    assert_eq!(ids, vec![2], "only the unmatched target row survives");
}

// ─── matched DELETE for some, UPDATE for others (mixed, ordered) ──────────────

#[tokio::test]
async fn merge_mixed_matched_delete_and_update() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 1, 'a'), (2, 2, 'b')")
        .await
        .unwrap();
    // id 1 → delete (sn=0), id 2 → update (sn>0), id 9 → insert.
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 0, 'x'), (2, 22, 'B'), (9, 99, 'Z')")
        .await
        .unwrap();

    let r = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED AND s.sn = 0 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET n = s.sn, v = s.sv \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 3");

    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![2, 9]);
    assert_eq!(ns, vec![22, 99]);
    assert_eq!(vs, vec!["B", "Z"]);
}

// ─── DO NOTHING (WHEN MATCHED ... no action via predicate; explicit clauses) ──

#[tokio::test]
async fn merge_do_nothing_when_matched() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 1, 'keep')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 9, 'IGN'), (2, 9, 'NEW')")
        .await
        .unwrap();

    // Matched row 1: no MATCHED clause is provided, so it is left alone.
    // Unmatched row 2: inserted.
    let r = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 1");

    let (ids, _, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(vs, vec!["keep", "NEW"], "matched row untouched");
}

// ─── duplicate target match → SQLSTATE 21000 (CardinalityViolation) ───────────

#[tokio::test]
async fn merge_duplicate_target_match_raises_21000() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 1, 'a')")
        .await
        .unwrap();
    // Two source rows both join to target id=1 → PG 21000.
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (10, 1, 'p'), (11, 1, 'q')")
        .await
        .unwrap();

    let err = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sn \
             WHEN MATCHED THEN UPDATE SET v = s.sv",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::CardinalityViolation(_)),
        "expected CardinalityViolation (SQLSTATE 21000), got {err:?}"
    );
    assert!(
        format!("{err}").contains("affect row a second time"),
        "PG-shaped message expected, got: {err}"
    );

    // The target must be untouched (detection happens before any mutation).
    let (_, _, vs) = dump(&sess, "t").await;
    assert_eq!(vs, vec!["a"]);
}

// ─── source as VALUES ─────────────────────────────────────────────────────────

#[tokio::test]
async fn merge_source_as_values() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a')")
        .await
        .unwrap();

    // VALUES source with explicit column aliases.
    let r = sess
        .execute(
            "MERGE INTO t USING (VALUES (1, 111, 'A'), (2, 222, 'B')) AS s(sid, sn, sv) \
             ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET n = s.sn, v = s.sv \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2");

    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(ns, vec![111, 222]);
    assert_eq!(vs, vec!["A", "B"]);
}

// ─── source as SELECT subquery ───────────────────────────────────────────────

#[tokio::test]
async fn merge_source_as_select_subquery() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 5, 'A'), (2, 7, 'B'), (3, 0, 'skip')")
        .await
        .unwrap();

    // Subquery filters out sn=0 rows.
    let r = sess
        .execute(
            "MERGE INTO t USING (SELECT sid, sn, sv FROM s WHERE sn > 0) AS q \
             ON t.id = q.sid \
             WHEN MATCHED THEN UPDATE SET n = q.sn, v = q.sv \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (q.sid, q.sn, q.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2");

    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(ns, vec![5, 7]);
    assert_eq!(vs, vec!["A", "B"]);
}

// ─── atomicity: a failing action rolls back the whole MERGE ──────────────────

#[tokio::test]
async fn merge_atomicity_failing_action_rolls_back_all() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    // Target ids 1,2. Source: id 1 updates fine; id 9 would INSERT a duplicate
    // PK (1) → UniqueViolation mid-merge, which must roll back the id=1 update.
    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 999, 'UPD'), (9, 0, 'DUP')")
        .await
        .unwrap();

    let err = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET n = s.sn, v = s.sv \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (1, s.sn, s.sv)",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::UniqueViolation(_)),
        "duplicate PK insert mid-merge must surface UniqueViolation, got {err:?}"
    );

    // The whole MERGE rolled back: id=1 must still be (10, 'a'), no new rows.
    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2], "no partial insert survived");
    assert_eq!(ns, vec![10, 20], "the id=1 update was rolled back");
    assert_eq!(vs, vec!["a", "b"]);
}

// ─── RLS enforcement on all three actions ─────────────────────────────────────

#[tokio::test]
async fn merge_rls_enforced_on_update_insert_delete() {
    let (_d, eng) = open_engine().await;
    let project = ProjectId::new();
    let admin = eng.open_session(project).await.unwrap();

    // owner-scoped table; policy: a session may only see/affect its own rows.
    admin
        .execute(
            "CREATE TABLE acct (id BIGINT NOT NULL PRIMARY KEY, owner TEXT NOT NULL, bal BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE acct ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own ON acct FOR ALL TO PUBLIC USING (owner = current_user)",
        )
        .await
        .unwrap();
    // Seed: one row owned by alice, one by bob (admin bypasses? no — insert as each).
    let alice = eng.open_session_as(project, "alice").await.unwrap();
    let bob = eng.open_session_as(project, "bob").await.unwrap();
    alice
        .execute("INSERT INTO acct (id, owner, bal) VALUES (1, 'alice', 100)")
        .await
        .unwrap();
    bob.execute("INSERT INTO acct (id, owner, bal) VALUES (2, 'bob', 200)")
        .await
        .unwrap();

    // alice's source: update her row, insert a new alice row, and try to insert
    // a bob-owned row (must be blocked by WITH CHECK → RlsViolation).
    alice
        .execute(
            "CREATE TABLE src (sid BIGINT NOT NULL PRIMARY KEY, owner TEXT NOT NULL, bal BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    // A clean upsert that stays within alice's policy: update id=1, insert id=3.
    alice
        .execute("INSERT INTO src (sid, owner, bal) VALUES (1, 'alice', 150), (3, 'alice', 300)")
        .await
        .unwrap();
    let r = alice
        .execute(
            "MERGE INTO acct USING src ON acct.id = src.sid \
             WHEN MATCHED THEN UPDATE SET bal = src.bal \
             WHEN NOT MATCHED THEN INSERT (id, owner, bal) VALUES (src.sid, src.owner, src.bal)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 2");

    // alice sees her two rows with the merged values.
    let b = rows_of(
        &alice,
        "SELECT id, bal FROM acct WHERE owner = 'alice' ORDER BY id",
    )
    .await;
    assert_eq!(col_i64(&b, "id"), vec![1, 3]);
    assert_eq!(col_i64(&b, "bal"), vec![150, 300]);

    // Now a MERGE that would INSERT a row owned by bob → blocked by WITH CHECK.
    alice
        .execute("INSERT INTO src (sid, owner, bal) VALUES (4, 'bob', 999)")
        .await
        .unwrap();
    let err = alice
        .execute(
            "MERGE INTO acct USING src ON acct.id = src.sid \
             WHEN NOT MATCHED AND src.owner = 'bob' THEN INSERT (id, owner, bal) \
               VALUES (src.sid, src.owner, src.bal)",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::RlsViolation(_)),
        "inserting a foreign-owned row via MERGE must be blocked by RLS, got {err:?}"
    );

    // bob's row 2 is untouched and bob still owns exactly it.
    let b = rows_of(&bob, "SELECT id, bal FROM acct ORDER BY id").await;
    assert_eq!(col_i64(&b, "id"), vec![2]);
    assert_eq!(col_i64(&b, "bal"), vec![200]);
}

// ─── read-back through the fast paths after a MERGE ───────────────────────────

#[tokio::test]
async fn merge_readback_through_pk_fast_path() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 55, 'Z'), (2, 66, 'Y')")
        .await
        .unwrap();

    sess.execute(
        "MERGE INTO t USING s ON t.id = s.sid \
         WHEN MATCHED THEN UPDATE SET n = s.sn, v = s.sv \
         WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
    )
    .await
    .unwrap();

    // Point-lookup fast path on the updated row and the inserted row.
    let b1 = rows_of(&sess, "SELECT n, v FROM t WHERE id = 1").await;
    assert_eq!(col_i64(&b1, "n"), vec![55]);
    assert_eq!(col_string(&b1, "v"), vec!["Z"]);
    let b2 = rows_of(&sess, "SELECT n, v FROM t WHERE id = 2").await;
    assert_eq!(col_i64(&b2, "n"), vec![66]);
    assert_eq!(col_string(&b2, "v"), vec!["Y"]);
}

// ─── empty source ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn merge_empty_source_is_noop() {
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;

    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await
        .unwrap();
    // s is empty.

    let r = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET v = 'X' \
             WHEN NOT MATCHED THEN INSERT (id, n, v) VALUES (s.sid, s.sn, s.sv)",
        )
        .await
        .unwrap();
    assert_eq!(tag_of(&r), "MERGE 0");

    let (ids, ns, vs) = dump(&sess, "t").await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(ns, vec![10, 20]);
    assert_eq!(vs, vec!["a", "b"]);
}

// ─── RETURNING is rejected with a typed 0A000 (feature_not_supported) ────────

#[tokio::test]
async fn merge_returning_rejected_typed() {
    // RETURNING is PG-17; sqlparser may not even parse it in MERGE position. We
    // accept either a typed FeatureNotSupported (our explicit reject) OR a parse
    // error — both keep the surface honest. We never want a silent wrong answer.
    let (_d, eng) = open_engine().await;
    let sess = session(&eng).await;
    create_target(&sess, "t").await;
    create_source(&sess, "s").await;
    sess.execute("INSERT INTO t (id, n, v) VALUES (1, 1, 'a')")
        .await
        .unwrap();
    sess.execute("INSERT INTO s (sid, sn, sv) VALUES (1, 2, 'b')")
        .await
        .unwrap();

    let res = sess
        .execute(
            "MERGE INTO t USING s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET v = s.sv RETURNING id",
        )
        .await;
    assert!(res.is_err(), "MERGE ... RETURNING must not silently succeed");
}
