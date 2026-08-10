//! Composite ON CONFLICT target coverage.
//!
//! Tests for:
//!   1.  ON CONFLICT (a, b) DO UPDATE against composite PK
//!   2.  ON CONFLICT (a, b) DO UPDATE against multi-column UNIQUE constraint
//!   3.  ON CONFLICT (a, b) DO NOTHING
//!   4.  EXCLUDED refs with composite targets
//!   5.  Mismatch: target doesn't match any constraint → SQLSTATE 42P10
//!       (BasinError::AmbiguousConflictTarget)
//!   6.  Multi-row VALUES with mixed conflict / no-conflict rows
//!   7.  NULL semantics: NULLs in composite key do NOT produce a conflict
//!   8.  ON CONFLICT ON CONSTRAINT <name> DO UPDATE
//!   9.  ON CONFLICT ON CONSTRAINT <name> DO NOTHING
//!  10.  ON CONFLICT ON CONSTRAINT <unknown name> → 42P10
//!  11.  ON CONFLICT ON CONSTRAINT using the implicit PK name (<table>_pkey)
//!
//! Place this file as `tests/integration/tests/composite_on_conflict.rs` in
//! the live tree.  Mirror path:
//! `/tmp/basin_composite_cursors/testing/tests/composite_on_conflict.rs`.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
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

fn col_str(batches: &[RecordBatch], name: &str) -> Vec<String> {
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

fn rows(result: ExecResult) -> Vec<RecordBatch> {
    match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => vec![],
    }
}

// ---------------------------------------------------------------------------
// 1. Composite PK upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn composite_pk_upsert_do_update() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE order_items (\
         order_id BIGINT NOT NULL, item_id BIGINT NOT NULL, qty BIGINT NOT NULL, \
         PRIMARY KEY (order_id, item_id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO order_items VALUES (1, 1, 5), (1, 2, 3)")
        .await
        .unwrap();

    // Upsert: (1,1) conflicts → update qty; (1,3) is new → insert.
    sess.execute(
        "INSERT INTO order_items (order_id, item_id, qty) VALUES (1, 1, 99), (1, 3, 7) \
         ON CONFLICT (order_id, item_id) DO UPDATE SET qty = EXCLUDED.qty",
    )
    .await
    .expect("composite pk upsert should succeed");

    let batches = rows(
        sess.execute("SELECT order_id, item_id, qty FROM order_items ORDER BY order_id, item_id")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&batches, "order_id"), vec![1, 1, 1]);
    assert_eq!(col_i64(&batches, "item_id"), vec![1, 2, 3]);
    assert_eq!(col_i64(&batches, "qty"), vec![99, 3, 7]);
}

// ---------------------------------------------------------------------------
// 2. Multi-column UNIQUE upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn composite_unique_upsert_do_update() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE memberships (\
         id BIGINT PRIMARY KEY, org_id BIGINT NOT NULL, user_id BIGINT NOT NULL, \
         role TEXT NOT NULL, \
         UNIQUE (org_id, user_id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO memberships VALUES (1, 10, 100, 'viewer')")
        .await
        .unwrap();

    // Conflict on (org_id=10, user_id=100) → update role.
    sess.execute(
        "INSERT INTO memberships (id, org_id, user_id, role) VALUES (999, 10, 100, 'admin') \
         ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role",
    )
    .await
    .expect("composite unique upsert should succeed");

    let batches = rows(
        sess.execute("SELECT role FROM memberships WHERE org_id = 10 AND user_id = 100")
            .await
            .unwrap(),
    );
    assert_eq!(col_str(&batches, "role"), vec!["admin"]);

    // id must remain unchanged — still exactly 1 row in the table.
    let cnt = rows(
        sess.execute("SELECT COUNT(*) AS n FROM memberships")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&cnt, "n"), vec![1], "still exactly 1 row");
}

// ---------------------------------------------------------------------------
// 3. DO NOTHING with composite target
// ---------------------------------------------------------------------------

#[tokio::test]
async fn composite_pk_do_nothing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE kv2 (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, v TEXT NOT NULL, \
         PRIMARY KEY (k1, k2))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO kv2 VALUES (1, 1, 'original')")
        .await
        .unwrap();

    // Attempt to insert (1,1) again — must be silently suppressed.
    sess.execute(
        "INSERT INTO kv2 VALUES (1, 1, 'ignored'), (2, 2, 'new') \
         ON CONFLICT (k1, k2) DO NOTHING",
    )
    .await
    .expect("DO NOTHING should succeed");

    let batches = rows(
        sess.execute("SELECT k1, k2, v FROM kv2 ORDER BY k1, k2")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&batches, "k1"), vec![1, 2]);
    assert_eq!(col_i64(&batches, "k2"), vec![1, 2]);
    assert_eq!(col_str(&batches, "v"), vec!["original", "new"]);
}

// ---------------------------------------------------------------------------
// 4. EXCLUDED references with composite target
// ---------------------------------------------------------------------------

#[tokio::test]
async fn excluded_refs_with_composite_target() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE scores (\
         player TEXT NOT NULL, game TEXT NOT NULL, score BIGINT NOT NULL, \
         PRIMARY KEY (player, game))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO scores VALUES ('alice', 'chess', 100)")
        .await
        .unwrap();

    // DO UPDATE using EXCLUDED to pick the higher score.
    sess.execute(
        "INSERT INTO scores (player, game, score) VALUES ('alice', 'chess', 150) \
         ON CONFLICT (player, game) DO UPDATE \
         SET score = CASE WHEN EXCLUDED.score > scores.score \
                     THEN EXCLUDED.score ELSE scores.score END",
    )
    .await
    .expect("EXCLUDED ref composite upsert");

    let batches = rows(
        sess.execute("SELECT score FROM scores WHERE player = 'alice' AND game = 'chess'")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&batches, "score"), vec![150]);
}

// ---------------------------------------------------------------------------
// 5. Mismatch: target doesn't match any constraint → SQLSTATE 42P10
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conflict_target_mismatch_42p10() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Only (a) is a PK — (a, b) together don't form any unique constraint.
    sess.execute("CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a))")
        .await
        .unwrap();

    let err = sess
        .execute("INSERT INTO t VALUES (1, 2) ON CONFLICT (a, b) DO UPDATE SET b = EXCLUDED.b")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::AmbiguousConflictTarget(_)),
        "expected AmbiguousConflictTarget (SQLSTATE 42P10), got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("no unique or exclusion constraint"),
        "msg = {msg}"
    );
}

// ---------------------------------------------------------------------------
// 6. Multi-row VALUES: mixed conflict / no-conflict
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multirow_mixed_conflict_no_conflict() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE events (\
         src TEXT NOT NULL, day TEXT NOT NULL, cnt BIGINT NOT NULL, \
         PRIMARY KEY (src, day))",
    )
    .await
    .unwrap();
    // Seed two rows.
    sess.execute("INSERT INTO events VALUES ('web', '2024-01-01', 10), ('app', '2024-01-01', 5)")
        .await
        .unwrap();

    // Upsert 3 rows: 2 conflict (web, app), 1 fresh (cli).
    sess.execute(
        "INSERT INTO events (src, day, cnt) VALUES \
         ('web', '2024-01-01', 20), \
         ('app', '2024-01-01', 8), \
         ('cli', '2024-01-01', 3) \
         ON CONFLICT (src, day) DO UPDATE SET cnt = cnt + EXCLUDED.cnt",
    )
    .await
    .expect("mixed conflict upsert");

    let batches = rows(
        sess.execute("SELECT src, cnt FROM events ORDER BY src")
            .await
            .unwrap(),
    );
    assert_eq!(col_str(&batches, "src"), vec!["app", "cli", "web"]);
    assert_eq!(
        col_i64(&batches, "cnt"),
        vec![13, 3, 30],
        "app=5+8=13, cli=3 (new), web=10+20=30"
    );
}

// ---------------------------------------------------------------------------
// 7. NULL semantics: NULLs in composite key do NOT produce a conflict
// ---------------------------------------------------------------------------

#[tokio::test]
async fn null_in_composite_key_no_conflict() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // A UNIQUE constraint on nullable columns.  Per SQL standard and PG:
    // NULL != NULL, so two rows with NULL in the same column do not conflict.
    sess.execute(
        "CREATE TABLE nullable_uk (\
         id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, \
         UNIQUE (a, b))",
    )
    .await
    .unwrap();

    // These two rows both have NULL for 'a'; they must coexist.
    sess.execute("INSERT INTO nullable_uk VALUES (1, NULL, 1)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nullable_uk VALUES (2, NULL, 1)")
        .await
        .unwrap();

    let cnt = rows(
        sess.execute("SELECT COUNT(*) AS n FROM nullable_uk")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&cnt, "n"), vec![2], "both NULL rows must coexist");

    // ON CONFLICT DO NOTHING: new NULL row must pass through — NOT suppressed.
    sess.execute(
        "INSERT INTO nullable_uk (id, a, b) VALUES (3, NULL, 1) \
         ON CONFLICT (a, b) DO NOTHING",
    )
    .await
    .unwrap();

    let cnt = rows(
        sess.execute("SELECT COUNT(*) AS n FROM nullable_uk")
            .await
            .unwrap(),
    );
    assert_eq!(
        col_i64(&cnt, "n"),
        vec![3],
        "NULL rows must not be suppressed by DO NOTHING"
    );
}

// ---------------------------------------------------------------------------
// 8. ON CONFLICT ON CONSTRAINT <name> DO UPDATE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_conflict_on_constraint_name_do_update() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE tags (\
         id BIGINT PRIMARY KEY, \
         category TEXT NOT NULL, tag TEXT NOT NULL, weight BIGINT NOT NULL, \
         CONSTRAINT tags_cat_tag_uk UNIQUE (category, tag))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO tags VALUES (1, 'color', 'red', 10)")
        .await
        .unwrap();

    // ON CONFLICT ON CONSTRAINT <name> form.
    sess.execute(
        "INSERT INTO tags (id, category, tag, weight) VALUES (999, 'color', 'red', 50) \
         ON CONFLICT ON CONSTRAINT tags_cat_tag_uk DO UPDATE SET weight = EXCLUDED.weight",
    )
    .await
    .expect("ON CONSTRAINT upsert should succeed");

    let batches = rows(
        sess.execute("SELECT weight FROM tags WHERE category = 'color' AND tag = 'red'")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&batches, "weight"), vec![50]);
}

// ---------------------------------------------------------------------------
// 9. ON CONFLICT ON CONSTRAINT <name> DO NOTHING
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_conflict_on_constraint_name_do_nothing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE tags2 (\
         id BIGINT PRIMARY KEY, \
         category TEXT NOT NULL, tag TEXT NOT NULL, \
         CONSTRAINT tags2_cat_tag_uk UNIQUE (category, tag))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO tags2 VALUES (1, 'size', 'large')")
        .await
        .unwrap();

    // Duplicate on the named constraint — must be silently suppressed.
    sess.execute(
        "INSERT INTO tags2 (id, category, tag) VALUES (999, 'size', 'large') \
         ON CONFLICT ON CONSTRAINT tags2_cat_tag_uk DO NOTHING",
    )
    .await
    .expect("ON CONSTRAINT DO NOTHING should succeed");

    let cnt = rows(
        sess.execute("SELECT COUNT(*) AS n FROM tags2")
            .await
            .unwrap(),
    );
    assert_eq!(col_i64(&cnt, "n"), vec![1], "still exactly 1 row");
}

// ---------------------------------------------------------------------------
// 10. ON CONFLICT ON CONSTRAINT <unknown name> → SQLSTATE 42P10
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_conflict_on_constraint_unknown_name_42p10() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t2 (a BIGINT PRIMARY KEY, b TEXT NOT NULL)")
        .await
        .unwrap();

    let err = sess
        .execute(
            "INSERT INTO t2 VALUES (1, 'x') \
             ON CONFLICT ON CONSTRAINT no_such_constraint DO UPDATE SET b = EXCLUDED.b",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::AmbiguousConflictTarget(_)),
        "expected AmbiguousConflictTarget (SQLSTATE 42P10), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// 11. ON CONFLICT ON CONSTRAINT using implicit PK constraint name (<tbl>_pkey)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_conflict_on_constraint_implicit_pkey_name() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO widgets VALUES (1, 'original')")
        .await
        .unwrap();

    // The implicit PK constraint name is "widgets_pkey".
    sess.execute(
        "INSERT INTO widgets (id, name) VALUES (1, 'updated') \
         ON CONFLICT ON CONSTRAINT widgets_pkey DO UPDATE SET name = EXCLUDED.name",
    )
    .await
    .expect("implicit _pkey constraint upsert");

    let batches = rows(
        sess.execute("SELECT name FROM widgets WHERE id = 1")
            .await
            .unwrap(),
    );
    assert_eq!(col_str(&batches, "name"), vec!["updated"]);
}
