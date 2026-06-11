//! Batched multi-row `ON CONFLICT` coverage.
//!
//! `try_on_conflict_do_update` (executor.rs) batches eligible multi-row
//! upserts: one chunked `SELECT <ck> … WHERE <ck> IN (…)` existence probe and
//! one chunked `UPDATE … SET c = CASE <ck> WHEN … END WHERE <ck> IN (…)` per
//! conflict batch, replacing the per-row probe + per-row UPDATE loop. The
//! per-row path remains the fallback for composite conflict targets,
//! non-literal conflict keys, unsupported key types, and SET assignments that
//! can't be rendered as CASE arms.
//!
//! These tests pin:
//!   * batched results are identical to a sequential single-row upsert twin
//!     (single-row statements always take the per-row path),
//!   * all-conflict / all-fresh / mixed partitions,
//!   * NULL conflict keys never conflict (PG NULLS DISTINCT),
//!   * string keys with embedded quotes survive IN-list / CASE rendering,
//!   * EXCLUDED + arithmetic assignment shapes ride inside CASE arms,
//!   * non-batchable assignment shapes (function call) fall back per-row,
//!   * composite conflict targets still work via the per-row path,
//!   * DO NOTHING benefits from the same batched probe,
//!   * intra-statement duplicate keys error before any row is touched.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness (mirrors dml_extras.rs) ────────────────────────────────────────

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

/// Nullable BIGINT column extraction (for the NULL-conflict-key test).
fn col_i64_opt(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

async fn rows_of(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

// ─── batched vs sequential single-row twin ──────────────────────────────────

/// 50-row mixed upsert (25 conflicting + 25 fresh) with both EXCLUDED and
/// existing-row-arithmetic assignments. The batched statement must produce
/// values identical to (a) a hand-computed expectation and (b) the same data
/// applied as 50 sequential single-row upserts — single-row statements always
/// take the per-row path, so the twin is the unbatched reference.
#[tokio::test]
async fn bulk_upsert_mixed_matches_sequential_single_row() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    for t in ["up_batch", "up_seq"] {
        sess.execute(&format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL, n BIGINT NOT NULL)"
        ))
        .await
        .unwrap();
        // Seed ids 0..24 with v='old{i}', n=i.
        let mut seed = format!("INSERT INTO {t} (id, v, n) VALUES ");
        for i in 0i64..25 {
            if i > 0 {
                seed.push(',');
            }
            seed.push_str(&format!("({i}, 'old{i}', {i})"));
        }
        sess.execute(&seed).await.unwrap();
    }

    // Batched: one 50-row statement.
    let mut upsert = String::from("INSERT INTO up_batch (id, v, n) VALUES ");
    for i in 0i64..50 {
        if i > 0 {
            upsert.push(',');
        }
        upsert.push_str(&format!("({i}, 'new{i}', {})", i * 10));
    }
    upsert.push_str(" ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, n = up_batch.n + EXCLUDED.n");
    let res = sess.execute(&upsert).await.expect("batched upsert");
    match res {
        ExecResult::Empty { ref tag } => {
            assert_eq!(tag, "INSERT 0 50", "PG-style upsert tag");
        }
        other => panic!("expected Empty, got {other:?}"),
    }

    // Sequential twin: 50 single-row statements (per-row path).
    for i in 0i64..50 {
        sess.execute(&format!(
            "INSERT INTO up_seq (id, v, n) VALUES ({i}, 'new{i}', {}) \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, n = up_seq.n + EXCLUDED.n",
            i * 10
        ))
        .await
        .expect("sequential single-row upsert");
    }

    let batched = rows_of(&sess, "SELECT id, v, n FROM up_batch ORDER BY id").await;
    let serial = rows_of(&sess, "SELECT id, v, n FROM up_seq ORDER BY id").await;

    // Hand-computed expectation: conflicting ids 0..24 → n = i + 10i;
    // fresh ids 25..49 → n = 10i; all v = 'new{i}'.
    let expect_ids: Vec<i64> = (0..50).collect();
    let expect_v: Vec<String> = (0..50).map(|i| format!("new{i}")).collect();
    let expect_n: Vec<i64> = (0..50)
        .map(|i| if i < 25 { i + i * 10 } else { i * 10 })
        .collect();

    assert_eq!(col_i64(&batched, "id"), expect_ids);
    assert_eq!(col_string(&batched, "v"), expect_v);
    assert_eq!(col_i64(&batched, "n"), expect_n);

    assert_eq!(col_i64(&serial, "id"), col_i64(&batched, "id"));
    assert_eq!(col_string(&serial, "v"), col_string(&batched, "v"));
    assert_eq!(col_i64(&serial, "n"), col_i64(&batched, "n"));
}

// ─── all-conflict / all-fresh ───────────────────────────────────────────────

/// Every row conflicts; the SET mixes an existing-row arithmetic reference
/// with EXCLUDED — both ride inside the CASE arms.
#[tokio::test]
async fn bulk_upsert_all_conflict_arithmetic_assignments() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE up_all (id BIGINT NOT NULL PRIMARY KEY, amount BIGINT NOT NULL)")
        .await
        .unwrap();

    let mut seed = String::from("INSERT INTO up_all (id, amount) VALUES ");
    for i in 0i64..30 {
        if i > 0 {
            seed.push(',');
        }
        seed.push_str(&format!("({i}, {i})"));
    }
    sess.execute(&seed).await.unwrap();

    let mut upsert = String::from("INSERT INTO up_all (id, amount) VALUES ");
    for i in 0i64..30 {
        if i > 0 {
            upsert.push(',');
        }
        upsert.push_str(&format!("({i}, {})", 100 + i));
    }
    upsert.push_str(" ON CONFLICT (id) DO UPDATE SET amount = up_all.amount + EXCLUDED.amount");
    sess.execute(&upsert).await.expect("all-conflict upsert");

    let batches = rows_of(&sess, "SELECT id, amount FROM up_all ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), (0..30).collect::<Vec<i64>>());
    assert_eq!(
        col_i64(&batches, "amount"),
        (0..30).map(|i| i + 100 + i).collect::<Vec<i64>>(),
        "amount must be old + EXCLUDED per row"
    );
}

/// Zero conflicts → the batched probe reports NoConflicts and the whole batch
/// flows through the normal INSERT path. Covered for both the empty table and
/// the populated-but-disjoint case.
#[tokio::test]
async fn bulk_upsert_all_fresh() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE up_fresh (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();

    // Empty table.
    let mut first = String::from("INSERT INTO up_fresh (id, v) VALUES ");
    for i in 0i64..50 {
        if i > 0 {
            first.push(',');
        }
        first.push_str(&format!("({i}, 'a')"));
    }
    first.push_str(" ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v");
    sess.execute(&first)
        .await
        .expect("all-fresh on empty table");

    // Populated table, disjoint key range.
    let mut second = String::from("INSERT INTO up_fresh (id, v) VALUES ");
    for i in 50i64..100 {
        if i > 50 {
            second.push(',');
        }
        second.push_str(&format!("({i}, 'b')"));
    }
    second.push_str(" ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v");
    sess.execute(&second)
        .await
        .expect("all-fresh on populated table");

    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM up_fresh").await;
    assert_eq!(col_i64(&batches, "n"), vec![100]);
    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM up_fresh WHERE v = 'b'").await;
    assert_eq!(col_i64(&batches, "n"), vec![50]);
}

// ─── NULL conflict keys ─────────────────────────────────────────────────────

/// PG NULLS DISTINCT: a NULL conflict-key value never conflicts — the row is
/// routed to INSERT even when other NULLs exist in the unique column. The
/// non-NULL key in the same batch must still match and UPDATE.
#[tokio::test]
async fn bulk_upsert_null_conflict_keys() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE up_null (id BIGINT NOT NULL PRIMARY KEY, u BIGINT UNIQUE, v TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO up_null (id, u, v) VALUES (1, 10, 'seed'), (9, NULL, 'seednull')")
        .await
        .unwrap();

    // u=10 conflicts → UPDATE id=1; u=NULL never conflicts (even though a
    // NULL already exists in the column) → INSERT id=2.
    sess.execute(
        "INSERT INTO up_null (id, u, v) VALUES (2, NULL, 'x'), (3, 10, 'y') \
         ON CONFLICT (u) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .expect("NULL conflict keys must not conflict");

    let batches = rows_of(&sess, "SELECT id, u, v FROM up_null ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 9]);
    assert_eq!(col_i64_opt(&batches, "u"), vec![Some(10), None, None]);
    assert_eq!(
        col_string(&batches, "v"),
        vec!["y", "x", "seednull"],
        "u=10 row updated; NULL-key row inserted; pre-existing NULL row untouched"
    );

    // Pre-existing (preserved) divergence from PG: two NULL keys in ONE
    // statement render identically for the intra-statement dup check and
    // error — the per-row path behaved the same way before batching.
    let err = sess
        .execute(
            "INSERT INTO up_null (id, u, v) VALUES (4, NULL, 'p'), (5, NULL, 'q') \
             ON CONFLICT (u) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .expect_err("two NULL keys in one statement keep erroring (pre-existing semantics)");
    assert!(
        err.to_string().contains("cannot affect row a second time"),
        "unexpected error: {err}"
    );
}

// ─── quoted string keys ─────────────────────────────────────────────────────

/// TEXT conflict keys with embedded single quotes must survive the IN-list
/// and CASE-arm rendering (sqlparser Display re-escapes them, exactly like
/// the per-row `WHERE k = '…'` did). The conflict target is a non-PK UNIQUE
/// column so the cheap memtable/bloom probe declines and the chunked string
/// IN-list probe is exercised for real.
#[tokio::test]
async fn bulk_upsert_quoted_string_keys() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE up_q (id BIGINT NOT NULL PRIMARY KEY, k TEXT UNIQUE, v BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO up_q (id, k, v) VALUES (1, 'O''Brien', 1), (2, 'plain', 2)")
        .await
        .unwrap();

    sess.execute(
        "INSERT INTO up_q (id, k, v) VALUES \
         (10, 'O''Brien', 100), (11, 'plain', 200), (12, 'new''one', 300), (13, 'x', 400) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .expect("quoted-key upsert");

    let batches = rows_of(&sess, "SELECT k, v FROM up_q ORDER BY k").await;
    assert_eq!(
        col_string(&batches, "k"),
        vec!["O'Brien", "new'one", "plain", "x"]
    );
    assert_eq!(
        col_i64(&batches, "v"),
        vec![100, 300, 200, 400],
        "conflicting quoted keys updated in place; fresh quoted keys inserted"
    );

    // The two conflicting rows kept their original PKs (UPDATE, not re-insert).
    let batches = rows_of(&sess, "SELECT id FROM up_q ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 12, 13]);
}

// ─── non-batchable assignment shape → per-row fallback ──────────────────────

/// A function call on the SET RHS is not CASE-arm safe — the whole statement
/// must fall back to the per-row path and still produce correct results.
#[tokio::test]
async fn bulk_upsert_function_assignment_falls_back() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE up_fn (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO up_fn (id, v) VALUES (0, 'aa'), (1, 'bb'), (2, 'cc')")
        .await
        .unwrap();

    sess.execute(
        "INSERT INTO up_fn (id, v) VALUES (0,'zz'),(1,'zz'),(2,'zz'),(3,'zz'),(4,'zz') \
         ON CONFLICT (id) DO UPDATE SET v = upper(v)",
    )
    .await
    .expect("function-RHS upsert (per-row fallback)");

    let batches = rows_of(&sess, "SELECT id, v FROM up_fn ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![0, 1, 2, 3, 4]);
    assert_eq!(
        col_string(&batches, "v"),
        vec!["AA", "BB", "CC", "zz", "zz"],
        "conflicting rows uppercased from existing values; fresh rows inserted"
    );
}

// ─── composite conflict target (per-row path) ───────────────────────────────

/// Composite conflict targets stay on the per-row path — pinned here so the
/// batched-path gate can't regress them.
#[tokio::test]
async fn bulk_upsert_composite_conflict_target_still_works() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE up_mc (a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT NOT NULL, \
         PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO up_mc (a, b, v) VALUES (1, 1, 'old'), (1, 2, 'old')")
        .await
        .unwrap();

    sess.execute(
        "INSERT INTO up_mc (a, b, v) VALUES (1, 1, 'new'), (1, 2, 'new'), (2, 1, 'new') \
         ON CONFLICT (a, b) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .expect("composite-target upsert");

    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM up_mc").await;
    assert_eq!(col_i64(&batches, "n"), vec![3]);
    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM up_mc WHERE v = 'new'").await;
    assert_eq!(col_i64(&batches, "n"), vec![3], "all rows must read 'new'");
}

// ─── DO NOTHING with the batched probe ──────────────────────────────────────

/// Multi-row DO NOTHING goes through the same batched existence probe
/// (single constraint group, single column): existing keys suppressed,
/// fresh keys inserted, intra-batch duplicates deduped.
#[tokio::test]
async fn bulk_do_nothing_mixed_batched_probe() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE dn_b (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();

    let mut seed = String::from("INSERT INTO dn_b (id, v) VALUES ");
    for i in 0i64..25 {
        if i > 0 {
            seed.push(',');
        }
        seed.push_str(&format!("({i}, 'keep')"));
    }
    sess.execute(&seed).await.unwrap();

    // ids 0..24 exist → suppressed; ids 25..49 fresh → inserted; id 25 is
    // repeated in-batch → second occurrence deduped.
    let mut ins = String::from("INSERT INTO dn_b (id, v) VALUES ");
    for i in 0i64..50 {
        if i > 0 {
            ins.push(',');
        }
        ins.push_str(&format!("({i}, 'x')"));
    }
    ins.push_str(", (25, 'dup')");
    ins.push_str(" ON CONFLICT (id) DO NOTHING");
    sess.execute(&ins).await.expect("mixed DO NOTHING");

    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM dn_b").await;
    assert_eq!(col_i64(&batches, "n"), vec![50]);
    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM dn_b WHERE v = 'keep'").await;
    assert_eq!(
        col_i64(&batches, "n"),
        vec![25],
        "existing rows must be untouched"
    );
    let batches = rows_of(&sess, "SELECT COUNT(*) AS n FROM dn_b WHERE v = 'dup'").await;
    assert_eq!(
        col_i64(&batches, "n"),
        vec![0],
        "in-batch duplicate key must be deduped (first occurrence wins)"
    );
}

// ─── duplicate conflict keys error before any row is touched ────────────────

/// Two rows sharing a conflict key error with PG's "cannot affect row a
/// second time". On the batched path the check runs before any UPDATE
/// executes, so the table must be completely unchanged afterwards (the old
/// per-row path could partially apply before hitting the duplicate).
#[tokio::test]
async fn bulk_upsert_duplicate_key_errors_before_any_update() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE up_dup (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO up_dup (id, v) VALUES (1, 'seed1'), (2, 'seed2')")
        .await
        .unwrap();

    let err = sess
        .execute(
            "INSERT INTO up_dup (id, v) VALUES (1, 'a'), (2, 'b'), (1, 'c') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .expect_err("duplicate conflict key must error");
    assert!(
        err.to_string().contains("cannot affect row a second time"),
        "unexpected error: {err}"
    );

    let batches = rows_of(&sess, "SELECT id, v FROM up_dup ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2]);
    assert_eq!(
        col_string(&batches, "v"),
        vec!["seed1", "seed2"],
        "batched path must not touch any row before the duplicate-key error"
    );
}
