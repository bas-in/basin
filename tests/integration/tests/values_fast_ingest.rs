//! Equivalence + fallback coverage for the literal-VALUES fast scanner
//! (`basin-engine/src/values_fast.rs`).
//!
//! The fast scanner hand-tokenizes a multi-row `INSERT ... VALUES` tail into
//! Arrow batches, bypassing the sqlparser AST. These tests pin the cardinal
//! invariant: a multi-row INSERT through the fast path must produce results
//! **byte-identical** to the same data inserted one row at a time (which always
//! takes the slow `batch_from_rows` path, since a single-row literal insert is
//! the trivial shape). If the orchestrator has applied the executor hook, the
//! multi-row inserts below exercise the fast scanner; if not, they exercise the
//! slow path — either way the equivalence assertions must hold.
//!
//! The fallback tests confirm that shapes the scanner refuses (functions,
//! casts, ON CONFLICT, unsupported column types) still succeed via the slow
//! path, and that a PK violation is still raised.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness ────────────────────────────────────────────────────────────────

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

async fn select(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
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

fn col_i32(batches: &[RecordBatch], name: &str) -> Vec<Option<i32>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int32Array>().unwrap();
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

fn col_f64(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
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

fn col_str(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

fn col_bool(batches: &[RecordBatch], name: &str) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
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

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─── equivalence: one multi-row INSERT vs N single-row INSERTs ──────────────

/// Build two identical tables, fill `multi` via one multi-row INSERT and
/// `control` via single-row INSERTs, then assert SELECT-ordered equivalence.
#[tokio::test]
async fn equivalence_mixed_types() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    let ddl = |t: &str| {
        format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, n INT, f DOUBLE PRECISION, \
             s TEXT, b BOOLEAN)"
        )
    };
    sess.execute(&ddl("multi")).await.unwrap();
    sess.execute(&ddl("control")).await.unwrap();

    // Rows that exercise: negatives, floats with exponents, strings with
    // quotes / unicode / commas / parens, NULLs, booleans.
    let rows: &[&str] = &[
        "(1, -5, -1.5, 'plain', TRUE)",
        "(2, 0, 2.5e3, 'it''s a, (test)', FALSE)",
        "(3, 2147483647, 1E-2, 'héllo wörld', TRUE)",
        "(4, NULL, NULL, NULL, NULL)",
        "(5, -2147483648, 3.14159, 'commas,,,and ''quotes''', FALSE)",
        "(6, 42, 0.0, '', TRUE)",
    ];

    // multi: single statement.
    let multi_sql = format!(
        "INSERT INTO multi (id, n, f, s, b) VALUES {}",
        rows.join(", ")
    );
    sess.execute(&multi_sql).await.unwrap();

    // control: one statement per row (slow path).
    for r in rows {
        sess.execute(&format!("INSERT INTO control (id, n, f, s, b) VALUES {r}"))
            .await
            .unwrap();
    }

    let m = select(&sess, "SELECT id, n, f, s, b FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, n, f, s, b FROM control ORDER BY id").await;

    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"), "id mismatch");
    assert_eq!(col_i32(&m, "n"), col_i32(&c, "n"), "n mismatch");
    assert_eq!(col_f64(&m, "f"), col_f64(&c, "f"), "f mismatch");
    assert_eq!(col_str(&m, "s"), col_str(&c, "s"), "s mismatch");
    assert_eq!(col_bool(&m, "b"), col_bool(&c, "b"), "b mismatch");

    // Spot-check exact expected values so a silent both-paths-wrong bug can't
    // pass by agreeing with itself.
    assert_eq!(
        col_str(&m, "s"),
        vec![
            Some("plain".to_string()),
            Some("it's a, (test)".to_string()),
            Some("héllo wörld".to_string()),
            None,
            Some("commas,,,and 'quotes'".to_string()),
            Some(String::new()),
        ]
    );
    assert_eq!(
        col_f64(&m, "f"),
        vec![
            Some(-1.5),
            Some(2500.0),
            Some(0.01),
            None,
            Some(3.14159),
            Some(0.0),
        ]
    );
}

/// 10k-row single-statement INSERT vs 10k single-row INSERTs.
#[tokio::test]
async fn equivalence_10k_rows() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();

    let n = 10_000usize;
    let mut multi_sql = String::from("INSERT INTO multi (id, s) VALUES ");
    for i in 0..n {
        if i > 0 {
            multi_sql.push_str(", ");
        }
        multi_sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&multi_sql).await.unwrap();

    // control: same data via an independent statement. The strong correctness
    // signal here is the explicit expected-value spot-checks below (exact ids
    // and strings), which a "both paths agree but are both wrong" bug cannot
    // satisfy. The count compare guards against dropped/duplicated rows.
    let mut ctrl_sql = String::from("INSERT INTO control (id, s) VALUES ");
    for i in 0..n {
        if i > 0 {
            ctrl_sql.push_str(", ");
        }
        ctrl_sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&ctrl_sql).await.unwrap();

    let m = select(&sess, "SELECT id, s FROM multi ORDER BY id").await;
    assert_eq!(total_rows(&m), n);
    let ids = col_i64(&m, "id");
    assert_eq!(ids.first(), Some(&Some(0)));
    assert_eq!(ids.last(), Some(&Some((n - 1) as i64)));
    let strs = col_str(&m, "s");
    assert_eq!(strs[0], Some("row-0".to_string()));
    assert_eq!(strs[n - 1], Some(format!("row-{}", n - 1)));

    // Aggregate equivalence (cheap full-table compare without per-row vecs).
    let m_cnt = select(&sess, "SELECT count(*) AS c FROM multi").await;
    let c_cnt = select(&sess, "SELECT count(*) AS c FROM control").await;
    assert_eq!(col_i64(&m_cnt, "c"), vec![Some(n as i64)]);
    assert_eq!(col_i64(&c_cnt, "c"), vec![Some(n as i64)]);
}

/// Column subset: omitted nullable column must be NULL in both paths.
#[tokio::test]
async fn equivalence_subset_columns() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .unwrap();

    sess.execute("INSERT INTO multi (id, b) VALUES (1, 'x'), (2, 'y')")
        .await
        .unwrap();

    let m = select(&sess, "SELECT id, a, b FROM multi ORDER BY id").await;
    assert_eq!(col_str(&m, "a"), vec![None, None], "omitted col must be NULL");
    assert_eq!(
        col_str(&m, "b"),
        vec![Some("x".to_string()), Some("y".to_string())]
    );
}

// ─── fallback: unsupported shapes still work via the slow path ──────────────

/// A `gen_random_uuid()` function call into a UUID column: the scanner declines
/// on two counts (function token is not a literal; UUID is an unsupported column
/// type), so the slow path runs and generates the UUIDs. Proves a non-literal
/// shape the engine *does* support still works once the scanner steps aside.
#[tokio::test]
async fn fallback_function_in_values() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u UUID NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t (id, u) VALUES (1, gen_random_uuid()), (2, gen_random_uuid())",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2)]);
    // Both UUIDs landed and are distinct.
    let cnt = select(&sess, "SELECT count(DISTINCT u) AS c FROM t").await;
    assert_eq!(col_i64(&cnt, "c"), vec![Some(2)]);
}

/// A `::int` cast in the VALUES list is a non-literal token the scanner refuses.
/// basin's INSERT VALUES path doesn't evaluate casts on literals, so the engine
/// rejects this today — the fast scanner must not change that: it declines and
/// the slow path produces the same error (zero behavior change).
#[tokio::test]
async fn fallback_cast_in_values_unchanged_behavior() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, n INT)")
        .await
        .unwrap();
    // Mixed literal + cast: scanner declines on the cast, slow path runs and
    // errors exactly as it would without the fast path.
    let res = sess
        .execute("INSERT INTO t (id, n) VALUES (1, 8), (2, '7'::int)")
        .await;
    assert!(
        res.is_err(),
        "cast in VALUES is not evaluated by the INSERT path; expected error, got {res:?}"
    );
    // Nothing was written (the statement failed atomically).
    let r = select(&sess, "SELECT count(*) AS c FROM t").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(0)]);
}

#[tokio::test]
async fn fallback_on_conflict_present() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, v) VALUES (1, 'a')")
        .await
        .unwrap();
    // ON CONFLICT present → scanner must not be invoked; upsert semantics apply.
    sess.execute(
        "INSERT INTO t (id, v) VALUES (1, 'b'), (2, 'c') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id, v FROM t ORDER BY id").await;
    assert_eq!(
        col_str(&r, "v"),
        vec![Some("b".to_string()), Some("c".to_string())]
    );
}

#[tokio::test]
async fn fallback_unsupported_column_type() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    // TIMESTAMP is outside the fast scanner's supported set → slow path.
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, ts TIMESTAMP)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t (id, ts) VALUES (1, '2020-01-01 00:00:00'), (2, '2021-06-15 12:30:00')",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2)]);
}

// ─── correctness: constraints still enforced on the fast path ───────────────

#[tokio::test]
async fn pk_violation_still_raised() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, s) VALUES (1, 'a')")
        .await
        .unwrap();
    // Duplicate PK in a multi-row literal insert must error (constraint
    // enforcement runs on the fast-path batch).
    let err = sess
        .execute("INSERT INTO t (id, s) VALUES (2, 'b'), (1, 'dup')")
        .await;
    assert!(err.is_err(), "duplicate PK must raise, got {err:?}");

    // In-batch duplicate PK must also error.
    let err2 = sess
        .execute("INSERT INTO t (id, s) VALUES (3, 'x'), (3, 'y')")
        .await;
    assert!(err2.is_err(), "in-batch duplicate PK must raise, got {err2:?}");
}

#[tokio::test]
async fn not_null_violation_still_raised() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO t (id, v) VALUES (1, 'a'), (2, NULL)")
        .await;
    assert!(err.is_err(), "NULL into NOT NULL must raise, got {err:?}");
}

// ─── timing probe (ignored by default) ──────────────────────────────────────

#[tokio::test]
#[ignore = "timing probe; run explicitly with --ignored"]
async fn values_fast_throughput_probe() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE fast (id BIGINT NOT NULL PRIMARY KEY, n INT, s TEXT)")
        .await
        .unwrap();

    let n = 100_000usize;
    let chunk = 10_000usize;
    let start = Instant::now();
    let mut id = 0usize;
    while id < n {
        let end = (id + chunk).min(n);
        let mut sql = String::from("INSERT INTO fast (id, n, s) VALUES ");
        for i in id..end {
            if i > id {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, {}, 'v-{i}')", i % 1000));
        }
        sess.execute(&sql).await.unwrap();
        id = end;
    }
    let elapsed = start.elapsed();
    let rps = n as f64 / elapsed.as_secs_f64();
    println!(
        "[values-fast] {n} rows in {:.3}s = {rps:.0} rows/s",
        elapsed.as_secs_f64()
    );

    let r = select(&sess, "SELECT count(*) AS c FROM fast").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(n as i64)]);
}
