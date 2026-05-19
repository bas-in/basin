//! Smoke test for the `APPROX_COUNT_DISTINCT` aggregate UDF.
//!
//! Creates a table, inserts rows with a known number of distinct key values,
//! and verifies that `APPROX_COUNT_DISTINCT(k)` returns a result within ±5 %
//! of the true cardinality.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn query_i64(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.expect("query failed") {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() > 0 {
                    let col = b.column(0);
                    // APPROX_COUNT_DISTINCT returns Int64
                    let arr = col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("expected Int64Array from APPROX_COUNT_DISTINCT");
                    return arr.value(0);
                }
            }
            panic!("no rows returned from: {sql}");
        }
        ExecResult::Empty { tag } => panic!("expected rows but got DDL/DML tag: {tag}"),
    }
}

fn assert_within(est: i64, true_val: i64, pct: f64) {
    let err = (est as f64 - true_val as f64).abs() / true_val as f64;
    assert!(
        err <= pct,
        "APPROX_COUNT_DISTINCT: estimate={est}, true={true_val}, error={:.2}% > {:.0}%",
        err * 100.0,
        pct * 100.0,
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// 17 distinct integer keys — matches the spec gate example.
#[tokio::test]
async fn approx_count_distinct_17_keys() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE acd17 (k BIGINT, v TEXT)").await;

    // Insert 17 distinct keys, each repeated 5 times (85 rows total).
    let mut values: Vec<String> = Vec::new();
    for k in 0i64..17 {
        for rep in 0..5i64 {
            values.push(format!("({k}, 'val_{k}_{rep}')"));
        }
    }
    let insert = format!("INSERT INTO acd17 (k, v) VALUES {}", values.join(", "));
    exec(&sess, &insert).await;

    let est = query_i64(&sess, "SELECT APPROX_COUNT_DISTINCT(k) FROM acd17").await;
    // 17 distinct values — exact or near-exact expected (small-range correction).
    assert_within(est, 17, 0.05);
}

/// 1 000 distinct integer keys — exercises the main estimation path.
#[tokio::test]
async fn approx_count_distinct_1k_keys() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE acd1k (k BIGINT)").await;

    // Batch inserts in groups of 100 to keep individual SQL small.
    for chunk_start in (0i64..1000).step_by(100) {
        let mut vals: Vec<String> = Vec::new();
        for k in chunk_start..chunk_start + 100 {
            vals.push(format!("({k})"));
        }
        let insert = format!("INSERT INTO acd1k (k) VALUES {}", vals.join(", "));
        exec(&sess, &insert).await;
    }

    let est = query_i64(&sess, "SELECT APPROX_COUNT_DISTINCT(k) FROM acd1k").await;
    assert_within(est, 1_000, 0.05);
}

/// String column variant — 50 distinct strings.
#[tokio::test]
async fn approx_count_distinct_string_column() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE acdstr (s TEXT)").await;

    // 50 distinct strings, each inserted twice (100 total rows).
    let mut vals: Vec<String> = Vec::new();
    for i in 0..50i64 {
        vals.push(format!("('key_{i}')"));
        vals.push(format!("('key_{i}')"));
    }
    let insert = format!("INSERT INTO acdstr (s) VALUES {}", vals.join(", "));
    exec(&sess, &insert).await;

    let est = query_i64(&sess, "SELECT APPROX_COUNT_DISTINCT(s) FROM acdstr").await;
    assert_within(est, 50, 0.05);
}

/// Empty table should return 0.
#[tokio::test]
async fn approx_count_distinct_empty_table() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE acdempty (k BIGINT)").await;

    match sess
        .execute("SELECT APPROX_COUNT_DISTINCT(k) FROM acdempty")
        .await
        .expect("APPROX_COUNT_DISTINCT on empty table failed")
    {
        ExecResult::Rows { batches, .. } => {
            // Zero rows in the result means the empty-table short-circuit fired.
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total_rows > 0 {
                // Some engines return a single row with 0 for empty aggregation.
                let col = batches[0].column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64Array");
                assert_eq!(arr.value(0), 0, "empty table should return 0");
            }
            // Zero rows is also acceptable (no rows → count = 0 is the contract).
        }
        ExecResult::Empty { tag } => panic!("expected rows, got tag: {tag}"),
    }
}
