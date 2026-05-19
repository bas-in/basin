//! Smoke test for the `APPROX_PERCENTILE` aggregate UDF.
//!
//! Creates a table, inserts 100k rows with sequential integer ids, and verifies
//! that `APPROX_PERCENTILE(id, 0.5)` returns ≈ 50 000 (±1 %).

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
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

async fn query_f64(sess: &ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await.expect("query failed") {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() > 0 {
                    let col = b.column(0);
                    let arr = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("expected Float64Array from APPROX_PERCENTILE");
                    return arr.value(0);
                }
            }
            panic!("no rows returned from: {sql}");
        }
        ExecResult::Empty { tag } => panic!("expected rows but got DDL/DML tag: {tag}"),
    }
}

/// Insert 100k sequential integer ids and verify that APPROX_PERCENTILE(id, 0.5)
/// returns ≈ 50000 within ±1 %.
#[tokio::test]
async fn approx_percentile_median_100k() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE ap100k (id BIGINT)").await;

    // Insert in batches of 1000 to avoid huge SQL strings.
    for chunk_start in (0i64..100_000).step_by(1000) {
        let vals: Vec<String> = (chunk_start..chunk_start + 1000)
            .map(|i| format!("({i})"))
            .collect();
        let insert = format!("INSERT INTO ap100k (id) VALUES {}", vals.join(", "));
        exec(&sess, &insert).await;
    }

    let est = query_f64(&sess, "SELECT APPROX_PERCENTILE(id, 0.5) FROM ap100k").await;
    // True median of 0..100000 is 49999.5
    let true_val = 49_999.5_f64;
    let err = (est - true_val).abs() / 100_000.0;
    assert!(
        err <= 0.01,
        "APPROX_PERCENTILE p=0.5: estimate={est:.1} true={true_val} rel_err={:.4} > 1%",
        err
    );
}

/// Verify p=0.95 is also within ±1 %.
#[tokio::test]
async fn approx_percentile_p95_100k() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE app95 (id BIGINT)").await;

    for chunk_start in (0i64..100_000).step_by(1000) {
        let vals: Vec<String> = (chunk_start..chunk_start + 1000)
            .map(|i| format!("({i})"))
            .collect();
        let insert = format!("INSERT INTO app95 (id) VALUES {}", vals.join(", "));
        exec(&sess, &insert).await;
    }

    let est = query_f64(&sess, "SELECT APPROX_PERCENTILE(id, 0.95) FROM app95").await;
    // True p95 of 0..100000 is ~95000
    let true_val = 95_000.0_f64;
    let err = (est - true_val).abs() / 100_000.0;
    assert!(
        err <= 0.01,
        "APPROX_PERCENTILE p=0.95: estimate={est:.1} true={true_val} rel_err={:.4} > 1%",
        err
    );
}

/// Empty table should return NULL (Float64 None).
#[tokio::test]
async fn approx_percentile_empty_table() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(&sess, "CREATE TABLE apempty (id BIGINT)").await;

    match sess
        .execute("SELECT APPROX_PERCENTILE(id, 0.5) FROM apempty")
        .await
        .expect("query failed")
    {
        ExecResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total_rows > 0 {
                let col = batches[0].column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64Array");
                // Empty aggregate should return NULL.
                assert!(
                    arr.is_null(0),
                    "empty table should return NULL, got {:?}",
                    arr.value(0)
                );
            }
        }
        ExecResult::Empty { tag } => panic!("expected rows, got tag: {tag}"),
    }
}
