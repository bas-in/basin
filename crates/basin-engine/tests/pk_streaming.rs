//! Correctness gate for the bounded-memory streaming PRIMARY KEY check
//! (`enforce_pk_streaming`). Forces the streaming path for EVERY batch via
//! `BASIN_PK_STREAMING_MIN_ROWS=0` and asserts it is byte-equivalent to the
//! in-RAM path: clean unique loads pass, cross-file duplicates are rejected,
//! intra-batch duplicates are rejected, and a fresh key is never a false
//! positive. Single test in its own binary so the process-global env override
//! cannot race other tests.

use std::sync::Arc;

use arrow_schema::Schema;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
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
    Engine::new(EngineConfig { storage, catalog, shard: None })
}

fn row(id: i64, x: i64) -> Vec<Option<String>> {
    vec![Some(id.to_string()), Some(x.to_string())]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_streaming_is_correct() {
    // Force the streaming path for every existing-table check, even at tiny
    // row counts.
    std::env::set_var("BASIN_PK_STREAMING_MIN_ROWS", "0");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let schema = Arc::new(Schema::empty());
    let cols = vec!["id".to_string(), "x".to_string()];

    sess.execute("DROP TABLE IF EXISTS t").await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x INT)").await.unwrap();

    // Seed ids 1..=30_000 across 3 separate chunks (each becomes its own data
    // file). Every chunk after the first runs the streaming check against the
    // prior files — all keys unique, so no false positive.
    for c in 0..3u64 {
        let batch: Vec<Vec<Option<String>>> = (c * 10_000..(c + 1) * 10_000)
            .map(|i| row(i as i64 + 1, (i % 7) as i64))
            .collect();
        let n = sess
            .ingest_csv_batch("t", schema.clone(), Some(&cols), batch)
            .await
            .expect("clean unique load must pass under the streaming path");
        assert_eq!(n, 10_000, "ingested count");
    }

    // Cross-file duplicate: id=5 lives in the first file. Must be rejected.
    let dup = vec![row(5, 0)];
    let err = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), dup)
        .await;
    assert!(
        err.is_err(),
        "streaming path must reject a cross-file duplicate PK (id=5)"
    );

    // Intra-batch duplicate: two rows with the same id in one batch.
    let intra = vec![row(900_001, 0), row(900_001, 1)];
    let err2 = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), intra)
        .await;
    assert!(
        err2.is_err(),
        "must reject an intra-batch duplicate PK (id=900001 twice)"
    );

    // A fresh unique id must still succeed — no false positive from pruning.
    let ok = vec![row(500_001, 9)];
    let n = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), ok)
        .await
        .expect("a fresh unique id must pass");
    assert_eq!(n, 1);

    // Final row count: 30_000 seeded + 1 fresh = 30_001.
    let cnt = match sess.execute("SELECT count(*) FROM t").await.unwrap() {
        basin_engine::ExecResult::Rows { batches, .. } => {
            use arrow_array::Array;
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(-1)
        }
        _ => -1,
    };
    assert_eq!(cnt, 30_001, "final row count after rejected dups + one insert");
}
