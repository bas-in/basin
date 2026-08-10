//! Integration tests for the `basin_project_usage` SQL view.
//!
//! The view is a per-project live read of [`basin_common::ProjectCounterRegistry`].
//! These tests assert:
//!
//! 1. A session in project A sees exactly one row — its own.
//! 2. After a write workload, `bytes_written_total` > 0.
//! 3. A freshly opened project B's session sees its own row, NOT project A's
//!    counters (cross-project isolation — the safety guarantee any cloud-side
//!    billing aggregator relies on).
//!
//! Mirrors the per-project-scoping convention of `pg_stat_activity` /
//! `pg_locks` (see `crates/basin-engine/src/info_schema_provider.rs`).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array as _;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn collect(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(ExecResult::Empty { tag }) => {
            panic!("expected Rows for {sql:?}; got Empty({tag})")
        }
        Err(e) => panic!("execute failed for {sql:?}: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basin_project_usage_shows_own_project_after_writes() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project_a = ProjectId::new();
    let sess_a = engine.open_session(project_a).await.unwrap();

    sess_a
        .execute("CREATE TABLE usage_smoke (id BIGINT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    sess_a
        .execute("INSERT INTO usage_smoke VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("INSERT");
    let _ = collect(&sess_a, "SELECT * FROM usage_smoke").await;

    let batches = collect(
        &sess_a,
        "SELECT project_id, bytes_read_total, bytes_written_total, \
         class_a_ops_total, class_b_ops_total, cpu_seconds_total \
         FROM basin_project_usage",
    )
    .await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "basin_project_usage must return exactly one row"
    );

    let b = &batches[0];
    let pid = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(pid.value(0), project_a.to_string());

    let bw = b
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert!(
        bw.value(0) > 0,
        "bytes_written_total must be > 0 after an INSERT; got {}",
        bw.value(0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basin_project_usage_cross_project_isolation() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    let sess_a = engine.open_session(project_a).await.unwrap();
    sess_a
        .execute("CREATE TABLE noisy (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE noisy");
    let big_blob = "x".repeat(1024);
    let mut values = String::new();
    for i in 0..200 {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("({i}, '{big_blob}')"));
    }
    sess_a
        .execute(&format!("INSERT INTO noisy VALUES {values}"))
        .await
        .expect("INSERT noisy");

    let a_batches = collect(
        &sess_a,
        "SELECT project_id, bytes_written_total FROM basin_project_usage",
    )
    .await;
    assert_eq!(a_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let a_pid = a_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    let a_bw = a_batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(a_pid, project_a.to_string());
    assert!(a_bw > 0, "A's bytes_written must be > 0; got {a_bw}");

    let sess_b = engine.open_session(project_b).await.unwrap();
    let b_batches = collect(
        &sess_b,
        "SELECT project_id, bytes_written_total FROM basin_project_usage",
    )
    .await;
    assert_eq!(b_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let row = &b_batches[0];
    let b_pid = row
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    let b_bw = row
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(b_pid, project_b.to_string(), "B must see B's id, not A's");
    assert_ne!(b_pid, a_pid, "B must NOT see A's project_id");
    assert!(
        b_bw < a_bw,
        "B's bytes_written ({b_bw}) must be strictly less than A's ({a_bw}); \
         otherwise A's counters have leaked into B's session"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basin_project_usage_schema_is_stable() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let batches = collect(&sess, "SELECT * FROM basin_project_usage").await;
    assert!(!batches.is_empty());
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for required in &[
        "project_id",
        "bytes_read_total",
        "bytes_written_total",
        "class_a_ops_total",
        "class_b_ops_total",
        "cpu_seconds_total",
        "snapshot_at",
    ] {
        assert!(
            names.contains(required),
            "basin_project_usage must expose column {required:?}; got {names:?}"
        );
    }
}
