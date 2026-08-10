//! Integration coverage for the PK point-probe fast path **on the shard
//! path** — i.e. when `EngineConfig.shard = Some(_)`.
//!
//! Before this commit the probe was gated by `shard.is_none()` with the
//! comment "a shard read merges its own in-RAM tail and ignores
//! `live_paths`". The reasoning was sound but overly conservative:
//!
//!   * The engine's `MemTableRegistry` probe (`probe_memtable`) already
//!     runs above the PK probe and covers the hot tier (any committed
//!     INSERT lands in the registry via `htap_promote_to_registry`).
//!   * `execute_simple_select` invokes `shard.flush_to_parquet()` before
//!     consulting the catalog, draining the shard's own `state.tail` to
//!     cold-tier Parquet.
//!
//! With both hot caches accounted for, a cold-tier prune is sound. The
//! shard branch now detects a pk-probe-narrowed candidate set and reads
//! those paths directly via `storage.read_paths_with_schema`, bypassing
//! `handle.read` (which would re-list every live data file and defeat
//! the prune).
//!
//! The latency win is decisive: at the 100k-row bench scale Point-query
//! p50 was 4 ms (1366x slower than PG) because the shard path scanned
//! every cold-tier file even for `WHERE id = <lit>`. After this change
//! the cold-tier read touches at most a single file (typically zero).
//!
//! This test seeds the catalog directly with multiple cold-tier files
//! (no shard.tail / memtable rows) so the read path is forced to the PK
//! probe + storage.read_paths_with_schema branch. That isolates the
//! gate-removal change without depending on background flush timing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build an engine with a real shard wired in — the same shape the
/// benchmark harness uses.
async fn build_engine() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

fn two_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn sequential_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let payloads: StringArray = (start..start + len)
        .map(|v| Some(format!("p{v}")))
        .collect();
    RecordBatch::try_new(two_col_schema(), vec![Arc::new(ids), Arc::new(payloads)]).unwrap()
}

/// Write a batch directly to storage with a bloom on `id` and register it
/// in the catalog. Bypasses SQL INSERT so the row never lands in the shard
/// tail or the engine's MemTableRegistry — guaranteeing the read path is
/// the cold-tier PK probe.
async fn seed_cold_file(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    expected_snapshot: SnapshotId,
) -> SnapshotId {
    let opts = WriteOptions {
        bloom_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let part = PartitionKey::default_key();
    let df = engine
        .config()
        .storage
        .write_batch_with_options(project, table, &part, &batch, &opts)
        .await
        .unwrap();
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats,
        bloom_filters: df.bloom_filters,
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };
    let updated = engine
        .config()
        .catalog
        .append_data_files(project, table, expected_snapshot, vec![file_ref])
        .await
        .unwrap();
    updated.current_snapshot
}

/// With shard enabled and three cold-tier files, an out-of-range PK
/// lookup must prune all files via zone-map / bloom (no file body read).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_lookup_on_shard_prunes_all_files_for_absent_key() {
    let (_sd, _wd, engine, bg, wal) = build_engine().await;
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();

    // Seed three disjoint cold-tier files: [0,1000), [2000,3000), [4000,5000).
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(0, 1000), snap).await;
    snap = seed_cold_file(
        &engine,
        &project,
        &table,
        sequential_batch(2000, 1000),
        snap,
    )
    .await;
    let _ = seed_cold_file(
        &engine,
        &project,
        &table,
        sequential_batch(4000, 1000),
        snap,
    )
    .await;

    let before = engine.blooms_skipped_count();

    // Out-of-range key: every file pruned by zone-map (id=999999 > max).
    let res = sess
        .execute("SELECT id FROM events WHERE id = 999999")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "out-of-range PK lookup must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after >= before + 3,
        "PK probe on shard path must prune all 3 cold-tier files for the \
         out-of-range key (before={before}, after={after}). If the counter \
         did not advance the shard.is_none() gate has been reintroduced."
    );

    // In-range existing key (1500 lands in file 2 — [2000,3000)). The
    // probe narrows to one file; the read must return exactly that row.
    let res = sess
        .execute("SELECT id, payload FROM events WHERE id = 2500")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "existing PK lookup must return exactly one row");

    bg.shutdown().await;
    wal.close().await.unwrap();
}
