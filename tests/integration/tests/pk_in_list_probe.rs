//! Integration coverage for the PK IN-list probe fast path:
//! `WHERE pk IN (v1, v2, …, vN)` and the equivalent
//! `WHERE pk = ANY('{v1,…}'::int[])` shape (lowered to IN-list by pg_ast).
//!
//! The same bloom+zone-map file-prune that fires for `WHERE pk = <lit>` now
//! also fires for IN-list shapes.  For each live file the probe checks whether
//! ANY of the queried values could be present (zone-map OR of Eq atoms plus
//! bloom OR across all values).  Files where no value can possibly be present
//! are pruned; the result set is read only from the surviving candidate files.
//!
//! Test structure:
//!   1. IN-list returns correct rows (span two files, some absent values).
//!   2. IN-list with all absent values prunes all files → zero rows + bloom
//!      skip counter advances (validates the probe fired).
//!   3. IN-list with values spanning multiple files returns all matching rows.
//!   4. `= ANY('{…}'::int[])` shape (pg_ast lowered) behaves identically to
//!      the equivalent IN-list.
//!   5. Shard path: engine wired with `shard = Some(…)`, same correctness.

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

// ── Helpers (no-shard engine) ─────────────────────────────────────────────────

fn build_engine_noshard(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

fn two_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn sequential_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let payloads: StringArray = (start..start + len).map(|v| Some(format!("p{v}"))).collect();
    RecordBatch::try_new(two_col_schema(), vec![Arc::new(ids), Arc::new(payloads)]).unwrap()
}

/// Write a batch with a bloom on `id` and register it in the catalog.
/// Bypasses SQL INSERT so no row lands in the shard tail or memtable —
/// the read is forced onto the cold-tier PK probe path.
async fn seed_cold_file(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    snap: SnapshotId,
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
    engine
        .config()
        .catalog
        .append_data_files(project, table, snap, vec![file_ref])
        .await
        .unwrap()
        .current_snapshot
}

async fn query_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected Rows for {sql:?}, got {other:?}"),
        Err(e) => panic!("query {sql:?} failed: {e}"),
    }
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn ids_sorted(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("id").unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

/// Build a multi-file table and return (engine, session, project, table).
async fn setup_multifile_table(
    dir: &TempDir,
) -> (Engine, basin_engine::ProjectSession, ProjectId, TableName) {
    let engine = build_engine_noshard(dir);
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();

    // Three disjoint cold-tier files:
    //   f0: ids [0,  100)
    //   f1: ids [200, 300)
    //   f2: ids [400, 500)
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(0, 100), snap).await;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(200, 100), snap).await;
    let _ = seed_cold_file(&engine, &project, &table, sequential_batch(400, 100), snap).await;

    (engine, sess, project, table)
}

// ── Test 1: IN-list spanning two files returns correct rows ───────────────────

#[tokio::test]
async fn in_list_spanning_two_files_returns_correct_rows() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // Values 5 and 250 live in f0 and f1 respectively; 150 is absent.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (5, 150, 250)").await;
    let ids = ids_sorted(&rows);
    assert_eq!(ids, vec![5, 250], "only existing ids must be returned");
}

// ── Test 2: All-absent IN-list prunes all files → zero rows + bloom counter ──

#[tokio::test]
async fn in_list_all_absent_prunes_all_files() {
    let dir = TempDir::new().unwrap();
    let (engine, sess, _, _) = setup_multifile_table(&dir).await;

    let before = engine.blooms_skipped_count();

    // 150, 350, 999 all fall in the gaps between the three files.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (150, 350, 999)").await;
    assert_eq!(row_count(&rows), 0, "all-absent IN-list must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after > before,
        "bloom/zone-map probe must have skipped at least one file \
         for all-absent IN-list (before={before}, after={after})"
    );
}

// ── Test 3: IN-list spanning all three files returns all matching rows ────────

#[tokio::test]
async fn in_list_spanning_all_files_returns_all_matching_rows() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // One value per file; absent value 150 must not appear.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (10, 150, 250, 450)").await;
    let ids = ids_sorted(&rows);
    assert_eq!(ids, vec![10, 250, 450], "three matching rows across three files");
}

// ── Test 4: `= ANY('{…}'::int[])` shape behaves identically to IN ────────────

#[tokio::test]
async fn any_array_shape_behaves_identically_to_in_list() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // pg_ast lowers `= ANY('{5,250}'::int[])` to `IN (5, 250)` before parse.
    let rows = query_rows(
        &sess,
        "SELECT * FROM t WHERE id = ANY('{5, 150, 250}'::int[])",
    )
    .await;
    let ids = ids_sorted(&rows);
    assert_eq!(
        ids,
        vec![5, 250],
        "= ANY('{{...}}'::int[]) must return the same rows as the equivalent IN-list"
    );
}

// ── Test 5: IN-list out-of-range prunes everything ────────────────────────────

#[tokio::test]
async fn in_list_out_of_range_prunes_all_files_and_returns_empty() {
    let dir = TempDir::new().unwrap();
    let (engine, sess, _, _) = setup_multifile_table(&dir).await;

    let before = engine.blooms_skipped_count();

    // All values are above the max stored id (499) — zone-map proves absent.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (1000, 2000, 9999)").await;
    assert_eq!(row_count(&rows), 0, "out-of-range IN-list must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after >= before + 3,
        "zone-map probe must prune all 3 files for out-of-range IN-list \
         (before={before}, after={after})"
    );
}

// ── Shard path: same correctness and bloom-skip guarantee ────────────────────

async fn build_shard_engine() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(
            LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
        ),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap(),
            ),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_list_on_shard_path_prunes_files_and_returns_correct_rows() {
    let (_sd, _wd, engine, bg, wal) = build_shard_engine().await;
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, payload TEXT) \
         WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();

    // Seed three disjoint cold-tier files (bypass shard tail).
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

    // All-absent IN-list: every file must be pruned by zone-map.
    let res = sess
        .execute("SELECT id FROM events WHERE id IN (1500, 3500, 9999)")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "all-absent IN-list on shard must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after >= before + 3,
        "PK IN-list probe on shard path must prune all 3 cold-tier files for \
         the all-absent IN-list (before={before}, after={after})"
    );

    // Spanning IN-list: values in f0 and f2.
    let res = sess
        .execute("SELECT id FROM events WHERE id IN (100, 1500, 4500)")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..arr.len()).map(move |i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![100, 4500],
        "IN-list spanning two files must return exactly the matching rows"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
