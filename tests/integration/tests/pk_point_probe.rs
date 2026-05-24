//! Integration coverage for the PK point-probe fast path
//! (`fast_select::execute_simple_select` + `index_probe::pk_point_probe`).
//!
//! A `SELECT * FROM t WHERE pk = <lit>` on a single-column primary key is the
//! canonical OLTP point lookup. The fast path uses the per-file zone-map
//! (min/max) and catalog bloom filter to prune files that cannot contain the
//! key — a definitive miss returns zero rows without opening any file body.
//!
//! These tests assert correctness across the full overlay stack:
//!   1. existing key returns its row,
//!   2. absent key returns zero rows (and, when a bloom is present, proves the
//!      bloom short-circuit fired via the engine's bloom-skip counter),
//!   3. a freshly INSERTed key (memtable overlay) is visible,
//!   4. a DELETE-fastpath tombstone suppresses the cold-tier row,
//!   5. a key on a table spread across many files/row-groups returns exactly
//!      one row.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_engine(dir: &TempDir) -> Engine {
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

/// Execute `sql` and return the result batches, panicking on error or a
/// non-rows result.
async fn query_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected rows for {sql:?}, got {other:?}"),
        Err(e) => panic!("query {sql:?} failed: {e}"),
    }
}

/// Total live rows across all batches.
fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Pull the `id` (Int64) value out of the first row of the first batch.
fn first_id(batches: &[RecordBatch]) -> i64 {
    let b = batches.first().expect("at least one batch");
    let idx = b.schema().index_of("id").expect("id column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64")
        .value(0)
}

fn two_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Build a batch with `id` values `[start, start+len)` and a derived payload.
fn sequential_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let payloads: StringArray = (start..start + len).map(|v| Some(format!("p{v}"))).collect();
    RecordBatch::try_new(two_col_schema(), vec![Arc::new(ids), Arc::new(payloads)]).unwrap()
}

/// Write `batch` to storage with a bloom on `id` and append the resulting
/// data-file to the catalog, returning the new snapshot id. Bypasses SQL
/// INSERT so we can seed many separate files cheaply.
async fn write_and_commit(
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

// ---------------------------------------------------------------------------
// 1. Existing key returns the row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn point_lookup_existing_key_returns_row() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(42,'answer')")
        .await
        .unwrap();

    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 42").await;
    assert_eq!(row_count(&rows), 1, "exactly one row for id=42");
    assert_eq!(first_id(&rows), 42);
}

// ---------------------------------------------------------------------------
// 2. Absent key returns zero rows (and proves bloom short-circuit)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn point_lookup_absent_key_returns_empty_and_skips_files() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();

    // Seed three files via the catalog, each spanning a disjoint id range so
    // a probe for an out-of-range key prunes all of them by zone-map, and an
    // in-range-but-absent key is pruned by the per-file bloom on `id`.
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    // f0: ids [0,100); f1: [200,300); f2: [400,500). Gaps at [100,200) etc.
    snap = write_and_commit(&engine, &project, &table, sequential_batch(0, 100), snap).await;
    snap = write_and_commit(&engine, &project, &table, sequential_batch(200, 100), snap).await;
    let _ = write_and_commit(&engine, &project, &table, sequential_batch(400, 100), snap).await;

    let before = engine.blooms_skipped_count();

    // Out-of-range key: every file pruned by zone-map.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 999999").await;
    assert_eq!(row_count(&rows), 0, "out-of-range key must return zero rows");

    // In-range-but-absent key (150 falls in the [100,200) gap, inside the
    // overall min/max so zone-map alone cannot prune f0/f1, but the bloom on
    // `id` proves it absent). Must still return zero rows.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 150").await;
    assert_eq!(row_count(&rows), 0, "in-range absent key must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after > before,
        "bloom/zone-map short-circuit must have skipped at least one file \
         (before={before}, after={after})"
    );
}

// ---------------------------------------------------------------------------
// 3. Freshly INSERTed key (memtable overlay) is visible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn point_lookup_after_insert_sees_memtable_row() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1,'a'),(2,'b')")
        .await
        .unwrap();

    // Insert a brand-new key; it should be visible to the point lookup whether
    // it is served from the memtable overlay or the cold tier.
    sess.execute("INSERT INTO t VALUES (7,'lucky')")
        .await
        .unwrap();

    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 7").await;
    assert_eq!(row_count(&rows), 1, "newly inserted key must be visible");
    assert_eq!(first_id(&rows), 7);
}

// ---------------------------------------------------------------------------
// 4. DELETE-fastpath tombstone suppresses the cold-tier row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn point_lookup_after_delete_returns_empty() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (10,'x'),(20,'y'),(30,'z')")
        .await
        .unwrap();

    // Confirm the row exists first.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 20").await;
    assert_eq!(row_count(&rows), 1, "id=20 should exist before delete");

    // Delete it, then probe again: the tombstone overlay must suppress it.
    sess.execute("DELETE FROM t WHERE id = 20").await.unwrap();
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 20").await;
    assert_eq!(row_count(&rows), 0, "deleted key must return zero rows");

    // A sibling key must still be visible (delete did not over-suppress).
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 10").await;
    assert_eq!(row_count(&rows), 1, "non-deleted sibling must still exist");
    assert_eq!(first_id(&rows), 10);
}

// ---------------------------------------------------------------------------
// 5. Key on a table spread across many files returns exactly one row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn point_lookup_many_files_returns_single_row() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')")
        .await
        .unwrap();

    // Seed 10 disjoint files of 1_000 rows each → 10k rows across 10 files.
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    for f in 0..10i64 {
        let start = f * 1_000;
        snap = write_and_commit(&engine, &project, &table, sequential_batch(start, 1_000), snap)
            .await;
    }

    // A key in the middle of file 7 must resolve to exactly one row.
    let target = 7_321i64;
    let rows = query_rows(&sess, &format!("SELECT * FROM t WHERE id = {target}")).await;
    assert_eq!(
        row_count(&rows),
        1,
        "exactly one row for id={target} across 10 files"
    );
    assert_eq!(first_id(&rows), target);

    // A key past the last file must return zero rows.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id = 10000").await;
    assert_eq!(row_count(&rows), 0, "id beyond all files returns zero rows");
}
