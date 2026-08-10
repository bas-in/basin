//! Integration test: compaction-time adaptive sort (Phase 5.14.D2).
//!
//! Verifies that when a table is created with
//! `WITH (basin.adaptive_sort_override = 'true')`, the compactor re-sorts
//! the output file by the dominant ORDER BY pattern observed via
//! `QueryHistory::top_pattern` — even when no `CLUSTER BY` was declared.
//!
//! Test plan:
//!   1. Build a shard+engine pair with an in-memory catalog.
//!   2. CREATE TABLE with `basin.adaptive_sort_override = 'true'`.
//!   3. Insert a shuffled batch of rows via the engine (shard path).
//!   4. Fire 110 SELECT … ORDER BY score DESC queries so that `score` is
//!      the dominant pattern in QueryHistory (≥ 30 % and ≥ 100 total).
//!   5. Trigger a synchronous compaction via `shard.flush_to_parquet()`.
//!   6. Read the compacted Parquet file back through the shard handle and
//!      assert rows arrive in `score` order.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a shuffled batch of 200 rows with columns `(id, score)`.
/// `id` is monotone-increasing; `score` is deliberately out of order so
/// the pre-compaction layout has no sort guarantee on `score`.
fn shuffled_batch(schema: Arc<Schema>) -> RecordBatch {
    const N: usize = 200;
    let ids: Int64Array = (0..N as i64).collect();
    // Assign score as (i * 7 + 3) mod N — pseudo-random, not sorted by score.
    let scores: Int64Array = (0..N as i64).map(|i| (i * 7 + 3) % N as i64).collect();
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(scores)]).unwrap()
}

/// Collect all `score` values from rows where `id >= id_min`, returned in
/// the order they appear in the batches.  Used to verify that the
/// adaptively-sorted compaction output is monotone on `score`.
fn scores_for_ids_gte(batches: &[RecordBatch], id_min: i64) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must be Int64");
        let score_col = batch
            .column_by_name("score")
            .expect("score column must exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("score must be Int64");
        for i in 0..batch.num_rows() {
            if id_col.value(i) >= id_min {
                out.push(score_col.value(i));
            }
        }
    }
    out
}

fn is_sorted_ascending(values: &[i64]) -> bool {
    values.windows(2).all(|w| w[0] <= w[1])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_adaptive_sort_applies_observed_pattern() {
    basin_common::telemetry::try_init_for_tests();

    // ── 1. Build shard + engine ──────────────────────────────────────────────
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
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

    // Engine::new wires the query_history adapter into the shard automatically.
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });

    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    // ── 2. CREATE TABLE with adaptive_sort_override = true ───────────────────
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE metrics (\
            id    BIGINT NOT NULL,\
            score BIGINT NOT NULL\
         ) WITH (basin.adaptive_sort_override = 'true')",
    )
    .await
    .unwrap();

    // ── 3. Insert a shuffled batch via the shard write path ──────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let partition = PartitionKey::default_key();
    let table = TableName::new("metrics").unwrap();

    let handle = shard.get(&project, &partition).await.unwrap();
    handle
        .write_batch(&table, shuffled_batch(schema))
        .await
        .unwrap();

    // ── 4. Fire 110 ORDER BY score queries to seed QueryHistory ──────────────
    // Each SELECT forces a synchronous compaction (flush-before-read) but the
    // shard tail is already empty after the first flush, so subsequent SELECTs
    // are pure Parquet reads. The important side-effect is that each query
    // records `score` into QueryHistory via the engine's executor.
    for _ in 0..110 {
        sess.execute("SELECT id, score FROM metrics ORDER BY score")
            .await
            .unwrap();
    }

    // ── 5. Re-insert to give the compactor fresh data to sort ────────────────
    // The previous SELECT already flushed the tail; insert a new shuffled
    // batch so the next compaction has something to write.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let ids: Int64Array = (200..400i64).collect();
    let scores: Int64Array = (200..400i64).map(|i| (i * 13 + 7) % 200).collect();
    let new_batch = RecordBatch::try_new(schema2, vec![Arc::new(ids), Arc::new(scores)]).unwrap();
    handle.write_batch(&table, new_batch).await.unwrap();

    // ── 6. Trigger compaction and verify sort order ──────────────────────────
    shard.flush_to_parquet().await.unwrap();

    let batches = handle.read(&table, ReadOptions::default()).await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "expected rows after compaction, got 0");

    // The second batch (ids 200..400) was written after the QueryHistory
    // threshold was met and after `adaptive_sort_override = true` was in
    // effect.  The compactor must have sorted those rows by `score`.
    let fresh_scores = scores_for_ids_gte(&batches, 200);
    assert!(
        !fresh_scores.is_empty(),
        "no rows with id >= 200 found after compaction"
    );
    assert!(
        is_sorted_ascending(&fresh_scores),
        "rows with id >= 200 should be sorted by score after adaptive-sort \
         compaction; got {} rows but they are not in score order.\n\
         First 10 scores: {:?}",
        fresh_scores.len(),
        &fresh_scores[..fresh_scores.len().min(10)]
    );

    // Also confirm the compactions_with_adaptive_sort counter incremented.
    let stats = shard.stats();
    assert!(
        stats.compactions_with_adaptive_sort >= 1,
        "expected compactions_with_adaptive_sort >= 1, got {}",
        stats.compactions_with_adaptive_sort
    );

    wal.close().await.unwrap();
}
