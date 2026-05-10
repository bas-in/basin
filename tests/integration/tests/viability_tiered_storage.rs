//! Viability test: per-table tiered storage (Phase 5.5).
//!
//! Card: `viability_tiered_storage`
//!
//! Two bars:
//!   - `cold_files_after_sweep == 1`
//!   - `total_rows_visible == 20`
//!
//! What we test:
//! 1. Two Parquet files land for the same `(tenant, table)`: one with rows
//!    timestamped 100 days ago, one with rows timestamped 10 days ago. Both
//!    initially live under `tables/<t>/data/...`.
//! 2. The catalog gets a tier policy with `cold_after_seconds = 30 days`
//!    and `cold_age_column = "ts"`.
//! 3. `Shard::run_tiering_sweep` walks the table, identifies the 100-day-old
//!    file as cold-eligible by its `ts` max, copies it to
//!    `tables/<t>/cold/...`, atomic-replaces the catalog manifest, and
//!    deletes the original.
//! 4. After the sweep:
//!    * Exactly one file lives under `cold/` and one under `data/`.
//!    * `Storage::read` (which DataFusion uses transparently) returns all
//!      20 rows, regardless of tier.
//!
//! We bypass SQL on purpose. The tiering machinery sits below the engine and
//! the goal of this card is to prove the storage + catalog + compactor
//! contract. Engine-driven SELECT is exercised by the lower-stakes assertion
//! at the bottom of the test (via the storage read path the engine itself
//! invokes).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use chrono::Utc;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const SECS_PER_DAY: i64 = 86_400;

/// Build a `RecordBatch` of `n` rows whose `ts` column is `days_ago` days
/// before now. We use Arrow's `TimestampMicrosecondArray` so the Parquet
/// writer emits Int64-microsecond statistics, which the tiering compactor
/// decodes as i64-LE in `column_stats.max_bytes`.
fn build_batch(start_id: i64, n: usize, days_ago: i64) -> RecordBatch {
    let now_micros = Utc::now().timestamp_micros();
    let offset_micros = days_ago * SECS_PER_DAY * 1_000_000;
    let base = now_micros - offset_micros;

    let ids: Int64Array = (start_id..start_id + n as i64).collect();
    // Spread the rows over a 60-second window so min != max. We only ever
    // consult max in the tiering decision; the spread is just defensive
    // against a Parquet writer that elides min==max stats.
    let ts: TimestampMicrosecondArray = (0..n as i64)
        .map(|i| Some(base + i * 1_000_000))
        .collect::<Vec<_>>()
        .into_iter()
        .collect();
    let payloads: Vec<String> = (0..n).map(|i| format!("p-{start_id}-{i}")).collect();
    let payload: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            // The TimestampMicrosecondArray needs a tz attached to match
            // the schema; rebuild with the explicit tz.
            Arc::new(ts.with_timezone("UTC".to_string())),
            Arc::new(payload),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_tiered_storage_compactor_moves_old_files() {
    basin_common::telemetry::try_init_for_tests();

    // ----------------------------------------------------------------------
    // Storage + catalog setup. LocalFS-backed; no network. The shard owner
    // gets a real WAL too because that's what `Shard::new` expects, but we
    // never actually write through the WAL in this test — we hand files to
    // the storage layer and the catalog directly so the sweep has a known
    // file inventory to act on.
    // ----------------------------------------------------------------------
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(storage_fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // The shard's tier sweep needs a resident partition for this tenant so
    // it knows the tenant exists. We get that for free by opening a handle.
    let _handle = shard.get(&tenant, &part).await.unwrap();

    // ----------------------------------------------------------------------
    // Seed the catalog: create the table, then write two distinct Parquet
    // files (one per batch) and commit both via append_data_files. The first
    // batch is 100 days old, the second is 10 days old.
    // ----------------------------------------------------------------------
    let old_batch = build_batch(0, 10, 100);
    let new_batch = build_batch(1000, 10, 10);
    catalog
        .create_table(&tenant, &table, old_batch.schema().as_ref())
        .await
        .unwrap();

    let old_file = storage
        .write_batch(&tenant, &table, &part, &old_batch)
        .await
        .unwrap();
    let new_file = storage
        .write_batch(&tenant, &table, &part, &new_batch)
        .await
        .unwrap();

    // Both initial files live under data/.
    assert!(
        old_file.path.as_ref().contains("/data/"),
        "old file should land under hot tier: {}",
        old_file.path
    );
    assert!(
        new_file.path.as_ref().contains("/data/"),
        "new file should land under hot tier: {}",
        new_file.path
    );

    // Commit each file as its own snapshot so replace_data_files in the
    // sweep has a single file to swap per call (the tiering compactor walks
    // files one at a time and refreshes `expected_snapshot` per migration).
    let mut current_snap = basin_catalog::SnapshotId::GENESIS;
    let after_old = catalog
        .append_data_files(
            &tenant,
            &table,
            current_snap,
            vec![basin_catalog::DataFileRef {
                path: old_file.path.as_ref().to_string(),
                size_bytes: old_file.size_bytes,
                row_count: old_file.row_count,
                column_stats: old_file.column_stats.clone(),
            }],
        )
        .await
        .unwrap();
    current_snap = after_old.current_snapshot;
    let _after_new = catalog
        .append_data_files(
            &tenant,
            &table,
            current_snap,
            vec![basin_catalog::DataFileRef {
                path: new_file.path.as_ref().to_string(),
                size_bytes: new_file.size_bytes,
                row_count: new_file.row_count,
                column_stats: new_file.column_stats.clone(),
            }],
        )
        .await
        .unwrap();

    // ----------------------------------------------------------------------
    // Configure the per-table tier policy: 30 days cold-after, age column
    // = "ts". The 100-day-old file's `ts` max is well below the cutoff, the
    // 10-day-old file's max is well above it, so exactly one migration is
    // expected.
    // ----------------------------------------------------------------------
    catalog
        .set_tier_policy(
            &tenant,
            &table,
            Some(30 * SECS_PER_DAY as u64),
            Some("ts".to_string()),
        )
        .await
        .unwrap();

    // Pre-sweep sanity: list_data_files reports both as Hot.
    let pre = storage.list_data_files(&tenant, &table).await.unwrap();
    assert_eq!(pre.len(), 2, "expected two hot files pre-sweep, got {}", pre.len());
    for f in &pre {
        assert_eq!(
            f.tier,
            basin_storage::Tier::Hot,
            "pre-sweep file in unexpected tier: {} ({:?})",
            f.path,
            f.tier,
        );
    }

    // ----------------------------------------------------------------------
    // Run the sweep. Synchronous one-shot.
    // ----------------------------------------------------------------------
    shard.run_tiering_sweep().await.unwrap();

    // ----------------------------------------------------------------------
    // Assertions.
    // ----------------------------------------------------------------------
    let post = storage.list_data_files(&tenant, &table).await.unwrap();
    let cold_files: Vec<_> = post
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Cold))
        .collect();
    let hot_files: Vec<_> = post
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Hot))
        .collect();

    println!(
        "tiered_storage post-sweep: {} hot, {} cold, {} total",
        hot_files.len(),
        cold_files.len(),
        post.len()
    );
    for f in &post {
        println!("  {:?} {}", f.tier, f.path);
    }

    assert_eq!(
        cold_files.len(),
        1,
        "expected exactly one cold file after sweep, got {}: {:?}",
        cold_files.len(),
        post.iter().map(|f| f.path.as_ref()).collect::<Vec<_>>()
    );
    assert!(
        cold_files[0].path.as_ref().contains("/cold/"),
        "cold file path should contain /cold/: {}",
        cold_files[0].path
    );
    assert_eq!(
        hot_files.len(),
        1,
        "expected exactly one hot file after sweep, got {}",
        hot_files.len()
    );
    assert!(
        hot_files[0].path.as_ref().contains("/data/"),
        "hot file path should still contain /data/: {}",
        hot_files[0].path
    );

    // The original hot path of the migrated file must be physically gone.
    let still_alive: Vec<&object_store::path::Path> = post
        .iter()
        .map(|f| &f.path)
        .filter(|p| p.as_ref() == old_file.path.as_ref())
        .collect();
    assert!(
        still_alive.is_empty(),
        "migrated hot file should have been deleted: {:?}",
        still_alive
    );

    // SELECT * across tiers — Storage::read transparently includes both.
    let stream = storage
        .read(&tenant, &table, ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 20,
        "all rows must remain visible after tier migration; got {total_rows}"
    );

    // Idempotency: running the sweep again is a no-op (the cold file gets
    // skipped, the still-hot file is below the threshold).
    shard.run_tiering_sweep().await.unwrap();
    let post2 = storage.list_data_files(&tenant, &table).await.unwrap();
    assert_eq!(
        post2.len(),
        2,
        "second sweep must not duplicate files; got {}",
        post2.len()
    );
    let cold2: usize = post2
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Cold))
        .count();
    assert_eq!(cold2, 1, "second sweep must not move more files; got {cold2} cold");

    // ----------------------------------------------------------------------
    // Dashboard: the card has two qualitative bars; the JSON primary uses
    // `cold_files_after_sweep` (we also report total rows in details).
    // ----------------------------------------------------------------------
    let cold_after_sweep = cold_files.len() as f64;
    let pass = (cold_after_sweep - 1.0).abs() < f64::EPSILON && total_rows == 20;
    report_viability(
        "tiered_storage",
        "Per-table tiered storage (hot/cold)",
        "Files older than the configured threshold migrate from the default 'hot' object-store prefix to a cheaper 'cold' prefix. Reads transparently follow the catalog regardless of tier.",
        pass,
        PrimaryMetric {
            label: "cold_files_after_sweep".into(),
            value: cold_after_sweep,
            unit: "files".into(),
            bar: BarOp::eq(1.0),
        },
        json!({
            "total_rows_visible": total_rows,
            "hot_files_after_sweep": hot_files.len(),
            "cold_files_after_sweep": cold_files.len(),
            "cold_after_seconds": 30 * SECS_PER_DAY,
            "cold_age_column": "ts",
        }),
    );

    assert!(pass, "card primary failed: cold={cold_after_sweep} rows={total_rows}");
}
