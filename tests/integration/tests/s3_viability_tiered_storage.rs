//! S3 port of `viability_tiered_storage.rs`.
//!
//! Same shape: write a 100-day-old and a 10-day-old Parquet file, configure a
//! 30-day cold-after policy on the table, run `Shard::run_tiering_sweep`, and
//! assert exactly one file ends up under `cold/`. Storage is real S3; the WAL
//! stays LocalFS (the test never writes through it — it hands files to the
//! storage layer + catalog directly).
//!
//! Card id: `tiered_storage`. Bar: `cold_files_after_sweep == 1`.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, ProjectId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use chrono::Utc;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tempfile::TempDir;

const TEST_NAME: &str = "s3_viability_tiered_storage";
const SECS_PER_DAY: i64 = 86_400;

fn build_batch(start_id: i64, n: usize, days_ago: i64) -> RecordBatch {
    let now_micros = Utc::now().timestamp_micros();
    let offset_micros = days_ago * SECS_PER_DAY * 1_000_000;
    let base = now_micros - offset_micros;

    let ids: Int64Array = (start_id..start_id + n as i64).collect();
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
            Arc::new(ts.with_timezone("UTC".to_string())),
            Arc::new(payload),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_tiered_storage() {
    basin_common::telemetry::try_init_for_tests();

    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    // The shard's tier sweep needs a WAL handle. The test bypasses it for
    // file writes (those go straight through `storage`), so a LocalFS-backed
    // WAL in a tempdir is fine and avoids spinning a second S3 connection.
    let wal_dir = TempDir::new().unwrap();
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

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    let _handle = shard.get(&project, &part).await.unwrap();

    let old_batch = build_batch(0, 10, 100);
    let new_batch = build_batch(1000, 10, 10);
    catalog
        .create_table(&project, &table, old_batch.schema().as_ref())
        .await
        .unwrap();

    let old_file = storage
        .write_batch(&project, &table, &part, &old_batch)
        .await
        .unwrap();
    let new_file = storage
        .write_batch(&project, &table, &part, &new_batch)
        .await
        .unwrap();

    assert!(old_file.path.as_ref().contains("/data/"));
    assert!(new_file.path.as_ref().contains("/data/"));

    let mut current_snap = basin_catalog::SnapshotId::GENESIS;
    let after_old = catalog
        .append_data_files(
            &project,
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
            &project,
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

    catalog
        .set_tier_policy(
            &project,
            &table,
            Some(30 * SECS_PER_DAY as u64),
            Some("ts".to_string()),
        )
        .await
        .unwrap();

    let pre = storage.list_data_files(&project, &table).await.unwrap();
    assert_eq!(pre.len(), 2);
    for f in &pre {
        assert_eq!(f.tier, basin_storage::Tier::Hot);
    }

    shard.run_tiering_sweep().await.unwrap();

    let post = storage.list_data_files(&project, &table).await.unwrap();
    let cold_files: Vec<_> = post
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Cold))
        .collect();
    let hot_files: Vec<_> = post
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Hot))
        .collect();

    println!(
        "[S3 viability_tiered_storage] post-sweep: hot={} cold={} total={}",
        hot_files.len(),
        cold_files.len(),
        post.len()
    );
    for f in &post {
        println!("  {:?} {}", f.tier, f.path);
    }

    assert_eq!(cold_files.len(), 1, "expected exactly one cold file");
    assert!(cold_files[0].path.as_ref().contains("/cold/"));
    assert_eq!(hot_files.len(), 1);
    assert!(hot_files[0].path.as_ref().contains("/data/"));

    let still_alive: Vec<_> = post
        .iter()
        .map(|f| &f.path)
        .filter(|p| p.as_ref() == old_file.path.as_ref())
        .collect();
    assert!(still_alive.is_empty(), "migrated hot file should be gone");

    let stream = storage
        .read(&project, &table, ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20);

    // Idempotency
    shard.run_tiering_sweep().await.unwrap();
    let post2 = storage.list_data_files(&project, &table).await.unwrap();
    assert_eq!(post2.len(), 2);
    let cold2: usize = post2
        .iter()
        .filter(|f| matches!(f.tier, basin_storage::Tier::Cold))
        .count();
    assert_eq!(cold2, 1);

    let cold_after_sweep = cold_files.len() as f64;
    let pass = (cold_after_sweep - 1.0).abs() < f64::EPSILON && total_rows == 20;
    report_real_viability(
        "tiered_storage",
        "Per-table tiered storage (hot/cold) on real S3",
        "Files older than the configured threshold migrate from the default 'hot' object-store prefix to a cheaper 'cold' prefix on real S3. Reads transparently follow the catalog regardless of tier.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "card primary failed: cold={cold_after_sweep} rows={total_rows}"
    );
}
