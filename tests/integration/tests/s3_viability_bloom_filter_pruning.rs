//! S3 port of `viability_bloom_filter_pruning.rs`.
//!
//! Card: `viability_bloom_filter_pruning` (real-cloud dashboard).
//! Bar: ≥80% of row groups pruned by bloom filter alone on the in-range
//! absent-id query — same as LocalFS. The point of running this on real
//! S3 is to verify the reader actually issues the bloom-filter footer
//! fetch (typically a separate range GET), evaluates the absence proof,
//! and skips the row-group fetch — over real network. If something in
//! the parquet reader's seek path defeated bloom pruning on remote
//! object stores, this card would catch it.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig, WriteOptions};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_bloom_filter_pruning";
const ROWS: i64 = 1_000;
const ROW_GROUP_SIZE: usize = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_shuffled_batch() -> RecordBatch {
    let mut ids: Vec<i64> = (0..ROWS).collect();
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }

    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = (0..ROWS).map(|i| format!("p-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_bloom_filter_pruning() {
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

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    let catalog = InMemoryCatalog::new();
    catalog
        .create_table(&project, &table, &schema())
        .await
        .unwrap();
    catalog
        .set_bloom_filter_columns(&project, &table, vec!["id".to_string()])
        .await
        .unwrap();
    let meta = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(meta.bloom_filter_columns, vec!["id".to_string()]);

    let batch = build_shuffled_batch();
    let opts = WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        cluster_columns: vec![],
        max_row_group_size: Some(ROW_GROUP_SIZE),
        ..Default::default()
    };
    let df = storage
        .write_batch_with_options(&project, &table, &part, &batch, &opts)
        .await
        .unwrap();
    let total_groups_in_file = ROWS as usize / ROW_GROUP_SIZE;
    println!(
        "[S3 viability_bloom] wrote {} rows, {} bytes, {} row groups (target)",
        df.row_count, df.size_bytes, total_groups_in_file
    );

    let listed = storage
        .list_data_files_with_stats(&project, &table)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "exactly one parquet file expected");

    // Existing-id query.
    let counters = storage.read_counters().clone();
    counters.reset();
    let existing_target: i64 = 42;
    let opts_eq = ReadOptions {
        filters: vec![Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(existing_target),
        )],
        ..Default::default()
    };
    let mut stream = storage.read(&project, &table, opts_eq).await.unwrap();
    let mut hit_rows = 0usize;
    while let Some(b) = stream.next().await {
        hit_rows += b.unwrap().num_rows();
    }
    let existing_snap = counters.snapshot();
    assert_eq!(
        hit_rows, 1,
        "expected exactly one row for id={existing_target}",
    );

    // Out-of-range absent (stats catches this; sanity).
    counters.reset();
    let absent_target: i64 = ROWS + 12_345;
    let opts_eq = ReadOptions {
        filters: vec![Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(absent_target),
        )],
        ..Default::default()
    };
    let mut stream = storage.read(&project, &table, opts_eq).await.unwrap();
    let mut absent_rows = 0usize;
    while let Some(b) = stream.next().await {
        absent_rows += b.unwrap().num_rows();
    }
    assert_eq!(absent_rows, 0);
    let absent_snap = counters.snapshot();

    // Bloom-only phase: an in-range absent id (550, in the 500..600 gap).
    let bloom_only_target = run_bloom_only_phase(&storage, &counters).await;

    let total_considered = bloom_only_target.row_groups_considered as f64;
    let pruned_by_bloom = bloom_only_target.row_groups_pruned_by_bloom as f64;
    let pruned_fraction = if total_considered > 0.0 {
        pruned_by_bloom / total_considered
    } else {
        0.0
    };
    let bar = 0.8;
    let pass = pruned_fraction >= bar;

    println!(
        "[S3 viability_bloom] bloom-only: groups considered={}, scanned={}, pruned_stats={}, pruned_bloom={}, fraction={:.2} (bar >={:.0}%) {}",
        bloom_only_target.row_groups_considered,
        bloom_only_target.row_groups_scanned,
        bloom_only_target.row_groups_pruned_by_stats,
        bloom_only_target.row_groups_pruned_by_bloom,
        pruned_fraction,
        bar * 100.0,
        if pass { "PASS" } else { "FAIL" },
    );

    report_real_viability(
        "bloom_filter_pruning",
        "Bloom filter pruning for absent point queries (real S3)",
        "On a multi-row-group Parquet file living on real S3 with `bloom_filter_columns = [\"id\"]`, an in-range \
         point query for a non-existent id prunes ≥80% of row groups via the per-group bloom filter, even when \
         min/max stats cannot.",
        pass,
        PrimaryMetric {
            label: "row_groups_pruned_by_bloom / row_groups_considered".into(),
            value: pruned_fraction,
            unit: "fraction".into(),
            bar: BarOp::ge(bar),
        },
        json!({
            "rows": ROWS,
            "row_group_size": ROW_GROUP_SIZE,
            "absent_in_range_target": "550 (outside [500, 600) gap)",
            "existing_phase": {
                "groups_considered": existing_snap.row_groups_considered,
                "groups_scanned": existing_snap.row_groups_scanned,
                "pruned_by_stats": existing_snap.row_groups_pruned_by_stats,
                "pruned_by_bloom": existing_snap.row_groups_pruned_by_bloom,
            },
            "out_of_range_absent_phase": {
                "groups_considered": absent_snap.row_groups_considered,
                "groups_scanned": absent_snap.row_groups_scanned,
                "pruned_by_stats": absent_snap.row_groups_pruned_by_stats,
                "pruned_by_bloom": absent_snap.row_groups_pruned_by_bloom,
            },
            "bloom_only_phase": {
                "groups_considered": bloom_only_target.row_groups_considered,
                "groups_scanned": bloom_only_target.row_groups_scanned,
                "pruned_by_stats": bloom_only_target.row_groups_pruned_by_stats,
                "pruned_by_bloom": bloom_only_target.row_groups_pruned_by_bloom,
            },
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "bloom-filter pruning fraction {:.2} < bar {:.2} (groups_considered={}, pruned_by_bloom={})",
        pruned_fraction,
        bar,
        bloom_only_target.row_groups_considered,
        bloom_only_target.row_groups_pruned_by_bloom,
    );
}

async fn run_bloom_only_phase(
    storage: &Storage,
    counters: &Arc<basin_storage::ReadCounters>,
) -> basin_storage::ReadCountersSnapshot {
    let project = ProjectId::new();
    let table = TableName::new("bloom_only").unwrap();
    let part = PartitionKey::default_key();

    let mut ids: Vec<i64> = Vec::with_capacity(ROWS as usize);
    ids.extend(0i64..500);
    ids.extend(600i64..1100);
    let mut state: u64 = 0xCAFE_F00D_BAAD_BABE;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }
    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = (0..ROWS).map(|i| format!("p-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    let batch =
        RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap();

    let opts = WriteOptions {
        bloom_filter_columns: vec!["id".to_string()],
        cluster_columns: vec![],
        max_row_group_size: Some(ROW_GROUP_SIZE),
        ..Default::default()
    };
    storage
        .write_batch_with_options(&project, &table, &part, &batch, &opts)
        .await
        .unwrap();

    counters.reset();
    let target: i64 = 550;
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(target))],
        ..Default::default()
    };
    let mut stream = storage.read(&project, &table, opts).await.unwrap();
    let mut rows = 0usize;
    while let Some(b) = stream.next().await {
        rows += b.unwrap().num_rows();
    }
    assert_eq!(
        rows, 0,
        "expected zero rows for absent in-range id={target}"
    );
    counters.snapshot()
}
