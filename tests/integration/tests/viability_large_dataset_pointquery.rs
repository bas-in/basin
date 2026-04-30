//! Viability test 6: large-dataset point query.
//!
//! Claim: Basin handles datasets that are awkward for SQLite-class systems.
//! 10 million rows split across 10 files, with non-overlapping `id` ranges
//! so a single point lookup hits exactly one file's footer + one row group.
//!
//! We seed the data via `Storage::write_batch` + `Catalog::append_data_files`
//! directly — going through the engine's `INSERT ... VALUES` path for 10M
//! rows would burn budget on SQL parsing. The DataFusion path is exercised
//! for the SELECT, which is what we're actually measuring.
//!
//! Bar: <1000 ms for the SELECT.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;

const FILES: usize = 10;
const ROWS_PER_FILE: usize = 1_000_000;
const TOTAL_ROWS: usize = FILES * ROWS_PER_FILE;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let ts: Int64Array = (start..start + len as i64).map(|v| v * 1000).collect();
    // Smallish but non-trivial payload — avoid all-equal so dictionary doesn't
    // collapse the whole column to one value.
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("p-{:020}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(ts), Arc::new(payload_arr)],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_6_large_dataset_pointquery() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let tenant = TenantId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    catalog.create_namespace(&tenant).await.unwrap();
    catalog.create_table(&tenant, &table, &schema()).await.unwrap();

    // Seed phase: 10 files, each one a separate write_batch. Run them in
    // parallel — debug builds spend most of their time in parquet encoding,
    // which parallelizes well. Total volume is 10M rows; on a stock laptop
    // this is the long pole of the test (~30-60s in debug).
    println!(
        "WARNING: viability_6 seeds {} rows across {} files; this is slow on debug builds",
        TOTAL_ROWS, FILES
    );

    let seed_started = Instant::now();
    let mut writers: JoinSet<basin_storage::DataFile> = JoinSet::new();
    for f in 0..FILES {
        let storage = storage.clone();
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let start = (f * ROWS_PER_FILE) as i64;
            let batch = build_batch(start, ROWS_PER_FILE);
            storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap()
        });
    }
    let mut data_files: Vec<DataFileRef> = Vec::with_capacity(FILES);
    while let Some(r) = writers.join_next().await {
        let df = r.unwrap();
        data_files.push(DataFileRef {
            path: df.path.as_ref().to_string(),
            size_bytes: df.size_bytes,
            row_count: df.row_count,
        });
    }
    let seed_elapsed = seed_started.elapsed();

    // Single atomic catalog commit covering all 10 files.
    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    catalog
        .append_data_files(&tenant, &table, meta.current_snapshot, data_files)
        .await
        .unwrap();

    // Open the engine session AFTER the seed commit so DataFusion sees all
    // 10 files. (Engine sessions cache the listing-table; in production
    // we'd `refresh_table` after the commit, but for this test re-opening
    // is simpler.)
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
    });
    let sess = engine.open_session(tenant).await.unwrap();

    // Pick a target id that lands in the middle of file 4 (so one and only
    // one file should match by stats).
    let target_id: i64 = (4 * ROWS_PER_FILE as i64) + (ROWS_PER_FILE as i64 / 2);

    // Warm DataFusion once so the first-run JIT / catalog walk isn't part of
    // the measurement. We only measure the second run.
    let warm_sql = format!("SELECT id, payload FROM t WHERE id = {}", target_id);
    let _warm = sess.execute(&warm_sql).await.unwrap();

    let q_started = Instant::now();
    let res = sess.execute(&warm_sql).await.unwrap();
    let q_elapsed = q_started.elapsed();
    let q_ms = q_elapsed.as_secs_f64() * 1000.0;

    let row_count = match &res {
        ExecResult::Rows { batches, .. } => {
            let mut hits = 0usize;
            for b in batches {
                let ids = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..ids.len() {
                    assert_eq!(ids.value(i), target_id);
                    hits += 1;
                }
            }
            hits
        }
        ExecResult::Empty { .. } => panic!("expected rows"),
    };
    assert_eq!(row_count, 1, "expected exactly one row, got {row_count}");

    let pass = q_ms < 1000.0;
    println!(
        "[VIABILITY 6] large dataset point query: rows={}, files={}, seed_elapsed={:.2}s, point_query_ms={:.1} (bar <1000 ms) {}",
        TOTAL_ROWS,
        FILES,
        seed_elapsed.as_secs_f64(),
        q_ms,
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "large_dataset_pointquery",
        "Large-dataset point query",
        "Point queries on a 10M-row dataset return in under 1 second.",
        pass,
        PrimaryMetric {
            label: "point_query_ms".into(),
            value: q_ms,
            unit: "ms".into(),
            bar: BarOp::lt(1000.0),
        },
        json!({
            "rows": TOTAL_ROWS,
            "files": FILES,
            "seed_elapsed_s": seed_elapsed.as_secs_f64(),
        }),
    );

    assert!(pass, "point query took {q_ms:.1} ms, bar <1000 ms");
}
