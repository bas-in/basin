//! S3 port of `viability_large_dataset_pointquery.rs`.
//!
//! 10M rows in 10 files on S3. The point query reads one Parquet footer +
//! one row group via byte-range GETs over HTTP. We bump the bar to 5_000 ms
//! since S3 round-trip dominates — the LocalFS bar of 1_000 ms was set on
//! page-cache reads that don't apply here.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio::task::JoinSet;

const TEST_NAME: &str = "s3_large_dataset_pointquery";
const FILES: usize = 10;
const ROWS_PER_FILE: usize = 1_000_000;
const TOTAL_ROWS: usize = FILES * ROWS_PER_FILE;
// Loosened from 1 s (LocalFS) to 5 s — S3 GETs incur per-request HTTP RTT.
const BAR_MS: f64 = 5_000.0;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_large_dataset_pointquery() {
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

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let tenant = TenantId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    catalog.create_namespace(&tenant).await.unwrap();
    catalog.create_table(&tenant, &table, &schema()).await.unwrap();

    println!(
        "WARNING: s3_large_dataset_pointquery seeds {} rows across {} files to S3",
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

    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    catalog
        .append_data_files(&tenant, &table, meta.current_snapshot, data_files)
        .await
        .unwrap();

    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let sess = engine.open_session(tenant).await.unwrap();

    let target_id: i64 = (4 * ROWS_PER_FILE as i64) + (ROWS_PER_FILE as i64 / 2);
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

    let pass = q_ms < BAR_MS;
    println!(
        "[S3 large_dataset_pointquery] rows={}, files={}, seed_elapsed={:.2}s, point_query_ms={:.1} (bar <{} ms) {}",
        TOTAL_ROWS,
        FILES,
        seed_elapsed.as_secs_f64(),
        q_ms,
        BAR_MS,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "large_dataset_pointquery",
        "Large-dataset point query (real S3)",
        "Point queries on a 10M-row dataset stored on S3 return in under 5 seconds.",
        pass,
        PrimaryMetric {
            label: "point_query_ms".into(),
            value: q_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_MS),
        },
        json!({
            "rows": TOTAL_ROWS,
            "files": FILES,
            "seed_elapsed_s": seed_elapsed.as_secs_f64(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(pass, "point query took {q_ms:.1} ms, bar <{BAR_MS} ms");
}
