//! S3 port of `viability_large_dataset_pointquery.rs`.
//!
//! Card: `viability_large_dataset_pointquery` (real-cloud dashboard).
//!
//! 10 M rows in 10 files on S3. Each point query reads one Parquet
//! footer + one row group via byte-range GETs over HTTP.
//!
//! Bar (post benchmark-honesty audit): cold p99 of a 1000-iteration
//! random-working-set point-query workload < 8000 ms. The old version
//! ran one warm-up SELECT and *one* timed SELECT against the same id,
//! reported that single sample, and put the bar at 5000 ms. That
//! number was a cache hit on the file's row-group fragment after the
//! warm-up; a real client whose query stream walks different ids
//! cannot hide behind a single warm-up.
//!
//! The new bar is set against COLD p99 of a workload where each query
//! picks a fresh id from a fixed-seed pool of 1000 hot ids. On real
//! S3 the per-RPC HTTP RTT (~30-100 ms in-region) plus footer fetch
//! plus row-group fetch plus decode lands cold p99 around 1500-3000 ms
//! in practice; 8000 ms gives ~3-5× headroom over honest cold p99
//! while still being a meaningful "S3-class point query" claim.
//!
//! TODO: re-tune the bar after the in-flight cache-defaults agent's
//! work lands. Cache defaults flipping ON (currently off in shipping
//! `StorageConfig`) will lower honest cold p99 by 5-10× since most
//! repeated working-set ids will be served from local NVMe.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_integration_tests::workload::{run_workload, LatencyDistribution, WorkloadConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio::task::JoinSet;

const TEST_NAME: &str = "s3_large_dataset_pointquery";
const FILES: usize = 10;
const ROWS_PER_FILE: usize = 1_000_000;
const TOTAL_ROWS: u64 = (FILES * ROWS_PER_FILE) as u64;

/// Cold p99 bar in milliseconds. See module docs for the choice.
const BAR_COLD_P99_MS: f64 = 8_000.0;

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

/// One point query through the engine. Asserts exactly one row matches.
async fn engine_point_query(sess: &TenantSession, id: i64) -> Result<(), String> {
    let sql = format!("SELECT id, payload FROM t WHERE id = {}", id);
    let res = sess
        .execute(&sql)
        .await
        .map_err(|e| format!("execute({id}): {e:?}"))?;
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut hits = 0usize;
            for b in &batches {
                let ids = b
                    .column_by_name("id")
                    .ok_or_else(|| format!("id={id}: missing id column"))?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| format!("id={id}: id column not Int64"))?;
                for i in 0..ids.len() {
                    if ids.value(i) != id {
                        return Err(format!(
                            "id={id}: returned row had id={}",
                            ids.value(i)
                        ));
                    }
                    hits += 1;
                }
            }
            if hits != 1 {
                return Err(format!("id={id}: expected 1 row, got {hits}"));
            }
            Ok(())
        }
        ExecResult::Empty { .. } => Err(format!("id={id}: expected rows, got Empty")),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_large_dataset_pointquery() {
    let cfg_test = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg_test.s3_or_skip(TEST_NAME) {
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
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
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

    // One throwaway SELECT to flush DataFusion's per-session lazy
    // initialisation — same rationale as the LocalFS card.
    let _ = sess
        .execute("SELECT count(*) FROM t")
        .await
        .expect("warmup count");

    let cfg = WorkloadConfig::default_for_point_query();

    // ---- cold pass ----
    //
    // Random working-set queries against an empty page/disk cache.
    // The CleanupOnDrop guard above and the per-test cache tempdir
    // (created fresh by `default_test_disk_cache` on every call) both
    // ensure cold really is cold.
    let cold_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let sess = &sess;
        async move { engine_point_query(sess, id as i64).await }
    })
    .await;

    // ---- warm pass ----
    let warm_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let sess = &sess;
        async move { engine_point_query(sess, id as i64).await }
    })
    .await;

    let pass = cold_dist.p99_ms < BAR_COLD_P99_MS;

    println!(
        "[S3 large_dataset_pointquery] rows={}, files={}, seed_elapsed={:.2}s",
        TOTAL_ROWS,
        FILES,
        seed_elapsed.as_secs_f64(),
    );
    println!(
        "[S3 large_dataset_pointquery] cold p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        cold_dist.p50_ms, cold_dist.p99_ms, cold_dist.p999_ms, cold_dist.n,
    );
    println!(
        "[S3 large_dataset_pointquery] warm p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={}) {}",
        warm_dist.p50_ms,
        warm_dist.p99_ms,
        warm_dist.p999_ms,
        warm_dist.n,
        if pass { "PASS" } else { "FAIL" },
    );

    let rows = vec![
        json!({
            "phase": "cold",
            "p50_ms": cold_dist.p50_ms,
            "p99_ms": cold_dist.p99_ms,
            "p999_ms": cold_dist.p999_ms,
            "min_ms": cold_dist.min_ms,
            "max_ms": cold_dist.max_ms,
            "mean_ms": cold_dist.mean_ms,
        }),
        json!({
            "phase": "warm",
            "p50_ms": warm_dist.p50_ms,
            "p99_ms": warm_dist.p99_ms,
            "p999_ms": warm_dist.p999_ms,
            "min_ms": warm_dist.min_ms,
            "max_ms": warm_dist.max_ms,
            "mean_ms": warm_dist.mean_ms,
        }),
    ];

    report_real_viability(
        "large_dataset_pointquery",
        "Large-dataset point query (real S3)",
        "Cold p99 of a 1000-iteration random-working-set point-query workload \
         on a 10 M-row dataset stored on S3 finishes under 8 seconds. Each \
         query picks a different id from a fixed-seed pool of 1000 hot ids. \
         The bar is on COLD p99 (caches empty at the start of the run); a \
         'warm' phase is reported alongside.",
        pass,
        PrimaryMetric {
            label: "cold p99 ms".into(),
            value: cold_dist.p99_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_COLD_P99_MS),
        },
        json!({
            "total_rows": TOTAL_ROWS,
            "files": FILES,
            "seed_elapsed_s": seed_elapsed.as_secs_f64(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
            "working_set_size": cfg.working_set_size,
            "n_iterations": cfg.n_iterations,
            "seed": cfg.seed,
            "bar_cold_p99_ms": BAR_COLD_P99_MS,
            "rows": rows,
        }),
    );

    assert!(
        cold_dist.p99_ms < BAR_COLD_P99_MS,
        "cold p99 {:.2}ms exceeds bar {:.0}ms (p50={:.2}, p999={:.2})",
        cold_dist.p99_ms,
        BAR_COLD_P99_MS,
        cold_dist.p50_ms,
        cold_dist.p999_ms,
    );
}
