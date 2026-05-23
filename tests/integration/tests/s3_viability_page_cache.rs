//! S3 port of `viability_page_cache.rs`.
//!
//! Card: `viability_page_cache` (real-cloud dashboard).
//!
//! Bar: cold p99 of a random-working-set point-query workload < 2500 ms
//! against a real S3-compatible backend. The LocalFS card uses a
//! sub-millisecond LocalFileSystem and lands cold p99 around 25-40 ms;
//! on real S3 the cold tail is dominated by network RTT (200-500 ms per
//! cold fetch from APAC, plus TLS warmup), so the bar lands at 2500 ms
//! to give honest headroom while still failing if the cache short-circuit
//! breaks.
//!
//! On SeaweedFS (local network, sub-ms RTT) the cold→warm gap is modest
//! since cold fetches are nearly free anyway — the page cache still cuts
//! Parquet decode work, but the wall-clock difference is small.
//!
//! What this version does (mirrors the LocalFS card):
//!   * 100k-row dataset across 10 Parquet files written to a real bucket.
//!   * 1000-id random working set with a fixed PRNG seed.
//!   * 200 cold-pass iterations (caches empty for the under-test Storage).
//!   * 200 warm-pass iterations (working set already touched).
//!   * Cold p50 / p99 / p999 + warm p50 / p99 / p999 reported.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_integration_tests::workload::{run_workload, LatencyDistribution, WorkloadConfig};
use basin_storage::{PageCacheConfig, Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::stream::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_page_cache";

const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 10;
const TOTAL_ROWS: u64 = (ROWS_PER_BATCH * BATCHES) as u64;

const CACHE_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Cold p99 bar in milliseconds. See module docs.
const BAR_COLD_P99_MS: f64 = 2_500.0;

/// Iteration count per phase. 200 keeps wall-clock manageable on cloud
/// RTT; statistically tight enough for a regression on cold p99.
const ITERATIONS_PER_PHASE: usize = 200;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn make_batch(start: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + ROWS_PER_BATCH as i64).collect();
    let payloads: Vec<String> = (0..ROWS_PER_BATCH)
        .map(|i| format!("payload-{:08}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

async fn point_query(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    id: i64,
) -> Result<(), String> {
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(id))],
        ..Default::default()
    };
    let mut stream = storage
        .read(project, table, opts)
        .await
        .map_err(|e| format!("read({id}): {e}"))?;
    let mut rows = 0usize;
    while let Some(b) = stream.next().await {
        rows += b.map_err(|e| format!("batch({id}): {e}"))?.num_rows();
    }
    if rows != 1 {
        return Err(format!("id={id}: expected 1 row, got {rows}"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_viability_page_cache() {
    basin_common::telemetry::try_init_for_tests();

    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store: Arc<dyn ObjectStore> = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };
    let prefix_path = ObjectPath::from(run_prefix.as_str());

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // ---- write phase (no caches on writer Storage) -----------------------
    let writer_storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: None,
        page_cache: None,
    });
    for b in 0..BATCHES {
        let batch = make_batch((b * ROWS_PER_BATCH) as i64);
        writer_storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .expect("write");
    }
    drop(writer_storage);

    // ---- under-test storage: page cache attached, disk cache off --------
    //
    // This card measures the in-RAM page cache in isolation; the disk
    // cache card covers SSD.
    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: None,
        page_cache: Some(PageCacheConfig::new(CACHE_BUDGET_BYTES)),
    });

    let workload_cfg = WorkloadConfig {
        working_set_size: 1_000,
        n_iterations: ITERATIONS_PER_PHASE,
        ..WorkloadConfig::default_for_point_query()
    };

    // ---- cold pass --------------------------------------------------------
    let cold_dist: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage;
        let project = &project;
        let table = &table;
        async move { point_query(storage, project, table, id as i64).await }
    })
    .await;

    let counters_after_cold = storage
        .page_cache()
        .map(|pc| pc.counters())
        .unwrap_or_default();

    // ---- warm pass --------------------------------------------------------
    let warm_dist: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage;
        let project = &project;
        let table = &table;
        async move { point_query(storage, project, table, id as i64).await }
    })
    .await;

    let counters_after_warm = storage
        .page_cache()
        .map(|pc| pc.counters())
        .unwrap_or_default();

    let pass = cold_dist.p99_ms < BAR_COLD_P99_MS;

    println!(
        "[S3 page_cache] cold p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        cold_dist.p50_ms, cold_dist.p99_ms, cold_dist.p999_ms, cold_dist.n,
    );
    println!(
        "[S3 page_cache] warm p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        warm_dist.p50_ms, warm_dist.p99_ms, warm_dist.p999_ms, warm_dist.n,
    );
    println!(
        "[S3 page_cache] cold counters: hits={}, misses={}, evictions={}",
        counters_after_cold.hits, counters_after_cold.misses, counters_after_cold.evictions,
    );
    println!(
        "[S3 page_cache] warm counters: hits={}, misses={}, evictions={}",
        counters_after_warm.hits, counters_after_warm.misses, counters_after_warm.evictions,
    );

    let cold_to_warm_p50_ratio = if warm_dist.p50_ms > 0.0 {
        cold_dist.p50_ms / warm_dist.p50_ms
    } else {
        f64::INFINITY
    };

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
        "page_cache",
        "Parquet page cache (real S3)",
        "Cold p99 of a random-working-set point-query workload finishes \
         under 2500 ms on a real S3-compatible backend. Cold pass starts \
         with an empty page cache (every working-set id is a miss); warm \
         pass repeats the same workload with the cache populated. The bar \
         is set against COLD p99 so an implementation can't pass by leaning \
         on a pre-warmed cache.",
        pass,
        PrimaryMetric {
            label: "cold p99 ms".into(),
            value: cold_dist.p99_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_COLD_P99_MS),
        },
        json!({
            "total_rows": TOTAL_ROWS,
            "batches": BATCHES,
            "working_set_size": workload_cfg.working_set_size,
            "n_iterations": workload_cfg.n_iterations,
            "seed": workload_cfg.seed,
            "cache_budget_bytes": CACHE_BUDGET_BYTES,
            "bar_cold_p99_ms": BAR_COLD_P99_MS,
            "rows": rows,
            "cold_to_warm_p50_ratio": cold_to_warm_p50_ratio,
            "cold_hits": counters_after_cold.hits,
            "cold_misses": counters_after_cold.misses,
            "warm_hits_total": counters_after_warm.hits,
            "warm_misses_total": counters_after_warm.misses,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
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

    assert!(
        counters_after_warm.hits > counters_after_cold.hits,
        "warm phase produced no additional cache hits (cold hits={}, warm hits={}); \
         is the page cache key matching across iterations?",
        counters_after_cold.hits,
        counters_after_warm.hits,
    );
}
