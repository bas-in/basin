//! Viability test: NVMe disk cache.
//!
//! Card: `viability_disk_cache`
//!
//! Bar (post benchmark-honesty audit): cold p99 of a random-working-set
//! point-query workload, with a 50 ms-per-RPC latency injector standing
//! in for cross-region S3, finishes under 250 ms. Without the disk cache
//! every read pays the 50 ms RPC fee on every range GET — a single
//! footer + row-group fetch costs ~150-200 ms uncached, so the bar
//! demands the cache covers most of the working set under load.
//!
//! What the old version of this test did: read the whole table cold,
//! then read it again warm, and reported `cold_ms / warm_ms ≥ 10×`.
//! The "warm" path was a single full-table scan with all caches hot;
//! that's a streaming bandwidth measurement, not a point-query latency
//! measurement, and the ratio number was inflated by the cold pass
//! making 50+ slow inner GETs in series.
//!
//! What this version does:
//!   * 100k-row dataset across 10 Parquet files, behind a 50 ms latency
//!     injector wrapping a LocalFileSystem.
//!   * 1000-id random working set drawn with a fixed PRNG seed.
//!   * 1000 point queries cold (fresh disk-cache tempdir + fresh
//!     Storage so the in-process metadata cache is also empty).
//!   * 1000 point queries warm (working set populated, metadata cache
//!     hot).
//!   * Cold p50 / p99 / p999 + warm p50 / p99 / p999 reported. Bar
//!     asserts against COLD p99.
//!
//! Why 250 ms cold p99: the inner GET pays 50 ms; a single point query
//! against an unread file requires at least one footer fetch + one
//! row-group fetch (≈100 ms on cold pass). Add Parquet decode overhead
//! and tail-latency variance, and an honest cold p99 lands around
//! 100-150 ms; 250 ms is ~2× headroom over typical and ~5× headroom
//! over the median — tight enough that a regression that breaks the
//! cache short-circuit obviously fails, loose enough that CI noise
//! doesn't flake.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_common::{PartitionKey, TableName, ProjectId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_integration_tests::workload::{run_workload, LatencyDistribution, WorkloadConfig};
use basin_storage::{DiskCacheConfig, Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use serde_json::json;
use tempfile::TempDir;

/// 100k rows split into 10 batches of 10k. Multiple Parquet files give
/// the disk cache realistic working-set granularity.
const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 10;
const TOTAL_ROWS: u64 = (ROWS_PER_BATCH * BATCHES) as u64;

/// Per-`get_opts` injected latency. 50 ms is a realistic in-region S3
/// GET on AWS.
const SIMULATED_LATENCY: Duration = Duration::from_millis(50);

/// Disk cache budget. Has to comfortably fit every Parquet footer +
/// every row-group fragment the warm phase will request.
const CACHE_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Cold p99 bar in milliseconds. See module docs for the choice.
const BAR_COLD_P99_MS: f64 = 250.0;

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

/// Object-store wrapper that injects a fixed sleep on every read RPC.
/// Stands in for a high-latency remote store on local CI without
/// requiring a real S3 bucket.
#[derive(Debug)]
struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    inner_gets: AtomicUsize,
}

impl std::fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        tokio::time::sleep(self.latency).await;
        self.inner_gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }
    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<Bytes> {
        tokio::time::sleep(self.latency).await;
        self.inner_gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(location, range).await
    }
    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// One point query. Returns `Ok(())` on the expected single-row result.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_disk_cache() {
    basin_common::telemetry::try_init_for_tests();

    let bucket_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();

    let real_fs: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap());
    let latency: Arc<LatencyStore> = Arc::new(LatencyStore {
        inner: real_fs,
        latency: SIMULATED_LATENCY,
        inner_gets: AtomicUsize::new(0),
    });

    let dc_cfg = DiskCacheConfig::new(cache_dir.path().to_path_buf(), CACHE_BUDGET_BYTES);

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // ---- write phase: populate the bucket ---------------------------------
    //
    // Non-cached Storage so the cache never sees these PUTs and the cold
    // pass exercises the full miss path on every file.
    let writer_storage = Storage::new(StorageConfig {
        object_store: latency.clone() as Arc<dyn ObjectStore>,
        root_prefix: None,
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
    latency.inner_gets.store(0, Ordering::Relaxed);

    // ---- under-test storage: disk cache attached, page cache off ---------
    //
    // Page cache deliberately off — this card measures the SSD layer in
    // isolation. The page-cache benchmark in `viability_page_cache.rs`
    // covers the in-RAM layer.
    let storage = Storage::new(StorageConfig {
        object_store: latency.clone() as Arc<dyn ObjectStore>,
        root_prefix: None,
        disk_cache: Some(dc_cfg.clone()),
        page_cache: None,
    });

    let cfg = WorkloadConfig::default_for_point_query();

    // ---- cold pass: caches empty, working-set ids are misses --------------
    let inner_gets_at_cold_start = latency.inner_gets.load(Ordering::Relaxed);
    let cold_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let storage = &storage;
        let project = &project;
        let table = &table;
        async move { point_query(storage, project, table, id as i64).await }
    })
    .await;
    let cold_inner_gets = latency.inner_gets.load(Ordering::Relaxed) - inner_gets_at_cold_start;

    // ---- warm pass: caches populated --------------------------------------
    //
    // Same Storage (so the metadata cache stays hot) and same workload
    // seed (so the picker re-asks for ids the cache has now seen at
    // least once).
    let inner_gets_at_warm_start = latency.inner_gets.load(Ordering::Relaxed);
    let warm_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let storage = &storage;
        let project = &project;
        let table = &table;
        async move { point_query(storage, project, table, id as i64).await }
    })
    .await;
    let warm_inner_gets = latency.inner_gets.load(Ordering::Relaxed) - inner_gets_at_warm_start;

    let pass = cold_dist.p99_ms < BAR_COLD_P99_MS;

    println!(
        "[VIABILITY disk_cache] cold p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        cold_dist.p50_ms, cold_dist.p99_ms, cold_dist.p999_ms, cold_dist.n,
    );
    println!(
        "[VIABILITY disk_cache] warm p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        warm_dist.p50_ms, warm_dist.p99_ms, warm_dist.p999_ms, warm_dist.n,
    );
    println!(
        "[VIABILITY disk_cache] cold_inner_gets={cold_inner_gets}, warm_inner_gets={warm_inner_gets}, simulated_latency={}ms",
        SIMULATED_LATENCY.as_millis(),
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

    report_viability(
        "disk_cache",
        "NVMe disk cache (Phase 5.7 A1)",
        "Cold p99 of a 1000-iteration random-working-set point-query workload, \
         behind a 50 ms-per-RPC latency injector standing in for cross-region S3, \
         finishes under 250 ms. The 'cold' phase starts with both the disk cache \
         and the in-process metadata cache empty (every working-set id is a \
         miss); the 'warm' phase repeats the same workload (caches now \
         populated). The bar is set against COLD p99 so an implementation \
         can't pass by leaning on a pre-warmed cache.",
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
            "working_set_size": cfg.working_set_size,
            "n_iterations": cfg.n_iterations,
            "seed": cfg.seed,
            "simulated_latency_ms": SIMULATED_LATENCY.as_millis() as u64,
            "cache_budget_bytes": CACHE_BUDGET_BYTES,
            "bar_cold_p99_ms": BAR_COLD_P99_MS,
            "rows": rows,
            "cold_inner_gets": cold_inner_gets,
            "warm_inner_gets": warm_inner_gets,
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

    // Sanity bound: the warm pass should make far fewer inner GETs than
    // the cold pass. If they're comparable, the disk cache isn't doing
    // anything and the warm-vs-cold gap below is incidental.
    assert!(
        warm_inner_gets * 2 < cold_inner_gets || warm_inner_gets == 0,
        "warm phase made too many inner GETs: warm={warm_inner_gets} vs cold={cold_inner_gets}",
    );
}
