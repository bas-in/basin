//! S3 port of `viability_perf_stack.rs` (LocalFS file name kept for legacy;
//! the card itself is a *scaling* card with id `perf_stack`).
//!
//! Card: `perf_stack` (real-cloud dashboard, scaling kind).
//!
//! Bars on real S3:
//!   * (d) full-stack cold p99 < 3000 ms — layer (d) with disk + page cache
//!     + bloom filter should hide the worst-case S3 RTT on the warm
//!     working set; the tail is dominated by the few cold misses.
//!   * speedup_a_to_d ≥ 5× — the 100× LocalFS bar relied on the
//!     synthetic `LatencyStore` injecting a fixed 50 ms per RPC. On
//!     real S3 the speedup magnitude depends on (a) real RTT and (b)
//!     decode work; from APAC to R2 we see 20-50× honestly. On
//!     SeaweedFS over loopback the cold path itself is sub-millisecond,
//!     so the speedup compresses to ~3-10× — the bar is loose enough
//!     that the local-network case still passes when caches are
//!     correctly wired but tight enough that a regression that disables
//!     the cache short-circuit fails.
//!
//! Why no `LatencyStore` here: the LocalFS card uses `LatencyStore` as a
//! stand-in for cross-region S3. On real S3 the network *is* the
//! latency, so we wire `Storage` directly to the bucket. The same
//! 4-layer cumulative stack is exercised:
//!   (a) baseline       — no caches, no bloom
//!   (b) +disk cache
//!   (c) +disk + page cache
//!   (d) +disk + page cache + bloom filter
//!
//! Iteration count per layer is reduced to 100 (LocalFS is 200) to keep
//! 4 layers × 100 iterations × ~100-500 ms per cold query inside a 5
//! min wall-clock budget on R2 from APAC. The headline (cold p50 of
//! each layer + speedup ratio) is unchanged.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_integration_tests::workload::{run_workload, LatencyDistribution, WorkloadConfig};
use basin_storage::{
    DiskCacheConfig, PageCacheConfig, Predicate, ReadOptions, ScalarValue, Storage, StorageConfig,
    WriteOptions,
};
use futures::stream::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

const TEST_NAME: &str = "s3_scaling_perf_stack";

/// 100k rows split into 10 batches.
const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 10;
const TOTAL_ROWS: u64 = (ROWS_PER_BATCH * BATCHES) as u64;

/// Force ≥10 row groups per file so bloom-filter pruning has room to work.
const ROW_GROUP_SIZE: usize = 1_024;

const DISK_CACHE_BUDGET: u64 = 256 * 1024 * 1024;
const PAGE_CACHE_BUDGET: u64 = 256 * 1024 * 1024;

/// Bars (see module docs).
const BAR_D_MAX_P99_MS: f64 = 3_000.0;
const BAR_SPEEDUP: f64 = 5.0;

/// Iterations per layer. 100 keeps 4 layers × R2 RTT inside ~5 min.
const ITERATIONS_PER_LAYER: usize = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Shuffled-id batch so per-row-group min/max can't alone prune; bloom
/// filter is the load-bearing prune for layer (d).
fn make_batch(seed: u64, batch_idx: usize) -> RecordBatch {
    let start = (batch_idx * ROWS_PER_BATCH) as i64;
    let mut ids: Vec<i64> = (start..start + ROWS_PER_BATCH as i64).collect();
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(batch_idx as u64);
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }
    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = ids.iter().map(|i| format!("payload-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap()
}

async fn point_query(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
    id: i64,
) -> Result<(), String> {
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(id))],
        ..Default::default()
    };
    let mut stream = storage
        .read(tenant, table, opts)
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
#[ignore]
async fn s3_scaling_perf_stack() {
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

    let tenant = TenantId::new();
    let part = PartitionKey::default_key();

    let no_bloom_table = TableName::new("events").unwrap();
    let with_bloom_table = TableName::new("events_bloom").unwrap();

    // ---- Write phase --------------------------------------------------
    let writer_storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: None,
        page_cache: None,
    });
    let no_bloom_opts = WriteOptions {
        bloom_filter_columns: vec![],
        max_row_group_size: Some(ROW_GROUP_SIZE),
    };
    let bloom_opts = WriteOptions {
        bloom_filter_columns: vec!["id".to_string()],
        max_row_group_size: Some(ROW_GROUP_SIZE),
    };
    for b in 0..BATCHES {
        let batch = make_batch(0xCAFEBABE_DEAD_BEEFu64, b);
        writer_storage
            .write_batch_with_options(&tenant, &no_bloom_table, &part, &batch, &no_bloom_opts)
            .await
            .expect("write no-bloom");
        writer_storage
            .write_batch_with_options(&tenant, &with_bloom_table, &part, &batch, &bloom_opts)
            .await
            .expect("write with-bloom");
    }
    drop(writer_storage);

    let workload_cfg = WorkloadConfig {
        working_set_size: 200,
        n_iterations: ITERATIONS_PER_LAYER,
        ..WorkloadConfig::default_for_point_query()
    };

    // ---- (a) Baseline: no caches, no bloom ----------------------------
    let storage_a = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: None,
        page_cache: None,
    });
    let dist_a: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage_a;
        let tenant = &tenant;
        let table = &no_bloom_table;
        async move { point_query(storage, tenant, table, id as i64).await }
    })
    .await;

    // ---- (b) Disk cache only ------------------------------------------
    let dc_dir_b = TempDir::new().unwrap();
    let storage_b = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: Some(DiskCacheConfig::new(
            dc_dir_b.path().to_path_buf(),
            DISK_CACHE_BUDGET,
        )),
        page_cache: None,
    });
    let dist_b: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage_b;
        let tenant = &tenant;
        let table = &no_bloom_table;
        async move { point_query(storage, tenant, table, id as i64).await }
    })
    .await;

    // ---- (c) Disk + page cache ----------------------------------------
    let dc_dir_c = TempDir::new().unwrap();
    let storage_c = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: Some(DiskCacheConfig::new(
            dc_dir_c.path().to_path_buf(),
            DISK_CACHE_BUDGET,
        )),
        page_cache: Some(PageCacheConfig::new(PAGE_CACHE_BUDGET)),
    });
    let dist_c: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage_c;
        let tenant = &tenant;
        let table = &no_bloom_table;
        async move { point_query(storage, tenant, table, id as i64).await }
    })
    .await;

    // ---- (d) Disk + page cache + bloom filter -------------------------
    let dc_dir_d = TempDir::new().unwrap();
    let storage_d = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(prefix_path.clone()),
        disk_cache: Some(DiskCacheConfig::new(
            dc_dir_d.path().to_path_buf(),
            DISK_CACHE_BUDGET,
        )),
        page_cache: Some(PageCacheConfig::new(PAGE_CACHE_BUDGET)),
    });
    let dist_d: LatencyDistribution = run_workload(&workload_cfg, TOTAL_ROWS, |id| {
        let storage = &storage_d;
        let tenant = &tenant;
        let table = &with_bloom_table;
        async move { point_query(storage, tenant, table, id as i64).await }
    })
    .await;

    let speedup = if dist_d.p50_ms > 0.0 {
        dist_a.p50_ms / dist_d.p50_ms
    } else {
        f64::INFINITY
    };

    let bar_d_ok = dist_d.p99_ms < BAR_D_MAX_P99_MS;
    let bar_speedup_ok = speedup >= BAR_SPEEDUP;
    let pass = bar_d_ok && bar_speedup_ok;

    println!(
        "[S3 perf_stack] (a) baseline:  p50={:.2}ms p99={:.2}ms p999={:.2}ms",
        dist_a.p50_ms, dist_a.p99_ms, dist_a.p999_ms,
    );
    println!(
        "[S3 perf_stack] (b) +disk:     p50={:.2}ms p99={:.2}ms p999={:.2}ms",
        dist_b.p50_ms, dist_b.p99_ms, dist_b.p999_ms,
    );
    println!(
        "[S3 perf_stack] (c) +page:     p50={:.2}ms p99={:.2}ms p999={:.2}ms",
        dist_c.p50_ms, dist_c.p99_ms, dist_c.p999_ms,
    );
    println!(
        "[S3 perf_stack] (d) +bloom:    p50={:.2}ms p99={:.2}ms p999={:.2}ms",
        dist_d.p50_ms, dist_d.p99_ms, dist_d.p999_ms,
    );
    println!(
        "[S3 perf_stack] speedup a→d (p50) = {:.1}× (bar ≥ {:.0}×); d p99 < {:.0}ms? {}",
        speedup, BAR_SPEEDUP, BAR_D_MAX_P99_MS, bar_d_ok,
    );

    let layer_row =
        |key: &str, label: &str, d: &LatencyDistribution| -> serde_json::Value {
            json!({
                "stack_layer": key,
                "label": label,
                "p50_ms": d.p50_ms,
                "p99_ms": d.p99_ms,
                "p999_ms": d.p999_ms,
                "min_ms": d.min_ms,
                "max_ms": d.max_ms,
                "mean_ms": d.mean_ms,
            })
        };
    let rows = vec![
        layer_row("a_baseline", "(a) no cache", &dist_a),
        layer_row("b_disk", "(b) +disk cache", &dist_b),
        layer_row("c_disk_page", "(c) +page cache", &dist_c),
        layer_row("d_full", "(d) +bloom filter", &dist_d),
    ];

    report_real_scaling(
        "perf_stack",
        "Full perf stack on a real S3 point query",
        "Same SELECT … WHERE id = X measured four ways under a random- \
         working-set point-query workload against a real S3-compatible \
         backend: (a) no cache, (b) +disk cache, (c) +page cache, (d) \
         +bloom filter. The headline claim is speedup ≥ 5× from (a)'s \
         cold p50 to (d)'s cold p50, with (d)'s cold p99 under 3000 ms.",
        pass,
        AxisSpec {
            key: "stack_layer".into(),
            label: "Stack layer".into(),
        },
        vec![
            SeriesSpec {
                key: "p50_ms".into(),
                label: "p50 latency".into(),
                unit: Some("ms".into()),
            },
            SeriesSpec {
                key: "p99_ms".into(),
                label: "p99 latency".into(),
                unit: Some("ms".into()),
            },
        ],
        rows,
        Some(PrimaryMetric {
            label: "speedup p50 (a→d)".into(),
            value: speedup,
            unit: "x".into(),
            bar: BarOp::ge(BAR_SPEEDUP),
        }),
    );

    assert!(
        bar_d_ok,
        "(d) full-stack p99 {:.2}ms exceeds bar < {:.0}ms",
        dist_d.p99_ms, BAR_D_MAX_P99_MS,
    );
    assert!(
        bar_speedup_ok,
        "speedup a→d (p50) = {:.2}× below bar ≥ {:.0}× \
         (a.p50={:.2}ms, d.p50={:.2}ms)",
        speedup, BAR_SPEEDUP, dist_a.p50_ms, dist_d.p50_ms,
    );
}
