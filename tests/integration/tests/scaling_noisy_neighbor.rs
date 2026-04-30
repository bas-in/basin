//! Scaling test 4: multi-tenant interference (noisy neighbor).
//!
//! Claim: A heavy "noisy" tenant doesn't crater a quiet tenant's latency.
//!
//! Honest expectation: in this single-process PoC, where every tenant
//! shares the same tokio runtime, the same DataFusion executor, the same
//! parquet reader, and the same OS file cache, there WILL be measurable
//! degradation. The bar is "doesn't fall off a cliff" (under_load.p99 /
//! baseline.p99 < 5x), not "zero impact". Phase 3 (real shard owners with
//! per-tenant CPU + memory governance) is the architectural fix; this
//! test calibrates how badly we need it.
//!
//! Setup:
//! - `quiet`: 10k rows, point-query workload `WHERE id = ?`.
//! - `noisy`: 5M rows, full-scan workload `SELECT * FROM events`.
//!
//! Phase A baseline: `quiet` runs 200 point queries alone.
//! Phase B under load: 4 parallel `noisy` full-scan tasks running while
//! `quiet` runs 200 point queries (after a 200ms warm-up).

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::dashboard::{
    report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;

const QUIET_ROWS: usize = 10_000;
const NOISY_ROWS: usize = 5_000_000;
const NOISY_BATCH: usize = 500_000;
const QUIET_QUERIES: usize = 200;
const NOISY_TASKS: usize = 4;
const WARMUP_MS: u64 = 200;
const BAR_P99_RATIO: f64 = 5.0;

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
        .map(|i| format!("payload-{:040}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(ts), Arc::new(payload_arr)],
    )
    .unwrap()
}

fn percentile(samples: &mut [f64], p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64) * p).floor() as usize;
    samples[idx.min(samples.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scaling_4_noisy_neighbor() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let quiet_tenant = TenantId::new();
    let noisy_tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // Provision quiet tenant + seed via storage.
    catalog.create_namespace(&quiet_tenant).await.unwrap();
    catalog
        .create_table(&quiet_tenant, &table, &schema())
        .await
        .unwrap();
    let q_batch = build_batch(0, QUIET_ROWS);
    let q_df = storage
        .write_batch(&quiet_tenant, &table, &part, &q_batch)
        .await
        .unwrap();
    {
        let meta = catalog.load_table(&quiet_tenant, &table).await.unwrap();
        catalog
            .append_data_files(
                &quiet_tenant,
                &table,
                meta.current_snapshot,
                vec![DataFileRef {
                    path: q_df.path.as_ref().to_string(),
                    size_bytes: q_df.size_bytes,
                    row_count: q_df.row_count,
                }],
            )
            .await
            .unwrap();
    }

    // Provision noisy tenant + seed.
    catalog.create_namespace(&noisy_tenant).await.unwrap();
    catalog
        .create_table(&noisy_tenant, &table, &schema())
        .await
        .unwrap();
    let n_batches = NOISY_ROWS / NOISY_BATCH;
    let mut noisy_files: Vec<DataFileRef> = Vec::with_capacity(n_batches);
    for b in 0..n_batches {
        let start = (b * NOISY_BATCH) as i64;
        let batch = build_batch(start, NOISY_BATCH);
        let df = storage
            .write_batch(&noisy_tenant, &table, &part, &batch)
            .await
            .unwrap();
        noisy_files.push(DataFileRef {
            path: df.path.as_ref().to_string(),
            size_bytes: df.size_bytes,
            row_count: df.row_count,
        });
    }
    {
        let meta = catalog.load_table(&noisy_tenant, &table).await.unwrap();
        catalog
            .append_data_files(&noisy_tenant, &table, meta.current_snapshot, noisy_files)
            .await
            .unwrap();
    }

    // Open engine sessions AFTER all seeding so DataFusion picks up files.
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
    });
    let quiet_sess = engine.open_session(quiet_tenant).await.unwrap();
    // Warm DataFusion once.
    let _ = quiet_sess
        .execute("SELECT id FROM events WHERE id = 1")
        .await
        .unwrap();

    // Phase A: baseline.
    let mut baseline: Vec<f64> = Vec::with_capacity(QUIET_QUERIES);
    for i in 0..QUIET_QUERIES {
        let id = (i as i64 * 37) % QUIET_ROWS as i64;
        let sql = format!("SELECT id FROM events WHERE id = {}", id);
        let started = Instant::now();
        let res = quiet_sess.execute(&sql).await.unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        if let ExecResult::Rows { batches, .. } = res {
            let mut hits = 0usize;
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                hits += arr.len();
            }
            assert!(hits >= 1, "expected hit on quiet tenant for id={id}");
        }
        baseline.push(elapsed_ms);
    }

    // Phase B: under load. Spawn 4 parallel noisy full-scans on a JoinSet.
    let stop = Arc::new(AtomicBool::new(false));
    let mut noisy_set: JoinSet<u64> = JoinSet::new();
    for _ in 0..NOISY_TASKS {
        let stop = stop.clone();
        let engine = engine.clone();
        noisy_set.spawn(async move {
            let sess = engine.open_session(noisy_tenant).await.unwrap();
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let res = sess.execute("SELECT id FROM events").await.unwrap();
                if let ExecResult::Rows { batches, .. } = res {
                    let _: usize = batches.iter().map(|b| b.num_rows()).sum();
                }
                count += 1;
            }
            count
        });
    }

    // Warm-up so the noisy tasks are actually running before we measure.
    tokio::time::sleep(Duration::from_millis(WARMUP_MS)).await;

    let mut under_load: Vec<f64> = Vec::with_capacity(QUIET_QUERIES);
    for i in 0..QUIET_QUERIES {
        let id = (i as i64 * 37) % QUIET_ROWS as i64;
        let sql = format!("SELECT id FROM events WHERE id = {}", id);
        let started = Instant::now();
        let _ = quiet_sess.execute(&sql).await.unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        under_load.push(elapsed_ms);
    }

    // Stop noisy tasks.
    stop.store(true, Ordering::Relaxed);
    let mut noisy_scans: u64 = 0;
    while let Some(r) = noisy_set.join_next().await {
        noisy_scans += r.unwrap_or(0);
    }

    let mut b_sorted = baseline.clone();
    let mut u_sorted = under_load.clone();
    let baseline_p50 = percentile(&mut b_sorted, 0.50);
    let baseline_p99 = percentile(&mut b_sorted, 0.99);
    let under_p50 = percentile(&mut u_sorted, 0.50);
    let under_p99 = percentile(&mut u_sorted, 0.99);

    let p50_ratio = under_p50 / baseline_p50.max(1e-9);
    let p99_ratio = under_p99 / baseline_p99.max(1e-9);

    println!("(noisy full scans completed during phase B: {})", noisy_scans);
    println!("{:>14} {:>10} {:>10}", "scenario", "p50_ms", "p99_ms");
    println!(
        "{:>14} {:>10.2} {:>10.2}",
        "baseline", baseline_p50, baseline_p99
    );
    println!(
        "{:>14} {:>10.2} {:>10.2}",
        "under_load", under_p50, under_p99
    );
    println!("{:>14} {:>10.2} {:>10.2}", "ratio", p50_ratio, p99_ratio);

    let pass = p99_ratio < BAR_P99_RATIO;
    println!(
        "[SCALING 4] noisy neighbor: under_load.p99/baseline.p99={:.2}x (bar <{}x) {}",
        p99_ratio,
        BAR_P99_RATIO,
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows = vec![
        json!({
            "scenario": "baseline",
            "p50_ms": baseline_p50,
            "p99_ms": baseline_p99,
        }),
        json!({
            "scenario": "under_load",
            "p50_ms": under_p50,
            "p99_ms": under_p99,
        }),
        json!({
            "scenario": "ratio",
            "p50_ms": p50_ratio,
            "p99_ms": p99_ratio,
        }),
    ];

    report_scaling(
        "noisy_neighbor",
        "Noisy-neighbor degradation",
        "A heavy noisy tenant doesn't crater a quiet tenant's p99 latency.",
        pass,
        AxisSpec {
            key: "scenario".into(),
            label: "scenario".into(),
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
        json_rows,
        Some(PrimaryMetric {
            label: "p99 ratio (under_load / baseline)".into(),
            value: p99_ratio,
            unit: "x".into(),
            bar: BarOp::lt(BAR_P99_RATIO),
        }),
    );

    if !pass {
        panic!(
            "FAIL: p99_ratio={p99_ratio:.2} (bar <{BAR_P99_RATIO}); baseline_p99={baseline_p99:.2}ms, under_load_p99={under_p99:.2}ms"
        );
    }
}
