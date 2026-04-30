//! Scaling test 3: concurrent reader fan-out.
//!
//! Claim: Aggregate read throughput scales as more concurrent readers are
//! added. If contention (mutex, single-threaded executor, IO serialization)
//! dominates, this test catches it — adding more cores wouldn't help.
//!
//! Setup: one tenant, one table, 1M rows in a single Parquet file.
//! For C in [1, 4, 16, 64], spawn C tasks. Each task runs random-id point
//! queries on a shared deadline (3 seconds). At deadline they return their
//! count. total_qps = sum / 3.
//!
//! Bar: total_qps(64) >= 4 * total_qps(1). The bar accepts that a single
//! core can't 64x; it just demands real fan-out, not pure queueing. If
//! the bar fails honestly (single-core saturation at C=4), surface it —
//! the brief explicitly says do not adjust.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{
    report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;

const ROWS: usize = 1_000_000;
const CONCURRENCIES: [usize; 4] = [1, 4, 16, 64];
const DEADLINE_SECS: u64 = 3;
const BAR_FANOUT_RATIO: f64 = 4.0; // total_qps(64) / total_qps(1)

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("payload-{:040}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

fn next_u64(state: &mut u64) -> u64 {
    // SplitMix64 — same PRNG used in the viability suite.
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scaling_3_concurrency() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // Seed: one big Parquet file with 1M rows.
    let batch = build_batch(0, ROWS);
    storage
        .write_batch(&tenant, &table, &part, &batch)
        .await
        .unwrap();

    struct RowOut {
        concurrency: usize,
        total_qps: f64,
        per_task_qps: f64,
        median_latency_us: f64,
    }
    let mut report: Vec<RowOut> = Vec::new();

    for &c in CONCURRENCIES.iter() {
        let storage = storage.clone();
        let table = table.clone();
        let deadline = Instant::now() + Duration::from_secs(DEADLINE_SECS);

        // Sampler task records latencies for the first ~256 queries it runs.
        let median_us = Arc::new(AtomicU64::new(0));

        let mut set: JoinSet<u64> = JoinSet::new();
        for task_idx in 0..c {
            let storage = storage.clone();
            let table = table.clone();
            let median_us = median_us.clone();
            let is_sampler = task_idx == 0;
            set.spawn(async move {
                let mut rng_state: u64 =
                    (task_idx as u64).wrapping_mul(0xA5A5A5A5).wrapping_add(1);
                let mut count: u64 = 0;
                let mut samples: Vec<u64> = Vec::with_capacity(if is_sampler { 256 } else { 0 });
                while Instant::now() < deadline {
                    let id = (next_u64(&mut rng_state) % ROWS as u64) as i64;
                    let opts = ReadOptions {
                        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(id))],
                        ..Default::default()
                    };
                    let q_start = Instant::now();
                    let mut stream = storage.read(&tenant, &table, opts).await.unwrap();
                    while let Some(b) = stream.next().await {
                        let _ = b.unwrap();
                    }
                    let q_us = q_start.elapsed().as_micros() as u64;
                    if is_sampler && samples.len() < 256 {
                        samples.push(q_us);
                    }
                    count += 1;
                }
                if is_sampler && !samples.is_empty() {
                    samples.sort_unstable();
                    median_us.store(samples[samples.len() / 2], Ordering::Relaxed);
                }
                count
            });
        }

        let mut total: u64 = 0;
        while let Some(r) = set.join_next().await {
            total += r.unwrap();
        }
        let total_qps = total as f64 / DEADLINE_SECS as f64;
        let per_task_qps = total_qps / c as f64;
        let med_us = median_us.load(Ordering::Relaxed) as f64;

        report.push(RowOut {
            concurrency: c,
            total_qps,
            per_task_qps,
            median_latency_us: med_us,
        });
    }

    println!(
        "{:>12} {:>12} {:>14} {:>20}",
        "concurrency", "total_qps", "per_task_qps", "median_latency_us"
    );
    for r in &report {
        println!(
            "{:>12} {:>12.1} {:>14.1} {:>20.1}",
            r.concurrency, r.total_qps, r.per_task_qps, r.median_latency_us
        );
    }

    let qps1 = report.first().unwrap().total_qps;
    let qps64 = report.last().unwrap().total_qps;
    let ratio = qps64 / qps1.max(1e-9);
    let pass = ratio >= BAR_FANOUT_RATIO;

    println!(
        "[SCALING 3] concurrency: total_qps(64)/total_qps(1)={:.2}x (bar >={}x) {}",
        ratio,
        BAR_FANOUT_RATIO,
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows: Vec<serde_json::Value> = report
        .iter()
        .map(|r| {
            json!({
                "concurrency": r.concurrency,
                "total_qps": r.total_qps,
                "per_task_qps": r.per_task_qps,
                "median_latency_us": r.median_latency_us,
            })
        })
        .collect();

    report_scaling(
        "concurrency",
        "Concurrent reader fan-out",
        "Read throughput scales as more concurrent readers are added.",
        pass,
        AxisSpec {
            key: "concurrency".into(),
            label: "concurrent readers".into(),
        },
        vec![
            SeriesSpec {
                key: "total_qps".into(),
                label: "Total QPS".into(),
                unit: Some("qps".into()),
            },
            SeriesSpec {
                key: "per_task_qps".into(),
                label: "Per-task QPS".into(),
                unit: Some("qps".into()),
            },
            SeriesSpec {
                key: "median_latency_us".into(),
                label: "Median latency".into(),
                unit: Some("us".into()),
            },
        ],
        json_rows,
        Some(PrimaryMetric {
            label: "total_qps speed-up (64 / 1)".into(),
            value: ratio,
            unit: "x".into(),
            bar: BarOp::ge(BAR_FANOUT_RATIO),
        }),
    );

    if !pass {
        panic!(
            "FAIL: total_qps(64)={qps64:.1} / total_qps(1)={qps1:.1} = {ratio:.2}x, bar {BAR_FANOUT_RATIO}x"
        );
    }
}
