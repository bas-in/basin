//! S3 port of `scaling_concurrency.rs`.
//!
//! S3 latency dominates per-query time, so concurrent reads are network-bound,
//! not CPU-bound. We expect concurrency to scale BETTER than on LocalFS
//! (where C=64 hits CPU contention before HW fan-out).
//!
//! Bar: peak(total_qps) / total_qps(C=1) >= 3.5x — same shape as LocalFS, but
//! we explicitly note in the dashboard text that this is a network-bound
//! scaling story, not a CPU one.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio::task::JoinSet;

const TEST_NAME: &str = "s3_scaling_concurrency";
const ROWS: usize = 1_000_000;
const CONCURRENCIES: [usize; 4] = [1, 4, 16, 64];
const DEADLINE_SECS: u64 = 3;
const BAR_FANOUT_RATIO: f64 = 3.5;

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
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn s3_scaling_concurrency() {
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
    });
    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

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
    let peak = report
        .iter()
        .map(|r| r.total_qps)
        .fold(f64::NEG_INFINITY, f64::max);
    let peak_c = report
        .iter()
        .find(|r| (r.total_qps - peak).abs() < 1e-9)
        .map(|r| r.concurrency)
        .unwrap_or(0);
    let ratio = peak / qps1.max(1e-9);
    let pass = ratio >= BAR_FANOUT_RATIO;

    println!(
        "[S3 scaling_concurrency] peak={:.1} qps at C={}, baseline={:.1} qps at C=1, speedup={:.2}x (bar >={}x) {}",
        peak,
        peak_c,
        qps1,
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

    report_real_scaling(
        "concurrency",
        "Concurrent reader fan-out (real S3)",
        "On real S3, concurrent readers scale: per-query latency is network-bound, so adding parallel readers compounds throughput.",
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
            label: "peak speed-up vs C=1".into(),
            value: ratio,
            unit: "x".into(),
            bar: BarOp::ge(BAR_FANOUT_RATIO),
        }),
    );

    if !pass {
        panic!(
            "FAIL: peak={peak:.1} (at C={peak_c}) / total_qps(1)={qps1:.1} = {ratio:.2}x, bar {BAR_FANOUT_RATIO}x"
        );
    }
}
