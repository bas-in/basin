//! S3 port of `scaling_data_size.rs`.
//!
//! Scaled down to [10K, 100K, 1M] (vs LocalFS [100K, 1M, 10M]) to keep the
//! cloud run under a few minutes — see brief. The shape is the same:
//! linear bytes-per-row, bounded point-query latency.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

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

const TEST_NAME: &str = "s3_scaling_data_size";
// Scaled down vs LocalFS: [100K, 1M, 10M] -> [10K, 100K, 1M].
const SCALES: [usize; 3] = [10_000, 100_000, 1_000_000];
const BATCH_SIZE: usize = 10_000;
const BAR_BYTES_PER_ROW_TOLERANCE: f64 = 0.30;
const BAR_POINT_RATIO: f64 = 5.0;

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

fn median_ms(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn s3_scaling_data_size() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    // One run prefix shared across all scales — each scale uses its own
    // sub-prefix below so disk numbers are isolated. CleanupOnDrop wipes
    // everything under run_prefix at exit.
    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    struct Row {
        rows: usize,
        disk_bytes: u64,
        bytes_per_row: f64,
        point_ms_p50: f64,
        full_scan_ms_p50: f64,
    }
    let mut report: Vec<Row> = Vec::new();

    for (scale_idx, &n_rows) in SCALES.iter().enumerate() {
        // Sub-prefix per scale so each scale's writes are isolated for
        // bytes-per-row measurement.
        let scale_prefix = format!("{run_prefix}/scale_{scale_idx}");
        let storage = Storage::new(StorageConfig {
            object_store: object_store.clone(),
            root_prefix: Some(ObjectPath::from(scale_prefix.as_str())),
        });

        let tenant = TenantId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();

        let n_batches = n_rows / BATCH_SIZE;
        let mut total_disk: u64 = 0;
        for b in 0..n_batches {
            let start = (b * BATCH_SIZE) as i64;
            let batch = build_batch(start, BATCH_SIZE);
            let df = storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap();
            total_disk += df.size_bytes;
        }

        let target_id: i64 = (n_rows as i64) / 2 + 7;

        // Point query: 5 runs, median.
        let mut point_samples: Vec<f64> = Vec::with_capacity(5);
        for _ in 0..5 {
            let opts = ReadOptions {
                filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(target_id))],
                ..Default::default()
            };
            let started = Instant::now();
            let mut stream = storage.read(&tenant, &table, opts).await.unwrap();
            let mut hits = 0usize;
            while let Some(b) = stream.next().await {
                hits += b.unwrap().num_rows();
            }
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(
                hits, 1,
                "expected one hit for id={target_id} at n_rows={n_rows}, got {hits}"
            );
            point_samples.push(elapsed_ms);
        }

        // Full scan: 3 runs, median.
        let mut full_samples: Vec<f64> = Vec::with_capacity(3);
        for _ in 0..3 {
            let started = Instant::now();
            let mut stream = storage
                .read(&tenant, &table, ReadOptions::default())
                .await
                .unwrap();
            let mut total = 0usize;
            while let Some(b) = stream.next().await {
                total += b.unwrap().num_rows();
            }
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(total, n_rows, "full scan at {n_rows} returned {total}");
            full_samples.push(elapsed_ms);
        }

        let bytes_per_row = total_disk as f64 / n_rows as f64;
        report.push(Row {
            rows: n_rows,
            disk_bytes: total_disk,
            bytes_per_row,
            point_ms_p50: median_ms(&point_samples),
            full_scan_ms_p50: median_ms(&full_samples),
        });
    }

    println!(
        "{:>12} {:>10} {:>15} {:>15} {:>18}",
        "rows", "disk_MiB", "bytes_per_row", "point_ms_p50", "full_scan_ms_p50"
    );
    for r in &report {
        println!(
            "{:>12} {:>10.2} {:>15.2} {:>15.2} {:>18.2}",
            r.rows,
            r.disk_bytes as f64 / (1024.0 * 1024.0),
            r.bytes_per_row,
            r.point_ms_p50,
            r.full_scan_ms_p50,
        );
    }

    let smallest_bpr = report.first().unwrap().bytes_per_row;
    let mut bpr_pass = true;
    let mut max_dev = 0.0_f64;
    for r in &report {
        let dev = (r.bytes_per_row - smallest_bpr).abs() / smallest_bpr;
        if dev > max_dev {
            max_dev = dev;
        }
        if dev > BAR_BYTES_PER_ROW_TOLERANCE {
            bpr_pass = false;
        }
    }

    let point_ratio =
        report.last().unwrap().point_ms_p50 / report.first().unwrap().point_ms_p50.max(1e-9);
    let point_pass = point_ratio <= BAR_POINT_RATIO;

    let pass = bpr_pass && point_pass;

    println!(
        "[S3 scaling_data_size] max_bpr_dev={:.1}% (bar <={:.0}%), point_ratio={:.2}x (bar <={}x) {}",
        max_dev * 100.0,
        BAR_BYTES_PER_ROW_TOLERANCE * 100.0,
        point_ratio,
        BAR_POINT_RATIO,
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows: Vec<serde_json::Value> = report
        .iter()
        .map(|r| {
            json!({
                "rows": r.rows,
                "disk_mib": r.disk_bytes as f64 / (1024.0 * 1024.0),
                "bytes_per_row": r.bytes_per_row,
                "point_ms_p50": r.point_ms_p50,
                "scan_ms_p50": r.full_scan_ms_p50,
            })
        })
        .collect();

    report_real_scaling(
        "data_size",
        "Single-tenant data size scale-up (real S3)",
        "On real S3, storage is linear in row count and point-query latency stays bounded.",
        pass,
        AxisSpec {
            key: "rows".into(),
            label: "row count".into(),
        },
        vec![
            SeriesSpec {
                key: "disk_mib".into(),
                label: "Disk".into(),
                unit: Some("MiB".into()),
            },
            SeriesSpec {
                key: "bytes_per_row".into(),
                label: "Bytes / row".into(),
                unit: Some("B".into()),
            },
            SeriesSpec {
                key: "point_ms_p50".into(),
                label: "Point query p50".into(),
                unit: Some("ms".into()),
            },
            SeriesSpec {
                key: "scan_ms_p50".into(),
                label: "Full scan p50".into(),
                unit: Some("ms".into()),
            },
        ],
        json_rows,
        Some(PrimaryMetric {
            label: "point query p50 growth (1M / 10K)".into(),
            value: point_ratio,
            unit: "x".into(),
            bar: BarOp::lt(BAR_POINT_RATIO),
        }),
    );

    if !pass {
        panic!(
            "FAIL: bpr_pass={bpr_pass} (max_dev={max_dev:.2}, bar {BAR_BYTES_PER_ROW_TOLERANCE}), point_pass={point_pass} (ratio={point_ratio:.2}, bar {BAR_POINT_RATIO})"
        );
    }
}
