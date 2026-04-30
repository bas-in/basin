//! Scaling test 2: data-size curve (scale-up).
//!
//! Claim: As one tenant's data grows from 100k to 10M rows:
//! - on-disk bytes scale linearly with row count (no nasty growth term)
//! - point-query latency stays bounded (does not degrade with table size)
//! - full-scan latency grows roughly linearly (the inevitable cost)
//!
//! This is the "scale up" half of the wedge — a single tenant getting big
//! shouldn't make their substrate fall apart.
//!
//! Bars:
//! - bytes_per_row at each scale within 30% of the smallest scale's value
//! - point_ms_p50 at 10M <= 5x of point_ms_p50 at 100k
//!
//! Notes:
//! - Writes go directly through `Storage::write_batch` to keep the curve clean
//!   (no SQL parsing overhead). The point query goes through `Storage::read`
//!   with a `Predicate::Eq` so we measure the substrate, not DataFusion.
//! - 100k row batches per Parquet file. At 10M rows that's 100 files; the
//!   storage layer prunes by file-level stats so a point query touches one
//!   file's footer + one row group.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

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

const SCALES: [usize; 3] = [100_000, 1_000_000, 10_000_000];
const BATCH_SIZE: usize = 100_000;
const BAR_BYTES_PER_ROW_TOLERANCE: f64 = 0.30; // within 30% of the smallest scale
const BAR_POINT_RATIO: f64 = 5.0; // point query at 10M <= 5x of 100k

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
    // 50-byte payload, varying with id so dictionary encoding doesn't collapse
    // it to one entry.
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
async fn scaling_2_data_size() {
    struct Row {
        rows: usize,
        disk_bytes: u64,
        bytes_per_row: f64,
        point_ms_p50: f64,
        full_scan_ms_p50: f64,
    }
    let mut report: Vec<Row> = Vec::new();

    for &n_rows in SCALES.iter() {
        // Fresh storage per scale so disk numbers reflect just this scale.
        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
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

        // Pick a known mid-table id for the point query.
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
            assert_eq!(hits, 1, "expected one hit for id={target_id} at n_rows={n_rows}, got {hits}");
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

    // Linearity bar.
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
        "[SCALING 2] data size: max_bpr_dev={:.1}% (bar <={:.0}%), point_ratio_10M_over_100k={:.2}x (bar <={}x) {}",
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

    report_scaling(
        "data_size",
        "Single-tenant data size scale-up",
        "Storage is linear in row count; point-query latency stays bounded.",
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
            label: "point query p50 growth (10M / 100k)".into(),
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
