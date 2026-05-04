//! S3 port of `viability_compression_ratio.rs`.
//!
//! Same 1M-row audit-log shape, same CSV-vs-Parquet ratio, but the Parquet
//! PUT lands on the configured S3-compatible service (local MinIO or real
//! cloud). The CSV side is still written to a tempdir for byte-counting —
//! we only need a baseline byte budget, not a roundtrip.
//!
//! Skips cleanly when `[s3]` is missing from `.basin-test.toml`.

#![allow(clippy::print_stdout)]

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tempfile::TempDir;

const TEST_NAME: &str = "s3_compression_ratio";
const ROWS: usize = 1_000_000;

const ACTORS: [&str; 8] = [
    "alice", "bob", "carol", "dan", "erin", "frank", "grace", "heidi",
];
const EVENTS: [&str; 4] = [
    "login", "logout", "create_record", "update_record",
];

fn body_for(i: usize) -> String {
    format!(
        "{{\"req\":\"abc-{:08}\",\"status\":200,\"ms\":{},\"path\":\"/api/v1/foo\"}}",
        i,
        47 + (i % 13)
    )
}

fn ts_for(i: usize) -> String {
    let total_ms = i as u64;
    let secs = total_ms / 1000;
    let ms = total_ms % 1000;
    let day = secs / 86_400;
    let secs_in_day = secs % 86_400;
    let hour = secs_in_day / 3600;
    let minute = (secs_in_day % 3600) / 60;
    let second = secs_in_day % 60;
    let day_of_jan = 1 + day;
    format!(
        "2026-01-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        day_of_jan, hour, minute, second, ms
    )
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Utf8, false),
        Field::new("actor", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn build_batch() -> RecordBatch {
    let ids: Int64Array = (0..ROWS as i64).collect();

    let mut ts: Vec<String> = Vec::with_capacity(ROWS);
    let mut actors: Vec<&'static str> = Vec::with_capacity(ROWS);
    let mut events: Vec<&'static str> = Vec::with_capacity(ROWS);
    let mut bodies: Vec<String> = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        ts.push(ts_for(i));
        actors.push(ACTORS[i % ACTORS.len()]);
        events.push(EVENTS[i % EVENTS.len()]);
        bodies.push(body_for(i));
    }

    let ts_arr: StringArray = ts.iter().map(|s| Some(s.as_str())).collect();
    let actor_arr: StringArray = actors.iter().map(|s| Some(*s)).collect();
    let event_arr: StringArray = events.iter().map(|s| Some(*s)).collect();
    let body_arr: StringArray = bodies.iter().map(|s| Some(s.as_str())).collect();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(ids),
            Arc::new(ts_arr),
            Arc::new(actor_arr),
            Arc::new(event_arr),
            Arc::new(body_arr),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn s3_compression_ratio() {
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

    let batch = build_batch();
    let df = storage
        .write_batch(&tenant, &table, &part, &batch)
        .await
        .unwrap();
    let parquet_bytes = df.size_bytes;

    // CSV writer to a tempdir for byte counting (not stored in S3 — we only
    // need a baseline).
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("rows.csv");
    let csv_bytes = {
        let f = File::create(&csv_path).unwrap();
        let mut w = BufWriter::new(f);
        writeln!(w, "id,ts,actor,event,body").unwrap();
        for i in 0..ROWS {
            let body = body_for(i);
            let body_escaped = body.replace('"', "\"\"");
            writeln!(
                w,
                "{},{},{},{},\"{}\"",
                i,
                ts_for(i),
                ACTORS[i % ACTORS.len()],
                EVENTS[i % EVENTS.len()],
                body_escaped
            )
            .unwrap();
        }
        w.flush().unwrap();
        std::fs::metadata(&csv_path).unwrap().len()
    };

    let ratio = csv_bytes as f64 / parquet_bytes as f64;
    let pass = ratio >= 10.0;
    println!(
        "[S3 compression_ratio] parquet={} bytes (s3), csv={} bytes, ratio=csv/parquet={:.2}x (bar >=10x) {}",
        parquet_bytes,
        csv_bytes,
        ratio,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "compression_ratio",
        "Audit-log Parquet vs CSV (real S3)",
        "Audit-log data is at least 10x smaller than CSV when stored on real S3.",
        pass,
        PrimaryMetric {
            label: "ratio (csv / parquet)".into(),
            value: ratio,
            unit: "x".into(),
            bar: BarOp::ge(10.0),
        },
        json!({
            "parquet_bytes": parquet_bytes,
            "csv_bytes": csv_bytes,
            "rows": ROWS,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "ratio {ratio:.2}x below 10x bar (parquet={parquet_bytes}, csv={csv_bytes})"
    );
}
