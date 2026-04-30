//! Viability test 1: compression ratio.
//!
//! Claim: Basin's Parquet substrate stores audit-log-shaped data >= 10x
//! smaller than the same data as CSV. CSV is a conservative lower bound for
//! any row-store engine; if Basin can't beat that by an order of magnitude on
//! audit-log-shaped data, dictionary + RLE encoding aren't paying off.
//!
//! Setup: 1_000_000 rows of (id, ts, actor, event, body) where actor and
//! event come from small fixed string sets (so dictionary encoding wins big)
//! and body is JSON-flavored with repeated keys.

#![allow(clippy::print_stdout)]

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::dashboard::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const ROWS: usize = 1_000_000;

const ACTORS: [&str; 8] = [
    "alice", "bob", "carol", "dan", "erin", "frank", "grace", "heidi",
];
const EVENTS: [&str; 4] = [
    "login", "logout", "create_record", "update_record",
];

fn body_for(i: usize) -> String {
    // ~120 bytes, varying digits keep entropy non-zero so we don't accidentally
    // hand Parquet a degenerate all-equal column.
    format!(
        "{{\"req\":\"abc-{:08}\",\"status\":200,\"ms\":{},\"path\":\"/api/v1/foo\"}}",
        i,
        47 + (i % 13)
    )
}

fn ts_for(i: usize) -> String {
    // Base 2026-01-01T00:00:00Z + i milliseconds, formatted RFC3339 to ms.
    let total_ms = i as u64;
    let secs = total_ms / 1000;
    let ms = total_ms % 1000;
    let day = secs / 86_400;
    let secs_in_day = secs % 86_400;
    let hour = secs_in_day / 3600;
    let minute = (secs_in_day % 3600) / 60;
    let second = secs_in_day % 60;
    // Keep January for the whole 1M-ms span (1M ms = ~17 minutes), so we don't
    // need calendar math.
    let day_of_jan = 1 + day; // day 0 -> 2026-01-01
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
async fn viability_1_compression_ratio() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
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

    // Hand-rolled CSV writer. Identical row content, identical column order.
    let csv_path = dir.path().join("rows.csv");
    let csv_bytes = {
        let f = File::create(&csv_path).unwrap();
        let mut w = BufWriter::new(f);
        // Header — 5 columns, comma-joined, newline-terminated. Matches the
        // shape any naive CSV consumer would expect.
        writeln!(w, "id,ts,actor,event,body").unwrap();
        for i in 0..ROWS {
            // body contains commas and quotes, so we wrap it in double quotes
            // and escape internal quotes with "" — RFC4180 minimal handling.
            // Content is JSON-ish but no embedded double quotes other than the
            // JSON ones, which we double-escape.
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
        "[VIABILITY 1] compression: parquet={} bytes, csv={} bytes, ratio=csv/parquet={:.2}x (bar >=10x) {}",
        parquet_bytes,
        csv_bytes,
        ratio,
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "compression_ratio",
        "Audit-log Parquet vs CSV",
        "Audit-log data is at least 10x smaller than CSV.",
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
        }),
    );

    assert!(
        pass,
        "ratio {ratio:.2}x below 10x bar (parquet={parquet_bytes}, csv={csv_bytes})"
    );
}
