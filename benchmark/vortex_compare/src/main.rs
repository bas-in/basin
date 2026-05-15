//! Parquet vs Vortex compression and scan-speed comparison.
//!
//! Generates the same synthetic audit-log dataset as
//! `tests/integration/tests/viability_compression_ratio.rs` (1M rows of
//! id/ts/actor/event/body), writes it once as ZSTD Parquet and once as a
//! Vortex file, then reports:
//!   - file sizes (bytes)
//!   - cold full-scan time (wall clock)
//!
//! Results are printed to stdout and written to
//! `benchmark/data/compare_vortex_parquet.json` relative to the workspace root.

#![allow(clippy::print_stdout)]

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use vortex_array::IntoArray;
use vortex_array::arrays::{ChunkedArray, StructArray, VarBinArray};
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSessionExt;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::{Buffer, ByteBufferMut};
use vortex_file::{
    OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder,
    register_default_encodings,
};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

const ROWS: usize = 1_000_000;
const CHUNK_SIZE: usize = 65_536; // ~64K rows per chunk

const ACTORS: [&str; 8] = [
    "alice", "bob", "carol", "dan", "erin", "frank", "grace", "heidi",
];
const EVENTS: [&str; 4] = ["login", "logout", "create_record", "update_record"];

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

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Utf8, false),
        Field::new("actor", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

/// Build one Arrow RecordBatch chunk starting at row `start`.
fn build_arrow_chunk(start: usize, count: usize, schema: Arc<Schema>) -> RecordBatch {
    let end = start + count;
    let ids: Int64Array = (start as i64..end as i64).collect();

    let ts_vec: Vec<String> = (start..end).map(ts_for).collect();
    let ts_arr: StringArray = ts_vec.iter().map(|s| Some(s.as_str())).collect();

    let actors: Vec<&'static str> = (start..end).map(|i| ACTORS[i % ACTORS.len()]).collect();
    let actor_arr: StringArray = actors.iter().map(|s| Some(*s)).collect();

    let events: Vec<&'static str> = (start..end).map(|i| EVENTS[i % EVENTS.len()]).collect();
    let event_arr: StringArray = events.iter().map(|s| Some(*s)).collect();

    let bodies: Vec<String> = (start..end).map(body_for).collect();
    let body_arr: StringArray = bodies.iter().map(|s| Some(s.as_str())).collect();

    RecordBatch::try_new(
        schema,
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

/// Write ZSTD Parquet to a file and return bytes written.
async fn write_parquet(path: &Path) -> u64 {
    let schema = arrow_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let file = tokio::fs::File::create(path).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut offset = 0;
    while offset < ROWS {
        let count = CHUNK_SIZE.min(ROWS - offset);
        let batch = build_arrow_chunk(offset, count, schema.clone());
        writer.write(&batch).await.unwrap();
        offset += count;
    }
    writer.close().await.unwrap();

    std::fs::metadata(path).unwrap().len()
}

/// Full scan of a Parquet file (read all rows). Returns elapsed nanos.
async fn scan_parquet(path: &Path) -> u64 {
    let file = tokio::fs::File::open(path).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    let mut stream = builder.build().unwrap();

    let start = Instant::now();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    let elapsed = start.elapsed().as_nanos() as u64;
    assert_eq!(rows, ROWS, "parquet scan row count mismatch: got {rows}");
    elapsed
}

fn build_vortex_session() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>()
        .with_tokio();
    register_default_encodings(&session);
    session
}

/// Write a Vortex file and return the byte count.
async fn write_vortex(path: &Path, session: &VortexSession) -> u64 {
    let num_chunks = (ROWS + CHUNK_SIZE - 1) / CHUNK_SIZE;
    let mut id_chunks = Vec::with_capacity(num_chunks);
    let mut ts_chunks = Vec::with_capacity(num_chunks);
    let mut actor_chunks = Vec::with_capacity(num_chunks);
    let mut event_chunks = Vec::with_capacity(num_chunks);
    let mut body_chunks = Vec::with_capacity(num_chunks);

    let mut offset = 0;
    while offset < ROWS {
        let count = CHUNK_SIZE.min(ROWS - offset);
        let end = offset + count;

        // id column: i64
        let ids: Vec<i64> = (offset as i64..end as i64).collect();
        id_chunks.push(Buffer::<i64>::from(ids).into_array());

        // ts column: varbin strings
        let ts_vec: Vec<String> = (offset..end).map(ts_for).collect();
        let ts_refs: Vec<&str> = ts_vec.iter().map(String::as_str).collect();
        ts_chunks.push(VarBinArray::from(ts_refs).into_array());

        // actor column
        let actors: Vec<&str> = (offset..end).map(|i| ACTORS[i % ACTORS.len()]).collect();
        actor_chunks.push(VarBinArray::from(actors).into_array());

        // event column
        let events: Vec<&str> = (offset..end).map(|i| EVENTS[i % EVENTS.len()]).collect();
        event_chunks.push(VarBinArray::from(events).into_array());

        // body column
        let bodies: Vec<String> = (offset..end).map(body_for).collect();
        let body_refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
        body_chunks.push(VarBinArray::from(body_refs).into_array());

        offset += count;
    }

    let id_col = ChunkedArray::from_iter(id_chunks).into_array();
    let ts_col = ChunkedArray::from_iter(ts_chunks).into_array();
    let actor_col = ChunkedArray::from_iter(actor_chunks).into_array();
    let event_col = ChunkedArray::from_iter(event_chunks).into_array();
    let body_col = ChunkedArray::from_iter(body_chunks).into_array();

    let struct_array = StructArray::from_fields(&[
        ("id", id_col),
        ("ts", ts_col),
        ("actor", actor_col),
        ("event", event_col),
        ("body", body_col),
    ])
    .unwrap();

    // Build the aggressive write strategy: BtrBlocks with Zstd (strings) + Pco (numerics).
    let compressor = BtrBlocksCompressorBuilder::default().with_compact();
    let strategy = WriteStrategyBuilder::default()
        .with_btrblocks_builder(compressor)
        .build();

    // Write to in-memory buffer.
    let mut buf = ByteBufferMut::empty();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut buf, struct_array.into_array().to_array_stream())
        .await
        .unwrap();

    // Persist to file.
    let bytes = buf.as_slice().to_vec();
    std::fs::write(path, &bytes).unwrap();
    bytes.len() as u64
}

/// Full scan of a Vortex file. Returns elapsed nanos.
async fn scan_vortex(path: &Path, session: &VortexSession) -> u64 {
    let start = Instant::now();
    let vf = session
        .open_options()
        .open_path(path)
        .await
        .unwrap();

    let stream = vf.scan().unwrap().into_array_stream().unwrap();
    futures::pin_mut!(stream);

    let mut rows = 0usize;
    while let Some(chunk) = stream.next().await {
        rows += chunk.unwrap().len();
    }
    let elapsed = start.elapsed().as_nanos() as u64;
    assert_eq!(rows, ROWS, "vortex scan row count mismatch: got {rows}");
    elapsed
}

/// Walk upward from CWD until we find the root Cargo.toml that lists workspace members.
/// We identify the root workspace by looking for a Cargo.toml that contains `members = [`
/// (i.e., not just `[workspace]` with no members, which is what standalone crates use).
fn repo_root() -> std::path::PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut candidate = cwd.as_path();
    loop {
        let cargo = candidate.join("Cargo.toml");
        if cargo.exists() {
            if let Ok(content) = std::fs::read_to_string(&cargo) {
                // A root workspace Cargo.toml has `members = [` while standalone crates
                // have `[workspace]` with no members array.
                if content.contains("members = [") {
                    return candidate.to_path_buf();
                }
            }
        }
        match candidate.parent() {
            Some(p) => candidate = p,
            None => return cwd,
        }
    }
}

#[tokio::main]
async fn main() {
    let tmp = {
        let dir = std::env::temp_dir().join(format!("vortex_compare_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    };
    let parquet_path = tmp.join("audit_log.parquet");
    let vortex_path = tmp.join("audit_log.vortex");

    println!("Generating {} rows of synthetic audit-log data...", ROWS);

    // --- Write Parquet ---
    let parquet_bytes = write_parquet(&parquet_path).await;
    println!("Parquet (ZSTD): {} bytes written", parquet_bytes);

    // --- Write Vortex ---
    let session = build_vortex_session();
    let vortex_bytes = write_vortex(&vortex_path, &session).await;
    println!("Vortex:         {} bytes written", vortex_bytes);

    let size_ratio = parquet_bytes as f64 / vortex_bytes as f64;
    println!(
        "Size  parquet/vortex = {:.3}x  (>1 means Vortex is smaller)",
        size_ratio
    );

    // --- Scan Parquet (cold) ---
    let parquet_ns = scan_parquet(&parquet_path).await;
    let parquet_ms = parquet_ns as f64 / 1_000_000.0;
    println!("Parquet full-scan: {:.1} ms", parquet_ms);

    // --- Scan Vortex (cold) ---
    let vortex_ns = scan_vortex(&vortex_path, &session).await;
    let vortex_ms = vortex_ns as f64 / 1_000_000.0;
    println!("Vortex full-scan:  {:.1} ms", vortex_ms);

    let scan_ratio = parquet_ms / vortex_ms;
    println!(
        "Scan  parquet/vortex = {:.3}x  (>1 means Vortex is faster)",
        scan_ratio
    );

    // --- Write JSON result ---
    let root = repo_root();
    let data_dir = root.join("benchmark").join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // NOTE: in the JSON schema "basin" is Parquet and "postgres" is Vortex since
    // CompareReport was designed for Basin-vs-Postgres. We document this in `note`.
    let report = serde_json::json!({
        "kind": "compare",
        "id": "vortex_parquet",
        "name": "Parquet (ZSTD) vs Vortex — audit-log 1M rows",
        "claim": "Vortex achieves competitive compression and fast full-scan vs ZSTD Parquet on audit-log data.",
        "available": true,
        "metrics": [
            {
                "label": "On-disk size",
                "basin": parquet_bytes as f64,
                "postgres": vortex_bytes as f64,
                "unit": "bytes",
                "better": if vortex_bytes <= parquet_bytes { "postgres" } else { "basin" },
                "ratio_text": format!("parquet / vortex = {:.2}x", size_ratio)
            },
            {
                "label": "Full-scan wall time",
                "basin": parquet_ms,
                "postgres": vortex_ms,
                "unit": "ms",
                "better": if vortex_ms <= parquet_ms { "postgres" } else { "basin" },
                "ratio_text": format!("parquet / vortex = {:.2}x", scan_ratio)
            }
        ],
        "details": {
            "rows": ROWS,
            "parquet_bytes": parquet_bytes,
            "vortex_bytes": vortex_bytes,
            "parquet_scan_ms": parquet_ms,
            "vortex_scan_ms": vortex_ms,
            "size_ratio_parquet_over_vortex": size_ratio,
            "scan_ratio_parquet_over_vortex": scan_ratio,
            "compression": "ZSTD (Parquet default level) vs Vortex sampling/BtrBlocks cascade",
            "dataset": "audit-log: id(i64), ts(utf8), actor(utf8/8-cardinality), event(utf8/4-cardinality), body(utf8/json-ish~120B)"
        },
        "note": "JSON key 'basin'=Parquet, 'postgres'=Vortex (schema reuse from Postgres compare card). Standalone binary: cargo run --manifest-path benchmark/vortex_compare/Cargo.toml --release",
        "generated_at": format!("@{}", now_secs)
    });

    let json_path = data_dir.join("compare_vortex_parquet.json");
    let tmp_path = data_dir.join("compare_vortex_parquet.json.tmp");
    let json_bytes = serde_json::to_vec_pretty(&report).unwrap();
    std::fs::write(&tmp_path, &json_bytes).unwrap();
    std::fs::rename(&tmp_path, &json_path).unwrap();

    println!("\nJSON written to: {}", json_path.display());
    println!("\nSummary:");
    println!(
        "  Parquet (ZSTD): {:>10} bytes   {:7.1} ms full-scan",
        parquet_bytes, parquet_ms
    );
    println!(
        "  Vortex:         {:>10} bytes   {:7.1} ms full-scan",
        vortex_bytes, vortex_ms
    );
    println!("  Size  parquet/vortex = {:.2}x", size_ratio);
    println!("  Scan  parquet/vortex = {:.2}x", scan_ratio);

    // Clean up temp files.
    let _ = std::fs::remove_dir_all(&tmp);
}
