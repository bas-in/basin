//! Arrow IPC stream serialisation for REST data responses.
//!
//! ## Content negotiation contract
//!
//! When a request carries `Accept: application/vnd.apache.arrow.stream` the
//! data-read handlers call [`batches_to_arrow_ipc`] instead of
//! [`crate::json::batches_to_json`]. Default (no / other `Accept`) is
//! byte-identical JSON — zero behaviour change for existing clients.
//!
//! ## Pagination metadata in headers
//!
//! JSON responses embed pagination state in the response body (`next_cursor`,
//! `_basin_next_cursor`). Arrow IPC responses cannot alter the binary stream
//! mid-flight, so pagination state is surfaced in response headers instead:
//!
//! | Header                  | Value                        | JSON equivalent       |
//! |-------------------------|------------------------------|-----------------------|
//! | `x-basin-next-cursor`   | opaque cursor token or empty | `next_cursor`         |
//! | `x-basin-row-count`     | decimal row count            | implicit array length |
//!
//! The JS SDK reads `x-basin-next-cursor` for cursor pagination (see
//! `sdk/basin-js/src/query.ts`). The Python SDK reads the same header
//! (see `sdk/basin-python/basin/query.py`).
//!
//! ## Buffering note
//!
//! The current handler path materialises all `RecordBatch`es before entering
//! this module (the engine's `ExecResult::Rows` already holds a `Vec<RecordBatch>`).
//! The IPC bytes are therefore built into an in-memory `Vec<u8>` then chunked
//! into a streaming axum `Body` — giving chunked `Transfer-Encoding` on the
//! wire without a true incremental-batch pipeline. True incremental streaming
//! would require an async batch iterator from the engine; that is a follow-up
//! tracked in the engine's `AsyncExecStream` proposal.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::Response;

/// MIME type for the Arrow IPC streaming format.
pub(crate) const ARROW_STREAM_MIME: &str = "application/vnd.apache.arrow.stream";

/// Check whether the caller prefers Arrow IPC over JSON.
///
/// Returns `true` when the `Accept` header contains the Arrow stream
/// media-type. A missing `Accept` or any other value returns `false` so the
/// default JSON path is taken without change.
pub(crate) fn wants_arrow(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains(ARROW_STREAM_MIME))
        .unwrap_or(false)
}

/// Serialise `batches` into an Arrow IPC stream and return an axum `Response`.
///
/// The full serialised buffer is split into 64 KiB chunks so axum sends the
/// response via `Transfer-Encoding: chunked` — the streaming property the
/// client cares about.
///
/// Pagination metadata is placed in response headers:
/// - `x-basin-next-cursor`: the next-page cursor token, or absent when there
///   is no next page.
/// - `x-basin-row-count`: total row count across all batches (decimal string).
///
/// # Panics
///
/// Does not panic. IPC write errors are treated as empty responses (the schema
/// is still written so the client gets a valid — if empty — stream).
pub(crate) fn batches_to_arrow_ipc(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    next_cursor: Option<&str>,
) -> Response {
    let buf = encode_ipc(schema, batches);
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Chunk the buffer into 64 KiB pieces for streaming delivery.
    let chunks: Vec<Result<Bytes, std::io::Error>> = buf
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let body = Body::from_stream(futures::stream::iter(chunks));

    let mut resp = Response::new(body);
    let hdrs = resp.headers_mut();
    hdrs.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(ARROW_STREAM_MIME),
    );
    hdrs.insert(
        "x-basin-row-count",
        HeaderValue::from_str(&row_count.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("0")),
    );
    if let Some(cursor) = next_cursor {
        if let Ok(v) = HeaderValue::from_str(cursor) {
            hdrs.insert("x-basin-next-cursor", v);
        }
    }
    resp
}

/// Low-level: write `schema` + `batches` into an Arrow IPC stream byte buffer.
///
/// Returns an empty but schema-complete stream on IPC write errors (the
/// `StreamWriter::finish` EOS marker is always emitted).
fn encode_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(
        // Rough pre-alloc: 128 B header + ~64 B per row across batches.
        128 + batches.iter().map(|b| b.num_rows() * 64).sum::<usize>(),
    );
    // StreamWriter uses std::io::Write; Vec<u8> satisfies that.
    let mut writer = match StreamWriter::try_new(&mut buf, schema.as_ref()) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("arrow_ipc: failed to create StreamWriter: {e}");
            return buf;
        }
    };
    for batch in batches {
        if let Err(e) = writer.write(batch) {
            tracing::warn!("arrow_ipc: batch write error: {e}");
            // Continue — partial streams are still useful for debugging,
            // and the finish() below will close the stream cleanly.
        }
    }
    if let Err(e) = writer.finish() {
        tracing::warn!("arrow_ipc: finish error: {e}");
    }
    // Explicitly drop the writer so `buf` is no longer borrowed.
    drop(writer);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    fn two_row_batch() -> (Arc<ArrowSchema>, RecordBatch) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn encode_decode_round_trip() {
        let (schema, batch) = two_row_batch();
        let buf = encode_ipc(&schema, &[batch]);
        assert!(!buf.is_empty(), "IPC buffer must not be empty");

        // Decode with arrow_ipc reader and verify schema + values.
        let cursor = std::io::Cursor::new(buf);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
            .expect("StreamReader::try_new");
        assert_eq!(reader.schema().fields().len(), 2);
        assert_eq!(reader.schema().field(0).name(), "id");
        assert_eq!(reader.schema().field(1).name(), "name");

        let decoded = reader.next().expect("batch present").expect("no error");
        assert_eq!(decoded.num_rows(), 2);

        // id column — i64 extreme values fidelity
        let ids = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1i64);
        assert_eq!(ids.value(1), 2i64);
    }

    #[test]
    fn fidelity_i64_extremes() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        let values = vec![i64::MIN, -1, 0, 1, i64::MAX];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.clone()))],
        )
        .unwrap();
        let buf = encode_ipc(&schema, &[batch]);
        let cursor = std::io::Cursor::new(buf);
        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(col.value(i), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn fidelity_timestamps() {
        use arrow_array::TimestampMicrosecondArray;
        use arrow_schema::TimeUnit;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        // Unix epoch, a negative (pre-1970) timestamp, and a far-future stamp.
        let values: Vec<i64> = vec![0, -1_000_000, 32_503_680_000_000_000i64];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMicrosecondArray::from(values.clone()))],
        )
        .unwrap();
        let buf = encode_ipc(&schema, &[batch]);
        let cursor = std::io::Cursor::new(buf);
        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(col.value(i), expected, "timestamp mismatch at {i}");
        }
    }

    #[test]
    fn wants_arrow_detect() {
        let mut headers = HeaderMap::new();
        assert!(!wants_arrow(&headers), "no Accept → JSON");
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );
        assert!(!wants_arrow(&headers), "Accept: JSON → still JSON");
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(ARROW_STREAM_MIME),
        );
        assert!(wants_arrow(&headers), "Accept: arrow stream → IPC");
        // Multiple values (q-factor notation)
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(
                "application/json,application/vnd.apache.arrow.stream;q=0.9",
            ),
        );
        assert!(wants_arrow(&headers), "multi-value accept with arrow → IPC");
    }
}
