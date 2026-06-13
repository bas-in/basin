//! Native Arrow IPC transport (feature `arrow`).
//!
//! Sends `Accept: application/vnd.apache.arrow.stream` on a query GET. When the
//! server replies with an Arrow IPC stream (matching `Content-Type`), the bytes
//! are decoded natively into [`arrow_array::RecordBatch`] values — zero JSON
//! round-trip, full i64 / timestamp fidelity. When the server returns JSON (an
//! older server without IPC support), client-side JSON-to-Arrow conversion is
//! *not* attempted here; instead the [`ArrowResult::JsonFallback`] variant
//! carries the parsed JSON rows so the caller can handle them.
//!
//! Pagination state for IPC responses lives in response headers (verified vs
//! `crates/basin-rest/src/arrow_ipc.rs`):
//! - `x-basin-next-cursor` — opaque cursor token (absent when no next page)
//! - `x-basin-row-count`   — decimal total row count across all IPC batches

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use reqwest::Method;

use crate::error::BasinError;
use crate::query::QueryBuilder;

/// MIME type for the Arrow IPC streaming format (mirrors the server constant
/// `ARROW_STREAM_MIME` in `crates/basin-rest/src/arrow_ipc.rs`).
pub const ARROW_STREAM_MIME: &str = "application/vnd.apache.arrow.stream";

/// The result of an Arrow-accepting query.
#[derive(Debug)]
pub enum ArrowResult {
    /// The server returned a native Arrow IPC stream, decoded into batches.
    Ipc {
        /// Decoded record batches (may be empty).
        batches: Vec<RecordBatch>,
        /// `x-basin-next-cursor` response header, when present.
        next_cursor: Option<String>,
        /// `x-basin-row-count` response header parsed as a count, when present.
        row_count: Option<usize>,
    },
    /// The server returned JSON (no IPC support); raw rows are passed through
    /// for the caller to convert.
    JsonFallback {
        /// Parsed JSON rows.
        rows: Vec<serde_json::Value>,
        /// `next_cursor` from the JSON body, when present.
        next_cursor: Option<String>,
    },
}

/// Extension trait adding Arrow IPC execution to [`QueryBuilder`].
///
/// ```no_run
/// # use basin::Client;
/// # use basin::arrow::ArrowQuery;
/// # async fn ex(client: Client) -> Result<(), basin::BasinError> {
/// let result = client.table("events").select("*").limit(1000).to_arrow().await?;
/// # let _ = result;
/// # Ok(())
/// # }
/// ```
#[allow(async_fn_in_trait)]
pub trait ArrowQuery {
    /// Run the query requesting Arrow IPC, decoding natively or falling back to
    /// JSON rows.
    async fn to_arrow(self) -> Result<ArrowResult, BasinError>;
}

impl ArrowQuery for QueryBuilder {
    async fn to_arrow(self) -> Result<ArrowResult, BasinError> {
        let transport = self.transport_ref().clone();
        let path = self.arrow_path();
        let query = self.arrow_query().to_vec();

        let headers = vec![("accept".to_string(), ARROW_STREAM_MIME.to_string())];
        let resp = transport
            .request(Method::GET, &path, &query, None, None, &headers, true)
            .await?;

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let next_cursor = resp
            .headers()
            .get("x-basin-next-cursor")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let row_count = resp
            .headers()
            .get("x-basin-row-count")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        if content_type.contains(ARROW_STREAM_MIME) {
            let bytes = resp.bytes().await.map_err(BasinError::from)?;
            let batches = decode_ipc(&bytes)?;
            Ok(ArrowResult::Ipc {
                batches,
                next_cursor,
                row_count,
            })
        } else {
            // JSON fallback.
            let text = resp.text().await.map_err(BasinError::from)?;
            let body: serde_json::Value = if text.is_empty() {
                serde_json::Value::Array(vec![])
            } else {
                serde_json::from_str(&text).map_err(|e| BasinError::Decode(e.to_string()))?
            };
            let (rows, json_cursor) = match body {
                serde_json::Value::Array(a) => (a, None),
                serde_json::Value::Object(map) => {
                    let rows = match map.get("rows") {
                        Some(serde_json::Value::Array(a)) => a.clone(),
                        _ => vec![],
                    };
                    let c = map
                        .get("next_cursor")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    (rows, c)
                }
                _ => (vec![], None),
            };
            Ok(ArrowResult::JsonFallback {
                rows,
                next_cursor: next_cursor.or(json_cursor),
            })
        }
    }
}

/// Decode an Arrow IPC stream into a vector of [`RecordBatch`].
pub fn decode_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>, BasinError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| BasinError::Decode(format!("arrow ipc: {e}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| BasinError::Decode(format!("arrow ipc batch: {e}")))?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_ipc() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    #[test]
    fn round_trips_ipc_bytes() {
        let bytes = sample_ipc();
        let batches = decode_ipc(&bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 1);
    }
}
