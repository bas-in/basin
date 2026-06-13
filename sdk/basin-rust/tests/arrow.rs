//! Arrow IPC transport tests (feature `arrow`).
#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use basin::arrow::{ArrowQuery, ArrowResult, ARROW_STREAM_MIME};
use basin::Client;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ipc_fixture() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
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

fn client(url: &str) -> Client {
    Client::builder(url).token("k").project_id("p").build().unwrap()
}

#[tokio::test]
async fn decodes_native_ipc_with_headers() {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rest/v1/events"))
        .and(header("accept", ARROW_STREAM_MIME))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", ARROW_STREAM_MIME)
                .insert_header("x-basin-next-cursor", "c9")
                .insert_header("x-basin-row-count", "3")
                .set_body_bytes(ipc_fixture()),
        )
        .mount(&s)
        .await;

    let result = client(&s.uri())
        .table("events")
        .select("*")
        .to_arrow()
        .await
        .unwrap();
    match result {
        ArrowResult::Ipc {
            batches,
            next_cursor,
            row_count,
        } => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(next_cursor.as_deref(), Some("c9"));
            assert_eq!(row_count, Some(3));
        }
        other => panic!("expected IPC, got {other:?}"),
    }
}

#[tokio::test]
async fn falls_back_to_json_rows() {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rest/v1/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(json!([{"id": 1}, {"id": 2}])),
        )
        .mount(&s)
        .await;

    let result = client(&s.uri())
        .table("events")
        .select("*")
        .to_arrow()
        .await
        .unwrap();
    match result {
        ArrowResult::JsonFallback { rows, .. } => assert_eq!(rows.len(), 2),
        other => panic!("expected JSON fallback, got {other:?}"),
    }
}
