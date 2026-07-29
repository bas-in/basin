//! Realtime WebSocket tests (feature `realtime`).
//!
//! Spins up a minimal local WS server that mimics basin-realtime's handshake
//! (echoing the `basin-v1` subprotocol) and frame protocol, then drives the
//! SDK's `listen()` stream against it.
#![cfg(feature = "realtime")]

use std::net::SocketAddr;

use basin::realtime::{ServerFrame, SubscribeOptions};
use basin::Client;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

/// Start a WS server that accepts the `basin-v1` subprotocol, expects a
/// `subscribe` frame, then emits one `event` frame.
async fn start_mock_ws() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        // Echo back the `basin-v1` subprotocol during the handshake, like the
        // server does, and assert the client sent the token in the protocol.
        // The large-Err lint fires on tungstenite's own callback signature.
        #[allow(clippy::result_large_err)]
        let callback = |req: &Request, mut response: Response| {
            let proto = req
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            assert!(proto.contains("basin-v1"), "missing basin-v1 subprotocol");
            assert!(proto.contains("test-token"), "missing token in subprotocol");
            response
                .headers_mut()
                .insert("sec-websocket-protocol", HeaderValue::from_static("basin-v1"));
            Ok(response)
        };

        let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
            .await
            .unwrap();

        // Expect the subscribe frame.
        if let Some(Ok(Message::Text(text))) = ws.next().await {
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(v["type"], "subscribe");
            assert_eq!(v["table"], "orders");
        }

        // Ack + one event.
        ws.send(Message::Text(
            r#"{"type":"subscribed","table":"orders"}"#.into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            r#"{"type":"event","project":"p","table":"orders","op":"INSERT","seq":1,"after":{"id":7}}"#
                .into(),
        ))
        .await
        .unwrap();

        // Keep the connection open briefly so the client reads both frames.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    addr
}

#[tokio::test]
async fn listen_receives_event_over_ws() {
    let addr = start_mock_ws().await;
    let client = Client::builder(format!("http://{addr}"))
        .token("test-token")
        .project_id("p")
        .build()
        .unwrap();

    let mut stream = client.realtime().listen("orders", SubscribeOptions::default());

    // Collect frames until we see the event (skip the `subscribed` ack).
    let mut saw_event = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(ServerFrame::Event { op, seq, after, .. }))) => {
                assert_eq!(op, "INSERT");
                assert_eq!(seq, 1);
                assert_eq!(after.unwrap()["id"], 7);
                saw_event = true;
                break;
            }
            Ok(Some(Ok(_))) => continue, // subscribed ack etc.
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(saw_event, "did not receive the event frame");
}
