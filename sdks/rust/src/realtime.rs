//! Realtime WebSocket client (feature `realtime`).
//!
//! Connects to `GET /realtime/v1/ws/:project` and delivers change / presence
//! frames as an async [`futures_util::Stream`].
//!
//! Protocol source of truth: `crates/basin-realtime/src/ws.rs`
//! (`ClientMsg` / `ServerMsg`) and `presence.rs`. Verified frame shapes:
//!
//! Client → server (`ClientMsg`, `tag = "type"`, snake_case):
//! - `{"type":"subscribe","table":…,"filter"?,"last_event_id"?}`
//! - `{"type":"unsubscribe","table":…}`
//! - `{"type":"presence_track","channel":…,"client_id":…,"metadata":…}`
//! - `{"type":"presence_untrack","channel":…,"client_id":…}`
//! - `{"type":"heartbeat","channel":…,"client_id":…}`
//!
//! Server → client (`ServerMsg`, `tag = "type"`, lowercase):
//! - `{"type":"event",project,table,op,before?,after?,seq}`
//! - `{"type":"subscribed"|"unsubscribed",table}`
//! - `{"type":"error",code,table,missed?}`
//! - `{"type":"gap",table,last_event_id,oldest_in_ring,newest_in_ring}`
//! - `{"type":"presence_state"|"presence_diff",channel,…}`
//! - `{"type":"presenceerror",code,channel,message}` (no underscore — the
//!   server's `PresenceError` variant uses `rename_all = "lowercase"`)
//!
//! Auth: the `Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol form (the
//! server also accepts `Authorization: Bearer`, but the subprotocol form is the
//! one the JS SDK uses and is browser-portable).

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::error::BasinError;
use crate::Client;

// ---------------------------------------------------------------------------
// Wire frame types
// ---------------------------------------------------------------------------

/// A presence channel member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresenceEntry {
    /// Client identity (bound to the JWT subject server-side).
    #[serde(default)]
    pub client_id: String,
    /// Arbitrary member metadata.
    #[serde(default)]
    pub metadata: Option<Value>,
    /// RFC 3339 join timestamp, if provided.
    #[serde(default)]
    pub joined_at: Option<String>,
}

/// A server → client frame. Deserialised via the `type` tag.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerFrame {
    /// A change event (`op` is `INSERT` / `UPDATE` / `DELETE`).
    Event {
        /// Owning project ULID.
        project: String,
        /// Table the change occurred on.
        table: String,
        /// Change operation: `"INSERT"`, `"UPDATE"`, or `"DELETE"`.
        op: String,
        /// Monotonic sequence number for reconnect resume.
        seq: u64,
        /// Row image before the change (UPDATE / DELETE).
        #[serde(default)]
        before: Option<Value>,
        /// Row image after the change (INSERT / UPDATE).
        #[serde(default)]
        after: Option<Value>,
    },
    /// Acknowledges a successful subscribe.
    Subscribed {
        /// Subscribed table.
        table: String,
    },
    /// Acknowledges a successful unsubscribe.
    Unsubscribed {
        /// Unsubscribed table.
        table: String,
    },
    /// The client lagged and missed events on a channel.
    Error {
        /// Stable error code.
        code: String,
        /// Affected table.
        table: String,
        /// Number of missed events, if known.
        #[serde(default)]
        missed: Option<u64>,
    },
    /// Reconnect-resume gap: the requested `last_event_id` predated the ring;
    /// cold re-sync is needed. The live stream resumes at `newest_in_ring`.
    Gap {
        /// Affected table.
        table: String,
        /// The `last_event_id` the client asked to resume from.
        last_event_id: u64,
        /// Oldest event still in the replay ring.
        oldest_in_ring: u64,
        /// Newest event in the replay ring.
        newest_in_ring: u64,
    },
    /// Initial presence snapshot for a channel.
    #[serde(rename = "presence_state")]
    PresenceState {
        /// Presence channel name.
        channel: String,
        /// Current members.
        #[serde(default)]
        presences: Vec<PresenceEntry>,
    },
    /// Incremental presence delta for a channel.
    #[serde(rename = "presence_diff")]
    PresenceDiff {
        /// Presence channel name.
        channel: String,
        /// Members that joined.
        #[serde(default)]
        joins: Vec<PresenceEntry>,
        /// Members that left.
        #[serde(default)]
        leaves: Vec<PresenceEntry>,
    },
    /// A presence operation was rejected; the connection stays open.
    #[serde(rename = "presenceerror")]
    PresenceError {
        /// Stable error code.
        code: String,
        /// Affected channel.
        channel: String,
        /// Human-readable detail.
        #[serde(default)]
        message: String,
    },
}

impl ServerFrame {
    /// The table or channel this frame pertains to, for routing.
    fn channel_key(&self) -> Option<&str> {
        match self {
            ServerFrame::Event { table, .. }
            | ServerFrame::Subscribed { table }
            | ServerFrame::Unsubscribed { table }
            | ServerFrame::Error { table, .. }
            | ServerFrame::Gap { table, .. } => Some(table),
            ServerFrame::PresenceState { channel, .. }
            | ServerFrame::PresenceDiff { channel, .. }
            | ServerFrame::PresenceError { channel, .. } => Some(channel),
        }
    }
}

/// Options for [`RealtimeClient::listen`].
#[derive(Debug, Clone, Default)]
pub struct SubscribeOptions {
    /// Optional server-side SQL predicate, e.g. `"NEW.status = 'paid'"`.
    pub filter: Option<String>,
    /// Reconnect cursor — the last `seq` the client successfully processed.
    pub last_event_id: Option<u64>,
}

// ---------------------------------------------------------------------------
// Reconnect / backoff
// ---------------------------------------------------------------------------

const BACKOFF_BASE_MS: u64 = 500;
const BACKOFF_MAX_MS: u64 = 30_000;

fn backoff(attempt: u32) -> Duration {
    let ms = BACKOFF_BASE_MS.saturating_mul(2u64.saturating_pow(attempt));
    Duration::from_millis(ms.min(BACKOFF_MAX_MS))
}

// ---------------------------------------------------------------------------
// RealtimeClient
// ---------------------------------------------------------------------------

/// Async WebSocket client for `GET /realtime/v1/ws/:project`.
///
/// Obtain via [`Client::realtime`](crate::Client::realtime). The primary API is
/// [`listen`](Self::listen), which returns a [`Stream`] of [`ServerFrame`]s for
/// one table or presence channel and transparently reconnects with exponential
/// backoff, re-issuing the subscription on each reconnect.
#[derive(Clone)]
pub struct RealtimeClient {
    client: Client,
}

impl RealtimeClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    async fn connect_url(&self) -> Result<(String, String), BasinError> {
        let project = self.client.resolve_project_id().await.ok_or_else(|| {
            BasinError::invalid_request(
                "realtime requires a project id — set project_id on the client builder",
            )
        })?;
        let token = self
            .client
            .transport()
            .bearer()
            .await?
            .ok_or_else(|| {
                BasinError::Realtime("realtime requires a JWT or API key".to_string())
            })?;
        let ws_base = self
            .client
            .base_url()
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        let url = format!("{ws_base}/realtime/v1/ws/{project}");
        Ok((url, token))
    }

    /// Subscribe to `table` and stream change frames.
    ///
    /// The returned [`Stream`] yields [`ServerFrame`]s (events, errors, gaps).
    /// On an unexpected disconnect the client reconnects with exponential
    /// backoff (0.5 s → 1 s → 2 s … capped at 30 s) and re-issues the
    /// subscription. Pass [`SubscribeOptions::last_event_id`] for server-side
    /// replay of missed events. Dropping the stream tears down the connection.
    pub fn listen(
        &self,
        table: impl Into<String>,
        opts: SubscribeOptions,
    ) -> impl Stream<Item = Result<ServerFrame, BasinError>> {
        let table = table.into();
        let this = self.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                match this.run_session(&table, &opts, &tx).await {
                    // Graceful end (receiver dropped) — stop.
                    Ok(SessionEnd::ReceiverGone) => break,
                    // Connection dropped — reconnect after backoff.
                    Ok(SessionEnd::Disconnected) => {}
                    Err(e) => {
                        // Surface the error to the consumer; then retry.
                        if tx.send(Err(e)).is_err() {
                            break;
                        }
                    }
                }
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(backoff(attempt - 1)).await;
                if tx.is_closed() {
                    break;
                }
            }
        });

        tokio_stream(rx)
    }

    /// Run a single WebSocket session: connect, subscribe, pump frames until
    /// the socket closes or the receiver is dropped.
    async fn run_session(
        &self,
        table: &str,
        opts: &SubscribeOptions,
        tx: &mpsc::UnboundedSender<Result<ServerFrame, BasinError>>,
    ) -> Result<SessionEnd, BasinError> {
        let (url, token) = self.connect_url().await?;

        let mut request = url
            .into_client_request()
            .map_err(|e| BasinError::Realtime(e.to_string()))?;
        let proto = format!("basin-v1, {token}");
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&proto).map_err(|e| BasinError::Realtime(e.to_string()))?,
        );

        let (ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| BasinError::Realtime(e.to_string()))?;
        let (mut sink, mut stream) = ws.split();

        // Send the subscribe frame.
        let mut sub = serde_json::Map::new();
        sub.insert("type".into(), Value::String("subscribe".into()));
        sub.insert("table".into(), Value::String(table.to_string()));
        if let Some(f) = &opts.filter {
            sub.insert("filter".into(), Value::String(f.clone()));
        }
        if let Some(id) = opts.last_event_id {
            sub.insert("last_event_id".into(), Value::from(id));
        }
        sink.send(Message::Text(Value::Object(sub).to_string()))
            .await
            .map_err(|e| BasinError::Realtime(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => return Ok(SessionEnd::Disconnected),
            };
            match msg {
                Message::Text(text) => {
                    if let Some(frame) = parse_frame(&text) {
                        // Route only frames for this table/channel; the server
                        // multiplexes one socket but a listener watches one key.
                        let relevant = frame
                            .channel_key()
                            .map(|k| k == table)
                            .unwrap_or(true);
                        if relevant && tx.send(Ok(frame)).is_err() {
                            return Ok(SessionEnd::ReceiverGone);
                        }
                    }
                }
                Message::Ping(p) => {
                    let _ = sink.send(Message::Pong(p)).await;
                }
                Message::Close(_) => return Ok(SessionEnd::Disconnected),
                _ => {}
            }
            if tx.is_closed() {
                return Ok(SessionEnd::ReceiverGone);
            }
        }
        Ok(SessionEnd::Disconnected)
    }
}

enum SessionEnd {
    Disconnected,
    ReceiverGone,
}

/// Parse a raw text frame into a [`ServerFrame`], returning `None` for unknown
/// or malformed frames (the caller logs and skips, matching the Python SDK).
fn parse_frame(raw: &str) -> Option<ServerFrame> {
    serde_json::from_str::<ServerFrame>(raw).ok()
}

/// Wrap an unbounded receiver as a [`Stream`].
fn tokio_stream<T>(mut rx: mpsc::UnboundedReceiver<T>) -> impl Stream<Item = T> {
    futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx))
}

/// A typed presence metadata map for [`RealtimeClient`] presence helpers.
pub type PresenceMetadata = HashMap<String, Value>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_frame() {
        let raw = r#"{"type":"event","project":"p","table":"orders","op":"INSERT","seq":7,"after":{"id":1}}"#;
        let f = parse_frame(raw).unwrap();
        match f {
            ServerFrame::Event {
                table, op, seq, after, ..
            } => {
                assert_eq!(table, "orders");
                assert_eq!(op, "INSERT");
                assert_eq!(seq, 7);
                assert_eq!(after.unwrap()["id"], 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parses_gap_frame() {
        let raw = r#"{"type":"gap","table":"t","last_event_id":3,"oldest_in_ring":10,"newest_in_ring":20}"#;
        assert!(matches!(parse_frame(raw), Some(ServerFrame::Gap { .. })));
    }

    #[test]
    fn parses_presenceerror_no_underscore() {
        let raw = r#"{"type":"presenceerror","code":"impersonation","channel":"room:1","message":"nope"}"#;
        assert!(matches!(
            parse_frame(raw),
            Some(ServerFrame::PresenceError { .. })
        ));
    }

    #[test]
    fn parses_presence_state() {
        let raw = r#"{"type":"presence_state","channel":"room:1","presences":[{"client_id":"c1"}]}"#;
        match parse_frame(raw).unwrap() {
            ServerFrame::PresenceState { channel, presences } => {
                assert_eq!(channel, "room:1");
                assert_eq!(presences.len(), 1);
                assert_eq!(presences[0].client_id, "c1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn unknown_frame_is_none() {
        assert!(parse_frame(r#"{"type":"weird"}"#).is_none());
        assert!(parse_frame("not json").is_none());
    }

    #[test]
    fn backoff_caps_at_30s() {
        assert_eq!(backoff(0), Duration::from_millis(500));
        assert_eq!(backoff(1), Duration::from_millis(1000));
        assert_eq!(backoff(100), Duration::from_millis(30_000));
    }

    #[test]
    fn channel_key_routes_event_to_table() {
        let f = ServerFrame::Event {
            project: "p".into(),
            table: "orders".into(),
            op: "INSERT".into(),
            seq: 1,
            before: None,
            after: None,
        };
        assert_eq!(f.channel_key(), Some("orders"));
    }
}
