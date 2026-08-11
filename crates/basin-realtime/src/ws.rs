//! WebSocket transport layer — Phase 5.11.R3 + R4 presence.
//!
//! Mounts a multiplexed WebSocket handler at `/realtime/v1/ws/:project`.
//! A single connection can subscribe to multiple tables simultaneously.
//! Control messages are JSON-framed text frames on the same WS connection.
//!
//! # Protocol
//!
//! Client → Server control messages (text frames, JSON):
//!
//! ```json
//! {"type":"subscribe","table":"orders"}
//! {"type":"subscribe","table":"users","filter":"NEW.status = 'paid'"}
//! {"type":"unsubscribe","table":"orders"}
//! ```
//!
//! Presence messages (R4, Phoenix Channels shape):
//!
//! ```json
//! {"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{…}}
//! {"type":"presence_untrack","channel":"room:1","client_id":"c1"}
//! {"type":"heartbeat","channel":"room:1","client_id":"c1"}
//! ```
//!
//! Server → Client event frames (text frames, JSON):
//!
//! ```json
//! {"type":"event","project":"01J…","table":"orders","op":"INSERT","after":{…},"seq":42}
//! {"type":"error","code":"lag","table":"orders","missed":5}
//! {"type":"subscribed","table":"orders"}
//! {"type":"unsubscribed","table":"orders"}
//! {"type":"presence_state","channel":"room:1","presences":[…]}
//! {"type":"presence_diff","channel":"room:1","joins":[…],"leaves":[…]}
//! ```
//!
//! Server → Client close codes (WebSocket close frames):
//!
//! - `4001` — `unauthorized` (JWT missing or invalid)
//! - `4003` — `forbidden` (cross-project JWT mismatch)
//! - `4008` — `project_deleted` (channel closed by server)
//!
//! # Multiplex implementation
//!
//! Each connection keeps its live subscriptions in a connection-local
//! `HashMap<TableName, tokio::task::JoinHandle<()>>` — one forwarder task per
//! subscribed table. A `tokio::select!` loop races:
//!
//! 1. Incoming WS frames (control messages from the client).
//! 2. Events arriving on any active subscription channel.
//!
//! Because `tokio::select!` cannot select over a dynamic set of futures, the
//! loop uses a single `tokio::sync::mpsc::Sender<Arc<ChangeEvent>>` per
//! connection. Each table subscription spawns a lightweight forwarder task that
//! reads from the per-table broadcast receiver and forwards events (annotated
//! with the table name) onto the connection-scoped mpsc channel. The main loop
//! then reads from one unified mpsc receiver — a classic "fan-in" pattern.
//!
//! # Auth + RLS
//!
//! JWT auth is performed during the HTTP upgrade handshake (before the WS
//! connection is established). The token must appear in either:
//! - `Authorization: Bearer <token>` header, OR
//! - `Sec-WebSocket-Protocol` subprotocol header (for browser clients that
//!   cannot set arbitrary headers: use `basin-v1,<token>` as the subprotocol
//!   value).
//!
//! Cross-project isolation is enforced identically to R2's SSE handler: the
//! `claims.project_id` must match the `:project` path segment.
//!
//! RLS filtering mirrors `sse.rs::rls_permits` — admin / service_role see all
//! events; normal users see only events whose `causation_user` matches their
//! `user_id`.
//!
//! # Wire-up
//!
//! ```ignore
//! // In services/basin-server/src/main.rs behind #[cfg(feature = "realtime")]:
//! let ws_router = basin_realtime::ws_router(
//!     realtime_sink.registry().clone(),
//!     auth_service.clone(),
//! );
//! app = app.merge(ws_router);
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::header::AUTHORIZATION;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use basin_auth::AuthService;
use basin_common::{ChangeEvent, ChangeOp, ProjectId, TableName};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::filter::Filter;
use crate::presence::{
    authorize_client_id, serialize_presence_diff, serialize_presence_state, validate_metadata_size,
    PresenceEvent, PresenceRegistry,
};
use crate::{ChannelKey, ChannelRegistry, FilteredReceiver, ReplayCursor, ReplayRingRegistry};

// ---------------------------------------------------------------------------
// Close codes (4000–4999 are application-defined per RFC 6455).
// ---------------------------------------------------------------------------

/// Close code sent when the JWT is missing or invalid (pre-upgrade, so
/// typically manifests as HTTP 401; kept here for WS-level close scenarios
/// when auth is revoked mid-connection in R4).
#[allow(dead_code)]
const CLOSE_UNAUTHORIZED: u16 = 4001;

/// Close code sent when the JWT project does not match the path project
/// (pre-upgrade HTTP 403; defined here for R4 mid-connection policy checks).
#[allow(dead_code)]
const CLOSE_FORBIDDEN: u16 = 4003;

/// Close code sent when the project's broadcast channel closes (project
/// deleted or server shutdown). Forwarded by `forwarder_task` via the fan-in
/// channel.
const CLOSE_PROJECT_DELETED: u16 = 4008;

// ---------------------------------------------------------------------------
// Handler state (shared, cheap to clone)
// ---------------------------------------------------------------------------

/// Shared state injected into every WS handler call.
#[derive(Clone)]
pub struct WsState {
    pub registry: ChannelRegistry,
    pub auth: Arc<AuthService>,
    /// Presence registry (R4). Shared across all connections.
    pub presence: PresenceRegistry,
    /// Per-`(project, table)` replay ring backing reconnect-resume.
    /// Defaults to an empty registry when the WS router is built without
    /// one (legacy behaviour); callers should pass
    /// `sink.replay_rings().clone()` so the ring the publisher writes into
    /// is the same one this handler reads from.
    pub replay_rings: ReplayRingRegistry,
}

// ---------------------------------------------------------------------------
// Router + serve
// ---------------------------------------------------------------------------

/// Build the WS sub-router. Merge this into the main axum app.
///
/// ```ignore
/// let ws_router = basin_realtime::ws::router(registry.clone(), auth.clone());
/// app = app.merge(ws_router);
/// ```
pub fn router(registry: ChannelRegistry, auth: Arc<AuthService>) -> Router {
    router_with_presence(registry, auth, PresenceRegistry::default())
}

/// Build the WS sub-router with a custom [`PresenceRegistry`].
///
/// Useful in tests that need shortened heartbeat timeouts.
pub fn router_with_presence(
    registry: ChannelRegistry,
    auth: Arc<AuthService>,
    presence: PresenceRegistry,
) -> Router {
    router_with_state(registry, auth, presence, ReplayRingRegistry::new())
}

/// Build the WS sub-router with explicit presence + replay ring state.
///
/// Callers wired to a [`crate::RealtimeSink`] should pass
/// `sink.replay_rings().clone()` so reconnect-resume sees publisher events.
pub fn router_with_state(
    registry: ChannelRegistry,
    auth: Arc<AuthService>,
    presence: PresenceRegistry,
    replay_rings: ReplayRingRegistry,
) -> Router {
    let state = WsState {
        registry,
        auth,
        presence,
        replay_rings,
    };
    Router::new()
        .route("/realtime/v1/ws/:project", get(ws_handler))
        .with_state(state)
}

/// Bind a standalone HTTP server for the WS router on `bind_addr` and run it
/// until the returned [`tokio::task::JoinHandle`] is dropped or cancelled.
///
/// Called from `basin-server/src/main.rs` behind `#[cfg(feature = "realtime")]`
/// so that `basin-server` doesn't need to import `axum` directly.
///
/// # Errors
///
/// Returns `Err` if binding the listener fails (e.g. port already in use).
pub async fn serve(
    bind_addr: std::net::SocketAddr,
    registry: ChannelRegistry,
    auth: Arc<AuthService>,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    let app = router_with_presence(registry, auth, PresenceRegistry::default());
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "basin-realtime WS server error");
        }
    });
    Ok(handle)
}

// ---------------------------------------------------------------------------
// JSON control-plane message types (client → server)
// ---------------------------------------------------------------------------

/// A control message sent by the client over the WebSocket text channel.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    /// Subscribe to change events for a table.
    Subscribe {
        table: String,
        /// Optional SQL predicate filter (Phase 5.11.R5). Parsed by `Filter::new`.
        /// If absent or empty, no filter is applied and all RLS-permitted events
        /// for the table are forwarded.
        #[serde(default)]
        filter: Option<String>,
        /// Optional reconnect cursor: the last `seq` the client successfully
        /// processed before disconnecting. When present and non-zero the
        /// server consults the per-channel replay ring and re-feeds any
        /// missed events (or emits a `gap` frame if events have already
        /// been evicted). Closes #54 P0 silent replay loss.
        #[serde(default)]
        last_event_id: Option<u64>,
    },
    /// Unsubscribe from a table. Connection stays open.
    Unsubscribe { table: String },

    // --- Presence (R4) -------------------------------------------------------
    /// Track this client in a presence channel.
    ///
    /// ```json
    /// {"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{…}}
    /// ```
    PresenceTrack {
        channel: String,
        client_id: String,
        #[serde(default)]
        metadata: serde_json::Value,
    },
    /// Remove this client from a presence channel.
    ///
    /// ```json
    /// {"type":"presence_untrack","channel":"room:1","client_id":"c1"}
    /// ```
    PresenceUntrack { channel: String, client_id: String },
    /// Refresh the heartbeat for `(channel, client_id)`.
    ///
    /// ```json
    /// {"type":"heartbeat","channel":"room:1","client_id":"c1"}
    /// ```
    Heartbeat { channel: String, client_id: String },
}

// ---------------------------------------------------------------------------
// JSON event frames (server → client)
// ---------------------------------------------------------------------------

/// A message sent by the server over the WebSocket text channel.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMsg<'a> {
    /// A change event delivered from the ring buffer.
    Event {
        project: String,
        table: &'a str,
        op: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        before: Option<&'a serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<&'a serde_json::Value>,
        seq: u64,
    },
    /// Acknowledges a successful subscribe.
    Subscribed { table: &'a str },
    /// Acknowledges a successful unsubscribe.
    Unsubscribed { table: &'a str },
    /// Informs the client that it lagged on a channel and missed events.
    Error {
        code: &'static str,
        table: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        missed: Option<u64>,
    },
    /// Informs the client that a presence operation was rejected (e.g.
    /// `client_id` impersonation or oversized metadata). The connection stays
    /// open; only the offending presence message is dropped.
    PresenceError {
        code: &'static str,
        channel: &'a str,
        message: String,
    },
    /// Reconnect-resume gap: the supplied `last_event_id` predated the
    /// oldest event still in the replay ring; events between
    /// `last_event_id` and `oldest_in_ring - 1` were evicted and cannot be
    /// replayed via this transport. The client should cold re-sync from
    /// the analytical store. The live stream continues from `newest_in_ring`
    /// after this frame. Closes #54 P0.
    Gap {
        table: &'a str,
        last_event_id: u64,
        oldest_in_ring: u64,
        newest_in_ring: u64,
    },
}

/// Convert a [`ChangeOp`] to the stable wire string.
fn op_str(op: ChangeOp) -> &'static str {
    match op {
        ChangeOp::Insert => "INSERT",
        ChangeOp::Update => "UPDATE",
        ChangeOp::Delete => "DELETE",
    }
}

// ---------------------------------------------------------------------------
// Auth helpers (mirrors sse.rs)
// ---------------------------------------------------------------------------

/// Extract the Bearer token from `Authorization: Bearer <token>` or from the
/// `Sec-WebSocket-Protocol: basin-v1,<token>` hack for browser clients.
fn extract_token(headers: &HeaderMap) -> Option<String> {
    // Primary: Authorization header.
    if let Some(v) = headers.get(AUTHORIZATION) {
        if let Ok(s) = v.to_str() {
            let token = s
                .strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_owned);
            if token.is_some() {
                return token;
            }
        }
    }
    // Fallback: `Sec-WebSocket-Protocol: basin-v1,<token>` (browser JS clients
    // cannot set arbitrary headers on WebSocket upgrades).
    if let Some(v) = headers.get("sec-websocket-protocol") {
        if let Ok(s) = v.to_str() {
            for part in s.split(',') {
                let part = part.trim();
                if part != "basin-v1" && !part.is_empty() {
                    return Some(part.to_owned());
                }
            }
        }
    }
    None
}

/// RLS gate — mirrors `sse.rs::rls_permits` exactly.
fn rls_permits(event: &ChangeEvent, subscriber_user_id: &str, subscriber_roles: &[String]) -> bool {
    if subscriber_roles
        .iter()
        .any(|r| r == "admin" || r == "service_role")
    {
        return true;
    }
    match &event.causation_user {
        None => true,
        Some(u) => u == subscriber_user_id,
    }
}

// ---------------------------------------------------------------------------
// Fan-in event envelopes
// ---------------------------------------------------------------------------

/// An event forwarded from a per-table forwarder task to the main loop.
struct IncomingEvent {
    /// The table this event came from (owned, so the forwarder doesn't need to
    /// hold a borrow on the connection state).
    table: TableName,
    /// The event payload, or `None` when the channel was closed (project
    /// deleted, server shutdown).
    payload: Option<Arc<ChangeEvent>>,
    /// When `Some(n)`, the receiver lagged by `n` events.
    lagged: Option<u64>,
}

/// A presence event forwarded from a per-channel presence forwarder task to
/// the main connection loop.
struct IncomingPresence {
    event: PresenceEvent,
}

// ---------------------------------------------------------------------------
// HTTP upgrade handler
// ---------------------------------------------------------------------------

/// GET `/realtime/v1/ws/:project`
///
/// Performs the HTTP → WebSocket upgrade. Auth and cross-project isolation
/// are checked *before* the upgrade so that invalid requests get a proper HTTP
/// status code (401 / 403) rather than a WebSocket close frame.
#[axum::debug_handler]
pub async fn ws_handler(
    State(state): State<WsState>,
    Path(project_str): Path<String>,
    headers: HeaderMap,
    ws_upgrade: WebSocketUpgrade,
) -> Response {
    // --- 1. Extract + verify JWT -------------------------------------------
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "missing Authorization header",
            )
                .into_response();
        }
    };

    let claims = match state.auth.verify_jwt(&token) {
        Ok(c) => c,
        Err(_) => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "invalid or expired JWT",
            )
                .into_response();
        }
    };

    // --- 2. Parse + validate project path param ----------------------------
    let project_id: ProjectId = match project_str.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "invalid project id in path",
            )
                .into_response();
        }
    };

    // --- 3. Cross-project isolation ----------------------------------------
    if claims.project_id != project_id {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "JWT project does not match path project",
        )
            .into_response();
    }

    debug!(project = %project_id, "WS upgrade accepted");

    // Capture auth info for the async connection handler.
    let user_id = claims.user_id.to_string();
    let roles = claims.roles.clone();
    let registry = state.registry.clone();
    let presence = state.presence.clone();
    let replay_rings = state.replay_rings.clone();

    // --- 4. Upgrade to WebSocket -------------------------------------------
    ws_upgrade.on_upgrade(move |socket| {
        handle_ws(
            socket,
            project_id,
            user_id,
            roles,
            registry,
            presence,
            replay_rings,
        )
    })
}

// ---------------------------------------------------------------------------
// WebSocket connection handler (runs for the lifetime of the connection)
// ---------------------------------------------------------------------------

/// Maximum number of queued fan-in events before the mpsc channel applies
/// back-pressure. This bounds memory per connection; excess events cause
/// the forwarder task to block (and eventually the broadcast receiver lags).
const FAN_IN_CAPACITY: usize = 256;

/// Heartbeat interval for ping frames sent by the server.
const PING_INTERVAL: Duration = Duration::from_secs(30);

/// After this many missed pongs the connection is closed.
const MAX_MISSED_PONGS: u32 = 2;

/// Drive a single WebSocket connection until the client disconnects, the
/// project is deleted, or auth is revoked.
#[allow(clippy::too_many_arguments)]
async fn handle_ws(
    mut socket: WebSocket,
    project_id: ProjectId,
    user_id: String,
    roles: Vec<String>,
    registry: ChannelRegistry,
    presence: PresenceRegistry,
    replay_rings: ReplayRingRegistry,
) {
    info!(project = %project_id, user = %user_id, "WS connection open");

    // Fan-in channel: all per-table forwarder tasks send events here.
    let (fan_tx, mut fan_rx) = mpsc::channel::<IncomingEvent>(FAN_IN_CAPACITY);

    // Presence fan-in channel: per-channel presence forwarder tasks.
    let (pres_tx, mut pres_rx) = mpsc::channel::<IncomingPresence>(64);

    // Per-table forwarder task handles, keyed by table name.
    // Dropping a handle cancels the task (tokio::task::JoinHandle drop = abort).
    let mut forwarder_handles: HashMap<TableName, tokio::task::JoinHandle<()>> = HashMap::new();

    // Per-presence-channel forwarder task handles, keyed by channel name.
    let mut presence_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    // Track which client_ids this connection has registered, per channel.
    // Used for cleanup on disconnect. Maps channel_name -> client_id.
    let mut tracked_clients: HashMap<String, String> = HashMap::new();

    let mut ping_timer = interval(PING_INTERVAL);
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut missed_pongs: u32 = 0;

    loop {
        tokio::select! {
            // Outgoing heartbeat pings.
            _ = ping_timer.tick() => {
                if missed_pongs >= MAX_MISSED_PONGS {
                    warn!(project = %project_id, "WS: too many missed pongs; closing");
                    let _ = socket.send(Message::Close(Some(CloseFrame {
                        code: 1001, // Going Away
                        reason: "pong timeout".into(),
                    }))).await;
                    break;
                }
                if socket.send(Message::Ping(vec![].into())).await.is_err() {
                    break;
                }
                missed_pongs += 1;
            }

            // Incoming message from the client.
            maybe_msg = socket.recv() => {
                let msg = match maybe_msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        debug!(error = %e, "WS recv error");
                        break;
                    }
                    None => {
                        // Client closed.
                        break;
                    }
                };

                match msg {
                    Message::Text(text) => {
                        if let Err(close) = handle_client_msg(
                            &text,
                            &mut socket,
                            &project_id,
                            &user_id,
                            &roles,
                            &registry,
                            &presence,
                            &replay_rings,
                            &fan_tx,
                            &pres_tx,
                            &mut forwarder_handles,
                            &mut presence_handles,
                            &mut tracked_clients,
                        ).await {
                            let _ = socket.send(Message::Close(Some(close))).await;
                            break;
                        }
                    }
                    Message::Binary(_) => {
                        // Binary frames not supported in this version.
                        debug!("WS: ignoring binary frame");
                    }
                    Message::Pong(_) => {
                        // Client replied to our ping — reset missed pong counter.
                        missed_pongs = missed_pongs.saturating_sub(1);
                    }
                    Message::Ping(data) => {
                        // axum's WS stack does not auto-reply to pings that
                        // originate from the client (it only handles the server
                        // side of the heartbeat). We reply explicitly here.
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    Message::Close(_) => {
                        break;
                    }
                }
            }

            // Event delivered by a per-table forwarder task.
            maybe_ev = fan_rx.recv() => {
                let ev = match maybe_ev {
                    Some(e) => e,
                    None => {
                        // All forwarder senders dropped — no subscriptions remain
                        // but connection stays open (client may re-subscribe).
                        continue;
                    }
                };

                if let Some(lagged) = ev.lagged {
                    // Broadcast ring overflowed — notify client.
                    let msg_json = serde_json::to_string(&ServerMsg::Error {
                        code: "lag",
                        table: ev.table.as_str(),
                        missed: Some(lagged),
                    })
                    .unwrap_or_else(|_| r#"{"type":"error","code":"lag"}"#.to_owned());
                    if socket.send(Message::Text(msg_json.into())).await.is_err() {
                        break;
                    }
                    continue;
                }

                match ev.payload {
                    None => {
                        // Channel closed (project deleted / server shutdown).
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: CLOSE_PROJECT_DELETED,
                            reason: "project_deleted".into(),
                        }))).await;
                        break;
                    }
                    Some(arc_event) => {
                        // RLS gate.
                        if !rls_permits(&arc_event, &user_id, &roles) {
                            continue;
                        }
                        let msg_json = serialize_event(&arc_event, ev.table.as_str());
                        if socket.send(Message::Text(msg_json.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }

            // Presence event delivered by a per-channel presence forwarder.
            maybe_pres = pres_rx.recv() => {
                let pev = match maybe_pres {
                    Some(p) => p,
                    None => continue,
                };
                let json = match &pev.event {
                    PresenceEvent::State { channel, presences } => {
                        serialize_presence_state(channel, presences)
                    }
                    PresenceEvent::Diff { channel, joins, leaves } => {
                        serialize_presence_diff(channel, joins, leaves)
                    }
                };
                if socket.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
        }
    }

    // On disconnect: untrack all presence entries owned by this connection.
    for (channel, client_id) in &tracked_clients {
        presence.untrack(project_id, channel, client_id);
    }

    // Drop all forwarder tasks (JoinHandle abort on drop).
    drop(forwarder_handles);
    drop(presence_handles);
    info!(project = %project_id, user = %user_id, "WS connection closed");
}

/// Serialize a [`ChangeEvent`] into a JSON `event` frame.
fn serialize_event(event: &ChangeEvent, table_str: &str) -> String {
    let msg = ServerMsg::Event {
        project: event.project.to_string(),
        table: table_str,
        op: op_str(event.op),
        before: event.before.as_ref(),
        after: event.after.as_ref(),
        seq: event.seq,
    };
    serde_json::to_string(&msg)
        .unwrap_or_else(|_| r#"{"type":"error","code":"serialize_error"}"#.to_owned())
}

/// Process a single text control frame from the client.
///
/// Returns `Ok(())` if the message was handled successfully, or
/// `Err(CloseFrame)` if the connection should be closed.
#[allow(clippy::too_many_arguments)]
async fn handle_client_msg(
    text: &str,
    socket: &mut WebSocket,
    project_id: &ProjectId,
    user_id: &str,
    roles: &[String],
    registry: &ChannelRegistry,
    presence: &PresenceRegistry,
    replay_rings: &ReplayRingRegistry,
    fan_tx: &mpsc::Sender<IncomingEvent>,
    pres_tx: &mpsc::Sender<IncomingPresence>,
    forwarder_handles: &mut HashMap<TableName, tokio::task::JoinHandle<()>>,
    presence_handles: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    tracked_clients: &mut HashMap<String, String>,
) -> Result<(), CloseFrame<'static>> {
    let msg: ClientMsg = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "WS: unrecognised control message; ignoring");
            // Don't close the connection for a bad message — just warn.
            return Ok(());
        }
    };

    match msg {
        ClientMsg::Subscribe {
            table,
            filter: filter_sql,
            last_event_id,
        } => {
            // Validate table name.
            let table_name = match TableName::new(&table) {
                Ok(n) => n,
                Err(_) => {
                    warn!(table = %table, "WS: invalid table name in subscribe; ignoring");
                    return Ok(());
                }
            };

            // Idempotent: if already subscribed, just ack.
            if forwarder_handles.contains_key(&table_name) {
                let ack = serde_json::to_string(&ServerMsg::Subscribed {
                    table: table_name.as_str(),
                })
                .unwrap_or_default();
                let _ = socket.send(Message::Text(ack.into())).await;
                return Ok(());
            }

            // Parse the optional subscriber-side predicate filter (P1-1 fix).
            // If the SQL string is present but unparseable, reject the subscribe
            // with a protocol error rather than silently ignoring the predicate
            // (fail-closed: a broken filter must not open the floodgate).
            let compiled_filter: Option<Filter> = match filter_sql.as_deref() {
                None | Some("") => None,
                Some(sql) => match Filter::new(sql) {
                    Ok(f) => {
                        debug!(
                            project = %project_id,
                            table = %table_name,
                            predicate = %sql,
                            "WS: subscribe filter compiled"
                        );
                        Some(f)
                    }
                    Err(e) => {
                        warn!(
                            project = %project_id,
                            table = %table_name,
                            predicate = %sql,
                            error = %e,
                            "WS: invalid subscribe filter predicate; rejecting subscribe"
                        );
                        let err_json = serde_json::to_string(&ServerMsg::Error {
                            code: "invalid_filter",
                            table: table_name.as_str(),
                            missed: None,
                        })
                        .unwrap_or_default();
                        let _ = socket.send(Message::Text(err_json.into())).await;
                        return Ok(());
                    }
                },
            };

            // Subscribe to the live broadcast channel *before* processing
            // replay, so no event is missed in the gap.
            let key = ChannelKey::new(*project_id, table_name.clone());
            let live_rx = registry.subscribe_filtered(key, compiled_filter);

            // --- Replay missed events from the per-channel ring -------------
            //
            // Bound to the publisher's ring registry so events the client
            // missed during the disconnect window are re-fed before the live
            // forwarder takes over. A cursor that predates the oldest ring
            // entry surfaces as a Gap server message; the client should then
            // cold re-sync from the analytical store. Closes #54 P0 SSE/WS
            // silent replay loss.
            let last_seen = last_event_id.unwrap_or(0);
            let cursor = ReplayCursor::with_rings(
                *project_id,
                table_name.clone(),
                last_seen,
                replay_rings.clone(),
            );
            let outcome = cursor.drain_outcome();

            if let crate::DrainOutcome::Gap {
                oldest_seq,
                newest_seq,
                ..
            } = &outcome
            {
                warn!(
                    project = %project_id,
                    table = %table_name,
                    last_event_id = last_seen,
                    oldest_in_ring = oldest_seq,
                    newest_in_ring = newest_seq,
                    "WS reconnect: Last-Event-ID predates ring; missed events evicted, client must cold re-sync"
                );
                let gap = serde_json::to_string(&ServerMsg::Gap {
                    table: table_name.as_str(),
                    last_event_id: last_seen,
                    oldest_in_ring: *oldest_seq,
                    newest_in_ring: *newest_seq,
                })
                .unwrap_or_else(|_| r#"{"type":"gap"}"#.to_owned());
                if socket.send(Message::Text(gap.into())).await.is_err() {
                    return Err(CloseFrame {
                        code: 1001,
                        reason: std::borrow::Cow::Borrowed("client disconnected"),
                    });
                }
            }

            // Deliver replay events synchronously on the WS socket before
            // handing off to the forwarder task.
            for arc_event in outcome.events() {
                if !rls_permits(arc_event, user_id, roles) {
                    continue;
                }
                let msg_json = serialize_event(arc_event, table_name.as_str());
                if socket.send(Message::Text(msg_json.into())).await.is_err() {
                    // Client disconnected during replay; bail out of the whole handler.
                    return Err(CloseFrame {
                        code: 1001,
                        reason: std::borrow::Cow::Borrowed("client disconnected"),
                    });
                }
            }

            // Spawn a forwarder task that fan-ins this channel's events.
            // `live_rx` is a `FilteredReceiver` — it skips events that don't
            // satisfy the subscriber's compiled predicate before forwarding.
            let fan_tx_clone = fan_tx.clone();
            let table_name_clone = table_name.clone();
            let handle = tokio::spawn(forwarder_task(live_rx, table_name_clone, fan_tx_clone));
            forwarder_handles.insert(table_name.clone(), handle);

            debug!(project = %project_id, table = %table_name, "WS: subscribed");

            // Send `subscribed` ack.
            let ack = serde_json::to_string(&ServerMsg::Subscribed {
                table: table_name.as_str(),
            })
            .unwrap_or_default();
            let _ = socket.send(Message::Text(ack.into())).await;
        }

        ClientMsg::Unsubscribe { table } => {
            let table_name = match TableName::new(&table) {
                Ok(n) => n,
                Err(_) => {
                    warn!(table = %table, "WS: invalid table name in unsubscribe; ignoring");
                    return Ok(());
                }
            };

            // Drop the forwarder task — this cancels it and drops the broadcast
            // receiver, which stops future events from being enqueued.
            if forwarder_handles.remove(&table_name).is_some() {
                debug!(project = %project_id, table = %table_name, "WS: unsubscribed");
            }

            let ack = serde_json::to_string(&ServerMsg::Unsubscribed {
                table: table_name.as_str(),
            })
            .unwrap_or_default();
            let _ = socket.send(Message::Text(ack.into())).await;
        }

        // --- Presence (R4) --------------------------------------------------
        ClientMsg::PresenceTrack {
            channel,
            client_id,
            metadata,
        } => {
            // 6.SEC.P1: bind the presence identity to the authenticated session.
            // A wire-supplied client_id that does not match this connection's
            // JWT identity is rejected — a client must not impersonate another.
            let bound_id = match authorize_client_id(user_id, &client_id) {
                Ok(id) => id.to_owned(),
                Err(rej) => {
                    warn!(
                        project = %project_id,
                        channel = %channel,
                        authenticated = %user_id,
                        requested = %client_id,
                        "WS: rejected presence track with forged client_id"
                    );
                    send_presence_error(socket, &channel, &rej).await;
                    return Ok(());
                }
            };

            // 6.SEC.P1: cap metadata size before storing or broadcasting it.
            if let Err(rej) = validate_metadata_size(&metadata) {
                warn!(
                    project = %project_id,
                    channel = %channel,
                    client = %bound_id,
                    "WS: rejected presence track with oversized metadata"
                );
                send_presence_error(socket, &channel, &rej).await;
                return Ok(());
            }

            // Subscribe this connection to the channel's presence broadcast if
            // not already subscribed. Subscribe *before* track so we don't miss
            // the diff we're about to publish.
            if !presence_handles.contains_key(&channel) {
                let pres_rx = presence.subscribe(*project_id, &channel);
                let pres_tx_clone = pres_tx.clone();
                let handle = tokio::spawn(presence_forwarder_task(pres_rx, pres_tx_clone));
                presence_handles.insert(channel.clone(), handle);
            }

            // Send the current snapshot to this client (Phoenix Channels:
            // `presence_state` on join).
            let current = presence.snapshot(*project_id, &channel);
            let state_json = serialize_presence_state(&channel, &current);
            let _ = socket.send(Message::Text(state_json.into())).await;

            // Track the client — publishes a diff to all other subscribers.
            presence.track(*project_id, &channel, &bound_id, metadata);

            // Remember for cleanup on disconnect.
            tracked_clients.insert(channel.clone(), bound_id.clone());

            debug!(
                project = %project_id,
                channel = %channel,
                client = %bound_id,
                "WS: presence tracked"
            );
        }

        ClientMsg::PresenceUntrack { channel, client_id } => {
            // 6.SEC.P1: only the authenticated identity may untrack itself.
            let bound_id = match authorize_client_id(user_id, &client_id) {
                Ok(id) => id.to_owned(),
                Err(rej) => {
                    warn!(
                        project = %project_id,
                        channel = %channel,
                        authenticated = %user_id,
                        requested = %client_id,
                        "WS: rejected presence untrack with forged client_id"
                    );
                    send_presence_error(socket, &channel, &rej).await;
                    return Ok(());
                }
            };
            presence.untrack(*project_id, &channel, &bound_id);
            tracked_clients.remove(&channel);
            debug!(
                project = %project_id,
                channel = %channel,
                client = %bound_id,
                "WS: presence untracked"
            );
        }

        ClientMsg::Heartbeat { channel, client_id } => {
            // 6.SEC.P1: a heartbeat may only refresh the authenticated identity.
            let bound_id = match authorize_client_id(user_id, &client_id) {
                Ok(id) => id.to_owned(),
                Err(rej) => {
                    warn!(
                        project = %project_id,
                        channel = %channel,
                        authenticated = %user_id,
                        requested = %client_id,
                        "WS: rejected presence heartbeat with forged client_id"
                    );
                    send_presence_error(socket, &channel, &rej).await;
                    return Ok(());
                }
            };
            presence.heartbeat(*project_id, &channel, &bound_id);
            debug!(
                project = %project_id,
                channel = %channel,
                client = %bound_id,
                "WS: presence heartbeat"
            );
        }
    }

    Ok(())
}

/// Send a `presence_error` frame to the client for a rejected presence op.
/// The connection stays open; only the offending message is dropped.
async fn send_presence_error(
    socket: &mut WebSocket,
    channel: &str,
    rejection: &crate::presence::PresenceRejection,
) {
    let json = serde_json::to_string(&ServerMsg::PresenceError {
        code: rejection.code(),
        channel,
        message: rejection.message(),
    })
    .unwrap_or_else(|_| r#"{"type":"presenceerror","code":"presence_error"}"#.to_owned());
    let _ = socket.send(Message::Text(json.into())).await;
}

/// Per-table forwarder task. Reads from a [`FilteredReceiver`] and forwards
/// matching events to the connection-scoped fan-in mpsc channel.
///
/// The `FilteredReceiver` already skips events that do not satisfy the
/// subscriber's compiled predicate (P1-1 fix). This task therefore only
/// forwards events that have passed both:
/// 1. The subscriber-side SQL predicate filter (if any), AND
/// 2. The broadcast channel's fan-out.
///
/// This task runs until:
/// - The broadcast channel is closed (project deleted / server shutdown).
/// - The mpsc receiver is dropped (connection closed).
/// - The [`tokio::task::JoinHandle`] is dropped (unsubscribe / disconnect).
async fn forwarder_task(
    mut rx: FilteredReceiver,
    table: TableName,
    fan_tx: mpsc::Sender<IncomingEvent>,
) {
    loop {
        let ev = match rx.recv().await {
            Ok(arc_ev) => IncomingEvent {
                table: table.clone(),
                payload: Some(arc_ev),
                lagged: None,
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(table = %table, missed = n, "WS forwarder lagged");
                IncomingEvent {
                    table: table.clone(),
                    payload: None,
                    lagged: Some(n),
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                // Channel closed — signal the connection handler.
                let _ = fan_tx
                    .send(IncomingEvent {
                        table: table.clone(),
                        payload: None,
                        lagged: None,
                    })
                    .await;
                return;
            }
        };

        if fan_tx.send(ev).await.is_err() {
            // Connection handler dropped the receiver — task is done.
            return;
        }
    }
}

/// Per-presence-channel forwarder task. Reads `PresenceEvent`s from a broadcast
/// receiver and forwards them to the connection-scoped presence mpsc channel.
///
/// Runs until the broadcast channel is closed or the mpsc receiver is dropped.
async fn presence_forwarder_task(
    mut rx: broadcast::Receiver<PresenceEvent>,
    pres_tx: mpsc::Sender<IncomingPresence>,
) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                if pres_tx.send(IncomingPresence { event }).await.is_err() {
                    // Connection handler dropped the receiver.
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(missed = n, "WS presence forwarder lagged; continuing");
                // Presence events are ephemeral; silently skip lagged items.
            }
            Err(broadcast::error::RecvError::Closed) => {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ChangeEvent, ChangeOp};
    use chrono::Utc;

    fn make_event(seq: u64, causation_user: Option<&str>) -> ChangeEvent {
        ChangeEvent {
            project: ProjectId::new(),
            table: TableName::new("orders").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": seq})),
            committed_at: Utc::now(),
            seq,
            causation_user: causation_user.map(String::from),
        }
    }

    // --- RLS tests (identical logic to sse.rs, verified here independently) --

    #[test]
    fn ws_rls_system_event_always_visible() {
        let event = make_event(1, None);
        assert!(rls_permits(&event, "user-a", &[]));
    }

    #[test]
    fn ws_rls_user_event_visible_to_owner() {
        let event = make_event(2, Some("user-a"));
        assert!(rls_permits(&event, "user-a", &[]));
    }

    #[test]
    fn ws_rls_user_event_hidden_from_other() {
        let event = make_event(3, Some("user-a"));
        assert!(!rls_permits(&event, "user-b", &[]));
    }

    #[test]
    fn ws_rls_admin_role_sees_all() {
        let event = make_event(4, Some("user-a"));
        assert!(rls_permits(&event, "user-b", &["admin".to_string()]));
    }

    #[test]
    fn ws_rls_service_role_sees_all() {
        let event = make_event(5, Some("user-a"));
        assert!(rls_permits(&event, "user-c", &["service_role".to_string()]));
    }

    // --- Serialize event ---------------------------------------------------

    #[test]
    fn serialize_event_produces_valid_json() {
        let event = make_event(7, None);
        let json_str = serialize_event(&event, "orders");
        let v: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
        assert_eq!(v["type"], "event");
        assert_eq!(v["table"], "orders");
        assert_eq!(v["op"], "INSERT");
        assert_eq!(v["seq"], 7);
    }

    // --- Client message parsing --------------------------------------------

    #[test]
    fn parse_subscribe_msg() {
        let text = r#"{"type":"subscribe","table":"orders"}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        assert!(matches!(msg, ClientMsg::Subscribe { table, .. } if table == "orders"));
    }

    #[test]
    fn parse_subscribe_with_filter() {
        let text = r#"{"type":"subscribe","table":"orders","filter":"NEW.status = 'paid'"}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        match msg {
            ClientMsg::Subscribe {
                table,
                filter,
                last_event_id,
            } => {
                assert_eq!(table, "orders");
                assert_eq!(filter.as_deref(), Some("NEW.status = 'paid'"));
                assert!(last_event_id.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_unsubscribe_msg() {
        let text = r#"{"type":"unsubscribe","table":"orders"}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        assert!(matches!(msg, ClientMsg::Unsubscribe { table } if table == "orders"));
    }

    /// Reconnect-resume: `last_event_id` carries the client's cursor across
    /// the disconnect (closes #54 P0).
    #[test]
    fn parse_subscribe_with_last_event_id() {
        let text = r#"{"type":"subscribe","table":"orders","last_event_id":42}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        match msg {
            ClientMsg::Subscribe {
                table,
                filter: _,
                last_event_id,
            } => {
                assert_eq!(table, "orders");
                assert_eq!(last_event_id, Some(42));
            }
            _ => panic!("wrong variant"),
        }
    }

    // --- Presence message parsing (R4) ------------------------------------

    #[test]
    fn parse_presence_track_msg() {
        let text = r#"{"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{"name":"Alice"}}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        match msg {
            ClientMsg::PresenceTrack {
                channel,
                client_id,
                metadata,
            } => {
                assert_eq!(channel, "room:1");
                assert_eq!(client_id, "c1");
                assert_eq!(metadata["name"], "Alice");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_presence_untrack_msg() {
        let text = r#"{"type":"presence_untrack","channel":"room:1","client_id":"c1"}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        assert!(
            matches!(msg, ClientMsg::PresenceUntrack { channel, client_id }
                if channel == "room:1" && client_id == "c1")
        );
    }

    #[test]
    fn parse_heartbeat_msg() {
        let text = r#"{"type":"heartbeat","channel":"room:1","client_id":"c1"}"#;
        let msg: ClientMsg = serde_json::from_str(text).expect("parse");
        assert!(matches!(msg, ClientMsg::Heartbeat { channel, client_id }
                if channel == "room:1" && client_id == "c1"));
    }

    // --- Token extraction --------------------------------------------------

    #[test]
    fn extract_token_from_bearer_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer mytoken123".parse().unwrap());
        assert_eq!(extract_token(&headers).as_deref(), Some("mytoken123"));
    }

    #[test]
    fn extract_token_from_subprotocol() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "sec-websocket-protocol",
            "basin-v1, mytoken456".parse().unwrap(),
        );
        assert_eq!(extract_token(&headers).as_deref(), Some("mytoken456"));
    }

    #[test]
    fn extract_token_missing_returns_none() {
        let headers = HeaderMap::new();
        assert!(extract_token(&headers).is_none());
    }

    // --- Fan-in / forwarder task -------------------------------------------

    #[tokio::test]
    async fn forwarder_task_forwards_events() {
        use crate::FilteredReceiver;
        use tokio::sync::broadcast;

        let (tx, rx) = broadcast::channel::<Arc<ChangeEvent>>(16);
        let filtered_rx = FilteredReceiver::from_receiver(rx, None);
        let table = TableName::new("orders").unwrap();
        let (fan_tx, mut fan_rx) = mpsc::channel::<IncomingEvent>(16);

        // Spawn the forwarder.
        let _handle = tokio::spawn(forwarder_task(filtered_rx, table.clone(), fan_tx));

        // Publish an event.
        let event = Arc::new(make_event(42, None));
        tx.send(event.clone()).unwrap();

        let incoming = tokio::time::timeout(Duration::from_secs(2), fan_rx.recv())
            .await
            .expect("timeout")
            .expect("channel open");
        assert_eq!(incoming.payload.as_ref().map(|e| e.seq), Some(42));
        assert_eq!(incoming.table, table);
        assert!(incoming.lagged.is_none());
    }

    #[tokio::test]
    async fn forwarder_task_signals_channel_close() {
        use crate::FilteredReceiver;
        use tokio::sync::broadcast;

        let (tx, rx) = broadcast::channel::<Arc<ChangeEvent>>(16);
        let filtered_rx = FilteredReceiver::from_receiver(rx, None);
        let table = TableName::new("orders").unwrap();
        let (fan_tx, mut fan_rx) = mpsc::channel::<IncomingEvent>(16);

        let _handle = tokio::spawn(forwarder_task(filtered_rx, table.clone(), fan_tx));

        // Close the channel by dropping the sender.
        drop(tx);

        let incoming = tokio::time::timeout(Duration::from_secs(2), fan_rx.recv())
            .await
            .expect("timeout")
            .expect("channel open");
        // A channel-closed signal has payload == None and lagged == None.
        assert!(incoming.payload.is_none());
        assert!(incoming.lagged.is_none());
    }

    // --- Multiplex scenario via ChannelRegistry ---------------------------

    /// Two subscriptions on one fan-in: events for both tables appear in order.
    #[tokio::test]
    async fn multiplex_two_tables_fan_in() {
        use crate::RealtimeSink;
        use basin_common::ChangeEventSink;

        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let (fan_tx, mut fan_rx) = mpsc::channel::<IncomingEvent>(64);

        // Subscribe to two tables (unfiltered — None filter passes all events).
        for table_name in ["orders", "users"] {
            let table = TableName::new(table_name).unwrap();
            let key = ChannelKey::new(project, table.clone());
            let rx = sink.registry().subscribe_filtered(key, None);
            tokio::spawn(forwarder_task(rx, table, fan_tx.clone()));
        }

        // Publish one event to each table.
        let ev_orders = ChangeEvent {
            project,
            table: TableName::new("orders").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        };
        let ev_users = ChangeEvent {
            project,
            table: TableName::new("users").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 2})),
            committed_at: Utc::now(),
            seq: 2,
            causation_user: None,
        };

        sink.publish(&ev_orders).await.unwrap();
        sink.publish(&ev_users).await.unwrap();

        // Collect both events.
        let mut received = std::collections::HashSet::new();
        for _ in 0..2 {
            let ev = tokio::time::timeout(Duration::from_secs(2), fan_rx.recv())
                .await
                .expect("timeout")
                .expect("channel open");
            if let Some(payload) = ev.payload {
                received.insert(payload.seq);
            }
        }
        assert!(received.contains(&1), "orders event expected");
        assert!(received.contains(&2), "users event expected");
    }
}
