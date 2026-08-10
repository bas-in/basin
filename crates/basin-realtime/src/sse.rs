//! Server-Sent Events (SSE) transport — Phase 5.11.R2.
//!
//! Mounts a streaming GET handler at `/realtime/v1/sse/:project/:table`.
//! Web clients connect via `EventSource` and receive a continuous JSON
//! event stream for every INSERT, UPDATE, or DELETE on the subscribed table.
//!
//! # Protocol
//!
//! Each emitted SSE frame looks like:
//!
//! ```text
//! id: 42
//! data: {"project":"01J…","table":"orders","op":"Insert","after":{…},"seq":42}
//!
//! ```
//!
//! Comment frames (`: heartbeat`) are emitted every 15 s so proxies and
//! load balancers don't close idle connections.
//!
//! # Replay on reconnect
//!
//! Clients send `Last-Event-ID: <seq>` on reconnect. The handler calls
//! [`ReplayCursor::drain`] to replay missed events with `seq > last_seen`
//! before handing off to the live broadcast stream.
//!
//! # Auth + RLS
//!
//! Every request must carry `Authorization: Bearer <jwt>`. The JWT is
//! verified via [`basin_auth::AuthService::verify_jwt`] (identical to
//! `basin-rest` per ADR 0006). The `claims.project_id` **must** match the
//! `:project` path segment — any mismatch returns 403 (cross-project
//! isolation gate). RLS filtering: only events whose `causation_user` is
//! `None` (admin/system) or whose roles the subscriber's JWT carries are
//! forwarded; all others are silently skipped.
//!
//! # Wire-up
//!
//! ```ignore
//! // In services/basin-server/src/main.rs behind #[cfg(feature = "realtime")]:
//! let sse_router = basin_realtime::sse::router(
//!     realtime_sink.registry().clone(),
//!     auth_service.clone(),
//! );
//! app = app.merge(sse_router);
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use basin_auth::AuthService;
use basin_common::{ChangeEvent, ChangeOp, ProjectId, TableName};
use futures::stream::{self, Stream, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::{ChannelKey, ChannelRegistry, ReplayCursor, ReplayRingRegistry};

/// Shared handler state.
#[derive(Clone)]
pub struct SseState {
    pub registry: ChannelRegistry,
    pub auth: Arc<AuthService>,
    /// Per-`(project, table)` ring used to honour `Last-Event-ID` on
    /// reconnect (closes #54 P0 SSE silent replay loss). Defaults to an
    /// empty registry — handlers built without one will see an empty
    /// replay (legacy behaviour) but will still log a warning when the
    /// client supplies a `Last-Event-ID` that cannot be honoured.
    pub replay_rings: ReplayRingRegistry,
}

/// Build the SSE sub-router. Merge this into the main axum app.
///
/// ```ignore
/// let sse_router = basin_realtime::sse::router(registry.clone(), auth.clone());
/// app = app.merge(sse_router);
/// ```
pub fn router(registry: ChannelRegistry, auth: Arc<AuthService>) -> Router {
    router_with_rings(registry, auth, ReplayRingRegistry::new())
}

/// Build the SSE sub-router with an explicit [`ReplayRingRegistry`].
///
/// Callers that have a [`crate::RealtimeSink`] should pass
/// `sink.replay_rings().clone()` so `Last-Event-ID` honours the events the
/// sink has buffered.
pub fn router_with_rings(
    registry: ChannelRegistry,
    auth: Arc<AuthService>,
    replay_rings: ReplayRingRegistry,
) -> Router {
    let state = SseState {
        registry,
        auth,
        replay_rings,
    };
    Router::new()
        .route("/realtime/v1/sse/:project/:table", get(sse_handler))
        .with_state(state)
}

/// Bind a standalone HTTP server for the SSE router on `bind_addr` and run it
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
    let app = router(registry, auth);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "basin-realtime SSE server error");
        }
    });
    Ok(handle)
}

/// Wire shape of an SSE `data:` frame. The `op` string is lowercased for
/// friendliness with JavaScript clients.
#[derive(Serialize)]
struct SsePayload<'a> {
    project: String,
    table: &'a str,
    op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<&'a serde_json::Value>,
    seq: u64,
}

/// Convert a [`ChangeOp`] to a stable JSON string.
fn op_str(op: ChangeOp) -> &'static str {
    match op {
        ChangeOp::Insert => "INSERT",
        ChangeOp::Update => "UPDATE",
        ChangeOp::Delete => "DELETE",
    }
}

/// Serialize a [`ChangeEvent`] into an SSE [`Event`] with `id: <seq>`.
fn change_event_to_sse(event: &ChangeEvent) -> Option<Event> {
    let payload = SsePayload {
        project: event.project.to_string(),
        table: event.table.as_str(),
        op: op_str(event.op),
        before: event.before.as_ref(),
        after: event.after.as_ref(),
        seq: event.seq,
    };
    let data = match serde_json::to_string(&payload) {
        Ok(s) => s,
        Err(e) => {
            warn!(seq = event.seq, error = %e, "SSE: failed to serialize change event");
            return None;
        }
    };
    Some(Event::default().id(event.seq.to_string()).data(data))
}

/// Error response helper — returns a plain SSE-compatible HTTP error before
/// the streaming body starts.
fn err_response(status: StatusCode, msg: &'static str) -> Response {
    (status, msg).into_response()
}

/// Parse the `Authorization: Bearer <token>` header and verify the JWT.
/// Returns the verified claims, or an error response ready to send.
fn extract_bearer(headers: &HeaderMap) -> Result<&str, Response> {
    let auth_hdr = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| err_response(StatusCode::UNAUTHORIZED, "missing Authorization header"))?
        .to_str()
        .map_err(|_| {
            err_response(
                StatusCode::UNAUTHORIZED,
                "Authorization header is not ASCII",
            )
        })?;

    let token = auth_hdr
        .strip_prefix("Bearer ")
        .or_else(|| auth_hdr.strip_prefix("bearer "))
        .ok_or_else(|| {
            err_response(
                StatusCode::UNAUTHORIZED,
                "Authorization must be `Bearer <token>`",
            )
        })?
        .trim();

    if token.is_empty() {
        return Err(err_response(StatusCode::UNAUTHORIZED, "empty bearer token"));
    }
    Ok(token)
}

/// Check whether the subscriber's JWT roles permit them to see this event.
///
/// RLS rule (Phase 5.11.R2 baseline):
/// - Events with `causation_user == None` are system/admin events — always
///   visible to authenticated subscribers of the same project.
/// - Events with `causation_user == Some(u)` are user-attributed. If the
///   subscriber's `user_id` matches `u`, they can see the event. Otherwise
///   the event is filtered out.
///
/// This is an intentionally conservative default. R5 (filter pushdown) will
/// replace this with arbitrary predicate evaluation; R4 (presence) can extend
/// it with membership-based policies. For now, the rule satisfies the spec's
/// "subscriber only sees rows their policies permit" requirement with a simple
/// row-level ownership check.
fn rls_permits(event: &ChangeEvent, subscriber_user_id: &str, subscriber_roles: &[String]) -> bool {
    // Admin-role subscribers see everything for their project.
    if subscriber_roles
        .iter()
        .any(|r| r == "admin" || r == "service_role")
    {
        return true;
    }
    match &event.causation_user {
        // System events are always visible.
        None => true,
        // User-attributed events: only the originating user sees them
        // (or an admin, handled above).
        Some(u) => u == subscriber_user_id,
    }
}

/// GET `/realtime/v1/sse/:project/:table`
///
/// Streams change events as SSE to the caller. Enforces:
/// 1. JWT auth (identical to basin-rest / ADR 0006).
/// 2. Cross-project isolation: `claims.project_id` must equal `:project`.
/// 3. RLS filtering via [`rls_permits`].
/// 4. Replay of missed events when `Last-Event-ID` header is present.
/// 5. Heartbeat every 15 s via axum's [`KeepAlive`].
#[axum::debug_handler]
pub async fn sse_handler(
    State(state): State<SseState>,
    Path((project_str, table_str)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    // --- 1. Auth -----------------------------------------------------------
    let token = extract_bearer(&headers)?;

    let claims = state
        .auth
        .verify_jwt(token)
        .map_err(|_| err_response(StatusCode::UNAUTHORIZED, "invalid or expired JWT"))?;

    // --- 2. Parse + validate path params -----------------------------------
    let project_id: ProjectId = project_str
        .parse()
        .map_err(|_| err_response(StatusCode::BAD_REQUEST, "invalid project id in path"))?;

    let table_name: TableName = TableName::new(&table_str)
        .map_err(|_| err_response(StatusCode::BAD_REQUEST, "invalid table name in path"))?;

    // --- 3. Cross-project isolation ----------------------------------------
    if claims.project_id != project_id {
        return Err(err_response(
            StatusCode::FORBIDDEN,
            "JWT project does not match path project",
        ));
    }

    // --- 4. Replay cursor (Last-Event-ID) ----------------------------------
    let last_seen: u64 = headers
        .get("last-event-id")
        .or_else(|| headers.get("Last-Event-ID"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // --- 5. Subscribe to live channel (before replay so no event is missed) --
    let key = ChannelKey::new(project_id, table_name.clone());
    let live_rx = state.registry.subscribe(key.clone());

    // --- 6. Drain replay events from the per-channel ring -------------------
    //
    // Bound to the live publisher's ring registry so events the client missed
    // during the disconnect window are re-fed before handoff to the live
    // stream. If the cursor predates the oldest still-buffered event we get
    // a [`DrainOutcome::Gap`] and emit a `gap` comment frame so the client
    // can fall back to a cold re-sync (closes #54 P0 SSE silent replay loss).
    let cursor = ReplayCursor::with_rings(
        project_id,
        table_name.clone(),
        last_seen,
        state.replay_rings.clone(),
    );
    let outcome = cursor.drain_outcome();
    let gap_comment = match &outcome {
        crate::DrainOutcome::Gap {
            oldest_seq,
            newest_seq,
            ..
        } => {
            warn!(
                project = %project_id,
                table = %table_name,
                last_seen,
                oldest_in_ring = oldest_seq,
                newest_in_ring = newest_seq,
                "SSE reconnect: Last-Event-ID predates ring; missed events evicted, client must cold re-sync"
            );
            Some(format!(
                "gap last_seen={} oldest_in_ring={} newest_in_ring={}",
                last_seen, oldest_seq, newest_seq
            ))
        }
        crate::DrainOutcome::Replay { .. } => None,
    };
    let replay_events: Vec<Arc<ChangeEvent>> = outcome.events().to_vec();

    // Clone the auth info for the filter closure (must be 'static).
    let subscriber_user_id = claims.user_id.to_string();
    let subscriber_roles = claims.roles.clone();

    debug!(
        project = %project_id,
        table = %table_name,
        last_seen,
        replay_count = replay_events.len(),
        "SSE subscriber connected",
    );

    // --- 7. Build the event stream -----------------------------------------
    //
    // Phase 1: emit replay events.
    // Phase 2: stream live broadcast events until the channel closes or the
    //          client disconnects.
    //
    // The channel-to-stream conversion via `async_stream` / `futures::stream`
    // keeps the handler body owned by the stream future, so state doesn't need
    // `'static` clones beyond what's captured below.

    let uid_clone = subscriber_user_id.clone();
    let roles_clone = subscriber_roles.clone();

    // Replay stream: convert Vec<Arc<ChangeEvent>> to a stream of SSE events.
    let replay_stream = stream::iter(replay_events).filter_map(move |arc_event| {
        let uid = uid_clone.clone();
        let roles = roles_clone.clone();
        async move {
            if !rls_permits(&arc_event, &uid, &roles) {
                return None;
            }
            change_event_to_sse(&arc_event).map(Ok)
        }
    });

    // Live stream: poll the broadcast receiver, filtering by RLS.
    let uid_live = subscriber_user_id.clone();
    let roles_live = subscriber_roles.clone();

    let live_stream = stream::unfold(live_rx, move |mut rx| {
        let uid = uid_live.clone();
        let roles = roles_live.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(arc_event) => {
                        if !rls_permits(&arc_event, &uid, &roles) {
                            // Skip this event; continue polling.
                            continue;
                        }
                        let sse_event = change_event_to_sse(&arc_event)
                            .map(Ok)
                            .unwrap_or_else(|| Ok(Event::default().comment("serialize_error")));
                        return Some((sse_event, rx));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            lagged = n,
                            "SSE subscriber lagged; some events were missed — client should reconnect"
                        );
                        // Emit a comment frame to inform the client it should
                        // reconnect with Last-Event-ID to replay missed events.
                        let comment = Ok(Event::default().comment(format!("lagged:{n}")));
                        return Some((comment, rx));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed (server shutting down). End the stream.
                        return None;
                    }
                }
            }
        }
    });

    // Chain: optional gap-signal comment → replay → live.
    //
    // When the reconnect cursor predates the oldest event in the ring we
    // emit a single `: gap …` comment frame so EventSource clients can
    // detect the loss and fall back to a cold re-sync.
    let gap_stream = stream::iter(
        gap_comment
            .into_iter()
            .map(|c| Ok(Event::default().comment(c)))
            .collect::<Vec<_>>(),
    );
    let stream = gap_stream.chain(replay_stream).chain(live_stream);

    // Heartbeat: axum's KeepAlive sends `: \n\n` comment frames every 15 s.
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    ))
}

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

    /// System events (no causation_user) are always visible.
    #[test]
    fn rls_system_event_always_visible() {
        let event = make_event(1, None);
        assert!(rls_permits(&event, "user-a", &[]));
    }

    /// User-attributed event: only the originating user sees it.
    #[test]
    fn rls_user_event_visible_to_owner() {
        let event = make_event(2, Some("user-a"));
        assert!(rls_permits(&event, "user-a", &[]));
    }

    /// User-attributed event: a different user cannot see it.
    #[test]
    fn rls_user_event_hidden_from_other() {
        let event = make_event(3, Some("user-a"));
        assert!(!rls_permits(&event, "user-b", &[]));
    }

    /// Admin role always passes RLS.
    #[test]
    fn rls_admin_role_sees_all() {
        let event = make_event(4, Some("user-a"));
        assert!(rls_permits(&event, "user-b", &["admin".to_string()]));
    }

    /// service_role always passes RLS.
    #[test]
    fn rls_service_role_sees_all() {
        let event = make_event(5, Some("user-a"));
        assert!(rls_permits(&event, "user-c", &["service_role".to_string()]));
    }

    /// SSE event serialization includes id and data.
    #[test]
    fn change_event_to_sse_produces_event() {
        let mut event = make_event(7, None);
        event.project = "01J0000000000000000000000A"
            .parse()
            .unwrap_or_else(|_| ProjectId::new());
        let sse = change_event_to_sse(&event);
        assert!(sse.is_some(), "expected Some(Event), got None");
    }
}
