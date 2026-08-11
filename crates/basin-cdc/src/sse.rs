//! Server-Sent Events delivery for the durable CDC stream (ADR 0028 Phase 1,
//! commit 3).
//!
//! Route: `GET /v1/cdc/:project/stream` (the path named in `cdc-design.md` §3).
//!
//! ## Contract
//!
//! ```text
//! GET /v1/cdc/:project/stream
//! Authorization: Bearer <jwt>          # same JWT path as basin-rest / realtime
//! Accept: text/event-stream
//!
//!   ?resume_after=<seq>   resume from events with seq > resume_after (or the
//!                         SSE `Last-Event-ID` header — EventSource sends it on
//!                         reconnect). Default: live only.
//!   ?tables=a,b           deliver only these tables (default: all).
//!   ?ops=insert,update    deliver only these ops (default: all).
//! ```
//!
//! Frames:
//! ```text
//! id: 1234
//! data: {"type":"event","seq":1234,"table":"orders","op":"insert","after":{…}}
//! ```
//! plus `: heartbeat` comments every 15s, and a one-shot
//! `data: {"type":"cursor_expired","resume_after":…,"min_available_seq":…}`
//! when the resume cursor predates the retention window.
//!
//! ## Replay-then-live
//!
//! 1. Auth + cross-project isolation (`claims.project_id` must equal `:project`).
//! 2. Subscribe to the in-process live tail **before** replaying, so no event
//!    committed during replay is missed (it will also be in the live channel
//!    and de-duplicated by the `seq > last_delivered` guard).
//! 3. Replay durable ring events with `seq > resume_after`. If the cursor
//!    predates `min_available_seq`, emit `cursor_expired` first.
//! 4. Live-tail the broadcast, skipping any `seq <= last_delivered` (covers the
//!    replay/live overlap window).
//!
//! Table/op filters are evaluated server-side (`O(1)` per event).

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use basin_auth::AuthService;
use basin_common::ProjectId;
use basin_storage::Storage;
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::live::LiveRegistry;
use crate::record::{CdcRecord, CdcSseFrame};
use crate::ring::ProjectRing;

/// Shared SSE handler state. Cheap to clone.
#[derive(Clone)]
pub struct CdcSseState {
    pub storage: Storage,
    pub live: LiveRegistry,
    pub auth: Arc<AuthService>,
}

/// Build the CDC SSE sub-router. Merge into the main axum app (same idiom as
/// `basin_realtime::sse::router`).
///
/// Routes:
/// - `GET /v1/cdc/:project/stream` — Basin-native JSON event frames (Phase 1).
/// - `GET /v1/cdc/:project/wal2json` — the same resumable stream, but each
///   frame is one [`crate::wal2json`] v2 action object (Phase 4), so a
///   `wal2json`-shaped consumer parses it without a Basin-specific codec.
pub fn cdc_router(state: CdcSseState) -> Router {
    Router::new()
        .route("/v1/cdc/:project/stream", get(cdc_stream_handler))
        .route("/v1/cdc/:project/wal2json", get(cdc_wal2json_handler))
        .with_state(state)
}

/// Query params for the stream (`cdc-design.md` §3).
#[derive(Debug, Deserialize, Default)]
pub struct StreamParams {
    #[serde(default)]
    resume_after: Option<u64>,
    #[serde(default)]
    tables: Option<String>,
    #[serde(default)]
    ops: Option<String>,
}

/// Server-side filter compiled once per connection.
#[derive(Clone)]
struct StreamFilter {
    tables: Option<HashSet<String>>,
    ops: Option<HashSet<String>>,
}

impl StreamFilter {
    fn from_params(p: &StreamParams) -> Self {
        let tables = p.tables.as_ref().map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect::<HashSet<_>>()
        });
        let ops = p.ops.as_ref().map(|s| {
            s.split(',')
                .map(|o| o.trim().to_ascii_lowercase())
                .filter(|o| !o.is_empty())
                .collect::<HashSet<_>>()
        });
        Self { tables, ops }
    }

    /// `O(1)` membership checks (HashSet lookups).
    fn matches(&self, rec: &CdcRecord) -> bool {
        if let Some(tables) = &self.tables {
            if !tables.contains(&rec.table) {
                return false;
            }
        }
        if let Some(ops) = &self.ops {
            if !ops.contains(rec.op_name()) {
                return false;
            }
        }
        true
    }
}

fn err(status: StatusCode, msg: &'static str) -> Response {
    (status, msg).into_response()
}

fn bearer(headers: &HeaderMap) -> Result<&str, Response> {
    let h = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| err(StatusCode::UNAUTHORIZED, "missing Authorization header"))?
        .to_str()
        .map_err(|_| {
            err(
                StatusCode::UNAUTHORIZED,
                "Authorization header is not ASCII",
            )
        })?;
    let token = h
        .strip_prefix("Bearer ")
        .or_else(|| h.strip_prefix("bearer "))
        .ok_or_else(|| {
            err(
                StatusCode::UNAUTHORIZED,
                "Authorization must be `Bearer <token>`",
            )
        })?
        .trim();
    if token.is_empty() {
        return Err(err(StatusCode::UNAUTHORIZED, "empty bearer token"));
    }
    Ok(token)
}

fn frame_for(rec: &CdcRecord) -> Option<Event> {
    let payload = CdcSseFrame::event(rec);
    match serde_json::to_string(&payload) {
        Ok(data) => Some(Event::default().id(rec.seq.to_string()).data(data)),
        Err(e) => {
            warn!(seq = rec.seq, error = %e, "cdc SSE: serialize failed");
            None
        }
    }
}

/// Build a wal2json v2 per-row action object frame for one record (Phase 4).
/// `id:` carries the `seq` cursor exactly like the native frame, so resume /
/// `Last-Event-ID` works identically across both stream shapes.
fn wal2json_frame_for(rec: &CdcRecord) -> Option<Event> {
    let payload = crate::wal2json::Wal2JsonTxn::row(rec);
    match serde_json::to_string(&payload) {
        Ok(data) => Some(Event::default().id(rec.seq.to_string()).data(data)),
        Err(e) => {
            warn!(seq = rec.seq, error = %e, "cdc wal2json SSE: serialize failed");
            None
        }
    }
}

/// `GET /v1/cdc/:project/stream`
#[axum::debug_handler]
pub async fn cdc_stream_handler(
    State(state): State<CdcSseState>,
    Path(project_str): Path<String>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    // --- 1. Auth ---------------------------------------------------------
    let token = bearer(&headers)?;
    let claims = state
        .auth
        .verify_jwt(token)
        .map_err(|_| err(StatusCode::UNAUTHORIZED, "invalid or expired JWT"))?;

    // --- 2. Parse + isolate ----------------------------------------------
    let project: ProjectId = project_str
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "invalid project id in path"))?;
    if claims.project_id != project {
        return Err(err(
            StatusCode::FORBIDDEN,
            "JWT project does not match path project",
        ));
    }

    // --- 3. Cursor (query param or Last-Event-ID) ------------------------
    let resume_after: u64 = params.resume_after.unwrap_or_else(|| {
        headers
            .get("last-event-id")
            .or_else(|| headers.get("Last-Event-ID"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    });

    let filter = StreamFilter::from_params(&params);

    // --- 4. Subscribe to live BEFORE replay (no-miss window) -------------
    let live_rx = state.live.subscribe(project);

    // --- 5. Durable replay + gap detection -------------------------------
    let ring = ProjectRing::with_root(
        project,
        state.storage.project_object_store(&project),
        state.storage.root_prefix_handle().map(|p| p.to_string()),
    );
    let mut gap_frame: Option<Event> = None;
    let replay: Vec<CdcRecord> = if resume_after == 0 {
        Vec::new() // live-only subscription
    } else {
        let min_avail = ring.min_available_seq().await.unwrap_or(None);
        if let Some(min) = min_avail {
            // resume_after points before the oldest retained event → gap.
            if resume_after + 1 < min {
                warn!(
                    project = %project,
                    resume_after,
                    min_available_seq = min,
                    "cdc: resume cursor predates retention; emitting cursor_expired",
                );
                let body = serde_json::json!({
                    "type": "cursor_expired",
                    "resume_after": resume_after,
                    "min_available_seq": min,
                });
                gap_frame = Some(Event::default().data(body.to_string()));
            }
        }
        ring.replay_after(resume_after).await.unwrap_or_default()
    };

    debug!(
        project = %project,
        resume_after,
        replay_count = replay.len(),
        "cdc SSE subscriber connected",
    );

    // Track the highest seq delivered so the live phase de-dups the
    // replay/live overlap (an event can be in both the ring and the channel).
    let last_replayed = replay.last().map(|r| r.seq).unwrap_or(resume_after);

    // --- 6. Build the stream: gap → replay → live -----------------------
    let gap_stream = stream::iter(gap_frame.into_iter().map(Ok));

    let f_replay = filter.clone();
    let replay_stream = stream::iter(replay).filter_map(move |rec| {
        let f = f_replay.clone();
        async move {
            if !f.matches(&rec) {
                return None;
            }
            frame_for(&rec).map(Ok)
        }
    });

    let f_live = filter.clone();
    let live_stream = stream::unfold(
        (live_rx, last_replayed, f_live),
        move |(mut rx, mut high, f)| async move {
            loop {
                match rx.recv().await {
                    Ok(rec) => {
                        if rec.seq <= high {
                            continue; // already delivered in replay
                        }
                        high = rec.seq;
                        if !f.matches(&rec) {
                            continue;
                        }
                        let ev = frame_for(&rec)
                            .map(Ok)
                            .unwrap_or_else(|| Ok(Event::default().comment("serialize_error")));
                        return Some((ev, (rx, high, f)));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            lagged = n,
                            "cdc SSE live tail lagged; client should reconnect with Last-Event-ID"
                        );
                        let c = Ok(Event::default().comment(format!("lagged:{n}")));
                        return Some((c, (rx, high, f)));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    let stream = gap_stream.chain(replay_stream).chain(live_stream);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    ))
}

/// `GET /v1/cdc/:project/wal2json` — the resumable CDC stream rendered as
/// `wal2json` v2 per-row action objects (ADR 0028 Phase 4).
///
/// Identical auth / cross-project isolation / replay-then-live / cursor-gap
/// machinery as [`cdc_stream_handler`]; only the per-record frame differs
/// (`wal2json_frame_for`). A `wal2json`-shaped consumer parses each `data:`
/// frame as one `{"action":"I|U|D", ...}` object.
#[axum::debug_handler]
pub async fn cdc_wal2json_handler(
    State(state): State<CdcSseState>,
    Path(project_str): Path<String>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    // --- 1. Auth ---------------------------------------------------------
    let token = bearer(&headers)?;
    let claims = state
        .auth
        .verify_jwt(token)
        .map_err(|_| err(StatusCode::UNAUTHORIZED, "invalid or expired JWT"))?;

    // --- 2. Parse + isolate ----------------------------------------------
    let project: ProjectId = project_str
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "invalid project id in path"))?;
    if claims.project_id != project {
        return Err(err(
            StatusCode::FORBIDDEN,
            "JWT project does not match path project",
        ));
    }

    // --- 3. Cursor (query param or Last-Event-ID) ------------------------
    let resume_after: u64 = params.resume_after.unwrap_or_else(|| {
        headers
            .get("last-event-id")
            .or_else(|| headers.get("Last-Event-ID"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    });

    let filter = StreamFilter::from_params(&params);

    // --- 4. Subscribe to live BEFORE replay (no-miss window) -------------
    let live_rx = state.live.subscribe(project);

    // --- 5. Durable replay + gap detection -------------------------------
    let ring = ProjectRing::with_root(
        project,
        state.storage.project_object_store(&project),
        state.storage.root_prefix_handle().map(|p| p.to_string()),
    );
    let mut gap_frame: Option<Event> = None;
    let replay: Vec<CdcRecord> = if resume_after == 0 {
        Vec::new()
    } else {
        let min_avail = ring.min_available_seq().await.unwrap_or(None);
        if let Some(min) = min_avail {
            if resume_after + 1 < min {
                warn!(
                    project = %project,
                    resume_after,
                    min_available_seq = min,
                    "cdc wal2json: resume cursor predates retention; emitting cursor_expired",
                );
                let body = serde_json::json!({
                    "type": "cursor_expired",
                    "resume_after": resume_after,
                    "min_available_seq": min,
                });
                gap_frame = Some(Event::default().data(body.to_string()));
            }
        }
        ring.replay_after(resume_after).await.unwrap_or_default()
    };

    debug!(
        project = %project,
        resume_after,
        replay_count = replay.len(),
        "cdc wal2json SSE subscriber connected",
    );

    let last_replayed = replay.last().map(|r| r.seq).unwrap_or(resume_after);

    // --- 6. Build the stream: gap → replay → live -----------------------
    let gap_stream = stream::iter(gap_frame.into_iter().map(Ok));

    let f_replay = filter.clone();
    let replay_stream = stream::iter(replay).filter_map(move |rec| {
        let f = f_replay.clone();
        async move {
            if !f.matches(&rec) {
                return None;
            }
            wal2json_frame_for(&rec).map(Ok)
        }
    });

    let f_live = filter.clone();
    let live_stream = stream::unfold(
        (live_rx, last_replayed, f_live),
        move |(mut rx, mut high, f)| async move {
            loop {
                match rx.recv().await {
                    Ok(rec) => {
                        if rec.seq <= high {
                            continue;
                        }
                        high = rec.seq;
                        if !f.matches(&rec) {
                            continue;
                        }
                        let ev = wal2json_frame_for(&rec)
                            .map(Ok)
                            .unwrap_or_else(|| Ok(Event::default().comment("serialize_error")));
                        return Some((ev, (rx, high, f)));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(lagged = n, "cdc wal2json SSE live tail lagged; client should reconnect with Last-Event-ID");
                        let c = Ok(Event::default().comment(format!("lagged:{n}")));
                        return Some((c, (rx, high, f)));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    let stream = gap_stream.chain(replay_stream).chain(live_stream);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(table: &str, op: u8, seq: u64) -> CdcRecord {
        CdcRecord {
            seq,
            wal_lsn: 0,
            project: ProjectId::new(),
            table: table.to_string(),
            op,
            tx_id: None,
            committed_at: 0,
            causation_user: None,
            before: None,
            after: Some(serde_json::json!({"id": seq})),
        }
    }

    #[test]
    fn table_filter_matches_only_listed() {
        let p = StreamParams {
            tables: Some("orders,users".into()),
            ..Default::default()
        };
        let f = StreamFilter::from_params(&p);
        assert!(f.matches(&rec("orders", 1, 1)));
        assert!(f.matches(&rec("users", 1, 2)));
        assert!(!f.matches(&rec("widgets", 1, 3)));
    }

    #[test]
    fn op_filter_matches_only_listed() {
        let p = StreamParams {
            ops: Some("update,delete".into()),
            ..Default::default()
        };
        let f = StreamFilter::from_params(&p);
        assert!(!f.matches(&rec("t", 1, 1))); // insert
        assert!(f.matches(&rec("t", 2, 2))); // update
        assert!(f.matches(&rec("t", 3, 3))); // delete
    }

    #[test]
    fn empty_filter_matches_all() {
        let f = StreamFilter::from_params(&StreamParams::default());
        assert!(f.matches(&rec("anything", 1, 1)));
        assert!(f.matches(&rec("anything", 3, 2)));
    }

    #[test]
    fn frame_carries_id_and_data() {
        let e = frame_for(&rec("orders", 1, 99));
        assert!(e.is_some());
    }
}
