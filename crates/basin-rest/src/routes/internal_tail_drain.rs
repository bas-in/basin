//! `POST /internal/v1/tail-drain` — the peer-side receive endpoint of the
//! multi-node metadata-aggregate READ BARRIER (mn2).
//!
//! N Basin engine nodes share one project's object-store catalog + cold data,
//! but each node's un-drained in-memory tail is visible ONLY to that node. A
//! `count(*)` / `max` answered from the shared catalog on node A therefore
//! silently omits rows sitting in node B's tail — the wrong-count class behind
//! the purge-confirm livelock (#70) and the restart under-count (#28). Before
//! answering a metadata aggregate on a multi-peer cluster, the querying node
//! POSTs to THIS handler on every non-self peer; the peer synchronously drains
//! its tail for `(project, table)` into committed catalog segments (visible to
//! everyone) and only then acks. On the querying side any failure here fails
//! the query LOUD (retryable) — never a wrong answer.
//!
//! # Wire format (must match
//! `basin_engine::write_forwarder::HttpPartitionForwardClient::drain_table_for_read`)
//!
//! * No body. Headers carry the target:
//!   - `x-basin-partition-project` — project ULID
//!   - `x-basin-partition-table`   — table name
//!   - `x-basin-partition-hop`     — must be `1` (loop guard: the receiver
//!     drains locally and never re-fans-out to its own peers).
//!   - `x-basin-forward-secret`    — the shared secret (constant-time checked).
//! * Responses:
//!   - `200` `{"had_tail":false}` — nothing resident for the table; O(1),
//!     RAM-only (the common clean-peer case).
//!   - `200` `{"had_tail":true}`  — tail rows existed and are NOW committed
//!     through the catalog (blocking, fail-loud drain).
//!   - `503` `{"code":"E_PARTITION_WARMING",...}` — the drain hit the typed
//!     retryable warming state; the client maps it back to
//!     `BasinError::PartitionWarming` (SQLSTATE 40001, retry).
//!   - `500` — the drain failed (e.g. wedged object-store PUT). The querying
//!     node MUST fail its aggregate: an un-acked drain means the count could
//!     be wrong.
//!
//! # Security
//!
//! Identical posture to [`crate::routes::internal_partition_forward`]:
//! fail-closed mounting (only when `BASIN_FORWARD_SECRET` is set; without it
//! the path 404s) + constant-time shared-secret check on every request. On Fly
//! the endpoint is additionally reachable only over the private 6PN network.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::{BasinError, ProjectId, TableName};
use basin_engine::write_forwarder::{
    TailDrainResponse, PARTITION_FORWARD_HOP_HEADER, PARTITION_FORWARD_PROJECT_HEADER,
    PARTITION_FORWARD_TABLE_HEADER, TAIL_DRAIN_WARMING_CODE,
};

use crate::errors::ApiError;
use crate::routes::internal_partition_forward::{check_forward_secret, required_header};
use crate::server::Inner;

/// `POST /internal/v1/tail-drain` — drain this node's in-memory tail for
/// `(project, table)` so a peer's metadata aggregate can trust the shared
/// catalog. Fail-closed on every ambiguous state: no secret / no shard / a
/// failed drain all surface as errors, because an optimistic `had_tail:false`
/// here becomes a silently-wrong count on the querying node.
pub(crate) async fn post_tail_drain(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    check_forward_secret(&headers, state.forward_secret.as_deref())?;

    // Loop guard, same contract as partition-write: a barrier drain carries
    // hop=1 and the receiver drains LOCALLY only — it must never fan out to
    // its own peers (A→B→A would spin, and the querying node already fans out
    // to every peer itself).
    let hop = required_header(&headers, PARTITION_FORWARD_HOP_HEADER)?;
    let hop: u32 = hop
        .trim()
        .parse()
        .map_err(|_| ApiError::invalid(format!("{PARTITION_FORWARD_HOP_HEADER} is not a number")))?;
    if hop != 1 {
        return Err(ApiError::invalid(format!(
            "tail-drain hop must be 1 (got {hop}); the receiver never re-fans-out"
        )));
    }

    let project = required_header(&headers, PARTITION_FORWARD_PROJECT_HEADER)?;
    let project = ProjectId::from_str(project)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_PROJECT_HEADER}: {e}")))?;
    let table = required_header(&headers, PARTITION_FORWARD_TABLE_HEADER)?;
    let table = TableName::new(table)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_TABLE_HEADER}: {e}")))?;

    // Fail closed on a shardless node: it cannot hold (or drain) a tail, but a
    // node reachable through BASIN_SHARD_PEERS without a shard is a
    // misconfiguration the querying side must hear about, not an implicit
    // "nothing here" (which would be indistinguishable from a real clean ack).
    let shard = state.cfg.engine.shard().ok_or_else(|| {
        ApiError::internal("tail-drain received but this node has no shard configured")
    })?;

    // TODO(mn2 warming gate): also refuse with 503 E_PARTITION_WARMING while
    // boot recovery (`recover_all`) has not yet completed on this node — a
    // node whose WAL-durable-but-uncompacted tail is not RESIDENT yet answers
    // `had_tail:false` here even though it holds unrecovered rows (the #67
    // no-serve-until-converged class). Needs a boot-recovery AtomicBool
    // plumbed from `main.rs` (recover_all success is currently non-fatal);
    // deferred to the server wiring follow-up.

    // O(1) clean-peer fast path: no resident tail rows for the table → ack
    // without touching storage. This is what keeps the barrier cheap on an
    // idle cluster (one RTT per peer, RAM-only on each).
    if !shard.has_pending_tail(&project, &table).await {
        return Ok((StatusCode::OK, Json(TailDrainResponse { had_tail: false })).into_response());
    }

    // Dirty: BLOCKING, scoped, fail-loud drain (see
    // `Shard::flush_tables_for_read`). Only after the catalog commit do we ack
    // `had_tail:true` — the ack MEANS "the tail is now in committed segments".
    match shard.flush_tables_for_read(&project, &table).await {
        Ok(()) => {
            Ok((StatusCode::OK, Json(TailDrainResponse { had_tail: true })).into_response())
        }
        // Typed retryable warming (boot convergence / lease handoff in
        // flight): 503 + machine-readable code so the client re-surfaces the
        // typed `PartitionWarming` (SQLSTATE 40001) instead of a hard error.
        Err(BasinError::PartitionWarming(msg)) => Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "code": TAIL_DRAIN_WARMING_CODE,
                "detail": msg,
            })),
        )
            .into_response()),
        // Anything else (wedged PUT, catalog commit failure, …): 500. The
        // querying node fails its aggregate — loud beats wrong.
        Err(e) => Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("tail-drain failed: {e}"),
            })),
        )
            .into_response()),
    }
}
