//! `POST /internal/v1/partition-write` — the partition-owner receive endpoint
//! for the multi-node bulk-ingest write path (#28).
//!
//! N Basin engine nodes share one project's object-store catalog + lease
//! registry + Tigris data. A bulk COPY lands on whichever node the client
//! connected to, but the write fan-out (`executor::write_batch_fanout`) spreads
//! a table's chunks across P partitions, each with ONE deterministic owner node
//! (see `basin_engine::partition_router`). When the sender node does not own a
//! partition it POSTs the already-parsed, already-constraint-checked Arrow
//! RecordBatch to THIS handler on the owner node, which writes it locally.
//!
//! # Wire format (must match `basin_engine::write_forwarder::HttpPartitionForwardClient`)
//!
//! * Body: a raw Arrow IPC stream (schema + exactly one RecordBatch), encoded by
//!   [`basin_engine::write_forwarder::encode_partition_batch`] and decoded here
//!   by [`basin_engine::write_forwarder::decode_partition_batch`]. No JSON / no
//!   base64 envelope — bulk batches are large.
//! * Headers carry the destination:
//!   - `x-basin-partition-project` — project ULID
//!   - `x-basin-partition-table`   — table name
//!   - `x-basin-partition-id`      — partition key
//!   - `x-basin-partition-hop`     — forward hop count; a forwarded write is
//!     always `1`. The receiver rejects `hop != 1` (loop guard: it must never
//!     re-forward, which would let A→B→A spin).
//!   - `x-basin-forward-secret`    — the shared secret (constant-time checked).
//! * Response: the rowcount written, as a bare JSON number (mirrors the
//!   sender's `text.trim().parse::<u64>()` decode).
//!
//! # Why no constraint re-check
//!
//! PK / UNIQUE / NOT NULL constraints already ran on the SENDER across ALL
//! partitions of the statement, before fan-out (see `write_batch_fanout`). The
//! receiver is by construction the partition's lease owner, so it does a RAW
//! partition write (`write_batch_opts(table, batch, durable=true)`) and does NOT
//! re-run constraints — re-checking here would be redundant work and, for a
//! cross-partition UNIQUE, would falsely fail (this node only sees its own
//! partition). Single-writer-per-partition is preserved by the lease CAS that
//! `shard.get` performs (`ensure_lease`): the owner holds the writer lease, so a
//! second node forwarding to the same partition cannot also write it.
//!
//! # Security
//!
//! Identical posture to [`crate::routes::internal_forward`]:
//!
//! * **Fail-closed mounting.** Mounted only when `BASIN_FORWARD_SECRET` is set
//!   (see [`crate::server::router`]). With no secret the path 404s.
//! * **Shared-secret check.** Every request must carry the secret in the
//!   `x-basin-forward-secret` header (constant-time compare). On Fly the
//!   endpoint is additionally reachable only over the private 6PN network.

use std::str::FromStr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::write_forwarder::{
    decode_partition_batch, FORWARD_SECRET_HEADER, PARTITION_FORWARD_HOP_HEADER,
    PARTITION_FORWARD_PARTITION_HEADER, PARTITION_FORWARD_PROJECT_HEADER,
    PARTITION_FORWARD_TABLE_HEADER,
};

use crate::errors::ApiError;
use crate::server::Inner;

/// Constant-time secret comparison so a timing side-channel can't reveal the
/// configured secret byte-by-byte. Mirrors `internal_forward::secrets_match`.
fn secrets_match(provided: &[u8], expected: &[u8]) -> bool {
    if provided.len() != expected.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (a, b) in provided.iter().zip(expected.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Verify the `x-basin-forward-secret` header against the configured secret.
/// Rejects (401) when the header is absent, non-ASCII, or mismatched. The
/// caller guarantees `expected` is `Some` (the route is only mounted with a
/// secret), but we re-check defensively and fail closed if it is ever `None`.
fn check_forward_secret(headers: &HeaderMap, expected: Option<&str>) -> Result<(), ApiError> {
    let expected = expected.ok_or_else(|| {
        ApiError::unauthenticated("partition-write endpoint is not configured (no shared secret)")
    })?;
    let provided = headers
        .get(FORWARD_SECRET_HEADER)
        .ok_or_else(|| ApiError::unauthenticated("missing forward secret header"))?
        .to_str()
        .map_err(|_| ApiError::unauthenticated("forward secret header is not ASCII"))?;
    if !secrets_match(provided.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::unauthenticated("forward secret mismatch"));
    }
    Ok(())
}

/// Read a required ASCII header, mapping absent / non-ASCII to a 400.
fn required_header<'h>(headers: &'h HeaderMap, name: &str) -> Result<&'h str, ApiError> {
    headers
        .get(name)
        .ok_or_else(|| ApiError::invalid(format!("missing {name} header")))?
        .to_str()
        .map_err(|_| ApiError::invalid(format!("{name} header is not ASCII")))
}

/// `POST /internal/v1/partition-write` — land a forwarded Arrow batch into the
/// partition this node owns. RAW write (no constraint re-check; see module
/// docs). Returns the rowcount as a bare JSON number.
pub(crate) async fn post_partition_write(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    check_forward_secret(&headers, state.forward_secret.as_deref())?;

    // Loop guard: a forwarded write carries hop=1. The receiver MUST land it
    // locally and never re-forward, so anything other than 1 is rejected
    // (prevents an A→B→A forward loop if owner resolution ever disagreed).
    let hop = required_header(&headers, PARTITION_FORWARD_HOP_HEADER)?;
    let hop: u32 = hop
        .trim()
        .parse()
        .map_err(|_| ApiError::invalid(format!("{PARTITION_FORWARD_HOP_HEADER} is not a number")))?;
    if hop != 1 {
        return Err(ApiError::invalid(format!(
            "partition-write hop must be 1 (got {hop}); the receiver never re-forwards"
        )));
    }

    let project = required_header(&headers, PARTITION_FORWARD_PROJECT_HEADER)?;
    let project = ProjectId::from_str(project)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_PROJECT_HEADER}: {e}")))?;
    let table = required_header(&headers, PARTITION_FORWARD_TABLE_HEADER)?;
    let table = TableName::new(table)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_TABLE_HEADER}: {e}")))?;
    let partition = required_header(&headers, PARTITION_FORWARD_PARTITION_HEADER)?;
    let partition = PartitionKey::new(partition)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_PARTITION_HEADER}: {e}")))?;

    let batch = decode_partition_batch(&body).map_err(ApiError::from)?;
    let rows = batch.num_rows() as u64;

    // The shard handle. `shard.get` performs the lease CAS (ensure_lease): this
    // node, the partition's deterministic owner, holds the writer lease, so the
    // RAW write below is the single writer for the partition.
    let shard = state.cfg.engine.shard().ok_or_else(|| {
        ApiError::internal("partition-write received but this node has no shard configured")
    })?;
    let handle = shard.get(&project, &partition).await.map_err(ApiError::from)?;
    // RAW partition write — durable. Constraints already ran on the SENDER
    // across all partitions before fan-out (see module docs); do NOT re-run.
    handle
        .write_batch_opts(&table, batch, true)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(rows)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn secret_headers(secret: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            FORWARD_SECRET_HEADER,
            HeaderValue::from_str(secret).unwrap(),
        );
        h
    }

    #[test]
    fn secret_match_is_constant_time_correct() {
        assert!(secrets_match(b"hunter2", b"hunter2"));
        assert!(!secrets_match(b"hunter2", b"hunter3"));
        assert!(!secrets_match(b"hunter2", b"hunter22"));
        assert!(!secrets_match(b"", b"x"));
        assert!(secrets_match(b"", b""));
    }

    #[test]
    fn missing_secret_header_rejected() {
        let headers = HeaderMap::new();
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn wrong_secret_rejected() {
        let headers = secret_headers("nope");
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn matching_secret_accepted() {
        let headers = secret_headers("s3cret");
        assert!(check_forward_secret(&headers, Some("s3cret")).is_ok());
    }

    #[test]
    fn no_configured_secret_fails_closed() {
        let headers = secret_headers("anything");
        let err = check_forward_secret(&headers, None).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn required_header_present_and_absent() {
        let mut h = HeaderMap::new();
        h.insert(PARTITION_FORWARD_TABLE_HEADER, HeaderValue::from_static("t"));
        assert_eq!(
            required_header(&h, PARTITION_FORWARD_TABLE_HEADER).unwrap(),
            "t"
        );
        let err = required_header(&h, PARTITION_FORWARD_PARTITION_HEADER).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::BAD_REQUEST);
    }
}
