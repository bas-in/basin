//! `POST /internal/v1/forward` — the home-region receive endpoint for the
//! multi-region `http-forward` write path (ADR 0009).
//!
//! When a write lands in a non-home region and `BASIN_WRITE_FORWARD_MODE=http-forward`
//! is set, the originating engine POSTs the verbatim statement (+ the principal
//! that resolves `current_user` for RLS) to THIS engine — the project's home
//! region — over Fly's private 6PN `.internal` network. This handler executes
//! the statement locally as that principal and returns the home region's
//! [`basin_engine::ExecResult`] serialised as a
//! [`basin_engine::write_forwarder::ForwardExecResult`].
//!
//! # Security
//!
//! This endpoint executes arbitrary SQL as an arbitrary principal, so it is the
//! most sensitive surface in the REST app. It is protected two ways:
//!
//! * **Fail-closed mounting.** The route is only added to the router when
//!   `BASIN_FORWARD_SECRET` is set (see [`crate::server::router`]). With no
//!   secret the path does not exist at all (404) — default deployments are
//!   byte-identical to before this feature.
//! * **Shared-secret check.** Every request must carry the secret in the
//!   `x-basin-forward-secret` header; a missing / mismatched secret is rejected
//!   with 401 before any SQL runs. The comparison is constant-time.
//!
//! On Fly the endpoint is additionally reachable only over the private 6PN
//! network (the public edge never routes `/internal/*`); the shared secret is
//! defence-in-depth on top of that network isolation.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_engine::write_forwarder::{ForwardExecResult, ForwardRequest, FORWARD_SECRET_HEADER};

use crate::errors::ApiError;
use crate::server::Inner;

/// Constant-time secret comparison so a timing side-channel can't reveal the
/// configured secret byte-by-byte.
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
        ApiError::unauthenticated("forward endpoint is not configured (no shared secret)")
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

/// `POST /internal/v1/forward` — execute a forwarded auto-commit statement in
/// this (home) region as the supplied principal and return its result.
pub(crate) async fn post_forward(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(req): Json<ForwardRequest>,
) -> Result<Response, ApiError> {
    check_forward_secret(&headers, state.forward_secret.as_deref())?;

    // Open a session bound to the project + principal so SQL's `current_user`
    // (and therefore RLS) resolves exactly as it would for a direct client of
    // the home region.
    let session = state
        .cfg
        .engine
        .open_session_as(req.project_id, req.current_user)
        .await
        .map_err(ApiError::from)?;
    let res = session.execute(&req.sql).await.map_err(ApiError::from)?;

    let wire = ForwardExecResult::from_exec_result(&res).map_err(ApiError::from)?;
    Ok((StatusCode::OK, Json(wire)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn secret_match_is_constant_time_correct() {
        assert!(secrets_match(b"hunter2", b"hunter2"));
        assert!(!secrets_match(b"hunter2", b"hunter3"));
        assert!(!secrets_match(b"hunter2", b"hunter22")); // length differs
        assert!(!secrets_match(b"", b"x"));
        assert!(secrets_match(b"", b""));
    }

    #[test]
    fn missing_secret_header_rejected() {
        let headers = HeaderMap::new();
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        // Unauthenticated maps to 401.
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn wrong_secret_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(FORWARD_SECRET_HEADER, HeaderValue::from_static("nope"));
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn matching_secret_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert(FORWARD_SECRET_HEADER, HeaderValue::from_static("s3cret"));
        assert!(check_forward_secret(&headers, Some("s3cret")).is_ok());
    }

    #[test]
    fn no_configured_secret_fails_closed() {
        // Defensive: even if the route were reachable without a configured
        // secret, the check rejects (the route should not be mounted at all).
        let mut headers = HeaderMap::new();
        headers.insert(FORWARD_SECRET_HEADER, HeaderValue::from_static("anything"));
        let err = check_forward_secret(&headers, None).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }
}
