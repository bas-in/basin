//! Signed-URL mint and verify for the object-storage surface (Phase 5.17.D).
//!
//! ## Routes
//!
//! ```text
//! POST /storage/v1/object/sign/:bucket/*path   — mint (JWT-gated)
//! GET  /storage/v1/object/sign/:bucket/*path   — download via signed URL (no JWT)
//! ```
//!
//! ## Signing scheme
//!
//! HMAC-SHA256 over the canonical message:
//!
//! ```text
//! <project_id>\n<bucket>\n<path>\n<expires_unix_secs>
//! ```
//!
//! The key is the server's JWT secret (`AuthService::signing_secret()`), so no
//! additional secret material needs to be provisioned.
//!
//! The token is hex-encoded and travels as a `?token=<hex>&expires=<ts>`
//! query-string pair on the download URL.
//!
//! ## Security properties
//!
//! - Token is bound to `(project, bucket, path, expiry)` — changing any field
//!   invalidates the MAC.
//! - Verification uses `subtle::ConstantTimeEq` to prevent timing attacks.
//! - Past-expiry tokens → 403 (expired, not a tamper indication).
//! - Tampered token bytes → 403 (MAC mismatch).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum TTL a caller may request (1 week). Prevents minting tokens that
/// never effectively expire despite the time-box contract.
const MAX_TTL_SECS: u64 = 7 * 24 * 3600;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct SignRequest {
    /// Token lifetime in seconds. Capped at [`MAX_TTL_SECS`].
    #[serde(default = "default_ttl_secs")]
    pub expires_in: u64,
}

fn default_ttl_secs() -> u64 {
    3600 // 1 hour default
}

#[derive(Debug, Serialize)]
pub(crate) struct SignResponse {
    #[serde(rename = "signedUrl")]
    pub signed_url: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String, // RFC 3339
}

#[derive(Debug, Deserialize)]
pub(crate) struct SignedDownloadQuery {
    pub token: String,
    pub expires: i64, // Unix timestamp (seconds)
}

// ---------------------------------------------------------------------------
// HMAC helpers
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256 over the canonical `(project, bucket, path, expiry)` tuple.
fn compute_mac(secret: &[u8], project: &str, bucket: &str, path: &str, expires: i64) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(project.as_bytes());
    mac.update(b"\n");
    mac.update(bucket.as_bytes());
    mac.update(b"\n");
    mac.update(path.as_bytes());
    mac.update(b"\n");
    mac.update(expires.to_string().as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time comparison of two byte slices.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

// ---------------------------------------------------------------------------
// Mint handler
// ---------------------------------------------------------------------------

/// `POST /storage/v1/object/sign/:bucket/*path` — mint a time-boxed signed URL (JWT-gated).
///
/// Body: `{ "expires_in": <seconds> }` (optional; defaults to 3600).
/// Returns: `{ "signedUrl": "...", "expiresAt": "<rfc3339>" }`.
#[axum::debug_handler]
pub(crate) async fn sign_object(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((bucket, path)): Path<(String, String)>,
    body: Option<Json<SignRequest>>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = claims.project_id;

    let req = body
        .map(|Json(r)| r)
        .unwrap_or_else(|| SignRequest { expires_in: default_ttl_secs() });
    let expires_in = req.expires_in.min(MAX_TTL_SECS).max(1);

    let now = Utc::now();
    let expires_ts = now.timestamp() + expires_in as i64;
    let expires_dt = chrono::DateTime::from_timestamp(expires_ts, 0)
        .ok_or_else(|| ApiError::internal("timestamp overflow"))?;

    let secret = state.cfg.auth.signing_secret();
    let project_str = project.to_string();
    let mac_bytes = compute_mac(secret, &project_str, &bucket, &path, expires_ts);
    let token_hex = hex::encode(&mac_bytes);

    // Build the signed URL. The project ID is embedded in the path so the
    // verify endpoint can identify the project without a bearer token.
    // Shape: /storage/v1/object/sign/:project/:bucket/*path?token=<hex>&expires=<ts>
    // We use a dedicated project-scoped path to avoid ambiguity with the
    // authenticated :bucket/*path routes.
    let signed_url = format!(
        "/storage/v1/object/sign/{project}/{bucket}/{path}?token={token_hex}&expires={expires_ts}"
    );

    Ok(Json(SignResponse {
        signed_url,
        expires_at: expires_dt.to_rfc3339(),
    })
    .into_response())
}

// ---------------------------------------------------------------------------
// Verify + download handler
// ---------------------------------------------------------------------------

/// `GET /storage/v1/object/sign/:project/:bucket/*path?token=<hex>&expires=<ts>` — no JWT.
///
/// Verifies the HMAC token against `(project, bucket, path, expires)` using
/// constant-time comparison. Serves the object bytes on success; 403 on any
/// verification failure (expired or tampered).
#[axum::debug_handler]
pub(crate) async fn download_signed_object(
    State(state): State<Arc<Inner>>,
    Path((project_str, bucket, path)): Path<(String, String, String)>,
    Query(q): Query<SignedDownloadQuery>,
) -> Result<Response, ApiError> {
    // Parse project ID.
    let project: basin_common::ProjectId = project_str
        .parse()
        .map_err(|_| ApiError::not_found(format!("project not found: {project_str}")))?;

    // Check expiry first (cheap, no MAC computation needed if already expired).
    let now_ts = Utc::now().timestamp();
    if now_ts > q.expires {
        return Err(ApiError::forbidden("signed URL has expired"));
    }

    // Verify MAC — constant-time.
    let secret = state.cfg.auth.signing_secret();
    let expected = compute_mac(secret, &project_str, &bucket, &path, q.expires);
    let provided = hex::decode(&q.token)
        .map_err(|_| ApiError::forbidden("invalid token encoding"))?;

    if !ct_eq(&expected, &provided) {
        return Err(ApiError::forbidden("invalid or tampered signed URL"));
    }

    // MAC is valid and not expired — serve the object.
    let (meta, data) = state
        .blob_store
        .get_object(&project, &bucket, &path)
        .await
        .map_err(super::storage::blob_err_to_api_pub)?;

    let mut builder = axum::http::Response::builder().status(StatusCode::OK);
    if let Some(ref ct) = meta.mime_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/octet-stream");
    }
    builder = builder.header(header::ETAG, &meta.etag);
    builder = builder.header(header::CONTENT_LENGTH, data.len().to_string());

    let response = builder
        .body(axum::body::Body::from(data))
        .map_err(|e| ApiError::internal(format!("response build error: {e}")))?;
    Ok(response)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-32-bytes-long-enough!";

    #[test]
    fn mac_round_trip() {
        let mac1 = compute_mac(SECRET, "proj1", "bucket", "path/to/file.txt", 9999999);
        let mac2 = compute_mac(SECRET, "proj1", "bucket", "path/to/file.txt", 9999999);
        assert_eq!(mac1, mac2, "deterministic");
    }

    #[test]
    fn mac_differs_on_project_change() {
        let mac1 = compute_mac(SECRET, "proj1", "bucket", "file.txt", 9999);
        let mac2 = compute_mac(SECRET, "proj2", "bucket", "file.txt", 9999);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn mac_differs_on_path_change() {
        let mac1 = compute_mac(SECRET, "proj", "bucket", "a.txt", 9999);
        let mac2 = compute_mac(SECRET, "proj", "bucket", "b.txt", 9999);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn mac_differs_on_expiry_change() {
        let mac1 = compute_mac(SECRET, "proj", "bucket", "file.txt", 1000);
        let mac2 = compute_mac(SECRET, "proj", "bucket", "file.txt", 1001);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn ct_eq_same_bytes() {
        assert!(ct_eq(b"hello", b"hello"));
    }

    #[test]
    fn ct_eq_different_bytes() {
        assert!(!ct_eq(b"hello", b"world"));
    }

    #[test]
    fn ct_eq_different_lengths() {
        assert!(!ct_eq(b"hi", b"hello"));
    }

    #[test]
    fn hex_encode_decode_round_trip() {
        let mac = compute_mac(SECRET, "p", "b", "f", 42);
        let encoded = hex::encode(&mac);
        let decoded = hex::decode(&encoded).unwrap();
        assert_eq!(mac, decoded);
    }

    #[test]
    fn tampered_token_fails_ct_eq() {
        let mac = compute_mac(SECRET, "proj", "bucket", "file.txt", 9999);
        let mut tampered = mac.clone();
        tampered[0] ^= 0xFF;
        assert!(!ct_eq(&mac, &tampered));
    }
}
