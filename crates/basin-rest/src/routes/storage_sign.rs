//! Signed-URL mint and verify for the object-storage surface (Phase 5.17.D).
//!
//! ## Routes
//!
//! ```text
//! POST /storage/v1/object/sign/upload/:bucket/*path          — mint (JWT-gated)
//! GET  /storage/v1/object/sign/:project/:bucket/*path        — download via signed URL (no JWT)
//! ```
//!
//! The `upload` literal on the POST route disambiguates it from the GET route
//! in axum's router: without a literal segment both paths begin with
//! `/storage/v1/object/sign/` followed by a path parameter, which axum cannot
//! distinguish at registration time (route conflict, fixes #55).
//!
//! ## Signing scheme
//!
//! HMAC-SHA256 over the canonical message:
//!
//! ```text
//! <project_id>\n<bucket>\n<path>\n<expires_unix_secs>
//! ```
//!
//! The key is a dedicated `BlobSigningSecret` held in the server state
//! (P2-1), rotatable independently of the JWT secret via
//! `POST /admin/v1/storage/signing-key/rotate`.
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
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

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
// Mint handler
// ---------------------------------------------------------------------------

/// `POST /storage/v1/object/sign/upload/:bucket/*path` — mint a time-boxed signed URL (JWT-gated).
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

    let req = body.map(|Json(r)| r).unwrap_or_else(|| SignRequest {
        expires_in: default_ttl_secs(),
    });
    let expires_in = req.expires_in.min(MAX_TTL_SECS).max(1);

    let now = Utc::now();
    let expires_ts = now.timestamp() + expires_in as i64;
    let expires_dt = chrono::DateTime::from_timestamp(expires_ts, 0)
        .ok_or_else(|| ApiError::internal("timestamp overflow"))?;

    let project_str = project.to_string();
    let mac_bytes = state
        .blob_signing_secret
        .compute_mac(&project_str, &bucket, &path, expires_ts);
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

    // Verify MAC — constant-time via BlobSigningSecret (P2-1).
    let provided =
        hex::decode(&q.token).map_err(|_| ApiError::forbidden("invalid token encoding"))?;

    if !state
        .blob_signing_secret
        .verify_mac(&project_str, &bucket, &path, q.expires, &provided)
    {
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
    use basin_blob::signing::BlobSigningSecret;

    const SECRET_A: &[u8] = b"test-secret-32-bytes-long-enough!";
    const SECRET_B: &[u8] = b"rotated-secret-32-bytes-or-more!!";

    fn make_secret() -> BlobSigningSecret {
        BlobSigningSecret::new(SECRET_A)
    }

    #[test]
    fn mac_round_trip() {
        let s = make_secret();
        let mac1 = s.compute_mac("proj1", "bucket", "path/to/file.txt", 9999999);
        let mac2 = s.compute_mac("proj1", "bucket", "path/to/file.txt", 9999999);
        assert_eq!(mac1, mac2, "deterministic");
    }

    #[test]
    fn mac_differs_on_project_change() {
        let s = make_secret();
        let mac1 = s.compute_mac("proj1", "bucket", "file.txt", 9999);
        let mac2 = s.compute_mac("proj2", "bucket", "file.txt", 9999);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn mac_differs_on_path_change() {
        let s = make_secret();
        let mac1 = s.compute_mac("proj", "bucket", "a.txt", 9999);
        let mac2 = s.compute_mac("proj", "bucket", "b.txt", 9999);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn mac_differs_on_expiry_change() {
        let s = make_secret();
        let mac1 = s.compute_mac("proj", "bucket", "file.txt", 1000);
        let mac2 = s.compute_mac("proj", "bucket", "file.txt", 1001);
        assert_ne!(mac1, mac2);
    }

    #[test]
    fn verify_round_trip() {
        let s = make_secret();
        let mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert!(s.verify_mac("proj", "bucket", "file.txt", 9999, &mac));
    }

    #[test]
    fn hex_encode_decode_round_trip() {
        let s = make_secret();
        let mac = s.compute_mac("p", "b", "f", 42);
        let encoded = hex::encode(&mac);
        let decoded = hex::decode(&encoded).unwrap();
        assert_eq!(mac, decoded);
    }

    #[test]
    fn tampered_token_fails_verify() {
        let s = make_secret();
        let mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        let mut tampered = mac.clone();
        tampered[0] ^= 0xFF;
        assert!(!s.verify_mac("proj", "bucket", "file.txt", 9999, &tampered));
    }

    // ── P2-1 rotation ─────────────────────────────────────────────────────

    /// Token minted before rotation must fail verification after rotation.
    #[test]
    fn rotation_invalidates_old_signed_url_token() {
        let s = make_secret();

        // Mint a token (simulating a signed URL being issued).
        let old_mac = s.compute_mac("proj", "my-bucket", "photos/cat.jpg", 9_999_999);
        assert!(
            s.verify_mac("proj", "my-bucket", "photos/cat.jpg", 9_999_999, &old_mac),
            "token must verify before rotation"
        );

        // Rotate to a new key — old URLs are now invalid.
        s.rotate(SECRET_B);

        assert!(
            !s.verify_mac("proj", "my-bucket", "photos/cat.jpg", 9_999_999, &old_mac),
            "token minted before rotation must be rejected after rotation (P2-1)"
        );
    }

    /// A freshly-signed URL (minted after rotation) must verify successfully.
    #[test]
    fn freshly_signed_url_verifies_after_rotation() {
        let s = make_secret();
        s.rotate(SECRET_B);

        let new_mac = s.compute_mac("proj", "my-bucket", "photos/cat.jpg", 9_999_999);
        assert!(
            s.verify_mac("proj", "my-bucket", "photos/cat.jpg", 9_999_999, &new_mac),
            "token minted after rotation must verify against the new key"
        );
    }
}
