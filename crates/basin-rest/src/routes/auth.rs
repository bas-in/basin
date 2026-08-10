//! `/auth/v1/*` handlers — thin axum wrappers over [`basin_auth::AuthService`].
//!
//! Each handler decodes a small request struct, calls into `AuthService`, and
//! returns either tokens or a sentinel `{ "ok": true }` body. Error mapping
//! is the standard [`ApiError`] flow.
//!
//! The intent is end-to-end usability: a frontend can `fetch('/auth/v1/...')`
//! to get a JWT, then `fetch('/rest/v1/...')` with that JWT in the
//! `Authorization` header, all on a single host.
//!
//! ## OAuth routes (Phase 5.10.O)
//!
//! - `GET  /auth/v1/oauth/:provider/authorize` — build PKCE/state URL → JSON
//!   `{redirect_url, state}` (client-side redirect or 302 per convention).
//! - `GET  /auth/v1/oauth/:provider/callback?code=&state=` — exchange code →
//!   issue Basin JWT + refresh; returns token body.
//!
//! ## Sign-out
//!
//! - `POST /auth/v1/signout` — revoke the presented refresh token server-side.
//!   Body: `{ "refresh_token": "..." }`. Returns `{ "ok": true }` on success.
//!   Requires a valid (at-least-parseable) refresh JWT in the body; a missing
//!   or structurally invalid token returns 401. Idempotent: revoking an already-
//!   revoked token also returns `{ "ok": true }`.
//!
//! ## MFA routes (Phase 5.10.M)
//!
//! - `POST   /auth/v1/factors`                          — enroll (TOTP or WebAuthn)
//! - `GET    /auth/v1/factors`                          — list factors for current user
//! - `POST   /auth/v1/factors/:id/verify`               — confirm enrollment
//! - `POST   /auth/v1/factors/:id/challenge`            — begin step-up challenge
//! - `POST   /auth/v1/factors/:id/challenge/verify`     — complete step-up → aal2 JWT
//! - `DELETE /auth/v1/factors/:id`                      — unenroll (requires aal2)

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_auth::oauth::EncryptionProvider;
use basin_common::{BasinError, ProjectId};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// AES-256-GCM encryption provider
// ---------------------------------------------------------------------------
//
// Implements `basin_auth::oauth::EncryptionProvider` using AES-256-GCM with
// a random 12-byte nonce per encrypt call.  The ciphertext format is:
//   base64url( nonce(12) || GCM-ciphertext-with-tag(len+16) )
// prefixed with `aes:` so future cipher migrations are detectable.
//
// Key source: `BASIN_AUTH_ENCRYPTION_KEY` env var — hex-encoded 32 bytes.
// Fail-closed: if the env var is absent or malformed, construction returns
// an error (callers surface this as a 500; server operators must set the key).

/// AES-256-GCM backed encryption for OAuth client secrets + MFA seeds.
struct AesGcmEncryption {
    key: [u8; 32],
}

/// Env var name for the AES-256-GCM root key (hex-encoded 32 bytes).
const AUTH_ENCRYPTION_KEY_ENV: &str = "BASIN_AUTH_ENCRYPTION_KEY";

impl AesGcmEncryption {
    /// Construct from `BASIN_AUTH_ENCRYPTION_KEY`.  Fails closed if absent.
    fn from_env() -> std::result::Result<Self, ApiError> {
        let hex_str = std::env::var(AUTH_ENCRYPTION_KEY_ENV).map_err(|_| {
            ApiError::internal(format!(
                "encryption key not configured: env var {AUTH_ENCRYPTION_KEY_ENV} is not set; \
                 set it to a 64-character lowercase hex string (32 random bytes)"
            ))
        })?;
        let bytes = hex::decode(hex_str.trim()).map_err(|e| {
            ApiError::internal(format!("{AUTH_ENCRYPTION_KEY_ENV} is not valid hex: {e}"))
        })?;
        if bytes.len() != 32 {
            return Err(ApiError::internal(format!(
                "{AUTH_ENCRYPTION_KEY_ENV} must be exactly 32 bytes (64 hex chars), got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self { key })
    }
}

impl EncryptionProvider for AesGcmEncryption {
    fn encrypt(&self, plaintext: &str) -> basin_common::Result<String> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
        use base64::Engine as _;
        use rand::RngCore;

        let mut nonce_bytes = [0u8; 12];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);

        let key = Key::<Aes256Gcm>::from_slice(&self.key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ct = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| BasinError::internal(format!("aes-gcm encrypt: {e}")))?;

        let mut payload = Vec::with_capacity(12 + ct.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&ct);

        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload);
        Ok(format!("aes:{encoded}"))
    }

    fn decrypt(&self, ciphertext: &str) -> basin_common::Result<String> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
        use base64::Engine as _;

        let b64 = ciphertext.strip_prefix("aes:").ok_or_else(|| {
            BasinError::internal(
                "AesGcmEncryption: ciphertext missing 'aes:' prefix (was it encrypted with a different provider?)"
                    .to_owned(),
            )
        })?;

        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64)
            .map_err(|e| BasinError::internal(format!("aes-gcm decrypt: base64 decode: {e}")))?;

        if payload.len() < 12 + 16 + 1 {
            return Err(BasinError::internal(format!(
                "aes-gcm decrypt: payload too short ({} bytes)",
                payload.len()
            )));
        }

        let (nonce_bytes, ct) = payload.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&self.key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plain = cipher.decrypt(nonce, ct).map_err(|_| {
            BasinError::internal(
                "aes-gcm decrypt: authentication failed (wrong key or tampered ciphertext)"
                    .to_owned(),
            )
        })?;

        String::from_utf8(plain)
            .map_err(|e| BasinError::internal(format!("aes-gcm decrypt: utf8: {e}")))
    }
}

/// Build the per-request encryption provider, failing closed if the key is
/// absent or misconfigured.
fn encryption_provider() -> std::result::Result<AesGcmEncryption, ApiError> {
    AesGcmEncryption::from_env()
}

/// Under `#[cfg(test)]` only: the plaintext passthrough so existing test
/// fixtures that register mock providers with `plain:` prefixes still work.
#[cfg(test)]
pub(crate) use basin_auth::oauth::PlaintextEncryption;

#[derive(Debug, Deserialize)]
pub(crate) struct SignupRequest {
    pub project_id: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SigninRequest {
    pub project_id: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SignoutRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VerifyEmailRequest {
    pub project_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PasswordResetRequest {
    pub project_id: String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PasswordResetConfirm {
    pub project_id: String,
    pub token: String,
    pub new_password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MagicLinkRequest {
    pub project_id: String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MagicLinkConfirm {
    pub project_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmailLinkRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmailLinkConsume {
    pub token: String,
}

fn parse_project(s: &str) -> Result<ProjectId, ApiError> {
    s.parse()
        .map_err(|_| ApiError::invalid(format!("invalid project_id: {s:?}")))
}

#[axum::debug_handler]
pub(crate) async fn signup(
    State(state): State<Arc<Inner>>,
    Json(req): Json<SignupRequest>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    let user = state
        .cfg
        .auth
        .signup(&project, &req.email, &req.password)
        .await
        .map_err(ApiError::from)?;
    let body = json!({ "ok": true, "user_id": user.to_string() });
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

#[axum::debug_handler]
pub(crate) async fn signin(
    State(state): State<Arc<Inner>>,
    Json(req): Json<SigninRequest>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    let toks = state
        .cfg
        .auth
        .signin(&project, &req.email, &req.password)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(token_body(&toks)).into_response())
}

#[axum::debug_handler]
pub(crate) async fn refresh(
    State(state): State<Arc<Inner>>,
    Json(req): Json<RefreshRequest>,
) -> Result<Response, ApiError> {
    match state.cfg.auth.refresh(&req.refresh_token).await {
        Ok(toks) => Ok(Json(token_body(&toks)).into_response()),
        Err(e) => {
            // Map the revoked-token sentinel message (set by basin-auth's
            // refresh flow) to a stable client-visible code.
            let msg = e.to_string();
            if msg.contains("revoked") {
                Err(ApiError::revoked_token(msg))
            } else {
                Err(ApiError::from(e))
            }
        }
    }
}

/// `POST /auth/v1/signout` — revoke the presented refresh token server-side.
///
/// Body: `{ "refresh_token": "..." }`. The token is written into
/// `auth_revoked_refresh_tokens` so that a subsequent `POST /auth/v1/refresh`
/// with the same token returns 401 `E_REVOKED_TOKEN`.
///
/// - **Idempotent**: revoking an already-revoked token returns `{ "ok": true }`.
/// - **Missing / invalid body**: a missing `refresh_token` field, an empty
///   string, or a token with an invalid signature / wrong audience returns
///   401 `E_UNAUTHENTICATED`. Callers that have no token to present are
///   already signed out; the response is consistent with that state.
/// - **Expired tokens accepted**: an expired but structurally valid token is
///   still revocable. The access token is *not* affected — its short TTL
///   (default 60 s) is the remaining exposure window.
#[axum::debug_handler]
pub(crate) async fn signout(
    State(state): State<Arc<Inner>>,
    Json(req): Json<SignoutRequest>,
) -> Result<Response, ApiError> {
    // Pre-validate structure (signature + audience, ignoring expiry). This
    // distinguishes a token we actually issued (revocable) from random bytes
    // (reject with 401). basin-auth's own `signout` is deliberately lenient
    // (malformed → Ok) for caller convenience; the HTTP contract is stricter.
    if req.refresh_token.is_empty() {
        return Err(ApiError::unauthenticated("refresh_token is required"));
    }
    state
        .cfg
        .auth
        .parse_refresh_token(&req.refresh_token)
        .map_err(|_| {
            ApiError::unauthenticated(
                "invalid refresh token: sign-out requires a token issued by this service",
            )
        })?;
    state
        .cfg
        .auth
        .signout(&req.refresh_token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
pub(crate) async fn verify_email(
    State(state): State<Arc<Inner>>,
    Json(req): Json<VerifyEmailRequest>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    state
        .cfg
        .auth
        .verify_email(&project, &req.token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
pub(crate) async fn request_password_reset(
    State(state): State<Arc<Inner>>,
    Json(req): Json<PasswordResetRequest>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    state
        .cfg
        .auth
        .request_password_reset(&project, &req.email)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
pub(crate) async fn reset_password(
    State(state): State<Arc<Inner>>,
    Json(req): Json<PasswordResetConfirm>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    state
        .cfg
        .auth
        .reset_password(&project, &req.token, &req.new_password)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

/// Legacy per-project magic-link request. The HTTP routes now point at the
/// project-agnostic email-link login; this handler stays for in-process
/// callers (and to keep the AuthService surface easy to reach from Rust
/// embeds). A future wedge customer that needs per-project magic links can
/// re-route it.
#[axum::debug_handler]
#[allow(dead_code)]
pub(crate) async fn request_magic_link(
    State(state): State<Arc<Inner>>,
    Json(req): Json<MagicLinkRequest>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    state
        .cfg
        .auth
        .request_magic_link(&project, &req.email)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
#[allow(dead_code)]
pub(crate) async fn signin_magic_link(
    State(state): State<Arc<Inner>>,
    Json(req): Json<MagicLinkConfirm>,
) -> Result<Response, ApiError> {
    let project = parse_project(&req.project_id)?;
    let toks = state
        .cfg
        .auth
        .signin_with_magic_link(&project, &req.token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(token_body(&toks)).into_response())
}

/// Project-agnostic email-link login (request). Body: `{ "email": "..." }`.
/// Always responds 204 (never confirm whether the email is known) unless
/// outbound mail is disabled, in which case 503 + `E_EMAIL_DISABLED`.
#[axum::debug_handler]
pub(crate) async fn request_email_link(
    State(state): State<Arc<Inner>>,
    Json(req): Json<EmailLinkRequest>,
) -> Result<Response, ApiError> {
    if !state.cfg.auth.is_email_enabled() {
        return Err(ApiError::email_disabled("outbound email is not configured"));
    }
    match state.cfg.auth.request_email_link(&req.email).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("E_EMAIL_DISABLED") {
                return Err(ApiError::email_disabled(msg));
            }
            // Any other failure path (validation, DB, mail send) is a 500-
            // class — surface as InvalidRequest if it's clearly a user
            // input issue, otherwise let `From<BasinError>` decide.
            Err(ApiError::from(e))
        }
    }
}

/// Consume a magic-link token. Single-use. Returns `{access_token,
/// refresh_token,...}` on success.
#[axum::debug_handler]
pub(crate) async fn consume_email_link(
    State(state): State<Arc<Inner>>,
    Json(req): Json<EmailLinkConsume>,
) -> Result<Response, ApiError> {
    let toks = state
        .cfg
        .auth
        .consume_email_link(&req.token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(token_body(&toks)).into_response())
}

fn token_body(t: &basin_auth::Tokens) -> serde_json::Value {
    json!({
        "access_token": t.access_token,
        "refresh_token": t.refresh_token,
        "access_expires_at": t.access_expires_at.to_rfc3339(),
        "refresh_expires_at": t.refresh_expires_at.to_rfc3339(),
    })
}

// --- API key management -------------------------------------------------
//
// All three endpoints are gated on the JWT (not on an API key) — minting
// or revoking a key with another key is a privilege-escalation footgun.

#[derive(Debug, Deserialize)]
pub(crate) struct CreateApiKeyRequest {
    pub name: String,
}

#[axum::debug_handler]
pub(crate) async fn create_api_key(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let issued = state
        .cfg
        .auth
        .issue_api_key(claims.user_id, &claims.project_id, &req.name)
        .await
        .map_err(ApiError::from)?;
    let body = json!({
        "id": issued.id,
        "name": issued.name,
        "secret": issued.secret,
        "created_at": issued.created_at.to_rfc3339(),
    });
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

#[axum::debug_handler]
pub(crate) async fn list_api_keys(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let keys = state
        .cfg
        .auth
        .list_api_keys(claims.user_id, &claims.project_id)
        .await
        .map_err(ApiError::from)?;
    let body: Vec<serde_json::Value> = keys
        .into_iter()
        .map(|d| {
            json!({
                "id": d.id,
                "name": d.name,
                "created_at": d.created_at.to_rfc3339(),
                "last_used_at": d.last_used_at.map(|t| t.to_rfc3339()),
                "revoked_at": d.revoked_at.map(|t| t.to_rfc3339()),
            })
        })
        .collect();
    Ok(Json(body).into_response())
}

#[axum::debug_handler]
pub(crate) async fn delete_api_key(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    state
        .cfg
        .auth
        .revoke_api_key(id, &claims.project_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

// ---------------------------------------------------------------------------
// OAuth routes (Phase 5.10.O)
// ---------------------------------------------------------------------------

/// Type alias for the `None` store — uses the in-memory `OAuthStateCache` /
/// `MfaCache` path. The `PostgresAuthStore` implements both `OAuthStore` and
/// `MfaStore`; passing `None::<&PostgresAuthStore>` satisfies the generic
/// bound while routing through the in-memory fallback that lives on `Inner`.
/// This is correct for single-process in-process deployments; multi-replica
/// Postgres-backed paths require injecting a real store (future work).
type PgStore = basin_auth::store::postgres::PostgresAuthStore;

/// Query parameters for `GET /auth/v1/oauth/:provider/authorize`.
#[derive(Debug, Deserialize)]
pub(crate) struct OAuthAuthorizeQuery {
    /// Project for which the OAuth flow is being initiated. Required so we
    /// can look up the provider config and sign the CSRF state correctly.
    pub project_id: String,
    /// URL to redirect to after the flow completes. Validated against the
    /// per-project allowlist stored in `{schema}_oauth_providers.redirect_uri`.
    #[serde(default)]
    pub redirect_to: String,
}

/// Query parameters for `GET /auth/v1/oauth/:provider/callback`.
#[derive(Debug, Deserialize)]
pub(crate) struct OAuthCallbackQuery {
    /// Authorization code from the provider.
    pub code: String,
    /// CSRF state echoed back by the provider.
    pub state: String,
    /// The callback URL used in the original authorize request. Must match
    /// what was passed as `redirect_to` during authorize so the PKCE verifier
    /// is valid.
    #[serde(default)]
    pub redirect_uri: String,
    /// Provider hint for the callback (same as the path param).
    #[serde(default)]
    pub provider: String,
}

/// `GET /auth/v1/oauth/:provider/authorize?project_id=&redirect_to=`
///
/// Builds the provider authorize URL (with PKCE + signed CSRF state) and
/// returns it as JSON. The client should redirect the browser to
/// `redirect_url`.
///
/// Response: `{ "redirect_url": "https://provider.com/oauth/authorize?...", "state": "..." }`
///
/// The `oauth_store` is `None` — in-memory `OAuthStateCache` is used.
/// For Postgres-backed multi-replica setups, inject a real `OAuthStore`.
#[axum::debug_handler]
pub(crate) async fn oauth_authorize(
    State(state): State<Arc<Inner>>,
    Path(provider): Path<String>,
    Query(q): Query<OAuthAuthorizeQuery>,
) -> Result<Response, ApiError> {
    if provider.is_empty() || provider.len() > 64 {
        return Err(ApiError::invalid("invalid provider name"));
    }
    let project_id = parse_project(&q.project_id)?;

    let result = state
        .cfg
        .auth
        .begin_oauth_authorize(None::<&PgStore>, &project_id, &provider, &q.redirect_to)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(json!({
        "redirect_url": result.redirect_url,
        "state": result.state,
    }))
    .into_response())
}

/// `GET /auth/v1/oauth/:provider/callback?code=&state=`
///
/// Handles the OAuth provider redirect-back. Validates state + HMAC,
/// exchanges the code (with PKCE verifier), fetches userinfo, links/creates
/// the user, and issues Basin JWT + refresh tokens.
///
/// Response: same token body as `/auth/v1/signin`.
#[axum::debug_handler]
pub(crate) async fn oauth_callback(
    State(state): State<Arc<Inner>>,
    Path(provider): Path<String>,
    Query(q): Query<OAuthCallbackQuery>,
) -> Result<Response, ApiError> {
    if provider.is_empty() || provider.len() > 64 {
        return Err(ApiError::invalid("invalid provider name"));
    }

    let enc = encryption_provider()?;
    let result = state
        .cfg
        .auth
        .handle_oauth_callback(
            &enc,
            None::<&PgStore>,
            &provider,
            &q.code,
            &q.state,
            &q.redirect_uri,
        )
        .await
        .map_err(ApiError::from)?;

    Ok(Json(token_body(&result.tokens)).into_response())
}

// ---------------------------------------------------------------------------
// MFA routes (Phase 5.10.M)
// ---------------------------------------------------------------------------

fn parse_uuid(s: &str, label: &str) -> Result<Uuid, ApiError> {
    s.parse::<Uuid>()
        .map_err(|_| ApiError::invalid(format!("invalid {label}: {s:?}")))
}

/// Request body for `POST /auth/v1/factors` (enroll a new factor).
#[derive(Debug, Deserialize)]
pub(crate) struct EnrollFactorRequest {
    /// `"totp"` or `"webauthn"`.
    pub factor_type: String,
    /// Human-readable label (e.g. "Authenticator App", "YubiKey 5").
    #[serde(default)]
    pub friendly_name: String,
}

/// Request body for `POST /auth/v1/factors/:id/verify` (confirm enrollment).
#[derive(Debug, Deserialize)]
pub(crate) struct VerifyFactorRequest {
    /// TOTP: the 6-digit OTP code from the authenticator app.
    #[serde(default)]
    pub code: Option<String>,
    /// WebAuthn: attestation response JSON from `navigator.credentials.create()`.
    #[serde(default)]
    pub attestation: Option<String>,
    /// WebAuthn: the `challenge_id` returned by `POST /auth/v1/factors`.
    #[serde(default)]
    pub challenge_id: Option<String>,
}

/// Request body for `POST /auth/v1/factors/:id/challenge/verify`.
#[derive(Debug, Deserialize)]
pub(crate) struct ChallengeVerifyRequest {
    /// The `challenge_id` returned by `POST /auth/v1/factors/:id/challenge`.
    pub challenge_id: String,
    /// TOTP: 6-digit OTP code.
    #[serde(default)]
    pub code: Option<String>,
    /// WebAuthn: assertion response JSON from `navigator.credentials.get()`.
    #[serde(default)]
    pub assertion: Option<String>,
}

/// `POST /auth/v1/factors` — enroll a new MFA factor.
///
/// **TOTP**: body `{ "factor_type": "totp", "friendly_name": "..." }`.
/// Returns `{ factor_id, factor_type, secret_b32, otpauth_uri }`.
///
/// **WebAuthn**: body `{ "factor_type": "webauthn", "friendly_name": "..." }`.
/// Returns `{ factor_id, factor_type, challenge_id, creation_options_json }`.
///
/// The factor is `unverified` until `POST /auth/v1/factors/:id/verify`.
#[axum::debug_handler]
pub(crate) async fn enroll_factor(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(req): Json<EnrollFactorRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let enc = encryption_provider()?;

    let body = match req.factor_type.as_str() {
        "totp" => {
            let enr = state
                .cfg
                .auth
                .enroll_totp(
                    None::<&PgStore>,
                    &enc,
                    &claims.project_id,
                    claims.user_id,
                    &req.friendly_name,
                )
                .await
                .map_err(ApiError::from)?;
            json!({
                "factor_id": enr.factor_id.to_string(),
                "factor_type": "totp",
                "secret_b32": enr.secret_b32,
                "otpauth_uri": enr.otpauth_uri,
            })
        }
        "webauthn" => {
            let enr = state
                .cfg
                .auth
                .enroll_webauthn(
                    None::<&PgStore>,
                    &claims.project_id,
                    claims.user_id,
                    &req.friendly_name,
                )
                .await
                .map_err(ApiError::from)?;
            json!({
                "factor_id": enr.factor_id.to_string(),
                "factor_type": "webauthn",
                "challenge_id": enr.challenge_id.to_string(),
                "creation_options_json": enr.creation_options_json,
            })
        }
        other => {
            return Err(ApiError::invalid(format!(
                "unknown factor_type {other:?}; expected \"totp\" or \"webauthn\""
            )))
        }
    };

    Ok((StatusCode::CREATED, Json(body)).into_response())
}

/// `GET /auth/v1/factors` — list MFA factors for the current user.
///
/// Returns an array of factor descriptors (id, type, status, friendly_name,
/// timestamps). The secret/credential is never included.
#[axum::debug_handler]
pub(crate) async fn list_factors(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let factors = state
        .cfg
        .auth
        .list_factors(None::<&PgStore>, &claims.project_id, claims.user_id)
        .await
        .map_err(ApiError::from)?;
    let body: Vec<serde_json::Value> = factors
        .into_iter()
        .map(|f| {
            json!({
                "id": f.id.to_string(),
                "factor_type": f.factor_type.to_string(),
                "status": f.status.to_string(),
                "friendly_name": f.friendly_name,
                "created_at": f.created_at.to_rfc3339(),
                "updated_at": f.updated_at.to_rfc3339(),
            })
        })
        .collect();
    Ok(Json(body).into_response())
}

/// `POST /auth/v1/factors/:id/verify` — confirm factor enrollment.
///
/// **TOTP**: body `{ "code": "123456" }`.
/// **WebAuthn**: body `{ "attestation": "<json>", "challenge_id": "<uuid>" }`.
///
/// On first verified factor, also returns `recovery_codes` (plaintext,
/// single-issue — client must store them).
#[axum::debug_handler]
pub(crate) async fn verify_factor(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(factor_id_str): Path<String>,
    Json(req): Json<VerifyFactorRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let factor_id = parse_uuid(&factor_id_str, "factor_id")?;
    let enc = encryption_provider()?;

    let recovery_codes = if let Some(attestation) = req.attestation.as_deref() {
        // WebAuthn path.
        let challenge_id_str = req
            .challenge_id
            .as_deref()
            .ok_or_else(|| ApiError::invalid("challenge_id required for webauthn verify"))?;
        let challenge_id = parse_uuid(challenge_id_str, "challenge_id")?;
        state
            .cfg
            .auth
            .verify_webauthn_factor(
                None::<&PgStore>,
                &enc,
                &claims.project_id,
                claims.user_id,
                factor_id,
                challenge_id,
                attestation,
            )
            .await
            .map_err(ApiError::from)?
    } else {
        // TOTP path.
        let code = req
            .code
            .as_deref()
            .ok_or_else(|| ApiError::invalid("code required for totp verify"))?;
        state
            .cfg
            .auth
            .verify_totp_factor(
                None::<&PgStore>,
                &enc,
                &claims.project_id,
                claims.user_id,
                factor_id,
                code,
            )
            .await
            .map_err(ApiError::from)?
    };

    let mut body = json!({ "ok": true });
    if let Some(codes) = recovery_codes {
        body["recovery_codes"] = json!(codes);
    }
    Ok(Json(body).into_response())
}

/// `POST /auth/v1/factors/:id/challenge` — begin a step-up challenge.
///
/// **TOTP**: returns `{ "challenge_id": "<uuid>" }`.
/// **WebAuthn**: returns `{ "challenge_id": "<uuid>", "request_options_json": "..." }`.
///
/// The response `challenge_id` is used in the subsequent
/// `POST /auth/v1/factors/:id/challenge/verify` call.
#[axum::debug_handler]
pub(crate) async fn begin_challenge(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(factor_id_str): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let factor_id = parse_uuid(&factor_id_str, "factor_id")?;

    // Try TOTP first (most common). On factor-type mismatch, fall back to
    // WebAuthn. The error text from mfa::require_factor_type is
    // "factor type mismatch: expected totp, got webauthn" — we detect it by
    // substring match to avoid cross-crate coupling on error variants.
    let totp_result = state
        .cfg
        .auth
        .begin_totp_challenge(
            None::<&PgStore>,
            &claims.project_id,
            claims.user_id,
            factor_id,
        )
        .await;

    match totp_result {
        Ok(challenge_id) => {
            Ok(Json(json!({ "challenge_id": challenge_id.to_string() })).into_response())
        }
        Err(e) if e.to_string().contains("factor type mismatch") => {
            let (challenge_id, options_json) = state
                .cfg
                .auth
                .begin_webauthn_challenge(
                    None::<&PgStore>,
                    &claims.project_id,
                    claims.user_id,
                    factor_id,
                )
                .await
                .map_err(ApiError::from)?;
            Ok(Json(json!({
                "challenge_id": challenge_id.to_string(),
                "request_options_json": options_json,
            }))
            .into_response())
        }
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `POST /auth/v1/factors/:id/challenge/verify` — complete step-up → aal2 JWT.
///
/// **TOTP**: body `{ "challenge_id": "<uuid>", "code": "123456" }`.
/// **WebAuthn**: body `{ "challenge_id": "<uuid>", "assertion": "<json>" }`.
///
/// Returns the same token body as `/auth/v1/signin` but with `aal2` in the
/// JWT claims.
#[axum::debug_handler]
pub(crate) async fn verify_challenge(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(factor_id_str): Path<String>,
    Json(req): Json<ChallengeVerifyRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let _factor_id = parse_uuid(&factor_id_str, "factor_id")?;
    let challenge_id = parse_uuid(&req.challenge_id, "challenge_id")?;
    let enc = encryption_provider()?;

    let result = if let Some(code) = req.code.as_deref() {
        state
            .cfg
            .auth
            .verify_totp_challenge(
                None::<&PgStore>,
                &enc,
                &claims.project_id,
                claims.user_id,
                challenge_id,
                code,
            )
            .await
            .map_err(ApiError::from)?
    } else if let Some(assertion) = req.assertion.as_deref() {
        state
            .cfg
            .auth
            .verify_webauthn_challenge(
                None::<&PgStore>,
                &enc,
                &claims.project_id,
                claims.user_id,
                challenge_id,
                assertion,
            )
            .await
            .map_err(ApiError::from)?
    } else {
        return Err(ApiError::invalid(
            "one of `code` (TOTP) or `assertion` (WebAuthn) is required",
        ));
    };

    Ok(Json(token_body(&result.tokens)).into_response())
}

/// `DELETE /auth/v1/factors/:id` — unenroll an MFA factor.
///
/// Requires `aal2` in the caller's JWT (the AAL check is also enforced
/// inside `AuthService::unenroll_factor`).
#[axum::debug_handler]
pub(crate) async fn unenroll_factor(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(factor_id_str): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let factor_id = parse_uuid(&factor_id_str, "factor_id")?;

    state
        .cfg
        .auth
        .unenroll_factor(
            None::<&PgStore>,
            &claims.project_id,
            claims.user_id,
            factor_id,
            &claims.aal,
        )
        .await
        .map_err(ApiError::from)?;

    Ok(Json(json!({ "ok": true })).into_response())
}

// ---------------------------------------------------------------------------
// Unit tests for AesGcmEncryption
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{AesGcmEncryption, AUTH_ENCRYPTION_KEY_ENV};
    use basin_auth::oauth::EncryptionProvider;
    use std::sync::Mutex;

    /// A known 32-byte test key as a hex string (64 chars).
    const TEST_KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    /// Process-wide mutex so tests that mutate the env var don't race each
    /// other.  Cargo runs unit tests on multiple threads; any test that
    /// touches `AUTH_ENCRYPTION_KEY_ENV` must hold this lock.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Set the test key and return the guard (released when dropped).
    fn set_test_key() -> std::sync::MutexGuard<'static, ()> {
        let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(AUTH_ENCRYPTION_KEY_ENV, TEST_KEY_HEX);
        guard
    }

    fn make_enc() -> AesGcmEncryption {
        AesGcmEncryption::from_env().expect("test key should be valid")
    }

    // ------------------------------------------------------------------
    // P0: encrypted output must NOT be plaintext
    // ------------------------------------------------------------------

    #[test]
    fn oauth_secret_is_not_stored_as_plaintext() {
        let _g = set_test_key();
        let enc = make_enc();
        let secret = "super_secret_oauth_client_secret";

        let ct = enc.encrypt(secret).expect("encrypt must succeed");

        // Must NOT have the `plain:` prefix that PlaintextEncryption would emit.
        assert!(
            !ct.starts_with("plain:"),
            "ciphertext must NOT start with 'plain:' prefix; got: {ct:?}"
        );

        // Must NOT contain the plaintext verbatim anywhere in the ciphertext string.
        assert!(
            !ct.contains(secret),
            "ciphertext must NOT contain the plaintext secret verbatim; got: {ct:?}"
        );

        // Should use our `aes:` prefix.
        assert!(
            ct.starts_with("aes:"),
            "ciphertext must start with 'aes:' prefix; got: {ct:?}"
        );

        // Ciphertext must differ from the input.
        assert_ne!(ct.as_str(), secret, "ciphertext must differ from plaintext");
    }

    #[test]
    fn mfa_seed_is_not_stored_as_plaintext() {
        let _g = set_test_key();
        let enc = make_enc();
        let totp_seed = "JBSWY3DPEHPK3PXP"; // example base32 TOTP seed

        let ct = enc.encrypt(totp_seed).expect("encrypt must succeed");

        assert!(
            !ct.starts_with("plain:"),
            "MFA seed ciphertext must NOT have 'plain:' prefix; got: {ct:?}"
        );
        assert!(
            !ct.contains(totp_seed),
            "MFA seed must NOT appear verbatim in ciphertext; got: {ct:?}"
        );
        assert!(
            ct.starts_with("aes:"),
            "MFA seed ciphertext must start with 'aes:' prefix; got: {ct:?}"
        );
    }

    // ------------------------------------------------------------------
    // Round-trip: encrypt → decrypt recovers the original
    // ------------------------------------------------------------------

    #[test]
    fn encrypt_decrypt_round_trip_oauth_secret() {
        let _g = set_test_key();
        let enc = make_enc();
        let original = "my-oauth-client-secret-abc123";

        let ct = enc.encrypt(original).expect("encrypt");
        let recovered = enc.decrypt(&ct).expect("decrypt");

        assert_eq!(
            recovered, original,
            "decrypt(encrypt(x)) must equal x for OAuth secrets"
        );
    }

    #[test]
    fn encrypt_decrypt_round_trip_mfa_seed() {
        let _g = set_test_key();
        let enc = make_enc();
        let original = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP";

        let ct = enc.encrypt(original).expect("encrypt");
        let recovered = enc.decrypt(&ct).expect("decrypt");

        assert_eq!(
            recovered, original,
            "decrypt(encrypt(x)) must equal x for MFA seeds"
        );
    }

    // ------------------------------------------------------------------
    // Each encrypt call produces a unique ciphertext (random nonce)
    // ------------------------------------------------------------------

    #[test]
    fn two_encryptions_of_same_plaintext_differ() {
        let _g = set_test_key();
        let enc = make_enc();
        let plain = "same-secret";

        let ct1 = enc.encrypt(plain).expect("encrypt 1");
        let ct2 = enc.encrypt(plain).expect("encrypt 2");

        assert_ne!(
            ct1, ct2,
            "two encrypt calls on the same plaintext must produce different ciphertexts (random nonce)"
        );
    }

    // ------------------------------------------------------------------
    // Tampered ciphertext fails authentication
    // ------------------------------------------------------------------

    #[test]
    fn tampered_ciphertext_fails_decrypt() {
        let _g = set_test_key();
        let enc = make_enc();
        let ct = enc.encrypt("some value").expect("encrypt");

        // Corrupt the last byte of the base64 payload by appending garbage.
        let bad = format!("{ct}XXXX");
        let result = enc.decrypt(&bad);
        assert!(
            result.is_err(),
            "tampered ciphertext must fail authentication"
        );
    }

    // ------------------------------------------------------------------
    // Wrong key fails authentication
    // ------------------------------------------------------------------

    #[test]
    fn wrong_key_fails_decrypt() {
        let _g = set_test_key();
        let enc_a = make_enc();
        let ct = enc_a.encrypt("value").expect("encrypt");

        // Build a second provider with a different key (still under the lock).
        let other_hex = "fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0";
        std::env::set_var(AUTH_ENCRYPTION_KEY_ENV, other_hex);
        let enc_b = AesGcmEncryption::from_env().expect("enc_b");

        let result = enc_b.decrypt(&ct);
        assert!(
            result.is_err(),
            "decrypting with a different key must fail authentication"
        );
    }

    // ------------------------------------------------------------------
    // Missing env var fails closed (returns error, NOT plaintext fallback)
    // ------------------------------------------------------------------

    #[test]
    fn missing_env_var_fails_closed() {
        // Acquire the lock, then remove the var while holding it so no other
        // env-var-touching test can race us.
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var(AUTH_ENCRYPTION_KEY_ENV);

        let result = AesGcmEncryption::from_env();
        match result {
            Ok(_) => panic!(
                "absent BASIN_AUTH_ENCRYPTION_KEY must return an error, not fall back to plaintext"
            ),
            Err(err) => {
                assert!(
                    err.message.contains(AUTH_ENCRYPTION_KEY_ENV),
                    "error message should name the missing env var: {}",
                    err.message
                );
            }
        }
    }
}
