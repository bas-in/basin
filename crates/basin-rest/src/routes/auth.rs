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
use basin_auth::oauth::PlaintextEncryption;
use basin_common::ProjectId;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

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
        .begin_oauth_authorize(
            None::<&PgStore>,
            &project_id,
            &provider,
            &q.redirect_to,
        )
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

    let enc = PlaintextEncryption;
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
    let enc = PlaintextEncryption;

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
    let enc = PlaintextEncryption;

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
    let enc = PlaintextEncryption;

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
