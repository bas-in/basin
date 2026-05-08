//! `/auth/v1/*` handlers — thin axum wrappers over [`basin_auth::AuthService`].
//!
//! Each handler decodes a small request struct, calls into `AuthService`, and
//! returns either tokens or a sentinel `{ "ok": true }` body. Error mapping
//! is the standard [`ApiError`] flow.
//!
//! The intent is end-to-end usability: a frontend can `fetch('/auth/v1/...')`
//! to get a JWT, then `fetch('/rest/v1/...')` with that JWT in the
//! `Authorization` header, all on a single host.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::TenantId;
use serde::Deserialize;
use serde_json::json;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

#[derive(Debug, Deserialize)]
pub(crate) struct SignupRequest {
    pub tenant_id: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SigninRequest {
    pub tenant_id: String,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VerifyEmailRequest {
    pub tenant_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PasswordResetRequest {
    pub tenant_id: String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PasswordResetConfirm {
    pub tenant_id: String,
    pub token: String,
    pub new_password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MagicLinkRequest {
    pub tenant_id: String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MagicLinkConfirm {
    pub tenant_id: String,
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

fn parse_tenant(s: &str) -> Result<TenantId, ApiError> {
    s.parse()
        .map_err(|_| ApiError::invalid(format!("invalid tenant_id: {s:?}")))
}

#[axum::debug_handler]
pub(crate) async fn signup(
    State(state): State<Arc<Inner>>,
    Json(req): Json<SignupRequest>,
) -> Result<Response, ApiError> {
    let tenant = parse_tenant(&req.tenant_id)?;
    let user = state
        .cfg
        .auth
        .signup(&tenant, &req.email, &req.password)
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
    let tenant = parse_tenant(&req.tenant_id)?;
    let toks = state
        .cfg
        .auth
        .signin(&tenant, &req.email, &req.password)
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
    let tenant = parse_tenant(&req.tenant_id)?;
    state
        .cfg
        .auth
        .verify_email(&tenant, &req.token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
pub(crate) async fn request_password_reset(
    State(state): State<Arc<Inner>>,
    Json(req): Json<PasswordResetRequest>,
) -> Result<Response, ApiError> {
    let tenant = parse_tenant(&req.tenant_id)?;
    state
        .cfg
        .auth
        .request_password_reset(&tenant, &req.email)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

#[axum::debug_handler]
pub(crate) async fn reset_password(
    State(state): State<Arc<Inner>>,
    Json(req): Json<PasswordResetConfirm>,
) -> Result<Response, ApiError> {
    let tenant = parse_tenant(&req.tenant_id)?;
    state
        .cfg
        .auth
        .reset_password(&tenant, &req.token, &req.new_password)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}

/// Legacy per-tenant magic-link request. The HTTP routes now point at the
/// tenant-agnostic email-link login; this handler stays for in-process
/// callers (and to keep the AuthService surface easy to reach from Rust
/// embeds). A future wedge customer that needs per-tenant magic links can
/// re-route it.
#[axum::debug_handler]
#[allow(dead_code)]
pub(crate) async fn request_magic_link(
    State(state): State<Arc<Inner>>,
    Json(req): Json<MagicLinkRequest>,
) -> Result<Response, ApiError> {
    let tenant = parse_tenant(&req.tenant_id)?;
    state
        .cfg
        .auth
        .request_magic_link(&tenant, &req.email)
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
    let tenant = parse_tenant(&req.tenant_id)?;
    let toks = state
        .cfg
        .auth
        .signin_with_magic_link(&tenant, &req.token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(token_body(&toks)).into_response())
}

/// Tenant-agnostic email-link login (request). Body: `{ "email": "..." }`.
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
        .issue_api_key(claims.user_id, &claims.tenant_id, &req.name)
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
        .list_api_keys(claims.user_id, &claims.tenant_id)
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
        .revoke_api_key(id, &claims.tenant_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": true })).into_response())
}
