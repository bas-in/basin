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

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::TenantId;
use serde::Deserialize;
use serde_json::json;

use crate::errors::ApiError;
use crate::server::Inner;

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
    let toks = state
        .cfg
        .auth
        .refresh(&req.refresh_token)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(token_body(&toks)).into_response())
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

#[axum::debug_handler]
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

fn token_body(t: &basin_auth::Tokens) -> serde_json::Value {
    json!({
        "access_token": t.access_token,
        "refresh_token": t.refresh_token,
        "access_expires_at": t.access_expires_at.to_rfc3339(),
        "refresh_expires_at": t.refresh_expires_at.to_rfc3339(),
    })
}
