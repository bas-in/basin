//! `/v1/cdc/:project/webhooks` — CDC webhook subscription CRUD (ADR 0028
//! Phase 2).
//!
//! ## Surface
//!
//! | Verb + Path                              | Action                       | Status |
//! |------------------------------------------|------------------------------|--------|
//! | `POST   /v1/cdc/:project/webhooks`       | register a webhook           | 201    |
//! | `GET    /v1/cdc/:project/webhooks`       | list webhooks + delivery state | 200  |
//! | `DELETE /v1/cdc/:project/webhooks/:id`   | drop a webhook               | 204    |
//!
//! ## Auth
//!
//! Project-scoped, not admin-only — same JWT path as the rest of `/rest/v1/*`
//! and the CDC SSE stream (`GET /v1/cdc/:project/stream`). The bearer's
//! `project_id` claim must equal the `:project` path segment (cross-project
//! isolation invariant, mirroring the SSE handler). API keys work too — they
//! resolve to a `project_id` via [`crate::server::authorize`].
//!
//! ## Wire shape
//!
//! `POST` body:
//! ```json
//! { "url": "https://example.com/cdc",
//!   "tables": ["orders"],          // optional; omit/null ⇒ all tables
//!   "ops": ["insert","update"] }   // optional; omit/null ⇒ all ops
//! ```
//! → `201 Created` with the minted id **and the signing secret** (returned
//! exactly once — it is never echoed by the GET route):
//! ```json
//! { "id": "01J…", "secret": "<64 hex>" }
//! ```
//!
//! `GET` response — observability for each endpoint (ADR Phase 2 §3). The
//! `secret` is deliberately omitted; the delivery cursor + last status are
//! exposed:
//! ```json
//! { "webhooks": [
//!   { "id":"01J…","url":"https://…","tables":["orders"],"ops":null,
//!     "active":true,"last_seq":42,"last_status":"200","retry_count":0,
//!     "disabled_at":null }
//! ] }
//! ```

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::{validate_cdc_webhook, Catalog as _, CdcWebhookDef, CdcWebhookError};
use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

/// `POST /v1/cdc/:project/webhooks` body.
#[derive(Debug, Deserialize)]
pub(crate) struct RegisterWebhookRequest {
    pub url: String,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    #[serde(default)]
    pub ops: Option<Vec<String>>,
}

/// One row in the `GET` response — the subscription minus the secret, plus
/// delivery state.
#[derive(Debug, Serialize)]
struct WebhookView {
    id: String,
    url: String,
    tables: Option<Vec<String>>,
    ops: Option<Vec<String>>,
    active: bool,
    last_seq: u64,
    last_status: Option<String>,
    retry_count: u32,
    disabled_at: Option<String>,
}

/// Enforce that the authenticated project matches the URL path project — the
/// same cross-project isolation the CDC SSE handler applies.
fn assert_project_match(
    claims: &basin_auth::Claims,
    path_project: &ProjectId,
) -> Result<(), ApiError> {
    if claims.project_id != *path_project {
        return Err(ApiError::forbidden(format!(
            "token scoped to project {} cannot manage webhooks for project {}",
            claims.project_id, path_project,
        )));
    }
    Ok(())
}

fn parse_project(s: &str) -> Result<ProjectId, ApiError> {
    s.parse()
        .map_err(|e| ApiError::invalid(format!("invalid project id in path: {e}")))
}

/// `POST /v1/cdc/:project/webhooks`
#[axum::debug_handler]
pub(crate) async fn register_webhook(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_str): Path<String>,
    Json(req): Json<RegisterWebhookRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    // Validate URL + op tokens before minting anything.
    validate_cdc_webhook(&req.url, req.ops.as_deref()).map_err(cdc_err)?;

    // Normalise op tokens to lowercase so the worker's filter matching is
    // canonical regardless of how the caller cased them.
    let ops = req.ops.map(|v| {
        v.into_iter()
            .map(|o| o.trim().to_ascii_lowercase())
            .collect::<Vec<_>>()
    });
    let tables = req.tables.map(|v| {
        v.into_iter()
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });

    let id = basin_cdc::mint_webhook_id();
    let secret = basin_cdc::mint_secret_hex();

    let def = CdcWebhookDef {
        id: id.clone(),
        project,
        url: req.url.trim().to_string(),
        tables,
        ops,
        secret_hex: secret.clone(),
        active: true,
    };

    state
        .cfg
        .engine
        .config()
        .catalog
        .register_cdc_webhook(def)
        .await
        .map_err(ApiError::from)?;

    tracing::info!(%project, webhook = %id, "cdc webhook registered");

    // The secret is returned exactly once here; the GET route never echoes it.
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": id, "secret": secret })),
    )
        .into_response())
}

/// `GET /v1/cdc/:project/webhooks`
#[axum::debug_handler]
pub(crate) async fn list_webhooks(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_str): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    let rows = state
        .cfg
        .engine
        .config()
        .catalog
        .list_cdc_webhooks(&project)
        .await;

    let views: Vec<WebhookView> = rows
        .into_iter()
        .map(|r| WebhookView {
            id: r.def.id,
            url: r.def.url,
            tables: r.def.tables,
            ops: r.def.ops,
            active: r.def.active,
            last_seq: r.state.last_seq,
            last_status: r.state.last_status,
            retry_count: r.state.retry_count,
            disabled_at: r.state.disabled_at,
        })
        .collect();

    Ok(Json(json!({ "webhooks": views })).into_response())
}

/// `DELETE /v1/cdc/:project/webhooks/:id`
#[axum::debug_handler]
pub(crate) async fn delete_webhook(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((project_str, id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    state
        .cfg
        .engine
        .config()
        .catalog
        .drop_cdc_webhook(&project, &id)
        .await
        .map_err(ApiError::from)?;

    tracing::info!(%project, webhook = %id, "cdc webhook dropped");
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Map a [`CdcWebhookError`] to the REST error shape.
fn cdc_err(e: CdcWebhookError) -> ApiError {
    match e {
        CdcWebhookError::InvalidUrl(m) => ApiError::invalid(format!("invalid webhook url: {m}")),
        CdcWebhookError::InvalidOp(m) => ApiError::invalid(format!(
            "invalid op filter {m:?} (allowed: insert, update, delete)"
        )),
        CdcWebhookError::NotFound => ApiError::not_found("cdc webhook not found"),
    }
}
