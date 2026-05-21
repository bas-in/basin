//! `/admin/v1/*` — operator-grade endpoints for per-project pgwire credentials
//! and BYO-bucket registration (T-049 engine-side).
//!
//! Auth: every route requires a JWT with `is_admin: true`. The check is
//! deliberately blunt for v0.1 — the wedge customer's control-plane mints
//! one such token at deploy time and uses it to provision new projects.
//! Everything else (rotate, list, BYO-bucket) takes the same gate.
//!
//! Wire shape mirrors the marketing copy: `POST /admin/v1/projects` returns a
//! drop-in `connection_url` the customer pastes into their Postgres driver.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::{S3Config, ProjectMetadata};
use basin_common::ProjectId;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ProvisionRequest {
    /// Optional override; defaults to `"basin"` server-side.
    pub dbname: Option<String>,
    /// Optional preset project id. When absent, a fresh one is allocated.
    /// Useful for the "I already have a project id from elsewhere" case.
    pub project_id: Option<String>,
}

fn require_admin(claims: &basin_auth::Claims) -> Result<(), ApiError> {
    if !claims.is_admin {
        return Err(ApiError::unauthenticated(
            "admin endpoint requires `is_admin: true` claim",
        ));
    }
    Ok(())
}

fn connection_info_to_json(info: &basin_auth::ConnectionInfo) -> Value {
    json!({
        "project_id": info.project_id.to_string(),
        "pgwire_user": info.pgwire_user,
        "dbname": info.dbname,
        "password": info.password_secret,
        "connection_url": info.connection_url,
    })
}

/// `POST /admin/v1/projects` — issue a fresh project id (or use the supplied
/// one) and return the per-project pgwire URL. The plaintext `password` field
/// in the response is the only place the secret ever leaves the server.
#[axum::debug_handler]
pub(crate) async fn provision_project(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(req): Json<ProvisionRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project = match req.project_id.as_deref() {
        Some(s) => s
            .parse::<ProjectId>()
            .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?,
        None => ProjectId::new(),
    };

    let info = state
        .cfg
        .auth
        .provision_project_db(&project, req.dbname.as_deref())
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::CREATED, Json(connection_info_to_json(&info))).into_response())
}

/// `POST /admin/v1/projects/{id}/rotate` — rotate a credential's password.
/// `id` is the `pgwire_user` (`project_<8 hex>`), not the project ULID. The
/// older password stops validating immediately; the response carries the new
/// `connection_url`.
#[axum::debug_handler]
pub(crate) async fn rotate_project(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(pgwire_user): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let info = state
        .cfg
        .auth
        .rotate_pgwire_password(&pgwire_user)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(connection_info_to_json(&info)).into_response())
}

/// `GET /admin/v1/projects/{id}/credentials` — list credential descriptors
/// for a project. `id` is the project ULID. Never returns the plaintext or
/// the hash.
#[axum::debug_handler]
pub(crate) async fn list_project_credentials(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;

    let descriptors = state
        .cfg
        .auth
        .list_project_credentials(&project)
        .await
        .map_err(ApiError::from)?;

    let body: Vec<Value> = descriptors
        .into_iter()
        .map(|d| {
            json!({
                "id": d.id,
                "project_id": d.project_id.to_string(),
                "pgwire_user": d.pgwire_user,
                "dbname": d.dbname,
                "created_at": d.created_at.to_rfc3339(),
                "rotated_at": d.rotated_at.map(|t| t.to_rfc3339()),
            })
        })
        .collect();

    Ok(Json(body).into_response())
}

// ---------------------------------------------------------------------------
// T-049: BYO bucket registration (cloud-gated)
// ---------------------------------------------------------------------------

/// Body for `POST /admin/v1/projects/:project_id/byo-bucket`.
///
/// Mirrors the [`basin_catalog::S3Config`] shape with two cosmetic renames:
/// `endpoint_url` instead of `endpoint`, and the secret arrives as the
/// plaintext field `secret_access_key`. The OSS engine persists whatever
/// bytes it receives into `S3Config::secret_access_key_enc` verbatim — the
/// cloud control-plane is responsible for envelope-encrypting the value
/// before posting if it wants the catalog row to hold ciphertext.
///
/// `prefix` and `session_token` are accepted for forward-compat with the
/// cloud client's wire shape and currently ignored — the storage layer
/// applies its own `projects/{project_id}/` prefix on top of the bucket,
/// and STS session tokens aren't plumbed through `AmazonS3Builder` yet.
#[derive(Debug, Deserialize)]
pub(crate) struct ByoBucketRequest {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub prefix: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub session_token: Option<String>,
    /// When `true`, force path-style S3 URLs (MinIO, some custom providers).
    /// Defaults to `false` (virtual-hosted, the AWS default).
    #[serde(default)]
    pub force_path_style: bool,
}

/// `POST /admin/v1/projects/:project_id/byo-bucket` — register a customer-
/// owned S3-compatible bucket for `project_id`. Idempotent: re-posting the
/// same body replaces the prior registration in both the catalog and the
/// in-process per-project store map. Returns 204 on success.
#[axum::debug_handler]
pub(crate) async fn register_byo_bucket(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(req): Json<ByoBucketRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;

    if req.bucket.trim().is_empty() {
        return Err(ApiError::invalid("bucket must be non-empty"));
    }
    if req.access_key_id.trim().is_empty() {
        return Err(ApiError::invalid("access_key_id must be non-empty"));
    }
    if req.secret_access_key.is_empty() {
        return Err(ApiError::invalid("secret_access_key must be non-empty"));
    }
    let endpoint = req
        .endpoint_url
        .clone()
        .unwrap_or_else(|| "https://s3.amazonaws.com".to_string());

    // Catalog row: persist the secret bytes verbatim. The cloud control-plane
    // is the one that envelope-encrypts; the OSS engine is opaque per the
    // S3Config doc comment.
    let cfg = S3Config {
        endpoint,
        bucket: req.bucket.clone(),
        region: req.region.clone(),
        access_key_id: req.access_key_id.clone(),
        secret_access_key_enc: req.secret_access_key.as_bytes().to_vec(),
        force_path_style: req.force_path_style,
    };

    let engine_cfg = state.cfg.engine.config();
    engine_cfg
        .catalog
        .set_project_metadata(
            &project,
            ProjectMetadata {
                byo_bucket: Some(cfg.clone()),
            },
        )
        .await
        .map_err(ApiError::from)?;

    // Wire the per-project object_store. The plaintext arrives in the body for
    // the OSS flow; in cloud deployments the control-plane decrypts before
    // calling here.
    engine_cfg
        .storage
        .register_byo_object_store_from_config_with_secret(
            project,
            &cfg,
            &req.secret_access_key,
        )
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT.into_response())
}
