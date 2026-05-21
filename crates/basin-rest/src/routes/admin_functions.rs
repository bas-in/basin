//! `/admin/v1/functions/*` — control-plane API for the basin-fn function
//! catalog (T-078 / T-079 / T-080 / T-081).
//!
//! ## Surface
//!
//! | Verb + Path                                  | Action                  | Status |
//! |---|---|---|
//! | `POST /admin/v1/functions/deploy`            | upload Wasm bytes       | 201 |
//! | `GET /admin/v1/functions`                    | list deployed functions | 200 |
//! | `DELETE /admin/v1/functions/:name`           | delete by name          | 204 |
//! | `GET /admin/v1/functions/:name/logs`         | recent log lines        | 200 (empty) |
//! | `GET /admin/v1/functions/:name/cpu-ms`       | per-fn CPU-ms counter   | 200 (zero) |
//!
//! ## Auth
//!
//! Same admin gate as the rest of `/admin/v1/*` — every route requires a JWT
//! with `is_admin: true`. The function is scoped to the *project* carried in
//! the admin JWT's `project_id` claim — there is no cross-project list.
//!
//! ## basin-fn isolation
//!
//! basin-rest deliberately does **not** depend on basin-fn (see the comment
//! in `routes/fn_handler.rs`: keeping wasmtime out of the REST crate's
//! dependency footprint). To preserve that invariant, this module treats the
//! Wasm component bytes as opaque blobs — it does not parse, compile, or
//! execute them. A downstream wiring (W6's catalog-backed
//! [`crate::FunctionInvoker`] impl) is the layer that hands the bytes to
//! `basin_fn::HandlerHarness::new`.
//!
//! ## TODOs for follow-on basin-fn instrumentation
//!
//! Per the task soft-cap, `logs` and `cpu-ms` ship as **stubs** in this PR:
//! - `logs` returns an empty list. basin-fn does not currently capture per-
//!   function log lines — the `basin:fn/log` host import forwards to
//!   `tracing` without buffering. A future basin-fn change should add a
//!   ring-buffered per-`(project, name)` capture that this endpoint reads.
//! - `cpu-ms` returns 0. basin-fn's `FunctionGovernance` enforces a CPU cap
//!   (`cpu_ticks` epoch counter) but does not expose a wall-CPU counter per
//!   function. A future change should add a per-`(project, name)` atomic
//!   counter incremented inside `invoke_with_caps` and read here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;

use crate::errors::ApiError;
use crate::parser::validate_ident;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// In-process function registry
// ---------------------------------------------------------------------------

/// One deployed function, keyed by `(project, name)`. `bytes` is the opaque
/// Wasm component body — basin-rest never parses it. `version` is bumped on
/// every redeploy of the same `(project, name)` pair so the downstream
/// invoker's cache can invalidate.
#[derive(Clone)]
pub(crate) struct FunctionEntry {
    pub version: i64,
    pub deployed_at: SystemTime,
    pub bytes: Vec<u8>,
}

/// In-process function catalog. Each project has its own namespace; cross-
/// project lookups never cross. Cheap to clone (`Arc` inside).
///
/// Per-project cost is O(deployed functions × bytes/function) — there is no
/// global structure that scales with the number of distinct projects ever
/// seen (the outer map only carries projects that have deployed at least one
/// function). Matches the multi-project isolation rule.
#[derive(Default, Clone)]
pub(crate) struct FunctionRegistry {
    inner: Arc<RwLock<HashMap<ProjectId, HashMap<String, FunctionEntry>>>>,
}

impl FunctionRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Insert (or replace) a function. Returns the post-write version so the
    /// caller can echo it in the response.
    async fn put(&self, project: ProjectId, name: String, bytes: Vec<u8>) -> i64 {
        let mut map = self.inner.write().await;
        let project_map = map.entry(project).or_default();
        let version = project_map
            .get(&name)
            .map(|e| e.version + 1)
            .unwrap_or(1);
        project_map.insert(
            name,
            FunctionEntry {
                version,
                deployed_at: SystemTime::now(),
                bytes,
            },
        );
        version
    }

    async fn list(&self, project: &ProjectId) -> Vec<(String, FunctionEntry)> {
        let map = self.inner.read().await;
        map.get(project)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Remove a function. Returns true if it existed.
    async fn delete(&self, project: &ProjectId, name: &str) -> bool {
        let mut map = self.inner.write().await;
        match map.get_mut(project) {
            Some(m) => m.remove(name).is_some(),
            None => false,
        }
    }

    async fn exists(&self, project: &ProjectId, name: &str) -> bool {
        let map = self.inner.read().await;
        map.get(project)
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Admin gate (shared with admin.rs)
// ---------------------------------------------------------------------------

fn require_admin(claims: &basin_auth::Claims) -> Result<(), ApiError> {
    if !claims.is_admin {
        return Err(ApiError::unauthenticated(
            "admin endpoint requires `is_admin: true` claim",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// POST /admin/v1/functions/deploy
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct DeployRequest {
    pub name: String,
    /// Base64-encoded Wasm component bytes.
    pub wasm_b64: String,
}

#[derive(Debug, Serialize)]
struct DeployResponse {
    function_id: String,
    project_id: String,
    name: String,
    version: i64,
}

#[axum::debug_handler]
pub(crate) async fn deploy_function(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(req): Json<DeployRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let name = validate_ident(&req.name)?;
    let bytes = B64
        .decode(req.wasm_b64.as_bytes())
        .map_err(|e| ApiError::invalid(format!("wasm_b64 is not valid base64: {e}")))?;
    if bytes.is_empty() {
        return Err(ApiError::invalid("wasm bytes are empty"));
    }

    let version = state
        .function_registry
        .put(claims.project_id, name.clone(), bytes)
        .await;

    let function_id = format!("{}:{}", claims.project_id, name);
    Ok((
        StatusCode::CREATED,
        Json(DeployResponse {
            function_id,
            project_id: claims.project_id.to_string(),
            name,
            version,
        }),
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// GET /admin/v1/functions
// ---------------------------------------------------------------------------

#[axum::debug_handler]
pub(crate) async fn list_functions(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let entries = state.function_registry.list(&claims.project_id).await;
    let body: Vec<_> = entries
        .into_iter()
        .map(|(name, e)| {
            json!({
                "project_id": claims.project_id.to_string(),
                "name": name,
                "version": e.version,
                "deployed_at": deploy_rfc3339(e.deployed_at),
                "size_bytes": e.bytes.len(),
            })
        })
        .collect();
    Ok(Json(body).into_response())
}

fn deploy_rfc3339(t: SystemTime) -> String {
    let dur = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    chrono::DateTime::<chrono::Utc>::from_timestamp(
        dur.as_secs() as i64,
        dur.subsec_nanos(),
    )
    .map(|dt| dt.to_rfc3339())
    .unwrap_or_else(|| "1970-01-01T00:00:00Z".into())
}

// ---------------------------------------------------------------------------
// DELETE /admin/v1/functions/:name
// ---------------------------------------------------------------------------

#[axum::debug_handler]
pub(crate) async fn delete_function(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;
    let name = validate_ident(&name)?;

    let removed = state
        .function_registry
        .delete(&claims.project_id, &name)
        .await;
    if !removed {
        return Err(ApiError::not_found(format!(
            "function {name:?} not found for project"
        )));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------------------
// GET /admin/v1/functions/:name/logs
// ---------------------------------------------------------------------------
//
// Stub: returns an empty `lines` array regardless of how many invocations the
// function has served. basin-fn does not currently capture per-function log
// output (the `basin:fn/log` host import forwards directly to `tracing`),
// so there is nothing to read back. A follow-up basin-fn change should add a
// ring-buffered capture this endpoint reads from.

#[axum::debug_handler]
pub(crate) async fn function_logs(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;
    let name = validate_ident(&name)?;

    if !state
        .function_registry
        .exists(&claims.project_id, &name)
        .await
    {
        return Err(ApiError::not_found(format!(
            "function {name:?} not found for project"
        )));
    }
    Ok(Json(json!({
        "project_id": claims.project_id.to_string(),
        "name": name,
        "lines": Vec::<String>::new(),
        // TODO basin-fn: surface per-function log lines via a ring buffer
        //      keyed by (project, name); this stub returns empty until then.
    }))
    .into_response())
}

// ---------------------------------------------------------------------------
// GET /admin/v1/functions/:name/cpu-ms
// ---------------------------------------------------------------------------
//
// Stub: returns `cpu_ms = 0`. basin-fn's `FunctionGovernance` enforces a CPU
// cap (epoch-based) but does not export a wall-CPU counter per function.
// A follow-up basin-fn change should add an atomic counter incremented inside
// `FunctionGovernance::invoke_with_caps`; this endpoint will then read it.

#[axum::debug_handler]
pub(crate) async fn function_cpu_ms(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;
    let name = validate_ident(&name)?;

    if !state
        .function_registry
        .exists(&claims.project_id, &name)
        .await
    {
        return Err(ApiError::not_found(format!(
            "function {name:?} not found for project"
        )));
    }
    Ok(Json(json!({
        "project_id": claims.project_id.to_string(),
        "name": name,
        "cpu_ms": 0u64,
        // TODO basin-fn: surface per-function cumulative wall-CPU; this stub
        //      returns 0 until basin-fn exposes a metering counter.
    }))
    .into_response())
}
