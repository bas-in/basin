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
//! | `GET /admin/v1/functions/:name/logs`         | recent log lines        | 200 |
//! | `GET /admin/v1/functions/:name/cpu-ms`       | per-fn CPU-ms counter   | 200 |
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
//! ## Per-function instrumentation (closes #55 sub-items)
//!
//! `FunctionRegistry` now carries two instrumentation structures per
//! `(project, name)` pair, both written by the `FunctionInvoker`
//! implementation that runs Wasm after each invocation:
//!
//! - **`LogBuffer`** — a bounded ring buffer (default cap 100) of
//!   [`LogEntry`] values. Callers append via
//!   [`FunctionRegistry::append_log`]; the `/logs` endpoint reads a
//!   snapshot via [`FunctionRegistry::logs`].
//! - **CPU counter** — a per-`(project, name)` `AtomicU64` accumulating
//!   wall-CPU milliseconds. Callers add via
//!   [`FunctionRegistry::add_cpu_ms`]; the `/cpu-ms` endpoint reads via
//!   [`FunctionRegistry::cpu_ms`].

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
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
// Per-function log buffer
// ---------------------------------------------------------------------------

/// Maximum log entries retained per `(project, name)`. Oldest entries are
/// evicted when the cap is reached (ring-buffer semantics).
pub const LOG_BUFFER_CAP: usize = 100;

/// A single log line captured from the `basin:fn/log` host import.
#[derive(Clone, Debug, Serialize)]
pub struct LogEntry {
    /// Log level string: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`.
    pub level: String,
    /// The log message text.
    pub message: String,
    /// Wall-clock timestamp when the entry was appended (seconds since epoch).
    pub ts_secs: u64,
}

/// Bounded ring buffer of [`LogEntry`] values for one `(project, name)`.
///
/// - Appends are O(1) amortised.
/// - When the buffer is at `cap`, the oldest entry is dropped before inserting.
/// - [`LogBuffer::snapshot`] returns a `Vec` in oldest-first order.
struct LogBuffer {
    entries: VecDeque<LogEntry>,
    cap: usize,
}

impl LogBuffer {
    fn new(cap: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(cap.min(256)),
            cap,
        }
    }

    /// Append `entry`, evicting the oldest if the buffer is full.
    fn push(&mut self, entry: LogEntry) {
        if self.entries.len() >= self.cap {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Return all entries in oldest-first order.
    fn snapshot(&self) -> Vec<LogEntry> {
        self.entries.iter().cloned().collect()
    }
}

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

/// Shared instrumentation state for one `(project, name)` pair.
struct FnInstrumentation {
    logs: LogBuffer,
    cpu_ms: AtomicU64,
}

impl FnInstrumentation {
    fn new() -> Self {
        Self {
            logs: LogBuffer::new(LOG_BUFFER_CAP),
            cpu_ms: AtomicU64::new(0),
        }
    }
}

/// Per-project catalog + instrumentation. Cheap to clone (`Arc` inside).
///
/// Per-project cost is O(deployed functions × bytes/function + log entries) —
/// there is no global structure that scales with the number of distinct
/// projects ever seen. Matches the multi-project isolation rule.
#[derive(Default, Clone)]
pub struct FunctionRegistry {
    inner: Arc<RwLock<HashMap<ProjectId, HashMap<String, FunctionEntry>>>>,
    /// Instrumentation is keyed by `(project, name)` independently of the
    /// deploy map so CPU / log state survives a redeploy of the same
    /// function. An `Arc<Mutex<…>>` inside the outer `Arc<RwLock<…>>` would
    /// require holding the outer lock to update counters; instead we use a
    /// second top-level `RwLock`-guarded map so log appends and CPU
    /// increments acquire only the instr lock, not the deploy lock.
    instr: Arc<RwLock<HashMap<(ProjectId, String), FnInstrumentation>>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
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

    pub(crate) async fn exists(&self, project: &ProjectId, name: &str) -> bool {
        let map = self.inner.read().await;
        map.get(project)
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }

    // -----------------------------------------------------------------------
    // Instrumentation write path (called by FunctionInvoker implementations)
    // -----------------------------------------------------------------------

    /// Append a log line for `(project, name)`. Oldest entries are evicted at
    /// `LOG_BUFFER_CAP`. This is the write side of the `/logs` endpoint.
    ///
    /// `level` should be one of `"trace"`, `"debug"`, `"info"`, `"warn"`,
    /// `"error"`.
    pub async fn append_log(
        &self,
        project: ProjectId,
        name: impl Into<String>,
        level: impl Into<String>,
        message: impl Into<String>,
    ) {
        let name = name.into();
        let ts_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = LogEntry {
            level: level.into(),
            message: message.into(),
            ts_secs,
        };
        let mut instr = self.instr.write().await;
        instr
            .entry((project, name))
            .or_insert_with(FnInstrumentation::new)
            .logs
            .push(entry);
    }

    /// Add `delta_ms` milliseconds to the cumulative CPU counter for
    /// `(project, name)`. This is the write side of the `/cpu-ms` endpoint.
    pub async fn add_cpu_ms(&self, project: ProjectId, name: impl Into<String>, delta_ms: u64) {
        let name = name.into();
        let instr = self.instr.read().await;
        if let Some(entry) = instr.get(&(project, name.clone())) {
            entry.cpu_ms.fetch_add(delta_ms, Ordering::Relaxed);
            return;
        }
        drop(instr);
        // Slow path: create the entry under write lock.
        let mut instr = self.instr.write().await;
        let e = instr
            .entry((project, name))
            .or_insert_with(FnInstrumentation::new);
        e.cpu_ms.fetch_add(delta_ms, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Instrumentation read path (called by admin endpoint handlers)
    // -----------------------------------------------------------------------

    /// Return a snapshot of the log buffer for `(project, name)`,
    /// oldest-first. Returns an empty `Vec` if no entries have been captured
    /// yet.
    pub async fn logs(&self, project: &ProjectId, name: &str) -> Vec<LogEntry> {
        let instr = self.instr.read().await;
        instr
            .get(&(*project, name.to_string()))
            .map(|e| e.logs.snapshot())
            .unwrap_or_default()
    }

    /// Return the cumulative CPU-ms counter for `(project, name)`. Returns `0`
    /// if the function has never been invoked.
    pub async fn cpu_ms(&self, project: &ProjectId, name: &str) -> u64 {
        let instr = self.instr.read().await;
        instr
            .get(&(*project, name.to_string()))
            .map(|e| e.cpu_ms.load(Ordering::Relaxed))
            .unwrap_or(0)
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

    let lines = state
        .function_registry
        .logs(&claims.project_id, &name)
        .await;

    Ok(Json(json!({
        "project_id": claims.project_id.to_string(),
        "name": name,
        "lines": lines,
    }))
    .into_response())
}

// ---------------------------------------------------------------------------
// GET /admin/v1/functions/:name/cpu-ms
// ---------------------------------------------------------------------------

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

    let cpu_ms = state
        .function_registry
        .cpu_ms(&claims.project_id, &name)
        .await;

    Ok(Json(json!({
        "project_id": claims.project_id.to_string(),
        "name": name,
        "cpu_ms": cpu_ms,
    }))
    .into_response())
}
