//! `ANY /fn/v1/:name` — HTTP-handler-shape function invocation (Phase 5.11.W2).
//!
//! ## Contract
//!
//! - **Method-agnostic** (`axum::routing::any`): GET, POST, PUT, PATCH, DELETE
//!   and any custom verb are forwarded into the function. The Wasm guest is the
//!   one that decides which methods make sense for its handler.
//! - **Auth**: JWT or API-key bearer, mirrored exactly from
//!   [`crate::routes::rpc::post_rpc`]. There is no anonymous `/fn/v1/*` mount
//!   — every request is authenticated and scoped to a project before the
//!   function name is even resolved.
//! - **Project-scoped lookup**: the request's `claims.project_id` is the only
//!   scope used to resolve the name. There is no global function namespace.
//!
//! ## Route-reconciliation choice (recorded in `decisions.md`)
//!
//! basin-cli's `basin functions {deploy,list,logs,delete}` (5.11.W3) targets
//! `/rest/v1/functions/*` for **CRUD** on the function catalog (register a
//! component, list registered names, tail logs, delete one). This file ships
//! only the **invoke** path under `/fn/v1/:name`, matching the dedicated
//! invoke prefix declared in `TASK.md` § 5.11.W2.
//!
//! The split is: CRUD on `/rest/v1/functions/*`, invoke on `/fn/v1/:name`.
//! - CRUD reads / writes catalog rows; sits alongside every other catalog
//!   admin verb under `/rest/v1`. Standard JSON envelopes, standard verbs.
//! - Invoke runs guest code and proxies its raw response back to the caller.
//!   Different content-types, different status codes, different headers — it
//!   needs its own prefix so the function never collides with a table named
//!   "functions" or with the rest of the `/rest/v1/*` namespace.
//!
//! ## FunctionStore seam
//!
//! `basin-rest` resolves `(project, name)` through a small trait,
//! [`FunctionInvoker`]. W2 ships:
//! - The trait, the request / response types (`InvokeRequest`, `InvokeResponse`),
//!   and the route handler.
//! - A no-op default implementation ([`NoopFunctionInvoker`]) installed in
//!   `Inner` so the mount works in deployments that haven't wired a function
//!   runtime yet — every name resolves to 404.
//!
//! W6 will provide a real implementation backed by the function catalog +
//! `basin_fn::HandlerHarness`. The trait deliberately uses plain-data types so
//! `basin-rest` never has to depend on `basin-fn` directly — keeping wasmtime
//! out of the REST crate's dependency footprint.

use std::sync::{Arc, OnceLock, RwLock};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use basin_common::ProjectId;

use crate::errors::ApiError;
use crate::parser::validate_ident;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// Public trait + request/response types
// ---------------------------------------------------------------------------

/// HTTP request handed to a function. Plain data (no axum types) so this
/// trait can be implemented by crates that don't pull axum in.
#[derive(Clone, Debug)]
pub struct InvokeRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP response a function returns.
#[derive(Clone, Debug)]
pub struct InvokeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Per-deployment hook that maps `(project, function-name, request)` to a
/// response.
///
/// W2 ships this as a trait so the `/fn/v1/:name` route is **independent of
/// the W6 function catalog** — tests implement it directly; W6 plugs in a
/// catalog-backed implementation that drives `basin_fn::HandlerHarness`.
///
/// Implementations must be `Send + Sync` because the trait object is stored
/// inside `Arc<Inner>` and shared across every axum worker.
#[async_trait::async_trait]
pub trait FunctionInvoker: Send + Sync {
    /// Resolve `(project, name)` and invoke the function with `req`.
    ///
    /// Return `Ok(None)` if no function with that name exists for the project
    /// (the route layer turns that into a 404). `Ok(Some(resp))` returns the
    /// function's response verbatim. `Err(msg)` becomes a 500 with the message
    /// in the body — for the W2 mount we don't try to classify errors further
    /// because the function is opaque to us.
    async fn invoke(
        &self,
        project: ProjectId,
        name: &str,
        req: InvokeRequest,
    ) -> Result<Option<InvokeResponse>, String>;
}

/// Default invoker installed when no real one is wired. Every lookup
/// returns `Ok(None)` → 404 from the route layer.
///
/// This means the `/fn/v1/:name` mount is always live, but in a deployment
/// without a function runtime callers get a clean "no such function" rather
/// than a 500.
pub struct NoopFunctionInvoker;

#[async_trait::async_trait]
impl FunctionInvoker for NoopFunctionInvoker {
    async fn invoke(
        &self,
        _project: ProjectId,
        _name: &str,
        _req: InvokeRequest,
    ) -> Result<Option<InvokeResponse>, String> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Process-wide invoker slot (test / W6 injection seam)
// ---------------------------------------------------------------------------
//
// W2 keeps `crate::server::Inner` untouched (the W1-followup / W5 / W6
// siblings are also editing that module) by storing the invoker in a
// module-local slot instead of a new `Inner` field. The slot is set via
// [`set_global_invoker`]; if it's empty the route falls back to
// [`NoopFunctionInvoker`].
//
// This is a deliberate trade-off:
//   - Pros: no `server.rs` field churn, no `RestConfig` API break, the
//     test surface stays the same as `RestService::new(cfg)`.
//   - Cons: the invoker is process-wide rather than per-`RestService`. For
//     production deployments W6 will lift this to an `Inner` field; for
//     W2's single-binary deployments and for tests there's exactly one
//     `RestService` per process so the slot is a faithful equivalent.

static GLOBAL_INVOKER: OnceLock<RwLock<Arc<dyn FunctionInvoker>>> = OnceLock::new();

fn invoker_slot() -> &'static RwLock<Arc<dyn FunctionInvoker>> {
    GLOBAL_INVOKER.get_or_init(|| RwLock::new(Arc::new(NoopFunctionInvoker)))
}

/// Install a [`FunctionInvoker`] for the process. Subsequent
/// `/fn/v1/:name` requests resolve through it.
///
/// Tests call this before the first request to inject a stub backed by
/// `basin_fn::HandlerHarness`; W6 will call it from server bootstrap once
/// the catalog-backed implementation is wired.
pub fn set_global_invoker(invoker: Arc<dyn FunctionInvoker>) {
    let slot = invoker_slot();
    *slot.write().expect("invoker slot poisoned") = invoker;
}

fn current_invoker() -> Arc<dyn FunctionInvoker> {
    invoker_slot()
        .read()
        .expect("invoker slot poisoned")
        .clone()
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

/// `ANY /fn/v1/:name`
///
/// Authenticates the caller (mirrors [`crate::routes::rpc::post_rpc`]),
/// validates `name` as a SQL-safe identifier, looks the function up via
/// [`FunctionInvoker`], and proxies the response back.
#[axum::debug_handler]
pub(crate) async fn any_fn_handler(
    State(state): State<Arc<Inner>>,
    Path(name): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let name = validate_ident(&name)?;
    let invoker = current_invoker();

    // Forward headers as (name, value) tuples. Non-ASCII header values are
    // dropped — the same rule axum uses for `HeaderValue::to_str` failures.
    let fwd_headers: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.as_str().to_owned(), s.to_owned())))
        .collect();

    let req = InvokeRequest {
        method: method.as_str().to_owned(),
        // The mount path is fixed; pass `/fn/v1/:name` so the function can
        // identify itself in logs. We deliberately do not pass the query
        // string in W2 — handler-style functions consume the body / headers
        // they were given, and querystring routing is a W2-followup.
        path: format!("/fn/v1/{name}"),
        headers: fwd_headers,
        body: body.to_vec(),
    };

    match invoker.invoke(claims.project_id, &name, req).await {
        Ok(None) => Err(ApiError::not_found(format!(
            "function {name:?} not found for project"
        ))),
        Ok(Some(resp)) => Ok(render_invoke_response(resp)),
        Err(msg) => {
            tracing::warn!(name = %name, project = %claims.project_id, err = %msg,
                "function invocation failed");
            Err(ApiError::internal(format!("function invocation failed: {msg}")))
        }
    }
}

/// Translate a guest [`InvokeResponse`] back into an axum [`Response`].
///
/// - Unknown / invalid status codes fall back to 502 (the function returned
///   something we can't surface).
/// - Headers with invalid byte sequences are dropped silently — the function
///   sent something that doesn't fit HTTP, we don't propagate the breakage.
fn render_invoke_response(resp: InvokeResponse) -> Response {
    let status = StatusCode::from_u16(resp.status)
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = HeaderMap::with_capacity(resp.headers.len());
    for (k, v) in resp.headers {
        let Ok(name) = HeaderName::try_from(k.as_str()) else { continue };
        let Ok(value) = HeaderValue::try_from(v.as_str()) else { continue };
        headers.insert(name, value);
    }
    (status, headers, resp.body).into_response()
}
