//! Feature 1 (5.11.W6): catalog-backed [`FunctionInvoker`] for
//! `LANGUAGE javascript` (and `LANGUAGE wasm` component-model) functions.
//!
//! Wires `basin_fn::FunctionRuntime` to the live catalog + REST invoker slot
//! so that `/fn/v1/:name` actually executes stored functions instead of
//! returning 404.
//!
//! ## Architecture
//!
//! ```text
//! REST: POST /fn/v1/:name
//!     → CatalogInvoker::invoke
//!         → FunctionRuntime::invoke
//!             → CatalogFunctionStore::lookup   ← catalog round-trip
//!             → HandlerHarness::handle          ← wasmtime component call
//! ```
//!
//! [`CatalogFunctionStore`] implements `basin_fn::runtime::FunctionStore` so
//! the runtime can look up function bytes + version by `(project, name)`.  A
//! cache miss hits `catalog.list_sql_functions(project)` once and returns the
//! matching record; the [`basin_fn::FunctionRuntime`] then caches the compiled
//! harness keyed by `(project, name, version)` so a hot path is O(1).
//!
//! [`CatalogInvoker`] implements `basin_rest::FunctionInvoker` and is
//! installed via [`basin_rest::set_global_invoker`] during server startup.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use basin_catalog::{Catalog, SqlFunctionLanguage};
use basin_common::ProjectId;
use basin_fn::runtime::{FunctionRecord, FunctionRuntime, FunctionStore, LookupFuture};
use basin_rest::{FunctionInvoker, InvokeRequest, InvokeResponse};

// ---------------------------------------------------------------------------
// CatalogFunctionStore
// ---------------------------------------------------------------------------

/// [`FunctionStore`] backed by `Arc<dyn Catalog>`.
///
/// Looks up `LANGUAGE javascript` and `LANGUAGE wasm` component-model
/// functions by scanning the catalog's SQL-function list. Returns `None` for
/// `LANGUAGE sql` functions (those are DataFusion scalar UDFs, not handler
/// components) and for functions with non-base64 bodies.
pub struct CatalogFunctionStore {
    catalog: Arc<dyn Catalog>,
}

impl CatalogFunctionStore {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

impl FunctionStore for CatalogFunctionStore {
    fn lookup<'a>(&'a self, project: &'a str, name: &'a str) -> LookupFuture<'a> {
        Box::pin(async move {
            // Parse the project id.
            let project_id: ProjectId = match project.parse() {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(project, error = %e, "fn-runtime: invalid project id in lookup");
                    return None;
                }
            };

            let fns = self.catalog.list_sql_functions(&project_id).await;
            let def = fns.into_iter().find(|f| {
                f.name == name
                    && matches!(
                        f.language,
                        SqlFunctionLanguage::Javascript | SqlFunctionLanguage::Wasm
                    )
            })?;

            // Decode the base64 body to raw Wasm component bytes.
            let body = match B64.decode(def.body.trim().as_bytes()) {
                Ok(b) if !b.is_empty() => b as Vec<u8>,
                Ok(_) => {
                    tracing::warn!(name, "fn-runtime: decoded empty Wasm body for function");
                    return None;
                }
                Err(e) => {
                    tracing::warn!(name, error = %e, "fn-runtime: base64 decode failed for function body");
                    return None;
                }
            };

            Some(FunctionRecord {
                project: project.to_string(),
                name: name.to_string(),
                body,
                version: def.version,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// CatalogInvoker — implements basin_rest::FunctionInvoker
// ---------------------------------------------------------------------------

/// [`FunctionInvoker`] that routes every `/fn/v1/:name` request through the
/// `basin_fn::FunctionRuntime` backed by the live catalog.
pub struct CatalogInvoker {
    runtime: Arc<FunctionRuntime>,
}

impl CatalogInvoker {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        let store: Arc<dyn FunctionStore> = Arc::new(CatalogFunctionStore::new(catalog));
        Self {
            runtime: Arc::new(FunctionRuntime::new(store)),
        }
    }
}

#[async_trait::async_trait]
impl FunctionInvoker for CatalogInvoker {
    async fn invoke(
        &self,
        project: ProjectId,
        name: &str,
        req: InvokeRequest,
    ) -> Result<Option<InvokeResponse>, String> {
        use basin_fn::engine::InvocationContext;
        use basin_fn::handler::HandlerRequest;
        use basin_fn::runtime::InvokeResult;

        let hr = HandlerRequest {
            method: req.method,
            path: req.path,
            headers: req.headers,
            body: req.body,
        };

        let ctx = InvocationContext::mock();

        match self
            .runtime
            .invoke(&project.to_string(), name, ctx, hr)
            .await
        {
            InvokeResult::Ok(resp) => Ok(Some(InvokeResponse {
                status: resp.status,
                headers: resp.headers,
                body: resp.body,
            })),
            InvokeResult::NotFound => Ok(None),
            InvokeResult::Rejected(reason) => Err(format!("governance rejected: {reason}")),
            InvokeResult::RuntimeError(msg) => Err(msg),
        }
    }
}

// ---------------------------------------------------------------------------
// install_catalog_invoker — called from main.rs startup
// ---------------------------------------------------------------------------

/// Build a [`CatalogInvoker`] from the live catalog and install it as the
/// process-wide function invoker.
///
/// After this call, every `/fn/v1/:name` request is routed through
/// `basin_fn::FunctionRuntime` → the compiled wasmtime component.
pub fn install_catalog_invoker(catalog: Arc<dyn Catalog>) {
    let invoker = Arc::new(CatalogInvoker::new(catalog));
    basin_rest::set_global_invoker(invoker);
    tracing::info!(
        "fn-runtime: catalog-backed FunctionInvoker installed (LANGUAGE javascript/wasm)"
    );
}
