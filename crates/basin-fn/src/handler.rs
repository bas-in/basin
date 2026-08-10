//! [`HandlerHarness`] — pre-compiled Wasm component implementing the
//! `basin-functions-handler` world (Phase 5.11.W2).
//!
//! ## What this is
//!
//! W1 shipped `ComponentHarness` for components that export `run()` — the
//! cron / one-shot shape. W2 adds the HTTP-handler shape: a component that
//! exports `basin:functions/handler#handle(request) -> response`. The host's
//! `/fn/v1/:name` mount instantiates one of these per registered function and
//! dispatches every matching request through `HandlerHarness::handle` (no
//! caps) or `HandlerHarness::handle_with` (W5 caps, Phase 6.P0.C).
//!
//! ## Mirroring W1
//!
//! The pattern is intentionally identical to [`crate::harness::ComponentHarness`]:
//!
//! * One process-wide `wasmtime::Engine` (shared with `ComponentHarness` via
//!   the same `component_engine` constructor in `harness.rs` — re-used here
//!   through `component_engine_arc`).
//! * `wasmtime::component::bindgen!` generates host-trait + linker glue for
//!   the new `basin-functions-handler` world. The four host imports
//!   (`query`/`http`/`log`/`secret`) reuse the same `FunctionHost` trait
//!   impls from `crate::host` — wasmtime's component model lets us point a
//!   second world's bindings at the same `Host` types.
//! * One fresh `Store<FunctionHost>` per invocation, with the same
//!   epoch-deadline cap (5 epochs) as W1.

use std::sync::Arc;

use wasmtime::component::Component;
use wasmtime::{Engine, Store};

use basin_common::ids::ProjectId;

use crate::engine::InvocationContext;
use crate::governance::{FunctionGovernance, MemoryLimiter};
use crate::host::{FunctionCallContext, FunctionHost};

// ---------------------------------------------------------------------------
// bindgen! — generates BasinFunctionsHandler + basin::functions::handler
// ---------------------------------------------------------------------------
//
// Re-uses the same `basin:functions/{query,http,log,secret}` host imports as
// the W1 world; the `with` clause maps them onto the types already generated
// in `crate::harness::basin::functions::{…}` so the linker accepts the same
// `FunctionHost` instance without a second set of trait impls.

wasmtime::component::bindgen!({
    path: "wit/basin-fn.wit",
    world: "basin-functions-handler",
    with: {
        "basin:functions/query":  crate::harness::basin::functions::query,
        "basin:functions/http":   crate::harness::basin::functions::http,
        "basin:functions/log":    crate::harness::basin::functions::log,
        "basin:functions/secret": crate::harness::basin::functions::secret,
    },
});

// The world-level `handle` export uses the `request` record from the
// `basin:functions/handler` interface (pulled in via `use handler.{…}`).
// bindgen surfaces it under the generated `basin::functions::handler`
// module. We alias the request type to build the call; the response is read
// field-by-field below.
use basin::functions::handler::Request as WitRequest;

// ---------------------------------------------------------------------------
// Host-facing request / response types
// ---------------------------------------------------------------------------

/// HTTP request handed to a function. Owned, plain-data — cheap to construct
/// from an axum `Request` in the REST layer without pulling axum into
/// `basin-fn`.
#[derive(Clone, Debug)]
pub struct HandlerRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP response returned by a function.
#[derive(Clone, Debug)]
pub struct HandlerResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HandlerResponse {
    /// Convenience constructor for tests / stub paths.
    pub fn plain(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
            body: body.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerHarness
// ---------------------------------------------------------------------------

/// Pre-compiled component that implements the `basin-functions-handler` world.
///
/// Build once per registered function (per uploaded `.wasm`); call
/// [`HandlerHarness::handle`] (no governance) or
/// [`HandlerHarness::handle_with`] (governance-wrapped, Phase 6.P0.C) for
/// every incoming `/fn/v1/:name` request. Thread-safe.
pub struct HandlerHarness {
    engine: Arc<Engine>,
    component: Component,
    linker: wasmtime::component::Linker<FunctionHost>,
    governance: Option<Arc<FunctionGovernance>>,
}

impl HandlerHarness {
    /// Compile `wasm_bytes` into a component and pre-link the four host
    /// imports, against the process-wide engine and **without governance**.
    ///
    /// `wasm_bytes` must be a valid Wasm **component** (component model format,
    /// not a core module) that exports the `basin:functions/handler`
    /// interface.
    pub fn new(wasm_bytes: &[u8]) -> anyhow::Result<Self> {
        let engine = crate::harness::component_engine_pub();
        Self::build(engine, wasm_bytes, None)
    }

    /// Compile `wasm_bytes` against `governance`'s engine and attach the
    /// governance so [`Self::handle_with`] enforces every cap.
    pub fn with_governance(
        wasm_bytes: &[u8],
        governance: Arc<FunctionGovernance>,
    ) -> anyhow::Result<Self> {
        let engine = governance.engine().clone();
        Self::build(engine, wasm_bytes, Some(governance))
    }

    fn build(
        engine: Arc<Engine>,
        wasm_bytes: &[u8],
        governance: Option<Arc<FunctionGovernance>>,
    ) -> anyhow::Result<Self> {
        let component = Component::new(&engine, wasm_bytes)?;
        let mut linker: wasmtime::component::Linker<FunctionHost> =
            wasmtime::component::Linker::new(&engine);
        // Re-use the W1 linker wiring for query / http / log / secret —
        // the trait impls live in `crate::host` and are world-agnostic.
        crate::host::add_host_to_linker(&mut linker)?;

        Ok(Self {
            engine,
            component,
            linker,
            governance,
        })
    }

    /// Whether the harness was built with [`Self::with_governance`].
    pub fn has_governance(&self) -> bool {
        self.governance.is_some()
    }

    /// Instantiate the component with the provided `ctx` and call
    /// `handler.handle(req)`.
    ///
    /// Returns the guest's response, or maps a guest-side `Err(String)` to a
    /// 500 with the error message in the body. Trap / linkage failures
    /// propagate via `anyhow::Result`.
    ///
    /// **Synchronous, no governance.** This entrypoint is kept for the test
    /// fixtures and any caller that has its own governance wrapping; the
    /// production W6 path goes through [`Self::handle_with`].
    pub fn handle(
        &self,
        ctx: InvocationContext,
        req: HandlerRequest,
    ) -> anyhow::Result<HandlerResponse> {
        let call_ctx = FunctionCallContext::new(ctx);
        let host = FunctionHost::new(call_ctx);
        let mut store = Store::new(&self.engine, host);

        // Match the W1 per-invocation epoch cap.
        store.set_epoch_deadline(5);

        let wit_req = WitRequest {
            method: req.method,
            path: req.path,
            headers: req.headers,
            body: req.body,
        };

        let bindings =
            BasinFunctionsHandler::instantiate(&mut store, &self.component, &self.linker)?;
        let res = bindings.call_handle(&mut store, &wit_req)?;

        Ok(Self::lift_response(res))
    }

    /// Governance-wrapped invocation (the W2 / W6 production path). Enforces
    /// CPU (epoch trap), memory (per-Store [`MemoryLimiter`]), wall-clock
    /// timeout, per-project semaphore, and runs the synchronous wasm work
    /// on the dedicated wasm tokio runtime.
    ///
    /// Panics if the harness was built via [`Self::new`] (without
    /// governance). Use [`Self::has_governance`] to check.
    pub async fn handle_with(
        self: &Arc<Self>,
        project: ProjectId,
        ctx: InvocationContext,
        req: HandlerRequest,
    ) -> anyhow::Result<HandlerResponse> {
        let gov = self.governance.clone().expect(
            "HandlerHarness::handle_with requires a governance-attached harness; \
             use HandlerHarness::with_governance(..) at construction",
        );
        let me = self.clone();
        let caps = gov.caps().clone();
        // Clone `gov` for the closure body; the outer `gov` is borrowed by
        // `invoke_with_caps`. Both Arcs alias the same FunctionGovernance.
        let gov_inner = gov.clone();
        gov.invoke_with_caps(project, move || {
            let call_ctx = FunctionCallContext::new(ctx);
            let host =
                FunctionHost::with_limiter(call_ctx, MemoryLimiter::new(caps.memory_max_bytes));
            let mut store = Store::new(&me.engine, host);
            gov_inner
                .prepare_store_with_limiter(&mut store, |h: &mut FunctionHost| h.limiter_mut());

            let wit_req = WitRequest {
                method: req.method,
                path: req.path,
                headers: req.headers,
                body: req.body,
            };
            let bindings =
                BasinFunctionsHandler::instantiate(&mut store, &me.component, &me.linker)?;
            let res = bindings.call_handle(&mut store, &wit_req)?;
            Ok(Self::lift_response(res))
        })
        .await
    }

    /// Shared response lift used by both [`Self::handle`] and
    /// [`Self::handle_with`].
    fn lift_response(res: Result<basin::functions::handler::Response, String>) -> HandlerResponse {
        match res {
            Ok(r) => HandlerResponse {
                status: r.status,
                headers: r.headers,
                body: r.body,
            },
            Err(msg) => HandlerResponse {
                status: 500,
                headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
                body: format!("function returned error: {msg}").into_bytes(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// FunctionStore — minimal lookup trait (extended in W6)
// ---------------------------------------------------------------------------

/// Per-project lookup hook: maps a function name to its pre-compiled
/// [`HandlerHarness`] plus the [`InvocationContext`] that should be threaded
/// into every call.
///
/// W2 ships this as a trait so `basin-rest` can route `/fn/v1/:name`
/// independently of the full function catalog (which W6 lands). Tests
/// implement it directly with an in-memory `HashMap`; W6 will provide a
/// catalog-backed impl.
pub trait FunctionStore: Send + Sync {
    /// Resolve `(project, name)` → handler + invocation context.
    ///
    /// Returns `None` if no function with that name exists for the project.
    fn lookup(
        &self,
        project: basin_common::ProjectId,
        name: &str,
    ) -> Option<(Arc<HandlerHarness>, InvocationContext)>;
}
