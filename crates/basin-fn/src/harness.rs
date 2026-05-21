//! [`ComponentHarness`] — instantiates a Wasm component, wires the four host
//! imports, and calls the exported `run` entrypoint.
//!
//! ## Component model approach
//!
//! * WIT in `crates/basin-fn/wit/basin-fn.wit` defines the ABI.
//! * `wasmtime::component::bindgen!` generates host-side trait + linker glue.
//! * `FunctionHost` (in `crate::host`) implements all four generated traits.
//! * One `wasmtime::Engine` is shared across all invocations (JIT compilation
//!   is thread-safe). `Component`s and `Linker`s are cached by the harness.
//!
//! ## Governance integration (Phase 6.P0.C)
//!
//! When constructed via [`ComponentHarness::with_governance`], the harness
//! uses the governance's [`wasmtime::Engine`] (whose epoch is ticked by the
//! governance background task), installs the per-Store [`MemoryLimiter`]
//! onto the [`FunctionHost`], and routes the synchronous instantiate + call
//! pipeline through [`crate::governance::FunctionGovernance::invoke_with_caps`]
//! so the per-project semaphore, wall-clock timeout, and dedicated wasm
//! runtime are all enforced. The legacy [`ComponentHarness::new`] path stays
//! for existing benchmark / bench-harness callers that build a harness
//! without governance.

use std::sync::Arc;

use wasmtime::{Config, Engine, Store};
use wasmtime::component::Component;

use basin_common::ids::ProjectId;

use crate::engine::InvocationContext;
use crate::governance::{FunctionGovernance, MemoryLimiter};
use crate::host::{FunctionCallContext, FunctionHost, add_host_to_linker};

// ---------------------------------------------------------------------------
// bindgen! — generates BasinFunctions + basin::functions::{query,http,…}
// ---------------------------------------------------------------------------

// The generated types are used both here and in `host.rs`; we re-export the
// module so `host.rs` can import from `crate::harness`.
wasmtime::component::bindgen!({
    path: "wit/basin-fn.wit",
    world: "basin-functions",
});

// ---------------------------------------------------------------------------
// Process-wide wasmtime Engine (legacy / governance-free path)
// ---------------------------------------------------------------------------

static COMPONENT_ENGINE: std::sync::OnceLock<Arc<Engine>> = std::sync::OnceLock::new();

/// Return (or lazily initialise) the process-wide wasmtime Engine with
/// the component model enabled and epoch interruption on.
///
/// Used by [`ComponentHarness::new`] (no governance attached) and by
/// [`crate::handler::HandlerHarness::new`]. When governance IS attached
/// callers should reuse `governance.engine()` instead — that engine has the
/// per-Engine memory reservation + epoch ticker active.
fn component_engine() -> Arc<Engine> {
    COMPONENT_ENGINE
        .get_or_init(|| {
            let mut cfg = Config::new();
            cfg.wasm_component_model(true)
                .epoch_interruption(true);
            let engine = Engine::new(&cfg).expect("wasmtime Engine::new failed");
            Arc::new(engine)
        })
        .clone()
}

/// Sibling-module accessor for the shared wasmtime Engine. Used by
/// `crate::handler::HandlerHarness` so the W1 and W2 worlds share one
/// JIT cache rather than spinning up a second `Engine`.
pub(crate) fn component_engine_pub() -> Arc<Engine> {
    component_engine()
}

// ---------------------------------------------------------------------------
// ComponentHarness
// ---------------------------------------------------------------------------

/// Pre-compiled component harness.
///
/// Build once per unique Wasm binary (or per component type in a pool);
/// call [`ComponentHarness::run`] for each invocation. Thread-safe.
///
/// ```rust,no_run
/// use basin_fn::ComponentHarness;
/// use basin_fn::engine::InvocationContext;
///
/// let wasm_bytes: Vec<u8> = std::fs::read("my_component.wasm").unwrap();
/// let harness = ComponentHarness::new(&wasm_bytes).unwrap();
/// let ctx = InvocationContext::mock();
/// // sync `run` retained for benchmark callers that don't need governance.
/// let result = harness.run(ctx).unwrap();
/// ```
pub struct ComponentHarness {
    engine: Arc<Engine>,
    component: Component,
    linker: wasmtime::component::Linker<FunctionHost>,
    /// Optional governance handle. When present, [`ComponentHarness::run_with`]
    /// routes through [`FunctionGovernance::invoke_with_caps`] for the full
    /// CPU + memory + wall-clock + per-project-semaphore + dedicated-runtime
    /// enforcement chain.
    governance: Option<Arc<FunctionGovernance>>,
}

impl ComponentHarness {
    /// Compile `wasm_bytes` into a component and pre-link the host imports.
    ///
    /// Built against the process-wide engine; **no governance is attached**.
    /// Callers that need per-invocation governance should use
    /// [`ComponentHarness::with_governance`].
    ///
    /// `wasm_bytes` must be a valid Wasm component (component model format,
    /// not a core module). Use `wasm-tools component new` or Javy /
    /// ComponentizeJS to produce one.
    pub fn new(wasm_bytes: &[u8]) -> anyhow::Result<Self> {
        let engine = component_engine();
        Self::build(engine, wasm_bytes, None)
    }

    /// Compile `wasm_bytes` against `governance`'s engine and attach
    /// `governance` so [`ComponentHarness::run_with`] enforces every cap.
    ///
    /// Re-add of the builder reverted in W5; per Phase 6.P0.C every real
    /// caller of `ComponentHarness::run` must go through here so the W5
    /// caps actually fire on the W2 / W6 invoke path.
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
        add_host_to_linker(&mut linker)?;

        Ok(Self { engine, component, linker, governance })
    }

    /// Instantiate the component with the provided `ctx` and call `run`.
    ///
    /// Returns the guest's `run()` return value: `None` on success, or
    /// `Some(error_message)` if the guest reports an error. The wasmtime
    /// `Result` wraps trap / linkage failures.
    ///
    /// Each call creates a fresh `Store`; Stores are not reused across
    /// invocations (Wasm linear memory is reset each time).
    ///
    /// **No governance**: this entrypoint is for benchmark / bench-harness
    /// callers that build harnesses via [`ComponentHarness::new`]. Production
    /// callers should use [`ComponentHarness::run_with`].
    pub fn run(&self, ctx: InvocationContext) -> anyhow::Result<Option<String>> {
        let call_ctx = FunctionCallContext::new(ctx);
        let host = FunctionHost::new(call_ctx);
        let mut store = Store::new(&self.engine, host);

        // Apply per-invocation caps matching the 5.11.J epoch mechanism.
        // 5 epochs ≈ 5 × BASIN_WASM_EPOCH_MS ms of CPU time. Without a
        // governance attached there is no ticker, so this acts as defence
        // in depth for the bench harness only.
        store.set_epoch_deadline(5);

        let bindings = BasinFunctions::instantiate(&mut store, &self.component, &self.linker)?;
        Ok(bindings.call_run(&mut store)?)
    }

    /// Governance-wrapped invocation: enforces CPU (epoch trap), memory
    /// ([`MemoryLimiter`] installed on the Store), wall-clock timeout,
    /// per-project semaphore, and runs the synchronous wasm work on the
    /// **dedicated wasm tokio runtime** rather than the global
    /// `spawn_blocking` pool.
    ///
    /// `project` is the calling project. The semaphore is keyed on it so
    /// concurrency is bounded per-tenant.
    ///
    /// Panics if the harness was built via [`ComponentHarness::new`]
    /// (without governance). Callers that don't know which constructor was
    /// used should call [`ComponentHarness::has_governance`] first.
    pub async fn run_with(
        self: &Arc<Self>,
        project: ProjectId,
        ctx: InvocationContext,
    ) -> anyhow::Result<Option<String>> {
        let gov = self.governance.clone().expect(
            "ComponentHarness::run_with requires a governance-attached harness; \
             use ComponentHarness::with_governance(..) at construction",
        );
        let me = self.clone();
        let caps = gov.caps().clone();
        // Clone `gov` for the closure body; the outer `gov` is borrowed by
        // `invoke_with_caps`. Both Arcs alias the same FunctionGovernance.
        let gov_inner = gov.clone();
        gov.invoke_with_caps(project, move || {
            let call_ctx = FunctionCallContext::new(ctx);
            let host = FunctionHost::with_limiter(
                call_ctx,
                MemoryLimiter::new(caps.memory_max_bytes),
            );
            let mut store = Store::new(&me.engine, host);
            // Install the per-Store memory limiter + CPU trap. The closure
            // reaches the limiter through `FunctionHost::limiter_mut`.
            gov_inner.prepare_store_with_limiter(
                &mut store,
                |h: &mut FunctionHost| h.limiter_mut(),
            );
            let bindings =
                BasinFunctions::instantiate(&mut store, &me.component, &me.linker)?;
            Ok(bindings.call_run(&mut store)?)
        })
        .await
    }

    /// Whether this harness was built with [`Self::with_governance`].
    pub fn has_governance(&self) -> bool {
        self.governance.is_some()
    }
}
