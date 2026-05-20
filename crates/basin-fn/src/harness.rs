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

use std::sync::Arc;

use wasmtime::{Config, Engine, Store};
use wasmtime::component::Component;

use crate::engine::InvocationContext;
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
// Process-wide wasmtime Engine
// ---------------------------------------------------------------------------

static COMPONENT_ENGINE: std::sync::OnceLock<Arc<Engine>> = std::sync::OnceLock::new();

/// Return (or lazily initialise) the process-wide wasmtime Engine with
/// the component model enabled and epoch interruption on.
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
/// let result = harness.run(ctx).unwrap();
/// ```
pub struct ComponentHarness {
    engine: Arc<Engine>,
    component: Component,
    linker: wasmtime::component::Linker<FunctionHost>,
}

impl ComponentHarness {
    /// Compile `wasm_bytes` into a component and pre-link the host imports.
    ///
    /// `wasm_bytes` must be a valid Wasm component (component model format,
    /// not a core module). Use `wasm-tools component new` or Javy /
    /// ComponentizeJS to produce one.
    pub fn new(wasm_bytes: &[u8]) -> anyhow::Result<Self> {
        let engine = component_engine();
        let component = Component::new(&engine, wasm_bytes)?;

        let mut linker: wasmtime::component::Linker<FunctionHost> =
            wasmtime::component::Linker::new(&engine);
        add_host_to_linker(&mut linker)?;

        Ok(Self { engine, component, linker })
    }

    /// Instantiate the component with the provided `ctx` and call `run`.
    ///
    /// Returns the guest's `run()` return value: `None` on success, or
    /// `Some(error_message)` if the guest reports an error. The wasmtime
    /// `Result` wraps trap / linkage failures.
    ///
    /// Each call creates a fresh `Store`; Stores are not reused across
    /// invocations (Wasm linear memory is reset each time).
    pub fn run(&self, ctx: InvocationContext) -> anyhow::Result<Option<String>> {
        let call_ctx = FunctionCallContext::new(ctx);
        let host = FunctionHost::new(call_ctx);
        let mut store = Store::new(&self.engine, host);

        // Apply per-invocation caps matching the 5.11.J epoch mechanism.
        // 5 epochs ≈ 5 × BASIN_WASM_EPOCH_MS ms of CPU time.
        store.set_epoch_deadline(5);

        let bindings = BasinFunctions::instantiate(&mut store, &self.component, &self.linker)?;
        Ok(bindings.call_run(&mut store)?)
    }
}
