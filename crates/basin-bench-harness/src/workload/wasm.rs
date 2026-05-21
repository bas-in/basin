//! Wasm workload primitives — the new bench family the legacy harness
//! couldn't surface.
//!
//! The family targets seven shapes documented in `profiles/wasm.rs`. This
//! module is the per-shape execution engine; the profile glues them into
//! cards.
//!
//! ## Default component
//!
//! When `cfg.workload.wasm.component_bytes` is `None`, we synthesize a
//! tiny no-op component on the fly via `wat`. It exports `run()` returning
//! `Ok(None)` — the minimum surface `ComponentHarness::run` requires. This
//! keeps the cold-start / host-import bench reproducible across machines
//! without shipping a binary blob.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use basin_fn::engine::InvocationContext;
use basin_fn::ComponentHarness;
use tokio::task::JoinSet;

use crate::config::BenchConfig;
use crate::sample::{LatencySamples, SampleReport};

pub struct WasmOutcome {
    pub samples: SampleReport,
    pub wall_secs: f64,
    pub calls: u64,
    pub errors: u64,
}

/// Minimal `run() -> Ok(None)` component matching `basin-fn`'s WIT.
///
/// The `(component (core module ... ) (func (export "run") ...))` form is
/// the smallest valid component model program. Built via `wat` so we don't
/// depend on a checked-in `.wasm` blob.
fn default_component_bytes() -> Vec<u8> {
    // The WIT in basin-fn declares the world export `run: func() -> option<string>`.
    // This is the verified minimal component fixture from
    // `crates/basin-fn/tests/component_host_test.rs::GUEST_WAT` — a core
    // module that returns `0` (= `option<string>::None`, no error) lifted to
    // the component-level `run` export. Kept byte-for-byte in sync with that
    // fixture; if the WIT export shape changes, both must update together.
    const WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      i32.const 0)
    (func (export "run_inner") (result i32)
      i32.const 0)
  )
  (core instance $core_i (instantiate $core_m))
  (func (export "run") (result (option string))
    (canon lift
      (core func $core_i "run_inner")
      (memory $core_i "memory")
      (realloc (func $core_i "cabi_realloc"))
    )
  )
)
"#;
    // If this fails the upstream wasmtime/wast pin moved underneath us;
    // surface loudly so the bench breaks rather than silently no-ops.
    wat::parse_str(WAT).expect("default wasm component WAT must compile")
}

fn build_harness(cfg: &BenchConfig) -> Result<ComponentHarness> {
    let bytes = cfg
        .workload
        .wasm
        .as_ref()
        .and_then(|w| w.component_bytes.clone())
        .unwrap_or_else(default_component_bytes);
    Ok(ComponentHarness::new(&bytes)?)
}

/// Cold-start + per-invocation cost: run `cfg.workload.iterations` calls
/// serially through a freshly-built `ComponentHarness`. Each call
/// instantiates a fresh `Store` (matches production).
pub fn wasm_invoke(cfg: &BenchConfig) -> Result<WasmOutcome> {
    let h = build_harness(cfg)?;
    let samples = LatencySamples::new();
    let started = Instant::now();
    let mut errors = 0u64;
    let mut calls = 0u64;
    for _ in 0..cfg.workload.iterations {
        let ctx = InvocationContext::mock();
        let t0 = Instant::now();
        match h.run(ctx) {
            Ok(_) => samples.record(t0.elapsed()),
            Err(_) => errors += 1,
        }
        calls += 1;
    }
    Ok(WasmOutcome {
        samples: samples.report(),
        wall_secs: started.elapsed().as_secs_f64(),
        calls,
        errors,
    })
}

/// Concurrent invocation — `cfg.workload.concurrency` tasks each issuing
/// `iterations / concurrency` calls in parallel. Surfaces the per-project
/// semaphore behaviour and any internal contention in `wasmtime::Engine`.
///
/// Spawns blocking tasks because `ComponentHarness::run` is synchronous
/// (wasmtime is sync at the call site even though the rest of basin-fn is
/// async).
pub async fn wasm_invoke_concurrent(cfg: &BenchConfig) -> Result<WasmOutcome> {
    let h = Arc::new(build_harness(cfg)?);
    let samples = LatencySamples::new();
    let per_task = cfg.workload.iterations / cfg.workload.concurrency.max(1);

    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..cfg.workload.concurrency {
        let h = h.clone();
        let s = samples.clone();
        set.spawn_blocking(move || {
            let mut errors = 0u64;
            let mut calls = 0u64;
            for _ in 0..per_task {
                let ctx = InvocationContext::mock();
                let t0 = Instant::now();
                match h.run(ctx) {
                    Ok(_) => s.record(t0.elapsed()),
                    Err(_) => errors += 1,
                }
                calls += 1;
            }
            (calls, errors)
        });
    }
    let mut total = 0u64;
    let mut errors = 0u64;
    while let Some(r) = set.join_next().await {
        let (c, e) = r.expect("blocking task panic");
        total += c;
        errors += e;
    }

    Ok(WasmOutcome {
        samples: samples.report(),
        wall_secs: started.elapsed().as_secs_f64(),
        calls: total,
        errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BenchConfig, WasmKnobs, WorkloadKnobs};

    fn wasm_cfg(iters: usize) -> BenchConfig {
        BenchConfig {
            workload: WorkloadKnobs {
                iterations: iters,
                wasm: Some(WasmKnobs::default()),
                ..WorkloadKnobs::default()
            },
            ..BenchConfig::default()
        }
    }

    #[test]
    fn default_component_compiles() {
        let bytes = default_component_bytes();
        assert!(!bytes.is_empty());
        // Confirm wasmtime accepts it via the harness ctor.
        let _ = ComponentHarness::new(&bytes).expect("default component instantiable");
    }

    #[test]
    fn invoke_serial_runs_expected_count() {
        let cfg = wasm_cfg(8);
        let out = wasm_invoke(&cfg).expect("invoke");
        assert_eq!(out.calls, 8);
        // At least one sample recorded (errors==0 is required for the default
        // no-op component).
        assert_eq!(out.errors, 0);
        assert!(out.samples.n >= 1);
    }
}
