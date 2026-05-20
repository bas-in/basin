//! W5 acceptance tests — actual Wasm fixtures exercise each per-invocation
//! resource cap implemented in [`basin_fn::governance`].
//!
//! - `cpu_cap_kills_spinner`: a WAT component whose `run` body is a
//!   `(loop $spin (br $spin))` exceeds the configured epoch deadline and is
//!   killed by the epoch trap.
//! - `memory_cap_kills_grow_past_cap`: a WAT component whose `run` body calls
//!   `memory.grow` with a huge page count traps because the engine's hard
//!   memory cap is reached.
//! - `wall_timeout_kills_long_sleep`: a long-running blocking closure
//!   wrapped by [`FunctionGovernance::invoke_with_caps`] is killed by the
//!   `tokio::time::timeout` after the configured wall-clock budget.
//! - `project_concurrency_limits_in_flight`: with `project_concurrency=2`,
//!   three concurrent invocations on the same project produce at most 2
//!   simultaneous in-flight executions; the third waits.
//!
//! The component tests reuse the governance's own `Engine` so the epoch
//! ticker actually drives those stores' deadlines (the harness's
//! process-wide engine is a different one).

use std::sync::Arc;
use std::time::Duration;

use basin_common::ids::ProjectId;
use basin_fn::engine::InvocationContext;
use basin_fn::governance::{FunctionCaps, FunctionGovernance};

// ---------------------------------------------------------------------------
// WAT fixtures
// ---------------------------------------------------------------------------

/// Infinite-loop guest. The exported `run` returns option<string>; the core
/// body never returns, so the epoch trap is the only way it can terminate.
const SPINNER_WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      i32.const 0)
    (func (export "run_inner") (result i32)
      (loop $spin (br $spin))
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

/// Memory-grow guest. The core body calls `memory.grow` with a huge page
/// count (10000 × 64 KiB ≈ 640 MiB), far past the 1 MiB cap the test
/// configures. With the engine's hard memory cap, the grow attempt traps
/// the guest.
const MEMORY_HOG_WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      i32.const 0)
    (func (export "run_inner") (result i32)
      ;; Attempt to grow by 10000 pages (≈ 640 MiB). With memory_may_move=false
      ;; and reservation == cap, this either returns -1 *or* traps; we then
      ;; deliberately also write into the grown region to force a trap if
      ;; the grow merely returned -1.
      i32.const 10000
      memory.grow
      drop
      ;; Write to address 100 MiB to force a bounds-check trap regardless.
      i32.const 104857600
      i32.const 42
      i32.store
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn caps_for_cpu_test() -> FunctionCaps {
    FunctionCaps {
        // Two ticks (≈ 200 ms) is plenty to spot a spinner. The ticker fires
        // every 100 ms so the trap should hit within ~300 ms wall time.
        cpu_ticks: 2,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(5),
        project_concurrency: 4,
    }
}

fn caps_for_mem_test() -> FunctionCaps {
    FunctionCaps {
        cpu_ticks: 50,
        // Cap at 1 MiB so a 640 MiB grow is far over the line.
        memory_max_bytes: 1024 * 1024,
        wall_timeout: Duration::from_secs(5),
        project_concurrency: 4,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// W5 acceptance #1: a Wasm fixture that spins forever is killed at the
/// CPU cap (epoch trap).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpu_cap_kills_spinner() {
    let caps = caps_for_cpu_test();
    let gov = FunctionGovernance::new(caps);

    // Build the harness against the SAME engine the governance ticks, so the
    // epoch counter increments actually affect this store.
    let wasm = wat::parse_str(SPINNER_WAT).expect("WAT spinner parses");

    // The harness's `new()` uses its own process-wide engine; for this test
    // we need the governance's engine to drive the ticks. Build the harness
    // with that engine explicitly via the closure passed to invoke_with_caps.
    let engine = gov.engine().clone();
    let component = wasmtime::component::Component::new(&engine, &wasm)
        .expect("component compiles");
    let component = Arc::new(component);

    let project = ProjectId::new();

    let started = std::time::Instant::now();
    let result: anyhow::Result<()> = gov
        .invoke_with_caps(project, {
            let gov = gov.clone();
            let engine = engine.clone();
            let component = component.clone();
            move || {
                // Build a Store backed by an empty FunctionHost; the spinner
                // doesn't call any host imports.
                let ctx = InvocationContext::mock();
                let call_ctx = basin_fn::FunctionCallContext::new(ctx);
                let host = basin_fn::FunctionHost::new(call_ctx);
                let mut store = wasmtime::Store::new(&engine, host);
                gov.prepare_store(&mut store);

                let mut linker: wasmtime::component::Linker<basin_fn::FunctionHost> =
                    wasmtime::component::Linker::new(&engine);
                basin_fn::host::add_host_to_linker(&mut linker)?;
                // Instantiating + calling `run` must trap once the epoch
                // counter passes the deadline.
                let instance = linker.instantiate(&mut store, &component)?;
                let run = instance
                    .get_typed_func::<(), (Option<String>,)>(&mut store, "run")?;
                run.call(&mut store, ())?;
                Ok(())
            }
        })
        .await;
    let elapsed = started.elapsed();

    let err = result.expect_err("spinner should be killed by the CPU cap");
    let msg = format!("{err:#}");
    // Wasmtime's epoch trap surfaces as "interrupt" / "wasm trap" — either
    // path is acceptable so long as the spinner did not run to completion.
    assert!(
        msg.contains("interrupt") || msg.contains("trap") || msg.contains("epoch"),
        "expected epoch interrupt / trap, got: {msg}"
    );
    // With 2-tick deadline and 100 ms tick, the kill should land well under
    // the 5 s wall timeout — give generous headroom for CI jitter.
    assert!(
        elapsed < Duration::from_secs(3),
        "spinner kill should happen well before the 5 s wall timeout (took {elapsed:?})"
    );
}

/// W5 acceptance #2: a Wasm fixture that tries to grow memory past the cap
/// is killed.
///
/// The cap is enforced at the engine `Config` level
/// (`memory_reservation` + `memory_reservation_for_growth(0)` +
/// `memory_may_move(false)`) — see `governance.rs` for the rationale. The
/// guest's `memory.grow` request either returns `-1` (in which case our
/// fixture's follow-up store traps on bounds-check) or traps directly. Both
/// paths surface as a trap from `invoke_with_caps`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_cap_kills_grow_past_cap() {
    let caps = caps_for_mem_test();
    let gov = FunctionGovernance::new(caps);
    let engine = gov.engine().clone();
    let wasm = wat::parse_str(MEMORY_HOG_WAT).expect("WAT memory_hog parses");
    let component = Arc::new(
        wasmtime::component::Component::new(&engine, &wasm).expect("component compiles"),
    );

    let project = ProjectId::new();

    let result: anyhow::Result<()> = gov
        .invoke_with_caps(project, {
            let gov = gov.clone();
            let engine = engine.clone();
            let component = component.clone();
            move || {
                let ctx = InvocationContext::mock();
                let call_ctx = basin_fn::FunctionCallContext::new(ctx);
                let host = basin_fn::FunctionHost::new(call_ctx);
                let mut store = wasmtime::Store::new(&engine, host);
                gov.prepare_store(&mut store);

                let mut linker: wasmtime::component::Linker<basin_fn::FunctionHost> =
                    wasmtime::component::Linker::new(&engine);
                basin_fn::host::add_host_to_linker(&mut linker)?;
                let instance = linker.instantiate(&mut store, &component)?;
                let run = instance
                    .get_typed_func::<(), (Option<String>,)>(&mut store, "run")?;
                run.call(&mut store, ())?;
                Ok(())
            }
        })
        .await;
    let err = result.expect_err("memory_hog should be killed by the memory cap");
    let msg = format!("{err:#}");
    // The exact wording depends on which path tripped (grow rejection vs
    // bounds-check on the follow-up store) — either is a trap.
    assert!(
        msg.contains("trap")
            || msg.contains("out of bounds")
            || msg.contains("memory")
            || msg.contains("cap"),
        "expected memory-related trap, got: {msg}"
    );
}

/// W5 acceptance #3: a fixture that sleeps past `wall_timeout` is killed
/// via `tokio::time::timeout`.
///
/// We exercise this through `invoke_with_caps` directly: the spec describes
/// a "fixture that sleeps", and the wall guard treats blocking-thread sleep
/// identically to any other long-running synchronous work. A pure Wasm
/// fixture cannot sleep without WASI, and W5 governance is WASI-agnostic,
/// so a blocking-closure fixture is the faithful test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wall_timeout_kills_long_sleep() {
    let caps = FunctionCaps {
        cpu_ticks: 1000,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_millis(80),
        project_concurrency: 4,
    };
    let gov = FunctionGovernance::new(caps);
    let project = ProjectId::new();

    let started = std::time::Instant::now();
    let result: anyhow::Result<()> = gov
        .invoke_with_caps(project, move || {
            std::thread::sleep(Duration::from_millis(800));
            Ok(())
        })
        .await;
    let elapsed = started.elapsed();

    let err = result.expect_err("long sleep should be killed by wall timeout");
    assert!(
        err.to_string().contains("wall-clock timeout"),
        "expected wall-clock timeout error, got: {err}"
    );
    // We should NOT have waited the full 800 ms — wall timeout is 80 ms.
    assert!(
        elapsed < Duration::from_millis(500),
        "wall timeout should fire well under the sleep duration (took {elapsed:?})"
    );
}

/// W5 acceptance #4: with `project_concurrency=2`, three concurrent
/// invocations on the same project → exactly 2 run at once, 3rd waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn project_concurrency_limits_in_flight() {
    let caps = FunctionCaps {
        cpu_ticks: 1000,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(5),
        project_concurrency: 2,
    };
    let gov = FunctionGovernance::new(caps);
    let project = ProjectId::new();

    let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..3 {
        let gov = gov.clone();
        let in_flight = in_flight.clone();
        let max_seen = max_seen.clone();
        handles.push(tokio::spawn(async move {
            gov.invoke_with_caps(project, move || {
                let now = in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                // Hold the permit long enough that the third invocation
                // genuinely waits at the semaphore, not just races through.
                std::thread::sleep(Duration::from_millis(120));
                in_flight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, anyhow::Error>(())
            })
            .await
            .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let peak = max_seen.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        peak, 2,
        "expected at most 2 concurrent invocations under project_concurrency=2, saw peak={peak}"
    );
}
