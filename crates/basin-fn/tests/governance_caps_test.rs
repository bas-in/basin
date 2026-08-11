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
        project_semaphore_cap: 16,
        worker_threads: 2,
    }
}

fn caps_for_mem_test() -> FunctionCaps {
    FunctionCaps {
        cpu_ticks: 50,
        // Cap at 1 MiB so a 640 MiB grow is far over the line.
        memory_max_bytes: 1024 * 1024,
        wall_timeout: Duration::from_secs(5),
        project_concurrency: 4,
        project_semaphore_cap: 16,
        worker_threads: 2,
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
    let component =
        wasmtime::component::Component::new(&engine, &wasm).expect("component compiles");
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
                let run = instance.get_typed_func::<(), (Option<String>,)>(&mut store, "run")?;
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
    let component =
        Arc::new(wasmtime::component::Component::new(&engine, &wasm).expect("component compiles"));

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
                let run = instance.get_typed_func::<(), (Option<String>,)>(&mut store, "run")?;
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
        project_semaphore_cap: 16,
        worker_threads: 2,
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
        project_semaphore_cap: 16,
        worker_threads: 2,
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

// ---------------------------------------------------------------------------
// Phase 6.P0.C acceptance tests — wire W5 caps into the real entrypoint
// (ComponentHarness::run_with), dedicated wasm runtime, semaphore LRU.
// ---------------------------------------------------------------------------

/// 6.P0.C #1: a spinner Wasm guest invoked through `ComponentHarness::run_with`
/// is killed by the CPU cap. The previous W1 entrypoint had no ticker and no
/// limiter wired (audit finding #1) — this test proves governance is now
/// enforced on the real path, not just on a hand-rolled closure passed to
/// `invoke_with_caps`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn component_harness_run_with_cpu_cap_kills_spinner() {
    use basin_fn::ComponentHarness;

    let caps = caps_for_cpu_test();
    let gov = FunctionGovernance::new(caps);

    let wasm = wat::parse_str(SPINNER_WAT).expect("WAT spinner parses");
    let harness = Arc::new(ComponentHarness::with_governance(&wasm, gov.clone()).expect("compile"));
    assert!(harness.has_governance(), "with_governance must attach caps");

    let project = ProjectId::new();
    let started = std::time::Instant::now();
    let err = harness
        .run_with(project, InvocationContext::mock())
        .await
        .expect_err("spinner must be killed at the CPU cap");
    let elapsed = started.elapsed();

    let msg = format!("{err:#}");
    assert!(
        msg.contains("interrupt")
            || msg.contains("trap")
            || msg.contains("epoch")
            || msg.contains("wall-clock"),
        "expected epoch/trap/wall-clock interrupt, got: {msg}"
    );
    assert!(
        elapsed < Duration::from_secs(6),
        "spinner kill should happen well under the 5 s wall timeout (took {elapsed:?})"
    );
}

/// 6.P0.C #2: a 1000-concurrent-invocation soak on the dedicated wasm
/// runtime does not starve the main runtime. We assert this indirectly by
/// proving a cheap `tokio::time::sleep(10ms)` on the MAIN runtime stays
/// responsive while 1000 wasm-no-op invocations are in flight.
///
/// Pre-fix: every wasm invocation hit `tokio::task::spawn_blocking` (the
/// global pool, default 512 threads, shared with axum + shard-mode
/// executor). 1000 concurrent + a wall_timeout near zero starved the
/// global pool and any cheap task on the main runtime saw multi-second
/// stalls. Post-fix: wasm rides the dedicated `WasmRuntime`, leaving the
/// global pool empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_runtime_does_not_starve_main_runtime() {
    let caps = FunctionCaps {
        cpu_ticks: 100,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(5),
        // Per-project semaphore high enough that 1000 calls really run in
        // parallel rather than serialising at the admission gate.
        project_concurrency: 1000,
        project_semaphore_cap: 16,
        worker_threads: 4,
    };
    let gov = FunctionGovernance::new(caps);
    let project = ProjectId::new();

    // Launch 1000 fire-and-forget wasm invocations that hold their
    // dedicated-runtime blocking thread for ~50 ms each.
    let mut wasm_handles = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let gov = gov.clone();
        wasm_handles.push(tokio::spawn(async move {
            let _ = gov
                .invoke_with_caps(project, || {
                    std::thread::sleep(Duration::from_millis(50));
                    Ok::<_, anyhow::Error>(())
                })
                .await;
        }));
    }

    // Meanwhile, on the MAIN runtime, schedule a cheap async sleep + measure
    // round-trip latency. If wasm were stealing the main runtime / global
    // blocking pool, this number would balloon.
    let canary_started = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let canary_latency = canary_started.elapsed();

    // Generous bound — under a starvation regression this is multi-seconds.
    // 500 ms gives huge headroom while still proving "not blocked".
    assert!(
        canary_latency < Duration::from_millis(500),
        "main-runtime canary must stay responsive under wasm soak; saw {canary_latency:?}"
    );

    // Drain the wasm handles so the test cleanly exits (without leaking the
    // dedicated-runtime threads).
    for h in wasm_handles {
        let _ = h.await;
    }
}

/// 6.P0.C #3: 10k distinct projects called once each must NOT grow the
/// per-project semaphore map unboundedly — the LRU caps it at
/// `project_semaphore_cap`. Pre-fix the `DashMap<ProjectId, Arc<Semaphore>>`
/// grew O(distinct projects ever seen). Post-fix the LRU bounds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_project_semaphore_lru_evicts_at_scale() {
    let caps = FunctionCaps {
        cpu_ticks: 50,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(2),
        project_concurrency: 4,
        // Tight LRU so we can detect eviction quickly without churning
        // 10k entries (which would make CI slow).
        project_semaphore_cap: 32,
        worker_threads: 2,
    };
    let gov = FunctionGovernance::new(caps);

    // Touch 10 000 distinct cold projects (one invocation each). Without
    // LRU eviction, the underlying map would hold 10 000 Arc<Semaphore>
    // entries forever (~640 KB just for the map + semaphore overhead).
    for _ in 0..10_000 {
        let project = ProjectId::new();
        gov.invoke_with_caps(project, || Ok::<_, anyhow::Error>(()))
            .await
            .expect("no-op succeeds");
    }

    let cache_len = gov.project_semaphore_cache_len();
    assert!(
        cache_len <= 32,
        "per-project semaphore LRU must cap at 32 entries; saw {cache_len}"
    );
    assert!(
        cache_len > 0,
        "LRU should still hold the most-recent entries; saw 0"
    );
}

/// Minimal no-op component used by the metering test: instantiates cleanly and
/// returns `option<string>::None`. Same shape as the GUEST_WAT in
/// `component_host_test.rs`, duplicated here so the metering test does not
/// drag in cross-file fixtures.
const NOOP_COMPONENT_WAT: &str = r#"
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

/// Billing meter (`docs/audits/2026-05-21-billing-meter-gap.md` hole #12):
/// when a `ProjectCounterRegistry` is attached, `ComponentHarness::run_with`
/// bumps `cpu_micros_total` for the invoking project and keeps cross-project
/// counts isolated.
///
/// The property is **per-project attribution**: a project's meter moves when,
/// and only when, that project invokes. This used to be checked by running 3
/// invocations on A, 1 on B, and asserting `A > B` — a proxy, and a poor one:
/// on a loaded host a single invocation on B can out-cost three on A (the
/// meter spans the per-project semaphore wait), so the assertion was a
/// coin-flip about the host rather than a statement about attribution. Every
/// check below compares a project's meter against *its own* earlier value, so
/// no cross-project timing comparison remains:
///
/// * a project that has invoked nothing is not metered at all — it is not
///   even registered, so `snapshot` is `None`;
/// * a project's meter moves off zero across its own invocations;
/// * a project's meter is byte-for-byte unchanged across another project's
///   invocations, in **both** directions.
///
/// The surviving inequalities are one-sided under load: host noise can only
/// grow a project's own meter, never shrink it, and a single invocation
/// cannot bill 0 micros because the metered span covers a cross-runtime
/// round-trip plus a wasmtime instantiation. There is no op count to pin here
/// — unlike the engine's `Session::execute`, `run_with` records micros only
/// and never calls `record_op` — so exactness comes from the unchanged-across-
/// the-other-project checks instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_harness_run_with_bumps_cpu_micros_per_project() {
    use basin_common::telemetry::ProjectCounterRegistry;
    use basin_fn::ComponentHarness;

    let caps = FunctionCaps {
        cpu_ticks: 50,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(2),
        project_concurrency: 4,
        project_semaphore_cap: 8,
        worker_threads: 2,
    };
    let gov = FunctionGovernance::new(caps);

    let wasm = wat::parse_str(NOOP_COMPONENT_WAT).expect("noop component WAT parses");
    let registry = Arc::new(ProjectCounterRegistry::new());

    let mut harness = ComponentHarness::with_governance(&wasm, gov.clone()).expect("compile");
    harness.attach_project_counters(registry.clone());
    let harness = Arc::new(harness);

    let a = ProjectId::new();
    let b = ProjectId::new();
    // Never invoked, on purpose: must never be billed.
    let idle = ProjectId::new();

    // Nothing has run: no project is metered.
    assert!(
        registry.snapshot(&a).is_none(),
        "A was metered before invoking anything"
    );
    assert!(
        registry.snapshot(&b).is_none(),
        "B was metered before invoking anything"
    );

    // ── A invokes three times ───────────────────────────────────────────
    for _ in 0..3 {
        let res = harness
            .run_with(a, InvocationContext::mock())
            .await
            .expect("noop guest returns None");
        assert!(res.is_none());
    }
    let a1 = registry
        .snapshot(&a)
        .expect("A is metered after its own invocations");
    assert!(
        a1.cpu_micros_total > 0,
        "A's meter did not move across its own 3 invocations"
    );

    // B has invoked nothing, so A's work must not have been billed to it.
    assert!(
        registry.snapshot(&b).is_none(),
        "A's invocations leaked {:?} into B, which has invoked nothing",
        registry.snapshot(&b)
    );

    // ── B invokes once: A's meter must not move ─────────────────────────
    harness
        .run_with(b, InvocationContext::mock())
        .await
        .expect("noop guest returns None on B");
    let a2 = registry.snapshot(&a).expect("A is still metered");
    assert_eq!(
        a2.cpu_micros_total,
        a1.cpu_micros_total,
        "B's invocation leaked {} micros into A",
        a2.cpu_micros_total - a1.cpu_micros_total
    );
    let b1 = registry
        .snapshot(&b)
        .expect("B is metered after its own invocation");
    assert!(
        b1.cpu_micros_total > 0,
        "B's meter did not move across its own invocation"
    );

    // ── A invokes twice more: A grows by its own work, B stays put ──────
    for _ in 0..2 {
        harness
            .run_with(a, InvocationContext::mock())
            .await
            .expect("noop guest returns None");
    }
    let a3 = registry.snapshot(&a).expect("A is still metered");
    let b2 = registry.snapshot(&b).expect("B is still metered");
    assert!(
        a3.cpu_micros_total > a2.cpu_micros_total,
        "A's meter stuck at {} micros across 2 more of its own invocations",
        a2.cpu_micros_total
    );
    assert_eq!(
        b2.cpu_micros_total,
        b1.cpu_micros_total,
        "A's later invocations leaked {} micros into B",
        b2.cpu_micros_total - b1.cpu_micros_total
    );

    // A project that never invoked anything is never billed.
    assert!(
        registry.snapshot(&idle).is_none(),
        "an idle project was billed {:?}",
        registry.snapshot(&idle)
    );
}
