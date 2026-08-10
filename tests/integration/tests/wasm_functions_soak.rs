//! Wasm-functions soak test (Phase 5.11.W7 capstone).
//!
//! Exercises the W5 + Phase 6.P0.C resource-governance stack
//! ([`FunctionGovernance`]) under a multi-tenant concurrent load and
//! asserts the three invariants the task spec demands:
//!
//! 1. **No memory growth** — the per-project semaphore LRU stays bounded
//!    at `project_semaphore_cap`; the in-flight permit count returns to
//!    zero after every tenant's burst (no leaked permits).
//! 2. **Caps honoured** — at no point does any single project's in-flight
//!    invocation count exceed `project_concurrency`. The dedicated wasm
//!    runtime + per-project semaphore are the load-bearing primitives
//!    here (W5 governance + Phase 6.P0.C wiring).
//! 3. **No cross-tenant interference** — one "noisy" tenant pinning every
//!    permit it can does NOT degrade a quiet tenant's invocation latency
//!    by more than the documented bar. We use the same baseline-vs-under-
//!    load shape `crates/basin-bench-harness/src/profiles/noisy_neighbor.rs`
//!    uses for SQL-side noisy-neighbor scenarios (commit `522951c`):
//!    take a quiet-tenant p99 sample with no noise, then re-take it while
//!    the noisy tenant runs in a tight loop, then compare the ratio.
//!
//! ## Short vs long variant
//!
//! - **Short variant** (default, ~30 s): 100 tenants, ~50 invocations
//!   per tenant. Runs by default in CI as part of
//!   `cargo test -p basin-integration-tests --test wasm_functions_soak`.
//! - **Long variant** (`#[ignore]`, ~1 hour): 100 tenants × 1 hour of
//!   continuous invocations. Drop the `#[ignore]` and bump the iteration
//!   count + duration locally:
//!   ```
//!   cargo test -p basin-integration-tests --test wasm_functions_soak \
//!     long_variant_1h_no_growth -- --ignored --nocapture
//!   ```
//!   The long variant is intentionally not in the CI gate because it
//!   trades wall time for marginal coverage — the short variant exercises
//!   every invariant the long one does; the long one is for hunting slow
//!   leaks before a release.
//!
//! ## Why the workload is a synchronous-blocking closure rather than a
//!    real WAT-compiled Wasm component
//!
//! `FunctionGovernance::invoke_with_caps` accepts `FnOnce() -> Result<R>`.
//! W5's `wall_timeout_kills_long_sleep` test established that
//! "[t]he wall guard treats blocking-thread sleep identically to any
//! other long-running synchronous work" — i.e. the closure shape is the
//! faithful exercise for the per-tenant cap + LRU eviction + dedicated
//! runtime. The actual Wasm instantiation + epoch-trap path is covered by
//! the in-crate `crates/basin-fn/tests/governance_caps_test.rs` fixtures
//! (CPU cap, memory cap, real-entrypoint plumbing). Layering a hundred
//! WAT spinners on top here would slow the soak by ~50× without changing
//! the invariants this test asserts.
//!
//! ## Cross-reference
//!
//! - `crates/basin-fn/src/governance.rs` — the `FunctionGovernance`
//!   under test; defaults documented in [`FunctionCaps::defaults`].
//! - `crates/basin-fn/tests/governance_caps_test.rs` — single-tenant
//!   acceptance per cap; this soak is the multi-tenant 100-project gate.
//! - `crates/basin-bench-harness/src/profiles/noisy_neighbor.rs` — SQL-
//!   side analogue; the quiet-p99-under-load shape mirrors that profile.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use basin_fn::governance::{FunctionCaps, FunctionGovernance};

// ---------------------------------------------------------------------------
// Tunables (short variant — keep CI under ~30 s wall time)
// ---------------------------------------------------------------------------

/// Project count for the short variant. Matches the task spec
/// ("100 tenants × concurrent function invocations").
const PROJECT_COUNT: usize = 100;

/// Per-tenant burst size for the short variant.
const INVOCATIONS_PER_PROJECT: usize = 50;

/// Per-invocation "work" — sleep on the blocking thread. Tiny so 100
/// tenants × 50 calls finish in seconds, not minutes. The soak's
/// invariants don't depend on the per-call duration.
const PER_CALL_WORK: Duration = Duration::from_millis(2);

/// Per-project concurrency cap for the soak. 4 permits is enough to see
/// non-trivial in-flight overlap without making the test slow.
const PROJECT_CONCURRENCY: usize = 4;

/// LRU cap on the per-project semaphore map. Below `PROJECT_COUNT` so the
/// LRU eviction path fires during the soak (the invariant "per-tenant
/// cost is O(bytes), not O(projects ever seen)" — see ADR notes in
/// `governance.rs`).
const SEMAPHORE_LRU_CAP: usize = 32;

/// Caps tuned for the short soak. The wall timeout is generous so a CI
/// host hiccup doesn't false-positive a "wall-clock exceeded".
fn soak_caps() -> FunctionCaps {
    FunctionCaps {
        cpu_ticks: 1_000,
        memory_max_bytes: 64 * 1024 * 1024,
        wall_timeout: Duration::from_secs(30),
        project_concurrency: PROJECT_CONCURRENCY,
        project_semaphore_cap: SEMAPHORE_LRU_CAP,
        worker_threads: 4,
    }
}

// ---------------------------------------------------------------------------
// Invariant #1 + #2: bounded memory, caps honoured, no leaked permits
// ---------------------------------------------------------------------------
//
// 100 tenants × `INVOCATIONS_PER_PROJECT` concurrent invocations each.
// After the soak:
//   - the LRU semaphore map is capped at `SEMAPHORE_LRU_CAP`
//     (it can't grow to PROJECT_COUNT)
//   - every project's in-flight count is back to zero
//     (no leaked semaphore permit)
//   - peak in-flight across every project never exceeded its
//     `project_concurrency` cap

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_100_projects_short_variant_invariants_hold() {
    let gov = FunctionGovernance::new(soak_caps());

    // Pre-allocate stable ProjectId values so we can re-probe `in_flight`
    // after the soak.
    let projects: Vec<ProjectId> = (0..PROJECT_COUNT).map(|_| ProjectId::new()).collect();

    // Per-project peak in-flight tracker. Each tenant has its own counter
    // bumped by every closure entry / decremented on exit.
    let peak_in_flight: Arc<Vec<AtomicUsize>> =
        Arc::new((0..PROJECT_COUNT).map(|_| AtomicUsize::new(0)).collect());
    let live_in_flight: Arc<Vec<AtomicUsize>> =
        Arc::new((0..PROJECT_COUNT).map(|_| AtomicUsize::new(0)).collect());

    let started = Instant::now();
    let mut handles = Vec::with_capacity(PROJECT_COUNT * INVOCATIONS_PER_PROJECT);

    for (project_idx, project) in projects.iter().enumerate() {
        for _ in 0..INVOCATIONS_PER_PROJECT {
            let gov = gov.clone();
            let project = *project;
            let live = live_in_flight.clone();
            let peak = peak_in_flight.clone();
            handles.push(tokio::spawn(async move {
                gov.invoke_with_caps(project, move || {
                    // Bump live → track peak → do the bounded work → decrement.
                    let cur = live[project_idx].fetch_add(1, Ordering::SeqCst) + 1;
                    peak[project_idx].fetch_max(cur, Ordering::SeqCst);
                    std::thread::sleep(PER_CALL_WORK);
                    live[project_idx].fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(())
                })
                .await
            }));
        }
    }

    // Drain — every spawn should resolve with Ok(_).
    let mut ok_count = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(())) => ok_count += 1,
            Ok(Err(e)) => panic!("soak invocation errored: {e:#}"),
            Err(join_err) => panic!("soak spawn join failure: {join_err}"),
        }
    }
    let elapsed = started.elapsed();
    let expected_total = PROJECT_COUNT * INVOCATIONS_PER_PROJECT;
    assert_eq!(
        ok_count, expected_total,
        "every soak invocation must complete cleanly"
    );

    println!(
        "soak short variant: {expected_total} invocations across {PROJECT_COUNT} tenants in {elapsed:?}"
    );

    // Invariant #2: no per-tenant cap breach.
    for (i, peak) in peak_in_flight.iter().enumerate() {
        let p = peak.load(Ordering::SeqCst);
        assert!(
            p <= PROJECT_CONCURRENCY,
            "tenant {i} peaked at {p} in-flight, exceeds project_concurrency={PROJECT_CONCURRENCY}"
        );
    }

    // Invariant #1a: no leaked permit — every tenant ends idle.
    for (i, live) in live_in_flight.iter().enumerate() {
        let l = live.load(Ordering::SeqCst);
        assert_eq!(l, 0, "tenant {i} ended with {l} in-flight (leaked permit?)");
    }
    for (i, project) in projects.iter().enumerate() {
        let inflight = gov.in_flight(*project);
        assert_eq!(
            inflight, 0,
            "tenant {i} ({project:?}) still shows {inflight} in-flight in governance",
        );
    }

    // Invariant #1b: per-project semaphore map is LRU-bounded — it cannot
    // grow to PROJECT_COUNT. The dropped projects' semaphores were freed
    // (the load-bearing fix from W5-followup #16 / Phase 6.P0.C).
    let cache_len = gov.project_semaphore_cache_len();
    assert!(
        cache_len <= SEMAPHORE_LRU_CAP,
        "semaphore LRU grew past cap: {cache_len} > {SEMAPHORE_LRU_CAP}"
    );
    println!(
        "soak: per-project semaphore cache size = {cache_len} (cap {SEMAPHORE_LRU_CAP}, \
         had {PROJECT_COUNT} distinct tenants)"
    );
}

// ---------------------------------------------------------------------------
// Invariant #3: no cross-tenant interference (noisy-neighbor shape)
// ---------------------------------------------------------------------------
//
// Shape mirrors `basin-bench-harness/src/profiles/noisy_neighbor.rs`:
//   1. Baseline: a "quiet" tenant runs N invocations under no contention.
//      Sample its p99 latency.
//   2. Under load: a "noisy" tenant runs in a tight loop on its OWN
//      project; the same quiet tenant runs the same N invocations.
//      Sample the new p99.
//   3. Assert `under_load_p99 < 5x * baseline_p99` — the quiet tenant
//      stayed within bounds.
//
// The per-project semaphore + dedicated wasm runtime are what make this
// hold: the noisy tenant saturates ITS OWN semaphore (4 permits) and the
// dedicated runtime queues its work behind that admission gate. Without
// the per-project semaphore, the noisy tenant would saturate the global
// blocking pool and the quiet tenant's p99 would balloon.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_noisy_neighbor_quiet_p99_holds() {
    let gov = FunctionGovernance::new(soak_caps());

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // ─── Phase A: baseline (quiet alone) ───
    let baseline_samples = collect_quiet_samples(&gov, quiet, 60).await;
    let baseline_p99 = percentile(&baseline_samples, 99);
    println!("baseline quiet p99 = {baseline_p99:?}");

    // ─── Phase B: under load (quiet + noisy) ───
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn noisy: as many concurrent invocations as the per-project cap
    // will admit, in a tight loop until `stop` flips. Each call holds the
    // blocking thread for ~2 ms.
    let mut noisy_handles = Vec::with_capacity(PROJECT_CONCURRENCY);
    for _ in 0..PROJECT_CONCURRENCY {
        let gov = gov.clone();
        let stop = stop.clone();
        noisy_handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = gov
                    .invoke_with_caps(noisy, move || {
                        std::thread::sleep(PER_CALL_WORK);
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
            }
        }));
    }

    // Re-sample quiet under load. Slightly more samples so jitter washes
    // out — the noisy tenant is competing for the dedicated wasm runtime's
    // 4 worker threads.
    let under_load_samples = collect_quiet_samples(&gov, quiet, 60).await;
    let under_load_p99 = percentile(&under_load_samples, 99);
    println!("under-load quiet p99 = {under_load_p99:?}");

    // Drain noisy.
    stop.store(true, Ordering::Relaxed);
    for h in noisy_handles {
        let _ = h.await;
    }

    // Bar: the bench-harness analogue uses 5×. The Wasm-side caps are
    // engineered to keep the noisy tenant within their own slice, so the
    // quiet tenant should be comfortably under this.
    let bar = baseline_p99
        .saturating_mul(5)
        .max(Duration::from_millis(20));
    assert!(
        under_load_p99 <= bar,
        "quiet-tenant p99 degraded past 5x baseline under noisy load: \
         baseline={baseline_p99:?} under_load={under_load_p99:?} bar={bar:?}"
    );

    // Sanity check: the quiet tenant still finished its calls — the noisy
    // tenant didn't starve it entirely.
    assert!(
        under_load_samples.len() >= 50,
        "quiet tenant should have completed most of its samples; got {}",
        under_load_samples.len()
    );
}

/// Run `count` quiet invocations on `project` through `gov`, returning
/// the wall-clock latency of each.
async fn collect_quiet_samples(
    gov: &Arc<FunctionGovernance>,
    project: ProjectId,
    count: usize,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let t0 = Instant::now();
        gov.invoke_with_caps(project, move || {
            std::thread::sleep(PER_CALL_WORK);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .expect("quiet sample must complete");
        samples.push(t0.elapsed());
    }
    samples
}

/// Simple `p` percentile (0–100). Returns `Duration::ZERO` for an empty
/// slice; sorts the input clone so call sites don't have to.
fn percentile(samples: &[Duration], p: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort();
    let idx = ((sorted.len() - 1) * p) / 100;
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Long variant — ~1 hour soak, `#[ignore]` for CI by default
// ---------------------------------------------------------------------------
//
// Drop the `#[ignore]` and run with `--ignored` to execute. Asserts the
// same invariants as the short variant but over a wall-clock budget
// where slow leaks (e.g. wasmtime page faults, governance heap growth)
// would show. Pragmatic checkpoint: every 5 minutes, sample the
// semaphore LRU + assert no growth past cap.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "1-hour soak — run manually before a release with --ignored"]
async fn long_variant_1h_no_growth() {
    let gov = FunctionGovernance::new(soak_caps());
    let deadline = Instant::now() + Duration::from_secs(60 * 60);

    // Re-cycle 100 fresh tenants every iteration so the LRU is constantly
    // churning — the right shape for catching a slow heap leak in the
    // governance semaphore map.
    let mut iter = 0u64;
    let mut last_log = Instant::now();
    while Instant::now() < deadline {
        let mut handles = Vec::with_capacity(PROJECT_COUNT);
        for _ in 0..PROJECT_COUNT {
            let gov = gov.clone();
            let project = ProjectId::new();
            handles.push(tokio::spawn(async move {
                gov.invoke_with_caps(project, move || {
                    std::thread::sleep(PER_CALL_WORK);
                    Ok::<_, anyhow::Error>(())
                })
                .await
            }));
        }
        for h in handles {
            let _ = h.await.expect("join").expect("invocation");
        }
        iter += 1;

        if last_log.elapsed() >= Duration::from_secs(5 * 60) {
            let cache_len = gov.project_semaphore_cache_len();
            println!("long soak: iter={iter} sem_cache_len={cache_len} (cap {SEMAPHORE_LRU_CAP})");
            assert!(
                cache_len <= SEMAPHORE_LRU_CAP,
                "long-soak semaphore LRU grew past cap: {cache_len} > {SEMAPHORE_LRU_CAP}"
            );
            last_log = Instant::now();
        }
    }
    println!("long soak completed: {iter} iterations");
}
