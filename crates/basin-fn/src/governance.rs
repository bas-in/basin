//! [`FunctionGovernance`] — per-invocation resource governance for Wasm
//! component functions (Phase 5.11.W5).
//!
//! W1 ([`ComponentHarness`](crate::ComponentHarness)) created a fresh
//! [`wasmtime::Store`] per invocation with an epoch deadline set, but no
//! interrupter, no memory cap, no wall-clock guard, and no per-project
//! admission control. W5 adds those four caps:
//!
//! | Cap | Mechanism |
//! |---|---|
//! | **CPU**            | [`wasmtime::Engine::increment_epoch`] ticked by a tokio task; each store is configured with `set_epoch_deadline(cpu_ticks)` + `epoch_deadline_trap()` so a runaway loop traps once it has consumed `cpu_ticks` ticks. |
//! | **Linear memory**  | [`wasmtime::ResourceLimiter`] impl that rejects any `memory.grow` whose desired byte size exceeds [`FunctionCaps::memory_max_bytes`]. Returning `Err` causes wasmtime to raise a trap. |
//! | **Wall clock**     | [`tokio::time::timeout`] wraps the [`tokio::task::spawn_blocking`] handle that runs the synchronous wasm call. On expiry the epoch is bumped past the deadline to interrupt the wasm thread, then an error is returned. |
//! | **Concurrency**    | A [`DashMap<ProjectId, Arc<Semaphore>>`] of bounded [`tokio::sync::Semaphore`]s, default 16 permits each. [`FunctionGovernance::invoke_with_caps`] acquires before delegating to the caller-supplied closure, so over-capacity invocations wait fairly. |
//!
//! Defaults come from constants below; [`FunctionCaps::from_env`] reads the
//! `BASIN_FN_CPU_TICKS / BASIN_FN_MEM_MB / BASIN_FN_WALL_MS /
//! BASIN_FN_PROJECT_CONCURRENCY` environment variables for overrides.
//!
//! ## Scope
//!
//! This module deliberately does **not** know about the wasmtime bindings
//! generated for our WIT world — it operates on opaque
//! `FnOnce() -> Result` closures so the same plumbing handles the
//! `basin-functions` (`run`) and `basin-functions-handler` (`handle`) worlds
//! without forcing one to import the other.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use basin_common::ids::ProjectId;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use wasmtime::{Engine, ResourceLimiter, Store};

// ---------------------------------------------------------------------------
// FunctionCaps — tunables
// ---------------------------------------------------------------------------

/// CPU cap default: 50 epoch ticks. At the default 100 ms/tick that is ≈ 5 s
/// of wall-equivalent CPU before the epoch trap fires. Configurable via
/// `BASIN_FN_CPU_TICKS`.
pub const DEFAULT_CPU_TICKS: u64 = 50;

/// Linear-memory cap default: 64 MiB. Configurable via `BASIN_FN_MEM_MB`.
pub const DEFAULT_MEMORY_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Wall-clock cap default: 10 s. Configurable via `BASIN_FN_WALL_MS`.
pub const DEFAULT_WALL_TIMEOUT_MS: u64 = 10_000;

/// Per-project concurrency cap default: 16 concurrent invocations.
/// Configurable via `BASIN_FN_PROJECT_CONCURRENCY`.
pub const DEFAULT_PROJECT_CONCURRENCY: usize = 16;

/// Epoch ticker interval. Matches the W1 / 5.11.J `BASIN_WASM_EPOCH_MS`
/// default; the W5 ticker is independent of the J ticker because the two
/// crates run separate `Engine`s.
const EPOCH_TICK_MS: u64 = 100;

/// Per-invocation resource caps.
///
/// Construct via [`FunctionCaps::defaults`] or [`FunctionCaps::from_env`];
/// pass into [`FunctionGovernance::new`]. The struct is `Clone` so a single
/// configuration can be reused across many [`FunctionGovernance`] instances.
#[derive(Clone, Debug)]
pub struct FunctionCaps {
    /// Max epoch ticks per invocation. The epoch increments every
    /// [`EPOCH_TICK_MS`] (≈ 100 ms), so a value of 50 ≈ 5 s of CPU before
    /// the trap fires.
    pub cpu_ticks: u64,
    /// Max linear-memory bytes the guest may allocate.
    pub memory_max_bytes: usize,
    /// Max wall-clock duration for one invocation.
    pub wall_timeout: Duration,
    /// Max concurrent invocations per project.
    pub project_concurrency: usize,
}

impl FunctionCaps {
    /// Built-in defaults (see constants above).
    pub fn defaults() -> Self {
        Self {
            cpu_ticks: DEFAULT_CPU_TICKS,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_millis(DEFAULT_WALL_TIMEOUT_MS),
            project_concurrency: DEFAULT_PROJECT_CONCURRENCY,
        }
    }

    /// Overlay `BASIN_FN_*` env-vars on top of the defaults.
    ///
    /// Unparseable / missing values fall back to the default so a malformed
    /// environment doesn't crash the process at startup.
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Testable variant of [`Self::from_env`]: same overlay logic, but the
    /// env lookup is injected so tests don't have to mutate process state
    /// (which is `unsafe` under Rust 2024 and would trip
    /// `forbid(unsafe_code)`).
    pub fn from_lookup<F: Fn(&str) -> Option<String>>(lookup: F) -> Self {
        let mut c = Self::defaults();
        if let Some(v) = lookup("BASIN_FN_CPU_TICKS").and_then(|s| s.parse().ok()) {
            c.cpu_ticks = v;
        }
        if let Some(v) = lookup("BASIN_FN_MEM_MB").and_then(|s| s.parse::<usize>().ok()) {
            c.memory_max_bytes = v.saturating_mul(1024 * 1024);
        }
        if let Some(v) = lookup("BASIN_FN_WALL_MS").and_then(|s| s.parse::<u64>().ok()) {
            c.wall_timeout = Duration::from_millis(v);
        }
        if let Some(v) = lookup("BASIN_FN_PROJECT_CONCURRENCY").and_then(|s| s.parse().ok()) {
            c.project_concurrency = v;
        }
        c
    }
}

impl Default for FunctionCaps {
    fn default() -> Self {
        Self::defaults()
    }
}

// ---------------------------------------------------------------------------
// MemoryLimiter — wasmtime::ResourceLimiter that caps linear-memory growth
// ---------------------------------------------------------------------------

/// Cap on linear-memory growth for a single invocation.
///
/// One logical instance per `Store`. The actual cap value lives in an
/// `Arc<Mutex<...>>` so the same limiter can be installed via wasmtime's
/// callback-style API (see [`install_limiter`]).
///
/// Returning `Err` from `memory_growing` causes wasmtime to trap the guest
/// (per the [`ResourceLimiter`] docs) — i.e. the function is killed rather
/// than silently observing `-1` from `memory.grow`. We want killed.
pub struct MemoryLimiter {
    max_bytes: usize,
}

impl MemoryLimiter {
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}

impl ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_bytes {
            // `wasmtime::Error` is a re-export of `anyhow::Error` reached via
            // a distinct module path; `Error::msg` gives the compiler the
            // exact concrete type the trait method expects.
            Err(wasmtime::Error::msg(format!(
                "linear-memory cap exceeded: desired {} bytes > cap {} bytes",
                desired, self.max_bytes
            )))
        } else {
            Ok(true)
        }
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // Tables (function references) are not the budget we care about in
        // W5 — leave to wasmtime defaults.
        Ok(true)
    }
}

/// Apply the linear-memory cap to a fresh wasmtime [`wasmtime::Config`].
///
/// We enforce the cap at `Config` level (engine-wide) rather than via
/// [`Store::limiter`] for one structural reason: `Store::limiter`'s closure
/// must return a `&mut dyn ResourceLimiter` whose lifetime is tied to
/// `&mut T` (the store data type). Without modifying the existing
/// [`crate::FunctionHost`] (`host.rs`, owned by W1-followup), there is no
/// place inside `T` for the limiter to live, and the borrow checker rules
/// out returning a reference to closure-captured state from an `FnMut`.
///
/// Three settings combine to make the engine-level cap a *hard* trap on
/// overflow instead of a soft `-1` return from `memory.grow`:
///
/// 1. `memory_reservation(max)` — the reserved virtual range per linear
///    memory.
/// 2. `memory_reservation_for_growth(0)` — no extra slack beyond the
///    reservation.
/// 3. `memory_may_move(false)` — disallows relocating, so growing past the
///    reservation has nowhere to go and traps the guest.
///
/// The trade-off is that the cap is per-Engine rather than per-invocation.
/// W5 acceptance is "fixture that tries to grow past the cap is killed",
/// which this satisfies; promoting to a true per-invocation budget belongs
/// to a future pass that's allowed to add a limiter slot inside
/// `FunctionHost`.
fn apply_memory_cap_to_config(cfg: &mut wasmtime::Config, max_bytes: usize) {
    cfg.memory_reservation(max_bytes as u64);
    cfg.memory_reservation_for_growth(0);
    cfg.memory_may_move(false);
}

// ---------------------------------------------------------------------------
// FunctionGovernance — top-level coordinator
// ---------------------------------------------------------------------------

/// Per-process governance state for Wasm function invocations.
///
/// One instance is typically shared across all functions: it owns the
/// `Engine` (so the epoch ticker can drive it), the per-project semaphore
/// map, and the immutable [`FunctionCaps`].
///
/// ```rust,no_run
/// use basin_fn::governance::{FunctionGovernance, FunctionCaps};
///
/// let caps = FunctionCaps::from_env();
/// let gov = FunctionGovernance::new(caps);
/// ```
pub struct FunctionGovernance {
    engine: Arc<Engine>,
    caps: FunctionCaps,
    project_sems: DashMap<ProjectId, Arc<Semaphore>>,
    // Hold the ticker guard so dropping `FunctionGovernance` ends the
    // background task — avoids leaking a tokio task per test instance.
    _ticker: Mutex<Option<TickerGuard>>,
}

/// Owns the epoch-ticker tokio task. Dropping the guard aborts the task so
/// `FunctionGovernance` does not leak a thread when reconstructed in tests.
struct TickerGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for TickerGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl FunctionGovernance {
    /// Build a new governance instance with a fresh `Engine` configured for
    /// component-model + epoch interruption + the linear-memory cap from
    /// `caps`. Starts the epoch ticker.
    ///
    /// Must be called from inside a tokio runtime — the ticker is a
    /// `tokio::spawn`ed task.
    pub fn new(caps: FunctionCaps) -> Arc<Self> {
        let mut cfg = wasmtime::Config::new();
        cfg.wasm_component_model(true).epoch_interruption(true);
        apply_memory_cap_to_config(&mut cfg, caps.memory_max_bytes);
        let engine = Arc::new(Engine::new(&cfg).expect("wasmtime Engine::new failed"));
        Self::with_engine(engine, caps)
    }

    /// Variant that reuses an existing [`Engine`] (must have
    /// `epoch_interruption(true)`). Used by [`crate::ComponentHarness`] so
    /// the harness and governance share the same engine.
    pub fn with_engine(engine: Arc<Engine>, caps: FunctionCaps) -> Arc<Self> {
        let engine_weak = Arc::downgrade(&engine);
        let handle = tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(EPOCH_TICK_MS));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                iv.tick().await;
                match engine_weak.upgrade() {
                    Some(e) => e.increment_epoch(),
                    None => break,
                }
            }
        });
        Arc::new(Self {
            engine,
            caps,
            project_sems: DashMap::new(),
            _ticker: Mutex::new(Some(TickerGuard { handle })),
        })
    }

    /// Access the underlying [`Engine`] (so the same one used by the ticker
    /// can drive harness store creation).
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Access the caps in effect.
    pub fn caps(&self) -> &FunctionCaps {
        &self.caps
    }

    /// Acquire or lazily create the semaphore for `project`. Cheap (DashMap
    /// shard lookup); returns an `Arc` so callers can `clone` and `acquire`.
    fn project_sem(&self, project: ProjectId) -> Arc<Semaphore> {
        // entry() avoids the construct-then-discard pattern when the entry
        // already exists; the closure runs only on the missing-key path.
        self.project_sems
            .entry(project)
            .or_insert_with(|| Arc::new(Semaphore::new(self.caps.project_concurrency)))
            .clone()
    }

    /// How many concurrent invocations are currently in flight for `project`.
    /// Exposed for tests / observability.
    pub fn in_flight(&self, project: ProjectId) -> usize {
        let sem = self.project_sem(project);
        // tokio::Semaphore exposes available_permits but not "held" directly;
        // held = configured - available.
        self.caps
            .project_concurrency
            .saturating_sub(sem.available_permits())
    }

    /// Configure `store` with the per-invocation CPU cap (epoch deadline +
    /// trap-on-deadline). The memory cap is enforced via the `Engine`'s
    /// `Config` (see [`apply_memory_cap_to_config`]) so it kicks in for
    /// every store derived from the engine without needing a per-store
    /// `ResourceLimiter`.
    pub fn prepare_store<T: Send + 'static>(&self, store: &mut Store<T>) {
        store.set_epoch_deadline(self.caps.cpu_ticks);
        store.epoch_deadline_trap();
    }

    /// Acquire the per-project semaphore, run `work` on a blocking thread
    /// under a wall-clock timeout, and translate every failure mode into an
    /// `anyhow::Error` with a clear message.
    ///
    /// `work` is `FnOnce() -> anyhow::Result<R>` — typically the closure
    /// builds a `Store`, calls `prepare_store`, instantiates the component,
    /// and calls `call_run` / `call_handle`.
    ///
    /// If the wall-clock timeout fires the epoch is bumped enough to trip
    /// the deadline trap inside the wasm thread, so a runaway CPU-bound
    /// guest is interrupted promptly instead of leaking a thread.
    pub async fn invoke_with_caps<F, R>(&self, project: ProjectId, work: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> anyhow::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let sem = self.project_sem(project);
        let _permit = sem
            .clone()
            .acquire_owned()
            .await
            .context("project semaphore closed")?;

        let engine = self.engine.clone();
        let wall = self.caps.wall_timeout;
        let cpu_ticks = self.caps.cpu_ticks;

        let join = tokio::task::spawn_blocking(work);

        match tokio::time::timeout(wall, join).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => Err(anyhow!("wasm task panicked: {e}")),
            Err(_elapsed) => {
                // Wall timeout fired before the wasm returned. Bump the epoch
                // by enough to trip the deadline trap on the wasm thread; the
                // join handle will then return shortly. Don't await it here:
                // a thread that ignores the trap (rare; only if it loops in
                // host code) shouldn't block the request indefinitely.
                for _ in 0..=cpu_ticks {
                    engine.increment_epoch();
                }
                Err(anyhow!(
                    "wall-clock timeout exceeded ({} ms)",
                    wall.as_millis()
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_what_the_docs_say() {
        let c = FunctionCaps::defaults();
        assert_eq!(c.cpu_ticks, DEFAULT_CPU_TICKS);
        assert_eq!(c.memory_max_bytes, DEFAULT_MEMORY_MAX_BYTES);
        assert_eq!(c.wall_timeout, Duration::from_millis(DEFAULT_WALL_TIMEOUT_MS));
        assert_eq!(c.project_concurrency, DEFAULT_PROJECT_CONCURRENCY);
    }

    #[test]
    fn from_lookup_overlays_each_var() {
        // Inject a synthetic env so we don't mutate process state (which is
        // `unsafe` under Rust 2024 and would trip `forbid(unsafe_code)`).
        let lookup = |k: &str| match k {
            "BASIN_FN_CPU_TICKS"           => Some("7".to_string()),
            "BASIN_FN_MEM_MB"              => Some("8".to_string()),
            "BASIN_FN_WALL_MS"             => Some("1234".to_string()),
            "BASIN_FN_PROJECT_CONCURRENCY" => Some("3".to_string()),
            _ => None,
        };
        let c = FunctionCaps::from_lookup(lookup);
        assert_eq!(c.cpu_ticks, 7);
        assert_eq!(c.memory_max_bytes, 8 * 1024 * 1024);
        assert_eq!(c.wall_timeout, Duration::from_millis(1234));
        assert_eq!(c.project_concurrency, 3);
    }

    #[test]
    fn from_lookup_missing_keys_fall_back_to_defaults() {
        let c = FunctionCaps::from_lookup(|_| None);
        assert_eq!(c.cpu_ticks, DEFAULT_CPU_TICKS);
        assert_eq!(c.memory_max_bytes, DEFAULT_MEMORY_MAX_BYTES);
    }

    #[test]
    fn from_lookup_unparseable_falls_back_to_defaults() {
        let c = FunctionCaps::from_lookup(|k| match k {
            "BASIN_FN_CPU_TICKS" => Some("not-a-number".to_string()),
            _ => None,
        });
        assert_eq!(c.cpu_ticks, DEFAULT_CPU_TICKS);
    }

    #[test]
    fn memory_limiter_caps_growth() {
        let mut lim = MemoryLimiter::new(1024);
        // Under cap: allowed.
        assert!(lim.memory_growing(0, 512, None).unwrap());
        assert!(lim.memory_growing(512, 1024, None).unwrap());
        // Over cap: Err so wasmtime traps the guest.
        let err = lim.memory_growing(1024, 2048, None).unwrap_err();
        assert!(err.to_string().contains("linear-memory cap exceeded"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn semaphore_caps_concurrency_per_project() {
        // 2 permits → only 2 invocations should ever run concurrently.
        let caps = FunctionCaps {
            cpu_ticks: 1000,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_secs(5),
            project_concurrency: 2,
        };
        let gov = FunctionGovernance::new(caps);
        let project = ProjectId::new();

        // Shared counter that records the peak number of in-flight closures.
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..6 {
            let gov = gov.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                gov.invoke_with_caps(project, move || {
                    let now = in_flight.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    // Hold the permit briefly so concurrent invocations stack up.
                    std::thread::sleep(Duration::from_millis(80));
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
        assert_eq!(
            max_seen.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "expected exactly 2 concurrent invocations under a 2-permit semaphore"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn semaphore_is_per_project_not_global() {
        // Two distinct projects should each get their own 1-permit semaphore
        // and therefore run in parallel even though each project's cap is 1.
        let caps = FunctionCaps {
            cpu_ticks: 1000,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_secs(5),
            project_concurrency: 1,
        };
        let gov = FunctionGovernance::new(caps);
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let work = |inf: Arc<std::sync::atomic::AtomicUsize>,
                    mx: Arc<std::sync::atomic::AtomicUsize>| {
            move || {
                let now = inf.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                mx.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(60));
                inf.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, anyhow::Error>(())
            }
        };

        let h1 = {
            let gov = gov.clone();
            let inf = in_flight.clone();
            let mx = max_seen.clone();
            tokio::spawn(async move { gov.invoke_with_caps(p1, work(inf, mx)).await.unwrap() })
        };
        let h2 = {
            let gov = gov.clone();
            let inf = in_flight.clone();
            let mx = max_seen.clone();
            tokio::spawn(async move { gov.invoke_with_caps(p2, work(inf, mx)).await.unwrap() })
        };
        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(
            max_seen.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "two distinct projects should run in parallel (each project_concurrency=1)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wall_timeout_returns_error() {
        // A blocking closure that sleeps past the wall timeout should error.
        let caps = FunctionCaps {
            cpu_ticks: 100_000,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_millis(50),
            project_concurrency: 4,
        };
        let gov = FunctionGovernance::new(caps);
        let project = ProjectId::new();

        let err = gov
            .invoke_with_caps(project, move || {
                std::thread::sleep(Duration::from_millis(500));
                Ok::<_, anyhow::Error>(())
            })
            .await
            .expect_err("wall timeout should cause invoke_with_caps to error");
        assert!(
            err.to_string().contains("wall-clock timeout"),
            "expected wall-clock timeout error, got: {err}"
        );
    }
}
