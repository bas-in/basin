//! [`FunctionGovernance`] — per-invocation resource governance for Wasm
//! component functions (Phase 5.11.W5, hardened by Phase 6.P0.C).
//!
//! W1 ([`ComponentHarness`](crate::ComponentHarness)) created a fresh
//! [`wasmtime::Store`] per invocation with an epoch deadline set, but no
//! interrupter, no memory cap, no wall-clock guard, and no per-project
//! admission control. W5 added those four caps; 6.P0.C wires them onto the
//! real `ComponentHarness::run` / `HandlerHarness::handle` entrypoints and
//! lifts the noisy-neighbor concerns flagged in
//! `docs/audits/2026-05-21-noisy-neighbor-fairness.md` (item #16):
//!
//! | Cap | Mechanism |
//! |---|---|
//! | **CPU**            | [`wasmtime::Engine::increment_epoch`] ticked by a tokio task; each store is configured with `set_epoch_deadline(cpu_ticks)` + `epoch_deadline_trap()` so a runaway loop traps once it has consumed `cpu_ticks` ticks. |
//! | **Linear memory**  | [`wasmtime::ResourceLimiter`] impl ([`MemoryLimiter`]) installed on every [`Store`] via `store.limiter(...)`. The cap is enforced per-Store *and* engine-wide via `memory_reservation` so the trap fires before host OOM. |
//! | **Wall clock**     | [`tokio::time::timeout`] wraps the dedicated-runtime blocking task that runs the synchronous wasm call. On expiry the epoch is bumped past the deadline to interrupt the wasm thread, then an error is returned. |
//! | **Concurrency**    | A bounded [`LruCache<ProjectId, Arc<Semaphore>>`] of bounded [`tokio::sync::Semaphore`]s, default 16 permits each. The LRU evicts least-recently-used entries on insert when full, so per-tenant cost is O(bytes), not O(distinct projects ever seen). |
//! | **Runtime**        | A dedicated [`tokio::runtime::Runtime`] sized via `BASIN_FN_WORKER_THREADS` (default 4) runs every blocking wasm call. Without this, Wasm shares the global `spawn_blocking` pool with axum, the shard-mode executor (basin-engine), and basin-net — a few aggressive tenants starve all of them. |
//!
//! Defaults come from constants below; [`FunctionCaps::from_env`] reads the
//! `BASIN_FN_CPU_TICKS / BASIN_FN_MEM_MB / BASIN_FN_WALL_MS /
//! BASIN_FN_PROJECT_CONCURRENCY / BASIN_FN_PROJECT_SEM_CAP /
//! BASIN_FN_WORKER_THREADS` environment variables for overrides.
//!
//! ## Scope
//!
//! This module deliberately does **not** know about the wasmtime bindings
//! generated for our WIT world — it operates on opaque
//! `FnOnce() -> Result` closures so the same plumbing handles the
//! `basin-functions` (`run`) and `basin-functions-handler` (`handle`) worlds
//! without forcing one to import the other.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use basin_common::ids::ProjectId;
use lru::LruCache;
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

/// Per-project semaphore-map capacity. Defaults to 10 000 active projects;
/// past this, the LRU evicts the least-recently-used entry on every insert.
/// Configurable via `BASIN_FN_PROJECT_SEM_CAP`.
pub const DEFAULT_PROJECT_SEMAPHORE_CAP: usize = 10_000;

/// Dedicated-runtime worker-thread count default: 4. Configurable via
/// `BASIN_FN_WORKER_THREADS`. The Wasm runtime is **separate** from the main
/// tokio runtime so a flood of wasm invocations cannot starve the rest of
/// the process (axum HTTP, shard-mode executor, basin-net outbound).
pub const DEFAULT_WORKER_THREADS: usize = 4;

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
    /// Max distinct projects whose per-project semaphore is held in memory.
    /// LRU-evicted; an idle project's semaphore is dropped when the cap is
    /// reached. Default [`DEFAULT_PROJECT_SEMAPHORE_CAP`].
    pub project_semaphore_cap: usize,
    /// Number of worker threads in the dedicated Wasm tokio runtime.
    /// Default [`DEFAULT_WORKER_THREADS`].
    pub worker_threads: usize,
}

impl FunctionCaps {
    /// Built-in defaults (see constants above).
    pub fn defaults() -> Self {
        Self {
            cpu_ticks: DEFAULT_CPU_TICKS,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_millis(DEFAULT_WALL_TIMEOUT_MS),
            project_concurrency: DEFAULT_PROJECT_CONCURRENCY,
            project_semaphore_cap: DEFAULT_PROJECT_SEMAPHORE_CAP,
            worker_threads: DEFAULT_WORKER_THREADS,
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
        if let Some(v) = lookup("BASIN_FN_PROJECT_SEM_CAP").and_then(|s| s.parse().ok()) {
            c.project_semaphore_cap = v;
        }
        if let Some(v) = lookup("BASIN_FN_WORKER_THREADS").and_then(|s| s.parse().ok()) {
            c.worker_threads = v;
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

/// Cap on linear-memory growth for a single invocation. Installed on every
/// [`Store`] via [`Store::limiter`] from inside
/// [`FunctionGovernance::prepare_store_with_limiter`].
///
/// Returning `Err` from `memory_growing` causes wasmtime to trap the guest
/// (per the [`ResourceLimiter`] docs) — i.e. the function is killed rather
/// than silently observing `-1` from `memory.grow`. We want killed.
///
/// Per-Store rather than per-Engine: each invocation gets its own
/// [`MemoryLimiter`] with the configured cap, so accounting is isolated.
#[derive(Clone, Debug)]
pub struct MemoryLimiter {
    max_bytes: usize,
}

impl MemoryLimiter {
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }

    /// The configured cap. Exposed so [`crate::FunctionHost`] can construct a
    /// limiter from caps without re-importing the constant.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
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
/// Defence in depth: per-Store [`MemoryLimiter`] is the primary cap; engine
/// reservation is the backstop in case a host fails to install the limiter.
fn apply_memory_cap_to_config(cfg: &mut wasmtime::Config, max_bytes: usize) {
    cfg.memory_reservation(max_bytes as u64);
    cfg.memory_reservation_for_growth(0);
    cfg.memory_may_move(false);
}

// ---------------------------------------------------------------------------
// Dedicated tokio runtime — keeps wasm off the global spawn_blocking pool
// ---------------------------------------------------------------------------

/// Owns the dedicated multi-thread tokio runtime that runs every Wasm
/// invocation's blocking closure.
///
/// Why a *separate* runtime? `tokio::task::spawn_blocking` posts onto the
/// global blocking pool (default 512 threads), shared with axum's blocking
/// extractors, the basin-engine shard-mode executor, basin-net's blocking
/// HTTP fallbacks, and any other crate that ever calls `spawn_blocking`. A
/// burst of wasm guests that hold their blocking thread for the full wall
/// timeout (e.g. 10 s default) can starve everything else process-wide.
/// The dedicated runtime gives wasm a sized, isolated budget — over-capacity
/// callers wait on the wasm semaphore, not on the global pool.
///
/// ## Why the `Runtime` lives on a side thread
///
/// `tokio::runtime::Runtime` panics when dropped from inside another
/// runtime's async context — but [`FunctionGovernance`] is created and
/// dropped from inside tokio tasks in tests and in production. The fix is
/// to build the runtime on a dedicated std thread that *owns* the
/// `Runtime` value; the rest of the process interacts via a cheap
/// [`tokio::runtime::Handle`] clone. Dropping the [`WasmRuntime`] signals
/// the owner thread to shut the runtime down off any tokio context.
pub(crate) struct WasmRuntime {
    handle: tokio::runtime::Handle,
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl WasmRuntime {
    fn new(worker_threads: usize) -> anyhow::Result<Self> {
        let workers = worker_threads.max(1);
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();

        let thread = std::thread::Builder::new()
            .name("basin-fn-wasm-rt".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(workers)
                    // Generous blocking-pool sizing per worker: each wasm
                    // invocation runs on a blocking thread (spawn_blocking)
                    // so the pool is the actual concurrency ceiling. Cap at
                    // workers * 16 — the per-project semaphore bounds
                    // in-flight wasm calls below this.
                    .max_blocking_threads(workers.saturating_mul(16).max(16))
                    .thread_name("basin-fn-wasm")
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(_) => return,
                };
                let _ = handle_tx.send(rt.handle().clone());
                // Block this thread until shutdown — we own the Runtime and
                // must outlive every spawn_blocking caller.
                let _ = shutdown_rx.recv();
                // Drop the runtime *off* any tokio context (this thread is
                // a vanilla std thread), so the blocking-pool shutdown does
                // not panic.
                drop(rt);
            })
            .context("spawn dedicated wasm runtime thread")?;

        let handle = handle_rx
            .recv()
            .context("dedicated wasm runtime failed to initialise")?;

        Ok(Self {
            handle,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    /// Spawn the synchronous `work` closure on the dedicated wasm pool's
    /// blocking thread set. Returns a `JoinHandle` callers await from the
    /// main runtime.
    fn spawn_blocking<F, R>(&self, work: F) -> tokio::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(work)
    }
}

impl Drop for WasmRuntime {
    fn drop(&mut self) {
        // Signal the owner thread; ignore send errors (means the thread
        // already exited). Then join so the Runtime is fully torn down
        // before this Drop returns — important for tests that count
        // threads.
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

// ---------------------------------------------------------------------------
// FunctionGovernance — top-level coordinator
// ---------------------------------------------------------------------------

/// Per-process governance state for Wasm function invocations.
///
/// One instance is typically shared across all functions: it owns the
/// `Engine` (so the epoch ticker can drive it), the bounded per-project
/// semaphore cache, the dedicated wasm runtime, and the immutable
/// [`FunctionCaps`].
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
    /// Bounded LRU of per-project semaphores. Mutex-guarded because
    /// `LruCache::get` mutates the recency order. Cheap (sub-µs lookup).
    project_sems: Mutex<LruCache<ProjectId, Arc<Semaphore>>>,
    /// Dedicated tokio runtime that owns every wasm-blocking thread.
    wasm_runtime: WasmRuntime,
    // Hold the ticker guard so dropping `FunctionGovernance` ends the
    // background task — avoids leaking a tokio task per test instance.
    _ticker: Mutex<Option<TickerGuard>>,
    /// Phase 6.X.D — optional cross-replica gate for the per-project
    /// `WasmConcurrency` cap (ADR 0023). When attached and the
    /// coordinator has handed out a slice, the gate refuses invocations
    /// that would breach the per-partition slice even though the local
    /// `project_sems` semaphore still has permits.
    slice_gate: Mutex<Option<basin_catalog::SliceGate>>,
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
    /// `caps`. Starts the epoch ticker and the dedicated wasm runtime.
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
        // LruCache requires a non-zero capacity. Floor at 1 so a malformed
        // env override doesn't panic; a cap of 1 just thrashes (acceptable
        // failure mode versus a process abort).
        let sem_cap = NonZeroUsize::new(caps.project_semaphore_cap.max(1))
            .expect("max(1) is non-zero");
        let wasm_runtime = WasmRuntime::new(caps.worker_threads)
            .expect("dedicated wasm runtime construction");
        Arc::new(Self {
            engine,
            caps,
            project_sems: Mutex::new(LruCache::new(sem_cap)),
            wasm_runtime,
            _ticker: Mutex::new(Some(TickerGuard { handle })),
            slice_gate: Mutex::new(None),
        })
    }

    /// Phase 6.X.D — attach a [`basin_catalog::SliceGate`] keyed on
    /// [`basin_catalog::CapKind::WasmConcurrency`]. The shard heartbeat
    /// loop fills the underlying [`basin_catalog::SliceBudgetView`];
    /// subsequent calls to [`Self::invoke_with_caps`] reject (with
    /// `WasmConcurrency slice exhausted`) when the project's per-partition
    /// slice is exhausted, closing the multi-instance bypass.
    pub fn attach_slice_gate(&self, gate: basin_catalog::SliceGate) {
        *self.slice_gate.lock().expect("slice_gate poisoned") = Some(gate);
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

    /// Acquire or lazily create the semaphore for `project`. LRU-bounded:
    /// when the cache is full and an entry is inserted, the least-recently
    /// used entry is dropped. A busy project is touched on every invocation
    /// so it stays warm at the head of the LRU.
    ///
    /// If a project IS evicted while holding outstanding permits, the
    /// dropped `Arc<Semaphore>` survives via the still-held
    /// `OwnedSemaphorePermit`s; the next call for that project builds a
    /// fresh semaphore. Slightly weakens the cap during the transition
    /// (worst case `project_concurrency × 2`) — acceptable given the LRU
    /// is sized at 10 K projects by default.
    fn project_sem(&self, project: ProjectId) -> Arc<Semaphore> {
        let mut cache = self.project_sems.lock().expect("project_sems poisoned");
        if let Some(existing) = cache.get(&project) {
            return existing.clone();
        }
        let sem = Arc::new(Semaphore::new(self.caps.project_concurrency));
        cache.put(project, sem.clone());
        sem
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

    /// How many distinct projects currently have a semaphore in the LRU.
    /// Exposed for tests; production code shouldn't need this.
    pub fn project_semaphore_cache_len(&self) -> usize {
        self.project_sems
            .lock()
            .expect("project_sems poisoned")
            .len()
    }

    /// Configure `store` with the per-invocation CPU cap (epoch deadline +
    /// trap-on-deadline). Engine-level `memory_reservation` is the only
    /// memory cap on this path; callers that want the per-Store
    /// [`MemoryLimiter`] (Phase 6.P0.C) should use
    /// [`Self::prepare_store_with_limiter`] instead.
    pub fn prepare_store<T: Send + 'static>(&self, store: &mut Store<T>) {
        store.set_epoch_deadline(self.caps.cpu_ticks);
        store.epoch_deadline_trap();
    }

    /// Variant of [`Self::prepare_store`] that ALSO installs a per-Store
    /// [`ResourceLimiter`] via [`wasmtime::Store::limiter`]. `limiter` is a
    /// closure that, given the store-data type `T`, returns a `&mut dyn
    /// ResourceLimiter` — typically a slot on `T` (e.g.
    /// [`crate::FunctionHost::limiter_mut`]).
    ///
    /// This is the Phase 6.P0.C path: the per-invocation memory cap fires
    /// from inside the `Store` (where wasmtime's accounting lives) rather
    /// than relying solely on the engine-level reservation.
    pub fn prepare_store_with_limiter<T>(
        &self,
        store: &mut Store<T>,
        limiter: impl (FnMut(&mut T) -> &mut dyn ResourceLimiter) + Send + Sync + 'static,
    ) {
        store.set_epoch_deadline(self.caps.cpu_ticks);
        store.epoch_deadline_trap();
        store.limiter(limiter);
    }

    /// Acquire the per-project semaphore, run `work` on the **dedicated wasm
    /// runtime**'s blocking thread set under a wall-clock timeout, and
    /// translate every failure mode into an `anyhow::Error` with a clear
    /// message.
    ///
    /// `work` is `FnOnce() -> anyhow::Result<R>` — typically the closure
    /// builds a `Store`, calls `prepare_store_with_limiter`, instantiates
    /// the component, and calls `call_run` / `call_handle`.
    ///
    /// If the wall-clock timeout fires the epoch is bumped enough to trip
    /// the deadline trap inside the wasm thread, so a runaway CPU-bound
    /// guest is interrupted promptly instead of leaking a thread.
    pub async fn invoke_with_caps<F, R>(&self, project: ProjectId, work: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> anyhow::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        // Phase 6.X.D — slice gate first. The per-process semaphore (below)
        // still bounds local in-flight invocations, but the slice gate is
        // the binding cap across all replicas when a coordinator total is
        // configured. Reject immediately when the slice is exhausted; do
        // not block on the local semaphore (a noisy tenant would otherwise
        // queue up against their own slice).
        //
        // Clone the gate out of the mutex into a local Option so the
        // `std::sync::MutexGuard` is dropped before we hit any `.await` —
        // otherwise the resulting future is `!Send` and tokio can't spawn it.
        let slice_gate: Option<basin_catalog::SliceGate> = {
            let g = self.slice_gate.lock().expect("slice_gate poisoned");
            g.as_ref().cloned()
        };
        if let Some(gate) = slice_gate {
            if gate
                .try_consume(project, basin_catalog::CapKind::WasmConcurrency, 1)
                .await
                .is_err()
            {
                return Err(anyhow!(
                    "per-project WasmConcurrency slice exhausted (coordinator-handed)"
                ));
            }
        }
        let sem = self.project_sem(project);
        let _permit = sem
            .clone()
            .acquire_owned()
            .await
            .context("project semaphore closed")?;

        let engine = self.engine.clone();
        let wall = self.caps.wall_timeout;
        let cpu_ticks = self.caps.cpu_ticks;

        // Route onto the dedicated wasm runtime's blocking pool. The global
        // `tokio::task::spawn_blocking` would compete with axum / shard-mode
        // executor / basin-net — see WasmRuntime docs.
        let join = self.wasm_runtime.spawn_blocking(work);

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
        assert_eq!(c.project_semaphore_cap, DEFAULT_PROJECT_SEMAPHORE_CAP);
        assert_eq!(c.worker_threads, DEFAULT_WORKER_THREADS);
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
            "BASIN_FN_PROJECT_SEM_CAP"     => Some("42".to_string()),
            "BASIN_FN_WORKER_THREADS"      => Some("9".to_string()),
            _ => None,
        };
        let c = FunctionCaps::from_lookup(lookup);
        assert_eq!(c.cpu_ticks, 7);
        assert_eq!(c.memory_max_bytes, 8 * 1024 * 1024);
        assert_eq!(c.wall_timeout, Duration::from_millis(1234));
        assert_eq!(c.project_concurrency, 3);
        assert_eq!(c.project_semaphore_cap, 42);
        assert_eq!(c.worker_threads, 9);
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
            project_semaphore_cap: 10,
            worker_threads: 2,
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
            project_semaphore_cap: 10,
            worker_threads: 2,
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
            project_semaphore_cap: 10,
            worker_threads: 2,
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

    /// LRU bound on per-project semaphores: with a cap of 4, adding 100 cold
    /// projects must leave only 4 entries — the 96 idle ones get evicted.
    /// This is the W5-followup #16 / noisy-neighbor audit fix: the prior
    /// `DashMap` grew unboundedly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn project_semaphore_map_is_lru_bounded() {
        let caps = FunctionCaps {
            cpu_ticks: 100,
            memory_max_bytes: DEFAULT_MEMORY_MAX_BYTES,
            wall_timeout: Duration::from_secs(2),
            project_concurrency: 4,
            // 4-entry LRU so we can prove eviction without burning time.
            project_semaphore_cap: 4,
            worker_threads: 2,
        };
        let gov = FunctionGovernance::new(caps);

        // Touch 100 distinct projects; each call completes before the next.
        for _ in 0..100 {
            let project = ProjectId::new();
            gov.invoke_with_caps(project, || Ok::<_, anyhow::Error>(()))
                .await
                .expect("succeeds");
        }

        let len = gov.project_semaphore_cache_len();
        assert!(
            len <= 4,
            "project semaphore LRU must cap at 4 entries; saw {len}"
        );
    }
}
