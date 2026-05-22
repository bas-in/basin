//! `basin-pool` — native connection pool for Basin.
//!
//! Caches `basin_engine::ProjectSession` objects keyed on
//! `(ProjectId, ClientKey)` so short-lived client connections (Lambda,
//! Cloud Run, serverless workers) reuse warm sessions instead of paying
//! `Engine::open_session` cost per request.
//!
//! See [ADR 0007](../../../docs/decisions/0007-connection-pooling.md) for
//! the full design rationale and trigger condition. The short version:
//!
//! 1. Postgres needs pgbouncer because it forks a process per connection.
//!    Basin uses tokio tasks; that problem doesn't apply.
//! 2. What *does* cost something is `Engine::open_session` (catalog list +
//!    `ListingTable` registration per project). For long-lived connections
//!    this is amortised; for short-lived ones it dominates.
//! 3. Pgbouncer's transaction-pooling mode would corrupt per-session
//!    state Basin keeps (prepared statements, snapshot pins). Wrong tool.
//!
//! ## Public surface
//!
//! - [`SessionPool::new`] wraps an `Engine` and a [`PoolConfig`].
//! - [`SessionPool::acquire`] returns a [`PooledSession`] that drops back
//!   into the pool. The pool keys on `(project, client_key)`; pass
//!   `client_key = None` for the anonymous case (today's router default).
//! - [`SessionPool::spawn_eviction`] starts the background idle-eviction
//!   loop and returns an [`EvictionHandle`] for shutdown.
//! - [`SessionPool::stats`] returns a [`PoolStats`] snapshot for metrics.
//!
//! ## Known limitation
//!
//! `ProjectSession::reset()` does not yet exist (ADR 0007's "fixed reset"
//! mitigation). For v1 we trust that the engine's session is reusable as
//! long as the previous holder finished its work cleanly. A leaked
//! transaction or a stale prepared statement *would* leak across client
//! connections that share a `(project, client_key)` slot. Adding a
//! `ProjectSession::reset()` and calling it on `PooledSession::Drop` is a
//! follow-up tracked separately.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use basin_common::{ProjectId, Result};
use basin_engine::{Engine, ProjectSession};
use tokio::sync::{oneshot, Mutex};
use tracing::instrument;

mod eviction;
mod mode;
mod pooled_session;
mod return_scrub;
mod state;
mod stats;

pub use eviction::EvictionHandle;
pub use mode::PoolMode;
pub use pooled_session::PooledSession;
pub use stats::PoolStats;

use state::{PoolKey, PoolState};
use stats::AtomicStats;

/// Pool tuning knobs. See ADR 0007 for the rationale behind each default.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Hard ceiling on resident sessions across all projects. Once reached,
    /// `acquire` for a not-yet-resident `(project, client_key)` waits for
    /// an existing session to be released.
    pub max_sessions: usize,
    /// How long an idle session may sit in the cache before the eviction
    /// loop drops it.  Only relevant for [`PoolMode::Session`]: in
    /// transaction / statement mode sessions are never cached.
    pub idle_ttl: Duration,
    /// Maximum sessions per project (in-use plus idle). Prevents one
    /// project's burst from starving the rest of the pool.
    pub per_project_cap: usize,
    /// Cadence of the background eviction tick.
    pub eviction_interval: Duration,
    /// Connection-assignment granularity.
    ///
    /// * [`PoolMode::Session`] (default) — classic: sessions are cached and
    ///   reused across logical-client checkouts. This is the default because
    ///   typical Postgres connections are session-oriented; reuse avoids a cold
    ///   session-open on every checkout.
    /// * [`PoolMode::Transaction`] — sessions are destroyed on return so no
    ///   per-session state crosses checkout boundaries. Opt-in (e.g. for
    ///   serverless / PgBouncer-style fan-in) via `?pool_mode=transaction`.
    /// * [`PoolMode::Statement`] — placeholder; same as Transaction today.
    pub pool_mode: PoolMode,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            idle_ttl: Duration::from_secs(300),
            per_project_cap: 64,
            eviction_interval: Duration::from_secs(60),
            pool_mode: PoolMode::Session,
        }
    }
}

/// Shared pool state. `SessionPool` is a thin `Arc<Inner>` wrapper so
/// cloning it costs nothing and so `PooledSession` can hold a back-pointer
/// without lifetime gymnastics.
pub(crate) struct Inner {
    pub(crate) engine: Engine,
    pub(crate) cfg: PoolConfig,
    pub(crate) state: Mutex<PoolState>,
    pub(crate) stats: AtomicStats,
}

/// The pool. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct SessionPool {
    inner: Arc<Inner>,
}

impl SessionPool {
    pub fn new(engine: Engine, cfg: PoolConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine,
                cfg,
                state: Mutex::new(PoolState::new()),
                stats: AtomicStats::default(),
            }),
        }
    }

    /// Borrow a session keyed by `(project, client_key)`.
    ///
    /// Hot path: a session for the same key is sitting idle, we pop it
    /// (one mutex acquisition) and return. `hits` increments.
    ///
    /// Cold path: nothing matches. We check the per-project cap and the
    /// global ceiling under the same lock. If either is full, we register
    /// a oneshot waiter and `await` it; once a session for the same
    /// project is released we wake and retry. Otherwise we reserve the
    /// slot, drop the lock, and call `engine.open_session(project)` —
    /// that I/O happens *without* the pool lock held, so concurrent
    /// hits aren't blocked by one slow open.
    #[instrument(skip(self), fields(project = %project, client_key = ?client_key))]
    pub async fn acquire(
        &self,
        project: ProjectId,
        client_key: Option<String>,
    ) -> Result<PooledSession> {
        let key = PoolKey {
            project,
            client_key,
        };

        loop {
            // Step 1: try to satisfy from the cache, or detect that we
            // need to open a fresh session and reserve the slot.
            //
            // Transaction / statement mode: bypass the idle cache entirely.
            // Each checkout must start with a fresh session so that cursors,
            // prepared statements, and per-session SET state from the
            // previous logical client cannot leak into this one.
            let action = {
                let mut state = self.inner.state.lock().await;

                let session_mode = self.inner.cfg.pool_mode == PoolMode::Session;

                if session_mode {
                    if let Some(queue) = state.available.get_mut(&key) {
                        if let Some(entry) = queue.pop_back() {
                            if queue.is_empty() {
                                state.available.remove(&key);
                            }
                            Action::Hit(entry.session)
                        } else {
                            // An empty queue should have been removed by the
                            // last popper, but treat it defensively.
                            state.available.remove(&key);
                            decide_open(&mut state, &self.inner.cfg, key.project)
                        }
                    } else {
                        decide_open(&mut state, &self.inner.cfg, key.project)
                    }
                } else {
                    // Transaction / statement mode: always open a fresh
                    // session; never serve from the cache.
                    decide_open(&mut state, &self.inner.cfg, key.project)
                }
            };

            match action {
                Action::Hit(session) => {
                    self.inner.stats.record_hit();
                    // Session mode: scrub the session before handing it to the
                    // new logical client.  Applying the scrub at checkout (not
                    // at return) keeps `Drop` synchronous and preserves the
                    // try_lock fast path on the return side.  Best-effort: a
                    // failed scrub is logged but does not abort the checkout —
                    // the next client would see stale state in the unlikely
                    // event the scrub fails, which is no worse than the
                    // pre-5.27.E baseline.
                    if let Err(e) =
                        return_scrub::scrub_session_for_pool_return(&session).await
                    {
                        tracing::warn!(
                            error = %e,
                            "pool scrub failed on checkout; session may carry stale state"
                        );
                    }
                    return Ok(pooled_session::build(self.inner.clone(), key, session));
                }
                Action::Open => {
                    // Slot already reserved. Open the session outside the
                    // lock; release the slot and surface the error if
                    // open_session fails so we don't leak capacity.
                    match self.inner.engine.open_session(key.project).await {
                        Ok(session) => {
                            self.inner.stats.record_miss();
                            let arc = Arc::new(session);
                            return Ok(pooled_session::build(self.inner.clone(), key, arc));
                        }
                        Err(e) => {
                            let mut state = self.inner.state.lock().await;
                            state.release_slot(key.project);
                            return Err(e);
                        }
                    }
                }
                Action::Wait(rx) => {
                    // Cap was hit. Park until a session for this project
                    // gets released, then loop back and retry from
                    // step 1. A cancelled sender (RecvError) is also a
                    // wakeup — the next iteration will re-check capacity.
                    let _ = rx.await;
                    continue;
                }
            }
        }
    }

    /// Snapshot the live counters. `resident_per_project` is the maximum
    /// across projects — the value to alert on when sizing the per-project
    /// cap.
    pub fn stats(&self) -> PoolStats {
        let (hits, misses, evictions) = self.inner.stats.snapshot();
        // Use try_lock so a metrics scrape never blocks behind an open.
        // If contended, fall back to a partial snapshot with zero
        // resident counts; the next scrape will catch up.
        let (resident_sessions, resident_per_project) = match self.inner.state.try_lock() {
            Ok(s) => (s.total, s.max_per_project()),
            Err(_) => (0, 0),
        };
        PoolStats {
            resident_sessions,
            resident_per_project,
            hits,
            misses,
            evictions,
        }
    }

    /// Spawn the background idle-eviction task. Drop the returned handle
    /// or call [`EvictionHandle::shutdown`] to stop it.
    pub fn spawn_eviction(&self) -> EvictionHandle {
        eviction::spawn(self.inner.clone())
    }

    /// Test-only: synchronously run one eviction pass. Skips the timer.
    #[cfg(test)]
    pub(crate) async fn run_eviction_once(&self) {
        eviction::run_once(&self.inner).await;
    }
}

enum Action {
    /// Cache hit; the popped session is ready to hand out.
    Hit(Arc<ProjectSession>),
    /// We reserved a slot under the lock; caller must call
    /// `engine.open_session` and either consume the slot or release it
    /// on error.
    Open,
    /// The per-project or global cap is saturated. Park on `rx` until a
    /// session for the same project is released, then retry.
    Wait(oneshot::Receiver<()>),
}

/// Decide whether a not-found-in-cache `acquire` should open a fresh
/// session, or wait for capacity. Caller already holds the state lock.
///
/// If the per-project cap is hit but there's an idle session for this
/// project under a different `client_key`, evict the LRU one so the
/// caller can take its slot. That converts what would otherwise be a
/// deadlock (waiter parked on `project`, but the only released sessions
/// belong to different `client_key`s) into forward progress.
fn decide_open(state: &mut PoolState, cfg: &PoolConfig, project: ProjectId) -> Action {
    let per_project = state.per_project.get(&project).copied().unwrap_or(0);
    let total = state.total;

    if per_project < cfg.per_project_cap && total < cfg.max_sessions {
        state.reserve_slot(project);
        return Action::Open;
    }

    // Try to free a slot for this project by evicting one of its idle
    // sessions under a different key. Pick the entry with the oldest
    // `last_used` to approximate LRU.
    let victim_key = state
        .available
        .iter()
        .filter(|(k, q)| k.project == project && !q.is_empty())
        .min_by_key(|(_, q)| q.front().map(|e| e.last_used))
        .map(|(k, _)| k.clone());

    if per_project >= cfg.per_project_cap {
        if let Some(k) = victim_key.clone() {
            if let Some(q) = state.available.get_mut(&k) {
                q.pop_front();
                if q.is_empty() {
                    state.available.remove(&k);
                }
            }
            // The evicted session vacated one resident slot for this
            // project. Account for it the same way the eviction loop
            // would, so per_project / total stay accurate.
            if let Some(c) = state.per_project.get_mut(&project) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    state.per_project.remove(&project);
                }
            }
            state.total = state.total.saturating_sub(1);

            // Re-evaluate global cap with the slot freed.
            if state.total < cfg.max_sessions {
                state.reserve_slot(project);
                return Action::Open;
            }
        }
    } else if total >= cfg.max_sessions {
        // Per-project cap is fine but the global ceiling is hit. Same
        // idea: evict any project's LRU idle session to free a global
        // slot. Picking *any* project's victim is correct because the
        // global ceiling is project-agnostic.
        let victim_key_any = state
            .available
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .min_by_key(|(_, q)| q.front().map(|e| e.last_used))
            .map(|(k, _)| k.clone());
        if let Some(k) = victim_key_any {
            let victim_project = k.project;
            if let Some(q) = state.available.get_mut(&k) {
                q.pop_front();
                if q.is_empty() {
                    state.available.remove(&k);
                }
            }
            if let Some(c) = state.per_project.get_mut(&victim_project) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    state.per_project.remove(&victim_project);
                }
            }
            state.total = state.total.saturating_sub(1);
            // Recheck per-project cap (unchanged from above).
            let per_project_after = state.per_project.get(&project).copied().unwrap_or(0);
            if per_project_after < cfg.per_project_cap && state.total < cfg.max_sessions {
                state.reserve_slot(project);
                return Action::Open;
            }
        }
    }

    let (tx, rx) = oneshot::channel();
    state.waiters.entry(project).or_default().push_back(tx);
    Action::Wait(rx)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::InMemoryCatalog;
    use basin_common::ProjectId;
    use basin_engine::EngineConfig;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;

    fn engine_in(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn cfg_default() -> PoolConfig {
        PoolConfig {
            max_sessions: 32,
            idle_ttl: Duration::from_secs(300),
            per_project_cap: 16,
            eviction_interval: Duration::from_secs(60),
            // Unit tests verify session-reuse semantics (hit/miss counters,
            // eviction, address stability). Use Session mode so the existing
            // cache-hit assertions remain valid.
            pool_mode: PoolMode::Session,
        }
    }

    fn session_addr(s: &ProjectSession) -> usize {
        s as *const ProjectSession as usize
    }

    #[tokio::test]
    async fn hit_then_release_then_hit() {
        let dir = TempDir::new().unwrap();
        let pool = SessionPool::new(engine_in(&dir), cfg_default());
        let project = ProjectId::new();

        let first = pool.acquire(project, None).await.unwrap();
        let first_addr = session_addr(first.session());
        drop(first);

        let second = pool.acquire(project, None).await.unwrap();
        let second_addr = session_addr(second.session());

        assert_eq!(
            first_addr, second_addr,
            "second acquire must reuse the cached session"
        );
        let stats = pool.stats();
        assert_eq!(stats.hits, 1, "second acquire should record a hit");
        assert_eq!(stats.misses, 1, "first acquire is a cold miss");
    }

    #[tokio::test]
    async fn miss_when_no_match() {
        let dir = TempDir::new().unwrap();
        let pool = SessionPool::new(engine_in(&dir), cfg_default());
        let project = ProjectId::new();

        let s1 = pool.acquire(project, None).await.unwrap();
        let s1_addr = session_addr(s1.session());
        drop(s1);

        let s2 = pool
            .acquire(project, Some("alice".to_string()))
            .await
            .unwrap();
        let s2_addr = session_addr(s2.session());

        assert_ne!(
            s1_addr, s2_addr,
            "different client_key must not share sessions"
        );
        let stats = pool.stats();
        assert_eq!(stats.misses, 2, "two distinct keys = two misses");
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn per_project_cap_blocks() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            per_project_cap: 2,
            max_sessions: 32,
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg);
        let project = ProjectId::new();

        // Two distinct keys so neither acquire is a hit on the other.
        let s1 = pool
            .acquire(project, Some("alice".to_string()))
            .await
            .unwrap();
        let s2 = pool
            .acquire(project, Some("bob".to_string()))
            .await
            .unwrap();

        let pool_clone = pool.clone();
        let mut blocked = tokio::spawn(async move {
            pool_clone
                .acquire(project, Some("carol".to_string()))
                .await
                .unwrap()
        });

        // The third acquire should still be parked — the per-project cap
        // is 2 and we hold both slots.
        let still_pending = tokio::time::timeout(Duration::from_millis(50), &mut blocked).await;
        assert!(
            still_pending.is_err(),
            "third acquire should be blocked behind the per-project cap"
        );

        // Release one; the parked acquire should now make progress.
        drop(s1);
        let third = tokio::time::timeout(Duration::from_millis(500), blocked)
            .await
            .expect("third acquire should complete after a slot frees")
            .expect("spawned task should not panic");

        // Hold all referenced sessions until end of scope so capacity is
        // observable but not double-counted.
        drop(s2);
        drop(third);
    }

    #[tokio::test]
    async fn per_project_cap_does_not_starve_other_projects() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            per_project_cap: 1,
            max_sessions: 32,
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg);
        let a = ProjectId::new();
        let b = ProjectId::new();

        let _sa = pool.acquire(a, None).await.unwrap();
        // B is a different project: its per-project slot is fresh. This
        // must not block on A's full bucket.
        let sb = tokio::time::timeout(Duration::from_millis(500), pool.acquire(b, None))
            .await
            .expect("B should not be blocked by A's saturation");
        let _sb = sb.unwrap();
    }

    #[tokio::test]
    async fn eviction_drops_idle_session() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            idle_ttl: Duration::from_millis(0),
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg);
        let project = ProjectId::new();

        let s = pool.acquire(project, None).await.unwrap();
        drop(s);
        // Yield once so the post-Drop spawn (if any) lands before we
        // run eviction; with `try_lock` succeeding this is overkill,
        // but it makes the test robust to scheduling.
        tokio::task::yield_now().await;
        // Make sure last_used is strictly in the past.
        tokio::time::sleep(Duration::from_millis(2)).await;

        pool.run_eviction_once().await;

        let stats = pool.stats();
        assert_eq!(stats.evictions, 1, "one idle session should be evicted");
        assert_eq!(
            stats.resident_sessions, 0,
            "resident count should drop to zero after eviction"
        );
        assert_eq!(stats.resident_per_project, 0);
    }

    #[tokio::test]
    async fn stats_track_hits_misses_evictions() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            idle_ttl: Duration::from_millis(0),
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg);
        let project = ProjectId::new();

        // Cold open + warm reuse.
        let s1 = pool.acquire(project, None).await.unwrap();
        drop(s1);
        let s2 = pool.acquire(project, None).await.unwrap();
        drop(s2);
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        pool.run_eviction_once().await;

        let stats = pool.stats();
        assert_eq!(stats.misses, 1, "exactly one miss");
        assert_eq!(stats.hits, 1, "exactly one hit");
        assert_eq!(stats.evictions, 1, "exactly one eviction");
    }

    #[tokio::test]
    async fn drop_returns_to_pool_under_contention() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            max_sessions: 16,
            per_project_cap: 16,
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg.clone());
        let project = ProjectId::new();

        let mut joins = Vec::new();
        for _ in 0..50 {
            let p = pool.clone();
            joins.push(tokio::spawn(async move {
                let s = p.acquire(project, None).await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(s);
            }));
        }
        for j in joins {
            j.await.unwrap();
        }

        // After every task finishes, the pool's resident count must not
        // exceed max_sessions. This catches the case where `Drop`'s
        // `try_lock` falls back to `tokio::spawn` and fails to enforce
        // the cap.
        let stats = pool.stats();
        assert!(
            stats.resident_sessions <= cfg.max_sessions,
            "resident_sessions = {} exceeds max_sessions = {}",
            stats.resident_sessions,
            cfg.max_sessions,
        );
        assert!(
            stats.resident_per_project <= cfg.per_project_cap,
            "resident_per_project = {} exceeds per_project_cap = {}",
            stats.resident_per_project,
            cfg.per_project_cap,
        );
    }

    #[tokio::test]
    async fn eviction_handle_shutdown_stops_loop() {
        let dir = TempDir::new().unwrap();
        let cfg = PoolConfig {
            eviction_interval: Duration::from_millis(10),
            ..cfg_default()
        };
        let pool = SessionPool::new(engine_in(&dir), cfg);
        let handle = pool.spawn_eviction();
        // Just verify shutdown completes; if the loop ignored shutdown
        // this test would hang.
        tokio::time::timeout(Duration::from_secs(2), handle.shutdown())
            .await
            .expect("eviction loop should respond to shutdown");
    }
}
