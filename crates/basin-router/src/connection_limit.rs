//! Per-project live-connection counting with an optional ceiling.
//!
//! ## Design
//!
//! A single [`ConnectionLimiter`] is shared across all connections (via
//! `Arc`). On every accepted pgwire connection the startup handler:
//!
//! 1. Resolves the project's current `max_connections` limit from the
//!    [`ConnectionLimitProvider`] (fresh call per connect — not lifetime-cached).
//! 2. Reads the project's live count from an atomic per-project counter.
//! 3. If `live_count >= limit`, sends a pgwire `ErrorResponse` with SQLSTATE
//!    `53300` (`too_many_connections`) and closes the connection.
//! 4. If allowed, increments the counter and returns a [`ConnectionGuard`]
//!    whose `Drop` decrements it — so panics and clean disconnects both
//!    decrement correctly.
//!
//! `None` limit (the default for self-hosted / no provider wired) means
//! **unlimited** — the correct behaviour for OSS deployments that run no
//! control plane.
//!
//! ## Multi-region accounting (ADR 0009 v0.2)
//!
//! The authoritative live count for a project lives in its **home region's**
//! limiter — the only region that accepts a write for it. A cross-region
//! forwarded write (see `basin_engine::write_forwarder`) is therefore counted
//! HERE, at home, exactly once, never bypassing `max_connections`:
//! `fly-replay` makes Fly re-fire the request at the home region (counted by
//! this limiter on the re-fired connection), and `http-forward` makes the
//! forward client connect to the home engine's admission path (counted by this
//! limiter on the forwarded connection). The originating region never counts a
//! forwarded write against the project. See
//! `basin_engine::write_forwarder::CeilingAccounting`.
//!
//! ## Control-plane wiring gap
//!
//! The [`ConnectionLimitProvider`] trait is the injection point. Today's
//! OSS tree has no control-plane channel that delivers per-project metadata,
//! so the only built-in provider is [`NoConnectionLimit`] (always unlimited).
//!
//! To wire cloud-derived limits, the cloud layer must implement
//! `ConnectionLimitProvider` against its project-metadata source and pass it
//! into [`crate::ServerConfig::connection_limiter`]. The exact gap:
//!
//! - The `ProjectResolver::resolve_credentials` method returns a `ProjectId`
//!   only. It would need to be extended (or a parallel `resolve_limit`
//!   method added) to also return `max_connections: Option<u32>`, OR
//! - A separate per-project metadata lookup (HTTP, gRPC, or in-process
//!   cache) must be wired that maps `ProjectId → Option<u32>`. The
//!   provider's `limit_for` is called on every connect, so a short-TTL
//!   (e.g. 30 s) in-memory cache is recommended; the call site does NOT
//!   cache — that responsibility belongs to the provider implementation
//!   so a plan upgrade propagates within the TTL.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use basin_catalog::Catalog;
use basin_common::ProjectId;

// ─── provider trait ──────────────────────────────────────────────────────────

/// Resolves the current `max_connections` ceiling for a project.
///
/// Implementations are called on **every new connection**, so they are
/// expected to be fast. A short-TTL in-memory cache is the recommended
/// implementation pattern; the limiter itself does not cache.
///
/// Returning `None` means unlimited — the correct default for self-hosted
/// deployments with no control plane.
#[async_trait]
pub trait ConnectionLimitProvider: Send + Sync {
    /// Return the maximum number of simultaneously open connections allowed
    /// for `project`, or `None` for no limit.
    async fn limit_for(&self, project: ProjectId) -> Option<u32>;
}

/// No-op provider — every project is unlimited. Used when no control-plane
/// channel is wired.
#[derive(Clone, Debug)]
pub struct NoConnectionLimit;

#[async_trait]
impl ConnectionLimitProvider for NoConnectionLimit {
    async fn limit_for(&self, _project: ProjectId) -> Option<u32> {
        None
    }
}

// ─── live-count tracker ──────────────────────────────────────────────────────

/// Per-project live-connection counter map, shared across all connections.
///
/// Cheap to clone (the interior is `Arc`-wrapped).
///
/// ## Locking strategy
///
/// The underlying `HashMap` is protected by an `RwLock` instead of a `Mutex`
/// so that the common path — a connection for a project that already has a
/// live-count entry — only takes a **shared read lock**. The exclusive write
/// lock is acquired only when a new project's counter must be lazily inserted,
/// which happens at most once per project per server lifetime. Under concurrent
/// connection load this removes the global exclusive-lock bottleneck from
/// every accept.
#[derive(Clone, Debug, Default)]
pub struct LiveCounts {
    counts: Arc<RwLock<HashMap<ProjectId, Arc<AtomicU32>>>>,
}

impl LiveCounts {
    /// Create an empty counter map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return (or lazily create) the `AtomicU32` for `project`.
    ///
    /// Fast path (project already present): acquires a shared read lock, clones
    /// the `Arc`, and returns immediately. Slow path (first connection for this
    /// project ever): drops the read lock, acquires the exclusive write lock,
    /// and inserts the new counter. The double-check after acquiring the write
    /// lock handles the race where two accepts both miss the read lock and then
    /// both try to insert.
    fn counter_for(&self, project: ProjectId) -> Arc<AtomicU32> {
        // Fast path: shared read lock — no exclusive contention when the
        // counter already exists (the common case for an established project).
        {
            let map = self.counts.read().expect("LiveCounts lock poisoned");
            if let Some(counter) = map.get(&project) {
                return counter.clone();
            }
        }
        // Slow path: first connection for this project — take the write lock
        // and insert. Double-check in case another thread beat us.
        let mut map = self.counts.write().expect("LiveCounts lock poisoned");
        map.entry(project)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone()
    }

    /// Return the current live-connection count for `project`.
    pub fn get(&self, project: ProjectId) -> u32 {
        self.counts
            .read()
            .expect("LiveCounts lock poisoned")
            .get(&project)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Attempt to increment the counter for `project`, subject to `limit`.
    ///
    /// Returns `Ok(ConnectionGuard)` when the connection is admitted.
    /// Returns `Err(u32)` — the current count — when `limit` is `Some(n)`
    /// and `count >= n`.
    pub fn try_increment(
        &self,
        project: ProjectId,
        limit: Option<u32>,
    ) -> Result<ConnectionGuard, u32> {
        let counter = self.counter_for(project);

        // Optimistic CAS loop so the check and increment are atomic: we
        // never overshoot the limit even under concurrent accepts.
        loop {
            let current = counter.load(Ordering::SeqCst);
            if let Some(max) = limit {
                if current >= max {
                    return Err(current);
                }
            }
            // Attempt to increment `current` → `current + 1`. If another
            // thread beat us, compare_exchange returns Err and we retry.
            match counter.compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(ConnectionGuard { counter, project }),
                Err(_) => continue,
            }
        }
    }
}

// ─── RAII guard ──────────────────────────────────────────────────────────────

/// Decrements the per-project live-connection counter on drop.
///
/// Returned by [`LiveCounts::try_increment`] on success. Must be kept alive
/// for the full lifetime of the connection; dropping it (or letting it be
/// cancelled) decrements the count.
pub struct ConnectionGuard {
    counter: Arc<AtomicU32>,
    /// Stored for tracing; not used by `Drop`.
    project: ProjectId,
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard")
            .field("project", &self.project)
            .field("live", &self.counter.load(Ordering::Relaxed))
            .finish()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Saturating so a double-drop (logic error) doesn't wrap to u32::MAX.
        let _ = self
            .counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                Some(v.saturating_sub(1))
            });
        tracing::trace!(
            project = %self.project,
            "pgwire connection closed; live count decremented",
        );
    }
}

// ─── combined limiter ────────────────────────────────────────────────────────

/// Combined per-project connection limiter: live-count tracking + ceiling
/// enforcement via a pluggable [`ConnectionLimitProvider`].
///
/// Construct once at startup, share via `Arc` across all connections.
pub struct ConnectionLimiter {
    counts: LiveCounts,
    provider: Arc<dyn ConnectionLimitProvider>,
}

impl ConnectionLimiter {
    /// Create a limiter with `provider` as the limit source.
    pub fn new(provider: Arc<dyn ConnectionLimitProvider>) -> Self {
        Self {
            counts: LiveCounts::new(),
            provider,
        }
    }

    /// Create a limiter with no ceiling (unlimited for all projects). Used
    /// when no control-plane provider is wired — the correct default for
    /// self-hosted OSS deployments.
    pub fn unlimited() -> Self {
        Self::new(Arc::new(NoConnectionLimit))
    }

    /// Resolve the current limit for `project`, then attempt to admit the
    /// connection.
    ///
    /// Returns `Ok(ConnectionGuard)` when admitted.
    /// Returns `Err(u32)` (current live count) when the limit is exceeded.
    pub async fn try_admit(&self, project: ProjectId) -> Result<ConnectionGuard, u32> {
        let limit = self.provider.limit_for(project).await;
        self.counts.try_increment(project, limit)
    }

    /// Current live count for a project. Primarily for tests and health
    /// endpoints.
    pub fn live_count(&self, project: ProjectId) -> u32 {
        self.counts.get(project)
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Provider that returns a fixed limit for all projects.
    struct FixedLimit(u32);

    #[async_trait]
    impl ConnectionLimitProvider for FixedLimit {
        async fn limit_for(&self, _project: ProjectId) -> Option<u32> {
            Some(self.0)
        }
    }

    fn limiter_with_cap(n: u32) -> ConnectionLimiter {
        ConnectionLimiter::new(Arc::new(FixedLimit(n)))
    }

    #[tokio::test]
    async fn admits_up_to_limit() {
        let lim = limiter_with_cap(3);
        let p = ProjectId::new();
        let g1 = lim.try_admit(p).await.expect("admit 1");
        let g2 = lim.try_admit(p).await.expect("admit 2");
        let g3 = lim.try_admit(p).await.expect("admit 3");
        assert_eq!(lim.live_count(p), 3);
        drop(g1);
        drop(g2);
        drop(g3);
    }

    #[tokio::test]
    async fn rejects_at_limit() {
        let lim = limiter_with_cap(2);
        let p = ProjectId::new();
        let _g1 = lim.try_admit(p).await.expect("admit 1");
        let _g2 = lim.try_admit(p).await.expect("admit 2");
        let err = lim.try_admit(p).await.expect_err("should reject at 2");
        assert_eq!(err, 2);
    }

    #[tokio::test]
    async fn decrement_on_drop_allows_new_connection() {
        let lim = limiter_with_cap(1);
        let p = ProjectId::new();
        {
            let _g = lim.try_admit(p).await.expect("admit");
            assert_eq!(lim.live_count(p), 1);
        } // guard dropped here
        assert_eq!(lim.live_count(p), 0);
        lim.try_admit(p).await.expect("admit after drop");
    }

    #[tokio::test]
    async fn unlimited_never_rejects() {
        let lim = ConnectionLimiter::unlimited();
        let p = ProjectId::new();
        let mut guards = Vec::new();
        for _ in 0..50 {
            guards.push(lim.try_admit(p).await.expect("unlimited should admit"));
        }
        assert_eq!(lim.live_count(p), 50);
        drop(guards);
        assert_eq!(lim.live_count(p), 0);
    }

    #[tokio::test]
    async fn separate_projects_have_independent_counts() {
        let lim = limiter_with_cap(1);
        let a = ProjectId::new();
        let b = ProjectId::new();
        let _ga = lim.try_admit(a).await.expect("admit a");
        // a is at limit but b is a fresh counter
        let _gb = lim.try_admit(b).await.expect("admit b");
        assert_eq!(lim.live_count(a), 1);
        assert_eq!(lim.live_count(b), 1);
    }
}

// ─── CatalogConnectionLimitProvider ─────────────────────────────────────────

/// The fail-closed default when no ceiling has been pushed by the cloud.
///
/// `25` = the Free tier ceiling per the cloud's pricing ladder.
pub const DEFAULT_PROJECT_MAX_CONNECTIONS: u32 = 25;

/// [`ConnectionLimitProvider`] that reads per-project ceilings from the
/// [`basin_catalog::Catalog`]. Fail-closed: `None` from catalog → 25.
pub struct CatalogConnectionLimitProvider {
    catalog: Arc<dyn Catalog>,
}

impl CatalogConnectionLimitProvider {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ConnectionLimitProvider for CatalogConnectionLimitProvider {
    async fn limit_for(&self, project: ProjectId) -> Option<u32> {
        match self.catalog.get_project_max_connections(&project).await {
            Ok(Some(n)) => Some(n),
            Ok(None) => Some(DEFAULT_PROJECT_MAX_CONNECTIONS),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    %project,
                    "get_project_max_connections catalog error; \
                     applying fail-closed default ({})",
                    DEFAULT_PROJECT_MAX_CONNECTIONS,
                );
                Some(DEFAULT_PROJECT_MAX_CONNECTIONS)
            }
        }
    }
}

#[cfg(test)]
mod catalog_limit_provider_tests {
    use super::*;
    use basin_catalog::InMemoryCatalog;

    #[tokio::test]
    async fn unset_returns_fail_closed_default() {
        let p = CatalogConnectionLimitProvider::new(Arc::new(InMemoryCatalog::new()));
        let project = ProjectId::new();
        assert_eq!(
            p.limit_for(project).await,
            Some(DEFAULT_PROJECT_MAX_CONNECTIONS)
        );
    }

    #[tokio::test]
    async fn set_ceiling_is_returned() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = CatalogConnectionLimitProvider::new(cat.clone());
        let project = ProjectId::new();
        cat.set_project_max_connections(&project, 250).await.unwrap();
        assert_eq!(p.limit_for(project).await, Some(250));
    }

    #[tokio::test]
    async fn concurrent_admission_race() {
        const K: u32 = 10;
        const N: u32 = 25;
        let cat = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        cat.set_project_max_connections(&project, K).await.unwrap();
        let limiter = Arc::new(ConnectionLimiter::new(Arc::new(
            CatalogConnectionLimitProvider::new(cat.clone()),
        )));
        // Each task RETURNS its guard (Ok) or None (refused) so the admitted
        // guards stay alive — otherwise a guard dropped at the end of the task
        // frees the slot before we count, and every attempt "succeeds"
        // sequentially without ever testing the ceiling.
        let mut handles = Vec::new();
        for _ in 0..N {
            let lim = limiter.clone();
            handles.push(tokio::spawn(async move {
                lim.try_admit(project).await.ok()
            }));
        }
        let guards: Vec<Option<ConnectionGuard>> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("task panicked"))
            .collect();
        let admitted = guards.iter().filter(|g| g.is_some()).count() as u32;
        let refused = guards.iter().filter(|g| g.is_none()).count() as u32;
        assert_eq!(admitted, K, "exactly K connections admitted under the ceiling");
        assert_eq!(refused, N - K, "the rest refused");
        assert_eq!(limiter.live_count(project), K, "live count == admitted guards");
        // Guards drop here, freeing all slots.
        drop(guards);
        assert_eq!(limiter.live_count(project), 0);
    }

    #[tokio::test]
    async fn drop_decrement_frees_slot() {
        let cat = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        cat.set_project_max_connections(&project, 1).await.unwrap();
        let limiter = Arc::new(ConnectionLimiter::new(Arc::new(
            CatalogConnectionLimitProvider::new(cat.clone()),
        )));
        let guard = limiter.try_admit(project).await.expect("admit");
        assert!(limiter.try_admit(project).await.is_err());
        drop(guard);
        assert_eq!(limiter.live_count(project), 0);
        limiter.try_admit(project).await.expect("re-admit");
    }

    #[tokio::test]
    async fn ceiling_lower_does_not_kill_existing_connections() {
        let cat = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        cat.set_project_max_connections(&project, 5).await.unwrap();
        let limiter = Arc::new(ConnectionLimiter::new(Arc::new(
            CatalogConnectionLimitProvider::new(cat.clone()),
        )));
        let g1 = limiter.try_admit(project).await.expect("1");
        let g2 = limiter.try_admit(project).await.expect("2");
        let g3 = limiter.try_admit(project).await.expect("3");
        assert_eq!(limiter.live_count(project), 3);
        // Lower ceiling to 2 — existing 3 guards survive (CEILING, not kill).
        cat.set_project_max_connections(&project, 2).await.unwrap();
        assert_eq!(limiter.live_count(project), 3, "no kills on ceiling lower");
        // New attempt refused (live 3 >= 2).
        assert!(limiter.try_admit(project).await.is_err());
        // Drop one: at 2 live, still refused.
        drop(g3);
        assert!(limiter.try_admit(project).await.is_err());
        // Drop another: at 1 live, a new admit succeeds (1 < 2).
        drop(g2);
        let g4 = limiter.try_admit(project).await.expect("re-admit at 1<2");
        drop(g1);
        drop(g4);
    }
}
