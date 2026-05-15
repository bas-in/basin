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
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
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
#[derive(Clone, Debug, Default)]
pub struct LiveCounts {
    // Mutex-guarded HashMap to lazily create per-project atomics. The lock
    // is only held during the map lookup / insertion, not while I/O is in
    // progress, so contention is minimal.
    counts: Arc<Mutex<HashMap<ProjectId, Arc<AtomicU32>>>>,
}

impl LiveCounts {
    /// Create an empty counter map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return (or lazily create) the `AtomicU32` for `project`.
    fn counter_for(&self, project: ProjectId) -> Arc<AtomicU32> {
        let mut map = self.counts.lock().expect("LiveCounts lock poisoned");
        map.entry(project)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone()
    }

    /// Return the current live-connection count for `project`.
    pub fn get(&self, project: ProjectId) -> u32 {
        self.counts
            .lock()
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
            match counter.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
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
        let _ = self.counter.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |v| Some(v.saturating_sub(1)),
        );
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
