//! Per-tenant realtime memory budget enforcement (Phase 5.11.R6).
//!
//! # Design
//!
//! Per the `feedback_multitenant_isolation` rule: **per-tenant cost is
//! O(bytes-in-flight), not O(active_subscribers)**. The implementation uses:
//!
//! - A single shared `tokio::sync::broadcast` cluster (the [`ChannelRegistry`]
//!   in `lib.rs`) for all tenants — no per-tenant channel.
//! - A [`DashMap<ProjectId, ProjectBudget>`] lazily allocating one
//!   [`ProjectBudget`] per project *that has actually published an event*.
//!   Idle projects pay zero.
//! - Each [`ProjectBudget`] holds a single `AtomicU64` (`bytes_in_flight`) and
//!   a `hard_cap: u64`. No semaphore pool; no per-subscriber allocation.
//!
//! # Hot path
//!
//! `BudgetTracker::try_reserve(project, size)` does:
//!
//! 1. Lazy-insert a [`ProjectBudget`] into the `DashMap` if absent
//!    (one-shot allocation per new project, amortised O(1)).
//! 2. CAS loop on `bytes_in_flight`: add `size` if the result stays ≤
//!    `hard_cap`; otherwise return `Err(BudgetError::BufferFull)`.
//!
//! `BudgetTracker::release(project, size)` subtracts `size` from
//! `bytes_in_flight` after the event has been forwarded to the broadcast
//! channel. This keeps the counter accurate: it tracks *in-flight* bytes
//! (broadcast ring + subscriber lag), not cumulative throughput.
//!
//! # BUFFER_FULL semantics
//!
//! When `try_reserve` returns `Err(BudgetError::BufferFull)`:
//! - The caller (`RealtimeSink::publish`) drops the event into the durable
//!   webhook retry log so no data is lost.
//! - The fast-path broadcast is skipped — the noisy tenant does not starve
//!   other tenants' ring-buffer slots.
//! - Other tenants' `bytes_in_flight` counters are unaffected.
//!
//! # Configuration
//!
//! Default hard cap: `BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES` env var, or
//! [`DEFAULT_PER_PROJECT_BUDGET_BYTES`] (16 MiB) if the variable is absent /
//! invalid.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use basin_common::ProjectId;
use dashmap::DashMap;

/// Default per-project in-flight byte cap: 16 MiB.
pub const DEFAULT_PER_PROJECT_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// Env var that overrides [`DEFAULT_PER_PROJECT_BUDGET_BYTES`].
pub const ENV_PER_PROJECT_BUDGET: &str = "BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES";

/// Error returned by [`BudgetTracker::try_reserve`] when the per-project
/// byte cap is exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetError {
    /// The tenant's in-flight byte budget is full. The event should be
    /// dropped to the durable retry log; the fast-path broadcast is skipped.
    BufferFull,
}

impl std::fmt::Display for BudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BudgetError::BufferFull => f.write_str("BUFFER_FULL: per-project realtime budget exhausted"),
        }
    }
}

impl std::error::Error for BudgetError {}

/// Per-project budget state. One allocation per active project, evicted never
/// (idle projects pay only the `DashMap` entry overhead — ~128 bytes).
#[derive(Debug)]
struct ProjectBudget {
    /// Current in-flight bytes for this project. Atomically updated on
    /// reserve (before broadcast) and release (after broadcast).
    bytes_in_flight: AtomicU64,
    /// Hard cap in bytes. Configurable at tracker construction time;
    /// uniform across all projects in the same tracker instance.
    hard_cap: u64,
}

impl ProjectBudget {
    fn new(hard_cap: u64) -> Self {
        Self {
            bytes_in_flight: AtomicU64::new(0),
            hard_cap,
        }
    }

    /// Try to reserve `size` bytes. Returns `Ok(new_total)` on success,
    /// `Err(BudgetError::BufferFull)` if the cap would be exceeded.
    fn try_reserve(&self, size: u64) -> Result<u64, BudgetError> {
        // CAS loop: add `size` only if the post-addition value stays ≤ hard_cap.
        let mut current = self.bytes_in_flight.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(size);
            if next > self.hard_cap {
                return Err(BudgetError::BufferFull);
            }
            match self.bytes_in_flight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(next),
                Err(actual) => current = actual,
            }
        }
    }

    /// Release `size` bytes. Called after the event has been handed to the
    /// broadcast channel (or immediately on the BUFFER_FULL path, which never
    /// reserved anything).
    fn release(&self, size: u64) {
        // Saturating sub prevents underflow if a bug causes double-release.
        self.bytes_in_flight.fetch_update(
            Ordering::AcqRel,
            Ordering::Relaxed,
            |v| Some(v.saturating_sub(size)),
        ).ok();
    }

    /// Current in-flight bytes (snapshot; may be stale by the time caller
    /// reads it under concurrent traffic).
    fn bytes_in_flight(&self) -> u64 {
        self.bytes_in_flight.load(Ordering::Relaxed)
    }
}

/// Shared multi-tenant budget tracker. Cheap to clone — all state is behind
/// an `Arc`-backed [`DashMap`].
///
/// # Usage
///
/// ```rust,ignore
/// let tracker = BudgetTracker::from_env();
/// let size = estimate_event_size(&event);
///
/// match tracker.try_reserve(event.project, size) {
///     Ok(guard) => {
///         registry.publish(&key, Arc::new(event.clone()));
///         drop(guard); // releases the bytes
///     }
///     Err(BudgetError::BufferFull) => {
///         retry_log.enqueue(...).await?;
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct BudgetTracker {
    inner: Arc<BudgetTrackerInner>,
}

#[derive(Debug)]
struct BudgetTrackerInner {
    budgets: DashMap<ProjectId, ProjectBudget>,
    hard_cap: u64,
}

impl BudgetTracker {
    /// Build with an explicit per-project `hard_cap` in bytes.
    pub fn new(hard_cap: u64) -> Self {
        Self {
            inner: Arc::new(BudgetTrackerInner {
                budgets: DashMap::new(),
                hard_cap,
            }),
        }
    }

    /// Build reading `BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES` from the
    /// environment. Falls back to [`DEFAULT_PER_PROJECT_BUDGET_BYTES`].
    pub fn from_env() -> Self {
        let cap = std::env::var(ENV_PER_PROJECT_BUDGET)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_PER_PROJECT_BUDGET_BYTES);
        Self::new(cap)
    }

    /// Try to reserve `size` bytes for `project`.
    ///
    /// Returns a [`BudgetGuard`] that releases the bytes when dropped.
    /// Returns `Err(BudgetError::BufferFull)` if the per-project cap is
    /// exceeded; the caller should drop the event to the retry log.
    pub fn try_reserve(
        &self,
        project: ProjectId,
        size: u64,
    ) -> Result<BudgetGuard, BudgetError> {
        let budget = self
            .inner
            .budgets
            .entry(project)
            .or_insert_with(|| ProjectBudget::new(self.inner.hard_cap));
        budget.try_reserve(size)?;
        // Keep a reference to the DashMap entry's value through the tracker Arc.
        // We drop the dashmap guard here and re-look up on release, which is
        // fine: the entry is never removed (projects are never evicted).
        drop(budget);
        Ok(BudgetGuard {
            tracker: self.clone(),
            project,
            size,
        })
    }

    /// Release `size` bytes for `project`. Called internally by [`BudgetGuard`].
    fn release(&self, project: ProjectId, size: u64) {
        if let Some(budget) = self.inner.budgets.get(&project) {
            budget.release(size);
        }
    }

    /// Current in-flight bytes for `project`. Returns 0 if the project has
    /// never published an event.
    pub fn bytes_in_flight(&self, project: ProjectId) -> u64 {
        self.inner
            .budgets
            .get(&project)
            .map(|b| b.bytes_in_flight())
            .unwrap_or(0)
    }

    /// Per-project hard cap this tracker was built with.
    pub fn hard_cap(&self) -> u64 {
        self.inner.hard_cap
    }

    /// Number of projects that have ever published at least one event (i.e.
    /// have a budget entry). Bounded by the number of active tenants.
    pub fn tracked_project_count(&self) -> usize {
        self.inner.budgets.len()
    }
}

impl Default for BudgetTracker {
    fn default() -> Self {
        Self::from_env()
    }
}

/// RAII guard that releases the reserved bytes back to the budget on drop.
///
/// Returned by [`BudgetTracker::try_reserve`]. Drop it after the event has
/// been handed to the broadcast channel (or at any point you want to release
/// the budget). If you need to release early (e.g. on an error path), just
/// drop the guard.
#[derive(Debug)]
pub struct BudgetGuard {
    tracker: BudgetTracker,
    project: ProjectId,
    size: u64,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        self.tracker.release(self.project, self.size);
    }
}

// ---------------------------------------------------------------------------
// Event size estimation
// ---------------------------------------------------------------------------

/// Estimate the serialised byte size of a [`basin_common::ChangeEvent`].
///
/// This is a *conservative upper bound*, not an exact measurement. The
/// broadcast ring stores `Arc<ChangeEvent>` (not the serialised form), so
/// we estimate: fixed overhead (128 bytes for the struct itself + Arc header)
/// plus the JSON payload sizes (`before` + `after`).
///
/// The estimate is intentionally generous: a too-small estimate would let
/// noisy tenants exceed their real cap; a too-large estimate wastes a few KiB
/// of headroom, which is acceptable.
pub fn estimate_event_size(event: &basin_common::ChangeEvent) -> u64 {
    const FIXED_OVERHEAD: u64 = 128;
    let before_size = event
        .before
        .as_ref()
        .map(|v| v.to_string().len() as u64)
        .unwrap_or(0);
    let after_size = event
        .after
        .as_ref()
        .map(|v| v.to_string().len() as u64)
        .unwrap_or(0);
    FIXED_OVERHEAD + before_size + after_size
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    /// Hard-cap rejection: reserving more than the cap returns BUFFER_FULL.
    #[test]
    fn hard_cap_rejection() {
        let tracker = BudgetTracker::new(1_000);
        let project = ProjectId::new();

        // First reservation fits (500 ≤ 1000).
        let guard1 = tracker.try_reserve(project, 500).expect("first reserve must succeed");
        assert_eq!(tracker.bytes_in_flight(project), 500);

        // Second reservation (500 more = 1000 ≤ 1000) fits exactly.
        let guard2 = tracker.try_reserve(project, 500).expect("second reserve must succeed");
        assert_eq!(tracker.bytes_in_flight(project), 1_000);

        // Third reservation would exceed the cap → BUFFER_FULL.
        let result = tracker.try_reserve(project, 1);
        assert_eq!(result.unwrap_err(), BudgetError::BufferFull);

        // Release guards; counter returns to zero.
        drop(guard1);
        drop(guard2);
        assert_eq!(tracker.bytes_in_flight(project), 0);
    }

    /// Guard drop releases bytes — subsequent reservation succeeds.
    #[test]
    fn guard_drop_releases_bytes() {
        let tracker = BudgetTracker::new(100);
        let project = ProjectId::new();

        {
            let _guard = tracker.try_reserve(project, 100).expect("must fit");
            assert_eq!(tracker.bytes_in_flight(project), 100);
            // Guard drops here.
        }
        assert_eq!(tracker.bytes_in_flight(project), 0);

        // After release the full cap is available again.
        let _guard2 = tracker.try_reserve(project, 100).expect("must fit after release");
    }

    /// BUFFER_FULL for one project does not affect another project.
    #[test]
    fn isolation_between_projects() {
        let tracker = BudgetTracker::new(500);
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        // Fill p1.
        let _g1 = tracker.try_reserve(p1, 500).expect("p1 first must fit");
        assert_eq!(tracker.try_reserve(p1, 1).unwrap_err(), BudgetError::BufferFull);

        // p2 is completely unaffected.
        let _g2 = tracker.try_reserve(p2, 500).expect("p2 must not be affected by p1 overflow");
        assert_eq!(tracker.bytes_in_flight(p2), 500);
    }

    /// BUFFER_FULL display string contains the expected signal.
    #[test]
    fn buffer_full_display() {
        let msg = BudgetError::BufferFull.to_string();
        assert!(msg.contains("BUFFER_FULL"), "display must contain BUFFER_FULL");
    }

    /// Lazy allocation: a project that never publishes has no entry.
    #[test]
    fn lazy_allocation_no_entry_for_idle_project() {
        let tracker = BudgetTracker::new(1_000);
        let idle = ProjectId::new();
        // No entry yet.
        assert_eq!(tracker.bytes_in_flight(idle), 0);
        assert_eq!(tracker.tracked_project_count(), 0);
    }

    /// estimate_event_size returns at least the fixed overhead.
    #[test]
    fn estimate_event_size_at_least_overhead() {
        use basin_common::{ChangeEvent, ChangeOp, TableName};
        use chrono::Utc;

        let event = ChangeEvent {
            project: ProjectId::new(),
            table: TableName::new("t").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: None,
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        };
        assert!(estimate_event_size(&event) >= 128);
    }

    /// estimate_event_size grows with payload size.
    #[test]
    fn estimate_event_size_grows_with_payload() {
        use basin_common::{ChangeEvent, ChangeOp, TableName};
        use chrono::Utc;

        let small = ChangeEvent {
            project: ProjectId::new(),
            table: TableName::new("t").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"x": 1})),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        };
        let large = ChangeEvent {
            after: Some(serde_json::json!({"data": "a".repeat(10_000)})),
            ..small.clone()
        };
        assert!(estimate_event_size(&large) > estimate_event_size(&small));
    }

    /// Concurrent reservations from multiple threads — no data races, total
    /// in-flight never exceeds the cap.
    #[test]
    fn concurrent_reserve_never_exceeds_cap() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc as StdArc;

        let cap = 10_000u64;
        let tracker = BudgetTracker::new(cap);
        let project = ProjectId::new();
        let overflow_count = StdArc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let t = tracker.clone();
                let oc = overflow_count.clone();
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        match t.try_reserve(project, 100) {
                            Ok(guard) => {
                                // Assert in-flight never exceeds cap.
                                let inflight = t.bytes_in_flight(project);
                                assert!(
                                    inflight <= cap,
                                    "in-flight {inflight} exceeded cap {cap}"
                                );
                                drop(guard);
                            }
                            Err(BudgetError::BufferFull) => {
                                oc.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().expect("thread must not panic");
        }

        // After all threads complete the counter returns to 0.
        assert_eq!(tracker.bytes_in_flight(project), 0);
    }
}
