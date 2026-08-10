//! Phase 6.X.D — heartbeat-reconciled budgets (ADR 0023).
//!
//! Closes the **multi-instance cap-bypass P0** from
//! `docs/audits/2026-05-21-noisy-neighbor-fairness.md`: every per-project cap
//! used to live in a per-process `DashMap`, so a project capped at e.g. 100/s
//! sustained `N × 100/s` simply by spraying traffic across `N` replicas. This
//! module is the coordinator side: every leaseholder pushes per-`(project,
//! partition)` usage deltas on its heartbeat tick; the coordinator sums them
//! into per-project totals and writes back a per-partition slice budget
//! (`project_budget / partition_count`) into the heartbeat response. Each
//! local cap consumer (REST QPS, pgwire QPS, memtable bytes, realtime
//! BUFFER_FULL, Wasm semaphore, basin-net outbound) consults its slice view
//! before admitting work. The hot path stays purely local — only the
//! heartbeat round-trip touches the coordinator.
//!
//! ## Why "slice budget" not "distributed counter"
//!
//! The audit framed the fix as central-coordinator vs Raft/gossip. ADR 0023
//! takes a third path: each leaseholder is the *single arbiter* for its
//! partition's caps, so the per-request decision is pure local arithmetic
//! (no RTT). The coordinator only does the asynchronous reconciliation of
//! per-project totals. Over-cap window is bounded by one heartbeat interval
//! (default 5 s) × scheduler slack — acceptable for billing and soft caps;
//! hard caps set the slice conservatively.
//!
//! ## Shape rule
//!
//! Same `feedback_multitenant_isolation` rule as everywhere else: per-project
//! cost is `O(bytes)`. Idle projects cost only a hash-map entry; the
//! coordinator holds one row per `(project, partition)` it has ever heard
//! about (no per-replica entry).
//!
//! ## Module layout
//!
//! - [`CapKind`] — the six cap consumer sites.
//! - [`UsageDelta`] / [`UsageSnapshot`] — per-`(project, partition)` numbers.
//! - [`ProjectBudget`] / [`SliceBudget`] — the per-project caps the operator
//!   configures and the per-partition slices the coordinator hands back.
//! - [`BudgetCoordinator`] — the trait every backend implements (in-memory
//!   for tests; the Postgres backend is a follow-on row on `partition_leases`).
//! - [`InMemoryBudgetCoordinator`] — the test/back-compat backend.
//! - [`SliceBudgetView`] — the cheap-to-clone handle cap consumers carry; it
//!   reads the latest slice the leaseholder's last heartbeat received.
//!
//! ## Failure path
//!
//! When the coordinator is unreachable, [`SliceBudgetView`] returns the last
//! slice it observed (stale; safe — it's the same number it had a heartbeat
//! ago). A brand-new `(project, partition)` whose heartbeat has never round-
//! tripped falls back to the local per-process cap the consumer was built
//! with — degraded (no cross-replica aggregation) but safe.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ProjectId, Result};
use tokio::sync::RwLock;

/// The six per-project cap consumer sites ADR 0023 names. Each gets its own
/// slice number per `(project, partition)` so a project that burns their REST
/// budget doesn't starve their pgwire / Wasm / memtable budget.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CapKind {
    /// `basin-rest` per-project HTTP rate limit (requests / sec).
    RestQps,
    /// `basin-router` pgwire per-project rate limit (statements / sec).
    PgQps,
    /// `basin-hottier` per-project memtable bytes-in-flight.
    MemtableBytes,
    /// `basin-realtime` per-project in-flight broadcast bytes
    /// (`BUFFER_FULL` trigger).
    RealtimeBytes,
    /// `basin-fn` per-project concurrent Wasm invocations
    /// (semaphore slots used).
    WasmConcurrency,
    /// `basin-net` per-project outbound HTTP rate (requests / sec).
    NetOutboundQps,
}

impl CapKind {
    /// All six in a stable order — useful for iteration in tests and the
    /// coordinator's reconciliation loop.
    pub const ALL: &'static [CapKind] = &[
        CapKind::RestQps,
        CapKind::PgQps,
        CapKind::MemtableBytes,
        CapKind::RealtimeBytes,
        CapKind::WasmConcurrency,
        CapKind::NetOutboundQps,
    ];
}

/// One heartbeat tick's worth of usage on a single `(project, partition)`
/// since the previous tick. The leaseholder accumulates these into its local
/// per-cap counters and resets them after each successful push.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UsageDelta {
    /// Per-cap usage in the units that cap is measured in (ops, bytes, slots).
    /// Missing entries are treated as zero.
    pub by_cap: HashMap<CapKind, u64>,
}

impl UsageDelta {
    /// Empty delta.
    pub fn zero() -> Self {
        Self::default()
    }

    /// Set or overwrite the count for one cap kind. Chains so call sites can
    /// build a delta in one expression.
    pub fn with(mut self, cap: CapKind, count: u64) -> Self {
        self.by_cap.insert(cap, count);
        self
    }

    /// Read this delta's value for `cap`, defaulting to zero. Used by the
    /// coordinator to aggregate across leaseholders.
    pub fn get(&self, cap: CapKind) -> u64 {
        self.by_cap.get(&cap).copied().unwrap_or(0)
    }

    /// Whether this delta carries any work at all. Heartbeats with an empty
    /// delta still need to round-trip (they pick up budget refreshes), but
    /// the coordinator can skip the aggregation step.
    pub fn is_empty(&self) -> bool {
        self.by_cap.values().all(|v| *v == 0)
    }
}

/// What the operator has configured for a project across all its partitions.
/// One row per project; the coordinator divides each cap by the live
/// partition count to compute the per-partition slice each leaseholder gets.
///
/// v0.1 keeps a single `ProjectBudget` per project, identical across cap
/// kinds at registration time but overridable per-kind via [`Self::with`].
/// `None` for a `CapKind` means "no project-wide cap on this consumer" —
/// the local per-process default still applies.
#[derive(Clone, Debug, Default)]
pub struct ProjectBudget {
    /// Per-cap project-wide budgets. Missing → uncapped (consumer falls back
    /// to its per-process default).
    pub caps: HashMap<CapKind, u64>,
}

impl ProjectBudget {
    /// Empty budget — every cap defers to the consumer's per-process default.
    pub fn uncapped() -> Self {
        Self::default()
    }

    /// Build with a single cap pre-populated; chainable.
    pub fn with(mut self, cap: CapKind, total: u64) -> Self {
        self.caps.insert(cap, total);
        self
    }

    /// Lookup a single cap's project-wide total.
    pub fn get(&self, cap: CapKind) -> Option<u64> {
        self.caps.get(&cap).copied()
    }
}

/// What the coordinator hands back to a single `(project, partition)`
/// leaseholder on the next heartbeat. The leaseholder writes this into its
/// [`SliceBudgetMap`] and every cap consumer reads from there on the hot
/// path.
#[derive(Clone, Debug, Default)]
pub struct SliceBudget {
    /// Per-cap allotment for this partition for the next heartbeat window.
    /// Missing → fall back to per-process default.
    pub slices: HashMap<CapKind, u64>,
}

impl SliceBudget {
    /// Empty slice — every cap defers to the consumer's per-process default.
    pub fn unconstrained() -> Self {
        Self::default()
    }

    /// Read one slice value, or `None` to mean "no coordinated cap; use
    /// per-process default".
    pub fn get(&self, cap: CapKind) -> Option<u64> {
        self.slices.get(&cap).copied()
    }

    /// Builder helper.
    pub fn with(mut self, cap: CapKind, slice: u64) -> Self {
        self.slices.insert(cap, slice);
        self
    }
}

/// Coordinator-side aggregate state for one `(project, partition)`. Snapshot
/// of the leaseholder's last push, plus the slice the coordinator computed
/// from the project totals.
#[derive(Clone, Debug, Default)]
pub struct UsageSnapshot {
    pub last_delta: UsageDelta,
    pub current_slice: SliceBudget,
}

/// The coordinator side of the heartbeat protocol. Backends:
///
/// - [`InMemoryBudgetCoordinator`] — in-process, used by tests and any
///   single-process deployment that wants the cap shape without a
///   coordinator.
/// - The Postgres backend lives alongside [`crate::leases::LeaseRegistry`]
///   on `PostgresCatalog`; same connection pool, same row primary-keyed by
///   `(project, partition)`. Follow-on landing in 6.X.D.2; the trait is
///   shaped so the swap is a backend change, no call-site edit.
#[async_trait]
pub trait BudgetCoordinator: Send + Sync {
    /// Replace the project-wide budget for `project`. Idempotent; coordinator
    /// recomputes slices on the next reconciliation.
    async fn set_project_budget(&self, project: &ProjectId, budget: ProjectBudget) -> Result<()>;

    /// Push one heartbeat's worth of usage for `(project, partition)` from
    /// `holder` and receive the slice budget that should apply for the next
    /// heartbeat window. Atomic w.r.t. concurrent pushes from peer
    /// leaseholders on the *same* project (different partitions) — the
    /// coordinator's reconciliation runs under one lock per project so the
    /// per-partition slices stay consistent with the live partition count.
    async fn push_heartbeat(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        delta: UsageDelta,
    ) -> Result<SliceBudget>;

    /// Drop the per-`(project, partition)` aggregate when a partition is
    /// deleted or its lease is permanently abandoned. Idempotent.
    async fn forget_partition(&self, project: &ProjectId, partition_id: &str) -> Result<()>;

    /// Coordinator-side snapshot of one partition's last push + slice. Used
    /// by tests; production reads the slice via [`SliceBudgetView`].
    async fn snapshot(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<UsageSnapshot>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// InMemoryBudgetCoordinator
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct InMemoryProjectState {
    budget: ProjectBudget,
    /// `(partition_id, holder) → last seen UsageDelta`. We key on holder too
    /// so a stale heartbeat from an old leaseholder (that never released the
    /// row via `forget_partition`) is replaced atomically on the new
    /// holder's first push.
    partitions: HashMap<String, (String, UsageDelta)>,
    /// Last-handed-back slice per partition; mirrors what the leaseholder's
    /// [`SliceBudgetMap`] currently shows.
    slices: HashMap<String, SliceBudget>,
}

/// In-process implementation. Sufficient for tests and the back-compat
/// "single-process deployment" path; the production multi-replica path
/// swaps in the Postgres backend (same trait, same call sites).
#[derive(Default)]
pub struct InMemoryBudgetCoordinator {
    projects: RwLock<HashMap<ProjectId, InMemoryProjectState>>,
}

impl InMemoryBudgetCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recompute slice for one partition, given the project's full state.
    /// `partition_count` is the live partition count for the project (i.e.
    /// the number of `(project, _)` rows in `partitions` after the push).
    /// Slice = `floor(project_cap / partition_count)`; conservative — a
    /// project never over-allocates due to integer rounding.
    fn compute_slice(budget: &ProjectBudget, partition_count: usize) -> SliceBudget {
        let mut out = SliceBudget::unconstrained();
        let n = partition_count.max(1) as u64;
        for cap in CapKind::ALL {
            if let Some(total) = budget.get(*cap) {
                out.slices.insert(*cap, total / n);
            }
        }
        out
    }
}

#[async_trait]
impl BudgetCoordinator for InMemoryBudgetCoordinator {
    async fn set_project_budget(&self, project: &ProjectId, budget: ProjectBudget) -> Result<()> {
        let mut g = self.projects.write().await;
        let entry = g.entry(*project).or_default();
        entry.budget = budget;
        // Re-slice every known partition so existing leaseholders pick up the
        // new cap on their next heartbeat.
        let n = entry.partitions.len();
        let new_slice = Self::compute_slice(&entry.budget, n.max(1));
        for (_, slice) in entry.slices.iter_mut() {
            *slice = new_slice.clone();
        }
        Ok(())
    }

    async fn push_heartbeat(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        delta: UsageDelta,
    ) -> Result<SliceBudget> {
        let mut g = self.projects.write().await;
        let entry = g.entry(*project).or_default();
        entry
            .partitions
            .insert(partition_id.to_string(), (holder.to_string(), delta));
        let n = entry.partitions.len();
        let slice = Self::compute_slice(&entry.budget, n);
        entry.slices.insert(partition_id.to_string(), slice.clone());
        Ok(slice)
    }

    async fn forget_partition(&self, project: &ProjectId, partition_id: &str) -> Result<()> {
        let mut g = self.projects.write().await;
        if let Some(entry) = g.get_mut(project) {
            entry.partitions.remove(partition_id);
            entry.slices.remove(partition_id);
            // Re-slice remaining partitions with the new (smaller) count.
            let n = entry.partitions.len().max(1);
            let new_slice = Self::compute_slice(&entry.budget, n);
            for (_, slice) in entry.slices.iter_mut() {
                *slice = new_slice.clone();
            }
        }
        Ok(())
    }

    async fn snapshot(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<UsageSnapshot>> {
        let g = self.projects.read().await;
        let Some(entry) = g.get(project) else {
            return Ok(None);
        };
        let last_delta = entry
            .partitions
            .get(partition_id)
            .map(|(_, d)| d.clone())
            .unwrap_or_default();
        let current_slice = entry.slices.get(partition_id).cloned().unwrap_or_default();
        Ok(Some(UsageSnapshot {
            last_delta,
            current_slice,
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SliceBudgetMap — the local hot-path view a cap consumer reads
// ─────────────────────────────────────────────────────────────────────────────

/// Cheap-to-clone, lock-free view of "what slice does my leaseholder think
/// this `(project, cap)` is allotted for the current heartbeat window?"
///
/// Each cap consumer (`basin-rest`, `basin-router`, `basin-hottier`, …) holds
/// one `SliceBudgetView` instance. On every admit it calls
/// [`SliceBudgetView::slice_for`] — a single hash-map lookup + atomic load,
/// no `.await`, no lock contention. The heartbeat loop writes the slice once
/// per tick via [`SliceBudgetView::set_slice`].
///
/// **No slice present** for a `(project, cap)` pair means "the leaseholder
/// has not received a coordinated slice yet, OR the project is uncapped".
/// The consumer falls back to its per-process default in that case — safe
/// degradation when the coordinator is unreachable or for projects with no
/// configured project-wide cap.
#[derive(Clone, Default)]
pub struct SliceBudgetView {
    inner: Arc<SliceBudgetViewInner>,
}

#[derive(Default)]
struct SliceBudgetViewInner {
    /// `(project, cap) → atomic slice (u64::MAX sentinel = "no slice")`.
    /// The `RwLock` is held only for the rare insert of a fresh
    /// `(project, cap)` row; subsequent updates and reads go through the
    /// atomic directly. Sized for `O(active_projects × 6)` entries.
    slots: RwLock<HashMap<(ProjectId, CapKind), Arc<AtomicU64>>>,
}

/// Sentinel meaning "no slice has been pushed yet (or it was explicitly
/// cleared)". `u64::MAX` is impossible as a real cap value at any cap site
/// in this codebase (the largest legal one is the 64 GiB memtable hard cap,
/// well below 2^63).
pub const SLICE_UNSET: u64 = u64::MAX;

impl SliceBudgetView {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write the slice for `(project, cap)`. Lock-free fast path: if the
    /// slot already exists we just store into its atomic. The first write
    /// for a `(project, cap)` pair allocates the slot under a brief write
    /// lock. Called by the heartbeat tick.
    pub async fn set_slice(&self, project: ProjectId, cap: CapKind, slice: u64) {
        {
            let g = self.inner.slots.read().await;
            if let Some(slot) = g.get(&(project, cap)) {
                slot.store(slice, Ordering::Relaxed);
                return;
            }
        }
        let mut g = self.inner.slots.write().await;
        g.entry((project, cap))
            .or_insert_with(|| Arc::new(AtomicU64::new(SLICE_UNSET)))
            .store(slice, Ordering::Relaxed);
    }

    /// Read the current slice for `(project, cap)`. Returns [`SLICE_UNSET`]
    /// if no slice has been pushed — caller should fall back to the
    /// per-process default. Hot path: at most one async lock acquisition
    /// (read), then one atomic load.
    pub async fn slice_for(&self, project: ProjectId, cap: CapKind) -> u64 {
        let g = self.inner.slots.read().await;
        g.get(&(project, cap))
            .map(|s| s.load(Ordering::Relaxed))
            .unwrap_or(SLICE_UNSET)
    }

    /// Synchronous variant of [`Self::slice_for`] for call sites that cannot
    /// `.await` (e.g. the realtime `try_reserve` hot path). Uses
    /// [`RwLock::try_read`] and degrades to "no slice" on contention; given
    /// the slot map is read-mostly this rarely matters in practice.
    pub fn slice_for_sync(&self, project: ProjectId, cap: CapKind) -> u64 {
        match self.inner.slots.try_read() {
            Ok(g) => g
                .get(&(project, cap))
                .map(|s| s.load(Ordering::Relaxed))
                .unwrap_or(SLICE_UNSET),
            Err(_) => SLICE_UNSET,
        }
    }

    /// Test/observability: clear all slices. Subsequent reads return
    /// [`SLICE_UNSET`] so consumers fall back to local caps.
    pub async fn clear(&self) {
        let mut g = self.inner.slots.write().await;
        g.clear();
    }
}

impl std::fmt::Debug for SliceBudgetView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceBudgetView").finish_non_exhaustive()
    }
}

/// Per-`(project, cap)` admission gate built around a [`SliceBudgetView`].
/// Holds a per-`(project, cap)` token counter that decrements on each
/// admission; the heartbeat loop refills it every tick. When the counter
/// underflows the gate rejects (returns `false`) — the cap consumer's
/// caller maps that to its existing "rejected" code path (`RateLimited`,
/// `BUFFER_FULL`, etc.).
///
/// Per-project cost: one `AtomicU64` per `(project, cap)` actually exercised.
/// Cheap to clone (Arc inside).
#[derive(Clone, Default)]
pub struct SliceGate {
    view: SliceBudgetView,
    counters: Arc<RwLock<HashMap<(ProjectId, CapKind), Arc<AtomicU64>>>>,
}

impl SliceGate {
    pub fn new(view: SliceBudgetView) -> Self {
        Self {
            view,
            counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Underlying view (so the heartbeat loop can push slices).
    pub fn view(&self) -> &SliceBudgetView {
        &self.view
    }

    /// Try to consume `count` units of the `(project, cap)` slice. Returns:
    /// - `Ok(())` if no slice is set (no coordinator cap on this pair — the
    ///   consumer's local per-process cap is the real one).
    /// - `Ok(())` if a slice IS set and after the decrement the counter is
    ///   non-negative.
    /// - `Err(())` if a slice is set and the decrement would underflow.
    ///
    /// The token counter is refilled every heartbeat tick via
    /// [`SliceGate::refill_from_view`].
    pub async fn try_consume(
        &self,
        project: ProjectId,
        cap: CapKind,
        count: u64,
    ) -> Result<(), ()> {
        let slice = self.view.slice_for(project, cap).await;
        if slice == SLICE_UNSET {
            // Coordinator has no opinion → defer to per-process default.
            return Ok(());
        }
        let counter = self.counter_for(project, cap).await;
        // Lazy initialise the counter to the slice on first sighting.
        let _ = counter.compare_exchange(SLICE_UNSET, slice, Ordering::AcqRel, Ordering::Relaxed);
        // CAS-loop decrement; refuse if it would underflow.
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current == SLICE_UNSET {
                current = slice;
            }
            if current < count {
                return Err(());
            }
            let next = current - count;
            match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Reset every existing counter to its current slice. Called by the
    /// heartbeat tick after [`SliceBudgetView::set_slice`] writes have
    /// landed.
    pub async fn refill_from_view(&self) {
        let g = self.counters.read().await;
        for ((project, cap), counter) in g.iter() {
            let slice = self.view.slice_for(*project, *cap).await;
            counter.store(slice, Ordering::Relaxed);
        }
    }

    async fn counter_for(&self, project: ProjectId, cap: CapKind) -> Arc<AtomicU64> {
        {
            let g = self.counters.read().await;
            if let Some(c) = g.get(&(project, cap)) {
                return c.clone();
            }
        }
        let mut g = self.counters.write().await;
        g.entry((project, cap))
            .or_insert_with(|| Arc::new(AtomicU64::new(SLICE_UNSET)))
            .clone()
    }
}

impl std::fmt::Debug for SliceGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceGate").finish_non_exhaustive()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delta_get_defaults_to_zero() {
        let d = UsageDelta::zero();
        assert_eq!(d.get(CapKind::RestQps), 0);
        let d = d.with(CapKind::RestQps, 7);
        assert_eq!(d.get(CapKind::RestQps), 7);
        assert_eq!(d.get(CapKind::PgQps), 0);
    }

    #[tokio::test]
    async fn coordinator_slices_floor_divide() {
        let c = InMemoryBudgetCoordinator::new();
        let p = ProjectId::new();
        c.set_project_budget(&p, ProjectBudget::default().with(CapKind::RestQps, 60))
            .await
            .unwrap();

        // First leaseholder for partition "p-0".
        let s0 = c
            .push_heartbeat(&p, "p-0", "replica-a", UsageDelta::zero())
            .await
            .unwrap();
        // Single partition: full 60.
        assert_eq!(s0.get(CapKind::RestQps), Some(60));

        // Second leaseholder for partition "p-1": now 2 partitions → 30 each.
        let s1 = c
            .push_heartbeat(&p, "p-1", "replica-b", UsageDelta::zero())
            .await
            .unwrap();
        assert_eq!(s1.get(CapKind::RestQps), Some(30));

        // Third partition: 60/3 = 20.
        let s2 = c
            .push_heartbeat(&p, "p-2", "replica-c", UsageDelta::zero())
            .await
            .unwrap();
        assert_eq!(s2.get(CapKind::RestQps), Some(20));
    }

    #[tokio::test]
    async fn coordinator_forget_partition_reslices() {
        let c = InMemoryBudgetCoordinator::new();
        let p = ProjectId::new();
        c.set_project_budget(&p, ProjectBudget::default().with(CapKind::RestQps, 60))
            .await
            .unwrap();
        let _ = c
            .push_heartbeat(&p, "p-0", "a", UsageDelta::zero())
            .await
            .unwrap();
        let _ = c
            .push_heartbeat(&p, "p-1", "b", UsageDelta::zero())
            .await
            .unwrap();

        // Drop p-1; pushing a fresh heartbeat for p-0 should now see the full 60.
        c.forget_partition(&p, "p-1").await.unwrap();
        let s = c
            .push_heartbeat(&p, "p-0", "a", UsageDelta::zero())
            .await
            .unwrap();
        assert_eq!(s.get(CapKind::RestQps), Some(60));
    }

    #[tokio::test]
    async fn coordinator_uncapped_project_returns_none_per_cap() {
        let c = InMemoryBudgetCoordinator::new();
        let p = ProjectId::new();
        let s = c
            .push_heartbeat(&p, "p-0", "a", UsageDelta::zero())
            .await
            .unwrap();
        assert!(s.get(CapKind::RestQps).is_none());
        assert!(s.get(CapKind::PgQps).is_none());
    }

    #[tokio::test]
    async fn slice_view_set_then_read() {
        let view = SliceBudgetView::new();
        let p = ProjectId::new();
        assert_eq!(view.slice_for(p, CapKind::RestQps).await, SLICE_UNSET);
        view.set_slice(p, CapKind::RestQps, 42).await;
        assert_eq!(view.slice_for(p, CapKind::RestQps).await, 42);
        assert_eq!(view.slice_for_sync(p, CapKind::RestQps), 42);
    }

    #[tokio::test]
    async fn slice_gate_admits_when_no_slice() {
        let g = SliceGate::new(SliceBudgetView::new());
        let p = ProjectId::new();
        for _ in 0..1000 {
            assert!(g.try_consume(p, CapKind::RestQps, 1).await.is_ok());
        }
    }

    #[tokio::test]
    async fn slice_gate_rejects_after_slice_exhausted() {
        let g = SliceGate::new(SliceBudgetView::new());
        let p = ProjectId::new();
        g.view().set_slice(p, CapKind::RestQps, 5).await;

        // First 5 admit.
        for _ in 0..5 {
            assert!(g.try_consume(p, CapKind::RestQps, 1).await.is_ok());
        }
        // 6th must reject.
        assert!(g.try_consume(p, CapKind::RestQps, 1).await.is_err());

        // Refill from a fresh slice → admits again.
        g.view().set_slice(p, CapKind::RestQps, 3).await;
        g.refill_from_view().await;
        for _ in 0..3 {
            assert!(g.try_consume(p, CapKind::RestQps, 1).await.is_ok());
        }
        assert!(g.try_consume(p, CapKind::RestQps, 1).await.is_err());
    }

    /// **The acceptance test.** Three "replicas" share one coordinator. The
    /// project is capped at 60 RestQps. Each replica spends its slice (20);
    /// the aggregate never exceeds 60. Mirrors the multi-instance bypass
    /// scenario from the audit.
    #[tokio::test]
    async fn multi_replica_aggregate_stays_under_project_cap() {
        let coord = Arc::new(InMemoryBudgetCoordinator::new());
        let p = ProjectId::new();
        coord
            .set_project_budget(&p, ProjectBudget::default().with(CapKind::RestQps, 60))
            .await
            .unwrap();

        // One slice gate per replica, one partition per replica.
        let gates: Vec<SliceGate> = (0..3)
            .map(|_| SliceGate::new(SliceBudgetView::new()))
            .collect();
        // First heartbeat round registers each partition. Slices the first
        // few replicas receive are stale (the coordinator hadn't seen the
        // later partitions yet). Acceptable per ADR 0023 — the over-cap
        // window is bounded to one heartbeat tick — so we follow with a
        // second round that converges. The test mirrors steady-state.
        for round in 0..2 {
            for (i, gate) in gates.iter().enumerate() {
                let slice = coord
                    .push_heartbeat(&p, &format!("p-{i}"), &format!("r-{i}"), UsageDelta::zero())
                    .await
                    .unwrap();
                for cap in CapKind::ALL {
                    if let Some(s) = slice.get(*cap) {
                        gate.view().set_slice(p, *cap, s).await;
                    }
                }
                gate.refill_from_view().await;
            }
            // Sanity: by the end of round 1 every replica must see slice=20.
            if round == 1 {
                for gate in &gates {
                    assert_eq!(gate.view().slice_for(p, CapKind::RestQps).await, 20);
                }
            }
        }

        // Each replica burns its slice. Slice = 60 / 3 = 20 each.
        let mut admitted_total = 0u64;
        for gate in &gates {
            for _ in 0..40 {
                if gate.try_consume(p, CapKind::RestQps, 1).await.is_ok() {
                    admitted_total += 1;
                }
            }
        }
        assert_eq!(
            admitted_total, 60,
            "aggregate admits across 3 replicas must equal the project cap (60), not 3 × 60"
        );
    }

    #[tokio::test]
    async fn snapshot_returns_last_delta_and_slice() {
        let c = InMemoryBudgetCoordinator::new();
        let p = ProjectId::new();
        c.set_project_budget(&p, ProjectBudget::default().with(CapKind::PgQps, 100))
            .await
            .unwrap();
        let delta = UsageDelta::zero().with(CapKind::PgQps, 7);
        let _ = c
            .push_heartbeat(&p, "part-x", "host", delta.clone())
            .await
            .unwrap();
        let snap = c.snapshot(&p, "part-x").await.unwrap().unwrap();
        assert_eq!(snap.last_delta.get(CapKind::PgQps), 7);
        assert_eq!(snap.current_slice.get(CapKind::PgQps), Some(100));
    }

    #[tokio::test]
    async fn slice_gate_count_greater_than_slice_rejects() {
        let g = SliceGate::new(SliceBudgetView::new());
        let p = ProjectId::new();
        g.view().set_slice(p, CapKind::MemtableBytes, 1024).await;
        // Single request claiming more than the entire slice → reject.
        assert!(g
            .try_consume(p, CapKind::MemtableBytes, 2048)
            .await
            .is_err());
    }
}
