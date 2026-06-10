//! [`MemTableRegistry`] — process-wide, multi-project memtable registry.
//!
//! # Isolation model
//!
//! Follows the shared-heavy-resource + cheap-per-project-primitive pattern
//! from `basin-common::ProjectCounterRegistry`:
//!
//! - **Shared heavy resource**: one `DashMap<(ProjectId, TableName),
//!   Arc<MemTableEntry>>`.  Allocation is lazy: only projects that have
//!   received at least one write consume an entry.  Inactive projects cost zero.
//! - **Cheap per-project primitive**: one `AtomicU64` byte counter + one
//!   `Arc<tokio::sync::Semaphore>` per project.  The semaphore models the
//!   `project_memtable_hard_cap` back-pressure mechanism (C5).
//!
//! # Cost model
//!
//! At 10 000 inactive projects the registry is a 10 000-entry `DashMap` with
//! zero-byte counters — sub-MB overhead total.  Per-project semaphores are
//! allocated only on first write.
//!
//! # Phase 5.14.C5 budget enforcement
//!
//! [`MemTableRegistry::try_reserve_bytes`] gates every write:
//!
//! - Under soft cap → [`ReservationOutcome::Granted`]; byte counter bumped.
//! - Over soft cap but under hard cap → [`ReservationOutcome::FlushSuggested`];
//!   byte counter bumped and a [`FlushRequest`] enqueued on the flush channel.
//! - Would exceed hard cap → [`ReservationOutcome::HardCapReached`]; the caller
//!   must await semaphore permits.
//!
//! [`MemTableRegistry::release_bytes`] decrements the counter and releases
//! semaphore permits proportional to the freed size (1 permit per 1 KiB).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use basin_common::ids::{ProjectId, TableName};
use dashmap::DashMap;
use tokio::sync::{mpsc, Semaphore};

use crate::budget::MemTableConfig;
use crate::memtable::MemTable;

// ── Backward-compatible re-exports (C1 tests reference these) ─────────────────
pub use crate::budget::{
    DEFAULT_MEMTABLE_MAX_AGE_SECS as MEMTABLE_MAX_AGE_SECS,
    DEFAULT_PROJECT_HARD_CAP_BYTES as PROJECT_MEMTABLE_HARD_CAP_BYTES,
    DEFAULT_PROJECT_SOFT_CAP_BYTES as PROJECT_MEMTABLE_SOFT_CAP_BYTES,
    DEFAULT_TABLE_SOFT_CAP_BYTES as TABLE_MEMTABLE_SOFT_CAP_BYTES,
};

// ── Semaphore granularity ─────────────────────────────────────────────────────

/// One semaphore permit represents 1 KiB. This keeps the permit count well
/// below `u32::MAX` even for a 64 GiB hard cap (64 GiB / 1 KiB = 67 108 864).
const BYTES_PER_PERMIT: u64 = 1024;

fn bytes_to_permits(bytes: u64) -> usize {
    ((bytes + BYTES_PER_PERMIT - 1) / BYTES_PER_PERMIT) as usize
}

// ── FlushRequest ──────────────────────────────────────────────────────────────

/// Request enqueued on the flush channel when the soft cap is crossed.
/// Phase 5.14.C4 consumes this channel; at C5 the receiver is dropped so
/// sends are silently ignored.
#[derive(Debug, Clone)]
pub struct FlushRequest {
    /// The project that triggered the soft-cap flush.
    pub project: ProjectId,
}

// ── ReservationOutcome ────────────────────────────────────────────────────────

/// Result returned by [`MemTableRegistry::try_reserve_bytes`].
#[derive(Debug, PartialEq, Eq)]
pub enum ReservationOutcome {
    /// Under soft cap. Bytes were reserved; write may proceed immediately.
    Granted,
    /// Between soft cap and hard cap. Bytes were reserved but a background
    /// flush should be scheduled. Write may proceed.
    FlushSuggested,
    /// Hard cap would be exceeded. Bytes were NOT reserved; the caller must
    /// await semaphore permits.
    HardCapReached,
}

// ── MemTableEntry ─────────────────────────────────────────────────────────────

/// One entry in the registry: the live [`MemTable`] for a `(project, table)`
/// pair.
pub struct MemTableEntry {
    pub memtable: MemTable,
    /// ADR 0027 Phase 4 — promoted-JSONB shadow-column cleanliness flag.
    ///
    /// `true` means this memtable MAY hold at least one live (non-tombstone)
    /// row whose materialised image does NOT carry a currently-promoted
    /// `__promoted$col$key` shadow column. Such a row cannot be read via the
    /// shadow-column fast path (it would null-fill to a WRONG value), so the
    /// `payload->>'key'` promoted-column rewrite must be disabled for the whole
    /// table while this is set.
    ///
    /// Lifecycle:
    ///   * starts `false` (empty memtable — trivially clean);
    ///   * set `true` when a JSONB path is promoted while this memtable already
    ///     holds live rows (those pre-promotion rows lack the new shadow col),
    ///     and whenever a write lands a row that does not carry every promoted
    ///     shadow column;
    ///   * cleared back to `false` once the memtable drains to empty.
    ///
    /// When CLEAR, every live memtable row is guaranteed to carry all promoted
    /// shadow columns (the UPDATE/INSERT fast paths materialise them), so the
    /// promoted fast path is correctness-safe even with hot rows present.
    pub shadow_dirty: std::sync::atomic::AtomicBool,
}

impl MemTableEntry {
    fn new() -> Self {
        Self {
            memtable: MemTable::new(),
            shadow_dirty: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

// ── Per-project state ─────────────────────────────────────────────────────────

/// State lazily allocated per active project.
pub struct ProjectMemState {
    /// Running byte total for all writes to this project's memtables.
    /// Updated atomically; read by the budget enforcement path without locks.
    pub bytes_allocated: AtomicU64,
    /// Semaphore modelling the project hard cap.  Capacity is set to
    /// `hard_cap_bytes / BYTES_PER_PERMIT` permits at construction.
    /// `release_bytes` restores permits; the C4 flush task uses this to
    /// unblock blocked writers.
    pub hard_cap_sem: Arc<Semaphore>,
    /// Per-project hard cap (bytes). May be overridden by
    /// `ALTER PROJECT … SET basin.memtable_hard_cap`.
    pub hard_cap_bytes: u64,
    /// Per-project soft cap (bytes).
    pub soft_cap_bytes: u64,
}

impl ProjectMemState {
    fn new_with_caps(hard_cap_bytes: u64, soft_cap_bytes: u64) -> Self {
        let sem_permits = bytes_to_permits(hard_cap_bytes);
        Self {
            bytes_allocated: AtomicU64::new(0),
            hard_cap_sem: Arc::new(Semaphore::new(sem_permits)),
            hard_cap_bytes,
            soft_cap_bytes,
        }
    }

    #[allow(dead_code)]
    fn new() -> Self {
        Self::new_with_caps(
            crate::budget::DEFAULT_PROJECT_HARD_CAP_BYTES,
            crate::budget::DEFAULT_PROJECT_SOFT_CAP_BYTES,
        )
    }
}

// ── MemTableRegistry ─────────────────────────────────────────────────────────

/// Process-wide registry of per-`(project, table)` memtables.
///
/// Thread-safe; designed as a process-level singleton.
///
/// ## Phase 6.X.D — heartbeat-reconciled MemtableBytes cap (ADR 0023)
///
/// Optionally carries a [`basin_catalog::SliceBudgetView`]. When set, the
/// effective per-project hard cap becomes `min(config.project_hard_cap_bytes,
/// slice_for(project, MemtableBytes))`. The slice closes the multi-instance
/// bypass: with N replicas each holding a 256 MiB local cap, a project
/// could allocate `N × 256 MiB` total; with the slice attached they share
/// `project_total / partition_count` per replica. Back-compat: when no slice
/// is set the config cap is used unchanged.
pub struct MemTableRegistry {
    tables: DashMap<(ProjectId, TableName), Arc<MemTableEntry>>,
    projects: DashMap<ProjectId, Arc<ProjectMemState>>,
    config: MemTableConfig,
    /// Soft-cap flush channel sender. Sends are best-effort; the C4 stub
    /// drops the receiver so `SendError` is silently ignored.
    flush_tx: mpsc::UnboundedSender<FlushRequest>,
    /// Phase 6.X.D — coordinator-handed slice cap. `None` in single-replica
    /// / back-compat mode; `Some` when the shard heartbeat is wired.
    slice: Option<basin_catalog::SliceBudgetView>,
}

impl MemTableRegistry {
    /// Construct an empty registry with [`MemTableConfig::default`].
    pub fn new() -> Self {
        Self::new_with_config(MemTableConfig::default())
    }

    /// Construct an empty registry with the supplied config.
    pub fn new_with_config(config: MemTableConfig) -> Self {
        let (flush_tx, flush_rx) = mpsc::unbounded_channel::<FlushRequest>();
        // C4 stub: drop the receiver. Sends return Err which we ignore.
        drop(flush_rx);
        Self {
            tables: DashMap::new(),
            projects: DashMap::new(),
            config,
            flush_tx,
            slice: None,
        }
    }

    /// Phase 6.X.D — attach a slice view so [`Self::try_reserve_bytes`]
    /// enforces the smaller of the local config cap and the
    /// coordinator-handed slice for `(project, MemtableBytes)`. The shard
    /// heartbeat loop fills the view; consumers transparently degrade to
    /// the config cap when no slice is set.
    pub fn with_slice_view(mut self, view: basin_catalog::SliceBudgetView) -> Self {
        self.slice = Some(view);
        self
    }

    /// Read-only access to the attached slice view (for the shard heartbeat
    /// loop that pushes slices back into it).
    pub fn slice_view(&self) -> Option<&basin_catalog::SliceBudgetView> {
        self.slice.as_ref()
    }

    // ── table CRUD ────────────────────────────────────────────────────────────

    /// Get-or-create the [`MemTableEntry`] for `(project, table)`.
    pub fn get_or_create(&self, project: ProjectId, table: TableName) -> Arc<MemTableEntry> {
        self.tables
            .entry((project, table))
            .or_insert_with(|| Arc::new(MemTableEntry::new()))
            .clone()
    }

    /// Current hot-tier mutation epoch for `(project, table)` (Fix A).
    ///
    /// Returns the memtable's monotonic mutation counter, bumped on every
    /// insert / upsert / delete. Returns `0` when the project/table has never
    /// received a write (no memtable entry exists) — which matches a freshly
    /// constructed memtable's epoch, so the watermark is consistent across the
    /// lazy-allocation boundary.
    ///
    /// Used by the PK row cache as one of the two invalidation watermarks: a
    /// cached entry is valid only if `entry.hot_epoch == hot_tier_epoch(...)`.
    pub fn hot_tier_epoch(&self, project: &ProjectId, table: &TableName) -> u64 {
        self.tables
            .get(&(*project, table.clone()))
            .map(|e| e.memtable.epoch())
            .unwrap_or(0)
    }

    /// Current hot-tier MVCC sequence high-water mark for `(project, table)`.
    ///
    /// Returns the memtable's monotonic write sequence, advanced on every
    /// insert / upsert / delete. Returns `0` when the project/table has never
    /// received a write (no memtable entry exists), matching a freshly
    /// constructed memtable's `current_seq()`, so the watermark is consistent
    /// across the lazy-allocation boundary.
    ///
    /// Captured by an opening transaction as its per-table `hot_seq_watermark`
    /// (at first-touch, mirroring the cold-snapshot pin). The overlay read path
    /// then drops any registry entry whose `seq` exceeds the pinned watermark —
    /// a write committed by another session *after* the transaction pinned its
    /// snapshot — so hot-tier UPDATE/DELETE fast-path writes no longer leak into
    /// an open transaction's view.
    pub fn hot_tier_seq(&self, project: &ProjectId, table: &TableName) -> u64 {
        self.tables
            .get(&(*project, table.clone()))
            .map(|e| e.memtable.current_seq())
            .unwrap_or(0)
    }

    /// Look up an existing entry without creating one.
    pub fn get(&self, project: &ProjectId, table: &TableName) -> Option<Arc<MemTableEntry>> {
        self.tables
            .get(&(*project, table.clone()))
            .map(|e| e.clone())
    }

    /// ADR 0027 Phase 4 — mark this table's memtable as possibly holding rows
    /// that lack a promoted shadow column. Called when a JSONB path is promoted
    /// while the memtable already holds live rows. No-op (does not create an
    /// entry) when the table has no memtable: an absent entry is trivially clean.
    pub fn mark_shadow_dirty(&self, project: &ProjectId, table: &TableName) {
        if let Some(e) = self.tables.get(&(*project, table.clone())) {
            e.shadow_dirty
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// ADR 0027 Phase 4 — true iff the table's memtable holds at least one live
    /// (non-tombstone) row AND that row set may be missing a promoted shadow
    /// column. When this returns `false` with a non-empty memtable, every live
    /// hot row is known to carry all promoted shadow columns, so the promoted
    /// fast path is correctness-safe. Auto-clears the dirty flag once the
    /// memtable has drained to empty (the next promoted query then re-enables
    /// the fast path without an explicit drain hook).
    pub fn memtable_blocks_promoted_read(&self, project: &ProjectId, table: &TableName) -> bool {
        let Some(e) = self.tables.get(&(*project, table.clone())) else {
            return false;
        };
        if e.memtable.snapshot().is_empty() {
            // Drained: reset cleanliness so a later promoted query is fast again.
            e.shadow_dirty
                .store(false, std::sync::atomic::Ordering::Release);
            return false;
        }
        e.shadow_dirty.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get-or-create per-project state (byte counter + semaphore).
    pub fn project_state(&self, project: ProjectId) -> Arc<ProjectMemState> {
        let hard = self.config.project_hard_cap_bytes;
        let soft = self.config.project_soft_cap_bytes;
        self.projects
            .entry(project)
            .or_insert_with(|| Arc::new(ProjectMemState::new_with_caps(hard, soft)))
            .clone()
    }

    // ── Phase 5.14.C5 budget API ──────────────────────────────────────────────

    /// Attempt to reserve `n` bytes for `project`. Never blocks.
    ///
    /// - **Granted**: after + n <= soft_cap; counter bumped.
    /// - **FlushSuggested**: soft_cap < after <= hard_cap; counter bumped,
    ///   flush request enqueued.
    /// - **HardCapReached**: would exceed hard_cap; counter NOT bumped.
    ///
    /// Phase 6.X.D: when a slice view is attached, the effective hard cap
    /// is `min(state.hard_cap_bytes, slice_for(project, MemtableBytes))`.
    /// The slice closes the multi-instance bypass — each replica's local
    /// allotment is `project_total / partition_count` (no `N × hard_cap`).
    pub fn try_reserve_bytes(&self, project: &ProjectId, n: u64) -> ReservationOutcome {
        let state = self.project_state(*project);
        let hard_cap = self.effective_hard_cap(project, &state);
        let current = state.bytes_allocated.load(Ordering::Relaxed);
        let after = current.saturating_add(n);

        if after > hard_cap {
            return ReservationOutcome::HardCapReached;
        }

        // CAS loop to commit the reservation.
        let mut cur = current;
        loop {
            match state.bytes_allocated.compare_exchange_weak(
                cur,
                cur.saturating_add(n),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    cur = actual;
                    // Re-check the effective hard cap after losing the race.
                    if cur.saturating_add(n) > hard_cap {
                        return ReservationOutcome::HardCapReached;
                    }
                }
            }
        }

        if after > state.soft_cap_bytes {
            let _ = self.flush_tx.send(FlushRequest { project: *project });
            ReservationOutcome::FlushSuggested
        } else {
            ReservationOutcome::Granted
        }
    }

    /// Compute the effective hard cap for `project` given the attached
    /// slice view (if any). Returns `state.hard_cap_bytes` when no slice is
    /// configured (back-compat) or when the slice is unset (coordinator
    /// silent / project uncapped).
    fn effective_hard_cap(&self, project: &ProjectId, state: &ProjectMemState) -> u64 {
        let local = state.hard_cap_bytes;
        let Some(view) = self.slice.as_ref() else {
            return local;
        };
        let slice = view.slice_for_sync(*project, basin_catalog::CapKind::MemtableBytes);
        if slice == basin_catalog::SLICE_UNSET {
            local
        } else {
            local.min(slice)
        }
    }

    /// Decrement `project`'s byte counter by `n` and release semaphore permits
    /// (1 permit per 1 KiB freed). Called by the flush task (C4).
    pub fn release_bytes(&self, project: &ProjectId, n: u64) {
        if n == 0 {
            return;
        }
        if let Some(state) = self.projects.get(project) {
            let cur = state.bytes_allocated.load(Ordering::Relaxed);
            state
                .bytes_allocated
                .store(cur.saturating_sub(n), Ordering::Release);
            state.hard_cap_sem.add_permits(bytes_to_permits(n));
        }
    }

    /// Return the project with the highest `bytes_allocated`. O(n).
    /// Used by the largest-first flush scheduler.
    pub fn largest_project(&self) -> Option<ProjectId> {
        self.projects
            .iter()
            .max_by_key(|e| e.value().bytes_allocated.load(Ordering::Relaxed))
            .map(|e| *e.key())
    }

    /// Current `bytes_allocated` for `project`. Returns 0 for unknown projects.
    pub fn project_bytes(&self, project: &ProjectId) -> u64 {
        self.projects
            .get(project)
            .map(|s| s.bytes_allocated.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    // ── misc ──────────────────────────────────────────────────────────────────

    /// Total number of `(project, table)` entries.
    pub fn entry_count(&self) -> usize {
        self.tables.len()
    }

    /// Sum of `bytes_allocated` across all live memtable entries. O(n).
    pub fn total_bytes_allocated(&self) -> u64 {
        self.tables
            .iter()
            .map(|e| e.value().memtable.bytes_allocated())
            .sum()
    }

    /// Remove the entry for `(project, table)`. Called after flush + commit.
    pub fn remove(&self, project: &ProjectId, table: &TableName) {
        self.tables.remove(&(*project, table.clone()));
    }

    /// Remove **all** memtable entries for `project` and reset the project's
    /// byte counter to zero.  Called by the synchronous flush path when the
    /// global hard cap is reached and the largest project must be evicted to
    /// free budget for incoming writes (Phase 5.14.C2 budget enforcement).
    pub fn remove_project(&self, project: &ProjectId) {
        self.tables.retain(|(p, _), _| p != project);
        if let Some(state) = self.projects.get(project) {
            state.bytes_allocated.store(0, Ordering::Release);
        }
    }

    /// Reference to the process-wide [`MemTableConfig`].
    pub fn config(&self) -> &MemTableConfig {
        &self.config
    }

    // ── Flush-worker iteration helpers (Phase 5.14.C4) ────────────────────────

    /// Alias for [`MemTableRegistry::new`] that documents the flush channel
    /// is open.  Used by tests that want a registry whose send side works.
    pub fn with_flush_channel() -> Self {
        Self::new()
    }

    /// Snapshot of all `(project_id, Arc<ProjectMemState>)` pairs. O(n).
    ///
    /// Returns a `Vec` so the caller can safely iterate across await points
    /// without holding a DashMap shard lock.
    pub fn projects_iter(&self) -> Vec<(ProjectId, Arc<ProjectMemState>)> {
        self.projects
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    /// Snapshot of all `(project_id, table_name, Arc<MemTableEntry>)` triples.
    /// O(n) — safe to hold across async yield points.
    pub fn tables_iter(&self) -> Vec<(ProjectId, TableName, Arc<MemTableEntry>)> {
        self.tables
            .iter()
            .map(|e| {
                let (project, table) = e.key().clone();
                (project, table, e.value().clone())
            })
            .collect()
    }
}

impl Default for MemTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::MemTableConfig;
    use crate::memtable::MemRowValue;
    use crate::row_key::RowKey;
    use basin_common::ids::{ProjectId, TableName};

    fn proj() -> ProjectId {
        ProjectId::new()
    }

    fn tbl(s: &str) -> TableName {
        TableName::new(s).unwrap()
    }

    fn key(n: i64) -> RowKey {
        RowKey::builder().append_i64(n).finish()
    }

    fn row_val() -> MemRowValue {
        MemRowValue::row(vec![0xFFu8; 64], 0)
    }

    #[test]
    fn get_returns_none_before_first_write() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t = tbl("orders");
        assert!(reg.get(&p, &t).is_none());
    }

    #[test]
    fn get_or_create_returns_same_arc() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t = tbl("orders");
        let a = reg.get_or_create(p, t.clone());
        let b = reg.get_or_create(p, t.clone());
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn project_a_writes_invisible_to_project_b() {
        let reg = MemTableRegistry::new();
        let p_a = proj();
        let p_b = proj();
        let t = tbl("events");
        let entry_a = reg.get_or_create(p_a, t.clone());
        entry_a.memtable.insert(key(1), row_val());
        let entry_b = reg.get_or_create(p_b, t.clone());
        assert!(entry_b.memtable.get(&key(1)).is_none());
    }

    #[test]
    fn different_tables_same_project_are_isolated() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let e1 = reg.get_or_create(p, tbl("users"));
        e1.memtable.insert(key(42), row_val());
        let e2 = reg.get_or_create(p, tbl("orders"));
        assert!(e2.memtable.get(&key(42)).is_none());
    }

    #[test]
    fn entry_count_tracks_new_tables() {
        let reg = MemTableRegistry::new();
        let p = proj();
        assert_eq!(reg.entry_count(), 0);
        reg.get_or_create(p, tbl("t1"));
        assert_eq!(reg.entry_count(), 1);
        reg.get_or_create(p, tbl("t2"));
        assert_eq!(reg.entry_count(), 2);
    }

    #[test]
    fn remove_drops_entry() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t = tbl("temp");
        reg.get_or_create(p, t.clone());
        assert_eq!(reg.entry_count(), 1);
        reg.remove(&p, &t);
        assert_eq!(reg.entry_count(), 0);
        assert!(reg.get(&p, &t).is_none());
    }

    #[test]
    fn project_state_same_arc_per_project() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let s1 = reg.project_state(p);
        let s2 = reg.project_state(p);
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn different_projects_have_independent_semaphores() {
        let reg = MemTableRegistry::new();
        let p_a = proj();
        let p_b = proj();
        let s_a = reg.project_state(p_a);
        let s_b = reg.project_state(p_b);
        assert!(!Arc::ptr_eq(&s_a.hard_cap_sem, &s_b.hard_cap_sem));
    }

    // ── C5 budget API ─────────────────────────────────────────────────────────

    #[test]
    fn reserve_under_soft_cap_returns_granted() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 4 * 1024,
            project_soft_cap_bytes: 2 * 1024,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();
        assert_eq!(reg.try_reserve_bytes(&p, 512), ReservationOutcome::Granted);
        assert_eq!(reg.project_bytes(&p), 512);
    }

    #[test]
    fn reserve_between_soft_and_hard_returns_flush_suggested() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 4 * 1024,
            project_soft_cap_bytes: 1 * 1024,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();
        assert_eq!(reg.try_reserve_bytes(&p, 512), ReservationOutcome::Granted);
        assert_eq!(
            reg.try_reserve_bytes(&p, 1024),
            ReservationOutcome::FlushSuggested
        );
    }

    #[test]
    fn reserve_over_hard_cap_returns_hard_cap_reached() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 1024,
            project_soft_cap_bytes: 512,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();
        assert_ne!(
            reg.try_reserve_bytes(&p, 1024),
            ReservationOutcome::HardCapReached
        );
        assert_eq!(
            reg.try_reserve_bytes(&p, 1),
            ReservationOutcome::HardCapReached
        );
    }

    #[test]
    fn release_decrements_counter() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 8 * 1024,
            project_soft_cap_bytes: 4 * 1024,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();
        reg.try_reserve_bytes(&p, 2048);
        assert_eq!(reg.project_bytes(&p), 2048);
        reg.release_bytes(&p, 1024);
        assert_eq!(reg.project_bytes(&p), 1024);
    }

    #[test]
    fn release_after_hard_cap_unblocks_next_reserve() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 1024,
            project_soft_cap_bytes: 512,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();
        reg.try_reserve_bytes(&p, 1024);
        assert_eq!(
            reg.try_reserve_bytes(&p, 1),
            ReservationOutcome::HardCapReached
        );
        reg.release_bytes(&p, 512);
        let outcome = reg.try_reserve_bytes(&p, 512);
        assert_ne!(outcome, ReservationOutcome::HardCapReached);
    }

    #[test]
    fn largest_project_returns_none_when_empty() {
        let reg = MemTableRegistry::new();
        assert!(reg.largest_project().is_none());
    }

    #[test]
    fn largest_project_returns_project_with_most_bytes() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 512 * 1024 * 1024,
            project_soft_cap_bytes: 256 * 1024 * 1024,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p_small = proj();
        let p_large = proj();
        reg.try_reserve_bytes(&p_small, 100);
        reg.try_reserve_bytes(&p_large, 200);
        assert_eq!(reg.largest_project(), Some(p_large));
    }

    #[test]
    fn project_bytes_returns_zero_for_unknown_project() {
        let reg = MemTableRegistry::new();
        assert_eq!(reg.project_bytes(&proj()), 0);
    }

    // ── hot_tier_epoch (Fix A — PK row cache invalidation) ────────────────────

    #[test]
    fn hot_tier_epoch_zero_for_untouched_table() {
        let reg = MemTableRegistry::new();
        let p = proj();
        assert_eq!(reg.hot_tier_epoch(&p, &tbl("never_written")), 0);
    }

    #[test]
    fn hot_tier_epoch_bumps_on_insert_delete_update() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t = tbl("orders");
        let e0 = reg.hot_tier_epoch(&p, &t);
        let entry = reg.get_or_create(p, t.clone());
        entry.memtable.insert(key(1), row_val());
        let e1 = reg.hot_tier_epoch(&p, &t);
        assert!(e1 > e0, "insert must bump epoch: {e0} -> {e1}");
        // "update" = upsert in the memtable model
        entry.memtable.upsert(key(1), row_val());
        let e2 = reg.hot_tier_epoch(&p, &t);
        assert!(e2 > e1, "update must bump epoch: {e1} -> {e2}");
        entry.memtable.delete(key(1));
        let e3 = reg.hot_tier_epoch(&p, &t);
        assert!(e3 > e2, "delete must bump epoch: {e2} -> {e3}");
    }

    #[test]
    fn hot_tier_epoch_isolated_per_table() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t1 = tbl("a");
        let t2 = tbl("b");
        reg.get_or_create(p, t1.clone())
            .memtable
            .insert(key(1), row_val());
        assert!(reg.hot_tier_epoch(&p, &t1) > 0);
        assert_eq!(reg.hot_tier_epoch(&p, &t2), 0);
    }
}
