//! [`MemTableRegistry`] — process-wide, multi-tenant memtable registry.
//!
//! # Isolation model
//!
//! Follows the shared-heavy-resource + cheap-per-tenant-primitive pattern
//! from `basin-common::ProjectCounterRegistry`:
//!
//! - **Shared heavy resource**: one `DashMap<(ProjectId, TableName),
//!   Arc<MemTableEntry>>`.  Allocation is lazy: only projects that have
//!   received at least one write consume an entry.  Inactive tenants cost zero.
//! - **Cheap per-tenant primitive**: one `AtomicU64` byte counter + one
//!   `Arc<tokio::sync::Semaphore>` per project.  The semaphore models the
//!   `project_memtable_hard_cap` back-pressure mechanism (C5); at Phase C1 it
//!   is initialised but not yet gated on writes — that wiring lives in C5.
//!
//! # Cost model
//!
//! At 10 000 inactive tenants the registry is a 10 000-entry `DashMap` with
//! zero-byte counters — sub-MB overhead total.  Per-project semaphores are
//! allocated only on first write.

use std::sync::Arc;

use basin_common::ids::{ProjectId, TableName};
use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::memtable::MemTable;

// ── Memory-budget constants ────────────────────────────────────────────────

/// Default project-level hard cap (256 MiB). Blocks new writes via the
/// per-project semaphore when exceeded.  Configurable via `ALTER PROJECT`
/// (Phase 5.14.C5).
pub const PROJECT_MEMTABLE_HARD_CAP_BYTES: u64 = 256 * 1024 * 1024;

/// Default project-level soft cap (192 MiB = 75% of hard cap). Triggers a
/// background flush of the project's largest single-table memtable (C4).
pub const PROJECT_MEMTABLE_SOFT_CAP_BYTES: u64 = 192 * 1024 * 1024;

/// Default per-table soft cap (16 MiB). Triggers a per-table flush (C4).
pub const TABLE_MEMTABLE_SOFT_CAP_BYTES: u64 = 16 * 1024 * 1024;

/// Default age-based flush threshold in seconds (C4).
pub const MEMTABLE_MAX_AGE_SECS: u64 = 60;

// ── MemTableEntry ─────────────────────────────────────────────────────────

/// One entry in the registry: the live [`MemTable`] for a `(project, table)`
/// pair.
pub struct MemTableEntry {
    pub memtable: MemTable,
}

impl MemTableEntry {
    fn new() -> Self {
        Self {
            memtable: MemTable::new(),
        }
    }
}

// ── Per-project state ─────────────────────────────────────────────────────

/// State lazily allocated per active project.
pub struct ProjectMemState {
    /// Monotonically increasing; updated by every write across all tables in
    /// this project.  Read by the C5 budget-enforcement path without acquiring
    /// any other lock.
    pub bytes_allocated: std::sync::atomic::AtomicU64,
    /// Semaphore modelling the project hard cap.  Capacity is set to
    /// `PROJECT_MEMTABLE_HARD_CAP_BYTES` (in whole bytes) at construction
    /// time.  C5 will acquire `row_bytes` permits before each write and
    /// release them after flush.  At Phase C1 it is constructed but not yet
    /// gate-checked on writes.
    pub hard_cap_sem: Arc<Semaphore>,
}

impl ProjectMemState {
    fn new() -> Self {
        // Semaphore capacity is u32 on most platforms (max ~2B).  We cap at the
        // hard cap in MiB-units so the semaphore fits: 256 MiB = 268 435 456 <
        // u32::MAX.
        let sem_permits = PROJECT_MEMTABLE_HARD_CAP_BYTES as usize;
        Self {
            bytes_allocated: std::sync::atomic::AtomicU64::new(0),
            hard_cap_sem: Arc::new(Semaphore::new(sem_permits)),
        }
    }
}

// ── MemTableRegistry ─────────────────────────────────────────────────────

/// Process-wide registry of per-`(project, table)` memtables.
///
/// Designed for one singleton per process (wrap in `OnceLock<MemTableRegistry>`
/// or embed in a shard state struct).  All operations are thread-safe.
pub struct MemTableRegistry {
    /// Per-`(project, table)` memtable entries.  `DashMap` shards the
    /// underlying RwLock into ~16 stripes so writers for different
    /// `(project, table)` pairs rarely contend.
    tables: DashMap<(ProjectId, TableName), Arc<MemTableEntry>>,
    /// Per-project allocation state.  Allocated lazily on first write from a
    /// project.
    projects: DashMap<ProjectId, Arc<ProjectMemState>>,
}

impl MemTableRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            projects: DashMap::new(),
        }
    }

    /// Get-or-create the [`MemTableEntry`] for `(project, table)`.
    ///
    /// Guaranteed to return the same `Arc` for the same key across concurrent
    /// callers — `DashMap::entry` serialises the creation race.
    pub fn get_or_create(
        &self,
        project: ProjectId,
        table: TableName,
    ) -> Arc<MemTableEntry> {
        self.tables
            .entry((project, table))
            .or_insert_with(|| Arc::new(MemTableEntry::new()))
            .clone()
    }

    /// Look up an existing entry without creating one.
    pub fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Option<Arc<MemTableEntry>> {
        self.tables.get(&(*project, table.clone())).map(|e| e.clone())
    }

    /// Get-or-create per-project state (byte counter + semaphore).
    pub fn project_state(&self, project: ProjectId) -> Arc<ProjectMemState> {
        self.projects
            .entry(project)
            .or_insert_with(|| Arc::new(ProjectMemState::new()))
            .clone()
    }

    /// Total number of `(project, table)` entries (including empty memtables).
    pub fn entry_count(&self) -> usize {
        self.tables.len()
    }

    /// Sum of `bytes_allocated` across all live memtable entries.
    /// O(n) — only suitable for monitoring / metrics, not hot-path use.
    pub fn total_bytes_allocated(&self) -> u64 {
        self.tables
            .iter()
            .map(|e| e.value().memtable.bytes_allocated())
            .sum()
    }

    /// Remove the entry for `(project, table)` from the registry.
    /// Called after a successful flush + cold-tier commit when the memtable
    /// has been drained to zero rows.
    pub fn remove(&self, project: &ProjectId, table: &TableName) {
        self.tables.remove(&(*project, table.clone()));
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

    // ── lazy allocation ───────────────────────────────────────────────────────

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
        assert!(Arc::ptr_eq(&a, &b), "must return the same Arc for the same key");
    }

    // ── multi-tenant isolation ────────────────────────────────────────────────

    #[test]
    fn project_a_writes_invisible_to_project_b() {
        let reg = MemTableRegistry::new();
        let p_a = proj();
        let p_b = proj();
        let t = tbl("events");

        let entry_a = reg.get_or_create(p_a, t.clone());
        entry_a.memtable.insert(key(1), row_val());

        // Project B has its own separate MemTableEntry.
        let entry_b = reg.get_or_create(p_b, t.clone());
        assert!(
            entry_b.memtable.get(&key(1)).is_none(),
            "project B must not see project A's row"
        );
    }

    #[test]
    fn different_tables_same_project_are_isolated() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let t1 = tbl("users");
        let t2 = tbl("orders");

        let e1 = reg.get_or_create(p, t1.clone());
        e1.memtable.insert(key(42), row_val());

        let e2 = reg.get_or_create(p, t2.clone());
        assert!(
            e2.memtable.get(&key(42)).is_none(),
            "different tables for same project must be isolated"
        );
    }

    // ── entry_count ───────────────────────────────────────────────────────────

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

    // ── remove ────────────────────────────────────────────────────────────────

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

    // ── per-project semaphore ─────────────────────────────────────────────────

    #[test]
    fn project_state_same_arc_per_project() {
        let reg = MemTableRegistry::new();
        let p = proj();
        let s1 = reg.project_state(p);
        let s2 = reg.project_state(p);
        assert!(Arc::ptr_eq(&s1, &s2), "must return same Arc for same project");
    }

    #[test]
    fn different_projects_have_independent_semaphores() {
        let reg = MemTableRegistry::new();
        let p_a = proj();
        let p_b = proj();
        let s_a = reg.project_state(p_a);
        let s_b = reg.project_state(p_b);
        assert!(
            !Arc::ptr_eq(&s_a.hard_cap_sem, &s_b.hard_cap_sem),
            "different projects must have independent semaphores"
        );
    }
}
