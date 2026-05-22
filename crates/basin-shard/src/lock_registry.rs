//! Process-wide lock registry for `pg_locks` view support (Phase 5.23.D).
//!
//! ## Design
//!
//! Basin uses optimistic concurrency with single-writer-per-shard semantics:
//! writers serialise per `(project, partition)` — there is no row-level
//! lock manager. This module exposes the *observable* lock state at the
//! granularity that `pg_locks` consumers care about:
//!
//! - **Relation locks**: a relation-level `RowExclusiveLock` is registered
//!   when a session begins a transaction that touches a table (INSERT /
//!   UPDATE / DELETE / SELECT FOR UPDATE). Released on COMMIT or ROLLBACK.
//! - **Advisory locks**: advisory session-scoped and transaction-scoped locks
//!   (`pg_advisory_lock` / `pg_try_advisory_lock`) land here too (v0.2 hook).
//!
//! ## Composition with `lock_wait.rs`
//!
//! [`crate::lock_wait::bounded_lock_wait`] is the *async* wait primitive for
//! lock acquisition timeouts (Phase 5.28.B). `LockRegistry` is the *observer*:
//! it records what is currently held so that `pg_locks` queries can read the
//! snapshot. The two are complementary:
//!
//! - `bounded_lock_wait` drives the acquisition loop (retries + deadline).
//! - `LockRegistry` is updated *after* acquisition succeeds (or on release).
//!
//! Waiters (locks not yet granted) are also tracked so that `pg_locks` can
//! surface `granted = false` rows — a prerequisite for lock-wait graph tooling.
//!
//! ## Per-project cost discipline
//!
//! The registry is process-wide (`Arc<LockRegistry>`) with a single
//! `Mutex<HashMap<ProjectId, Vec<LockEntry>>>`. Cost per idle project is
//! O(bytes) — just a missing key in the map. The `Mutex` guard is held only
//! for the duration of a `Vec::push` / `Vec::retain` call; it is never held
//! across an `await` point.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use basin_common::ProjectId;

/// A single lock entry visible via `pg_locks`.
///
/// `locktype`, `mode`, and `granted` mirror PG 15 `pg_locks` column semantics.
/// Other columns (relation, page, tuple, …) are populated for relation-level
/// locks and left `None` for advisory / virtual-xid locks.
#[derive(Clone, Debug)]
pub struct LockEntry {
    /// PG lock type: `"relation"`, `"virtualxid"`, or `"advisory"`.
    pub locktype: String,
    /// OID of the database owning the relation (`None` for advisory locks).
    pub database: Option<i64>,
    /// OID of the relation (table), if applicable.
    pub relation: Option<i64>,
    /// Synthetic "virtual transaction id" for this session (`"1/N"` form).
    pub virtualxid: Option<String>,
    /// Lock mode: `"ExclusiveLock"`, `"RowExclusiveLock"`, `"AccessShareLock"`, …
    pub mode: String,
    /// `true` when the lock is currently held; `false` when waiting.
    pub granted: bool,
    /// Synthetic backend pid (session id). `i32` to match `pg_locks.pid`.
    pub pid: i32,
    /// Table name (convenience, not a PG column — used for filtering).
    pub table_name: Option<String>,
    /// When the entry was recorded (used for internal ordering / debugging).
    pub recorded_at: Instant,
}

impl LockEntry {
    /// Create a granted relation-level lock entry for `table_name` in `project`.
    pub fn relation_lock(
        pid: i32,
        database: i64,
        relation_oid: i64,
        table_name: &str,
        mode: &str,
    ) -> Self {
        Self {
            locktype: "relation".to_string(),
            database: Some(database),
            relation: Some(relation_oid),
            virtualxid: None,
            mode: mode.to_string(),
            granted: true,
            pid,
            table_name: Some(table_name.to_string()),
            recorded_at: Instant::now(),
        }
    }

    /// Create a granted virtualxid lock (every session holds one while active).
    pub fn virtualxid_lock(pid: i32, vtxid: &str) -> Self {
        Self {
            locktype: "virtualxid".to_string(),
            database: None,
            relation: None,
            virtualxid: Some(vtxid.to_string()),
            mode: "ExclusiveLock".to_string(),
            granted: true,
            pid,
            table_name: None,
            recorded_at: Instant::now(),
        }
    }

    /// Create a granted advisory lock entry.
    ///
    /// `key` is the i64 advisory lock key; `pid` is the session's synthetic
    /// backend pid. `granted` should be `true` once the lock is held, `false`
    /// while waiting (for `pg_locks` observability of lock wait queues).
    pub fn advisory_lock(pid: i32, key: i64, granted: bool) -> Self {
        Self {
            locktype: "advisory".to_string(),
            database: None,
            relation: None,
            virtualxid: None,
            mode: "ExclusiveLock".to_string(),
            granted,
            pid,
            table_name: None,
            recorded_at: Instant::now(),
        }
    }
}

/// Process-wide, project-scoped lock registry.
///
/// Cheap to clone — just an `Arc` ref-count bump.
#[derive(Clone, Default, Debug)]
pub struct LockRegistry {
    /// `project → lock entries` map protected by a `Mutex`.
    ///
    /// The `Mutex` is `std::sync` (not `tokio`) because all operations are
    /// short (Vec push / retain) and never cross an `await` boundary.
    inner: Arc<Mutex<HashMap<ProjectId, Vec<LockEntry>>>>,
}

impl LockRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `entry` for `project`. Returns a `LockHandle` that removes the
    /// entry when dropped — RAII-style release on COMMIT / ROLLBACK / session drop.
    ///
    /// # Panics
    ///
    /// Panics only if the internal `Mutex` is poisoned (i.e. a previous holder
    /// panicked while holding it — a process-fatal condition).
    pub fn acquire(&self, project: &ProjectId, entry: LockEntry) -> LockHandle {
        let id = entry.recorded_at; // use Instant as a unique-enough key within a project
        {
            let mut map = self.inner.lock().expect("LockRegistry mutex poisoned");
            map.entry(*project).or_default().push(entry);
        }
        LockHandle {
            registry: self.inner.clone(),
            project: *project,
            recorded_at: id,
        }
    }

    /// Snapshot all lock entries for `project`.
    ///
    /// Called from `PgLocksLiveProvider::scan()` — holds the lock only for the
    /// `clone()` call, then releases immediately.
    pub fn snapshot(&self, project: &ProjectId) -> Vec<LockEntry> {
        let map = self.inner.lock().expect("LockRegistry mutex poisoned");
        map.get(project).cloned().unwrap_or_default()
    }

    /// Remove all entries for `project` (e.g. after session teardown).
    ///
    /// Prefer `LockHandle::drop` for individual entry release. This is a bulk
    /// emergency cleanup path (session crash / force-close).
    pub fn clear_project(&self, project: &ProjectId) {
        let mut map = self.inner.lock().expect("LockRegistry mutex poisoned");
        map.remove(project);
    }
}

/// RAII guard that removes a single `LockEntry` from the registry when dropped.
///
/// Obtained from [`LockRegistry::acquire`]; dropped on COMMIT / ROLLBACK / when
/// the owning `ProjectSession` is dropped. The `recorded_at` field (an
/// `Instant`) serves as the unique key within a project's entry list since two
/// `Instant`s for different `acquire` calls will differ.
#[derive(Debug)]
pub struct LockHandle {
    registry: Arc<Mutex<HashMap<ProjectId, Vec<LockEntry>>>>,
    project: ProjectId,
    recorded_at: Instant,
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            if let Some(entries) = map.get_mut(&self.project) {
                entries.retain(|e| e.recorded_at != self.recorded_at);
                if entries.is_empty() {
                    map.remove(&self.project);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    #[test]
    fn acquire_and_snapshot() {
        let reg = LockRegistry::new();
        let project = ProjectId::new();

        assert!(reg.snapshot(&project).is_empty(), "no locks initially");

        let handle = reg.acquire(
            &project,
            LockEntry::relation_lock(1, 100, 200, "my_table", "RowExclusiveLock"),
        );

        let snap = reg.snapshot(&project);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].locktype, "relation");
        assert_eq!(snap[0].mode, "RowExclusiveLock");
        assert!(snap[0].granted);

        drop(handle);
        assert!(reg.snapshot(&project).is_empty(), "released on drop");
    }

    #[test]
    fn multiple_entries() {
        let reg = LockRegistry::new();
        let project = ProjectId::new();

        let h1 = reg.acquire(
            &project,
            LockEntry::virtualxid_lock(1, "1/1"),
        );
        let h2 = reg.acquire(
            &project,
            LockEntry::relation_lock(1, 10, 20, "orders", "ExclusiveLock"),
        );

        assert_eq!(reg.snapshot(&project).len(), 2);

        drop(h1);
        assert_eq!(reg.snapshot(&project).len(), 1);
        assert_eq!(reg.snapshot(&project)[0].locktype, "relation");

        drop(h2);
        assert!(reg.snapshot(&project).is_empty());
    }

    #[test]
    fn two_projects_isolated() {
        let reg = LockRegistry::new();
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        let _h1 = reg.acquire(&p1, LockEntry::virtualxid_lock(1, "1/1"));
        let _h2 = reg.acquire(&p2, LockEntry::virtualxid_lock(2, "1/2"));

        // Each project sees only its own entries.
        assert_eq!(reg.snapshot(&p1).len(), 1);
        assert_eq!(reg.snapshot(&p2).len(), 1);
        assert_ne!(reg.snapshot(&p1)[0].pid, reg.snapshot(&p2)[0].pid);
    }
}
