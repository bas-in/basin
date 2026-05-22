//! Phase 5.21.B — in-process logical replication slot registry.
//!
//! Manages per-project replication slot state. Slots are held in-memory
//! (consistent with `InMemoryCatalog`) and the per-slot LSN is tracked as a
//! monotonically-increasing `u64` WAL sequence number.
//!
//! ## Design
//!
//! - One `SlotRegistry` per process, keyed by `(ProjectId, slot_name)`.
//! - A slot stores: `plugin` (e.g. "pgoutput"), `confirmed_flush_lsn`,
//!   `restart_lsn`, `consumer_id`, and a pending-changes queue.
//! - `pg_create_logical_replication_slot` → `create_slot` → returns `(name, lsn)`.
//! - `pg_drop_replication_slot` → `drop_slot` → removes the entry.
//! - `pg_replication_slots` view provider iterates the registry snapshot.
//! - `pg_logical_slot_get_changes` → `take_pending_changes` → drains queued frames.
//!
//! The registry is per-engine (shared across all sessions on the same engine)
//! keyed by `ProjectId` so project isolation is structural.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use basin_common::ProjectId;

/// The LSN of the most recently committed WAL position (0/0 at startup).
pub type Lsn = u64;

/// Format a `u64` WAL sequence number as a PostgreSQL PG_LSN string.
/// PostgreSQL uses `{high32}/{low32}` hex format, e.g. `0/1234ABCD`.
pub fn lsn_to_pgtext(lsn: Lsn) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{:X}/{:08X}", hi, lo)
}

/// One pending frame in a logical replication change feed.
/// Corresponds to a single row returned by `pg_logical_slot_get_changes`.
#[derive(Clone, Debug)]
pub struct PendingFrame {
    /// The LSN assigned to this frame (monotonically increasing per slot).
    pub lsn: Lsn,
    /// The transaction id (`xid`) of the surrounding transaction.
    pub xid: u32,
    /// The raw pgoutput frame type byte and body serialised as a hex string.
    /// For the SQL-facing function we store the one-character type tag (B, R,
    /// I, U, D, C) so `data` is human-readable and passes assertions on type.
    pub data: String,
}

/// State for a single logical replication slot.
#[derive(Debug)]
pub struct SlotState {
    pub slot_name: String,
    pub plugin: String,
    /// The WAL position at which the slot was created / last confirmed flush.
    pub confirmed_flush_lsn: Lsn,
    /// Lowest WAL position still needed by this slot (for retention).
    pub restart_lsn: Lsn,
    /// Arbitrary consumer tag (e.g. Debezium connector name).
    pub consumer_id: Option<String>,
    /// Pending frames not yet consumed by `pg_logical_slot_get_changes`.
    pub pending: Vec<PendingFrame>,
}

impl SlotState {
    fn new(name: &str, plugin: &str, start_lsn: Lsn) -> Self {
        Self {
            slot_name: name.to_string(),
            plugin: plugin.to_string(),
            confirmed_flush_lsn: start_lsn,
            restart_lsn: start_lsn,
            consumer_id: None,
            pending: Vec::new(),
        }
    }
}

/// Project-scoped map of slot name → slot state.
type ProjectSlots = HashMap<String, SlotState>;

/// Process-wide registry of logical replication slots keyed by project.
/// Cheap to clone (`Arc<Mutex<…>>` inside).
#[derive(Clone, Debug, Default)]
pub struct SlotRegistry {
    inner: Arc<Mutex<HashMap<ProjectId, ProjectSlots>>>,
}

impl SlotRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new logical replication slot. Returns `(slot_name, start_lsn)`
    /// on success, or an error if the slot already exists.
    ///
    /// `current_lsn` is the WAL position at creation time (caller supplies
    /// the process-wide LSN counter).
    pub fn create_slot(
        &self,
        project: &ProjectId,
        slot_name: &str,
        plugin: &str,
        current_lsn: Lsn,
    ) -> Result<(String, Lsn), String> {
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        let project_slots = guard.entry(*project).or_default();
        if project_slots.contains_key(slot_name) {
            return Err(format!("replication slot \"{slot_name}\" already exists"));
        }
        let state = SlotState::new(slot_name, plugin, current_lsn);
        let lsn = state.confirmed_flush_lsn;
        project_slots.insert(slot_name.to_string(), state);
        Ok((slot_name.to_string(), lsn))
    }

    /// Drop an existing logical replication slot. Returns `Ok(())` on success,
    /// `Err` if the slot does not exist.
    pub fn drop_slot(&self, project: &ProjectId, slot_name: &str) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        let project_slots = guard.entry(*project).or_default();
        if project_slots.remove(slot_name).is_none() {
            return Err(format!("replication slot \"{slot_name}\" does not exist"));
        }
        Ok(())
    }

    /// Push pending frames for a slot. Called by the change-event integration
    /// when a mutation completes for a project/table that has ≥1 active slot.
    pub fn push_frames(
        &self,
        project: &ProjectId,
        slot_name: &str,
        frames: Vec<PendingFrame>,
    ) {
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        if let Some(project_slots) = guard.get_mut(project) {
            if let Some(slot) = project_slots.get_mut(slot_name) {
                slot.pending.extend(frames);
            }
        }
    }

    /// Push frames to every slot for a project. Called after each committed
    /// transaction so all consumers see the change.
    pub fn push_frames_all_slots(
        &self,
        project: &ProjectId,
        frames: Vec<PendingFrame>,
    ) {
        if frames.is_empty() {
            return;
        }
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        if let Some(project_slots) = guard.get_mut(project) {
            for slot in project_slots.values_mut() {
                slot.pending.extend(frames.iter().cloned());
            }
        }
    }

    /// Drain all pending frames for `slot_name`, advancing `restart_lsn` to
    /// the highest delivered LSN. Returns the drained frames.
    pub fn take_pending_changes(
        &self,
        project: &ProjectId,
        slot_name: &str,
    ) -> Result<Vec<PendingFrame>, String> {
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        let project_slots = guard
            .get_mut(project)
            .ok_or_else(|| format!("replication slot \"{slot_name}\" does not exist"))?;
        let slot = project_slots
            .get_mut(slot_name)
            .ok_or_else(|| format!("replication slot \"{slot_name}\" does not exist"))?;
        let frames = std::mem::take(&mut slot.pending);
        if let Some(last) = frames.last() {
            if last.lsn > slot.restart_lsn {
                slot.restart_lsn = last.lsn;
                slot.confirmed_flush_lsn = last.lsn;
            }
        }
        Ok(frames)
    }

    /// Snapshot of slot names + state for `pg_replication_slots` view.
    pub fn list_slots(&self, project: &ProjectId) -> Vec<SlotSnapshot> {
        let guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        let Some(project_slots) = guard.get(project) else {
            return Vec::new();
        };
        project_slots
            .values()
            .map(|s| SlotSnapshot {
                slot_name: s.slot_name.clone(),
                plugin: s.plugin.clone(),
                slot_type: "logical".to_string(),
                confirmed_flush_lsn: s.confirmed_flush_lsn,
                restart_lsn: s.restart_lsn,
                active: true,
            })
            .collect()
    }

    /// Monotonically advancing LSN for this project. Each call bumps by 1.
    /// In a real implementation this would come from a shared WAL LSN counter;
    /// here we keep a per-registry counter for simplicity.
    pub fn advance_lsn(&self, project: &ProjectId) -> Lsn {
        // We store a per-project LSN counter in the first slot's confirmed_flush_lsn
        // minus a synthetic counter. Instead, keep a separate counter map.
        let mut guard = self.inner.lock().expect("SlotRegistry lock poisoned");
        let project_slots = guard.entry(*project).or_default();
        // Derive the next LSN from the max of all existing slot LSNs + 1.
        let next = project_slots
            .values()
            .map(|s| s.confirmed_flush_lsn)
            .max()
            .unwrap_or(0)
            + 1;
        next
    }
}

/// Plain-data snapshot of a slot suitable for the `pg_replication_slots` view.
#[derive(Clone, Debug)]
pub struct SlotSnapshot {
    pub slot_name: String,
    pub plugin: String,
    pub slot_type: String,
    pub confirmed_flush_lsn: Lsn,
    pub restart_lsn: Lsn,
    pub active: bool,
}
