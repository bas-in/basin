//! [`MemTable`] — per-`(project, table)` in-memory row buffer.
//!
//! Backed by `parking_lot::RwLock<BTreeMap<RowKey, MemRow>>`.  The lock is
//! held only for O(log n) BTree operations and never across any I/O.  A
//! `schema_version: u32` field on every row enables schema-evolution-safe reads
//! as specified in ADR 0016 addendum 2026-05-19.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::row_key::RowKey;

// ── Value types ───────────────────────────────────────────────────────────────

/// The value stored per primary key in the memtable.
#[derive(Clone, Debug)]
pub enum MemRowValue {
    /// Arrow IPC-encoded single-row `RecordBatch` + the schema version at
    /// write time (ADR 0016 schema-evolution addendum).
    Row {
        /// Arrow IPC wire encoding of a single-row `RecordBatch`.
        bytes: Vec<u8>,
        /// Schema version at insert time.  The merge reader applies the
        /// `SchemaDelta` chain from `schema_version` → current to fill in any
        /// added/dropped columns introduced by subsequent `ALTER TABLE` DDL.
        schema_version: u32,
    },
    /// The row has been deleted.  Tombstones suppress matching rows returned
    /// from the Vortex cold tier during read-merge (C3).
    Tombstone,
}

impl MemRowValue {
    /// Construct a `Row` variant from pre-encoded IPC bytes.
    pub fn row(bytes: Vec<u8>, schema_version: u32) -> Self {
        Self::Row { bytes, schema_version }
    }

    /// Return `true` if this is a live row (not a tombstone).
    pub fn is_row(&self) -> bool {
        matches!(self, Self::Row { .. })
    }

    /// Return `true` if this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }

    /// Byte size for memory accounting purposes.  Tombstones are zero.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::Row { bytes, .. } => bytes.len(),
            Self::Tombstone => 0,
        }
    }
}

// ── MemTable ─────────────────────────────────────────────────────────────────

/// In-memory row buffer for a single `(project_id, table_name)` pair.
///
/// Thread-safe via `parking_lot::RwLock`.  All mutations are O(log n).
/// The lock is **never** held across I/O.
pub struct MemTable {
    inner: RwLock<BTreeMap<RowKey, MemRowValue>>,
    /// Running estimate of heap bytes consumed by live `Row` values.
    /// Updated atomically on every write so callers can check caps without
    /// acquiring the lock.
    bytes_allocated: AtomicU64,
}

impl MemTable {
    /// Construct an empty memtable.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            bytes_allocated: AtomicU64::new(0),
        }
    }

    /// Insert a new row.  If a row already exists under `key` it is replaced
    /// (last-write-wins, matching Basin's snapshot-isolation model).
    pub fn insert(&self, key: RowKey, value: MemRowValue) {
        let new_bytes = value.heap_bytes() as u64;
        let mut map = self.inner.write();
        let old_bytes = map.get(&key).map(|v| v.heap_bytes() as u64).unwrap_or(0);
        map.insert(key, value);
        // Saturating arithmetic avoids underflow on pathological usage.
        let cur = self.bytes_allocated.load(Ordering::Relaxed);
        self.bytes_allocated
            .store(cur.saturating_add(new_bytes).saturating_sub(old_bytes), Ordering::Relaxed);
    }

    /// Upsert a row: insert or overwrite.  Same semantics as `insert`.
    #[inline]
    pub fn upsert(&self, key: RowKey, value: MemRowValue) {
        self.insert(key, value);
    }

    /// Write a `Tombstone` for `key`.  Subsequent reads will see no row for
    /// this key; the tombstone suppresses any matching cold-tier row during
    /// read-merge.
    pub fn delete(&self, key: RowKey) {
        let mut map = self.inner.write();
        let old_bytes = map.get(&key).map(|v| v.heap_bytes() as u64).unwrap_or(0);
        map.insert(key, MemRowValue::Tombstone);
        self.bytes_allocated
            .fetch_sub(old_bytes.min(self.bytes_allocated.load(Ordering::Relaxed)), Ordering::Relaxed);
    }

    /// Point lookup.  Returns `None` if the key has never been written.
    /// Returns `Some(MemRowValue::Tombstone)` if the row was deleted.
    pub fn get(&self, key: &RowKey) -> Option<MemRowValue> {
        self.inner.read().get(key).cloned()
    }

    /// Range scan: returns all `(RowKey, MemRowValue)` pairs where
    /// `lo <= key <= hi`.  The returned vector is in ascending key order
    /// (BTreeMap guarantees this).
    pub fn range_scan(&self, lo: &RowKey, hi: &RowKey) -> Vec<(RowKey, MemRowValue)> {
        use std::ops::Bound::Included;
        let map = self.inner.read();
        map.range((Included(lo), Included(hi)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Full snapshot of all entries (including tombstones).  Used by the flush
    /// task (C4) to clone a generation-bounded view before I/O.  The lock is
    /// released before returning.
    pub fn snapshot(&self) -> Vec<(RowKey, MemRowValue)> {
        let map = self.inner.read();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Number of live rows (excludes tombstones).
    pub fn live_row_count(&self) -> usize {
        self.inner.read().values().filter(|v| v.is_row()).count()
    }

    /// Total row count including tombstones.
    pub fn total_count(&self) -> usize {
        self.inner.read().len()
    }

    /// Current byte estimate for live rows.  Cheap — atomic load, no lock.
    pub fn bytes_allocated(&self) -> u64 {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    /// Remove all entries whose keys are in `keys`.  Used by the flush task
    /// (C4) after a successful cold-tier commit to GC flushed rows.  The write
    /// lock is held only for the duration of the removal loop.
    pub fn remove_flushed(&self, keys: &[RowKey]) {
        if keys.is_empty() {
            return;
        }
        let mut map = self.inner.write();
        let mut freed: u64 = 0;
        for k in keys {
            if let Some(v) = map.remove(k) {
                freed += v.heap_bytes() as u64;
            }
        }
        let cur = self.bytes_allocated.load(Ordering::Relaxed);
        self.bytes_allocated.store(cur.saturating_sub(freed), Ordering::Relaxed);
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_key::RowKey;

    fn key(n: i64) -> RowKey {
        RowKey::builder().append_i64(n).finish()
    }

    fn row(n: u8) -> MemRowValue {
        MemRowValue::row(vec![n; 32], 0)
    }

    // ── insert / get ─────────────────────────────────────────────────────────

    #[test]
    fn insert_and_get_roundtrip() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0xAA));
        let v = mt.get(&key(1)).expect("row must be present");
        assert!(v.is_row());
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mt = MemTable::new();
        assert!(mt.get(&key(99)).is_none());
    }

    // ── delete / tombstone ────────────────────────────────────────────────────

    #[test]
    fn delete_produces_tombstone() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        mt.delete(key(1));
        let v = mt.get(&key(1)).expect("tombstone must be present after delete");
        assert!(v.is_tombstone(), "expected Tombstone, got {:?}", v);
    }

    #[test]
    fn tombstone_on_unseen_key() {
        let mt = MemTable::new();
        mt.delete(key(42));
        assert!(mt.get(&key(42)).unwrap().is_tombstone());
    }

    // ── upsert ───────────────────────────────────────────────────────────────

    #[test]
    fn upsert_overwrites_existing_row() {
        let mt = MemTable::new();
        mt.upsert(key(1), row(0x01));
        mt.upsert(key(1), row(0x02));
        match mt.get(&key(1)).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0x02),
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn upsert_over_tombstone_reinstates_row() {
        let mt = MemTable::new();
        mt.delete(key(5));
        mt.upsert(key(5), row(0xBB));
        assert!(mt.get(&key(5)).unwrap().is_row());
    }

    // ── schema_version ────────────────────────────────────────────────────────

    #[test]
    fn schema_version_preserved() {
        let mt = MemTable::new();
        mt.insert(key(7), MemRowValue::row(vec![0u8; 16], 3));
        match mt.get(&key(7)).unwrap() {
            MemRowValue::Row { schema_version, .. } => assert_eq!(schema_version, 3),
            _ => panic!("expected Row"),
        }
    }

    // ── bytes accounting ──────────────────────────────────────────────────────

    #[test]
    fn bytes_accounting_insert() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0));
        assert_eq!(mt.bytes_allocated(), 100);
    }

    #[test]
    fn bytes_accounting_decreases_on_delete() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0));
        mt.delete(key(1));
        assert_eq!(mt.bytes_allocated(), 0);
    }

    #[test]
    fn bytes_accounting_upsert_adjusts_delta() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0));
        mt.upsert(key(1), MemRowValue::row(vec![0u8; 200], 0));
        assert_eq!(mt.bytes_allocated(), 200);
    }

    // ── range_scan ────────────────────────────────────────────────────────────

    #[test]
    fn range_scan_returns_ordered_subset() {
        let mt = MemTable::new();
        for i in 0..10i64 {
            mt.insert(key(i), row(i as u8));
        }
        let results = mt.range_scan(&key(3), &key(6));
        assert_eq!(results.len(), 4); // 3,4,5,6
        for (k, _) in &results {
            let raw = u64::from_be_bytes(k.as_bytes()[..8].try_into().unwrap());
            let v = (raw ^ 0x8000_0000_0000_0000) as i64;
            assert!((3..=6).contains(&v));
        }
    }

    // ── remove_flushed ────────────────────────────────────────────────────────

    #[test]
    fn remove_flushed_clears_entries() {
        let mt = MemTable::new();
        for i in 0..5i64 {
            mt.insert(key(i), row(i as u8));
        }
        let to_flush: Vec<_> = (0..3).map(key).collect();
        mt.remove_flushed(&to_flush);
        assert_eq!(mt.total_count(), 2);
        assert!(mt.get(&key(0)).is_none());
        assert!(mt.get(&key(3)).is_some());
    }

    // ── multi-tenant isolation ────────────────────────────────────────────────

    #[test]
    fn separate_memtables_are_isolated() {
        let mt_a = MemTable::new();
        let mt_b = MemTable::new();

        mt_a.insert(key(1), row(0xAA));
        mt_b.insert(key(1), row(0xBB));

        match mt_a.get(&key(1)).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0xAA),
            _ => panic!("expected Row in A"),
        }
        match mt_b.get(&key(1)).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0xBB),
            _ => panic!("expected Row in B"),
        }
    }
}
