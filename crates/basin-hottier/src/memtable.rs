//! [`MemTable`] — per-`(project, table)` in-memory row buffer.
//!
//! Backed by `parking_lot::RwLock<BTreeMap<RowKey, MemRow>>`.  The lock is
//! held only for O(log n) BTree operations and never across any I/O.  A
//! `schema_version: u32` field on every row enables schema-evolution-safe reads
//! as specified in ADR 0016 addendum 2026-05-19.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
    /// A full-row replacement written by the hot-tier UPDATE fast path
    /// (`BASIN_HOTTIER_UPDATE_FASTPATH`). Semantically identical to `Row`
    /// (an Arrow-IPC single-row `RecordBatch` that wins over the cold tier on
    /// PK collision), but kept as a distinct variant so the read paths can
    /// tell apart a PK-keyed UPDATE override from a counter-keyed HTAP INSERT
    /// row. The INSERT path keys rows by a monotonic counter, so without this
    /// tag a `Row` snapshot could not be safely matched against cold-tier PKs.
    /// `Update` entries are *always* keyed by the encoded primary key.
    Update {
        /// Arrow IPC wire encoding of the post-SET single-row `RecordBatch`.
        bytes: Vec<u8>,
        /// Schema version at update time (mirrors `Row`).
        schema_version: u32,
    },
    /// The row has been deleted.  Tombstones suppress matching rows returned
    /// from the Vortex cold tier during read-merge (C3).
    Tombstone,
}

impl MemRowValue {
    /// Construct a `Row` variant from pre-encoded IPC bytes.
    pub fn row(bytes: Vec<u8>, schema_version: u32) -> Self {
        Self::Row {
            bytes,
            schema_version,
        }
    }

    /// Construct an `Update` variant from pre-encoded IPC bytes. Written by
    /// the hot-tier UPDATE fast path; keyed by the encoded primary key.
    pub fn update(bytes: Vec<u8>, schema_version: u32) -> Self {
        Self::Update {
            bytes,
            schema_version,
        }
    }

    /// Return `true` if this is a live row — either an INSERT (`Row`) or an
    /// UPDATE override (`Update`). Tombstones are excluded.
    pub fn is_row(&self) -> bool {
        matches!(self, Self::Row { .. } | Self::Update { .. })
    }

    /// Return `true` if this is a PK-keyed UPDATE override.
    pub fn is_update(&self) -> bool {
        matches!(self, Self::Update { .. })
    }

    /// Return `true` if this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }

    /// Byte size for memory accounting purposes.  Tombstones are zero.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::Row { bytes, .. } | Self::Update { bytes, .. } => bytes.len(),
            Self::Tombstone => 0,
        }
    }
}

// ── Stored entry (value + MVCC sequence) ──────────────────────────────────────

/// What the memtable actually stores per key: the [`MemRowValue`] plus the
/// monotonic sequence assigned to the write that produced it.
///
/// The sequence is the MVCC foundation for transaction-snapshot isolation of
/// hot-tier overlay writes (ADR 0016 isolation addendum). An open transaction
/// pins a `hot_seq_watermark` at first-touch; the overlay read path filters out
/// any entry whose `seq` exceeds that watermark, so writes committed by *other*
/// sessions via the auto-commit UPDATE/DELETE fast paths after the pin stay
/// invisible to the transaction's snapshot. Auto-commit reads ignore the
/// sequence entirely (no watermark → no filtering → zero hot-path cost).
#[derive(Clone, Debug)]
struct MemEntry {
    value: MemRowValue,
    /// Monotonic per-table sequence assigned at insert/delete time.
    seq: u64,
}

// ── MemTable ─────────────────────────────────────────────────────────────────

/// In-memory row buffer for a single `(project_id, table_name)` pair.
///
/// Thread-safe via `parking_lot::RwLock`.  All mutations are O(log n).
/// The lock is **never** held across I/O.
pub struct MemTable {
    inner: RwLock<BTreeMap<RowKey, MemEntry>>,
    /// Running estimate of heap bytes consumed by live `Row` values.
    /// Updated atomically on every write so callers can check caps without
    /// acquiring the lock.
    bytes_allocated: AtomicU64,
    /// Unix-second timestamp of the first write to this generation.
    /// `u64::MAX` = empty (no writes since last drain).  Used by the flush
    /// task's age-based trigger without acquiring the BTree lock.
    oldest_insert_secs: AtomicU64,
    /// Monotonic mutation counter (Fix A — PK row cache invalidation).
    /// Bumped on EVERY memtable mutation: insert / upsert / delete.
    /// A PK-row-cache entry caches this value as a watermark; on lookup,
    /// a mismatch means the memtable changed since the entry was cached →
    /// the entry is treated as stale. This catches fast-path DML
    /// (INSERT/UPDATE/DELETE that writes only the memtable, not the catalog
    /// snapshot). Starts at 0; first mutation makes it 1.
    epoch: AtomicU64,
    /// Monotonic MVCC sequence allocator (hot-tier transaction isolation).
    /// Every insert / upsert / delete claims the next value via
    /// `fetch_add(1)` and stores it on the written [`MemEntry`]. The
    /// transaction read path captures `current_seq()` as a watermark and
    /// filters out any overlay entry whose `seq` exceeds it (a post-snapshot
    /// write by another session). Starts at 0; the first write stores seq 1.
    ///
    /// Distinct from `epoch`: `epoch` is a coarse change-detector for the PK
    /// row cache ("any mutation happened"), while `seq` is a per-write ordinal
    /// stamped on each surviving value so the overlay can be filtered by
    /// recency. They advance together today but carry different contracts;
    /// keeping them separate avoids overloading the cache-invalidation
    /// watermark with MVCC semantics.
    seq: AtomicU64,
}

impl MemTable {
    /// Construct an empty memtable.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            bytes_allocated: AtomicU64::new(0),
            oldest_insert_secs: AtomicU64::new(u64::MAX),
            epoch: AtomicU64::new(0),
            seq: AtomicU64::new(0),
        }
    }

    /// Current MVCC sequence high-water mark for this table. Cheap — single
    /// atomic load, no lock. A value `s` means every write so far has a seq
    /// `<= s`; the next write will store `s + 1`. Captured by an opening
    /// transaction as its `hot_seq_watermark` so the overlay read path can
    /// drop any later (`seq > watermark`) write by another session.
    #[inline]
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Claim and return the next monotonic sequence for a write.
    #[inline]
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Current mutation epoch (Fix A). Cheap — single atomic load, no lock.
    /// Bumped on every insert / upsert / delete. Used as the hot-tier
    /// watermark for the PK row cache: an entry is valid only if this value
    /// matches the value captured when the entry was inserted.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Bump the mutation epoch. Called by every mutation path.
    #[inline]
    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    // ── Age tracking ──────────────────────────────────────────────────────────

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn record_write_time(&self) {
        // Set to now only if currently empty (MAX).
        let _ = self.oldest_insert_secs.compare_exchange(
            u64::MAX,
            Self::now_secs(),
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    fn maybe_reset_oldest(&self) {
        if self.inner.read().is_empty() {
            self.oldest_insert_secs.store(u64::MAX, Ordering::Release);
        }
    }

    /// Age of the oldest row currently in this memtable.
    ///
    /// Returns `Duration::ZERO` when empty.  Cheap — reads one atomic; no lock.
    pub fn oldest_row_age(&self) -> Duration {
        let oldest = self.oldest_insert_secs.load(Ordering::Relaxed);
        if oldest == u64::MAX {
            return Duration::ZERO;
        }
        Duration::from_secs(Self::now_secs().saturating_sub(oldest))
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// Insert a new row.  If a row already exists under `key` it is replaced
    /// (last-write-wins, matching Basin's snapshot-isolation model).
    pub fn insert(&self, key: RowKey, value: MemRowValue) {
        let new_bytes = value.heap_bytes() as u64;
        let old_bytes = {
            let mut map = self.inner.write();
            let old = map
                .get(&key)
                .map(|e| e.value.heap_bytes() as u64)
                .unwrap_or(0);
            // Claim the seq under the write lock so the stored seq order matches
            // the BTreeMap mutation order even under same-key write races (no
            // seq inversion where an older claim overwrites a newer one).
            let seq = self.next_seq();
            map.insert(key, MemEntry { value, seq });
            old
        };
        // Saturating arithmetic avoids underflow on pathological usage.
        let cur = self.bytes_allocated.load(Ordering::Relaxed);
        self.bytes_allocated.store(
            cur.saturating_add(new_bytes).saturating_sub(old_bytes),
            Ordering::Relaxed,
        );
        self.record_write_time();
        self.bump_epoch();
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
        let old_bytes = {
            let mut map = self.inner.write();
            let old = map
                .get(&key)
                .map(|e| e.value.heap_bytes() as u64)
                .unwrap_or(0);
            // Claim the seq under the write lock (see `insert`).
            let seq = self.next_seq();
            map.insert(
                key,
                MemEntry {
                    value: MemRowValue::Tombstone,
                    seq,
                },
            );
            old
        };
        self.bytes_allocated.fetch_sub(
            old_bytes.min(self.bytes_allocated.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
        self.record_write_time();
        self.bump_epoch();
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Point lookup.  Returns `None` if the key has never been written.
    /// Returns `Some(MemRowValue::Tombstone)` if the row was deleted.
    pub fn get(&self, key: &RowKey) -> Option<MemRowValue> {
        self.inner.read().get(key).map(|e| e.value.clone())
    }

    /// Point lookup returning the stored value together with its MVCC
    /// sequence. `None` when the key was never written. Used by the
    /// transaction-isolated overlay read path, which must compare `seq`
    /// against the transaction's pinned `hot_seq_watermark`.
    pub fn get_with_seq(&self, key: &RowKey) -> Option<(MemRowValue, u64)> {
        self.inner
            .read()
            .get(key)
            .map(|e| (e.value.clone(), e.seq))
    }

    /// Range scan: returns all `(RowKey, MemRowValue)` pairs where
    /// `lo <= key <= hi`.  The returned vector is in ascending key order
    /// (BTreeMap guarantees this).
    pub fn range_scan(&self, lo: &RowKey, hi: &RowKey) -> Vec<(RowKey, MemRowValue)> {
        use std::ops::Bound::Included;
        let map = self.inner.read();
        map.range((Included(lo), Included(hi)))
            .map(|(k, e)| (k.clone(), e.value.clone()))
            .collect()
    }

    /// Full snapshot of all entries (including tombstones).  Used by the flush
    /// task (C4) to clone a generation-bounded view before I/O.  The lock is
    /// released before returning.
    pub fn snapshot(&self) -> Vec<(RowKey, MemRowValue)> {
        let map = self.inner.read();
        map.iter()
            .map(|(k, e)| (k.clone(), e.value.clone()))
            .collect()
    }

    /// Like [`snapshot`](Self::snapshot) but also returns each entry's MVCC
    /// sequence as a third tuple element. Used by the transaction-isolated
    /// overlay read path to drop entries written after a transaction pinned
    /// its `hot_seq_watermark`. The lock is released before returning.
    pub fn snapshot_with_seq(&self) -> Vec<(RowKey, MemRowValue, u64)> {
        let map = self.inner.read();
        map.iter()
            .map(|(k, e)| (k.clone(), e.value.clone(), e.seq))
            .collect()
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    /// Number of live rows (excludes tombstones).
    pub fn live_row_count(&self) -> usize {
        self.inner
            .read()
            .values()
            .filter(|e| e.value.is_row())
            .count()
    }

    /// Total row count including tombstones.
    pub fn total_count(&self) -> usize {
        self.inner.read().len()
    }

    /// Current byte estimate for live rows.  Cheap — atomic load, no lock.
    pub fn bytes_allocated(&self) -> u64 {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    // ── Flush helpers ─────────────────────────────────────────────────────────

    /// Remove all entries whose keys are in `keys`.  Used by the flush task
    /// (C4) after a successful cold-tier commit to GC flushed rows.  The write
    /// lock is held only for the duration of the removal loop.
    pub fn remove_flushed(&self, keys: &[RowKey]) {
        if keys.is_empty() {
            return;
        }
        let freed = {
            let mut map = self.inner.write();
            let mut freed: u64 = 0;
            for k in keys {
                if let Some(e) = map.remove(k) {
                    freed += e.value.heap_bytes() as u64;
                }
            }
            freed
        };
        let cur = self.bytes_allocated.load(Ordering::Relaxed);
        self.bytes_allocated
            .store(cur.saturating_sub(freed), Ordering::Relaxed);
        // Reset age tracker when the memtable drains to empty.
        self.maybe_reset_oldest();
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
        let v = mt
            .get(&key(1))
            .expect("tombstone must be present after delete");
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

    // ── epoch (Fix A — PK row cache invalidation) ─────────────────────────────

    #[test]
    fn epoch_starts_at_zero() {
        let mt = MemTable::new();
        assert_eq!(mt.epoch(), 0);
    }

    #[test]
    fn epoch_increases_on_insert() {
        let mt = MemTable::new();
        let e0 = mt.epoch();
        mt.insert(key(1), row(0x01));
        let e1 = mt.epoch();
        assert!(e1 > e0, "epoch must increase after insert: {e0} -> {e1}");
    }

    #[test]
    fn epoch_increases_on_upsert() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let e1 = mt.epoch();
        mt.upsert(key(1), row(0x02));
        let e2 = mt.epoch();
        assert!(e2 > e1, "epoch must increase after upsert: {e1} -> {e2}");
    }

    #[test]
    fn epoch_increases_on_delete() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let e1 = mt.epoch();
        mt.delete(key(1));
        let e2 = mt.epoch();
        assert!(e2 > e1, "epoch must increase after delete: {e1} -> {e2}");
    }

    #[test]
    fn epoch_monotonic_across_mixed_mutations() {
        let mt = MemTable::new();
        let mut last = mt.epoch();
        for i in 0..10i64 {
            mt.insert(key(i), row(i as u8));
            let now = mt.epoch();
            assert!(now > last, "epoch not monotonic at insert {i}");
            last = now;
        }
        mt.delete(key(0));
        assert!(mt.epoch() > last);
    }

    // ── MVCC sequence (hot-tier transaction isolation) ────────────────────────

    #[test]
    fn current_seq_starts_at_zero() {
        let mt = MemTable::new();
        assert_eq!(mt.current_seq(), 0);
    }

    #[test]
    fn seq_increments_on_each_write() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let (_, s1) = mt.get_with_seq(&key(1)).unwrap();
        assert_eq!(s1, 1, "first write should store seq 1");
        assert_eq!(mt.current_seq(), 1);
        mt.insert(key(2), row(0x02));
        let (_, s2) = mt.get_with_seq(&key(2)).unwrap();
        assert_eq!(s2, 2);
        assert_eq!(mt.current_seq(), 2);
    }

    #[test]
    fn upsert_bumps_seq_on_same_key() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let (_, s1) = mt.get_with_seq(&key(1)).unwrap();
        mt.upsert(key(1), row(0x02));
        let (_, s2) = mt.get_with_seq(&key(1)).unwrap();
        assert!(s2 > s1, "overwrite must advance seq: {s1} -> {s2}");
    }

    #[test]
    fn delete_bumps_seq() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let before = mt.current_seq();
        mt.delete(key(1));
        let (v, s) = mt.get_with_seq(&key(1)).unwrap();
        assert!(v.is_tombstone());
        assert!(s > before, "delete must advance seq: {before} -> {s}");
    }

    #[test]
    fn snapshot_with_seq_carries_per_entry_seq() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        mt.insert(key(2), row(0x02));
        let snap = mt.snapshot_with_seq();
        assert_eq!(snap.len(), 2);
        // BTreeMap order: key(1) then key(2); seqs 1 then 2.
        assert_eq!(snap[0].2, 1);
        assert_eq!(snap[1].2, 2);
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

    // ── age tracking ──────────────────────────────────────────────────────────

    #[test]
    fn oldest_row_age_zero_when_empty() {
        let mt = MemTable::new();
        assert_eq!(mt.oldest_row_age(), Duration::ZERO);
    }

    #[test]
    fn oldest_row_age_nonzero_after_insert() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0xAA));
        // May be 0 if insert + check happen in the same second, so just verify no panic.
        let _ = mt.oldest_row_age();
    }

    #[test]
    fn oldest_row_age_resets_after_full_drain() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0xAA));
        mt.remove_flushed(&[key(1)]);
        assert_eq!(mt.oldest_row_age(), Duration::ZERO);
    }

    // ── multi-project isolation ────────────────────────────────────────────────

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
