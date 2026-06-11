//! [`MemTable`] — per-`(project, table)` in-memory row buffer.
//!
//! Backed by `parking_lot::RwLock<BTreeMap<RowKey, MemEntry>>`.  The lock is
//! held only for O(log n) BTree operations and never across any I/O.  A
//! `schema_version: u32` field on every row enables schema-evolution-safe reads
//! as specified in ADR 0016 addendum 2026-05-19.
//!
//! # MVCC version chains (S4 row tier)
//!
//! Each key maps to a [`MemEntry`] holding a small **version chain** rather than
//! a single value: the newest version inline, older versions in an overflow
//! `Vec` (empty — no allocation — for a never-overwritten key). Overwriting a
//! key *pushes* a new newest version and demotes the prior one into the chain
//! instead of destroying it. This fixes the previously-documented
//! "single-version memtable" residual: a transaction that pinned its
//! `hot_seq_watermark` before another session overwrote the key can still read
//! the version it is entitled to via [`MemTable::get_with_seq`] /
//! [`MemTable::snapshot_with_seq`], which return the newest version with
//! `seq <= watermark`.
//!
//! ## Cap / GC decision (and the cold-fallback correctness argument)
//!
//! Versions are **NOT** length-capped per key. The chain is drained *entirely*
//! at flush ([`MemTable::remove_flushed`] removes the whole key, all versions).
//! This is deliberate. A naive length cap that dropped the oldest version would
//! be a **correctness regression**, because versions only exist for *overlay*
//! writes: the image that precedes the first overlay write of a key is the
//! **cold-tier row**. Concretely, suppose a pinned reader with watermark `w`
//! needs version `v1` (`v1.seq <= w < v2.seq`). If a cap dropped `v1`, then
//! `get_with_seq` would find no retained version `<= w` and return `None`; the
//! caller unions hot-miss with cold and the reader would see the *cold* image —
//! the state *before* `v1`, which is **older** than the snapshot the reader was
//! entitled to. Returning `v1` is correct; dropping it and falling to cold is
//! not. The only length-cap policy that is safe without watermark tracking is
//! one that never drops a version any live watermark could need; the simplest
//! such policy is "drop nothing until flush". Real workloads overwrite few keys
//! between flushes (chains form only for *overwritten* keys), so the unbounded
//! retention is cheap, and every retained version is charged against the byte
//! budget (so the flush-pressure trigger still bounds total memory).
//!
//! ### Future GC hook
//!
//! When a registry-level "minimum active hot watermark per table" is available
//! (today the watermarks live in `basin-engine`'s `TxState`, a cross-crate
//! concern), a chain-trim pass could safely drop every `older` version whose
//! `seq` is below that minimum — no live transaction could resolve to it. The
//! `MemEntry::older` layout (oldest-first) makes that a cheap prefix-drain. This
//! is intentionally deferred: the flush-boundary drain is already correct and
//! the plumbing for the live-watermark minimum is out of scope here.

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

// ── Stored entry (MVCC version chain) ─────────────────────────────────────────

/// What the memtable actually stores per key: a small MVCC **version chain**.
///
/// # Layout
///
/// The chain is split for the common case. The *newest* version is stored
/// inline (`value` + `seq`); every *older* version (produced when the key is
/// overwritten while a pinned reader might still need the prior image) lives in
/// `older`, ordered **oldest-first / newest-last**. For a key that has only
/// ever been written once — the overwhelmingly common case in real OLTP —
/// `older` is an empty `Vec`, which never allocates, so a single-version
/// `MemEntry` is byte-for-byte equivalent to the pre-version-chain struct
/// (`{ value, seq }` + an empty Vec header). This is the explicit
/// "single-version chain == today" argument that lets every existing read path
/// stay unchanged: `get` / `snapshot` read the inline newest exactly as before;
/// only the *new* `get_with_seq(key, watermark)` / `snapshot_with_seq(watermark)`
/// historical-lookup capability touches `older`.
///
/// # MVCC sequence
///
/// Each version carries the monotonic per-table sequence assigned to the write
/// that produced it. The sequence is the MVCC foundation for
/// transaction-snapshot isolation of hot-tier overlay writes (ADR 0016
/// isolation addendum). An open transaction pins a `hot_seq_watermark` at
/// first-touch; the historical overlay read path
/// ([`get_with_seq`](MemTable::get_with_seq) /
/// [`snapshot_with_seq`](MemTable::snapshot_with_seq)) returns, per key, the
/// **newest version whose `seq <= watermark`** — so a writer that overwrites a
/// key *after* another session pinned its snapshot no longer destroys the
/// version that session is entitled to see (the previously-documented
/// "single-version memtable" residual). Auto-commit reads ignore the sequence
/// entirely (no watermark → read the inline newest → zero hot-path cost).
///
/// # Chain bounding / GC
///
/// The chain is **unbounded per key between flushes** and drained *entirely*
/// at flush ([`remove_flushed`](MemTable::remove_flushed) removes the whole
/// key, inline + `older`). This is the deliberate, safe policy — see the module
/// note on [`MemTable`].
#[derive(Clone, Debug)]
struct MemEntry {
    /// The newest version's value (what `get` / `snapshot` return — unchanged
    /// behavior for every existing caller).
    value: MemRowValue,
    /// Monotonic per-table sequence of the newest version.
    seq: u64,
    /// Older versions, **oldest-first / newest-last**, retained so a pinned
    /// reader can still resolve the image at its watermark after the key was
    /// overwritten. Empty (no allocation) for a never-overwritten key, making
    /// a single-version entry equivalent to the legacy `{ value, seq }`.
    older: Vec<(u64, MemRowValue)>,
}

impl MemEntry {
    /// Construct a fresh single-version entry (empty `older` — no allocation).
    #[inline]
    fn single(value: MemRowValue, seq: u64) -> Self {
        Self {
            value,
            seq,
            older: Vec::new(),
        }
    }

    /// Push a new newest version, demoting the current inline newest into
    /// `older` (preserving oldest-first order). The caller has already claimed
    /// `seq` under the write lock, so `seq` is strictly greater than every
    /// sequence already in the chain.
    #[inline]
    fn push_version(&mut self, value: MemRowValue, seq: u64) {
        let prev_value = std::mem::replace(&mut self.value, value);
        let prev_seq = std::mem::replace(&mut self.seq, seq);
        self.older.push((prev_seq, prev_value));
    }

    /// Heap bytes consumed by **every** version in this chain (inline newest +
    /// all `older`). Used by [`MemTable`] memory accounting so each retained
    /// version is charged against the byte budget, and so `remove_flushed`
    /// frees the full chain.
    #[inline]
    fn chain_heap_bytes(&self) -> u64 {
        let mut total = self.value.heap_bytes() as u64;
        for (_, v) in &self.older {
            total += v.heap_bytes() as u64;
        }
        total
    }

    /// Return a clone of the newest version whose `seq <= watermark`, scanning
    /// newest → oldest. `None` when no version is old enough (the key's entire
    /// retained chain post-dates the watermark) — the caller then falls through
    /// to the cold tier, which holds the pre-overlay image (see [`MemTable`]).
    #[inline]
    fn version_at(&self, watermark: u64) -> Option<MemRowValue> {
        if self.seq <= watermark {
            return Some(self.value.clone());
        }
        // Newest-first scan of older versions (which are stored oldest-first).
        for (s, v) in self.older.iter().rev() {
            if *s <= watermark {
                return Some(v.clone());
            }
        }
        None
    }
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

    /// Insert a row.  If a row already exists under `key` its prior value is
    /// **retained as an older version** (MVCC version chain) and the new value
    /// becomes the newest version — last-write-wins for `get`/`snapshot`, but a
    /// pinned reader can still resolve the prior image via `get_with_seq`.
    ///
    /// For a key written for the first time this is identical to the legacy
    /// single-value insert (the chain is just the one inline version).
    pub fn insert(&self, key: RowKey, value: MemRowValue) {
        let new_bytes = value.heap_bytes() as u64;
        {
            let mut map = self.inner.write();
            // Claim the seq under the write lock so the stored seq order matches
            // the BTreeMap mutation order even under same-key write races (no
            // seq inversion where an older claim overwrites a newer one).
            let seq = self.next_seq();
            match map.get_mut(&key) {
                // Overwrite: push a new newest version, retaining the prior one.
                Some(entry) => entry.push_version(value, seq),
                // First write for this key: single-version chain (no allocation).
                None => {
                    map.insert(key, MemEntry::single(value, seq));
                }
            }
        }
        // Every retained version is charged against the budget, so an overwrite
        // ADDS the new version's bytes without subtracting the prior version's
        // (the prior version is retained, not freed). The full chain is freed at
        // flush via `remove_flushed`.
        let cur = self.bytes_allocated.load(Ordering::Relaxed);
        self.bytes_allocated
            .store(cur.saturating_add(new_bytes), Ordering::Relaxed);
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
        {
            let mut map = self.inner.write();
            // Claim the seq under the write lock (see `insert`).
            let seq = self.next_seq();
            match map.get_mut(&key) {
                // Overwrite with a tombstone version, retaining prior versions so
                // a pinned reader can still resolve the pre-delete image.
                Some(entry) => entry.push_version(MemRowValue::Tombstone, seq),
                // Tombstone for a never-written key: single-version chain.
                None => {
                    map.insert(key, MemEntry::single(MemRowValue::Tombstone, seq));
                }
            }
        }
        // A tombstone version is zero bytes, so the running estimate is
        // unchanged; any retained older row versions remain charged until the
        // whole chain is freed at flush.
        self.record_write_time();
        self.bump_epoch();
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Point lookup.  Returns `None` if the key has never been written.
    /// Returns `Some(MemRowValue::Tombstone)` if the row was deleted.
    pub fn get(&self, key: &RowKey) -> Option<MemRowValue> {
        self.inner.read().get(key).map(|e| e.value.clone())
    }

    /// MVCC point lookup at a watermark.
    ///
    /// * `watermark == None` — auto-commit read: returns the **newest** version
    ///   (identical to what a `get` would surface), so an auto-commit reader
    ///   always sees the latest committed hot-tier state. Cheapest path: reads
    ///   the inline newest, never scans the chain.
    /// * `watermark == Some(w)` — in-transaction read: returns the **newest
    ///   version whose `seq <= w`**, i.e. the image the pinning transaction is
    ///   entitled to. `None` when the key was never written *or* its entire
    ///   retained chain post-dates `w` (every version was written after the
    ///   transaction pinned its snapshot) — in both cases the caller falls
    ///   through to the cold tier, which holds the pre-overlay image, the
    ///   correct pinned view (see the module note on chain bounding).
    ///
    /// This is the capability that fixes the single-version-memtable residual:
    /// before version chains, an overwrite destroyed the prior version, so a
    /// pinned reader saw `None` (cold) even when it was entitled to the
    /// overwritten overlay value. Now the prior version is retained and served.
    pub fn get_with_seq(&self, key: &RowKey, watermark: Option<u64>) -> Option<MemRowValue> {
        let map = self.inner.read();
        let entry = map.get(key)?;
        match watermark {
            None => Some(entry.value.clone()),
            Some(w) => entry.version_at(w),
        }
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

    /// MVCC snapshot at a watermark.
    ///
    /// * `watermark == None` — auto-commit read: yields per key the **newest**
    ///   version (identical shape and contents to [`snapshot`](Self::snapshot)),
    ///   one entry per key. Cheapest path: reads inline newest, never scans
    ///   chains.
    /// * `watermark == Some(w)` — in-transaction read: yields per key the
    ///   **newest version whose `seq <= w`**. Keys whose entire retained chain
    ///   post-dates `w` are **omitted** (the transaction must not see them; it
    ///   falls through to cold). This is the fix to the previous behavior, which
    ///   skipped any *overwritten* key entirely on a pinned read — now the
    ///   historical version is served instead.
    ///
    /// The lock is released before returning.
    pub fn snapshot_with_seq(&self, watermark: Option<u64>) -> Vec<(RowKey, MemRowValue)> {
        let map = self.inner.read();
        match watermark {
            None => map
                .iter()
                .map(|(k, e)| (k.clone(), e.value.clone()))
                .collect(),
            Some(w) => map
                .iter()
                .filter_map(|(k, e)| e.version_at(w).map(|v| (k.clone(), v)))
                .collect(),
        }
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
                    // Free the WHOLE version chain (inline newest + all retained
                    // older versions): flush drains the key entirely, so every
                    // version's bytes are returned to the budget.
                    freed += e.chain_heap_bytes();
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
        // First write stores seq 1: visible at watermark 1, absent below it.
        assert!(mt.get_with_seq(&key(1), Some(1)).is_some());
        assert!(mt.get_with_seq(&key(1), Some(0)).is_none());
        assert_eq!(mt.current_seq(), 1);
        mt.insert(key(2), row(0x02));
        assert!(mt.get_with_seq(&key(2), Some(2)).is_some());
        assert!(mt.get_with_seq(&key(2), Some(1)).is_none());
        assert_eq!(mt.current_seq(), 2);
    }

    #[test]
    fn upsert_bumps_seq_on_same_key() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // seq 1
        mt.upsert(key(1), row(0x02)); // seq 2 (older v1 retained)
        // Auto-commit (None) sees the newest version.
        match mt.get_with_seq(&key(1), None).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0x02),
            _ => panic!("expected newest Row"),
        }
        // A reader pinned at seq 1 still sees the original (the fix).
        match mt.get_with_seq(&key(1), Some(1)).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0x01),
            _ => panic!("expected retained v1"),
        }
    }

    #[test]
    fn delete_bumps_seq() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // seq 1
        let before = mt.current_seq();
        mt.delete(key(1)); // seq 2 tombstone, v1 retained
        assert!(mt.current_seq() > before, "delete must advance seq");
        // Newest is the tombstone; the pre-delete reader still sees the row.
        assert!(mt.get_with_seq(&key(1), None).unwrap().is_tombstone());
        assert!(mt.get_with_seq(&key(1), Some(1)).unwrap().is_row());
    }

    #[test]
    fn snapshot_with_seq_newest_per_key_unfiltered() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        mt.insert(key(2), row(0x02));
        // Auto-commit snapshot: one newest entry per key (shape == snapshot()).
        let snap = mt.snapshot_with_seq(None);
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn snapshot_with_seq_serves_historical_version() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // seq 1
        mt.upsert(key(1), row(0x02)); // seq 2
        // Pinned at seq 1: snapshot yields the historical v1, not the newest.
        let snap = mt.snapshot_with_seq(Some(1));
        assert_eq!(snap.len(), 1);
        match &snap[0].1 {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0x01),
            _ => panic!("expected historical v1"),
        }
        // Pinned below the first write: key omitted (falls through to cold).
        assert!(mt.snapshot_with_seq(Some(0)).is_empty());
    }

    // ── MVCC version chains (S4 row tier) ─────────────────────────────────────

    #[test]
    fn single_version_key_behaves_like_legacy() {
        // A never-overwritten key: get / snapshot / get_with_seq(None) all
        // surface the one inline version — provably identical to pre-S4.
        let mt = MemTable::new();
        mt.insert(key(1), row(0x07));
        assert!(mt.get(&key(1)).unwrap().is_row());
        assert!(mt.get_with_seq(&key(1), None).unwrap().is_row());
        assert!(mt.get_with_seq(&key(1), Some(1)).unwrap().is_row());
        assert_eq!(mt.snapshot().len(), 1);
        assert_eq!(mt.snapshot_with_seq(None).len(), 1);
    }

    #[test]
    fn three_overwrites_form_a_resolvable_chain() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0xA0)); // seq 1
        mt.upsert(key(1), row(0xA1)); // seq 2
        mt.upsert(key(1), row(0xA2)); // seq 3
        mt.upsert(key(1), row(0xA3)); // seq 4
        // Newest wins for plain reads.
        match mt.get(&key(1)).unwrap() {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0xA3),
            _ => panic!(),
        }
        // Each watermark resolves to the newest version <= it.
        let at = |w: u64| match mt.get_with_seq(&key(1), Some(w)).unwrap() {
            MemRowValue::Row { bytes, .. } => bytes[0],
            _ => panic!(),
        };
        assert_eq!(at(1), 0xA0);
        assert_eq!(at(2), 0xA1);
        assert_eq!(at(3), 0xA2);
        assert_eq!(at(4), 0xA3);
        assert_eq!(at(99), 0xA3, "watermark beyond newest sees newest");
        // Below the first write: no version, falls through to cold.
        assert!(mt.get_with_seq(&key(1), Some(0)).is_none());
    }

    #[test]
    fn delete_then_reinsert_chain_resolves_per_epoch() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x10)); // seq 1: Row
        mt.delete(key(1)); // seq 2: Tombstone
        mt.upsert(key(1), row(0x11)); // seq 3: Row (resurrect)
        assert!(mt.get_with_seq(&key(1), Some(1)).unwrap().is_row());
        assert!(mt.get_with_seq(&key(1), Some(2)).unwrap().is_tombstone());
        assert!(mt.get_with_seq(&key(1), Some(3)).unwrap().is_row());
        // Newest (auto-commit) is the resurrected row.
        assert!(mt.get_with_seq(&key(1), None).unwrap().is_row());
    }

    #[test]
    fn flush_drains_whole_chain() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        mt.upsert(key(1), row(0x02));
        mt.upsert(key(1), row(0x03));
        mt.remove_flushed(&[key(1)]);
        // Post-flush: nothing in the memtable for any watermark.
        assert!(mt.get(&key(1)).is_none());
        assert!(mt.get_with_seq(&key(1), Some(1)).is_none());
        assert!(mt.get_with_seq(&key(1), None).is_none());
        assert_eq!(mt.bytes_allocated(), 0);
    }

    #[test]
    fn unfiltered_reads_always_see_newest_across_overwrites() {
        let mt = MemTable::new();
        for v in 0u8..5 {
            mt.upsert(key(1), row(v));
            match mt.get(&key(1)).unwrap() {
                MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], v),
                _ => panic!(),
            }
        }
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
    fn bytes_accounting_retains_row_version_under_tombstone() {
        // S4 version chains: DELETE pushes a zero-byte tombstone version but
        // RETAINS the prior row version (a pinned reader may still need it), so
        // the byte estimate holds the retained row's bytes until flush drains
        // the whole chain. (Pre-S4 this dropped to 0.)
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0));
        mt.delete(key(1));
        assert_eq!(mt.bytes_allocated(), 100);
        // remove_flushed frees the whole chain.
        mt.remove_flushed(&[key(1)]);
        assert_eq!(mt.bytes_allocated(), 0);
    }

    #[test]
    fn bytes_accounting_grows_with_retained_versions() {
        // S4 version chains: each overwrite ADDS a retained version's bytes
        // (the prior version is kept, not freed). (Pre-S4 this stored only the
        // newest, i.e. 200.)
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0));
        assert_eq!(mt.bytes_allocated(), 100);
        mt.upsert(key(1), MemRowValue::row(vec![0u8; 200], 0));
        assert_eq!(mt.bytes_allocated(), 300, "v1(100) + v2(200) retained");
        // Whole chain freed at flush.
        mt.remove_flushed(&[key(1)]);
        assert_eq!(mt.bytes_allocated(), 0);
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
