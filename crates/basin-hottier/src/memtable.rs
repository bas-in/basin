//! [`MemTable`] — per-`(project, table)` in-memory row buffer.
//!
//! Backed by `parking_lot::RwLock<BTreeMap<RowKey, MemEntry>>`.  The lock is
//! held only for O(log n) BTree operations and never across any I/O.  A
//! `schema_version: u32` field on every row enables schema-evolution-safe reads
//! as specified in ADR 0016 addendum 2026-05-19.
//!
//! # MVCC version chains (S4 row tier)
//!
//! Each key maps to a `MemEntry` holding a small **version chain** rather than
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
//! Versions are **NOT** length-capped per key. The chain is drained at flush
//! ack ([`MemTable::mark_flushed`] drops every version the ack covers; the
//! legacy [`MemTable::remove_flushed`] removes the whole key, all versions).
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
//!
//! # Clean/dirty residency (S4 age-based residency)
//!
//! A flush ack no longer *removes* acked rows — it marks them **clean**
//! ([`MemTable::mark_flushed`]): the cold tier now holds an identical image of
//! the acked version, so the entry can keep serving point reads from memory
//! and be evicted later ([`MemTable::evict_clean`]) under age/byte budgets
//! with zero correctness impact. An entry is clean iff
//! `seq <= flushed_up_to`; any later write re-dirties it. `bytes_dirty`
//! tracks the dirty subset of `bytes_allocated` (clean bytes = total − dirty)
//! and [`MemTable::dirty_snapshot`] is the flush input. Cleaned entries are
//! appended to a flush-order `clean_queue` so eviction is oldest-first;
//! queue entries are verified on pop because a key can be re-dirtied.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

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

// ── Small shared helpers ──────────────────────────────────────────────────────

/// Wall-clock seconds since the UNIX epoch (saturating to 0 on clock error).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Saturating add on a relaxed counter (mirrors the existing
/// `bytes_allocated` load/store update style).
#[inline]
fn add_bytes(counter: &AtomicU64, n: u64) {
    if n == 0 {
        return;
    }
    let cur = counter.load(Ordering::Relaxed);
    counter.store(cur.saturating_add(n), Ordering::Relaxed);
}

/// Saturating subtract on a relaxed counter.
#[inline]
fn sub_bytes(counter: &AtomicU64, n: u64) {
    if n == 0 {
        return;
    }
    let cur = counter.load(Ordering::Relaxed);
    counter.store(cur.saturating_sub(n), Ordering::Relaxed);
}

#[inline]
fn add_one(counter: &AtomicU64) {
    add_bytes(counter, 1);
}

#[inline]
fn sub_one(counter: &AtomicU64) {
    sub_bytes(counter, 1);
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
/// The chain is **unbounded per key between flushes** and drained at flush ack
/// ([`mark_flushed`](MemTable::mark_flushed) drops every version covered by
/// the ack; the legacy [`remove_flushed`](MemTable::remove_flushed) removes
/// the whole key, inline + `older`). This is the deliberate, safe policy — see
/// the module note on [`MemTable`].
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
    /// Highest write sequence covered by a cold-tier flush ack (S4 age-based
    /// residency). `0` = never acked. The entry is **clean** iff
    /// `seq <= flushed_up_to` (older versions all carry lower seqs): the cold
    /// tier holds an identical image of the newest version, so the entry is
    /// pure read acceleration and may be evicted at any time.
    flushed_up_to: u64,
    /// Wall-clock seconds (UNIX epoch, truncated to `u32` — valid until 2106)
    /// of this entry's creation or last re-dirtying write. Read by
    /// [`MemTable::evict_clean`]'s `min_age_secs` filter.
    written_secs: u32,
}

impl MemEntry {
    /// Construct a fresh single-version entry (empty `older` — no allocation).
    #[inline]
    fn single(value: MemRowValue, seq: u64) -> Self {
        Self {
            value,
            seq,
            older: Vec::new(),
            flushed_up_to: 0,
            written_secs: now_secs() as u32,
        }
    }

    /// Push a new newest version, demoting the current inline newest into
    /// `older` (preserving oldest-first order). The caller has already claimed
    /// `seq` under the write lock, so `seq` is strictly greater than every
    /// sequence already in the chain. Refreshes `written_secs` — a write
    /// (re-)dirties the entry, restarting its residency clock.
    #[inline]
    fn push_version(&mut self, value: MemRowValue, seq: u64) {
        let prev_value = std::mem::replace(&mut self.value, value);
        let prev_seq = std::mem::replace(&mut self.seq, seq);
        self.older.push((prev_seq, prev_value));
        self.written_secs = now_secs() as u32;
    }

    /// `true` iff every version in this chain is covered by a flush ack — the
    /// cold tier holds an identical image of the newest version, so the entry
    /// can be evicted without changing any observable read.
    #[inline]
    fn is_clean(&self) -> bool {
        self.seq <= self.flushed_up_to
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
    /// Heap bytes held by **dirty** entries (chains whose newest version is
    /// not yet covered by a flush ack). Always `<= bytes_allocated`; clean
    /// bytes = `bytes_allocated − bytes_dirty`. Maintained at every mutation
    /// site (insert / delete / mark_flushed / insert_clean / evict_clean /
    /// remove_flushed) so budget callers never need the lock.
    bytes_dirty: AtomicU64,
    /// O(1) count of entries whose **newest** version is a `Tombstone`.
    tombstone_count: AtomicU64,
    /// O(1) count of entries whose **newest** version is an `Update`
    /// (PK-keyed UPDATE fast-path override). While non-zero, the engine's
    /// PK-overlay union must stay engaged; [`MemTable::mark_flushed`]
    /// re-tags acked `Update`s as `Row`, draining this counter.
    update_count: AtomicU64,
    /// O(1) count of entries whose **newest** version is a `Row`. On the
    /// write path these are the counter-keyed HTAP INSERT rows (`Update` is
    /// always PK-keyed — see [`MemRowValue::Update`]); after a flush ack a
    /// re-tagged `Update` also counts here (it is then the PK-keyed cold
    /// image, byte-identical to cold, so treating it as a plain `Row` is
    /// read-safe).
    counter_key_rows: AtomicU64,
    /// Flush-order clean-eviction queue: `(seq, key)` appended by
    /// [`MemTable::mark_flushed`] / [`MemTable::insert_clean`] when an entry
    /// becomes clean. Popped oldest-first by [`MemTable::evict_clean`];
    /// entries are verified on pop (a key can be re-dirtied, re-acked, or
    /// removed after its queue entry was appended — stale entries are
    /// dropped). Lock ordering: never acquired while holding `inner`'s write
    /// lock *by writers*; only `evict_clean` holds both (queue → inner),
    /// which is cycle-free because no other path holds `inner` while waiting
    /// on the queue.
    clean_queue: Mutex<VecDeque<(u64, RowKey)>>,
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
            bytes_dirty: AtomicU64::new(0),
            tombstone_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            counter_key_rows: AtomicU64::new(0),
            clean_queue: Mutex::new(VecDeque::new()),
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

    // ── Per-kind newest-version counters ──────────────────────────────────────

    /// The O(1) counter tracking entries whose newest version matches `v`'s
    /// variant. See the field docs on `tombstone_count` / `update_count` /
    /// `counter_key_rows`.
    #[inline]
    fn kind_counter(&self, v: &MemRowValue) -> &AtomicU64 {
        match v {
            MemRowValue::Row { .. } => &self.counter_key_rows,
            MemRowValue::Update { .. } => &self.update_count,
            MemRowValue::Tombstone => &self.tombstone_count,
        }
    }

    #[inline]
    fn inc_kind(&self, v: &MemRowValue) {
        add_one(self.kind_counter(v));
    }

    #[inline]
    fn dec_kind(&self, v: &MemRowValue) {
        sub_one(self.kind_counter(v));
    }

    // ── Age tracking ──────────────────────────────────────────────────────────

    fn record_write_time(&self) {
        // Set to now only if currently empty (MAX).
        let _ = self.oldest_insert_secs.compare_exchange(
            u64::MAX,
            now_secs(),
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
        Duration::from_secs(now_secs().saturating_sub(oldest))
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
        let _ = self.insert_inner(key, value);
    }

    /// Shared mutation core for `insert` / `delete` / `insert_clean`.
    /// Returns the seq claimed by this write.
    ///
    /// Maintains, in one place: `bytes_allocated` (every retained version is
    /// charged), `bytes_dirty` (the new version is dirty; a previously-clean
    /// chain re-enters the dirty set wholesale), the per-kind newest-version
    /// counters, the age tracker, and the mutation epoch.
    fn insert_inner(&self, key: RowKey, value: MemRowValue) -> u64 {
        let new_bytes = value.heap_bytes() as u64;
        self.inc_kind(&value);
        let (seq, redirtied) = {
            let mut map = self.inner.write();
            // Claim the seq under the write lock so the stored seq order matches
            // the BTreeMap mutation order even under same-key write races (no
            // seq inversion where an older claim overwrites a newer one).
            let seq = self.next_seq();
            match map.get_mut(&key) {
                // Overwrite: push a new newest version, retaining the prior one.
                Some(entry) => {
                    // The newest version's kind changes: this entry no longer
                    // counts under its previous variant.
                    self.dec_kind(&entry.value);
                    // A clean entry re-enters the dirty set: its retained
                    // bytes (counted clean since the flush ack) move back to
                    // the dirty side along with the new version's bytes. The
                    // stale clean_queue entry is invalidated by the seq bump
                    // and dropped on pop.
                    let redirtied = if entry.is_clean() {
                        entry.chain_heap_bytes()
                    } else {
                        0
                    };
                    entry.push_version(value, seq);
                    (seq, redirtied)
                }
                // First write for this key: single-version chain (no allocation).
                None => {
                    map.insert(key, MemEntry::single(value, seq));
                    (seq, 0)
                }
            }
        };
        // Every retained version is charged against the budget, so an overwrite
        // ADDS the new version's bytes without subtracting the prior version's
        // (the prior version is retained, not freed). Covered versions are
        // freed at flush ack via `mark_flushed` (or wholesale via
        // `remove_flushed`).
        add_bytes(&self.bytes_allocated, new_bytes);
        add_bytes(&self.bytes_dirty, new_bytes.saturating_add(redirtied));
        self.record_write_time();
        self.bump_epoch();
        seq
    }

    /// Upsert a row: insert or overwrite.  Same semantics as `insert`.
    #[inline]
    pub fn upsert(&self, key: RowKey, value: MemRowValue) {
        self.insert(key, value);
    }

    /// Write a `Tombstone` for `key`.  Subsequent reads will see no row for
    /// this key; the tombstone suppresses any matching cold-tier row during
    /// read-merge.
    ///
    /// A tombstone version is zero bytes, so `bytes_allocated` is unchanged;
    /// any retained older row versions remain charged (and re-enter the dirty
    /// set if the entry was clean) until the chain is drained at flush ack.
    pub fn delete(&self, key: RowKey) {
        let _ = self.insert_inner(key, MemRowValue::Tombstone);
    }

    /// Insert a row that is **already durable in the cold tier** (write-through
    /// path, S4 age-based residency): the value is inserted and immediately
    /// acked, so the entry starts CLEAN — pure read acceleration, evictable at
    /// any time, never part of a [`dirty_snapshot`](Self::dirty_snapshot).
    ///
    /// Semantically `insert(key, value)` followed by an instantaneous flush
    /// ack of that write's seq (implemented exactly as that composition, so
    /// all ack invariants apply: prior covered versions are drained, a
    /// `Tombstone` value degenerates to removing the key, an `Update` value is
    /// re-tagged `Row`). The mutation **does** bump `epoch` — the observable
    /// value changes — unlike `mark_flushed` itself.
    pub fn insert_clean(&self, key: RowKey, value: MemRowValue) {
        let seq = self.insert_inner(key.clone(), value);
        let _ = self.mark_flushed(&[(key, seq)]);
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

    /// Full snapshot of all entries (including tombstones and clean
    /// flushed-and-retained entries).  The flush task uses
    /// [`dirty_snapshot`](Self::dirty_snapshot) instead — this is the read
    /// path's overlay view.  The lock is released before returning.
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

    /// Bytes held by **dirty** entries (not yet covered by a flush ack).
    /// Cheap — atomic load, no lock.
    pub fn bytes_dirty(&self) -> u64 {
        self.bytes_dirty.load(Ordering::Relaxed)
    }

    /// Bytes held by **clean** (flushed-and-retained) entries:
    /// `bytes_allocated − bytes_dirty`. Cheap — two atomic loads, no lock.
    pub fn bytes_clean(&self) -> u64 {
        self.bytes_allocated()
            .saturating_sub(self.bytes_dirty.load(Ordering::Relaxed))
    }

    /// O(1) count of entries whose newest version is a `Tombstone`.
    pub fn tombstone_count(&self) -> u64 {
        self.tombstone_count.load(Ordering::Relaxed)
    }

    /// O(1) count of entries whose newest version is an `Update` (PK-keyed
    /// UPDATE fast-path override). Drained by [`Self::mark_flushed`]'s
    /// `Update` → `Row` re-tag.
    pub fn update_count(&self) -> u64 {
        self.update_count.load(Ordering::Relaxed)
    }

    /// O(1) count of entries whose newest version is a `Row` — the
    /// counter-keyed HTAP INSERT rows on the write path (see the field doc
    /// for the post-ack re-tag nuance).
    pub fn counter_key_rows(&self) -> u64 {
        self.counter_key_rows.load(Ordering::Relaxed)
    }

    // ── Flush helpers ─────────────────────────────────────────────────────────

    /// Snapshot of all **dirty** entries — the flush input (S4 age-based
    /// residency). Yields `(key, newest value, newest seq)` per dirty entry in
    /// ascending key order; clean entries (already durable in the cold tier)
    /// are skipped. The returned seq is what the flush task hands back to
    /// [`Self::mark_flushed`] after the cold-tier commit, so a write that
    /// lands *after* this snapshot (bumping the entry's seq) is detectably
    /// newer than the ack and survives dirty. The lock is released before
    /// returning.
    pub fn dirty_snapshot(&self) -> Vec<(RowKey, MemRowValue, u64)> {
        let map = self.inner.read();
        map.iter()
            .filter(|(_, e)| !e.is_clean())
            .map(|(k, e)| (k.clone(), e.value.clone(), e.seq))
            .collect()
    }

    /// Acknowledge a successful cold-tier flush of the given `(key, seq)`
    /// pairs (the seqs captured by [`Self::dirty_snapshot`]). Returns the heap
    /// bytes actually freed (drained covered versions + removed tombstones) —
    /// the exact amount the flush task must release from the project budget.
    ///
    /// Per key, when the entry's newest seq is covered (`seq <= acked_seq`):
    ///
    /// * **`Tombstone`** entries are REMOVED — a clean tombstone is
    ///   meaningless; the cold-tier delete is committed, so there is no row
    ///   left to suppress.
    /// * **`Update`** entries are RE-TAGGED as `Row`: the acked image *is*
    ///   the cold image now, so staying `Update` would keep the engine's
    ///   PK-overlay union engaged forever. `update_count` is decremented (and
    ///   `counter_key_rows` incremented — the entry now counts as a `Row`).
    /// * **`Row`** entries are marked clean (`flushed_up_to = acked_seq`) and
    ///   retained as read acceleration.
    ///
    /// When the entry's newest seq is **newer** than the ack (a write landed
    /// after the flush snapshot), the entry STAYS DIRTY so the next flush
    /// picks it up — this fixes the latent version-loss bug where the legacy
    /// `remove_flushed` ack dropped whole chains by key, silently destroying
    /// versions written after the flush snapshot.
    ///
    /// In both cases, OLDER versions with `seq <= acked_seq` are drained from
    /// the chain: the cold tier now holds the acked image, so a pinned reader
    /// that misses here falls through to cold — the same flush-boundary
    /// behavior `remove_flushed` always had. Versions newer than the ack are
    /// never drained. Cleaned entries are appended to the clean queue for
    /// oldest-first eviction.
    ///
    /// # Epoch invariant
    ///
    /// `epoch` is deliberately **NOT** bumped: every observable read value is
    /// unchanged (clean entries still serve their version; removed tombstones
    /// read as the committed cold delete either way), so the PK row cache
    /// stays warm across flushes.
    pub fn mark_flushed(&self, acks: &[(RowKey, u64)]) -> u64 {
        if acks.is_empty() {
            return 0;
        }
        let mut queue_push: Vec<(u64, RowKey)> = Vec::new();
        // Bytes released from `bytes_allocated` (memory actually freed).
        let mut freed: u64 = 0;
        // Bytes released from `bytes_dirty` (freed OR moved to the clean side).
        let mut dirty_freed: u64 = 0;
        {
            let mut map = self.inner.write();
            for (key, acked_seq) in acks {
                let Some(entry) = map.get_mut(key) else {
                    // Removed since the snapshot (remove_flushed / DROP) —
                    // nothing left to ack.
                    continue;
                };
                if entry.is_clean() {
                    // Duplicate / overlapping ack: already clean, covered
                    // versions already drained, queue entry already appended.
                    continue;
                }
                // Drain every OLDER version the ack covers. `older` is
                // oldest-first, so the covered set is a prefix.
                let cut = entry.older.partition_point(|(s, _)| *s <= *acked_seq);
                for (_, v) in entry.older.drain(..cut) {
                    let b = v.heap_bytes() as u64;
                    freed += b;
                    dirty_freed += b;
                }
                if entry.seq > *acked_seq {
                    // Post-snapshot write: the newest version was NOT part of
                    // this flush — the entry stays dirty (version-loss fix).
                    continue;
                }
                // The ack covers the newest version → the entry becomes CLEAN.
                dirty_freed += entry.value.heap_bytes() as u64;
                if entry.value.is_tombstone() {
                    // Clean tombstone is meaningless — the cold delete is
                    // committed. Remove the entry outright (zero heap bytes).
                    map.remove(key);
                    sub_one(&self.tombstone_count);
                    continue;
                }
                if entry.value.is_update() {
                    // Re-tag Update → Row: the acked image IS the cold image.
                    let prior = std::mem::replace(&mut entry.value, MemRowValue::Tombstone);
                    if let MemRowValue::Update {
                        bytes,
                        schema_version,
                    } = prior
                    {
                        entry.value = MemRowValue::Row {
                            bytes,
                            schema_version,
                        };
                    }
                    sub_one(&self.update_count);
                    add_one(&self.counter_key_rows);
                }
                entry.flushed_up_to = *acked_seq;
                queue_push.push((entry.seq, key.clone()));
            }
        }
        sub_bytes(&self.bytes_allocated, freed);
        sub_bytes(&self.bytes_dirty, dirty_freed);
        if !queue_push.is_empty() {
            // The map write lock is released before the queue lock is taken
            // (see the `clean_queue` field doc for the lock-ordering rule).
            self.clean_queue.lock().extend(queue_push);
        }
        // Reset age tracker when the memtable drains to empty (e.g. an
        // all-tombstone ack). NOTE: no epoch bump — see doc above.
        self.maybe_reset_oldest();
        freed
    }

    /// Evict clean (flushed-and-retained) entries, oldest-acked first, until
    /// at least `target_bytes` have been freed or the queue runs out. Returns
    /// the heap bytes actually freed.
    ///
    /// Queue entries are verified on pop: a key that was re-dirtied (seq
    /// bumped), re-acked (fresher queue entry exists), or removed since its
    /// queue entry was appended is skipped. When `min_age_secs` is `Some`,
    /// eviction **stops** at the first entry younger than the threshold — the
    /// queue is flush-ack-ordered (≈ age-ordered), so everything behind it is
    /// at least as young; the entry is pushed back for a later sweep.
    ///
    /// # Epoch invariant
    ///
    /// `epoch` is deliberately **NOT** bumped: a clean entry is byte-identical
    /// to its cold image, so removing it changes no observable read (point
    /// lookups fall through to the cold tier and see the same row) and the PK
    /// row cache stays warm.
    pub fn evict_clean(&self, target_bytes: u64, min_age_secs: Option<u64>) -> u64 {
        if target_bytes == 0 {
            return 0;
        }
        let now = now_secs();
        let mut freed: u64 = 0;
        let mut queue = self.clean_queue.lock();
        while freed < target_bytes {
            let Some((seq, key)) = queue.pop_front() else {
                break;
            };
            let mut map = self.inner.write();
            // Verify on pop — drop stale queue entries.
            let Some(entry) = map.get(&key) else {
                continue;
            };
            if entry.seq != seq || !entry.is_clean() {
                continue;
            }
            if let Some(min_age) = min_age_secs {
                let age = now.saturating_sub(entry.written_secs as u64);
                if age < min_age {
                    // Too young — and so is everything behind it. Put it
                    // back and stop.
                    queue.push_front((seq, key));
                    break;
                }
            }
            let entry = map.remove(&key).expect("entry presence checked above");
            drop(map);
            // A clean chain is fully covered by its ack, so none of its bytes
            // are in `bytes_dirty`.
            freed += entry.chain_heap_bytes();
            self.dec_kind(&entry.value);
        }
        drop(queue);
        sub_bytes(&self.bytes_allocated, freed);
        // Reset age tracker when the memtable drains to empty.
        self.maybe_reset_oldest();
        freed
    }

    /// Remove all entries whose keys are in `keys`, freeing their **whole**
    /// version chains.  The write lock is held only for the duration of the
    /// removal loop.
    ///
    /// Retained for DROP/TRUNCATE and defensive callers that must drop keys
    /// unconditionally. **Flush paths should use [`Self::mark_flushed`]
    /// instead**: `remove_flushed` drops the entire chain by key, including
    /// versions written *after* the flush snapshot (the version-loss hazard
    /// `mark_flushed` exists to fix), and forfeits clean retention. Stale
    /// clean-queue entries for removed keys are dropped on pop.
    pub fn remove_flushed(&self, keys: &[RowKey]) {
        if keys.is_empty() {
            return;
        }
        let (freed, dirty_freed) = {
            let mut map = self.inner.write();
            let mut freed: u64 = 0;
            let mut dirty_freed: u64 = 0;
            for k in keys {
                if let Some(e) = map.remove(k) {
                    // Free the WHOLE version chain (inline newest + all retained
                    // older versions): every version's bytes are returned to
                    // the budget.
                    let chain = e.chain_heap_bytes();
                    freed += chain;
                    if !e.is_clean() {
                        // A dirty chain's bytes are all on the dirty side; a
                        // clean chain's are all on the clean side.
                        dirty_freed += chain;
                    }
                    self.dec_kind(&e.value);
                }
            }
            (freed, dirty_freed)
        };
        sub_bytes(&self.bytes_allocated, freed);
        sub_bytes(&self.bytes_dirty, dirty_freed);
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

    // ── mark_flushed (S4 age-based residency: clean/dirty + flush ack) ────────

    /// Recount the per-kind newest-version counters by scanning the map, for
    /// cross-checking the O(1) counters after mixed workloads.
    fn recount(mt: &MemTable) -> (u64, u64, u64) {
        let snap = mt.snapshot();
        let rows = snap
            .iter()
            .filter(|(_, v)| matches!(v, MemRowValue::Row { .. }))
            .count() as u64;
        let updates = snap.iter().filter(|(_, v)| v.is_update()).count() as u64;
        let tombs = snap.iter().filter(|(_, v)| v.is_tombstone()).count() as u64;
        (rows, updates, tombs)
    }

    /// Reproduce-first: the latent version-loss bug. A write that lands AFTER
    /// the flush snapshot must survive the flush ack (the legacy
    /// `remove_flushed` dropped the whole chain by key, destroying it).
    #[test]
    fn mark_flushed_post_snapshot_write_survives_dirty() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // v1, seq 1
        let snap = mt.dirty_snapshot();
        assert_eq!(snap.len(), 1);
        let acks: Vec<(RowKey, u64)> = snap.iter().map(|(k, _, s)| (k.clone(), *s)).collect();
        assert_eq!(acks[0].1, 1);

        // Post-snapshot write (what remove_flushed would have destroyed).
        mt.insert(key(1), row(0x02)); // v2, seq 2

        let freed = mt.mark_flushed(&acks);
        // v1 (32 bytes) was covered by the ack and drained; v2 survives DIRTY.
        assert_eq!(freed, 32, "only the acked v1 may be freed");
        match mt.get(&key(1)).expect("v2 must survive the ack") {
            MemRowValue::Row { bytes, .. } => assert_eq!(bytes[0], 0x02),
            other => panic!("expected v2 Row, got {other:?}"),
        }
        let dirty = mt.dirty_snapshot();
        assert_eq!(dirty.len(), 1, "v2 must still be flush input");
        assert_eq!(dirty[0].2, 2, "the dirty seq is v2's");
        assert_eq!(mt.bytes_dirty(), 32, "v2's bytes stay dirty");
        assert_eq!(mt.bytes_allocated(), 32);
    }

    #[test]
    fn mark_flushed_retags_update_to_row_and_drops_update_count() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::update(vec![0xAB; 32], 7)); // seq 1
        assert_eq!(mt.update_count(), 1);
        assert_eq!(mt.counter_key_rows(), 0);

        let freed = mt.mark_flushed(&[(key(1), 1)]);
        assert_eq!(freed, 0, "the retained value is not freed, only re-tagged");
        assert_eq!(mt.update_count(), 0, "re-tag must drop update_count");
        assert_eq!(mt.counter_key_rows(), 1, "re-tagged entry counts as Row");
        match mt
            .get(&key(1))
            .expect("re-tagged entry must remain readable")
        {
            MemRowValue::Row {
                bytes,
                schema_version,
            } => {
                assert_eq!(bytes[0], 0xAB, "payload unchanged by re-tag");
                assert_eq!(schema_version, 7, "schema_version preserved by re-tag");
            }
            other => panic!("expected Row after re-tag, got {other:?}"),
        }
        assert_eq!(mt.bytes_dirty(), 0, "acked entry is clean");
        assert_eq!(mt.bytes_clean(), 32);
    }

    #[test]
    fn mark_flushed_removes_acked_tombstone() {
        let mt = MemTable::new();
        mt.delete(key(1)); // seq 1 tombstone
        assert_eq!(mt.tombstone_count(), 1);
        let freed = mt.mark_flushed(&[(key(1), 1)]);
        assert_eq!(freed, 0, "tombstones are zero bytes");
        assert!(
            mt.get(&key(1)).is_none(),
            "clean tombstone is meaningless — the cold delete is committed"
        );
        assert_eq!(mt.tombstone_count(), 0);
        assert_eq!(mt.total_count(), 0);
    }

    #[test]
    fn mark_flushed_clean_dirty_byte_accounting_across_chains() {
        let mt = MemTable::new();
        // Chain: v1 (100) + v2 (200) — all dirty.
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0)); // seq 1
        mt.upsert(key(1), MemRowValue::row(vec![0u8; 200], 0)); // seq 2
        assert_eq!(mt.bytes_allocated(), 300);
        assert_eq!(mt.bytes_dirty(), 300);
        assert_eq!(mt.bytes_clean(), 0);

        // Ack at seq 2: v1 drained (freed), v2 retained clean.
        let freed = mt.mark_flushed(&[(key(1), 2)]);
        assert_eq!(freed, 100, "exactly the drained older version");
        assert_eq!(mt.bytes_allocated(), 200);
        assert_eq!(mt.bytes_dirty(), 0);
        assert_eq!(mt.bytes_clean(), 200);

        // Re-dirty: the clean 200 move back to dirty alongside the new 50.
        mt.upsert(key(1), MemRowValue::row(vec![0u8; 50], 0)); // seq 3
        assert_eq!(mt.bytes_allocated(), 250);
        assert_eq!(mt.bytes_dirty(), 250, "re-dirtied chain is fully dirty");
        assert_eq!(mt.bytes_clean(), 0);

        // Ack at seq 3: v2 (200) drained, v3 (50) retained clean.
        let freed = mt.mark_flushed(&[(key(1), 3)]);
        assert_eq!(freed, 200);
        assert_eq!(mt.bytes_allocated(), 50);
        assert_eq!(mt.bytes_dirty(), 0);
        assert_eq!(mt.bytes_clean(), 50);
    }

    #[test]
    fn mark_flushed_serves_pinned_readers_unchanged() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // seq 1
        mt.insert(key(2), row(0x02)); // seq 2
        mt.mark_flushed(&[(key(1), 1), (key(2), 2)]);
        // Clean entries still serve their version to pinned readers.
        assert!(mt.get_with_seq(&key(1), Some(1)).unwrap().is_row());
        assert!(mt.get_with_seq(&key(2), Some(2)).unwrap().is_row());
        assert!(mt.get_with_seq(&key(1), None).unwrap().is_row());
        // Below the first write: still omitted (falls through to cold).
        assert!(mt.get_with_seq(&key(1), Some(0)).is_none());
        let snap = mt.snapshot_with_seq(Some(2));
        assert_eq!(
            snap.len(),
            2,
            "snapshot_with_seq unchanged for pinned readers"
        );
    }

    #[test]
    fn mark_flushed_does_not_bump_epoch() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        let e = mt.epoch();
        mt.mark_flushed(&[(key(1), 1)]);
        assert_eq!(
            mt.epoch(),
            e,
            "mark_flushed must not bump epoch — observable values unchanged"
        );
    }

    #[test]
    fn mark_flushed_duplicate_ack_is_idempotent() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 64], 0));
        assert_eq!(mt.mark_flushed(&[(key(1), 1)]), 0);
        assert_eq!(
            mt.mark_flushed(&[(key(1), 1)]),
            0,
            "duplicate ack frees nothing"
        );
        assert_eq!(mt.bytes_allocated(), 64);
        assert_eq!(mt.bytes_clean(), 64);
        // One eviction still frees exactly one chain.
        assert_eq!(mt.evict_clean(u64::MAX, None), 64);
        assert_eq!(mt.bytes_allocated(), 0);
    }

    // ── insert_clean (write-through path) ─────────────────────────────────────

    #[test]
    fn insert_clean_starts_clean_and_readable() {
        let mt = MemTable::new();
        mt.insert_clean(key(1), MemRowValue::row(vec![0xCC; 64], 0));
        assert!(mt.get(&key(1)).unwrap().is_row());
        assert_eq!(mt.bytes_allocated(), 64);
        assert_eq!(mt.bytes_dirty(), 0, "write-through entry is never dirty");
        assert!(mt.dirty_snapshot().is_empty(), "never flush input");
        // Evictable immediately.
        assert_eq!(mt.evict_clean(u64::MAX, None), 64);
        assert!(mt.get(&key(1)).is_none());
    }

    #[test]
    fn insert_clean_bumps_epoch_and_drains_prior_versions() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0)); // dirty v1
        let e = mt.epoch();
        mt.insert_clean(key(1), MemRowValue::row(vec![0u8; 30], 0));
        assert!(mt.epoch() > e, "insert_clean changes the observable value");
        // Prior v1 was covered by the write-through ack and drained.
        assert_eq!(mt.bytes_allocated(), 30);
        assert_eq!(mt.bytes_dirty(), 0);
    }

    // ── evict_clean ───────────────────────────────────────────────────────────

    #[test]
    fn evict_clean_oldest_first_and_skips_redirtied() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 64], 0)); // seq 1
        mt.insert(key(2), MemRowValue::row(vec![0u8; 64], 0)); // seq 2

        // Ack in order: queue is [key1, key2] (oldest-acked first).
        mt.mark_flushed(&[(key(1), 1), (key(2), 2)]);
        // Re-dirty key 1: its queue entry is now stale.
        mt.upsert(key(1), MemRowValue::row(vec![0u8; 64], 0)); // seq 3

        // Ask for one entry's worth: key1 is popped first but skipped
        // (re-dirtied); key2 — the oldest still-clean entry — is evicted.
        let freed = mt.evict_clean(1, None);
        assert_eq!(freed, 64);
        assert!(mt.get(&key(1)).is_some(), "re-dirtied key must survive");
        assert!(mt.get(&key(2)).is_none(), "clean key evicted");
        assert_eq!(mt.bytes_dirty(), 128, "key1's chain (64+64) is dirty");
        assert_eq!(mt.bytes_clean(), 0);
    }

    #[test]
    fn evict_clean_respects_min_age() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 64], 0));
        mt.mark_flushed(&[(key(1), 1)]);
        // Entry was written just now: a 60 s floor must spare it...
        assert_eq!(mt.evict_clean(u64::MAX, Some(60)), 0);
        assert!(mt.get(&key(1)).is_some(), "young clean entry retained");
        // ...and the queue entry must survive for a later sweep.
        assert_eq!(
            mt.evict_clean(u64::MAX, Some(0)),
            64,
            "0 floor = retain nothing"
        );
        assert!(mt.get(&key(1)).is_none());
    }

    #[test]
    fn evict_clean_never_touches_dirty_entries() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01));
        assert_eq!(mt.evict_clean(u64::MAX, None), 0, "nothing clean to evict");
        assert!(mt.get(&key(1)).is_some());
        assert_eq!(mt.bytes_dirty(), 32);
    }

    #[test]
    fn evict_clean_does_not_bump_epoch() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 64], 0));
        mt.mark_flushed(&[(key(1), 1)]);
        let e = mt.epoch();
        mt.evict_clean(u64::MAX, None);
        assert_eq!(
            mt.epoch(),
            e,
            "evict_clean must not bump epoch — clean entries are cold-identical"
        );
    }

    // ── O(1) per-kind counters ────────────────────────────────────────────────

    #[test]
    fn kind_counters_consistent_after_mixed_workload() {
        let mt = MemTable::new();
        // Counter-keyed inserts.
        for i in 0..5i64 {
            mt.insert(key(i), row(i as u8)); // seqs 1..=5
        }
        // PK-keyed updates over two of them.
        mt.upsert(key(1), MemRowValue::update(vec![0x11; 32], 0)); // seq 6
        mt.upsert(key(2), MemRowValue::update(vec![0x22; 32], 0)); // seq 7

        // Deletes (one over an update, one over a row, one over a fresh key).
        mt.delete(key(2)); // seq 8
        mt.delete(key(3)); // seq 9
        mt.delete(key(99)); // seq 10

        let (rows, updates, tombs) = recount(&mt);
        assert_eq!(mt.counter_key_rows(), rows);
        assert_eq!(mt.update_count(), updates);
        assert_eq!(mt.tombstone_count(), tombs);
        assert_eq!((rows, updates, tombs), (2, 1, 3));

        // Flush ack of everything dirty, then verify the counters again
        // (tombstones removed, the update re-tagged Row, rows clean).
        let acks: Vec<(RowKey, u64)> = mt
            .dirty_snapshot()
            .into_iter()
            .map(|(k, _, s)| (k, s))
            .collect();
        mt.mark_flushed(&acks);
        let (rows, updates, tombs) = recount(&mt);
        assert_eq!(mt.counter_key_rows(), rows);
        assert_eq!(mt.update_count(), updates);
        assert_eq!(mt.tombstone_count(), tombs);
        assert_eq!((rows, updates, tombs), (3, 0, 0));

        // Evict everything clean and verify the counters drain to zero.
        mt.evict_clean(u64::MAX, None);
        assert_eq!(mt.counter_key_rows(), 0);
        assert_eq!(mt.update_count(), 0);
        assert_eq!(mt.tombstone_count(), 0);
        assert_eq!(mt.bytes_allocated(), 0);
        assert_eq!(mt.bytes_dirty(), 0);
    }

    #[test]
    fn remove_flushed_maintains_dirty_bytes_and_counters() {
        let mt = MemTable::new();
        mt.insert(key(1), MemRowValue::row(vec![0u8; 100], 0)); // dirty
        mt.insert(key(2), MemRowValue::row(vec![0u8; 50], 0)); // → clean below
        mt.delete(key(3)); // dirty tombstone
        mt.mark_flushed(&[(key(2), 2)]);
        assert_eq!(mt.bytes_dirty(), 100);
        assert_eq!(mt.bytes_clean(), 50);

        mt.remove_flushed(&[key(1), key(2), key(3)]);
        assert_eq!(mt.bytes_allocated(), 0);
        assert_eq!(mt.bytes_dirty(), 0);
        assert_eq!(mt.counter_key_rows(), 0);
        assert_eq!(mt.tombstone_count(), 0);
        // The stale clean-queue entry for key 2 is dropped on pop.
        assert_eq!(mt.evict_clean(u64::MAX, None), 0);
    }

    #[test]
    fn dirty_snapshot_skips_clean_entries() {
        let mt = MemTable::new();
        mt.insert(key(1), row(0x01)); // seq 1
        mt.insert(key(2), row(0x02)); // seq 2
        mt.mark_flushed(&[(key(1), 1)]);
        let dirty = mt.dirty_snapshot();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, key(2));
        assert_eq!(dirty[0].2, 2);
        // Plain snapshot still sees both (clean entries serve reads).
        assert_eq!(mt.snapshot().len(), 2);
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
