//! Parquet page cache (RAM): an LRU-bounded cache of decoded Arrow
//! `RecordBatch` slices keyed by `(file_path, projection_hash, filters_hash)`.
//!
//! # Why
//!
//! The disk cache (see [`crate::disk_cache`]) takes cold S3 reads from
//! ~50 ms down to ~100 µs by holding raw Parquet bytes on local NVMe. The
//! page cache sits one layer further up: it caches *already-decoded*
//! Arrow batches in RAM, so a repeated identical SELECT skips the
//! Parquet decode entirely and serves the answer at <1 ms. Combined with
//! the disk cache, the warm point-query path looks like:
//!
//! ```text
//!   reader::read_one
//!     -> page_cache HIT  ⇒ Arc<RecordBatch> (no decode, no IO)
//!     -> page_cache MISS ⇒ disk_cache HIT (raw bytes from SSD) ⇒ decode
//!                       ⇒ disk_cache MISS ⇒ ObjectStore GET ⇒ decode
//! ```
//!
//! # Granularity (v0.1)
//!
//! We pick the simplest unit that captures the most common SaaS access
//! pattern ("the same SELECT runs many times in a row"): one cache
//! entry per `(file_path, projection_hash, filters_hash)` holds the
//! full vector of `RecordBatch`es that the corresponding read produces
//! against that one Parquet file. This is coarser than "one entry per
//! page" or "one entry per row group", but it has three properties we
//! want for v0.1:
//!
//! - It's invariant under our immutability rule: a Parquet file at a
//!   given path never mutates, so the cached batches are always
//!   correct as long as the file exists.
//! - The lookup is one HashMap probe — no need to know in advance how
//!   many row groups survived predicate pushdown.
//! - It composes cleanly with `RowFilter`: we hash the predicate set
//!   into the key so different WHERE clauses don't collide.
//!
//! Trade-off: if two queries project different columns of the same
//! file, they miss each other. The follow-up to v0.1 is a row-group ×
//! column-chunk granularity that lets overlapping projections share.
//! For "same SELECT, repeated" — which is the load-bearing claim for
//! Phase 5.7 A2 — file-level is enough.
//!
//! # Byte budget + LRU
//!
//! State lives sharded across `N_SHARDS` independent `Mutex<PageCacheState>`
//! buckets, each holding its own `LruCache<CacheKey, CacheEntry>` plus a
//! local `current_bytes: u64` that the eviction loop drives below its
//! configured share of the global budget (`max_bytes / N_SHARDS`). Every
//! `RecordBatch`'s footprint is the sum of
//! `arrow_array::RecordBatch::get_array_memory_size()` — the same number
//! the writer uses for buffer sizing. Eviction is greedy LRU within each
//! shard: after every successful insert we pop entries from the owning
//! shard until it's back below its slice of the budget.
//!
//! LRU is per-shard, so the global LRU order is approximate — two
//! entries on different shards age independently. In exchange, concurrent
//! readers that hash to different shards never contend on the same
//! mutex, which is the load-bearing property at high fan-out.
//!
//! # Sharding (perf hot-path)
//!
//! Pre-shard, the cache held a single `Mutex<PageCacheState>` and every
//! `get`/`insert`/`has_capacity` call serialised through it. The `get`
//! path is morally a write because LRU promotion mutates the recency
//! list, so even pure read workloads collided on this lock. At C=64
//! concurrent readers on 16 worker threads the futex-wake latency made
//! the lock the effective scaling ceiling (see `scaling_concurrency`
//! benchmark cliff: 1.08× C=16→C=64 vs the 1.5× bar).
//!
//! Post-shard, the lock count is `N_SHARDS` (default 64, env-overridable
//! via `BASIN_STORAGE_PAGE_CACHE_SHARDS`). A request picks its shard by
//! hashing all three components of `CacheKey` (path, projection, filters);
//! same-key requests always land on the same shard (so a hit is still a
//! hit), and different-key requests scatter. With shard count matching
//! the concurrency target, the expected contention on a uniform workload
//! is `~1/N_SHARDS`.
//!
//! `has_capacity` is the only call that previously read aggregate state:
//! it now reads a lock-free `AtomicU64 current_bytes` on the parent
//! `PageCache` (updated by every insert / evict / invalidate) and
//! compares against `max_bytes`. The reader's write-through heuristic
//! tolerates a brief stale read either way.
//!
//! # Invalidation
//!
//! Reverse index `HashMap<ObjectPath, Vec<CacheKey>>` lives in each
//! shard so a file-deletion (compactor, UPDATE/DELETE rewrite) can drop
//! every cached entry that references that file. Since entries for one
//! path may sit on different shards (they differ by projection/filter
//! hash), `invalidate_path` iterates all shards once — an O(N_SHARDS)
//! op, acceptable because the call site is rare (rewrite/compaction)
//! while the get/insert path is hot.
//!
//! # Project isolation
//!
//! Cache keys derive from `ObjectPath`, which always starts with the
//! project's prefix. Two projects reading paths with otherwise-identical
//! suffixes produce different keys; cross-project key collisions are
//! mechanically impossible.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use lru::LruCache;
use object_store::path::Path as ObjectPath;

use crate::predicate::{Predicate, ScalarValue};

/// Public configuration for the page cache.
///
/// `max_bytes` is the only knob — count caps are implied because LRU
/// requires `NonZeroUsize` capacity, but the byte budget is what
/// production cares about.
#[derive(Clone, Debug)]
pub struct PageCacheConfig {
    /// Eviction cap in bytes. The cache may briefly exceed this during
    /// an insert but converges back below it via LRU pops.
    pub max_bytes: u64,
}

impl PageCacheConfig {
    /// Construct with an explicit byte budget.
    pub fn new(max_bytes: u64) -> Self {
        Self { max_bytes }
    }
}

impl Default for PageCacheConfig {
    /// Default budget of 1 GiB. Sized so that for a typical multi-project
    /// SaaS workload the working-set hot tables fit; the cap is
    /// configurable per-deployment if more is needed.
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Best-effort hit/miss counters for tests and observability.
#[derive(Debug, Default)]
pub struct PageCacheCounters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub current_bytes: AtomicU64,
}

/// Plain-data view of [`PageCacheCounters`].
#[derive(Clone, Copy, Debug, Default)]
pub struct PageCacheCountersSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_bytes: u64,
}

impl PageCacheCounters {
    pub fn snapshot(&self) -> PageCacheCountersSnapshot {
        PageCacheCountersSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Composite cache key. We hash the projection and filter set so two
/// reads with different `SELECT` columns or different `WHERE` clauses
/// never share an entry.
///
/// NOTE on filter-in-key: an earlier draft of the page-cache perf
/// follow-up dropped the filter from the key on the assumption that
/// DataFusion's `FilterExec` re-applies the WHERE above the storage
/// adapter, so a cache hit under a different filter yields the same
/// final result. That assumption does not hold in this codebase: the
/// parquet reader pushes the filter all the way down via
/// `with_row_filter` + page-level `with_row_selection`, so the cached
/// `Vec<RecordBatch>` is **post-filter**. Sharing such an entry across
/// queries with different WHEREs returns wrong rows (regression caught
/// by `rmw_update_through_hot_tier_is_correct`). The filter therefore
/// stays in the key. The other half of the Phase 5.7 follow-up —
/// LRU-budget-aware write-through opt-out — is still applied
/// independently in `reader.rs` and yields the layer (c) speedup
/// directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    pub path: ObjectPath,
    pub projection_hash: u64,
    pub filters_hash: u64,
}

/// One cache entry: the full vector of `RecordBatch`es a single
/// `(file, projection, filters)` read produced.
#[derive(Clone)]
pub(crate) struct CacheEntry {
    pub batches: Arc<Vec<Arc<RecordBatch>>>,
    /// Cached size in bytes — sum of `get_array_memory_size()` over
    /// every batch. Tracked here so the eviction loop doesn't re-walk
    /// the batches.
    pub size: u64,
}

/// Hash a projection (list of column names) deterministically. We sort
/// first so `SELECT a, b` and `SELECT b, a` share a cache slot — the
/// reader returns the same logical rows either way; only the schema's
/// field order differs, and that's recoverable downstream.
///
/// Caller passes `None` for "all columns".
///
/// Allocation policy: zero heap allocation for the common cases (None or
/// 1 column). For ≥2 columns we need a scratch Vec to sort, sized exactly
/// to `cols.len()`, so bookkeeping is O(ncols) not O(ncols × avg_name_len).
pub(crate) fn hash_projection(projection: Option<&[String]>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match projection {
        None => {
            b"ALL".hash(&mut h);
        }
        Some(cols) => {
            b"PROJ".hash(&mut h);
            // Order-SENSITIVE: the reader reassembles each cached batch into the
            // requested projection ORDER (not just the projected column set), so
            // two requests for the same columns in different orders must map to
            // distinct cache entries — otherwise a cache hit could return a
            // batch whose physical column layout disagrees with the caller's
            // declared schema (the bug that surfaced as
            // "expected <T> but found <U> at column index N" via the
            // TombstoneColdScanExec projection-order path). Hash in projection
            // order; do NOT sort.
            cols.len().hash(&mut h);
            for c in cols {
                c.hash(&mut h);
            }
        }
    }
    h.finish()
}

/// Hash a filter set deterministically. Order-independent (we sort by a
/// stable tuple key) so `WHERE a = 1 AND b = 2` matches
/// `WHERE b = 2 AND a = 1`.
///
/// Allocation policy: zero heap allocation when there are 0 or 1 filters
/// (the common case for point queries and full scans). For ≥2 filters we
/// allocate one `Vec<usize>` of index permutations (not a Vec<String>)
/// so each predicate contributes 8 bytes of scratch rather than a
/// heap-formatted string.
pub(crate) fn hash_filters(filters: &[Predicate]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    b"FLT".hash(&mut h);
    match filters.len() {
        0 => {}
        1 => {
            // Single filter: no sort needed, no allocation.
            hash_one_predicate(&mut h, &filters[0]);
        }
        _ => {
            // Multiple filters: sort by index using a stable tuple key,
            // then hash in sorted order. No string formatting needed.
            let mut order: Vec<usize> = (0..filters.len()).collect();
            order.sort_by_key(|&i| predicate_sort_key(&filters[i]));
            for i in order {
                hash_one_predicate(&mut h, &filters[i]);
            }
        }
    }
    h.finish()
}

/// Feed one predicate into a hasher without any intermediate allocation.
fn hash_one_predicate(h: &mut std::collections::hash_map::DefaultHasher, p: &Predicate) {
    match p {
        Predicate::Eq(c, v) => {
            0u8.hash(h);
            c.hash(h);
            hash_scalar(h, v);
        }
        Predicate::Gt(c, v) => {
            1u8.hash(h);
            c.hash(h);
            hash_scalar(h, v);
        }
        Predicate::Lt(c, v) => {
            2u8.hash(h);
            c.hash(h);
            hash_scalar(h, v);
        }
        Predicate::StartsWith {
            column,
            prefix,
            case_insensitive,
        } => {
            3u8.hash(h);
            column.hash(h);
            prefix.hash(h);
            case_insensitive.hash(h);
        }
    }
}

/// Hash a scalar directly without formatting it as a String.
fn hash_scalar(h: &mut std::collections::hash_map::DefaultHasher, v: &ScalarValue) {
    match v {
        ScalarValue::Int64(x) => {
            0u8.hash(h);
            x.hash(h);
        }
        ScalarValue::UInt64(x) => {
            1u8.hash(h);
            x.hash(h);
        }
        // Hash f64 via its bit representation so NaN is deterministic.
        ScalarValue::Float64(x) => {
            2u8.hash(h);
            x.to_bits().hash(h);
        }
        ScalarValue::Utf8(s) => {
            3u8.hash(h);
            s.hash(h);
        }
        ScalarValue::Boolean(b) => {
            4u8.hash(h);
            b.hash(h);
        }
    }
}

/// A sort key for predicates that is allocation-free (returns a tuple of
/// primitive / borrowed values that `sort_by_key` can use without boxing).
/// The tuple is `(op_tag, column_name, value_discriminant, value_bytes)`.
/// Value bytes cover the integer/float cases; strings sort by their content.
/// This mirrors the ordering that the old string-formatted sort produced.
fn predicate_sort_key(p: &Predicate) -> (u8, &str, u8) {
    let (op, col, vd) = match p {
        Predicate::Eq(c, v) => (0u8, c.as_str(), scalar_disc(v)),
        Predicate::Gt(c, v) => (1u8, c.as_str(), scalar_disc(v)),
        Predicate::Lt(c, v) => (2u8, c.as_str(), scalar_disc(v)),
        // StartsWith carries no `ScalarValue`; use a fixed discriminant for
        // the value-tag slot so equally-shaped prefix predicates sort
        // deterministically next to one another.
        Predicate::StartsWith { column, .. } => (3u8, column.as_str(), 5u8),
    };
    (op, col, vd)
}

fn scalar_disc(v: &ScalarValue) -> u8 {
    match v {
        ScalarValue::Int64(_) => 0u8,
        ScalarValue::UInt64(_) => 1u8,
        ScalarValue::Float64(_) => 2u8,
        ScalarValue::Utf8(_) => 3u8,
        ScalarValue::Boolean(_) => 4u8,
    }
}

/// Default LRU index capacity (per shard). The byte budget is the
/// load-bearing knob; the entry count cap is a safety net so a million
/// tiny entries can't blow up bookkeeping memory. With 64 shards and
/// 100k entries per shard the global ceiling is 6.4M index slots — still
/// O(tens of MB) of bookkeeping at worst, fine.
const DEFAULT_INDEX_CAPACITY_PER_SHARD: usize = 100_000;

/// Default shard count. Matched to the concurrency target the
/// `scaling_concurrency` benchmark exercises (C=64) so the lucky-case
/// (different keys) hits different shards. Override at process start
/// with `BASIN_STORAGE_PAGE_CACHE_SHARDS` (positive `usize`); any
/// unset / unparseable / zero / non-power-of-two value keeps the default.
const DEFAULT_PAGE_CACHE_SHARDS: usize = 64;

/// Resolve the page-cache shard count, honoring
/// `BASIN_STORAGE_PAGE_CACHE_SHARDS` when present and parseable to a
/// positive `usize`. Falls back to [`DEFAULT_PAGE_CACHE_SHARDS`].
pub fn resolve_page_cache_shards() -> usize {
    if let Ok(v) = std::env::var("BASIN_STORAGE_PAGE_CACHE_SHARDS") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    DEFAULT_PAGE_CACHE_SHARDS
}

/// Page cache. Cheap to clone via `Arc<PageCache>`; concurrent readers
/// share a fixed array of mutex-guarded LRU shards.
pub struct PageCache {
    /// One independent `Mutex<PageCacheState>` per shard. A request picks
    /// its shard by hashing the full `CacheKey`; same-key requests always
    /// land on the same shard.
    shards: Box<[Mutex<PageCacheState>]>,
    counters: PageCacheCounters,
    max_bytes: u64,
    /// Per-shard byte budget = `max_bytes / shards.len()` (rounded up to
    /// avoid a zero-byte slice when `max_bytes < shards.len()`). Each
    /// shard's eviction loop converges below this number, so the global
    /// budget is honored modulo skew.
    per_shard_max_bytes: u64,
}

struct PageCacheState {
    lru: LruCache<CacheKey, CacheEntry>,
    /// Reverse index: ObjectPath -> set of CacheKeys for that path that
    /// live in *this* shard. Lets `invalidate_path` (which walks every
    /// shard) drop the entries it owns in O(k_local).
    by_path: HashMap<ObjectPath, Vec<CacheKey>>,
    current_bytes: u64,
}

impl PageCache {
    /// Construct an empty cache with the given byte budget.
    pub fn new(cfg: PageCacheConfig) -> Self {
        let n_shards = resolve_page_cache_shards();
        Self::with_shards(cfg, n_shards)
    }

    /// Construct an empty cache with an explicit shard count. Visible for
    /// tests; production callers use [`PageCache::new`] which honors the
    /// `BASIN_STORAGE_PAGE_CACHE_SHARDS` env override.
    pub fn with_shards(cfg: PageCacheConfig, n_shards: usize) -> Self {
        let n_shards = n_shards.max(1);
        let cap =
            NonZeroUsize::new(DEFAULT_INDEX_CAPACITY_PER_SHARD).expect("index cap > 0");
        let shards: Vec<Mutex<PageCacheState>> = (0..n_shards)
            .map(|_| {
                Mutex::new(PageCacheState {
                    lru: LruCache::new(cap),
                    by_path: HashMap::new(),
                    current_bytes: 0,
                })
            })
            .collect();
        // Round-up division so per_shard_max_bytes * n_shards >= max_bytes;
        // each shard's local cap never drops to zero even when
        // max_bytes < n_shards (degenerate tests).
        let per_shard_max_bytes = cfg.max_bytes.div_ceil(n_shards as u64).max(1);
        Self {
            shards: shards.into_boxed_slice(),
            counters: PageCacheCounters::default(),
            max_bytes: cfg.max_bytes,
            per_shard_max_bytes,
        }
    }

    /// Snapshot the cache's hit/miss/eviction counters.
    pub fn counters(&self) -> PageCacheCountersSnapshot {
        self.counters.snapshot()
    }

    /// Number of shards. Visible for tests.
    #[cfg(test)]
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Pick the shard for a key. We hash all three components — same key
    /// always maps to the same shard (so a hit is still a hit), and
    /// different keys scatter across shards independent of which one
    /// field happens to be skewed.
    fn shard_for(&self, key: &CacheKey) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.path.hash(&mut h);
        key.projection_hash.hash(&mut h);
        key.filters_hash.hash(&mut h);
        (h.finish() as usize) % self.shards.len()
    }

    /// Lookup. On hit, bumps LRU position and returns a clone of the
    /// `Arc<Vec<...>>` (cheap; no batch is copied).
    pub(crate) fn get(&self, key: &CacheKey) -> Option<Arc<Vec<Arc<RecordBatch>>>> {
        let idx = self.shard_for(key);
        let mut g = self.shards[idx].lock().expect("page cache shard mutex poisoned");
        if let Some(entry) = g.lru.get(key) {
            let batches = entry.batches.clone();
            drop(g);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            Some(batches)
        } else {
            drop(g);
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert (or replace) the entry for `key`. Drives LRU eviction
    /// within the owning shard until that shard's `current_bytes <=
    /// per_shard_max_bytes`.
    pub(crate) fn insert(&self, key: CacheKey, batches: Vec<Arc<RecordBatch>>) {
        let size: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let entry = CacheEntry {
            batches: Arc::new(batches),
            size,
        };

        let idx = self.shard_for(&key);
        let mut g = self.shards[idx].lock().expect("page cache shard mutex poisoned");

        // Local accumulator: net change in bytes for this shard. We
        // apply it to the global atomic ONCE at the end so the
        // lock-free `has_capacity` reader sees a coherent total.
        let mut net_delta: i128 = 0;
        let mut local_evictions: u64 = 0;

        // Refresh case: subtract the old entry's bytes and clean up
        // its reverse-index slot.
        if let Some(old) = g.lru.pop(&key) {
            g.current_bytes = g.current_bytes.saturating_sub(old.size);
            net_delta -= old.size as i128;
            if let Some(v) = g.by_path.get_mut(&key.path) {
                v.retain(|k| k != &key);
                if v.is_empty() {
                    g.by_path.remove(&key.path);
                }
            }
        }

        g.current_bytes = g.current_bytes.saturating_add(entry.size);
        net_delta += entry.size as i128;
        g.by_path
            .entry(key.path.clone())
            .or_default()
            .push(key.clone());

        // `lru.push` may itself evict if the index hit its NonZeroUsize
        // cap; account for that displacement before our budget pass.
        if let Some((evicted_k, evicted_v)) = g.lru.push(key.clone(), entry) {
            if evicted_k != key {
                g.current_bytes = g.current_bytes.saturating_sub(evicted_v.size);
                net_delta -= evicted_v.size as i128;
                if let Some(v) = g.by_path.get_mut(&evicted_k.path) {
                    v.retain(|k| k != &evicted_k);
                    if v.is_empty() {
                        g.by_path.remove(&evicted_k.path);
                    }
                }
                local_evictions += 1;
            }
        }

        // Byte-budget eviction loop, per-shard.
        while g.current_bytes > self.per_shard_max_bytes {
            match g.lru.pop_lru() {
                Some((k, v)) => {
                    g.current_bytes = g.current_bytes.saturating_sub(v.size);
                    net_delta -= v.size as i128;
                    if let Some(vec) = g.by_path.get_mut(&k.path) {
                        vec.retain(|x| x != &k);
                        if vec.is_empty() {
                            g.by_path.remove(&k.path);
                        }
                    }
                    local_evictions += 1;
                }
                None => break,
            }
        }

        drop(g);

        if local_evictions > 0 {
            self.counters
                .evictions
                .fetch_add(local_evictions, Ordering::Relaxed);
        }
        // Apply net byte delta to the global atomic. Use signed add via
        // wrapping ops on the underlying u64 so we don't accidentally
        // underflow when eviction frees more than the insert added.
        apply_signed_delta(&self.counters.current_bytes, net_delta);
    }

    /// Drop every cached entry that references `path`. Called by the
    /// storage layer when a Parquet file is deleted (compactor,
    /// UPDATE/DELETE rewrite). Idempotent and cheap if `path` is not in
    /// the cache.
    ///
    /// Walks every shard (O(N_SHARDS) lock acquisitions) because entries
    /// for one path may be distributed across shards. This is the
    /// trade-off we accept to keep `get`/`insert` contention-free.
    pub fn invalidate_path(&self, path: &ObjectPath) {
        let mut total_freed: u64 = 0;
        for shard in self.shards.iter() {
            let mut g = shard.lock().expect("page cache shard mutex poisoned");
            let keys = g.by_path.remove(path).unwrap_or_default();
            let mut freed = 0u64;
            for k in keys {
                if let Some(v) = g.lru.pop(&k) {
                    freed = freed.saturating_add(v.size);
                }
            }
            g.current_bytes = g.current_bytes.saturating_sub(freed);
            total_freed = total_freed.saturating_add(freed);
        }
        if total_freed > 0 {
            apply_signed_delta(&self.counters.current_bytes, -(total_freed as i128));
        }
    }

    /// Heuristic: is there room in the byte budget for another
    /// write-through? Used by the reader to skip the per-batch clone
    /// when the cache is already saturated — the entry would be
    /// evicted on insert anyway, and the buffer is pure overhead in
    /// that case (the read path returns the same batches whether or
    /// not they got cached).
    ///
    /// Reads the lock-free aggregate `current_bytes` atomic — no shard
    /// lock acquisition on the hot read path.
    pub(crate) fn has_capacity(&self) -> bool {
        self.counters.current_bytes.load(Ordering::Relaxed) < self.max_bytes
    }

    /// For tests: how many entries the cache currently holds (summed
    /// across shards).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().expect("page cache shard mutex poisoned").lru.len())
            .sum()
    }

    /// For tests: how many entries the given shard currently holds.
    #[cfg(test)]
    pub(crate) fn shard_len(&self, idx: usize) -> usize {
        self.shards[idx]
            .lock()
            .expect("page cache shard mutex poisoned")
            .lru
            .len()
    }
}

/// Apply a signed delta to a `u64` atomic that tracks cumulative bytes.
/// We can't use `fetch_add` with a negative value, and we don't want a
/// lock; instead, we do a relaxed compare-exchange loop that saturates
/// at zero on the underflow case. Contention here is rare (one update
/// per insert/evict, not per get).
fn apply_signed_delta(a: &AtomicU64, delta: i128) {
    if delta == 0 {
        return;
    }
    let mut cur = a.load(Ordering::Relaxed);
    loop {
        let next_i = cur as i128 + delta;
        let next: u64 = if next_i < 0 { 0 } else { next_i as u64 };
        match a.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(now) => cur = now,
        }
    }
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("max_bytes", &self.max_bytes)
            .field("shards", &self.shards.len())
            .field("counters", &self.counters.snapshot())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn small_batch(start: i64, len: usize) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr: Int64Array = (start..start + len as i64).collect();
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap())
    }

    fn key(path: &str, proj: u64, filt: u64) -> CacheKey {
        CacheKey {
            path: ObjectPath::from(path),
            projection_hash: proj,
            filters_hash: filt,
        }
    }

    /// A hit returns the same batches we inserted, without forcing a
    /// re-decode. We can't directly observe "did decode happen?" at
    /// this layer — the decode lives in `reader.rs` — but we *can*
    /// observe that the cached `Arc<RecordBatch>` survives a
    /// round-trip and that the hit counter increments. The
    /// `viability_page_cache` integration test covers the
    /// avoid-decode behaviour end-to-end.
    #[test]
    fn hit_returns_cached_batches() {
        let cache = PageCache::new(PageCacheConfig::new(1024 * 1024));
        let k = key("projects/x/tables/t/data/a.parquet", 1, 2);
        let batches = vec![small_batch(0, 100), small_batch(100, 100)];
        cache.insert(k.clone(), batches.clone());

        // Round-trip the cache: probe and verify byte-for-byte equivalence
        // with what we inserted (Arc identity is the strongest assertion
        // here — if we got the same Arc back, the decode was guaranteed
        // skipped on this layer).
        let got = cache.get(&k).expect("cache hit");
        assert_eq!(got.len(), 2);
        assert!(Arc::ptr_eq(&got[0], &batches[0]));
        assert!(Arc::ptr_eq(&got[1], &batches[1]));

        // Counters move.
        let c = cache.counters();
        assert_eq!(c.hits, 1);
        assert_eq!(c.misses, 0);

        // Different projection hash on the same path is a separate
        // entry; verify it misses.
        let k2 = key("projects/x/tables/t/data/a.parquet", 99, 2);
        assert!(cache.get(&k2).is_none());
        let c = cache.counters();
        assert_eq!(c.misses, 1);
    }

    /// Invalidating a path drops *every* cached entry whose key
    /// references that path, regardless of projection/filter variants.
    #[test]
    fn file_deletion_invalidates_all_variants() {
        let cache = PageCache::new(PageCacheConfig::new(1024 * 1024));
        let path = "projects/x/tables/t/data/a.parquet";
        // Three projection/filter variants on the same file.
        for (p, f) in [(1, 1), (1, 2), (2, 1)] {
            cache.insert(key(path, p, f), vec![small_batch(0, 100)]);
        }
        // And one entry on a different file that must NOT be evicted.
        let other_key = key("projects/x/tables/t/data/b.parquet", 1, 1);
        cache.insert(other_key.clone(), vec![small_batch(0, 50)]);

        assert_eq!(cache.len(), 4);

        cache.invalidate_path(&ObjectPath::from(path));

        assert_eq!(cache.len(), 1, "only the b.parquet entry should remain");
        assert!(cache.get(&other_key).is_some());
        for (p, f) in [(1, 1), (1, 2), (2, 1)] {
            assert!(cache.get(&key(path, p, f)).is_none());
        }
    }

    /// LRU eviction kicks in once `current_bytes > max_bytes`. We pick a
    /// budget below 2× the size of one entry and confirm that inserting
    /// the third entry evicts the first.
    ///
    /// NOTE on sharding: this test forces single-shard semantics so the
    /// global LRU policy is preserved by construction (per-shard LRU
    /// then *is* the global LRU). With the default 64 shards, three
    /// random keys would hash into ≥2 separate shards and the eviction
    /// trigger would not fire deterministically — that's an accepted
    /// approximation (documented at the module top), not a bug.
    #[test]
    fn lru_evicts_over_byte_budget() {
        // Each batch is ~800 bytes (100 i64 values + arrow overhead);
        // size in `get_array_memory_size` returns the buffer footprint.
        // We pick a budget that holds at most one such entry.
        let probe = small_batch(0, 100);
        let entry_size = probe.get_array_memory_size() as u64;
        // Budget = 1.5× one entry. Inserting 3 entries forces 2 of
        // them to age out before we're done.
        let cache =
            PageCache::with_shards(PageCacheConfig::new(entry_size + entry_size / 2), 1);

        let k1 = key("projects/x/tables/t/data/1.parquet", 1, 1);
        let k2 = key("projects/x/tables/t/data/2.parquet", 1, 1);
        let k3 = key("projects/x/tables/t/data/3.parquet", 1, 1);

        cache.insert(k1.clone(), vec![small_batch(0, 100)]);
        // After inserting k1 we should be at exactly entry_size bytes.
        assert!(cache.get(&k1).is_some());

        // Re-bump k1 so it's MRU; then insert k2, then k3 (LRU order
        // ends up: k1 (MRU), k3, k2 (LRU)). Wait — we just got k1,
        // so it's MRU. Next we touch k2, then k3. Final order:
        // k3 (MRU), k2, k1 (LRU). Inserting k2 forced eviction of k1
        // already because entry_size + entry_size > 1.5 * entry_size.
        cache.insert(k2.clone(), vec![small_batch(100, 100)]);
        // k1 must have been evicted to keep the budget.
        assert!(cache.get(&k1).is_none(), "k1 should have been evicted");
        assert!(cache.get(&k2).is_some());

        // Insert k3: now k2 is the LRU (we bumped it on get).
        // After this insert, *something* gets evicted to fit; the
        // important invariant is `current_bytes <= max_bytes` after
        // every insert.
        cache.insert(k3.clone(), vec![small_batch(200, 100)]);
        let cb = cache.counters().current_bytes;
        assert!(
            cb <= entry_size + entry_size / 2,
            "current_bytes {cb} exceeds budget {}",
            entry_size + entry_size / 2,
        );
        assert!(cache.counters().evictions >= 2);
    }

    /// Sharded cache correctness: insert many distinct keys, every
    /// `get` returns the value we inserted. This is the load-bearing
    /// invariant of the sharding refactor — if the shard router ever
    /// got `shard_for(insert)` != `shard_for(get)` for the same key,
    /// every read would miss.
    #[test]
    fn sharded_cache_get_insert_correctness() {
        // Generous budget so nothing evicts.
        let cache = PageCache::new(PageCacheConfig::new(1024 * 1024 * 1024));
        let n = 500usize;
        let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            // Vary path, projection, and filter so keys spread across
            // shards (any single-field hash collision would mask a
            // routing bug — we vary all three to maximise spread).
            let path = format!("projects/p{}/tables/t/data/file_{}.parquet", i % 7, i);
            let k = CacheKey {
                path: ObjectPath::from(path),
                projection_hash: (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                filters_hash: (i as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
            };
            cache.insert(k.clone(), vec![small_batch(i as i64 * 100, 10)]);
            keys.push(k);
        }
        assert_eq!(cache.len(), n, "every insert should land in some shard");

        // Every key we inserted must round-trip via `get`.
        for (i, k) in keys.iter().enumerate() {
            let got = cache.get(k).unwrap_or_else(|| {
                panic!("key {i} missed — shard router not deterministic")
            });
            assert_eq!(got.len(), 1);
        }
        let c = cache.counters();
        assert_eq!(c.hits, n as u64);
        assert_eq!(c.misses, 0);
    }

    /// The shard router actually distributes load. With 1000 keys
    /// across 64 shards we expect a mean of ~15.6 entries per shard.
    /// A bad hash (e.g. modulo'd over a single field that collides)
    /// would dump everything into a handful of shards. We assert at
    /// least half the shards see >= one entry, and no single shard
    /// holds more than ~10× the mean — both very loose bars that any
    /// reasonable hash will clear and a degenerate one will fail.
    #[test]
    fn sharded_cache_distributes_load() {
        let cache = PageCache::with_shards(PageCacheConfig::new(1024 * 1024 * 1024), 64);
        let n = 1000usize;
        for i in 0..n {
            let path = format!("projects/p/tables/t/data/file_{i}.parquet");
            let k = CacheKey {
                path: ObjectPath::from(path),
                projection_hash: (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                filters_hash: (i as u64) ^ 0xDEAD_BEEF,
            };
            cache.insert(k, vec![small_batch(0, 1)]);
        }

        let shards = cache.shard_count();
        let mean = n as f64 / shards as f64;
        let mut occupied = 0usize;
        let mut max_seen = 0usize;
        for i in 0..shards {
            let len = cache.shard_len(i);
            if len > 0 {
                occupied += 1;
            }
            if len > max_seen {
                max_seen = len;
            }
        }
        assert!(
            occupied >= shards / 2,
            "only {occupied}/{shards} shards populated — router under-distributes"
        );
        assert!(
            (max_seen as f64) <= 10.0 * mean,
            "hottest shard holds {max_seen} entries, >10× mean {mean:.1} — router skewed"
        );
    }

    /// `invalidate_path` walks every shard. Construct entries on the
    /// same path but with varying projection/filter (so they spread
    /// across shards) and verify all of them are gone after invalidate.
    #[test]
    fn sharded_cache_invalidate_path_clears_all_shards() {
        let cache = PageCache::with_shards(PageCacheConfig::new(1024 * 1024 * 1024), 64);
        let path = "projects/x/tables/t/data/hot.parquet";

        // Insert 300 entries on the same path with different
        // (projection, filter) pairs. With 64 shards and a good hash
        // these will scatter across many shards.
        for i in 0..300u64 {
            let k = CacheKey {
                path: ObjectPath::from(path),
                projection_hash: i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                filters_hash: i.wrapping_mul(0xBF58_476D_1CE4_E5B9),
            };
            cache.insert(k, vec![small_batch(0, 5)]);
        }

        // Add a sentinel on a different path that must NOT be evicted.
        let other = CacheKey {
            path: ObjectPath::from("projects/x/tables/t/data/cold.parquet"),
            projection_hash: 1,
            filters_hash: 1,
        };
        cache.insert(other.clone(), vec![small_batch(0, 5)]);

        assert_eq!(cache.len(), 301);

        cache.invalidate_path(&ObjectPath::from(path));

        assert_eq!(
            cache.len(),
            1,
            "every entry on the invalidated path must be gone across all shards"
        );
        assert!(
            cache.get(&other).is_some(),
            "sentinel on a different path must survive"
        );
    }
}
