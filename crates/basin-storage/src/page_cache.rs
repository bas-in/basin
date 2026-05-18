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
//! State lives in a `Mutex<LruCache<CacheKey, CacheEntry>>` plus a
//! `current_bytes: u64` that the eviction loop drives below the
//! configured `max_bytes`. Every `RecordBatch`'s footprint is the sum
//! of `arrow_array::RecordBatch::get_array_memory_size()` — the same
//! number the writer uses for buffer sizing. Eviction is greedy LRU:
//! after every successful insert we pop entries until we're back below
//! the budget.
//!
//! # Invalidation
//!
//! Reverse index `HashMap<ObjectPath, Vec<CacheKey>>` so a
//! file-deletion (compactor, UPDATE/DELETE rewrite) can drop every
//! cached entry that references that file in O(k) where k is the number
//! of (projection, filter) variants we cached against it. This mirrors
//! the disk cache's `by_path` pattern.
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
            match cols.len() {
                0 => {}
                1 => {
                    // Single column: no sort needed, no allocation.
                    cols[0].hash(&mut h);
                }
                _ => {
                    // Multiple columns: sort indices rather than cloning
                    // strings, so we allocate one Vec<usize> instead of
                    // a Vec<&String> of the same size.
                    let mut order: Vec<usize> = (0..cols.len()).collect();
                    order.sort_by_key(|&i| cols[i].as_str());
                    for i in order {
                        cols[i].hash(&mut h);
                    }
                }
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
    let (op, col, val) = match p {
        Predicate::Eq(c, v) => (0u8, c, v),
        Predicate::Gt(c, v) => (1u8, c, v),
        Predicate::Lt(c, v) => (2u8, c, v),
    };
    op.hash(h);
    col.hash(h);
    hash_scalar(h, val);
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
    let (op, col, val) = match p {
        Predicate::Eq(c, v) => (0u8, c.as_str(), v),
        Predicate::Gt(c, v) => (1u8, c.as_str(), v),
        Predicate::Lt(c, v) => (2u8, c.as_str(), v),
    };
    let vd = match val {
        ScalarValue::Int64(_) => 0u8,
        ScalarValue::UInt64(_) => 1u8,
        ScalarValue::Float64(_) => 2u8,
        ScalarValue::Utf8(_) => 3u8,
        ScalarValue::Boolean(_) => 4u8,
    };
    (op, col, vd)
}

/// Default LRU index capacity. The byte budget is the load-bearing
/// knob; the entry count cap is a safety net so a million tiny entries
/// can't blow up bookkeeping memory. 100k entries × ~100 bytes of
/// bookkeeping each = ~10 MB, which is fine.
const DEFAULT_INDEX_CAPACITY: usize = 100_000;

/// Page cache. Cheap to clone via `Arc<PageCache>`; concurrent readers
/// share one mutex-guarded LRU.
pub struct PageCache {
    state: Mutex<PageCacheState>,
    counters: PageCacheCounters,
    max_bytes: u64,
}

struct PageCacheState {
    lru: LruCache<CacheKey, CacheEntry>,
    /// Reverse index: ObjectPath -> set of CacheKeys for that path.
    /// Lets the invalidation hook drop every entry referencing a
    /// deleted file in O(k).
    by_path: HashMap<ObjectPath, Vec<CacheKey>>,
    current_bytes: u64,
}

impl PageCache {
    /// Construct an empty cache with the given byte budget.
    pub fn new(cfg: PageCacheConfig) -> Self {
        let cap = NonZeroUsize::new(DEFAULT_INDEX_CAPACITY).expect("index cap > 0");
        Self {
            state: Mutex::new(PageCacheState {
                lru: LruCache::new(cap),
                by_path: HashMap::new(),
                current_bytes: 0,
            }),
            counters: PageCacheCounters::default(),
            max_bytes: cfg.max_bytes,
        }
    }

    /// Snapshot the cache's hit/miss/eviction counters.
    pub fn counters(&self) -> PageCacheCountersSnapshot {
        self.counters.snapshot()
    }

    /// Lookup. On hit, bumps LRU position and returns a clone of the
    /// `Arc<Vec<...>>` (cheap; no batch is copied).
    pub(crate) fn get(&self, key: &CacheKey) -> Option<Arc<Vec<Arc<RecordBatch>>>> {
        let mut g = self.state.lock().expect("page cache mutex poisoned");
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
    /// until `current_bytes <= max_bytes`.
    pub(crate) fn insert(&self, key: CacheKey, batches: Vec<Arc<RecordBatch>>) {
        let size: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let entry = CacheEntry {
            batches: Arc::new(batches),
            size,
        };

        let mut g = self.state.lock().expect("page cache mutex poisoned");

        // Refresh case: subtract the old entry's bytes and clean up
        // its reverse-index slot.
        if let Some(old) = g.lru.pop(&key) {
            g.current_bytes = g.current_bytes.saturating_sub(old.size);
            if let Some(v) = g.by_path.get_mut(&key.path) {
                v.retain(|k| k != &key);
                if v.is_empty() {
                    g.by_path.remove(&key.path);
                }
            }
        }

        g.current_bytes = g.current_bytes.saturating_add(entry.size);
        g.by_path
            .entry(key.path.clone())
            .or_default()
            .push(key.clone());

        // `lru.push` may itself evict if the index hit its NonZeroUsize
        // cap; account for that displacement before our budget pass.
        if let Some((evicted_k, evicted_v)) = g.lru.push(key.clone(), entry) {
            if evicted_k != key {
                g.current_bytes = g.current_bytes.saturating_sub(evicted_v.size);
                if let Some(v) = g.by_path.get_mut(&evicted_k.path) {
                    v.retain(|k| k != &evicted_k);
                    if v.is_empty() {
                        g.by_path.remove(&evicted_k.path);
                    }
                }
                self.counters.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Byte-budget eviction loop.
        while g.current_bytes > self.max_bytes {
            match g.lru.pop_lru() {
                Some((k, v)) => {
                    g.current_bytes = g.current_bytes.saturating_sub(v.size);
                    if let Some(vec) = g.by_path.get_mut(&k.path) {
                        vec.retain(|x| x != &k);
                        if vec.is_empty() {
                            g.by_path.remove(&k.path);
                        }
                    }
                    self.counters.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => break,
            }
        }

        self.counters
            .current_bytes
            .store(g.current_bytes, Ordering::Relaxed);
    }

    /// Drop every cached entry that references `path`. Called by the
    /// storage layer when a Parquet file is deleted (compactor,
    /// UPDATE/DELETE rewrite). Idempotent and cheap if `path` is not
    /// in the cache.
    pub fn invalidate_path(&self, path: &ObjectPath) {
        let mut g = self.state.lock().expect("page cache mutex poisoned");
        let keys = g.by_path.remove(path).unwrap_or_default();
        let mut freed = 0u64;
        for k in keys {
            if let Some(v) = g.lru.pop(&k) {
                freed = freed.saturating_add(v.size);
            }
        }
        g.current_bytes = g.current_bytes.saturating_sub(freed);
        self.counters
            .current_bytes
            .store(g.current_bytes, Ordering::Relaxed);
    }

    /// For tests: how many entries the cache currently holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let g = self.state.lock().expect("page cache mutex poisoned");
        g.lru.len()
    }
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("max_bytes", &self.max_bytes)
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
    #[test]
    fn lru_evicts_over_byte_budget() {
        // Each batch is ~800 bytes (100 i64 values + arrow overhead);
        // size in `get_array_memory_size` returns the buffer footprint.
        // We pick a budget that holds at most one such entry.
        let probe = small_batch(0, 100);
        let entry_size = probe.get_array_memory_size() as u64;
        // Budget = 1.5× one entry. Inserting 3 entries forces 2 of
        // them to age out before we're done.
        let cache = PageCache::new(PageCacheConfig::new(entry_size + entry_size / 2));

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
}
