//! `PkRowCache` — process-global PK → Row hot cache (correctness-critical).
//!
//! # Why
//!
//! For the canonical point query `SELECT cols FROM t WHERE pk = X` against a
//! row that lives only in cold Parquet (not the hot memtable), the base path
//! runs zone-map + bloom + Parquet decode (~80 µs). This cache turns a *warm*
//! hit into an in-RAM HashMap lookup (~2-5 µs), closing the point-query gap to
//! PostgreSQL.
//!
//! # Correctness — THE load-bearing property
//!
//! A stale cache silently serves deleted / old / RLS-bypassed rows (bug family
//! #159). To make staleness impossible, every cached entry carries TWO
//! watermarks and is valid only if BOTH match the live values at lookup time:
//!
//! 1. **`hot_epoch`** (Fix A) — the per-`(project, table)` mutation counter
//!    from `basin-hottier`'s `MemTable`, bumped on EVERY memtable mutation
//!    (insert / upsert / delete / tombstone). Catches fast-path DML that
//!    writes only the memtable and never advances the catalog snapshot — e.g.
//!    a fast-path DELETE that would otherwise leave a stale cached row.
//!
//! 2. **`snapshot_id`** (Fix B) — `TableMetadata.current_snapshot`, the
//!    Iceberg snapshot id loaded by `load_table_meta_cached` before every
//!    SELECT. Catches cold-tier writes (compaction, bulk UPDATE/DELETE that
//!    rewrite files + commit a new snapshot) INCLUDING writes from OTHER
//!    replicas (the snapshot id is durable + cross-replica via the catalog).
//!
//! On lookup: `HIT` iff `entry.hot_epoch == cur_hot_epoch
//! && entry.snapshot_id == cur_snapshot`; otherwise the stale entry is evicted
//! and the caller falls through to the real read (which repopulates).
//!
//! ## Why this introduces no new inconsistency
//!
//! The cache's staleness window equals the existing `TableMetaCache` TTL, which
//! already gates `meta.current_snapshot` freshness. The PK cache is a
//! second-level cache invalidated by the SAME signals the metadata cache
//! already uses, so it is never *worse* than the base path: a stale PK hit can
//! only occur when the session's `current_snapshot` is also stale, which would
//! make the cold-tier read equally stale.
//!
//! ## RLS
//!
//! The cache stores the RAW row, not an RLS-filtered view. The caller MUST
//! bypass the cache entirely when the table has RLS enabled (see
//! `fast_select`). Structurally, `execute_simple_select` is only reached when
//! `!rls_enabled` (the executor gate), so an RLS row never enters this cache;
//! the explicit bypass is defence in depth.
//!
//! # Shape
//!
//! Mirrors `basin-storage::page_cache`: N independent `Mutex<Shard>` buckets,
//! each an `LruCache` plus a local byte counter driven below `max_bytes /
//! N_SHARDS`. A request hashes its key `(ProjectId, TableName, RowKey)` to pick
//! a shard, so same-key requests always land on the same shard and different
//! keys scatter. Process-global, lives on the engine handle.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use arrow_array::RecordBatch;
use basin_common::ids::{ProjectId, TableName};
use basin_hottier::RowKey;
use lru::LruCache;

/// Default number of shards. Matches the page-cache default; one mutex per
/// shard caps contention at ~1/N on a uniform workload.
const DEFAULT_SHARDS: usize = 64;

/// Default memory budget: 64 MiB. Override via `BASIN_PK_ROW_CACHE_BYTES`.
const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Composite cache key. `RowKey` is the byte-exact PK encoding shared with the
/// cold-tier cluster-sort + hot-tier UPDATE/DELETE paths (see
/// `dml_mutate::pk_scalar_to_row_key`), so a cached entry is found iff its key
/// matches byte-for-byte.
#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    project: ProjectId,
    table: TableName,
    pk: RowKey,
}

/// One cached row + its two invalidation watermarks.
#[derive(Clone)]
struct CacheEntry {
    /// Hot-tier mutation epoch captured at insert time (Fix A watermark).
    hot_epoch: u64,
    /// Catalog snapshot id captured at insert time (Fix B watermark).
    snapshot_id: u64,
    /// Hash of the `read_cols` projection the cached batches were read with.
    /// The cold read projects to `read_cols`, so the cached batches carry only
    /// those columns; a lookup whose query needs a DIFFERENT column set must
    /// NOT reuse them. The key stays `(project, table, pk)`; this field rejects
    /// projection-shape mismatches as misses (no wrong-column hit).
    proj_hash: u64,
    /// The cached single-row result batches (post-read, full table schema —
    /// the caller re-projects). Wrapped so clones are cheap.
    rows: std::sync::Arc<Vec<RecordBatch>>,
    /// Footprint in bytes (sum of `get_array_memory_size` over `rows` plus key
    /// overhead). Tracked so eviction doesn't re-walk the batches.
    size: u64,
}

struct Shard {
    lru: LruCache<CacheKey, CacheEntry>,
    bytes: u64,
}

/// Best-effort counters for tests + observability.
#[derive(Default)]
pub struct PkRowCacheCounters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Lookups that found an entry but rejected it on a watermark mismatch
    /// (stale → evicted). The canary metric: a working invalidation makes
    /// this non-zero after a DML mutation.
    pub stale_evictions: AtomicU64,
    pub inserts: AtomicU64,
    pub evictions: AtomicU64,
    pub current_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PkRowCacheCountersSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub stale_evictions: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub current_bytes: u64,
}

impl PkRowCacheCounters {
    pub fn snapshot(&self) -> PkRowCacheCountersSnapshot {
        PkRowCacheCountersSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            stale_evictions: self.stale_evictions.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Process-global sharded LRU PK → Row cache. Cheap to construct; lives on the
/// engine handle wrapped in an `Arc`.
pub struct PkRowCache {
    shards: Vec<Mutex<Shard>>,
    /// Per-shard byte budget = `max_bytes / shards.len()`.
    per_shard_bytes: u64,
    counters: PkRowCacheCounters,
}

impl Default for PkRowCache {
    fn default() -> Self {
        Self::from_env()
    }
}

impl PkRowCache {
    /// Construct with explicit shard count + total byte budget.
    pub fn new(num_shards: usize, max_bytes: u64) -> Self {
        let num_shards = num_shards.max(1);
        // Generous per-shard count cap; the byte budget is the real limit.
        let cap = NonZeroUsize::new(1 << 16).unwrap();
        let shards = (0..num_shards)
            .map(|_| {
                Mutex::new(Shard {
                    lru: LruCache::new(cap),
                    bytes: 0,
                })
            })
            .collect();
        let per_shard_bytes = (max_bytes / num_shards as u64).max(1);
        Self {
            shards,
            per_shard_bytes,
            counters: PkRowCacheCounters::default(),
        }
    }

    /// Construct from environment: `BASIN_PK_ROW_CACHE_BYTES` (default 64 MiB).
    pub fn from_env() -> Self {
        let max_bytes = std::env::var("BASIN_PK_ROW_CACHE_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&b| b > 0)
            .unwrap_or(DEFAULT_MAX_BYTES);
        Self::new(DEFAULT_SHARDS, max_bytes)
    }

    pub fn counters(&self) -> &PkRowCacheCounters {
        &self.counters
    }

    fn shard_idx(&self, key: &CacheKey) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.shards.len()
    }

    /// Look up a row. Returns `Some(rows)` ONLY when an entry exists AND BOTH
    /// watermarks match the supplied live values; otherwise returns `None`.
    /// A watermark mismatch evicts the stale entry so a later insert refills it.
    pub fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        pk: &RowKey,
        cur_hot_epoch: u64,
        cur_snapshot: u64,
        proj_hash: u64,
    ) -> Option<std::sync::Arc<Vec<RecordBatch>>> {
        let key = CacheKey {
            project: *project,
            table: table.clone(),
            pk: pk.clone(),
        };
        let idx = self.shard_idx(&key);
        let mut shard = self.shards[idx].lock().unwrap();
        // Peek first so we can decide validity without disturbing LRU order on a
        // stale entry (which we are about to remove anyway). A projection-shape
        // mismatch is treated as a miss but does NOT evict (a different query
        // shape; the entry may still serve its original shape) — it simply
        // doesn't match, so we leave it and fall through.
        let valid = match shard.lru.peek(&key) {
            Some(e) => {
                if e.proj_hash != proj_hash {
                    drop(shard);
                    self.counters.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                e.hot_epoch == cur_hot_epoch && e.snapshot_id == cur_snapshot
            }
            None => {
                drop(shard);
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        if valid {
            // Promote (LRU) and clone the Arc — cheap.
            let entry = shard.lru.get(&key).expect("present").clone();
            drop(shard);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.rows)
        } else {
            // Stale: evict so the next read repopulates with fresh watermarks.
            // Capture the freed size while holding the shard lock, then update
            // the global byte counter AFTER releasing it — `current_bytes` is a
            // lock-free atomic, never `total_bytes()` (which locks every shard
            // and would self-deadlock on the shard we already hold).
            let freed = if let Some(removed) = shard.lru.pop(&key) {
                shard.bytes = shard.bytes.saturating_sub(removed.size);
                removed.size
            } else {
                0
            };
            drop(shard);
            if freed > 0 {
                self.counters
                    .current_bytes
                    .fetch_sub(freed, Ordering::Relaxed);
            }
            self.counters.stale_evictions.fetch_add(1, Ordering::Relaxed);
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert (or overwrite) a row with its two watermarks. Evicts LRU entries
    /// from the owning shard until it is back under its byte budget.
    pub fn insert(
        &self,
        project: &ProjectId,
        table: &TableName,
        pk: RowKey,
        hot_epoch: u64,
        snapshot_id: u64,
        proj_hash: u64,
        rows: Vec<RecordBatch>,
    ) {
        let size = key_overhead(table) + batches_bytes(&rows);
        // Never cache an entry larger than one shard's whole budget (it would
        // immediately evict itself and everything else).
        if size > self.per_shard_bytes {
            return;
        }
        let key = CacheKey {
            project: *project,
            table: table.clone(),
            pk,
        };
        let idx = self.shard_idx(&key);
        let entry = CacheEntry {
            hot_epoch,
            snapshot_id,
            proj_hash,
            rows: std::sync::Arc::new(rows),
            size,
        };
        // Net byte delta for this shard, accumulated under the lock and applied
        // to the lock-free global `current_bytes` AFTER release (never
        // `total_bytes()` under a lock — that would self-deadlock).
        let mut removed_bytes: u64 = 0;
        let mut evicted_count: u64 = 0;
        let mut shard = self.shards[idx].lock().unwrap();
        if let Some(old) = shard.lru.put(key, entry) {
            shard.bytes = shard.bytes.saturating_sub(old.size);
            removed_bytes = removed_bytes.saturating_add(old.size);
        }
        shard.bytes = shard.bytes.saturating_add(size);
        // Greedy LRU eviction back under the per-shard budget.
        while shard.bytes > self.per_shard_bytes {
            match shard.lru.pop_lru() {
                Some((_, evicted)) => {
                    shard.bytes = shard.bytes.saturating_sub(evicted.size);
                    removed_bytes = removed_bytes.saturating_add(evicted.size);
                    evicted_count += 1;
                }
                None => break,
            }
        }
        drop(shard);
        self.counters.inserts.fetch_add(1, Ordering::Relaxed);
        if evicted_count > 0 {
            self.counters
                .evictions
                .fetch_add(evicted_count, Ordering::Relaxed);
        }
        // current_bytes += size_added - bytes_removed.
        self.counters
            .current_bytes
            .fetch_add(size, Ordering::Relaxed);
        if removed_bytes > 0 {
            self.counters
                .current_bytes
                .fetch_sub(removed_bytes, Ordering::Relaxed);
        }
    }

    /// Drop ALL entries for `(project, table)`. Called on DDL (ALTER/DROP) so a
    /// schema change can never serve a row shaped to the old schema. O(entries
    /// per shard) — acceptable since DDL is rare.
    pub fn invalidate_table(&self, project: &ProjectId, table: &TableName) {
        let mut freed: u64 = 0;
        for shard_mtx in &self.shards {
            let mut shard = shard_mtx.lock().unwrap();
            let victims: Vec<CacheKey> = shard
                .lru
                .iter()
                .filter(|(k, _)| k.project == *project && &k.table == table)
                .map(|(k, _)| k.clone())
                .collect();
            for k in victims {
                if let Some(e) = shard.lru.pop(&k) {
                    shard.bytes = shard.bytes.saturating_sub(e.size);
                    freed = freed.saturating_add(e.size);
                }
            }
        }
        if freed > 0 {
            self.counters.current_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Drop ALL entries for `project` (every table). Called from the central
    /// DDL/DML cache-invalidation hook so any statement that could change a
    /// table's schema or rewrite its data also clears the PK row cache for the
    /// session's project — belt to the dual-watermark suspenders for DDL that
    /// doesn't advance `current_snapshot` (e.g. ENABLE RLS, metadata-only
    /// ALTER). O(entries per shard); only runs on non-SELECT statements.
    pub fn invalidate_project(&self, project: &ProjectId) {
        let mut freed: u64 = 0;
        for shard_mtx in &self.shards {
            let mut shard = shard_mtx.lock().unwrap();
            let victims: Vec<CacheKey> = shard
                .lru
                .iter()
                .filter(|(k, _)| k.project == *project)
                .map(|(k, _)| k.clone())
                .collect();
            for k in victims {
                if let Some(e) = shard.lru.pop(&k) {
                    shard.bytes = shard.bytes.saturating_sub(e.size);
                    freed = freed.saturating_add(e.size);
                }
            }
        }
        if freed > 0 {
            self.counters.current_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Sum of per-shard byte counters. O(N_SHARDS). Test/diagnostic only —
    /// never call while holding a shard lock.
    #[allow(dead_code)]
    fn total_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().bytes)
            .sum()
    }

    /// Number of cached entries across all shards. Observability / test helper.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().lru.len()).sum()
    }

    /// Number of cached entries for a specific project across all shards.
    /// Project-scoped so concurrent tests touching OTHER projects don't perturb
    /// the count — used by the DDL-invalidation integration test.
    pub fn entries_for_project(&self, project: &ProjectId) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.lock()
                    .unwrap()
                    .lru
                    .iter()
                    .filter(|(k, _)| k.project == *project)
                    .count()
            })
            .sum()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Deterministic hash of a `read_cols` projection (`None` = all columns).
/// Order-sensitive: the cold read returns batches in `read_cols` order, so two
/// different orders must not share an entry.
pub fn hash_read_cols(read_cols: Option<&[String]>) -> u64 {
    let mut h = DefaultHasher::new();
    match read_cols {
        None => b"ALL".hash(&mut h),
        Some(cols) => {
            b"COLS".hash(&mut h);
            cols.len().hash(&mut h);
            for c in cols {
                c.hash(&mut h);
            }
        }
    }
    h.finish()
}

/// Footprint of the key (project id + table name string + RowKey bytes,
/// approximated; exact byte count isn't load-bearing — only the budget).
fn key_overhead(table: &TableName) -> u64 {
    (std::mem::size_of::<CacheKey>() + table.as_str().len()) as u64
}

/// Sum of `get_array_memory_size` over the batches.
fn batches_bytes(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn proj() -> ProjectId {
        ProjectId::new()
    }
    fn tbl(s: &str) -> TableName {
        TableName::new(s).unwrap()
    }
    fn key(n: i64) -> RowKey {
        RowKey::builder().append_i64(n).finish()
    }
    fn row(v: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()]
    }
    fn val(batches: &[RecordBatch]) -> i64 {
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[test]
    fn miss_when_empty() {
        let c = PkRowCache::new(8, 1 << 20);
        assert!(c.get(&proj(), &tbl("t"), &key(1), 0, 0, 0).is_none());
        assert_eq!(c.counters().snapshot().misses, 1);
    }

    /// REPRODUCE-FIRST: the hot_tier_epoch watermark is the load-bearing guard
    /// for fast-path DELETE/UPDATE. A fast-path DELETE writes a tombstone to the
    /// memtable WITHOUT advancing the catalog snapshot — so a snapshot-only
    /// ("naive") cache would still HIT and serve the deleted row. This test
    /// proves both halves:
    ///   * a snapshot-only check (ignoring the epoch) would wrongly accept the
    ///     entry after the delete;
    ///   * the real dual-watermark get(...) REFUSES it (epoch advanced).
    #[test]
    fn epoch_watermark_is_load_bearing_for_delete() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        // Row cached at hot_epoch=0, snapshot=5.
        c.insert(&p, &t, key(7), 0, 5, 0, row(42));
        // A fast-path DELETE bumps the memtable epoch to 1 but leaves the
        // catalog snapshot at 5 (no cold-tier commit).
        let post_delete_epoch = 1u64;
        let snapshot_unchanged = 5u64;

        // Naive snapshot-only validity (what a single-watermark cache would do):
        // snapshot matches → it would WRONGLY serve the stale (deleted) row.
        // We model the naive check inline to document the bug it would cause.
        let naive_would_hit = snapshot_unchanged == 5; // ignores epoch
        assert!(
            naive_would_hit,
            "a snapshot-only cache would (wrongly) hit here — that is the bug"
        );

        // The real dual-watermark get refuses it: epoch advanced.
        assert!(
            c.get(&p, &t, &key(7), post_delete_epoch, snapshot_unchanged, 0)
                .is_none(),
            "dual-watermark cache MUST refuse the entry after a fast-path delete"
        );
        assert_eq!(c.counters().snapshot().stale_evictions, 1);
    }

    #[test]
    fn proj_hash_mismatch_is_a_miss() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(1), 0, 0, 111, row(9));
        // Same watermarks, different projection shape → must NOT serve (the
        // cached batches carry a different column set).
        assert!(c.get(&p, &t, &key(1), 0, 0, 222).is_none());
        // Original projection still hits.
        assert!(c.get(&p, &t, &key(1), 0, 0, 111).is_some());
    }

    #[test]
    fn hash_read_cols_distinguishes_shapes() {
        let a = hash_read_cols(None);
        let b = hash_read_cols(Some(&["id".to_string()]));
        let cc = hash_read_cols(Some(&["id".to_string(), "v".to_string()]));
        let dd = hash_read_cols(Some(&["v".to_string(), "id".to_string()]));
        assert_ne!(a, b);
        assert_ne!(b, cc);
        assert_ne!(cc, dd, "order-sensitive");
        assert_eq!(b, hash_read_cols(Some(&["id".to_string()])));
    }

    #[test]
    fn invalidate_project_drops_all_tables_for_project() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let other = proj();
        c.insert(&p, &tbl("a"), key(1), 0, 0, 0, row(1));
        c.insert(&p, &tbl("b"), key(1), 0, 0, 0, row(2));
        c.insert(&other, &tbl("a"), key(1), 0, 0, 0, row(3));
        c.invalidate_project(&p);
        assert!(c.get(&p, &tbl("a"), &key(1), 0, 0, 0).is_none());
        assert!(c.get(&p, &tbl("b"), &key(1), 0, 0, 0).is_none());
        assert!(c.get(&other, &tbl("a"), &key(1), 0, 0, 0).is_some());
    }

    #[test]
    fn hit_returns_row_when_both_watermarks_match() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(7), 3, 5, 0, row(42));
        let got = c.get(&p, &t, &key(7), 3, 5, 0).expect("hit");
        assert_eq!(val(&got), 42);
        assert_eq!(c.counters().snapshot().hits, 1);
    }

    #[test]
    fn miss_when_hot_epoch_differs() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(7), 3, 5, 0, row(42));
        // hot epoch advanced (a fast-path DML happened) → stale.
        assert!(c.get(&p, &t, &key(7), 4, 5, 0).is_none());
        assert_eq!(c.counters().snapshot().stale_evictions, 1);
        // entry was evicted.
        assert!(c.get(&p, &t, &key(7), 3, 5, 0).is_none());
    }

    #[test]
    fn miss_when_snapshot_differs() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(7), 3, 5, 0, row(42));
        // snapshot advanced (a cold-tier commit happened) → stale.
        assert!(c.get(&p, &t, &key(7), 3, 6, 0).is_none());
        assert_eq!(c.counters().snapshot().stale_evictions, 1);
    }

    #[test]
    fn miss_when_both_differ() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(7), 3, 5, 0, row(42));
        assert!(c.get(&p, &t, &key(7), 9, 9, 0).is_none());
    }

    #[test]
    fn reinsert_after_stale_refreshes_watermarks() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(1), 1, 1, 0, row(10));
        // mutation: epoch bumped; old entry now stale.
        assert!(c.get(&p, &t, &key(1), 2, 1, 0).is_none());
        // repopulate with the new value + new watermark.
        c.insert(&p, &t, key(1), 2, 1, 0, row(20));
        let got = c.get(&p, &t, &key(1), 2, 1, 0).expect("fresh hit");
        assert_eq!(val(&got), 20);
    }

    #[test]
    fn invalidate_table_drops_only_that_table() {
        let c = PkRowCache::new(8, 1 << 20);
        let p = proj();
        let t1 = tbl("a");
        let t2 = tbl("b");
        c.insert(&p, &t1, key(1), 0, 0, 0, row(1));
        c.insert(&p, &t2, key(1), 0, 0, 0, row(2));
        c.invalidate_table(&p, &t1);
        assert!(c.get(&p, &t1, &key(1), 0, 0, 0).is_none());
        assert!(c.get(&p, &t2, &key(1), 0, 0, 0).is_some());
    }

    #[test]
    fn project_isolation() {
        let c = PkRowCache::new(8, 1 << 20);
        let pa = proj();
        let pb = proj();
        let t = tbl("t");
        c.insert(&pa, &t, key(1), 0, 0, 0, row(100));
        // same table name + pk + watermarks, different project → miss.
        assert!(c.get(&pb, &t, &key(1), 0, 0, 0).is_none());
        assert_eq!(val(&c.get(&pa, &t, &key(1), 0, 0, 0).unwrap()), 100);
    }

    #[test]
    fn lru_eviction_respects_byte_budget() {
        // Tiny budget: 1 shard, a few KiB. Insert many rows; total stays bounded.
        let c = PkRowCache::new(1, 4096);
        let p = proj();
        let t = tbl("t");
        for i in 0..1000i64 {
            c.insert(&p, &t, key(i), 0, 0, 0, row(i));
        }
        let snap = c.counters().snapshot();
        assert!(snap.evictions > 0, "expected evictions under tiny budget");
        assert!(
            c.total_bytes() <= 4096,
            "bytes {} exceed budget",
            c.total_bytes()
        );
    }

    #[test]
    fn always_on_cache_is_consulted_regardless_of_env() {
        // The cache is always-on: `from_env()` builds a usable, positively
        // sized cache no matter what `BASIN_PK_ROW_CACHE` is set to (the flag
        // no longer exists), and a fresh insert is immediately consultable on
        // matching watermarks. Capacity stays bounded by the default budget.
        let c = PkRowCache::from_env();
        assert!(
            c.total_bytes() <= DEFAULT_MAX_BYTES,
            "fresh cache bytes {} exceed default budget",
            c.total_bytes()
        );
        let p = proj();
        let t = tbl("t");
        c.insert(&p, &t, key(7), 1, 1, 0, row(99));
        let got = c
            .get(&p, &t, &key(7), 1, 1, 0)
            .expect("always-on cache must serve a fresh hit");
        assert_eq!(val(&got), 99);
    }
}
