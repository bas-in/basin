//! Bounded LRU caches for hot read paths in `basin-storage`.
//!
//! Two caches live here for symmetry — both are keyed on `ObjectPath` and
//! both hold `Arc<T>` so cloning out of the cache is O(1) regardless of the
//! cached object's size.
//!
//! # [`ParquetMetaCache`]
//!
//! Caches the parsed `parquet::file::metadata::ParquetMetaData` (the footer
//! that `ParquetRecordBatchStreamBuilder::new` would otherwise re-fetch on
//! every read). For each table read we list data files, then for each file we
//! consult the cache; on hit we skip the metadata round-trip and feed the
//! cached metadata into `ParquetRecordBatchStreamBuilder::new_with_metadata`.
//!
//! ## Invalidation strategy
//!
//! **None.** Data files in `basin-storage` are immutable: every
//! [`crate::Storage::write_batch`] call generates a fresh ULID-named Parquet
//! object via [`crate::writer::write_batch`] and never mutates an existing
//! one. There is no public surface that can re-open, truncate, or overwrite a
//! data file at the same key. Therefore a cache entry, once present, is
//! valid for the lifetime of the file at that key — and when the file is
//! deleted (via the not-yet-implemented compactor), we simply rely on LRU
//! eviction plus the catalog no longer pointing readers at the deleted key.
//!
//! If a future code path *does* mutate a data file in place, this invariant
//! breaks and the cache must be invalidated explicitly. The crate's
//! `#![forbid(unsafe_code)]` lid plus the writer-only-creates-new-files
//! invariant are what makes this safe today.
//!
//! # [`HnswSegmentCache`]
//!
//! Same shape, different value: caches parsed
//! [`basin_vector::HnswIndex`] keyed on segment file path. Vector search
//! re-loads + re-parses every HNSW segment on every query without this
//! cache, which dominates wall-clock for repeated queries against a stable
//! corpus. Same immutability invariant applies (segments are written under a
//! fresh ULID per write_batch call).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use basin_vector::HnswIndex;
use lru::LruCache;
use object_store::path::Path as ObjectPath;
use parquet::file::metadata::ParquetMetaData;

/// One entry: parsed parquet metadata plus the underlying file size in
/// bytes. Caching the size lets the read path skip a `HEAD` round-trip on
/// every cache hit, which matters under heavy multi-tenant pressure where
/// a quiet tenant's point query would otherwise be one HEAD + one GET per
/// file.
#[derive(Clone)]
pub(crate) struct CachedParquetMeta {
    pub meta: Arc<ParquetMetaData>,
    pub size: u64,
}

/// Bounded LRU cache mapping `ObjectPath` → [`CachedParquetMeta`].
///
/// Cloned via `Arc<ParquetMetaCache>` from inside `Storage`, so concurrent
/// reads share one cache. Internal locking is a `Mutex<LruCache<...>>`; the
/// hot path (a single get-or-insert per file per read) is short and
/// non-contended in practice. If profiling later shows the lock as a hotspot
/// we can shard the cache by key hash — out of scope for this change.
pub(crate) struct ParquetMetaCache {
    inner: Mutex<LruCache<ObjectPath, CachedParquetMeta>>,
}

impl ParquetMetaCache {
    /// Construct with capacity `cap`. A capacity of 0 is bumped to 1 so the
    /// `NonZeroUsize` invariant of `LruCache` is upheld; a 0-sized cache is
    /// not a sensible request and silently treating it as 1 is the least
    /// surprising fallback.
    pub fn new(cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap.max(1)).expect("cap.max(1) >= 1");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Fetch and bump-to-front. `None` on miss.
    pub fn get(&self, k: &ObjectPath) -> Option<CachedParquetMeta> {
        let mut g = self.inner.lock().expect("ParquetMetaCache mutex poisoned");
        g.get(k).cloned()
    }

    /// Insert (or replace). Evicts the oldest entry if at capacity.
    pub fn insert(&self, k: ObjectPath, v: CachedParquetMeta) {
        let mut g = self.inner.lock().expect("ParquetMetaCache mutex poisoned");
        g.put(k, v);
    }
}

/// Bounded LRU cache mapping `ObjectPath` → `Arc<HnswIndex>`. Same shape and
/// invariants as [`ParquetMetaCache`].
///
/// Default capacity is 256 segments. At a few MB per segment that's roughly
/// a couple of GB at full saturation, which is past where we'd want to be in
/// memory for a single-process PoC; in practice the cache will be a small
/// fraction of that because workloads concentrate on a handful of segments.
/// A byte-aware bound is doable but unnecessary for v1 — count-based is
/// what the task brief calls for and matches the metadata cache's shape.
pub(crate) struct HnswSegmentCache {
    inner: Mutex<LruCache<ObjectPath, Arc<HnswIndex>>>,
}

impl HnswSegmentCache {
    pub fn new(cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap.max(1)).expect("cap.max(1) >= 1");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, k: &ObjectPath) -> Option<Arc<HnswIndex>> {
        let mut g = self.inner.lock().expect("HnswSegmentCache mutex poisoned");
        g.get(k).cloned()
    }

    pub fn insert(&self, k: ObjectPath, v: Arc<HnswIndex>) {
        let mut g = self.inner.lock().expect("HnswSegmentCache mutex poisoned");
        g.put(k, v);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::path::Path as ObjectPath;
    use parquet::file::metadata::{FileMetaData, ParquetMetaData};

    use super::*;

    fn fake_meta() -> CachedParquetMeta {
        // FileMetaData::new takes (version, num_rows, created_by,
        // key_value_metadata, schema_descr, column_orders). We construct the
        // smallest plausible one purely so the cache has something to hold —
        // no parquet body is read or written.
        use parquet::schema::types::{SchemaDescriptor, Type};
        let schema_root = Type::group_type_builder("root").build().unwrap();
        let descr = Arc::new(SchemaDescriptor::new(Arc::new(schema_root)));
        let file_meta = FileMetaData::new(0, 0, None, None, descr, None);
        CachedParquetMeta {
            meta: Arc::new(ParquetMetaData::new(file_meta, Vec::new())),
            size: 0,
        }
    }

    fn p(s: &str) -> ObjectPath {
        ObjectPath::from(s)
    }

    #[test]
    fn parquet_meta_cache_miss_then_hit() {
        let cache = ParquetMetaCache::new(4);
        let k = p("a/b/1.parquet");
        assert!(cache.get(&k).is_none(), "cold cache must miss");
        cache.insert(k.clone(), fake_meta());
        assert!(cache.get(&k).is_some(), "warm cache must hit");
    }

    #[test]
    fn parquet_meta_cache_evicts_oldest_at_capacity() {
        let cap = 3usize;
        let cache = ParquetMetaCache::new(cap);
        let keys: Vec<ObjectPath> = (0..cap + 1).map(|i| p(&format!("k{i}"))).collect();
        for k in &keys {
            cache.insert(k.clone(), fake_meta());
        }
        // The first inserted key should have been evicted by the (cap+1)-th
        // insert; all others must still be present.
        assert!(cache.get(&keys[0]).is_none(), "oldest entry must be evicted");
        for k in &keys[1..] {
            assert!(cache.get(k).is_some(), "newer entry {k} should still hit");
        }
    }

    #[tokio::test]
    async fn parquet_meta_cache_concurrent_get_insert() {
        // Spawn four tasks doing interleaved get/insert against the same
        // cache; primary assertion is "no panic, no deadlock". A barrier
        // would harden timing but adds noise; the simple loop catches the
        // common races.
        let cache = Arc::new(ParquetMetaCache::new(8));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..50u32 {
                    let k = p(&format!("t{t}-{i}"));
                    let _ = cache.get(&k);
                    cache.insert(k.clone(), fake_meta());
                    let _ = cache.get(&k);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    // --- HnswSegmentCache ---

    fn fake_hnsw() -> Arc<HnswIndex> {
        use basin_vector::{Distance, HnswIndexBuilder};
        let mut b = HnswIndexBuilder::new(Distance::L2, 4);
        b.insert(0, vec![0.0, 0.0, 0.0, 0.0]).unwrap();
        Arc::new(b.build())
    }

    #[test]
    fn hnsw_segment_cache_miss_then_hit() {
        let cache = HnswSegmentCache::new(4);
        let k = p("idx/seg-1.hnsw");
        assert!(cache.get(&k).is_none());
        cache.insert(k.clone(), fake_hnsw());
        assert!(cache.get(&k).is_some());
    }

    #[test]
    fn hnsw_segment_cache_evicts_oldest_at_capacity() {
        let cap = 3usize;
        let cache = HnswSegmentCache::new(cap);
        let keys: Vec<ObjectPath> = (0..cap + 1).map(|i| p(&format!("seg-{i}.hnsw"))).collect();
        for k in &keys {
            cache.insert(k.clone(), fake_hnsw());
        }
        assert!(cache.get(&keys[0]).is_none(), "oldest must be evicted");
        for k in &keys[1..] {
            assert!(cache.get(k).is_some());
        }
    }

    #[tokio::test]
    async fn hnsw_segment_cache_concurrent_get_insert() {
        let cache = Arc::new(HnswSegmentCache::new(8));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..50u32 {
                    let k = p(&format!("t{t}-{i}.hnsw"));
                    let _ = cache.get(&k);
                    cache.insert(k.clone(), fake_hnsw());
                    let _ = cache.get(&k);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
