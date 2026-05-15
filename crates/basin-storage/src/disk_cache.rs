//! NVMe disk cache: a local-SSD LRU between [`Storage`] and the underlying
//! [`ObjectStore`].
//!
//! # Why
//!
//! Cold S3 GETs cost ~50 ms (in-region) or 500+ ms (cross-region) per call.
//! Once a Parquet file is in active use, the same byte ranges are read over
//! and over: the footer, the column dictionaries, the row-group(s) that
//! survived predicate pushdown. If we keep those bytes on local NVMe, the
//! warm path drops to ~100 µs — a 100-500× point-query latency win.
//! Snowflake / Databricks / ClickHouse Cloud all do this; we follow the
//! same shape.
//!
//! # Architecture
//!
//! [`DiskCachedStore`] is a transparent [`ObjectStore`] wrapper. Every
//! `get` / `get_opts` / `get_range` consults the on-disk cache first; on
//! hit it returns the bytes from local SSD, on miss it fetches from the
//! inner store, writes the result through to the cache, and returns the
//! bytes. PUT / DELETE go straight through to the inner store and
//! invalidate any matching cache entries on the way past — Basin treats
//! data files as immutable, so this is mostly the "compactor deleted a
//! merged file" case.
//!
//! Wrapping order: the cache wrapper sits **inside** the per-project
//! `ProjectScopedStore`. So a typical RPC stack is
//!
//! ```text
//!   reader/writer
//!     -> ProjectScopedStore (per-project semaphore)
//!     -> DiskCachedStore   (this module)
//!     -> real ObjectStore  (LocalFS / S3 / Tigris / R2 / GCS / ...)
//! ```
//!
//! The cache key is the SHA-256 of `(path, range_marker)`. We key on
//! `(path, range)` rather than full files because Parquet's read pattern
//! is dominated by range reads — caching whole files would be wasteful
//! for any file bigger than the working set fits in RAM/SSD, and for
//! large benchmark tables the file size easily exceeds the configured
//! cache budget.
//!
//! # LRU choice: RAM-only with on-disk warm files, walked on startup
//!
//! State lives in a `Mutex<LruCache<KeyHash, FileMeta>>`. On startup we
//! walk `$root` once to repopulate the in-RAM index from the persisted
//! filenames; this is one `ls` call and survives process restarts — at
//! the cost of losing ordering information (we re-seed in directory-
//! walk order). The simpler-than-SQLite tradeoff is deliberate: the
//! cache is a performance tier, not a durable record.
//!
//! # Eviction
//!
//! Cap is `max_bytes`. After every successful insert we pop the least
//! recently used entry until the sum of cached file sizes falls back
//! below the cap. We hold the cache lock for the eviction loop but
//! never across an `await` — the actual file deletes are issued
//! synchronously via `std::fs::remove_file` inside a `spawn_blocking`
//! handle from the caller.
//!
//! # Singleflight
//!
//! Two concurrent misses on the same key would otherwise both fetch
//! from the inner store. We collapse them via a
//! `Mutex<HashMap<KeyHash, Arc<Notify>>>`: the first miss inserts a
//! `Notify`, the second one sees it and `notified().await`s on the
//! first's completion before re-checking the cache.
//!
//! # Project isolation
//!
//! The cache key is derived from the full object_store `Path`, which
//! always includes the project prefix (`projects/{ulid}/...`). Two
//! projects with otherwise-identical reads produce different keys
//! mechanically; there is no cross-project key collision possible.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use lru::LruCache;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, Notify};

/// On-disk file extension for cached fragments. Distinguishes them from
/// any future bookkeeping we might write under the same root.
const FRAGMENT_EXT: &str = "frag";

/// 32-byte SHA-256 cache key.
type KeyHash = [u8; 32];

fn key_for(path: &ObjectPath, range: Option<&GetRange>) -> KeyHash {
    let mut h = Sha256::new();
    h.update(b"basin-disk-cache-v1");
    h.update(b":path:");
    h.update(path.as_ref().as_bytes());
    h.update(b":range:");
    match range {
        None => h.update(b"FULL"),
        Some(GetRange::Bounded(r)) => {
            h.update(b"BND:");
            h.update(r.start.to_le_bytes());
            h.update(b":");
            h.update(r.end.to_le_bytes());
        }
        Some(GetRange::Offset(o)) => {
            h.update(b"OFF:");
            h.update(o.to_le_bytes());
        }
        Some(GetRange::Suffix(s)) => {
            h.update(b"SFX:");
            h.update(s.to_le_bytes());
        }
    }
    let out = h.finalize();
    let mut k = [0u8; 32];
    k.copy_from_slice(&out);
    k
}

fn hex_key(k: &KeyHash) -> String {
    hex::encode(k)
}

/// Per-entry metadata kept in the in-RAM LRU.
#[derive(Clone, Debug)]
struct FileMeta {
    /// Absolute path to the on-disk cached fragment.
    fs_path: PathBuf,
    /// Size of the cached payload in bytes; tracked so eviction can
    /// enforce the byte budget without re-stat'ing on every put.
    size: u64,
    /// The `ObjectPath` this entry caches. Used to invalidate by
    /// object path on PUT/DELETE without scanning the whole map.
    object_path: ObjectPath,
}

/// Public configuration for the disk cache.
///
/// Both fields are required — the wrapper has no useful defaults that
/// don't risk silently filling up `/tmp`.
#[derive(Clone, Debug)]
pub struct DiskCacheConfig {
    /// Filesystem root for cached fragments. Created if it doesn't
    /// exist. The cache treats this directory as exclusively owned;
    /// don't point two cache instances at the same path.
    pub root: PathBuf,
    /// Eviction cap in bytes. The cache may briefly exceed this during
    /// a put but converges back below it via LRU pops.
    pub max_bytes: u64,
}

impl DiskCacheConfig {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
        }
    }
}

/// Default LRU index capacity. The byte budget is the load-bearing knob;
/// the index size is a secondary safety net that prevents pathological
/// memory blow-ups if someone configures a cache with very small files.
/// 1M entries at ~200 bytes each is ~200 MB of in-RAM bookkeeping at
/// saturation, which is acceptable for a server holding a multi-GB cache.
const DEFAULT_INDEX_CAPACITY: usize = 1_000_000;

/// In-RAM index over the on-disk cache.
///
/// Every mutation that touches the LRU also touches `current_bytes` so
/// the eviction loop has an O(1) view of "are we over budget".
struct CacheIndex {
    lru: LruCache<KeyHash, FileMeta>,
    /// Reverse index: ObjectPath -> set of KeyHashes that cache fragments
    /// of that object. Lets PUT/DELETE invalidate every range entry for
    /// a given file without scanning the whole LRU.
    by_path: HashMap<ObjectPath, Vec<KeyHash>>,
    current_bytes: u64,
    max_bytes: u64,
}

impl CacheIndex {
    fn new(max_bytes: u64) -> Self {
        Self {
            lru: LruCache::new(NonZeroUsize::new(DEFAULT_INDEX_CAPACITY).unwrap()),
            by_path: HashMap::new(),
            current_bytes: 0,
            max_bytes,
        }
    }

    /// Insert a new entry. Returns the list of fragments that need to be
    /// physically deleted from disk to re-converge below `max_bytes`.
    /// The caller performs the deletes outside the lock.
    fn insert(&mut self, k: KeyHash, m: FileMeta) -> Vec<PathBuf> {
        // If an entry for this key already exists, treat as a refresh:
        // subtract its old size, drop the by_path entry, then re-insert.
        if let Some(old) = self.lru.pop(&k) {
            self.current_bytes = self.current_bytes.saturating_sub(old.size);
            if let Some(v) = self.by_path.get_mut(&old.object_path) {
                v.retain(|x| x != &k);
                if v.is_empty() {
                    self.by_path.remove(&old.object_path);
                }
            }
        }
        self.current_bytes = self.current_bytes.saturating_add(m.size);
        self.by_path
            .entry(m.object_path.clone())
            .or_default()
            .push(k);
        // `lru.put` may itself evict the LRU tail when the index hits
        // its NonZeroUsize cap; that eviction returns an `(K, V)` pair
        // we need to also account for in `current_bytes` and `by_path`.
        if let Some((evicted_k, evicted_v)) = self.lru.push(k, m) {
            // Only matters if the put displaced a different key (push
            // returns Some when at capacity OR when the key was a
            // refresh — but we already drained the refresh case above,
            // so `evicted_k != k` here in practice).
            if evicted_k != k {
                self.current_bytes = self.current_bytes.saturating_sub(evicted_v.size);
                if let Some(v) = self.by_path.get_mut(&evicted_v.object_path) {
                    v.retain(|x| x != &evicted_k);
                    if v.is_empty() {
                        self.by_path.remove(&evicted_v.object_path);
                    }
                }
                // Caller is responsible for unlinking the file. We
                // funnel both this and the byte-budget evictions
                // through one `Vec<PathBuf>` for symmetry.
                return self.evict_to_budget_with(vec![evicted_v.fs_path]);
            }
        }
        self.evict_to_budget_with(Vec::new())
    }

    fn evict_to_budget_with(&mut self, mut prefix: Vec<PathBuf>) -> Vec<PathBuf> {
        while self.current_bytes > self.max_bytes {
            match self.lru.pop_lru() {
                Some((k, v)) => {
                    self.current_bytes = self.current_bytes.saturating_sub(v.size);
                    if let Some(vec) = self.by_path.get_mut(&v.object_path) {
                        vec.retain(|x| x != &k);
                        if vec.is_empty() {
                            self.by_path.remove(&v.object_path);
                        }
                    }
                    prefix.push(v.fs_path);
                }
                None => break,
            }
        }
        prefix
    }

    /// Lookup with LRU bump on hit.
    fn get(&mut self, k: &KeyHash) -> Option<FileMeta> {
        self.lru.get(k).cloned()
    }

    /// Drop all entries that cache the given object path, returning the
    /// fragments to unlink.
    fn invalidate_path(&mut self, p: &ObjectPath) -> Vec<PathBuf> {
        let keys = self.by_path.remove(p).unwrap_or_default();
        let mut to_unlink = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(m) = self.lru.pop(&k) {
                self.current_bytes = self.current_bytes.saturating_sub(m.size);
                to_unlink.push(m.fs_path);
            }
        }
        to_unlink
    }
}

/// Disk-cache wrapper. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct DiskCachedStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<DiskCacheState>,
}

struct DiskCacheState {
    root: PathBuf,
    index: Mutex<CacheIndex>,
    /// Singleflight: one in-flight `Notify` per key so two concurrent
    /// misses join up instead of double-fetching. We use
    /// [`tokio::sync::Mutex`] because the lock is held very briefly
    /// (insert / remove of one entry) and we don't need to hold it
    /// across an await.
    inflight: AsyncMutex<HashMap<KeyHash, Arc<Notify>>>,
    /// Process-wide hit / miss counters. Best-effort observability,
    /// useful for asserting the cache is doing real work in tests.
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

/// Plain-data view of the cache counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiskCacheCounters {
    pub hits: u64,
    pub misses: u64,
}

impl DiskCachedStore {
    /// Wrap `inner` with a disk cache rooted at `cfg.root` and capped at
    /// `cfg.max_bytes`. Walks the cache directory once on construction
    /// to repopulate the in-RAM index from any fragments left over from
    /// a previous process.
    pub fn new(inner: Arc<dyn ObjectStore>, cfg: DiskCacheConfig) -> std::io::Result<Self> {
        std::fs::create_dir_all(&cfg.root)?;
        let mut index = CacheIndex::new(cfg.max_bytes);

        // Repopulate from disk. We do not have the original
        // `(path, range)` for orphan fragments — we only have the hash
        // (filename stem). The reverse-index (`by_path`) therefore stays
        // empty for orphans; their PUT/DELETE invalidation degrades to
        // "rely on LRU age-out". This is acceptable: the only way an
        // orphan can be wrong is if the same key gets reused for a
        // different object, which can't happen because the hash is
        // content-addressed on the full path string.
        //
        // We do, however, account for their bytes in `current_bytes` so
        // the budget stays honest.
        repopulate_from_disk(&cfg.root, &mut index)?;

        Ok(Self {
            inner,
            state: Arc::new(DiskCacheState {
                root: cfg.root,
                index: Mutex::new(index),
                inflight: AsyncMutex::new(HashMap::new()),
                hits: std::sync::atomic::AtomicU64::new(0),
                misses: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }

    pub fn counters(&self) -> DiskCacheCounters {
        use std::sync::atomic::Ordering;
        DiskCacheCounters {
            hits: self.state.hits.load(Ordering::Relaxed),
            misses: self.state.misses.load(Ordering::Relaxed),
        }
    }

    fn fragment_path(&self, k: &KeyHash) -> PathBuf {
        let hex = hex_key(k);
        // Fan out into two-byte sub-dirs so a single dir doesn't end up
        // with millions of files; LocalFS readdir scans don't love that.
        let (a, rest) = hex.split_at(2);
        let (b, rest) = rest.split_at(2);
        let mut p = self.state.root.clone();
        p.push(a);
        p.push(b);
        p.push(format!("{rest}.{FRAGMENT_EXT}"));
        p
    }

    /// Try the cache; on hit return the bytes, on miss fetch from inner
    /// and write through. The `range` argument matches the underlying
    /// `GetOptions::range` shape.
    async fn get_cached(
        &self,
        location: &ObjectPath,
        range: Option<GetRange>,
    ) -> object_store::Result<Bytes> {
        let key = key_for(location, range.as_ref());
        let fs_path = self.fragment_path(&key);

        // Fast path: cache hit.
        if let Some(b) = try_read_cached(&self.state, &key, &fs_path).await {
            self.state
                .hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(b);
        }

        // Singleflight: at most one in-flight fetch per key.
        let notify = {
            let mut g = self.state.inflight.lock().await;
            if let Some(existing) = g.get(&key) {
                let n = existing.clone();
                drop(g);
                n.notified().await;
                // Re-check the cache after the leader finished.
                if let Some(b) = try_read_cached(&self.state, &key, &fs_path).await {
                    self.state
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(b);
                }
                // Leader's fetch failed or its file got evicted before
                // we got there — fall through and fetch ourselves. We
                // still claim leadership for the recursive attempt.
                let n = Arc::new(Notify::new());
                self.state.inflight.lock().await.insert(key, n.clone());
                n
            } else {
                let n = Arc::new(Notify::new());
                g.insert(key, n.clone());
                n
            }
        };

        // We are the leader. Fetch from the inner store, write through
        // to disk, update the index, then notify waiters.
        let result = self
            .miss_fetch_and_store(location, range, &key, &fs_path)
            .await;

        {
            let mut g = self.state.inflight.lock().await;
            g.remove(&key);
        }
        notify.notify_waiters();

        self.state
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        result
    }

    async fn miss_fetch_and_store(
        &self,
        location: &ObjectPath,
        range: Option<GetRange>,
        key: &KeyHash,
        fs_path: &FsPath,
    ) -> object_store::Result<Bytes> {
        // Build the inner GetOptions from the range we were called with.
        let opts = GetOptions {
            range: range.clone(),
            ..GetOptions::default()
        };
        let got = self.inner.get_opts(location, opts).await?;
        let bytes = got.bytes().await?;

        // Write-through. Best effort: if the cache write fails (disk
        // full, permission, etc.) we still return the bytes to the
        // caller — the cache is a performance tier, not the durability
        // boundary. We log and move on.
        if let Err(e) = write_through(fs_path, &bytes).await {
            tracing::warn!(
                target = "basin_storage::disk_cache",
                fragment = %fs_path.display(),
                error = %e,
                "disk_cache: write-through failed; serving uncached bytes"
            );
            return Ok(bytes);
        }

        let meta = FileMeta {
            fs_path: fs_path.to_path_buf(),
            size: bytes.len() as u64,
            object_path: location.clone(),
        };

        // Index update + LRU eviction. We collect the fragments to
        // delete *outside* the lock, then unlink them.
        let to_delete = {
            let mut g = self
                .state
                .index
                .lock()
                .expect("disk-cache index mutex poisoned");
            g.insert(*key, meta)
        };
        for p in to_delete {
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(p)).await;
        }

        Ok(bytes)
    }

    /// Drop every cached entry that maps to `location`. Used by the
    /// invalidation hooks on PUT / DELETE.
    async fn invalidate(&self, location: &ObjectPath) {
        let to_delete = {
            let mut g = self
                .state
                .index
                .lock()
                .expect("disk-cache index mutex poisoned");
            g.invalidate_path(location)
        };
        for p in to_delete {
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(p)).await;
        }
    }
}

async fn try_read_cached(state: &DiskCacheState, key: &KeyHash, fs_path: &FsPath) -> Option<Bytes> {
    // Bump the LRU. If the index says we have it, read from disk.
    let exists_in_index = {
        let mut g = state.index.lock().expect("disk-cache index mutex poisoned");
        g.get(key).is_some()
    };
    if !exists_in_index {
        return None;
    }
    let path = fs_path.to_path_buf();
    match tokio::task::spawn_blocking(move || std::fs::read(&path)).await {
        Ok(Ok(bytes)) => Some(Bytes::from(bytes)),
        _ => {
            // The file vanished out from under us (orphan, manual
            // delete, FS error). Drop the index entry so we don't keep
            // claiming a hit on a nonexistent file.
            let mut g = state.index.lock().expect("disk-cache index mutex poisoned");
            if let Some(m) = g.lru.pop(key) {
                g.current_bytes = g.current_bytes.saturating_sub(m.size);
                if let Some(v) = g.by_path.get_mut(&m.object_path) {
                    v.retain(|x| x != key);
                    if v.is_empty() {
                        g.by_path.remove(&m.object_path);
                    }
                }
            }
            None
        }
    }
}

async fn write_through(fs_path: &FsPath, bytes: &Bytes) -> std::io::Result<()> {
    let parent = fs_path
        .parent()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent dir"))?
        .to_path_buf();
    let final_path = fs_path.to_path_buf();
    // Tmp + rename for atomic visibility — a partially-written cache
    // file would later be served as truth.
    let tmp = final_path.with_extension(format!("{FRAGMENT_EXT}.tmp"));
    let bytes = bytes.clone();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        std::fs::create_dir_all(&parent)?;
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &final_path)?;
        Ok(())
    })
    .await
    .map_err(std::io::Error::other)?
}

fn repopulate_from_disk(root: &FsPath, index: &mut CacheIndex) -> std::io::Result<()> {
    fn walk(dir: &FsPath, out: &mut Vec<(KeyHash, PathBuf, u64)>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let p = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(&p, out)?;
            } else if ft.is_file() {
                if p.extension().and_then(|s| s.to_str()) != Some(FRAGMENT_EXT) {
                    continue;
                }
                // Recover the hash from the filename. Layout:
                //   ROOT/<aa>/<bb>/<60-char-rest>.frag
                // i.e. <aa><bb><rest> = 64-char hex string.
                let stem = match p.file_stem().and_then(|s| s.to_str()) {
                    Some(s) => s,
                    None => continue,
                };
                let two1 = match p
                    .parent()
                    .and_then(|d| d.file_name())
                    .and_then(|s| s.to_str())
                {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let two0 = match p
                    .parent()
                    .and_then(|d| d.parent())
                    .and_then(|d| d.file_name())
                    .and_then(|s| s.to_str())
                {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let full_hex = format!("{}{}{}", two0, two1, stem);
                let bytes = match hex::decode(&full_hex) {
                    Ok(b) if b.len() == 32 => b,
                    _ => continue,
                };
                let mut k = [0u8; 32];
                k.copy_from_slice(&bytes);
                let size = entry.metadata()?.len();
                out.push((k, p, size));
            }
        }
        Ok(())
    }

    let mut found = Vec::new();
    walk(root, &mut found).ok();
    // We don't know the original `ObjectPath` for orphans; use a
    // sentinel that no real caller will produce so `invalidate_path`
    // never matches them. The orphan still ages out via LRU.
    let orphan_path = ObjectPath::from("__basin_disk_cache_orphan__");
    for (k, p, size) in found {
        index.lru.put(
            k,
            FileMeta {
                fs_path: p,
                size,
                object_path: orphan_path.clone(),
            },
        );
        index.current_bytes = index.current_bytes.saturating_add(size);
    }
    // Run one eviction pass in case the on-disk cache exceeds the cap.
    let to_delete = index.evict_to_budget_with(Vec::new());
    for p in to_delete {
        let _ = std::fs::remove_file(p);
    }
    Ok(())
}

impl std::fmt::Display for DiskCachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskCachedStore({})", self.inner)
    }
}

impl std::fmt::Debug for DiskCachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCachedStore")
            .field("root", &self.state.root)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ObjectStore for DiskCachedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let res = self.inner.put_opts(location, payload, opts).await;
        // Invalidate after the inner PUT regardless of outcome — a
        // failed PUT shouldn't leave stale-but-consistent cache entries
        // behind, and a successful PUT obviously requires invalidation.
        self.invalidate(location).await;
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // We cannot intercept the trailing `complete()`, so the safe
        // thing is to drop any cached entries up-front. Any subsequent
        // GET will re-fetch. The cost is one extra cold read, paid by
        // a code path that is rarely on the hot loop today.
        self.invalidate(location).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Pre-condition checks (if_match / if_none_match / version /
        // head) are not safe to serve from the cache — they require
        // the inner store to evaluate them against live metadata. Bail
        // straight through to the inner store in those cases.
        let safe_to_cache = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none()
            && !options.head;

        if !safe_to_cache {
            return self.inner.get_opts(location, options).await;
        }

        let range = options.range.clone();
        let bytes = self.get_cached(location, range.clone()).await?;

        // Fabricate a GetResult around the cached bytes. The
        // `ObjectMeta` size is the *full object size* in the
        // contract; for range reads we report the cached fragment's
        // size, which is what the caller will end up with on `bytes()`.
        // Downstream consumers of the read path (parquet) only inspect
        // `payload`, so this is safe in practice. The `meta` field is
        // populated best-effort.
        let len = bytes.len();
        let range_resolved = match &range {
            None => 0..len,
            Some(GetRange::Bounded(r)) => r.clone(),
            Some(GetRange::Offset(o)) => *o..(*o + len),
            Some(GetRange::Suffix(s)) => 0..*s,
        };
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size: len,
            e_tag: None,
            version: None,
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range: range_resolved,
            attributes: object_store::Attributes::default(),
        })
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<Bytes> {
        self.get_cached(location, Some(GetRange::Bounded(range)))
            .await
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        // HEAD is metadata, not bytes — pass through.
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        let res = self.inner.delete(location).await;
        // Invalidate even on inner-DELETE failure: the caller is
        // signalling intent to remove the file, and a stale cache entry
        // after a partial delete is the harder bug to debug.
        self.invalidate(location).await;
        res
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        let res = self.inner.copy(from, to).await;
        self.invalidate(to).await;
        res
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        let res = self.inner.copy_if_not_exists(from, to).await;
        self.invalidate(to).await;
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use object_store::local::LocalFileSystem;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// Counts inner GETs so a test can prove the cache short-circuited
    /// the second read.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: AtomicUsize,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn payload(b: &[u8]) -> PutPayload {
        PutPayload::from_bytes(Bytes::copy_from_slice(b))
    }

    #[tokio::test]
    async fn cache_hit_avoids_inner_get() {
        let inner_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let inner_real = Arc::new(LocalFileSystem::new_with_prefix(inner_dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner: inner_real,
            gets: AtomicUsize::new(0),
        });
        let cached = DiskCachedStore::new(
            counting.clone() as Arc<dyn ObjectStore>,
            DiskCacheConfig::new(cache_dir.path().to_path_buf(), 1024 * 1024),
        )
        .unwrap();

        let p = ObjectPath::from("k");
        cached.put(&p, payload(b"hello")).await.unwrap();

        let n0 = counting.gets.load(Ordering::Relaxed);
        let r1 = cached.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&r1[..], b"hello");
        let n1 = counting.gets.load(Ordering::Relaxed);
        assert_eq!(n1, n0 + 1, "first read should hit inner once");

        let r2 = cached.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&r2[..], b"hello");
        let n2 = counting.gets.load(Ordering::Relaxed);
        assert_eq!(n2, n1, "second read should be served from cache");

        let counters = cached.counters();
        assert!(counters.hits >= 1);
        assert!(counters.misses >= 1);
    }

    #[tokio::test]
    async fn delete_invalidates_cache() {
        let inner_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let inner_real = Arc::new(LocalFileSystem::new_with_prefix(inner_dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner: inner_real,
            gets: AtomicUsize::new(0),
        });
        let cached = DiskCachedStore::new(
            counting.clone() as Arc<dyn ObjectStore>,
            DiskCacheConfig::new(cache_dir.path().to_path_buf(), 1024 * 1024),
        )
        .unwrap();

        let p = ObjectPath::from("k");
        cached.put(&p, payload(b"v1")).await.unwrap();
        cached.get(&p).await.unwrap().bytes().await.unwrap(); // populate
        cached.get(&p).await.unwrap().bytes().await.unwrap(); // hit

        cached.delete(&p).await.unwrap();
        cached.put(&p, payload(b"v2")).await.unwrap();

        let n_before = counting.gets.load(Ordering::Relaxed);
        let bytes = cached.get(&p).await.unwrap().bytes().await.unwrap();
        let n_after = counting.gets.load(Ordering::Relaxed);
        assert_eq!(&bytes[..], b"v2");
        assert_eq!(n_after, n_before + 1, "post-PUT read must miss the cache");
    }

    #[tokio::test]
    async fn range_keys_dont_collide() {
        let inner_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let inner_real = Arc::new(LocalFileSystem::new_with_prefix(inner_dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner: inner_real,
            gets: AtomicUsize::new(0),
        });
        let cached = DiskCachedStore::new(
            counting.clone() as Arc<dyn ObjectStore>,
            DiskCacheConfig::new(cache_dir.path().to_path_buf(), 1024 * 1024),
        )
        .unwrap();

        let p = ObjectPath::from("k");
        cached.put(&p, payload(b"abcdefghij")).await.unwrap();

        let r1 = cached.get_range(&p, 0..3).await.unwrap();
        assert_eq!(&r1[..], b"abc");
        let r2 = cached.get_range(&p, 5..8).await.unwrap();
        assert_eq!(&r2[..], b"fgh");

        // Each distinct range is a distinct key, so each is its own
        // cold miss + warm hit.
        let n1 = counting.gets.load(Ordering::Relaxed);
        let r1b = cached.get_range(&p, 0..3).await.unwrap();
        assert_eq!(&r1b[..], b"abc");
        let r2b = cached.get_range(&p, 5..8).await.unwrap();
        assert_eq!(&r2b[..], b"fgh");
        let n2 = counting.gets.load(Ordering::Relaxed);
        assert_eq!(n2, n1, "both ranges should be warm now");
    }

    #[tokio::test]
    async fn lru_evicts_when_over_budget() {
        let inner_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let inner_real = Arc::new(LocalFileSystem::new_with_prefix(inner_dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner: inner_real,
            gets: AtomicUsize::new(0),
        });
        // 16-byte cap. Two 10-byte values force eviction of the older
        // on the second insert.
        let cached = DiskCachedStore::new(
            counting.clone() as Arc<dyn ObjectStore>,
            DiskCacheConfig::new(cache_dir.path().to_path_buf(), 16),
        )
        .unwrap();

        let a = ObjectPath::from("a");
        let b = ObjectPath::from("b");
        cached.put(&a, payload(&[0u8; 10])).await.unwrap();
        cached.put(&b, payload(&[1u8; 10])).await.unwrap();

        // Populate both. After inserting the second, `a` must be
        // evicted — the budget can't fit both.
        cached.get(&a).await.unwrap().bytes().await.unwrap();
        cached.get(&b).await.unwrap().bytes().await.unwrap();

        let before = counting.gets.load(Ordering::Relaxed);
        let _ = cached.get(&a).await.unwrap().bytes().await.unwrap();
        let after = counting.gets.load(Ordering::Relaxed);
        assert!(
            after > before,
            "expected `a` to have been evicted and re-fetched"
        );
    }
}
