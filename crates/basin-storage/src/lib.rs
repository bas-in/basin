//! `basin-storage` — tenant-aware Parquet-on-object-store substrate.
//!
//! Phase 1 scope. This crate is the bottom of the Basin stack: it knows
//! nothing about SQL, transactions, the WAL, or shard owners. Its only job
//! is to write Arrow `RecordBatch`es as immutable Parquet files under a
//! strict per-tenant key prefix and read them back with predicate and
//! projection pushdown.
//!
//! Tenant isolation is enforced by funneling every object key through one
//! private helper (`paths::data_file_key`) that always begins with
//! `tenants/{tenant_id}/`. There is no public escape hatch.

#![forbid(unsafe_code)]

mod concurrency;
mod data_file;
mod disk_cache;
mod metadata_cache;
mod page_cache;
mod paths;
mod predicate;
mod reader;
mod scheduler;
mod tier;
mod vector_index;
mod writer;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use basin_common::{Result, TableName, TenantId};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::sync::Semaphore;

pub use data_file::{ColumnStats, DataFile};
pub use disk_cache::{DiskCacheConfig, DiskCacheCounters, DiskCachedStore};
pub use page_cache::{PageCache, PageCacheConfig, PageCacheCounters, PageCacheCountersSnapshot};
pub use predicate::{
    evaluate as evaluate_predicate, evaluate_compound, evaluate_compound_for_pruning,
    CompoundPredicate, Predicate, PruneOutcome, ScalarValue,
};
pub use scheduler::{TenantIoStats, DEFAULT_GLOBAL_BUDGET};
pub use tier::Tier;
pub use vector_index::{vector_index_segment_key_for_data_file, VectorHit};
pub use writer::WriteOptions;

use arrow_array::RecordBatch;

/// Configuration for [`Storage`].
#[derive(Clone)]
pub struct StorageConfig {
    pub object_store: Arc<dyn ObjectStore>,
    /// Optional bucket sub-prefix that all tenant keys are nested under.
    /// `None` means keys live directly at the bucket root.
    pub root_prefix: Option<ObjectPath>,
    /// Optional NVMe / local-SSD disk cache between Storage and the
    /// underlying object store. `None` (the default) is the legacy
    /// behaviour: every read goes straight to the inner store. When
    /// `Some(...)`, the inner store is wrapped in a [`DiskCachedStore`]
    /// **before** per-tenant wrapping so the wrapping order is
    /// `TenantScopedStore -> DiskCachedStore -> real ObjectStore` —
    /// every cache hit still counts against the requesting tenant's
    /// per-tenant concurrency permit pool.
    ///
    /// See [`DiskCacheConfig`] for the cap / root-dir knobs and
    /// [`DiskCachedStore`] for the cache shape, eviction policy, and
    /// invalidation rules. The cache is opt-in: leaving this as `None`
    /// keeps every existing test byte-identical to the pre-cache build.
    pub disk_cache: Option<DiskCacheConfig>,
    /// Optional in-RAM cache of decoded Parquet pages (Phase 5.7 A2).
    /// `None` (the default) is the legacy behaviour: every read decodes
    /// Parquet bytes into Arrow on every call. When `Some(...)`, the
    /// reader probes a `(file_path, projection_hash, filters_hash)`
    /// keyed cache before issuing the row-group scan; on hit it yields
    /// the cached `Arc<RecordBatch>`es without touching the parquet
    /// crate.
    ///
    /// Layered above the [`DiskCachedStore`]: page cache HIT skips
    /// disk-cache + decode entirely; page cache MISS falls through to
    /// the disk cache (which itself may HIT or MISS) and then decodes.
    /// See [`PageCacheConfig`] for the byte budget knob and
    /// [`PageCache`] for the eviction / invalidation rules.
    pub page_cache: Option<PageCacheConfig>,
}

impl StorageConfig {
    /// Default disk-cache budget when no override is supplied: 10 GiB.
    /// Sized so a typical NVMe-backed deployment can cache the full
    /// working set of a multi-tenant SaaS workload while leaving room
    /// for the local filesystem itself; deployments with tighter disks
    /// override via [`StorageConfig::default_disk_cache_with_root`] or
    /// the `BASIN_DISK_CACHE_MAX_BYTES` env var on the production
    /// server.
    pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

    /// Default page-cache budget when no override is supplied: 1 GiB.
    /// Mirrors [`PageCacheConfig::default`].
    pub const DEFAULT_PAGE_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

    /// Default disk-cache root directory.
    ///
    /// Resolution order:
    /// 1. `BASIN_DISK_CACHE_ROOT` env var, if set and non-empty.
    /// 2. `std::env::temp_dir().join("basin-disk-cache")` otherwise.
    ///
    /// The production server (`services/basin-server`) instead defaults
    /// to `<XDG_CACHE_HOME or ~/.cache>/basin/disk-cache` because the
    /// system temp dir is not durable across reboots. Tests deliberately
    /// use the temp-dir fallback so per-test isolation is automatic.
    pub fn default_disk_cache_root() -> std::path::PathBuf {
        if let Ok(v) = std::env::var("BASIN_DISK_CACHE_ROOT") {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return std::path::PathBuf::from(trimmed);
            }
        }
        std::env::temp_dir().join("basin-disk-cache")
    }

    /// `Some(DiskCacheConfig)` populated from the default root + budget.
    /// Convenience for tests / fixtures that want caches on without
    /// hand-rolling the path.
    pub fn default_disk_cache() -> DiskCacheConfig {
        DiskCacheConfig::new(Self::default_disk_cache_root(), Self::DEFAULT_DISK_CACHE_BYTES)
    }

    /// Like [`default_disk_cache`](Self::default_disk_cache) but with an
    /// explicit root. Tests pass a per-test tempdir so concurrent tests
    /// don't share cache state.
    pub fn default_disk_cache_with_root(root: impl Into<std::path::PathBuf>) -> DiskCacheConfig {
        DiskCacheConfig::new(root, Self::DEFAULT_DISK_CACHE_BYTES)
    }

    /// `Some(PageCacheConfig)` populated from the default budget.
    pub fn default_page_cache() -> PageCacheConfig {
        PageCacheConfig::new(Self::DEFAULT_PAGE_CACHE_BYTES)
    }
}

impl Default for StorageConfig {
    /// Production-shaped defaults. The `object_store` slot is filled
    /// with an [`object_store::memory::InMemory`] instance so the type
    /// is `Default`-constructable; callers are expected to overwrite
    /// `object_store` (and usually `root_prefix`) for any non-test use.
    /// The cache fields are populated with sensible budgets:
    /// disk cache rooted under [`Self::default_disk_cache_root`] with
    /// [`Self::DEFAULT_DISK_CACHE_BYTES`], page cache with
    /// [`Self::DEFAULT_PAGE_CACHE_BYTES`].
    ///
    /// Existing fixtures that build `StorageConfig { ..., disk_cache:
    /// None, page_cache: None }` literally are unaffected — they
    /// explicitly opt out, which is what the unit-test layer in the
    /// per-crate `#[cfg(test)]` modules wants.
    fn default() -> Self {
        Self {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            root_prefix: None,
            disk_cache: Some(Self::default_disk_cache()),
            page_cache: Some(Self::default_page_cache()),
        }
    }
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("root_prefix", &self.root_prefix)
            .finish_non_exhaustive()
    }
}

/// Default per-tenant concurrent in-flight RPC cap. Must be ≥ the peak
/// concurrent permits a single Parquet scan needs (DataFusion target
/// partitions × parquet async range fan-out) AND ≥ that figure × the
/// number of concurrent queries one tenant might run — otherwise the
/// parquet reader's range fan-out deadlocks. Empirically cap=4 and
/// cap=8 both deadlock under the s3_scaling_noisy_neighbor workload
/// (4 concurrent full scans of 1M rows); 16 holds liveness. Per-tenant
/// state is O(1) per tenant (~140 bytes for the Semaphore), so this
/// scales to 1M tenants. v0.2 surfaces the value per tenant from the
/// catalog so a noisy tenant can be capped without throttling quiet
/// tenants.
pub const DEFAULT_TENANT_CONCURRENCY: usize = 16;

/// Read knobs for [`Storage::read`]. Filters are ANDed together.
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub projection: Option<Vec<String>>,
    pub filters: Vec<Predicate>,
    pub partition: Option<basin_common::PartitionKey>,
}

/// Tenant-aware Parquet store. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    object_store: Arc<dyn ObjectStore>,
    root_prefix: Option<ObjectPath>,
    /// Footer cache for the read path. See `metadata_cache` for the
    /// invalidation invariant (data files are immutable, so no explicit
    /// eviction is required — LRU + delete-by-compactor is sufficient).
    parquet_meta_cache: Arc<metadata_cache::ParquetMetaCache>,
    /// Parsed-HNSW-segment cache for the vector-search path. Same
    /// invariants apply.
    hnsw_segment_cache: Arc<metadata_cache::HnswSegmentCache>,
    /// Per-tenant concurrent-RPC semaphores, lazy-allocated on first
    /// use. The mutex protects the map only — permits are acquired on
    /// the [`Semaphore`] itself outside the mutex, so the lock is held
    /// for nanoseconds and never across an `await`. We accept the cost
    /// of one map lookup per RPC; on the hot path it's a HashMap probe
    /// with `TenantId` (a [`Ulid`] under the hood, so cheap to hash).
    tenant_semaphores: Mutex<HashMap<TenantId, Arc<Semaphore>>>,
    /// Default permit count for a newly-created tenant semaphore.
    default_tenant_concurrency: usize,
    /// Process-wide counters incremented by the read path so tests can
    /// assert that bloom-filter / stats pruning is doing real work.
    /// `row_groups_considered` counts every row group in every Parquet
    /// file the reader inspected; `row_groups_scanned` counts those that
    /// survived pruning and were actually read. The difference is the
    /// pruning win. Counters are best-effort and not load-bearing for
    /// correctness — they exist purely for observability.
    read_counters: Arc<ReadCounters>,
    /// Optional in-RAM cache of decoded `RecordBatch`es. See
    /// [`StorageConfig::page_cache`] for the rationale; `None` is the
    /// default (every read decodes from Parquet bytes).
    page_cache: Option<Arc<PageCache>>,
}

/// Best-effort counters for the read path. See [`Inner::read_counters`].
#[derive(Debug, Default)]
pub struct ReadCounters {
    pub row_groups_considered: AtomicU64,
    pub row_groups_scanned: AtomicU64,
    pub row_groups_pruned_by_stats: AtomicU64,
    pub row_groups_pruned_by_bloom: AtomicU64,
}

impl ReadCounters {
    pub fn snapshot(&self) -> ReadCountersSnapshot {
        ReadCountersSnapshot {
            row_groups_considered: self.row_groups_considered.load(Ordering::Relaxed),
            row_groups_scanned: self.row_groups_scanned.load(Ordering::Relaxed),
            row_groups_pruned_by_stats: self.row_groups_pruned_by_stats.load(Ordering::Relaxed),
            row_groups_pruned_by_bloom: self.row_groups_pruned_by_bloom.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.row_groups_considered.store(0, Ordering::Relaxed);
        self.row_groups_scanned.store(0, Ordering::Relaxed);
        self.row_groups_pruned_by_stats.store(0, Ordering::Relaxed);
        self.row_groups_pruned_by_bloom.store(0, Ordering::Relaxed);
    }
}

/// Plain-data view of [`ReadCounters`]; cheap to copy for assertions.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadCountersSnapshot {
    pub row_groups_considered: u64,
    pub row_groups_scanned: u64,
    pub row_groups_pruned_by_stats: u64,
    pub row_groups_pruned_by_bloom: u64,
}

/// Default capacity for the Parquet footer cache. 1024 entries is a few MB
/// of footer in the pessimistic case and sufficient to cover the working set
/// of every benchmark currently in `tests/integration`.
const DEFAULT_PARQUET_META_CACHE_CAP: usize = 1024;

/// Default capacity for the HNSW segment cache. 256 segments at "few MB
/// each" is the largest we'd want to hold in process; in practice the cache
/// stays much smaller because workloads concentrate on a handful of
/// segments per (tenant, table, column).
const DEFAULT_HNSW_SEGMENT_CACHE_CAP: usize = 256;

impl Storage {
    pub fn new(cfg: StorageConfig) -> Self {
        // If a disk cache is configured, wrap the supplied object store
        // with [`DiskCachedStore`] *before* the per-tenant wrapping that
        // [`Storage::tenant_object_store`] adds on top. The result is
        // `TenantScopedStore -> DiskCachedStore -> real ObjectStore`
        // for every read, which is what the design calls for: cache
        // hits still count against the tenant's permit pool, and cache
        // keys are content-addressed on the path (which always carries
        // the tenant prefix) so cross-tenant key collisions are
        // mechanically impossible.
        //
        // If construction of the cache fails (cache root not writable,
        // for example), we fall back to the un-wrapped store and log
        // — the cache is a performance tier, not the durability
        // boundary.
        let object_store = match cfg.disk_cache.clone() {
            Some(dc) => match disk_cache::DiskCachedStore::new(cfg.object_store.clone(), dc) {
                Ok(wrapped) => Arc::new(wrapped) as Arc<dyn ObjectStore>,
                Err(e) => {
                    tracing::warn!(
                        target = "basin_storage",
                        error = %e,
                        "disk_cache: setup failed; falling back to direct object store",
                    );
                    cfg.object_store
                }
            },
            None => cfg.object_store,
        };

        let page_cache = cfg
            .page_cache
            .map(|pc| Arc::new(PageCache::new(pc)));

        Self {
            inner: Arc::new(Inner {
                object_store,
                root_prefix: cfg.root_prefix,
                parquet_meta_cache: Arc::new(metadata_cache::ParquetMetaCache::new(
                    DEFAULT_PARQUET_META_CACHE_CAP,
                )),
                hnsw_segment_cache: Arc::new(metadata_cache::HnswSegmentCache::new(
                    DEFAULT_HNSW_SEGMENT_CACHE_CAP,
                )),
                tenant_semaphores: Mutex::new(HashMap::new()),
                default_tenant_concurrency: DEFAULT_TENANT_CONCURRENCY,
                read_counters: Arc::new(ReadCounters::default()),
                page_cache,
            }),
        }
    }

    /// Handle to the in-RAM page cache, or `None` if the cache is not
    /// enabled. Exposed for observability and for tests that want to
    /// assert hit/miss/eviction counters; production callers should
    /// not poke at the cache directly.
    pub fn page_cache(&self) -> Option<&Arc<PageCache>> {
        self.inner.page_cache.as_ref()
    }

    /// Process-wide read counters. See [`ReadCounters`] for what is tracked.
    /// These are observability-only and not part of the storage correctness
    /// contract — production callers should ignore them; tests use them to
    /// confirm that bloom / stats pruning is firing.
    pub fn read_counters(&self) -> &Arc<ReadCounters> {
        &self.inner.read_counters
    }

    /// Per-tenant live I/O stats. Stub: the v0.2 scheduler exported
    /// real numbers here; with the scheduler reverted, all fields are
    /// zero. The noisy detector falls back to its own per-engine
    /// counter.
    pub fn tenant_stats(&self, _tenant: &TenantId) -> TenantIoStats {
        TenantIoStats::default()
    }


    /// Per-tenant semaphore handle. Used internally to gate every
    /// underlying object_store RPC behind a tenant-scoped permit pool.
    /// Lazy-allocates on first call for a given tenant.
    fn tenant_semaphore(&self, tenant: &TenantId) -> Arc<Semaphore> {
        let mut map = self
            .inner
            .tenant_semaphores
            .lock()
            .expect("tenant semaphore map poisoned");
        if let Some(s) = map.get(tenant) {
            return s.clone();
        }
        let s = Arc::new(Semaphore::new(self.inner.default_tenant_concurrency));
        map.insert(*tenant, s.clone());
        s
    }

    /// Tenant-scoped view of the underlying object store. Every RPC
    /// (get / put / list / head / delete / copy) is gated on this
    /// tenant's semaphore so one tenant's heavy traffic cannot starve
    /// another tenant's quiet traffic.
    ///
    /// This is the right thing to register with DataFusion's runtime
    /// (`SessionContext::register_object_store`) for a session bound to
    /// `tenant`: every range read DataFusion drives for that session
    /// will then count against the tenant's permit pool.
    pub fn tenant_object_store(&self, tenant: &TenantId) -> Arc<dyn ObjectStore> {
        let sem = self.tenant_semaphore(tenant);
        Arc::new(concurrency::TenantScopedStore::new(
            self.inner.object_store.clone(),
            sem,
        ))
    }

    /// Internal accessor: returns the wrapped tenant-scoped store as a
    /// concrete `Arc<dyn ObjectStore>`, identical to
    /// [`tenant_object_store`] but at `pub(crate)` visibility for the
    /// reader / writer / vector-index modules.
    pub(crate) fn tenant_store(&self, tenant: &TenantId) -> Arc<dyn ObjectStore> {
        self.tenant_object_store(tenant)
    }

    pub(crate) fn parquet_meta_cache(&self) -> &Arc<metadata_cache::ParquetMetaCache> {
        &self.inner.parquet_meta_cache
    }

    pub(crate) fn page_cache_handle(&self) -> Option<&Arc<PageCache>> {
        self.inner.page_cache.as_ref()
    }

    pub(crate) fn hnsw_segment_cache(&self) -> &Arc<metadata_cache::HnswSegmentCache> {
        &self.inner.hnsw_segment_cache
    }

    /// The underlying [`ObjectStore`]. Exposed so higher layers (e.g.
    /// `basin-engine`) can register the same store with DataFusion's runtime
    /// without round-tripping through configuration. Tenant-prefix
    /// enforcement still happens inside `Storage`'s own read/write methods;
    /// callers handed a raw `ObjectStore` are responsible for not crossing
    /// tenant boundaries themselves.
    pub fn object_store_handle(&self) -> Arc<dyn ObjectStore> {
        self.inner.object_store.clone()
    }

    pub(crate) fn root_prefix(&self) -> Option<&ObjectPath> {
        self.inner.root_prefix.as_ref()
    }

    /// The configured root prefix, if any. Same caveats as
    /// [`Storage::object_store_handle`].
    pub fn root_prefix_handle(&self) -> Option<ObjectPath> {
        self.inner.root_prefix.clone()
    }

    /// Write one `RecordBatch` as an immutable Parquet file. Returns the
    /// resulting `DataFile` descriptor.
    #[tracing::instrument(skip(self, batch), fields(tenant=%tenant, table=%table, partition=%partition, rows=batch.num_rows()))]
    pub async fn write_batch(
        &self,
        tenant: &TenantId,
        table: &TableName,
        partition: &basin_common::PartitionKey,
        batch: &RecordBatch,
    ) -> Result<DataFile> {
        writer::write_batch(self, tenant, table, partition, batch).await
    }

    /// Like [`write_batch`](Self::write_batch) but with explicit per-write
    /// knobs (bloom-filter columns, row-group size). Used by callers that
    /// have read [`basin_catalog::TableMetadata::bloom_filter_columns`] and
    /// want the writer to materialise bloom filters in the Parquet footer.
    /// All defaults preserve the legacy [`write_batch`] behaviour exactly.
    #[tracing::instrument(skip(self, batch, opts), fields(tenant=%tenant, table=%table, partition=%partition, rows=batch.num_rows(), bloom_cols=opts.bloom_filter_columns.len()))]
    pub async fn write_batch_with_options(
        &self,
        tenant: &TenantId,
        table: &TableName,
        partition: &basin_common::PartitionKey,
        batch: &RecordBatch,
        opts: &WriteOptions,
    ) -> Result<DataFile> {
        writer::write_batch_with_options(self, tenant, table, partition, batch, opts).await
    }

    /// Stream all rows for one tenant+table that match the read options.
    #[tracing::instrument(skip(self, opts), fields(tenant=%tenant, table=%table))]
    pub async fn read(
        &self,
        tenant: &TenantId,
        table: &TableName,
        opts: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read(self, tenant, table, opts).await
    }

    /// List the data files for one tenant+table without reading their bodies.
    #[tracing::instrument(skip(self), fields(tenant=%tenant, table=%table))]
    pub async fn list_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<DataFile>> {
        reader::list_data_files(self, tenant, table).await
    }

    /// Like [`list_data_files`](Self::list_data_files) but populates each
    /// returned [`DataFile::row_count`] and [`DataFile::column_stats`] by
    /// fetching the Parquet footer (cached). Used by the copy-on-write
    /// UPDATE/DELETE pruner; reading-path callers prefer the cheaper
    /// `list_data_files`.
    #[tracing::instrument(skip(self), fields(tenant=%tenant, table=%table))]
    pub async fn list_data_files_with_stats(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<DataFile>> {
        reader::list_data_files_with_stats(self, tenant, table).await
    }

    /// Read every batch from a single Parquet data file. Used by the
    /// UPDATE/DELETE pruner when only some files need processing — the
    /// table-wide [`read`](Self::read) would force us to merge all files
    /// regardless of pruning.
    ///
    /// `path` must be a path returned by a prior [`list_data_files`]
    /// against the same tenant; passing a foreign path is the caller's
    /// bug (we only enforce tenant isolation at the listing/path-key
    /// boundary, not on a raw `read_file`). The `tenant` argument is
    /// purely so the read counts against this tenant's per-tenant
    /// concurrency permit pool — it is not re-validated against `path`.
    #[tracing::instrument(skip(self), fields(tenant=%tenant, path=%path))]
    pub async fn read_file(
        &self,
        tenant: &TenantId,
        path: &ObjectPath,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read_file(self, tenant, path).await
    }

    /// Copy a hot-tier data file to its cold-tier sibling and return the new
    /// [`DataFile`] descriptor. The original hot file is **left in place** —
    /// callers must atomically commit the catalog swap (`replace_data_files`)
    /// first and only then call [`Self::delete_file`] on the old path. This
    /// ordering ensures a crash mid-migration leaves the catalog pointing
    /// at a still-valid object.
    ///
    /// `from` must already exist under `tables/<t>/data/...`. If it doesn't
    /// follow the canonical layout the call returns `BasinError::Storage`.
    /// Files already in the cold tier are returned unchanged (the descriptor
    /// is rebuilt by re-stat'ing the cold object).
    #[tracing::instrument(skip(self), fields(tenant=%tenant, from=%from))]
    pub async fn migrate_to_cold(
        &self,
        tenant: &TenantId,
        from: &ObjectPath,
    ) -> Result<DataFile> {
        // Already cold? Re-stat and return without touching anything.
        if matches!(Tier::from_path(from.as_ref()), Tier::Cold) {
            let store = self.tenant_store(tenant);
            let head = store
                .head(from)
                .await
                .map_err(|e| basin_common::BasinError::storage(format!("head {from}: {e}")))?;
            return Ok(DataFile {
                path: from.clone(),
                size_bytes: head.size as u64,
                row_count: 0,
                column_stats: std::collections::BTreeMap::new(),
                tier: Tier::Cold,
            });
        }

        let to = paths::rewrite_to_cold(from).ok_or_else(|| {
            basin_common::BasinError::storage(format!(
                "migrate_to_cold: path does not match `tables/<t>/data/...`: {from}"
            ))
        })?;
        let store = self.tenant_store(tenant);
        // Belt-and-braces: confirm the target sits under this tenant's prefix.
        // The path was derived from `from`, which already cleared this check
        // at write time — we re-check defensively.
        let expected_prefix = format!("tenants/{}/", tenant.as_prefix());
        if !to.as_ref().contains(&expected_prefix) {
            return Err(basin_common::BasinError::isolation(format!(
                "migrate_to_cold target {to} missing tenant prefix {expected_prefix}"
            )));
        }
        // Use `copy` (overwrites) rather than `copy_if_not_exists` because a
        // stale prior attempt may have left a partial cold object behind; the
        // ULID embedded in the filename keeps cold-vs-cold collisions out of
        // the picture.
        store
            .copy(from, &to)
            .await
            .map_err(|e| basin_common::BasinError::storage(format!("copy {from} -> {to}: {e}")))?;
        let head = store
            .head(&to)
            .await
            .map_err(|e| basin_common::BasinError::storage(format!("head cold {to}: {e}")))?;
        Ok(DataFile {
            path: to,
            size_bytes: head.size as u64,
            row_count: 0,
            column_stats: std::collections::BTreeMap::new(),
            tier: Tier::Cold,
        })
    }

    /// Best-effort delete of one tenant-owned object. Used by the tiering
    /// compactor after an atomic catalog replace_data_files; failure is
    /// logged but not propagated because the catalog is already authoritative
    /// — a leftover hot object is wasted bytes, not a correctness violation.
    ///
    /// The caller is responsible for ensuring `path` is under `tenant`'s
    /// prefix; we re-check defensively.
    #[tracing::instrument(skip(self), fields(tenant=%tenant, path=%path))]
    pub async fn delete_file(&self, tenant: &TenantId, path: &ObjectPath) -> Result<()> {
        let expected_prefix = format!("tenants/{}/", tenant.as_prefix());
        if !path.as_ref().contains(&expected_prefix) {
            return Err(basin_common::BasinError::isolation(format!(
                "delete_file: {path} missing tenant prefix {expected_prefix}"
            )));
        }
        let store = self.tenant_store(tenant);
        store
            .delete(path)
            .await
            .map_err(|e| basin_common::BasinError::storage(format!("delete {path}: {e}")))?;
        // Drop any cached decoded batches for this file. Disk-cache
        // invalidation already happens inside `DiskCachedStore::delete`;
        // the page cache is one layer up and needs its own hook.
        if let Some(pc) = self.page_cache_handle() {
            pc.invalidate_path(path);
        }
        Ok(())
    }

    /// Bulk-delete every object under `tenant`'s key prefix. Returns the
    /// number of objects deleted.
    ///
    /// Implementation strategy:
    ///
    /// 1. List the tenant prefix through the per-tenant gated store, so
    ///    the LIST itself counts against this tenant's permit pool.
    /// 2. Pipe the resulting path stream through the **inner** object
    ///    store's `delete_stream`, *not* the wrapped one. The wrapper's
    ///    default `delete_stream` calls our gated `delete()` once per
    ///    key and `.buffered(10)` — that means LocalFS gets 10-way
    ///    parallel deletes (good) but S3 misses the native
    ///    `DeleteObjects` batching (1000 keys per request, 20 batches in
    ///    flight) that the AWS backend overrides. By piping into the
    ///    inner store directly we get S3's bulk path on real cloud
    ///    deployments and a 10-way `buffered` fan-out on LocalFS.
    /// 3. The fan-out is still bounded — on LocalFS by `buffered(10)`,
    ///    on S3 by the bucket-quota for `DeleteObjects`. We don't add
    ///    another semaphore around the bulk path because the LIST in
    ///    step 1 already counted against this tenant's pool, and the
    ///    deletes themselves are bounded by the underlying store's
    ///    own concurrency knobs.
    ///
    /// The method intentionally does *not* call `Catalog::drop_table`:
    /// catalog state is the engine's responsibility. This method's
    /// contract is "physically free the bytes under the tenant prefix".
    /// A typical caller (the engine's tenant-deletion path) drops the
    /// catalog rows first and only then asks storage to remove the data.
    #[tracing::instrument(skip(self), fields(tenant=%tenant))]
    pub async fn delete_tenant_prefix(&self, tenant: &TenantId) -> Result<usize> {
        // Build the tenant's full prefix, honouring the optional root.
        let mut p = self
            .inner
            .root_prefix
            .clone()
            .unwrap_or_else(|| ObjectPath::from(""));
        p = p.child(paths::TENANTS_SEGMENT);
        p = p.child(tenant.as_prefix());

        // Step 1: gated LIST.
        let gated = self.tenant_object_store(tenant);
        let paths_stream = gated.list(Some(&p)).map_ok(|m| m.location).boxed();

        // Step 2: hand the path stream to the *inner* store's
        // `delete_stream`. On AWS this picks up the
        // `aws::AmazonS3::delete_stream` override (1000-key batches,
        // 20-way parallel); on LocalFS / GCS / Azure it falls through
        // to the default `.buffered(10)` impl which still gives a 10×
        // speedup over a serial loop.
        let inner = self.inner.object_store.clone();
        let deleted: Vec<ObjectPath> = inner
            .delete_stream(paths_stream)
            .try_collect()
            .await
            .map_err(|e| {
                basin_common::BasinError::storage(format!(
                    "delete_tenant_prefix({tenant}): {e}"
                ))
            })?;
        // Drop any cached decoded batches for each deleted file. Same
        // rationale as `delete_file`: page cache lives one layer above
        // the disk cache and needs its own invalidation hook.
        if let Some(pc) = self.page_cache_handle() {
            for p in &deleted {
                pc.invalidate_path(p);
            }
        }
        Ok(deleted.len())
    }

    /// Approximate nearest-neighbour search across all HNSW segments for
    /// `(tenant, table, column)`. Returns the merged top-`k` by distance.
    ///
    /// The returned `VectorHit::file_path` currently points at the index
    /// segment file rather than the matching Parquet data file — we don't
    /// track the data-side correspondence yet (see ADR 0003 for the
    /// compactor-driven plan). Higher layers (see `basin-engine`) resolve
    /// the row by reading the table's Parquet files and matching `row_id`.
    #[tracing::instrument(skip(self, query), fields(tenant=%tenant, table=%table, column=%column, k=%k))]
    pub async fn vector_search(
        &self,
        tenant: &TenantId,
        table: &TableName,
        column: &str,
        query: &[f32],
        k: usize,
        distance: basin_vector::Distance,
    ) -> Result<Vec<VectorHit>> {
        vector_index::vector_search(self, tenant, table, column, query, k, distance).await
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("root_prefix", &self.inner.root_prefix)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    //! Crate-level integration tests live alongside the modules they exercise.
    //! See `paths.rs`, `writer.rs`, and `reader.rs` for the per-module suites,
    //! plus `tests_e2e` below for the cross-module roundtrip cases the task
    //! brief calls out explicitly.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{PartitionKey, TableName, TenantId};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts,
        PutOptions, PutPayload, PutResult,
    };
    use tempfile::TempDir;

    fn small_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn small_batch(start: i64, len: usize, name_prefix: &str) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        let owned: Vec<String> = (0..len)
            .map(|i| format!("{name_prefix}{}", start + i as i64))
            .collect();
        let names: StringArray = owned.iter().map(|s| Some(s.as_str())).collect();
        RecordBatch::try_new(small_schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn storage_in(dir: &TempDir) -> Storage {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        })
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let tenant = TenantId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();

        let batch = small_batch(0, 1_000, "row-");
        let df = s.write_batch(&tenant, &table, &part, &batch).await.unwrap();
        assert_eq!(df.row_count, 1_000);
        assert!(df.path.as_ref().contains(&format!("tenants/{tenant}/")));

        let stream = s
            .read(&tenant, &table, ReadOptions::default())
            .await
            .unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches
            .iter()
            .map(|b| b.as_ref().unwrap().num_rows())
            .sum();
        assert_eq!(total, 1_000);

        let first = batches[0].as_ref().unwrap();
        let names = first
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "row-0");
    }

    #[tokio::test]
    async fn tenant_isolation() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.write_batch(&a, &table, &part, &small_batch(0, 10, "a-"))
            .await
            .unwrap();
        s.write_batch(&b, &table, &part, &small_batch(0, 20, "b-"))
            .await
            .unwrap();

        let collect = |t: TenantId| {
            let s = s.clone();
            let table = table.clone();
            async move {
                let stream = s.read(&t, &table, ReadOptions::default()).await.unwrap();
                let batches: Vec<_> = stream.collect::<Vec<_>>().await;
                let total: usize = batches
                    .iter()
                    .map(|b| b.as_ref().unwrap().num_rows())
                    .sum();
                let any_name = batches
                    .first()
                    .map(|b| {
                        b.as_ref()
                            .unwrap()
                            .column_by_name("name")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0)
                            .to_string()
                    })
                    .unwrap_or_default();
                (total, any_name)
            }
        };
        let (na, name_a) = collect(a).await;
        let (nb, name_b) = collect(b).await;
        assert_eq!(na, 10);
        assert_eq!(nb, 20);
        assert!(name_a.starts_with("a-"));
        assert!(name_b.starts_with("b-"));
    }

    #[tokio::test]
    async fn projection_pushdown() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let tenant = TenantId::new();
        let table = TableName::new("wide").unwrap();
        let part = PartitionKey::default_key();

        // Five-column schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
            Field::new("e", DataType::Int64, false),
        ]));
        let make_col = |off: i64| -> Arc<dyn arrow_array::Array> {
            Arc::new((off..off + 100).collect::<Int64Array>())
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                make_col(0),
                make_col(1000),
                make_col(2000),
                make_col(3000),
                make_col(4000),
            ],
        )
        .unwrap();
        s.write_batch(&tenant, &table, &part, &batch).await.unwrap();

        let opts = ReadOptions {
            projection: Some(vec!["a".into(), "c".into()]),
            ..Default::default()
        };
        let stream = s.read(&tenant, &table, opts).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let first = batches[0].as_ref().unwrap();
        assert_eq!(first.num_columns(), 2);
        let fields: Vec<_> = first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, vec!["a".to_string(), "c".to_string()]);
    }

    /// Wraps an object store and counts byte-range GETs so we can prove that
    /// row-group pruning skipped most of the file.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        range_gets: AtomicUsize,
        range_bytes: AtomicUsize,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
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
            if let Some(r) = options.range.as_ref() {
                self.range_gets.fetch_add(1, Ordering::Relaxed);
                if let object_store::GetRange::Bounded(rng) = r {
                    self.range_bytes
                        .fetch_add(rng.end.saturating_sub(rng.start), Ordering::Relaxed);
                }
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
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

    #[tokio::test]
    async fn predicate_pushdown_prunes_row_groups() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner,
            range_gets: AtomicUsize::new(0),
            range_bytes: AtomicUsize::new(0),
        });
        let s = Storage::new(StorageConfig {
            object_store: counting.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let tenant = TenantId::new();
        let table = TableName::new("rg").unwrap();
        let part = PartitionKey::default_key();

        // Need ≥ 2 row groups for pruning to do work. Writer's row-group
        // cap is 65_536, so 200_000 rows splits into ~3 groups; a point
        // query for the last id lands in only one of them.
        let batch = small_batch(0, 200_000, "v");
        s.write_batch(&tenant, &table, &part, &batch).await.unwrap();

        // Reset counters so we measure only the read path.
        counting.range_gets.store(0, Ordering::Relaxed);
        counting.range_bytes.store(0, Ordering::Relaxed);

        // id = 199_500 lives only in the last row group.
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(199_500))],
            ..Default::default()
        };
        let stream = s.read(&tenant, &table, opts).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches
            .iter()
            .map(|b| b.as_ref().unwrap().num_rows())
            .sum();
        assert!(total >= 1, "expected the matching row");

        // 10 row groups exist; pruning must drop the vast majority. We allow
        // some metadata GETs but assert overall byte volume is much less than
        // the full file. A single matching row group + footer should be far
        // under half the file size.
        let bytes = counting.range_bytes.load(Ordering::Relaxed);
        let full_file = std::fs::metadata(
            walkdir_first_parquet(dir.path())
                .expect("parquet file to exist"),
        )
        .unwrap()
        .len() as usize;
        assert!(
            bytes * 2 < full_file,
            "row-group pruning failed: read {bytes} bytes of {full_file}"
        );
    }

    fn walkdir_first_parquet(root: &std::path::Path) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(root).ok()? {
            let entry = entry.ok()?;
            let p = entry.path();
            if p.is_dir() {
                if let Some(found) = walkdir_first_parquet(&p) {
                    return Some(found);
                }
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                return Some(p);
            }
        }
        None
    }

    #[tokio::test]
    async fn list_data_files_returns_only_one_tenant() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.write_batch(&a, &table, &part, &small_batch(0, 5, "a-"))
            .await
            .unwrap();
        s.write_batch(&a, &table, &part, &small_batch(5, 5, "a-"))
            .await
            .unwrap();
        s.write_batch(&b, &table, &part, &small_batch(0, 7, "b-"))
            .await
            .unwrap();

        let listed_a = s.list_data_files(&a, &table).await.unwrap();
        let listed_b = s.list_data_files(&b, &table).await.unwrap();
        assert_eq!(listed_a.len(), 2);
        assert_eq!(listed_b.len(), 1);
        let prefix_a = format!("tenants/{a}/");
        for f in &listed_a {
            assert!(
                f.path.as_ref().contains(&prefix_a),
                "leaked path {}",
                f.path
            );
            assert!(!f.path.as_ref().contains(&format!("tenants/{b}/")));
        }
    }
}
