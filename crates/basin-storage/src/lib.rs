//! `basin-storage` — project-aware Parquet-on-object-store substrate.
//!
//! Phase 1 scope. This crate is the bottom of the Basin stack: it knows
//! nothing about SQL, transactions, the WAL, or shard owners. Its only job
//! is to write Arrow `RecordBatch`es as immutable Parquet files under a
//! strict per-project key prefix and read them back with predicate and
//! projection pushdown.
//!
//! Project isolation is enforced by funneling every object key through one
//! private helper (`paths::data_file_key`) that always begins with
//! `projects/{project_id}/`. There is no public escape hatch.

#![forbid(unsafe_code)]

pub mod backends;
mod concurrency;
pub mod index;
mod data_file;
mod disk_cache;
pub mod encryption;
pub mod encryption_static;
#[cfg(any(test, feature = "test-helpers"))]
mod latency_store;
mod metadata_cache;
mod page_cache;
mod paths;
mod predicate;
mod reader;
mod scheduler;
mod tier;
mod vector_index;
mod vortex_footer_cache;
mod vortex_format;
mod writer;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use basin_catalog::Catalog;
use basin_common::{ProjectCounterRegistry, ProjectId, Result, TableName};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::Semaphore;

pub use basin_catalog::ProjectStorageConfig;
pub use data_file::{ColumnStats, DataFile};
pub use disk_cache::{DiskCacheConfig, DiskCacheCounters, DiskCachedStore};
pub use encryption::{EncryptionProvider, WrappedKey};
pub use encryption_static::{EnvKeyEncryption, StaticKeyEncryption};
#[cfg(any(test, feature = "test-helpers"))]
pub use latency_store::LatencyStore;
pub use page_cache::{PageCache, PageCacheConfig, PageCacheCounters, PageCacheCountersSnapshot};
pub use predicate::{
    evaluate as evaluate_predicate, evaluate_compound, evaluate_compound_for_pruning,
    CompoundPredicate, Predicate, PruneOutcome, ScalarValue,
};
pub use scheduler::{ProjectIoStats, Scheduler, DEFAULT_GLOBAL_BUDGET};
pub use tier::Tier;
pub use vector_index::{vector_index_segment_key_for_data_file, VectorHit};
pub use vortex_footer_cache::VortexFooterCache;
pub use writer::{
    bloom_from_bytes, EncodingMode, FileFormat, WriteOptions, DEFAULT_MAX_ROW_GROUP_SIZE,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// Configuration for [`Storage`].
#[derive(Clone)]
pub struct StorageConfig {
    pub object_store: Arc<dyn ObjectStore>,
    /// Optional bucket sub-prefix that all project keys are nested under.
    /// `None` means keys live directly at the bucket root.
    pub root_prefix: Option<ObjectPath>,
    /// Optional NVMe / local-SSD disk cache between Storage and the
    /// underlying object store. `None` (the default) is the legacy
    /// behaviour: every read goes straight to the inner store. When
    /// `Some(...)`, the inner store is wrapped in a [`DiskCachedStore`]
    /// **before** per-project wrapping so the wrapping order is
    /// `ProjectScopedStore -> DiskCachedStore -> real ObjectStore` —
    /// every cache hit still counts against the requesting project's
    /// per-project concurrency permit pool.
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
    /// working set of a multi-project SaaS workload while leaving room
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
        DiskCacheConfig::new(
            Self::default_disk_cache_root(),
            Self::DEFAULT_DISK_CACHE_BYTES,
        )
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

/// Default per-project concurrent in-flight RPC cap. Must be ≥ the peak
/// concurrent permits a single Parquet scan needs (DataFusion target
/// partitions × parquet async range fan-out) AND ≥ that figure × the
/// number of concurrent queries one project might run — otherwise the
/// parquet reader's range fan-out deadlocks. Empirically cap=4 and
/// cap=8 both deadlock under the s3_scaling_noisy_neighbor workload
/// (4 concurrent full scans of 1M rows); 16 holds liveness. Per-project
/// state is O(1) per project (~140 bytes for the Semaphore), so this
/// scales to 1M projects. v0.2 surfaces the value per project from the
/// catalog so a noisy project can be capped without throttling quiet
/// projects.
pub const DEFAULT_PROJECT_CONCURRENCY: usize = 64;

/// Resolve the per-project concurrency cap.
///
/// `BASIN_STORAGE_PROJECT_CONCURRENCY`, when present and parseable to a
/// nonzero `usize`, overrides the default. Otherwise we use
/// `max(DEFAULT_PROJECT_CONCURRENCY, 4 * num_cpus)` so a fat host gets a
/// proportionally larger pool. The old constant of 16 was sized to
/// preserve the ADR 0008 floor on a 4-core CI box; on modern hardware
/// and concurrent-reader workloads (C=64) it became the binding wall —
/// every additional concurrent reader past 16 queued behind a busy
/// permit. The new floor of 64 keeps the per-project semaphore from
/// dominating the C=64 scaling card.
pub fn resolve_project_concurrency() -> usize {
    if let Ok(v) = std::env::var("BASIN_STORAGE_PROJECT_CONCURRENCY") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    std::cmp::max(DEFAULT_PROJECT_CONCURRENCY, 4 * cpus)
}

/// Default TTL (seconds) after which an untouched per-project semaphore is
/// evicted from `project_semaphores`. The map grows monotonically as
/// projects are touched; on a long-running fleet host serving 100k+
/// projects this is the dominant per-project residency cost. Each entry is
/// ~140 bytes (Arc + Semaphore + HashMap slot + last_touched), so 100k
/// entries is ~14 MB — bounded but worth bounding. Default 30 minutes
/// matches the cloud pgwire-pool eviction TTL (which itself sits at 10 min
/// in front of this layer). Override with
/// `BASIN_STORAGE_PROJECT_STATE_TTL_SECS`; `0` or `off` disables eviction
/// entirely.
pub const DEFAULT_PROJECT_STATE_TTL_SECS: u64 = 1800;

/// How many candidate entries the inline-on-insert sweep is allowed to
/// scan per call. The HashMap's iterator order is randomised, so over many
/// inserts every entry is eventually inspected; bounding per-call work
/// keeps the insert latency bounded (a few hundred map probes) even when
/// the map is large. 256 is a heuristic — large enough that 100k stale
/// entries clear within a few hundred inserts, small enough that the
/// per-insert tail latency stays in the µs range.
const PROJECT_STATE_SWEEP_BUDGET: usize = 256;

/// Resolve the per-project-state TTL in seconds. Returns `None` when the
/// env var is `0` / `off` / `disabled` (eviction off entirely), `Some(n)`
/// otherwise. Falls back to [`DEFAULT_PROJECT_STATE_TTL_SECS`] when unset.
pub fn resolve_project_state_ttl_secs() -> Option<u64> {
    match std::env::var("BASIN_STORAGE_PROJECT_STATE_TTL_SECS") {
        Ok(v) => {
            let trimmed = v.trim();
            if matches!(trimmed.to_ascii_lowercase().as_str(), "0" | "off" | "disabled") {
                None
            } else if let Ok(n) = trimmed.parse::<u64>() {
                Some(n)
            } else {
                Some(DEFAULT_PROJECT_STATE_TTL_SECS)
            }
        }
        Err(_) => Some(DEFAULT_PROJECT_STATE_TTL_SECS),
    }
}

/// Resolve the scheduler's global RPC budget. `BASIN_STORAGE_GLOBAL_BUDGET`,
/// when present and parseable to a nonzero `usize`, overrides the default
/// computed by [`scheduler::default_global_budget`].
pub fn resolve_global_budget() -> usize {
    if let Ok(v) = std::env::var("BASIN_STORAGE_GLOBAL_BUDGET") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    scheduler::default_global_budget()
}

/// Read knobs for [`Storage::read`]. Filters are ANDed together.
///
/// `limit` (when `Some`) is a hint to stop emitting batches once `n` rows
/// have passed the predicate filter (post-filter count, matching PG
/// btree-scan LIMIT semantics). `None` (the default) preserves the legacy
/// behaviour of fully materialising the result.
///
/// `row_group_selection` (when `Some`) is a per-file allowlist of surviving
/// row-group indices, populated by upstream index probes (e.g. the JSONB
/// row-group GIN prune in
/// `basin-engine::index_probe::rowgroup_prune_for_containment`). The reader
/// looks up each scanned file in the map: a file present in the map reads
/// ONLY the listed row-groups; a file absent from the map reads every
/// row-group (so the un-summarised / never-indexed path is preserved
/// byte-identically). The predicate is still re-evaluated on every emitted
/// row — the selection is a SUPERSET filter on which row-groups to open,
/// not a substitute for the row-level filter.
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub projection: Option<Vec<String>>,
    pub filters: Vec<Predicate>,
    pub partition: Option<basin_common::PartitionKey>,
    /// Optional cap on the number of rows the reader emits (post-filter).
    /// `None` = unlimited.
    pub limit: Option<usize>,
    /// Optional per-file row-group allowlist. Files absent from the map are
    /// scanned in full; files present are restricted to the listed groups.
    /// `None` = scan every row-group of every file.
    pub row_group_selection: Option<HashMap<String, Vec<u32>>>,
    /// Optional hint naming the single column every cold file is physically
    /// ASC-sorted on (the table's effective cluster / single-PK column). The
    /// engine sets this from `effective_cluster_col(meta)` when it pushes a
    /// `Predicate::InInt64` on that same column. It enables the storage
    /// sorted-key skip: an `InInt64` filter on the sorted column is served by
    /// binary-searching each decode chunk and `take`-ing only the matching
    /// rows, instead of an O(n) Arrow filter over every (wide) column.
    /// `None` = no sort guarantee; the plain vectorized filter path is used.
    pub sorted_by: Option<String>,
}

/// Project-aware Parquet store. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

/// Per-project semaphore entry tracked by `Inner::project_semaphores`. The
/// `last_touched` field is an UNIX-seconds timestamp bumped on every
/// accessor call; the inline-on-insert sweep evicts entries whose
/// `last_touched` is older than the configured TTL. See #119.
struct ProjectSemaphoreEntry {
    sem: Arc<Semaphore>,
    last_touched: AtomicU64,
}

/// Current UNIX-seconds clock, monotonised against the test clock when
/// present. Tests can install a fake clock via [`set_project_state_now_secs`]
/// to advance time deterministically without sleeping for minutes.
#[cfg(test)]
static TEST_PROJECT_STATE_NOW_SECS: AtomicU64 = AtomicU64::new(0);

#[inline]
fn project_state_now_secs() -> u64 {
    #[cfg(test)]
    {
        let v = TEST_PROJECT_STATE_NOW_SECS.load(Ordering::Relaxed);
        if v != 0 {
            return v;
        }
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Test-only: pin the clock used by [`project_state_now_secs`]. `0` releases
/// the pin (falls back to `SystemTime::now`).
#[cfg(test)]
fn set_project_state_now_secs(v: u64) {
    TEST_PROJECT_STATE_NOW_SECS.store(v, Ordering::Relaxed);
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
    /// Per-project concurrent-RPC semaphores, lazy-allocated on first
    /// use. The mutex protects the map only — permits are acquired on
    /// the [`Semaphore`] itself outside the mutex, so the lock is held
    /// for nanoseconds and never across an `await`. We accept the cost
    /// of one map lookup per RPC; on the hot path it's a HashMap probe
    /// with `ProjectId` (a [`Ulid`] under the hood, so cheap to hash).
    ///
    /// #119 — each entry carries a `last_touched` UNIX-seconds timestamp
    /// that is bumped on every accessor call. On insert we run a bounded
    /// inline sweep (`PROJECT_STATE_SWEEP_BUDGET` candidates) and drop any
    /// entry whose `last_touched` is older than
    /// `project_state_ttl_secs`. This bounds the map's residency on
    /// long-running multi-tenant hosts where projects are touched once
    /// then go quiet.
    project_semaphores: Mutex<HashMap<ProjectId, ProjectSemaphoreEntry>>,
    /// TTL (seconds) after which an untouched per-project semaphore is
    /// evicted by the inline sweep. `None` disables eviction (legacy
    /// behaviour). Set at construction by [`resolve_project_state_ttl_secs`].
    project_state_ttl_secs: Option<u64>,
    /// Default permit count for a newly-created project semaphore.
    default_project_concurrency: usize,
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
    /// Cache of parsed Vortex file footers. Always enabled; capacity is
    /// [`DEFAULT_VORTEX_FOOTER_CACHE_CAP`]. See [`VortexFooterCache`] for
    /// the key-safety and cloneability rationale.
    vortex_footer_cache: Arc<VortexFooterCache>,
    /// Cache of per-file `(row_count, column_stats)` for the stats-pruning
    /// hot path (`reader::list_data_files_with_stats`). Keyed on
    /// `(path, size)`; same immutability invariant as the footer caches. A
    /// warm entry turns a per-query footer GET+parse into an in-RAM lookup,
    /// for both Parquet and Vortex files. See [`metadata_cache::DataFileStatsCache`].
    data_file_stats_cache: Arc<metadata_cache::DataFileStatsCache>,
    /// Optional per-project counters registry (Phase 6 telemetry). Attached by
    /// the engine via [`Storage::attach_project_counters`]; first attach wins.
    /// When present, every successful write/read bumps `bytes_written_total` /
    /// `bytes_read_total` for the calling project.
    project_counters: OnceLock<Arc<ProjectCounterRegistry>>,
    /// Optional BYO-key envelope-encryption provider (Phase 6). Attached by
    /// the cloud layer via [`Storage::attach_encryption_provider`]; first
    /// attach wins. When `None` the write/read path is byte-for-byte the
    /// plaintext path. When `Some`, every PUT envelope-encrypts with a
    /// fresh per-file data key and every GET unwraps before decoding.
    encryption: OnceLock<Arc<dyn encryption::EncryptionProvider>>,
    /// Optional `Catalog` handle. When attached, the encryption call path
    /// looks up the per-project [`ProjectStorageConfig`] (cached below) on
    /// each wrap / unwrap so the provider can route to the per-project
    /// CMK. When `None`, the encryption path falls back to the legacy
    /// `wrap_key` / `unwrap_key` shape — back-compat for callers that
    /// haven't yet wired a catalog handle into Storage.
    catalog: OnceLock<Arc<dyn Catalog>>,
    /// Process-local cache of [`ProjectStorageConfig`] keyed by project.
    /// Populated lazily on first lookup; invalidated by
    /// [`Storage::set_project_storage_config`]. The outer `RwLock` lets
    /// concurrent readers fan in cheaply; writes (cache fill or
    /// invalidation) are rare. Per-project cost discipline: one shared
    /// HashMap, no per-project heavy resource. Each entry stores
    /// `Option<ProjectStorageConfig>` so a known-empty result (project has
    /// no config) is also cached and avoids re-querying the catalog on
    /// the hot write path.
    project_config_cache: RwLock<HashMap<ProjectId, Option<ProjectStorageConfig>>>,
    /// Cross-project EDF (Earliest Deadline First) fair-share scheduler.
    /// One per `Storage`; cheap-to-clone `Arc` inside. Each underlying
    /// `ObjectStore` RPC issued through `project_object_store` acquires
    /// a permit from this scheduler AFTER the per-project Semaphore
    /// liveness floor. Point reads (HEAD / GET / small range / LIST)
    /// arrive with a 5ms deadline; bulk ops (PUT / large range) get a
    /// 1s deadline so they can't crowd out point lookups. See ADR 0008.
    scheduler: Scheduler,
    /// T-049 — per-project BYO object-store overrides. Registered by the
    /// cloud layer (or tests) after `Storage::new` via
    /// [`Storage::register_byo_object_store`]. The `Mutex` is held only
    /// for nanoseconds (map insert / remove / probe); it is never held
    /// across an `await`. When a project has an entry here,
    /// [`Storage::project_object_store`] routes its I/O to the
    /// registered store instead of `Inner::object_store`.
    byo_object_stores: Mutex<HashMap<ProjectId, Arc<dyn ObjectStore>>>,
}

/// Best-effort counters for the read path. See [`Inner::read_counters`].
#[derive(Debug, Default)]
pub struct ReadCounters {
    pub row_groups_considered: AtomicU64,
    pub row_groups_scanned: AtomicU64,
    pub row_groups_pruned_by_stats: AtomicU64,
    pub row_groups_pruned_by_bloom: AtomicU64,
    /// Number of rows selected (not skipped) by the page-index RowSelection
    /// within the surviving row groups. Zero when no page-index pruning fired.
    pub rows_selected_by_page_index: AtomicU64,
    /// Number of cold-file reads that actually fetched + decoded the file's
    /// bytes (a `read_one` invocation that passed every cache short-circuit and
    /// issued the object-store GET). This is the per-query "files touched"
    /// signal: a point lookup that file-prunes (whole-file column_stats
    /// min/max) down to one surviving file bumps this by 1, regardless of how
    /// many files the table holds. The scale-invariant gates assert this stays
    /// O(1) as both table size and file count grow.
    pub files_opened: AtomicU64,
    /// Number of reads answered entirely from an in-RAM cache (the top
    /// page-cache hit, the Vortex pre-GET unfiltered short-circuit, or the
    /// Vortex unfiltered-decode reuse hit) WITHOUT issuing the object-store
    /// GET. Counted separately from `files_opened` so a repeated point lookup
    /// (cache hit) is provably 0 cold reads, while a fresh-key lookup that
    /// fills the cache is provably ≥ 1. `files_opened + files_served_from_cache`
    /// is the total number of files the read path resolved.
    pub files_served_from_cache: AtomicU64,
    /// Rows materialized into Arrow from cold files — the unfiltered decode
    /// volume, NOT the post-filter row count. Vortex: rows in the batches
    /// `decode_with_cache` (or the cached unfiltered reuse) hands back, summed
    /// before the Arrow project/filter pass. Parquet: rows yielded by the
    /// parquet record-batch stream (which has the row-group + page + row
    /// filters pushed in, so this is the post-pushdown / pre-engine-filter
    /// volume). This is the work that scales with chunk/row-group size, not
    /// table size: a point read decodes at most one chunk (≤ 65536 rows) of the
    /// single surviving file. The scale-invariant gates assert it stays bounded
    /// by a chunk regardless of table size.
    pub rows_decoded: AtomicU64,
    /// Object-store GET bytes pulled by cold reads (best-effort: bumped at the
    /// same choke point as the per-project `record_bytes_read`, by the fetched
    /// blob length on both the Vortex and Parquet paths). Zero on cache-served
    /// reads. Observability only — the gates assert on `files_opened` /
    /// `rows_decoded`, not on bytes (bytes are format/compression dependent),
    /// but the summary prints record it so a regression that re-fetches whole
    /// files is visible.
    pub bytes_fetched: AtomicU64,
}

impl ReadCounters {
    pub fn snapshot(&self) -> ReadCountersSnapshot {
        ReadCountersSnapshot {
            row_groups_considered: self.row_groups_considered.load(Ordering::Relaxed),
            row_groups_scanned: self.row_groups_scanned.load(Ordering::Relaxed),
            row_groups_pruned_by_stats: self.row_groups_pruned_by_stats.load(Ordering::Relaxed),
            row_groups_pruned_by_bloom: self.row_groups_pruned_by_bloom.load(Ordering::Relaxed),
            rows_selected_by_page_index: self.rows_selected_by_page_index.load(Ordering::Relaxed),
            files_opened: self.files_opened.load(Ordering::Relaxed),
            files_served_from_cache: self.files_served_from_cache.load(Ordering::Relaxed),
            rows_decoded: self.rows_decoded.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.row_groups_considered.store(0, Ordering::Relaxed);
        self.row_groups_scanned.store(0, Ordering::Relaxed);
        self.row_groups_pruned_by_stats.store(0, Ordering::Relaxed);
        self.row_groups_pruned_by_bloom.store(0, Ordering::Relaxed);
        self.rows_selected_by_page_index.store(0, Ordering::Relaxed);
        self.files_opened.store(0, Ordering::Relaxed);
        self.files_served_from_cache.store(0, Ordering::Relaxed);
        self.rows_decoded.store(0, Ordering::Relaxed);
        self.bytes_fetched.store(0, Ordering::Relaxed);
    }
}

/// Plain-data view of [`ReadCounters`]; cheap to copy for assertions.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadCountersSnapshot {
    pub row_groups_considered: u64,
    pub row_groups_scanned: u64,
    pub row_groups_pruned_by_stats: u64,
    pub row_groups_pruned_by_bloom: u64,
    /// Rows selected (kept) by the page-index RowSelection. Zero when no
    /// page-level pruning fired (all pages kept or no page index present).
    pub rows_selected_by_page_index: u64,
    /// See [`ReadCounters::files_opened`].
    pub files_opened: u64,
    /// See [`ReadCounters::files_served_from_cache`].
    pub files_served_from_cache: u64,
    /// See [`ReadCounters::rows_decoded`].
    pub rows_decoded: u64,
    /// See [`ReadCounters::bytes_fetched`].
    pub bytes_fetched: u64,
}

impl ReadCountersSnapshot {
    /// Per-query delta: `self` (snapshot taken AFTER a query) minus `earlier`
    /// (snapshot taken BEFORE it). The counters are process-global atomics, so
    /// this is only a per-query measurement when the query ran serially between
    /// the two snapshots — which is the discipline the scale-invariant tests
    /// follow (`--test-threads=1`, one query per before/after pair). Saturating
    /// subtraction so a (shouldn't-happen) concurrent bump never underflows.
    pub fn delta(&self, earlier: &ReadCountersSnapshot) -> ReadCountersSnapshot {
        ReadCountersSnapshot {
            row_groups_considered: self
                .row_groups_considered
                .saturating_sub(earlier.row_groups_considered),
            row_groups_scanned: self
                .row_groups_scanned
                .saturating_sub(earlier.row_groups_scanned),
            row_groups_pruned_by_stats: self
                .row_groups_pruned_by_stats
                .saturating_sub(earlier.row_groups_pruned_by_stats),
            row_groups_pruned_by_bloom: self
                .row_groups_pruned_by_bloom
                .saturating_sub(earlier.row_groups_pruned_by_bloom),
            rows_selected_by_page_index: self
                .rows_selected_by_page_index
                .saturating_sub(earlier.rows_selected_by_page_index),
            files_opened: self.files_opened.saturating_sub(earlier.files_opened),
            files_served_from_cache: self
                .files_served_from_cache
                .saturating_sub(earlier.files_served_from_cache),
            rows_decoded: self.rows_decoded.saturating_sub(earlier.rows_decoded),
            bytes_fetched: self.bytes_fetched.saturating_sub(earlier.bytes_fetched),
        }
    }
}

/// Default capacity for the Parquet footer cache. Bumped from 1024 to 16384
/// after the scalability audit flagged cache thrash on multi-tenant fleets:
/// at 10k projects × ~5 tables × ~10 files = 500k live footers, the prior
/// cap re-fetched the cold tail on every promote. 16384 gives ~16× headroom
/// for ~10k tenants with light table density; in the pessimistic footer-size
/// case (a few KB each) the cache costs O(tens of MB) of resident RAM —
/// negligible against the bytes-per-tenant budget. Override at process start
/// with `BASIN_STORAGE_PARQUET_META_CACHE_CAP` (positive `usize`); any
/// unset / unparseable / zero value keeps the default.
const DEFAULT_PARQUET_META_CACHE_CAP: usize = 16_384;

/// Resolve the Parquet footer cache capacity, honoring
/// `BASIN_STORAGE_PARQUET_META_CACHE_CAP` when present and parseable to a
/// positive `usize`. Falls back to [`DEFAULT_PARQUET_META_CACHE_CAP`].
pub fn resolve_parquet_meta_cache_cap() -> usize {
    if let Ok(v) = std::env::var("BASIN_STORAGE_PARQUET_META_CACHE_CAP") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    DEFAULT_PARQUET_META_CACHE_CAP
}

/// Default capacity for the HNSW segment cache. 256 segments at "few MB
/// each" is the largest we'd want to hold in process; in practice the cache
/// stays much smaller because workloads concentrate on a handful of
/// segments per (project, table, column).
const DEFAULT_HNSW_SEGMENT_CACHE_CAP: usize = 256;

/// Default capacity for the Vortex footer cache: 512 entries. Each entry
/// holds a cloned `vortex_file::Footer` (Arc-wrapped internals, O(1) clone)
/// so the per-entry cost is dominated by the Arc ref counts, not by the
/// footer payload size. 512 covers the working set of all current benchmarks
/// and the Phase 5.7 integration suite without material RAM overhead.
const DEFAULT_VORTEX_FOOTER_CACHE_CAP: usize = 512;

/// Default capacity for the per-file data-file stats cache. Sized to match
/// [`DEFAULT_PARQUET_META_CACHE_CAP`] so a table whose footers all fit in the
/// parquet-meta cache also fits in the stats cache (no asymmetric eviction
/// that would force one format to re-parse while the other stays warm). Each
/// entry holds an `Arc<(u64, BTreeMap<..>)>` — a handful of columns of
/// min/max bytes — so 16k entries is a small RAM footprint.
const DEFAULT_DATA_FILE_STATS_CACHE_CAP: usize = 16_384;

impl Storage {
    pub fn new(cfg: StorageConfig) -> Self {
        // If a disk cache is configured, wrap the supplied object store
        // with [`DiskCachedStore`] *before* the per-project wrapping that
        // [`Storage::project_object_store`] adds on top. The result is
        // `ProjectScopedStore -> DiskCachedStore -> real ObjectStore`
        // for every read, which is what the design calls for: cache
        // hits still count against the project's permit pool, and cache
        // keys are content-addressed on the path (which always carries
        // the project prefix) so cross-project key collisions are
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

        let page_cache = cfg.page_cache.map(|pc| Arc::new(PageCache::new(pc)));

        Self {
            inner: Arc::new(Inner {
                object_store,
                root_prefix: cfg.root_prefix,
                parquet_meta_cache: Arc::new(metadata_cache::ParquetMetaCache::new(
                    resolve_parquet_meta_cache_cap(),
                )),
                hnsw_segment_cache: Arc::new(metadata_cache::HnswSegmentCache::new(
                    DEFAULT_HNSW_SEGMENT_CACHE_CAP,
                )),
                project_semaphores: Mutex::new(HashMap::new()),
                project_state_ttl_secs: resolve_project_state_ttl_secs(),
                default_project_concurrency: resolve_project_concurrency(),
                read_counters: Arc::new(ReadCounters::default()),
                page_cache,
                // Vortex footer cache: always enabled with the default capacity.
                // Mirrors parquet_meta_cache — always-on; zero Cargo.toml churn
                // (uses the `lru` dep already present). Non-breaking: no new
                // `StorageConfig` field means every existing call site compiles
                // unchanged.
                vortex_footer_cache: Arc::new(VortexFooterCache::new(
                    DEFAULT_VORTEX_FOOTER_CACHE_CAP,
                )),
                data_file_stats_cache: Arc::new(metadata_cache::DataFileStatsCache::new(
                    DEFAULT_DATA_FILE_STATS_CACHE_CAP,
                )),
                project_counters: OnceLock::new(),
                encryption: OnceLock::new(),
                catalog: OnceLock::new(),
                project_config_cache: RwLock::new(HashMap::new()),
                scheduler: Scheduler::new(resolve_global_budget()),
                byo_object_stores: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Attach a per-project counter registry. Idempotent (first attach wins).
    pub fn attach_project_counters(&self, registry: Arc<ProjectCounterRegistry>) {
        let _ = self.inner.project_counters.set(registry);
    }

    /// Attach a BYO-key envelope-encryption provider. Idempotent (first
    /// attach wins). Once attached, subsequent writes envelope-encrypt the
    /// Parquet body with a fresh per-file data key and persist the wrapped
    /// key as a `<path>.wrapped` sidecar; reads transparently unwrap.
    /// Files written before the attach remain readable as plaintext (no
    /// sidecar present means no decryption attempt).
    pub fn attach_encryption_provider(&self, provider: Arc<dyn encryption::EncryptionProvider>) {
        let _ = self.inner.encryption.set(provider);
    }

    /// Crate-private accessor: clone of the attached provider (if any).
    /// Used by the writer / reader modules to gate the envelope path.
    pub(crate) fn encryption_provider(&self) -> Option<Arc<dyn encryption::EncryptionProvider>> {
        self.inner.encryption.get().cloned()
    }

    /// Attach a [`Catalog`] handle. Idempotent (first attach wins). When
    /// attached, [`Storage::set_project_storage_config`] /
    /// [`Storage::get_project_storage_config`] become available and the
    /// encryption call path looks up each project's config to route the
    /// `EncryptionProvider`. Without a catalog the storage layer falls
    /// back to the legacy `wrap_key` / `unwrap_key` path — fully
    /// backwards compatible.
    pub fn attach_catalog(&self, catalog: Arc<dyn Catalog>) {
        let _ = self.inner.catalog.set(catalog);
    }

    // -----------------------------------------------------------------------
    // T-049 — per-project BYO object-store registration
    // -----------------------------------------------------------------------

    /// Register a pre-built [`ObjectStore`] as the BYO override for
    /// `project`. Subsequent calls to [`Storage::project_object_store`] for
    /// this project will route to `store` instead of the shared
    /// `Inner::object_store`. Replaces any previously registered store for
    /// the same project.
    ///
    /// The `store` should already be correctly scoped to the customer's
    /// bucket root; the storage layer will still apply the per-project
    /// `projects/{project_id}/` prefix on top of it (via
    /// `project_object_store`), so the customer's bucket must be writable at
    /// that prefix.
    pub fn register_byo_object_store(&self, project: ProjectId, store: Arc<dyn ObjectStore>) {
        self.inner
            .byo_object_stores
            .lock()
            .expect("byo_object_stores poisoned")
            .insert(project, store);
    }

    /// Remove a previously-registered BYO override for `project`. After
    /// this call, [`Storage::project_object_store`] falls back to the shared
    /// store. No-op when no override is registered.
    pub fn deregister_byo_object_store(&self, project: &ProjectId) {
        self.inner
            .byo_object_stores
            .lock()
            .expect("byo_object_stores poisoned")
            .remove(project);
    }

    /// Build a ready-to-use [`ObjectStore`] from a catalog-persisted
    /// [`basin_catalog::S3Config`] and the *already-decrypted* secret access
    /// key bytes (the cloud layer decrypts `S3Config::secret_access_key_enc`
    /// before calling here; the engine never sees the ciphertext).
    ///
    /// The returned store is suitable as the `store` argument to
    /// [`Storage::register_byo_object_store`].
    ///
    /// # Errors
    /// Returns an error when the `AmazonS3Builder` rejects the supplied
    /// config (e.g. invalid endpoint, empty bucket name, etc.).
    pub fn register_byo_object_store_from_config_with_secret(
        &self,
        project: ProjectId,
        cfg: &basin_catalog::S3Config,
        secret_plain: &str,
    ) -> Result<Arc<dyn ObjectStore>> {
        let store = Self::build_byo_object_store_from_config_with_secret(cfg, secret_plain)?;
        self.register_byo_object_store(project, store.clone());
        Ok(store)
    }

    /// Pure constructor: build an [`ObjectStore`] from a [`basin_catalog::S3Config`]
    /// and a *plaintext* secret key without mutating `Storage`. Useful when
    /// the caller wants to inspect the store before registering it, or when
    /// running tests that need multiple stores.
    ///
    /// The `secret_plain` value is the plaintext secret access key; the
    /// cloud layer is responsible for decrypting
    /// `S3Config::secret_access_key_enc` before calling here.
    pub fn build_byo_object_store_from_config_with_secret(
        cfg: &basin_catalog::S3Config,
        secret_plain: &str,
    ) -> Result<Arc<dyn ObjectStore>> {
        use object_store::aws::AmazonS3Builder;

        let b = AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_access_key_id(&cfg.access_key_id)
            .with_secret_access_key(secret_plain)
            .with_endpoint(&cfg.endpoint)
            // force_path_style = true → path-style (disable virtual-hosted);
            // force_path_style = false → virtual-hosted (the common default).
            .with_virtual_hosted_style_request(!cfg.force_path_style);

        let store = b.build().map_err(|e| {
            basin_common::BasinError::storage(format!(
                "build_byo_object_store_from_config_with_secret: {e}"
            ))
        })?;
        Ok(Arc::new(store))
    }

    // -----------------------------------------------------------------------
    // T-051 — BYO-bucket-aware project deletion
    // -----------------------------------------------------------------------

    /// Catalog-aware, BYO-bucket-safe project deletion.
    ///
    /// - When the project has a BYO bucket configured (`ProjectMetadata::byo_bucket
    ///   .is_some()`): logs a notice and **only** drops the catalog namespace.
    ///   The customer's bucket objects are left intact — Basin must never
    ///   delete data from a bucket it doesn't own.
    /// - Otherwise: delegates to the standard [`Storage::delete_project`]
    ///   which deletes all objects under the project prefix and drops the
    ///   catalog namespace.
    ///
    /// Returns the number of objects physically deleted (0 for BYO projects).
    #[tracing::instrument(skip(self, catalog), fields(project=%project))]
    pub async fn delete_project_byo_aware(
        &self,
        project: ProjectId,
        catalog: &dyn basin_catalog::Catalog,
    ) -> basin_common::Result<usize> {
        let meta = catalog.get_project_metadata(&project).await?;
        if meta.byo_bucket.is_some() {
            tracing::info!(
                target: "basin_storage::delete_project",
                project = %project,
                "project {project} BYO-bucket — leaving customer bucket intact",
            );
            catalog.drop_namespace(&project).await?;
            return Ok(0);
        }
        self.delete_project(catalog, &project).await
    }

    /// Persist a per-project storage config. Delegates to the attached
    /// [`Catalog`]; returns an error if no catalog has been attached.
    /// Invalidates the in-process cache for `project` so the next wrap /
    /// unwrap call picks up the new config — production deployments
    /// rotating CMK config rely on this.
    pub async fn set_project_storage_config(
        &self,
        project: &ProjectId,
        config: ProjectStorageConfig,
    ) -> Result<()> {
        let catalog = self.inner.catalog.get().ok_or_else(|| {
            basin_common::BasinError::Internal(
                "set_project_storage_config: no catalog attached to Storage".into(),
            )
        })?;
        catalog.set_project_storage_config(project, config).await?;
        // Invalidate cache so the next read re-fetches the freshly
        // persisted config. Holding the write lock briefly is fine; the
        // path is rare (admin / setup, not hot write).
        self.inner
            .project_config_cache
            .write()
            .expect("project_config_cache poisoned")
            .remove(project);
        Ok(())
    }

    /// Look up a project's persisted storage config. Goes through the
    /// in-process cache; populates it on miss. Returns an error if no
    /// catalog has been attached.
    pub async fn get_project_storage_config(
        &self,
        project: &ProjectId,
    ) -> Result<Option<ProjectStorageConfig>> {
        if let Some(cached) = self
            .inner
            .project_config_cache
            .read()
            .expect("project_config_cache poisoned")
            .get(project)
            .cloned()
        {
            return Ok(cached);
        }
        let catalog = self.inner.catalog.get().ok_or_else(|| {
            basin_common::BasinError::Internal(
                "get_project_storage_config: no catalog attached to Storage".into(),
            )
        })?;
        let cfg = catalog.get_project_storage_config(project).await?;
        self.inner
            .project_config_cache
            .write()
            .expect("project_config_cache poisoned")
            .insert(*project, cfg.clone());
        Ok(cfg)
    }

    /// Crate-private cache-aware accessor used by the encryption call
    /// path. Returns `Ok(None)` (and skips the catalog round-trip) when
    /// no catalog is attached so the writer / reader can degrade
    /// gracefully to the legacy `wrap_key` / `unwrap_key` shape.
    pub(crate) async fn project_storage_config_cached(
        &self,
        project: &ProjectId,
    ) -> Result<Option<ProjectStorageConfig>> {
        if self.inner.catalog.get().is_none() {
            return Ok(None);
        }
        self.get_project_storage_config(project).await
    }

    /// Look up the Arrow [`SchemaRef`] for a table from the attached catalog.
    /// Returns `Ok(None)` when no catalog is attached (degrades gracefully so
    /// reads without a catalog behave exactly as before — schema evolution
    /// synthesis is skipped). Used by the reader to synthesise NULL-filled
    /// columns for projected fields that pre-date an `ALTER TABLE ADD COLUMN`.
    pub(crate) async fn catalog_table_schema(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Option<SchemaRef>> {
        let Some(catalog) = self.inner.catalog.get() else {
            return Ok(None);
        };
        match catalog.load_table(project, table).await {
            Ok(meta) => Ok(Some(meta.schema)),
            // Table not found is not fatal — treat it like "no catalog".
            Err(basin_common::BasinError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Load the secondary-index declarations for a table from the attached
    /// catalog. Returns an empty vec when no catalog is attached or the table
    /// is unknown. Used by the HNSW sidecar builder to pick up per-column
    /// `WITH (ef_construction = N)` build parameters declared via
    /// `CREATE INDEX … USING hnsw`.
    pub(crate) async fn catalog_table_indexes(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<basin_catalog::SecondaryIndex>> {
        let Some(catalog) = self.inner.catalog.get() else {
            return Ok(Vec::new());
        };
        match catalog.load_table(project, table).await {
            Ok(meta) => Ok(meta.indexes),
            Err(basin_common::BasinError::NotFound(_)) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn project_counters(
        &self,
        project: &ProjectId,
    ) -> Option<Arc<basin_common::ProjectCounters>> {
        self.inner
            .project_counters
            .get()
            .map(|r| r.for_project(project))
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

    /// Per-project live I/O stats sourced from the EDF scheduler. Returns
    /// zeros for projects that have never run an op through this `Storage`.
    pub fn project_stats(&self, project: &ProjectId) -> ProjectIoStats {
        self.inner.scheduler.project_stats(project)
    }

    /// Handle to the cross-project EDF scheduler. Cheap to clone. Exposed
    /// so the engine layer can inspect global state and so tests can
    /// drive the scheduler directly.
    pub fn scheduler(&self) -> &Scheduler {
        &self.inner.scheduler
    }

    /// Per-project semaphore handle. Used internally to gate every
    /// underlying object_store RPC behind a project-scoped permit pool.
    /// Lazy-allocates on first call for a given project.
    ///
    /// #119 — every call bumps the entry's `last_touched` UNIX-seconds
    /// timestamp. On insert (first touch for a project) we run a bounded
    /// inline sweep of stale entries — see [`Self::sweep_stale_project_state`].
    fn project_semaphore(&self, project: &ProjectId) -> Arc<Semaphore> {
        let now = project_state_now_secs();
        let mut map = self
            .inner
            .project_semaphores
            .lock()
            .expect("project semaphore map poisoned");
        if let Some(entry) = map.get(project) {
            entry.last_touched.store(now, Ordering::Relaxed);
            return entry.sem.clone();
        }
        // First touch for this project: opportunistically evict stale entries
        // before inserting the new one. Bounded by PROJECT_STATE_SWEEP_BUDGET
        // so the worst-case insert latency stays predictable.
        if let Some(ttl) = self.inner.project_state_ttl_secs {
            Self::sweep_stale_project_state(&mut map, now, ttl, PROJECT_STATE_SWEEP_BUDGET);
        }
        let sem = Arc::new(Semaphore::new(self.inner.default_project_concurrency));
        map.insert(
            *project,
            ProjectSemaphoreEntry {
                sem: sem.clone(),
                last_touched: AtomicU64::new(now),
            },
        );
        sem
    }

    /// Inline sweep: scan up to `budget` entries and drop any whose
    /// `last_touched` is older than `now - ttl_secs`. Called from
    /// `project_semaphore` on insert (cold-path). The HashMap iterator
    /// order is randomised by the default `RandomState` hasher, so over
    /// many inserts every entry is eventually examined; bounding per-call
    /// work keeps the insert tail latency in the µs range even at 1M
    /// entries.
    ///
    /// Note: evicting an entry only drops the map's `Arc<Semaphore>`
    /// handle — any concurrent caller holding a clone (e.g. a permit
    /// guard) keeps the Semaphore alive until they release. After
    /// eviction, the next call to `project_semaphore` for the same
    /// project allocates a fresh Semaphore (new permit pool), which is
    /// the desired "cold project woke up" semantics.
    fn sweep_stale_project_state(
        map: &mut HashMap<ProjectId, ProjectSemaphoreEntry>,
        now: u64,
        ttl_secs: u64,
        budget: usize,
    ) {
        if map.is_empty() || budget == 0 {
            return;
        }
        let cutoff = now.saturating_sub(ttl_secs);
        let mut to_drop: Vec<ProjectId> = Vec::new();
        let mut scanned = 0usize;
        for (id, entry) in map.iter() {
            if scanned >= budget {
                break;
            }
            scanned += 1;
            if entry.last_touched.load(Ordering::Relaxed) <= cutoff {
                to_drop.push(*id);
            }
        }
        for id in to_drop {
            map.remove(&id);
        }
    }

    /// Process-internal accessor: number of live per-project state entries.
    /// Exposed for tests (#119) and operator observability.
    #[doc(hidden)]
    pub fn project_state_len(&self) -> usize {
        self.inner
            .project_semaphores
            .lock()
            .expect("project semaphore map poisoned")
            .len()
    }

    /// Project-scoped view of the underlying object store. Every RPC
    /// (get / put / list / head / delete / copy) is gated on this
    /// project's semaphore so one project's heavy traffic cannot starve
    /// another project's quiet traffic.
    ///
    /// This is the right thing to register with DataFusion's runtime
    /// (`SessionContext::register_object_store`) for a session bound to
    /// `project`: every range read DataFusion drives for that session
    /// will then count against the project's permit pool.
    ///
    /// T-049: if a BYO object store has been registered for `project` via
    /// [`Storage::register_byo_object_store`], that store is used as the
    /// backing store instead of the shared `Inner::object_store`. Prefix
    /// isolation (`projects/{project_id}/…`) is preserved either way.
    pub fn project_object_store(&self, project: &ProjectId) -> Arc<dyn ObjectStore> {
        // T-049: check for a per-project BYO store first.
        let backing = {
            let byo = self
                .inner
                .byo_object_stores
                .lock()
                .expect("byo_object_stores poisoned");
            byo.get(project).cloned()
        };
        let backing = backing.unwrap_or_else(|| self.inner.object_store.clone());

        let sem = self.project_semaphore(project);
        // Pull the per-project counters handle (None if no registry has
        // been attached). The wrapper's bump sites are a no-op when None,
        // so the legacy un-instrumented test paths stay byte-identical.
        let counters = self.project_counters(project);
        Arc::new(concurrency::ProjectScopedStore::new(
            backing,
            sem,
            self.inner.scheduler.clone(),
            *project,
            counters,
        ))
    }

    /// Internal accessor: returns the wrapped project-scoped store as a
    /// concrete `Arc<dyn ObjectStore>`, identical to
    /// [`project_object_store`] but at `pub(crate)` visibility for the
    /// reader / writer / vector-index modules.
    pub(crate) fn project_store(&self, project: &ProjectId) -> Arc<dyn ObjectStore> {
        self.project_object_store(project)
    }

    pub(crate) fn parquet_meta_cache(&self) -> &Arc<metadata_cache::ParquetMetaCache> {
        &self.inner.parquet_meta_cache
    }

    pub(crate) fn data_file_stats_cache(&self) -> &Arc<metadata_cache::DataFileStatsCache> {
        &self.inner.data_file_stats_cache
    }

    pub(crate) fn page_cache_handle(&self) -> Option<&Arc<PageCache>> {
        self.inner.page_cache.as_ref()
    }

    /// Handle to the Vortex footer cache. Exposed to the reader module so
    /// `read_one` can pass it through to `decode_with_cache`.
    pub(crate) fn vortex_footer_cache_handle(&self) -> &Arc<VortexFooterCache> {
        &self.inner.vortex_footer_cache
    }

    pub(crate) fn hnsw_segment_cache(&self) -> &Arc<metadata_cache::HnswSegmentCache> {
        &self.inner.hnsw_segment_cache
    }

    /// The underlying [`ObjectStore`]. Exposed so higher layers (e.g.
    /// `basin-engine`) can register the same store with DataFusion's runtime
    /// without round-tripping through configuration. Project-prefix
    /// enforcement still happens inside `Storage`'s own read/write methods;
    /// callers handed a raw `ObjectStore` are responsible for not crossing
    /// project boundaries themselves.
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
    #[tracing::instrument(skip(self, batch), fields(project=%project, table=%table, partition=%partition, rows=batch.num_rows()))]
    pub async fn write_batch(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition: &basin_common::PartitionKey,
        batch: &RecordBatch,
    ) -> Result<DataFile> {
        writer::write_batch(self, project, table, partition, batch).await
    }

    /// Like [`write_batch`](Self::write_batch) but with explicit per-write
    /// knobs (bloom-filter columns, row-group size). Used by callers that
    /// have read [`basin_catalog::TableMetadata::bloom_filter_columns`] and
    /// want the writer to materialise bloom filters in the Parquet footer.
    /// All defaults preserve the legacy [`write_batch`] behaviour exactly.
    #[tracing::instrument(skip(self, batch, opts), fields(project=%project, table=%table, partition=%partition, rows=batch.num_rows(), bloom_cols=opts.bloom_filter_columns.len()))]
    pub async fn write_batch_with_options(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition: &basin_common::PartitionKey,
        batch: &RecordBatch,
        opts: &WriteOptions,
    ) -> Result<DataFile> {
        writer::write_batch_with_options(self, project, table, partition, batch, opts).await
    }

    /// Stream all rows for one project+table that match the read options.
    #[tracing::instrument(skip(self, opts), fields(project=%project, table=%table))]
    pub async fn read(
        &self,
        project: &ProjectId,
        table: &TableName,
        opts: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read(self, project, table, opts).await
    }

    /// List the data files for one project+table without reading their bodies.
    #[tracing::instrument(skip(self), fields(project=%project, table=%table))]
    pub async fn list_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<DataFile>> {
        reader::list_data_files(self, project, table).await
    }

    /// Streaming variant of [`list_data_files`](Self::list_data_files):
    /// yields each [`DataFile`] as the underlying object-store LIST
    /// produces it, instead of materialising the full set in memory.
    /// Callers that only need a prefix (LIMIT, "stop on first match", …)
    /// should prefer this so the cold tail of the listing is never
    /// fetched — the scalability fix at >100k files/table.
    ///
    /// The returned stream borrows `self` for the lifetime of the call.
    /// Consumers that need a `'static` stream (e.g. to spawn on a
    /// background task) should clone the [`Storage`] handle first.
    #[tracing::instrument(skip(self), fields(project=%project, table=%table))]
    pub fn list_data_files_stream<'a>(
        &'a self,
        project: &'a ProjectId,
        table: &'a TableName,
    ) -> BoxStream<'a, Result<DataFile>> {
        reader::list_data_files_stream(self, project, table)
    }

    /// Like [`list_data_files`](Self::list_data_files) but populates each
    /// returned [`DataFile::row_count`] and [`DataFile::column_stats`] by
    /// fetching the Parquet footer (cached). Used by the copy-on-write
    /// UPDATE/DELETE pruner; reading-path callers prefer the cheaper
    /// `list_data_files`.
    #[tracing::instrument(skip(self), fields(project=%project, table=%table))]
    pub async fn list_data_files_with_stats(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<DataFile>> {
        reader::list_data_files_with_stats(self, project, table).await
    }

    /// Like [`read`](Self::read) but reads only the supplied `paths`
    /// instead of LIST'ing the table prefix. Used when the caller has
    /// already pruned the file set — typically via Phase 5.7 A4 catalog
    /// stats:
    ///
    /// 1. `Catalog::load_table` → `Snapshot::data_files` (each carries
    ///    `DataFileRef::column_stats` since A4).
    /// 2. Run [`evaluate_compound_for_pruning`] against each file's
    ///    stats; drop the ones whose stats prove `NoMatch`.
    /// 3. Pass the surviving paths to this method.
    ///
    /// Saves the LIST RPC plus, for catalog-pruned files, the per-file
    /// Parquet footer fetch. Project-prefix enforcement still applies:
    /// every path must contain the project prefix or the call returns
    /// [`basin_common::BasinError::IsolationViolation`].
    #[tracing::instrument(skip(self, paths, opts), fields(project=%project, n_paths=paths.len()))]
    pub async fn read_paths(
        &self,
        project: &ProjectId,
        paths: Vec<ObjectPath>,
        opts: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read_paths(self, project, paths, opts).await
    }

    /// Like [`read_paths`] but accepts a `catalog_schema` for callers that
    /// already hold the table schema. The Vortex filter-pushdown skip
    /// optimisation (avoiding the Arrow post-filter pass when all predicates
    /// were type-safe-pushed into the Vortex scan) requires the schema to
    /// verify column types. Passing `None` is identical to [`read_paths`].
    #[tracing::instrument(skip(self, paths, opts, catalog_schema), fields(project=%project, n_paths=paths.len()))]
    pub async fn read_paths_with_schema(
        &self,
        project: &ProjectId,
        paths: Vec<ObjectPath>,
        opts: ReadOptions,
        catalog_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read_paths_with_schema(self, project, paths, opts, catalog_schema).await
    }

    /// Read every batch from a single Parquet data file. Used by the
    /// UPDATE/DELETE pruner when only some files need processing — the
    /// table-wide [`read`](Self::read) would force us to merge all files
    /// regardless of pruning.
    ///
    /// `path` must be a path returned by a prior [`list_data_files`]
    /// against the same project; passing a foreign path is the caller's
    /// bug (we only enforce project isolation at the listing/path-key
    /// boundary, not on a raw `read_file`). The `project` argument is
    /// purely so the read counts against this project's per-project
    /// concurrency permit pool — it is not re-validated against `path`.
    #[tracing::instrument(skip(self), fields(project=%project, path=%path))]
    pub async fn read_file(
        &self,
        project: &ProjectId,
        path: &ObjectPath,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read_file(self, project, path).await
    }

    /// Like [`read_file`] but threads a [`ReadOptions`] through so callers
    /// can push down a column projection (and any other read option). The
    /// constraint enforcers (PK / UNIQUE / FK / EXCLUSION) use this with a
    /// `projection` set to only the constraint column(s), which skips JSONB
    /// / TEXT payload decode and yields the bulk-INSERT perf fix (Tier 1).
    #[tracing::instrument(skip(self, opts), fields(project=%project, path=%path))]
    pub async fn read_file_with_options(
        &self,
        project: &ProjectId,
        path: &ObjectPath,
        opts: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read_file_with_options(self, project, path, opts).await
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
    #[tracing::instrument(skip(self), fields(project=%project, from=%from))]
    pub async fn migrate_to_cold(
        &self,
        project: &ProjectId,
        from: &ObjectPath,
    ) -> Result<DataFile> {
        // Already cold? Re-stat and return without touching anything.
        if matches!(Tier::from_path(from.as_ref()), Tier::Cold) {
            let store = self.project_store(project);
            let head = store
                .head(from)
                .await
                .map_err(|e| basin_common::BasinError::storage(format!("head {from}: {e}")))?;
            return Ok(DataFile {
                path: from.clone(),
                size_bytes: head.size as u64,
                row_count: 0,
                column_stats: std::collections::BTreeMap::new(),
                bloom_filters: std::collections::BTreeMap::new(),
                hll_sketches: std::collections::BTreeMap::new(),
                tdigest_sketches: std::collections::BTreeMap::new(),
                tier: Tier::Cold,
            });
        }

        let to = paths::rewrite_to_cold(from).ok_or_else(|| {
            basin_common::BasinError::storage(format!(
                "migrate_to_cold: path does not match `tables/<t>/data/...`: {from}"
            ))
        })?;
        let store = self.project_store(project);
        // Belt-and-braces: confirm the target sits under this project's prefix.
        // The path was derived from `from`, which already cleared this check
        // at write time — we re-check defensively.
        let expected_prefix = format!("projects/{}/", project.as_prefix());
        if !to.as_ref().contains(&expected_prefix) {
            return Err(basin_common::BasinError::isolation(format!(
                "migrate_to_cold target {to} missing project prefix {expected_prefix}"
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
            bloom_filters: std::collections::BTreeMap::new(),
            hll_sketches: std::collections::BTreeMap::new(),
            tdigest_sketches: std::collections::BTreeMap::new(),
            tier: Tier::Cold,
        })
    }

    /// Bulk-delete the supplied object paths under one project. Returns
    /// the number of objects actually removed.
    ///
    /// Uses the same fan-out the project-prefix wipe path uses:
    /// `AmazonS3` rides its native `DeleteObjects` batch (1000 keys per
    /// RPC, 20 in flight), every other backend gets a 64-way
    /// `buffer_unordered` over per-key `delete()`. That's a ~6× speedup
    /// over the old `for p in paths { store.delete(&p).await }` loop on
    /// LocalFS / S3-mock backends.
    ///
    /// Project-prefix enforcement: every path must contain
    /// `projects/{project_id}/`; otherwise the whole call returns
    /// [`basin_common::BasinError::IsolationViolation`] without touching
    /// the store. Page-cache entries for each deleted file are
    /// invalidated on success — same hook as [`Storage::delete_file`].
    ///
    /// Errors on individual deletes are logged and swallowed; the
    /// catalog commit that authoritatively removed these files has
    /// already advanced the snapshot, so straggler files on disk are
    /// inefficiency, not correctness — same contract as the
    /// post-rewrite cleanup path in `basin_engine::dml_mutate::delete_objects`.
    pub async fn bulk_delete_files(
        &self,
        project: &ProjectId,
        paths: Vec<ObjectPath>,
    ) -> Result<usize> {
        if paths.is_empty() {
            return Ok(0);
        }
        let expected_prefix = format!("projects/{}/", project.as_prefix());
        for p in &paths {
            if !p.as_ref().contains(&expected_prefix) {
                return Err(basin_common::BasinError::isolation(format!(
                    "bulk_delete_files: {p} missing project prefix {expected_prefix}"
                )));
            }
        }
        // Use the per-project gated store so the deletes count against
        // this project's permit pool (the same contract as `delete_file`).
        let inner: Arc<dyn ObjectStore> = self.project_store(project);
        let n = paths.len();
        let _ = bulk_delete(&inner, paths.clone()).await.map_err(|e| {
            // Same shape as `delete_project_prefix`: we still report the
            // count attempted in the success path; per-key errors are
            // logged below. Aggregate failures only surface here.
            tracing::warn!(target: "basin_storage", error = %e, "bulk_delete_files: aggregate error");
            basin_common::BasinError::storage(format!("bulk_delete_files({project}): {e}"))
        })?;
        // Invalidate page-cache entries for every deleted file. The
        // disk-cache layer (when present) already invalidates on its
        // own `delete()` interception, but the page cache sits above
        // and needs its own hook (same as `delete_file`).
        if let Some(pc) = self.page_cache_handle() {
            for p in &paths {
                pc.invalidate_path(p);
            }
        }
        Ok(n)
    }

    /// Best-effort delete of one project-owned object. Used by the tiering
    /// compactor after an atomic catalog replace_data_files; failure is
    /// logged but not propagated because the catalog is already authoritative
    /// — a leftover hot object is wasted bytes, not a correctness violation.
    ///
    /// The caller is responsible for ensuring `path` is under `project`'s
    /// prefix; we re-check defensively.
    #[tracing::instrument(skip(self), fields(project=%project, path=%path))]
    pub async fn delete_file(&self, project: &ProjectId, path: &ObjectPath) -> Result<()> {
        let expected_prefix = format!("projects/{}/", project.as_prefix());
        if !path.as_ref().contains(&expected_prefix) {
            return Err(basin_common::BasinError::isolation(format!(
                "delete_file: {path} missing project prefix {expected_prefix}"
            )));
        }
        let store = self.project_store(project);
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

    /// Bulk-delete every object under `project`'s key prefix. Returns the
    /// number of objects deleted.
    ///
    /// Implementation strategy:
    ///
    /// 1. List the project prefix through the per-project gated store, so
    ///    the LIST itself counts against this project's permit pool.
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
    ///    step 1 already counted against this project's pool, and the
    ///    deletes themselves are bounded by the underlying store's
    ///    own concurrency knobs.
    ///
    /// The method intentionally does *not* call `Catalog::drop_table`:
    /// catalog state is the engine's responsibility. This method's
    /// contract is "physically free the bytes under the project prefix".
    /// A typical caller (the engine's project-deletion path) drops the
    /// catalog rows first and only then asks storage to remove the data.
    ///
    /// Prefer [`Storage::delete_project`] when a catalog is available — it
    /// fires `DeleteObjects` against the catalog-known files in parallel
    /// with the LIST RPC, hiding one round-trip on high-RTT object stores
    /// (~300-500ms saved on a project of 100 small files at high-latency endpoints).
    #[tracing::instrument(skip(self), fields(project=%project))]
    pub async fn delete_project_prefix(&self, project: &ProjectId) -> Result<usize> {
        let started = std::time::Instant::now();
        let p = self.project_root(project);

        // Step 1: gated LIST.
        let list_started = std::time::Instant::now();
        let gated = self.project_object_store(project);
        let paths_stream = gated.list(Some(&p)).map_ok(|m| m.location).boxed();

        // Step 2: hand the path stream to the *inner* store's
        // `delete_stream`. On AWS this picks up the
        // `aws::AmazonS3::delete_stream` override (1000-key batches,
        // 20-way parallel); on LocalFS / GCS / Azure we collect first
        // and fan-out 64-way through `bulk_delete` (the default
        // `.buffered(10)` is the bottleneck at 5000+ files).
        let inner = self.inner.object_store.clone();
        let collected: Vec<ObjectPath> = paths_stream.try_collect().await.map_err(|e| {
            basin_common::BasinError::storage(format!("delete_project_prefix({project}) list: {e}"))
        })?;
        let deleted: Vec<ObjectPath> = bulk_delete(&inner, collected).await.map_err(|e| {
            basin_common::BasinError::storage(format!("delete_project_prefix({project}): {e}"))
        })?;
        let list_delete_ms = list_started.elapsed().as_millis();
        // Drop any cached decoded batches for each deleted file. Same
        // rationale as `delete_file`: page cache lives one layer above
        // the disk cache and needs its own invalidation hook.
        if let Some(pc) = self.page_cache_handle() {
            for p in &deleted {
                pc.invalidate_path(p);
            }
        }
        let total_ms = started.elapsed().as_millis();
        tracing::info!(
            target: "basin_storage::delete_project",
            project = %project,
            mode = "list_only",
            files = deleted.len(),
            list_delete_ms = %list_delete_ms,
            total_ms = %total_ms,
            "delete_project_prefix",
        );
        Ok(deleted.len())
    }

    /// Build the project's full object-store prefix, honouring the optional
    /// configured root. `…/projects/{project}` — every key the storage layer
    /// emits for this project lives below this.
    fn project_root(&self, project: &ProjectId) -> ObjectPath {
        let mut p = self
            .inner
            .root_prefix
            .clone()
            .unwrap_or_else(|| ObjectPath::from(""));
        p = p.child(paths::PROJECTS_SEGMENT);
        p.child(project.as_prefix())
    }

    /// Catalog-aware project deletion. Eliminates the LIST → DELETE
    /// serial dependency on the hot path:
    ///
    /// 1. **Pull catalog file paths** (one fast SELECT or in-memory walk
    ///    via [`basin_catalog::Catalog::list_project_data_files`]). On
    ///    LocalFS / in-memory this is sub-millisecond; on a Postgres
    ///    catalog it's a single round-trip well under 50 ms even from
    ///    APAC.
    /// 2. **Fire `DeleteObjects` on the catalog set** *and* **start a
    ///    LIST** under the project prefix in parallel. The catalog is
    ///    authoritative for ~99% of the bytes (every Parquet data file);
    ///    LIST mops up files the catalog doesn't track (HNSW index
    ///    segments, write-aborted orphans, future per-project artefacts).
    /// 3. **Compute LIST diff and delete orphans.** On the common case
    ///    (no orphans) this resolves to a no-op `DeleteObjects` with an
    ///    empty key set; on the off case (a few HNSW segments) it's one
    ///    extra `DeleteObjects` RTT that runs *after* the bulk of the
    ///    bytes are gone.
    /// 4. **Drop catalog rows.** [`Catalog::drop_namespace`] cascades
    ///    through every table and snapshot row owned by `project`. The
    ///    in-memory and Postgres backends each implement this in a
    ///    single statement / single locked pass.
    ///
    /// On a high-latency S3-compatible store (e.g. cross-region) for a
    /// project of 100 small files, this drops the wall clock from ~3.2 s
    /// (LIST → bulk DELETE serial) to ~1.2 s (parallel LIST + DELETE; one
    /// RTT hidden).
    ///
    /// Falls back to the LIST-only path when [`Catalog::list_project_data_files`]
    /// returns an error (e.g. a transient catalog outage) — the storage
    /// layer's deletion contract is preserved either way.
    #[tracing::instrument(skip(self, catalog), fields(project=%project))]
    pub async fn delete_project(
        &self,
        catalog: &dyn basin_catalog::Catalog,
        project: &ProjectId,
    ) -> Result<usize> {
        let total_started = std::time::Instant::now();

        // ---- 1. Pull catalog file paths (fast) -----------------------
        let cat_started = std::time::Instant::now();
        let cat_files = match catalog.list_project_data_files(project).await {
            Ok(f) => f,
            Err(e) => {
                // Catalog read failed. Fall back to the LIST-only path so
                // a flaky catalog can't strand bytes. Engine callers that
                // want a hard failure can call `list_project_data_files`
                // themselves first.
                tracing::warn!(
                    target: "basin_storage::delete_project",
                    project = %project,
                    error = %e,
                    "catalog list_project_data_files failed; falling back to LIST-only path",
                );
                let n = self.delete_project_prefix(project).await?;
                // Still try the namespace drop so the catalog isn't left
                // dangling. Best-effort: a missing namespace returns Ok.
                let _ = catalog.drop_namespace(project).await;
                return Ok(n);
            }
        };
        let cat_lookup_ms = cat_started.elapsed().as_millis();
        let cat_files_count = cat_files.len();

        let inner = self.inner.object_store.clone();
        let prefix = self.project_root(project);

        // ---- 2. Concurrently: (a) DELETE catalog set, (b) LIST -------
        //
        // (a) catalog DELETE: feed an in-memory iterator of paths into
        // the inner store's `delete_stream`. On AWS S3 / Tigris the
        // AmazonS3 backend batches into 1000-key DeleteObjects requests,
        // 20 in flight; on LocalFS / GCS the default `.buffered(10)`
        // per-key path runs.
        let cat_paths: Vec<ObjectPath> = cat_files
            .into_iter()
            .map(|f| ObjectPath::from(f.path))
            .collect();

        let cat_delete_inner = inner.clone();
        let cat_paths_for_delete = cat_paths.clone();
        let cat_delete_fut = async move {
            if cat_paths_for_delete.is_empty() {
                return Ok::<Vec<ObjectPath>, basin_common::BasinError>(Vec::new());
            }
            bulk_delete(&cat_delete_inner, cat_paths_for_delete)
                .await
                .map_err(|e| {
                    basin_common::BasinError::storage(format!("delete_project catalog batch: {e}"))
                })
        };

        // (b) LIST under the project prefix, gated on the project's
        // semaphore so the LIST counts against this project's permit
        // pool (same property as the legacy `delete_project_prefix`
        // path). The full collected vector is small (one path per
        // file) and we need the whole set anyway to diff against the
        // catalog set, so a single buffered collect is fine.
        let gated = self.project_object_store(project);
        let list_prefix = prefix.clone();
        let list_fut = async move {
            gated
                .list(Some(&list_prefix))
                .map_ok(|m| m.location)
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| basin_common::BasinError::storage(format!("delete_project list: {e}")))
        };

        let cat_delete_started = std::time::Instant::now();
        let list_started = std::time::Instant::now();
        let (cat_delete_res, list_res) = tokio::join!(cat_delete_fut, list_fut);
        let cat_deleted = cat_delete_res?;
        let listed = list_res?;
        let cat_delete_ms = cat_delete_started.elapsed().as_millis();
        let list_ms = list_started.elapsed().as_millis();

        // ---- 3. Orphan delete: anything LIST saw that catalog didn't --
        let orphan_started = std::time::Instant::now();
        let cat_set: std::collections::HashSet<&str> =
            cat_paths.iter().map(|p| p.as_ref()).collect();
        let orphans: Vec<ObjectPath> = listed
            .into_iter()
            .filter(|p| !cat_set.contains(p.as_ref()))
            .collect();
        let orphan_count = orphans.len();
        let orphan_deleted = if !orphans.is_empty() {
            bulk_delete(&inner, orphans).await.map_err(|e| {
                basin_common::BasinError::storage(format!("delete_project orphan batch: {e}"))
            })?
        } else {
            Vec::new()
        };
        let orphan_delete_ms = orphan_started.elapsed().as_millis();

        // ---- 4. Drop catalog rows -----------------------------------
        let cat_drop_started = std::time::Instant::now();
        catalog.drop_namespace(project).await?;
        let cat_drop_ms = cat_drop_started.elapsed().as_millis();

        // Page-cache invalidation. Disk cache is invalidated in the
        // wrapping `DiskCachedStore::delete` already; page cache lives
        // one layer up. Same hook as `delete_file` / `delete_project_prefix`.
        if let Some(pc) = self.page_cache_handle() {
            for p in cat_deleted.iter().chain(orphan_deleted.iter()) {
                pc.invalidate_path(p);
            }
        }

        let total_files = cat_deleted.len() + orphan_deleted.len();
        let total_ms = total_started.elapsed().as_millis();
        tracing::info!(
            target: "basin_storage::delete_project",
            project = %project,
            mode = "catalog_first",
            cat_files_count = cat_files_count,
            cat_lookup_ms = %cat_lookup_ms,
            cat_delete_ms = %cat_delete_ms,
            list_ms = %list_ms,
            orphan_count = orphan_count,
            orphan_delete_ms = %orphan_delete_ms,
            cat_drop_ms = %cat_drop_ms,
            total_files = total_files,
            total_ms = %total_ms,
            "delete_project",
        );
        Ok(total_files)
    }

    /// Approximate nearest-neighbour search across all HNSW segments for
    /// `(project, table, column)`. Returns the merged top-`k` by distance.
    ///
    /// The returned `VectorHit::file_path` currently points at the index
    /// segment file rather than the matching Parquet data file — we don't
    /// track the data-side correspondence yet (see ADR 0003 for the
    /// compactor-driven plan). Higher layers (see `basin-engine`) resolve
    /// the row by reading the table's Parquet files and matching `row_id`.
    #[tracing::instrument(skip(self, query), fields(project=%project, table=%table, column=%column, k=%k))]
    pub async fn vector_search(
        &self,
        project: &ProjectId,
        table: &TableName,
        column: &str,
        query: &[f32],
        k: usize,
        distance: basin_vector::Distance,
    ) -> Result<Vec<VectorHit>> {
        vector_index::vector_search(self, project, table, column, query, k, distance).await
    }
}

/// Bulk-delete `paths` against the inner store with backend-aware concurrency.
///
/// Why this exists: `ObjectStore::delete_stream`'s default impl uses
/// `.buffered(10)` which means LocalFS gets only 10-way parallel unlinks.
/// On a 5000-file project that's 500 sequential rounds × ~1 ms each = ~500 ms
/// of pure latency. The `AmazonS3` backend overrides `delete_stream` with a
/// native `DeleteObjects` batch path (1000 keys per RPC, 20 in flight) which
/// is the right thing on the network — we keep using that when the inner
/// store is S3.
///
/// Detection: `ObjectStore: !Any`, so we can't `downcast_ref`. The
/// `Display` impl is the only public discriminator across backends —
/// `AmazonS3` prints `"AmazonS3(...)"`, `LocalFileSystem` prints
/// `"LocalFileSystem(...)"`, etc. Hacky but works and is what the
/// upstream crate does internally.
///
/// Concurrency: 64-way fan-out on LocalFS / non-S3 stores. `pollster`-style
/// 64 in-flight unlinks doesn't blow FD limits (macOS default 256;
/// Linux 1024) and the speedup over the default 10-way is linear up to
/// somewhere near the kernel's parallel-unlink ceiling.
async fn bulk_delete(
    inner: &Arc<dyn ObjectStore>,
    paths: Vec<ObjectPath>,
) -> std::result::Result<Vec<ObjectPath>, object_store::Error> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    // Heuristic backend detection via `Display`. If the inner store reports
    // itself as AmazonS3 we ride the bulk DeleteObjects path; otherwise
    // (LocalFS, in-memory, GCS, Azure — all per-key) we fan out 64-way
    // ourselves rather than rely on the default 10-way `.buffered`.
    //
    // TODO: when ObjectStore picks up a typed `delete_batch` capability
    // probe, swap this string check for a proper one.
    let display = format!("{}", inner);
    let is_s3 = display.starts_with("AmazonS3");

    if is_s3 {
        let stream = futures::stream::iter(paths.into_iter().map(Ok)).boxed();
        inner.delete_stream(stream).try_collect::<Vec<_>>().await
    } else {
        const FAN_OUT: usize = 64;
        let inner = inner.clone();
        let results: Vec<std::result::Result<ObjectPath, object_store::Error>> =
            futures::stream::iter(paths.into_iter())
                .map(|p| {
                    let inner = inner.clone();
                    async move {
                        inner.delete(&p).await?;
                        Ok::<ObjectPath, object_store::Error>(p)
                    }
                })
                .buffer_unordered(FAN_OUT)
                .collect()
                .await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
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
    use basin_common::{PartitionKey, ProjectId, TableName};
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOpts, PutOptions, PutPayload, PutResult,
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

    // ── #119: per-project state TTL eviction ─────────────────────────────────
    //
    // These tests share a process-wide fake clock and the
    // `BASIN_STORAGE_PROJECT_STATE_TTL_SECS` env var; serialise them through
    // `TTL_TEST_LOCK` so the parallel test runner can't stomp them.
    static TTL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Insert N projects with the fake clock at t=1000, advance the clock past
    /// the TTL, then insert one more project. The inline sweep should evict
    /// every stale entry within `PROJECT_STATE_SWEEP_BUDGET` candidates, so
    /// the final live-entry count is 1 (only the post-advance project).
    ///
    /// Uses the test-only fake clock to avoid sleeping for minutes.
    #[test]
    fn project_state_ttl_evicts_stale_entries() {
        let _g = TTL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        // Pin the fake clock so the resolved TTL (1800s by default in
        // production; we resolve it at construction time above) compares
        // against deterministic seconds.
        super::set_project_state_now_secs(1000);
        // Insert N projects. N is well within PROJECT_STATE_SWEEP_BUDGET (256)
        // so a single sweep covers all of them.
        let n: usize = 64;
        for _ in 0..n {
            let _ = s.project_semaphore(&ProjectId::new());
        }
        assert_eq!(s.project_state_len(), n, "all N projects must be live");
        // Advance past the default TTL (1800s).
        super::set_project_state_now_secs(1000 + super::DEFAULT_PROJECT_STATE_TTL_SECS + 1);
        // First touch of a brand-new project triggers the inline sweep, which
        // sees every existing entry as stale (last_touched=1000 < cutoff).
        let _ = s.project_semaphore(&ProjectId::new());
        assert_eq!(
            s.project_state_len(),
            1,
            "all N stale entries should be evicted, leaving only the fresh insert"
        );
        // Release the clock pin for other tests.
        super::set_project_state_now_secs(0);
    }

    /// Accessor on an existing project must bump `last_touched` and prevent
    /// eviction even when other entries become stale.
    #[test]
    fn project_state_touch_resets_ttl() {
        let _g = TTL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        super::set_project_state_now_secs(2000);
        let warm = ProjectId::new();
        let cold = ProjectId::new();
        let _ = s.project_semaphore(&warm);
        let _ = s.project_semaphore(&cold);
        // Advance halfway through the TTL: nothing should be stale yet.
        super::set_project_state_now_secs(2000 + super::DEFAULT_PROJECT_STATE_TTL_SECS / 2);
        let _ = s.project_semaphore(&warm); // bump warm
        // Advance the rest of the way past TTL relative to the initial
        // insert (2000). `cold` is now stale (last_touched=2000),
        // `warm` is still fresh (last_touched=2000+ttl/2).
        super::set_project_state_now_secs(2000 + super::DEFAULT_PROJECT_STATE_TTL_SECS + 1);
        // Trigger sweep via a new insert.
        let _ = s.project_semaphore(&ProjectId::new());
        // After sweep: `cold` evicted, `warm` retained, new project inserted.
        assert_eq!(s.project_state_len(), 2);
        super::set_project_state_now_secs(0);
    }

    /// TTL=0/off disables eviction entirely.
    #[test]
    fn project_state_ttl_off_disables_eviction() {
        let _g = TTL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Best-effort isolation: set the env var, build a fresh Storage so
        // `resolve_project_state_ttl_secs` sees the override. Use a guarded
        // restore so we don't leak the var to peer tests.
        struct EnvGuard(&'static str, Option<String>);
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                match &self.1 {
                    Some(v) => std::env::set_var(self.0, v),
                    None => std::env::remove_var(self.0),
                }
            }
        }
        let prev = std::env::var("BASIN_STORAGE_PROJECT_STATE_TTL_SECS").ok();
        std::env::set_var("BASIN_STORAGE_PROJECT_STATE_TTL_SECS", "0");
        let _guard = EnvGuard("BASIN_STORAGE_PROJECT_STATE_TTL_SECS", prev);

        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        super::set_project_state_now_secs(3000);
        for _ in 0..16 {
            let _ = s.project_semaphore(&ProjectId::new());
        }
        // Advance a million seconds; nothing should be evicted.
        super::set_project_state_now_secs(3000 + 1_000_000);
        let _ = s.project_semaphore(&ProjectId::new());
        assert_eq!(
            s.project_state_len(),
            17,
            "eviction off → map grows monotonically"
        );
        super::set_project_state_now_secs(0);
    }

    /// Parallel inserts + accesses must not panic and must not double-allocate
    /// the same project (HashMap guarantee). The mutex never crosses an
    /// `await`, so this is purely a sanity check that the new TTL plumbing
    /// hasn't introduced poison or a lock-order regression.
    #[test]
    fn project_state_ttl_concurrent_inserts_no_panic() {
        let _g = TTL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let s = Arc::new(storage_in(&dir));
        super::set_project_state_now_secs(4000);
        // Use a shared pool of N project IDs so half the threads insert
        // while the other half touch existing entries.
        let ids: Arc<Vec<ProjectId>> = Arc::new((0..32).map(|_| ProjectId::new()).collect());
        let threads: Vec<_> = (0..16)
            .map(|t| {
                let s = s.clone();
                let ids = ids.clone();
                std::thread::spawn(move || {
                    for k in 0..256 {
                        let id = ids[(t * 256 + k) % ids.len()];
                        let _ = s.project_semaphore(&id);
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().expect("worker panic");
        }
        // All 32 distinct IDs end up in the map (no eviction since the clock
        // never advanced past TTL).
        assert_eq!(s.project_state_len(), 32);
        super::set_project_state_now_secs(0);
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();

        let batch = small_batch(0, 1_000, "row-");
        let df = s
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
        assert_eq!(df.row_count, 1_000);
        assert!(df.path.as_ref().contains(&format!("projects/{project}/")));

        let stream = s
            .read(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
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
    async fn project_isolation() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = ProjectId::new();
        let b = ProjectId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.write_batch(&a, &table, &part, &small_batch(0, 10, "a-"))
            .await
            .unwrap();
        s.write_batch(&b, &table, &part, &small_batch(0, 20, "b-"))
            .await
            .unwrap();

        let collect = |t: ProjectId| {
            let s = s.clone();
            let table = table.clone();
            async move {
                let stream = s.read(&t, &table, ReadOptions::default()).await.unwrap();
                let batches: Vec<_> = stream.collect::<Vec<_>>().await;
                let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
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
        let project = ProjectId::new();
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
        s.write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();

        let opts = ReadOptions {
            projection: Some(vec!["a".into(), "c".into()]),
            ..Default::default()
        };
        let stream = s.read(&project, &table, opts).await.unwrap();
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
                    self.range_bytes.fetch_add(
                        rng.end.saturating_sub(rng.start) as usize,
                        Ordering::Relaxed,
                    );
                }
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let inner = self.inner.clone();
            let prefix = prefix.cloned();
            inner.list(prefix.as_ref())
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
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
        let project = ProjectId::new();
        let table = TableName::new("rg").unwrap();
        let part = PartitionKey::default_key();

        // Need ≥ 2 row groups for pruning to do work. Writer's row-group
        // cap is 65_536, so 200_000 rows splits into ~3 groups; a point
        // query for the last id lands in only one of them.
        //
        // Row-group / footer-stats pruning is a Parquet-format feature
        // (Vortex has no Parquet row groups; its native chunk pruning is a
        // separate path). This test validates the Parquet pruning path
        // specifically, so it pins the Parquet format explicitly rather
        // than relying on the default (which is Vortex, #161).
        let batch = small_batch(0, 200_000, "v");
        s.write_batch_with_options(
            &project,
            &table,
            &part,
            &batch,
            &WriteOptions {
                file_format: FileFormat::Parquet,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Reset counters so we measure only the read path.
        counting.range_gets.store(0, Ordering::Relaxed);
        counting.range_bytes.store(0, Ordering::Relaxed);

        // id = 199_500 lives only in the last row group.
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(199_500))],
            ..Default::default()
        };
        let stream = s.read(&project, &table, opts).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
        assert!(total >= 1, "expected the matching row");

        // 10 row groups exist; pruning must drop the vast majority. We allow
        // some metadata GETs but assert overall byte volume is much less than
        // the full file. A single matching row group + footer should be far
        // under half the file size.
        let bytes = counting.range_bytes.load(Ordering::Relaxed);
        let full_file =
            std::fs::metadata(walkdir_first_parquet(dir.path()).expect("parquet file to exist"))
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
    async fn list_data_files_returns_only_one_project() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = ProjectId::new();
        let b = ProjectId::new();
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
        let prefix_a = format!("projects/{a}/");
        for f in &listed_a {
            assert!(
                f.path.as_ref().contains(&prefix_a),
                "leaked path {}",
                f.path
            );
            assert!(!f.path.as_ref().contains(&format!("projects/{b}/")));
        }
    }

    // -----------------------------------------------------------------------
    // T-049 / T-051 BYO-bucket tests
    // -----------------------------------------------------------------------

    /// Minimal in-memory catalog stub for BYO tests. Supports
    /// `get_project_metadata` / `drop_namespace` / `list_project_data_files`;
    /// all other methods panic to ensure they aren't called unexpectedly.
    struct ByoCatalog {
        project_meta: std::sync::Mutex<HashMap<ProjectId, basin_catalog::ProjectMetadata>>,
        dropped: std::sync::Mutex<Vec<ProjectId>>,
    }

    impl ByoCatalog {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                project_meta: std::sync::Mutex::new(HashMap::new()),
                dropped: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn set_meta(&self, project: ProjectId, meta: basin_catalog::ProjectMetadata) {
            self.project_meta
                .lock()
                .unwrap()
                .insert(project, meta);
        }

        fn was_dropped(&self, project: &ProjectId) -> bool {
            self.dropped.lock().unwrap().contains(project)
        }
    }

    #[async_trait::async_trait]
    impl basin_catalog::Catalog for ByoCatalog {
        async fn create_namespace(&self, _p: &ProjectId) -> basin_common::Result<()> { Ok(()) }
        async fn drop_namespace(&self, p: &ProjectId) -> basin_common::Result<()> {
            self.dropped.lock().unwrap().push(*p);
            Ok(())
        }
        async fn create_table(&self, _p: &ProjectId, _t: &TableName, _schema: &arrow_schema::Schema) -> basin_common::Result<basin_catalog::TableMetadata> { unimplemented!() }
        async fn load_table(&self, _p: &ProjectId, _t: &TableName) -> basin_common::Result<basin_catalog::TableMetadata> { unimplemented!() }
        async fn drop_table(&self, _p: &ProjectId, _t: &TableName) -> basin_common::Result<()> { Ok(()) }
        async fn list_tables(&self, _p: &ProjectId) -> basin_common::Result<Vec<TableName>> { Ok(vec![]) }
        async fn append_data_files(&self, _p: &ProjectId, _t: &TableName, _exp: basin_catalog::SnapshotId, _files: Vec<basin_catalog::DataFileRef>) -> basin_common::Result<basin_catalog::TableMetadata> { unimplemented!() }
        async fn replace_data_files(&self, _p: &ProjectId, _t: &TableName, _exp: basin_catalog::SnapshotId, _removed: Vec<String>, _added: Vec<basin_catalog::DataFileRef>) -> basin_common::Result<basin_catalog::TableMetadata> { unimplemented!() }
        async fn list_snapshots(&self, _p: &ProjectId, _t: &TableName) -> basin_common::Result<Vec<basin_catalog::Snapshot>> { Ok(vec![]) }
        async fn set_partition_spec(&self, _p: &ProjectId, _t: &TableName, _spec: basin_catalog::PartitionSpec) -> basin_common::Result<()> { Ok(()) }
        async fn list_project_data_files(&self, _p: &ProjectId) -> basin_common::Result<Vec<basin_catalog::DataFileRef>> {
            // Return empty list so delete_project fast-paths to an empty bulk delete.
            Ok(vec![])
        }
        async fn get_project_metadata(&self, p: &ProjectId) -> basin_common::Result<basin_catalog::ProjectMetadata> {
            Ok(self
                .project_meta
                .lock()
                .unwrap()
                .get(p)
                .cloned()
                .unwrap_or_default())
        }
    }

    /// Thin spy wrapping an `InMemory` store; counts how many objects were
    /// passed through `delete_stream` (the bulk-delete path used by
    /// `delete_project` / `delete_project_prefix`).
    #[derive(Debug, Clone)]
    struct SpyStore {
        inner: Arc<object_store::memory::InMemory>,
        deletes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl SpyStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                deletes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
        fn delete_count(&self) -> usize {
            self.deletes.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl std::fmt::Display for SpyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SpyStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for SpyStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            // Intercept each path that passes through, bump the counter,
            // then delegate to the inner InMemory store. Both the counter
            // Arc and the inner Arc are cloned so the stream can be 'static.
            use futures::StreamExt;
            let counter = self.deletes.clone();
            let inner = self.inner.clone();
            let counted: BoxStream<'static, object_store::Result<ObjectPath>> =
                locations.map(move |r| {
                    if r.is_ok() {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    r
                }).boxed();
            inner.delete_stream(counted)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// T-049: register a BYO store; `project_object_store` must route to it.
    ///
    /// Strategy: register a SpyStore as BYO, write a batch, assert the data
    /// lives in the SpyStore's inner InMemory — NOT in the shared LocalFS.
    /// This confirms project_object_store routes through the BYO backing store.
    #[tokio::test]
    async fn register_then_lookup_returns_byo_store() {
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let project = ProjectId::new();
        let spy = SpyStore::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        // Register the spy as the BYO store.
        s.register_byo_object_store(project, Arc::new(spy.clone()) as Arc<dyn ObjectStore>);

        // Write a batch — this must hit the spy store.
        s.write_batch(&project, &table, &part, &small_batch(0, 5, "x-"))
            .await
            .unwrap();

        // Because the spy's inner InMemory received the PUT we can read it back.
        let stream = s.read(&project, &table, ReadOptions::default()).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
        assert_eq!(total, 5, "expected 5 rows read back from BYO store");

        // The shared store (LocalFS) must be empty for this project.
        let gated = {
            // Temporarily use the raw shared store to verify isolation.
            let gated_shared = Arc::new(concurrency::ProjectScopedStore::new(
                s.inner.object_store.clone(),
                s.project_semaphore(&project),
                s.inner.scheduler.clone(),
                project,
                None,
            ));
            let root = s.project_root(&project);
            gated_shared
                .list(Some(&root))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
        };
        assert!(
            gated.is_empty(),
            "shared store should have no objects for a BYO project"
        );
    }

    /// T-049: after `deregister_byo_object_store`, `project_object_store`
    /// must route to the shared store (fallback).
    #[tokio::test]
    async fn deregister_falls_back_to_shared() {
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let project = ProjectId::new();
        let spy = SpyStore::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.register_byo_object_store(project, Arc::new(spy.clone()) as Arc<dyn ObjectStore>);
        s.deregister_byo_object_store(&project);

        // Write a batch — must now land in the shared LocalFS store, not spy.
        s.write_batch(&project, &table, &part, &small_batch(0, 3, "y-"))
            .await
            .unwrap();

        // Reading back works (shared store has the data).
        let stream = s.read(&project, &table, ReadOptions::default()).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
        assert_eq!(total, 3, "expected 3 rows from shared store after deregister");
    }

    /// T-051: `delete_project_byo_aware` must NOT issue any DELETE to the BYO
    /// store when a BYO bucket is configured — it only drops the catalog
    /// namespace.
    #[tokio::test]
    async fn delete_byo_project_leaves_bucket_objects() {
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let project = ProjectId::new();
        let spy = SpyStore::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        // Register the BYO store and write a file into it.
        s.register_byo_object_store(project, Arc::new(spy.clone()) as Arc<dyn ObjectStore>);
        s.write_batch(&project, &table, &part, &small_batch(0, 5, "byo-"))
            .await
            .unwrap();

        // Set up a catalog that reports byo_bucket = Some(…).
        let catalog = ByoCatalog::new();
        catalog.set_meta(
            project,
            basin_catalog::ProjectMetadata {
                byo_bucket: Some(basin_catalog::S3Config {
                    endpoint: "https://s3.example.com".into(),
                    bucket: "cust-bucket".into(),
                    region: "us-east-1".into(),
                    access_key_id: "AKI".into(),
                    secret_access_key_enc: b"encrypted".to_vec(),
                    force_path_style: false,
                }),
                home_region: None,
            },
        );

        // Snapshot delete count before.
        let deletes_before = spy.delete_count();

        let deleted = s
            .delete_project_byo_aware(project, catalog.as_ref())
            .await
            .unwrap();

        assert_eq!(deleted, 0, "BYO deletion must return 0 (no objects removed)");
        assert_eq!(
            spy.delete_count(),
            deletes_before,
            "no DELETE must be issued to the BYO store"
        );
        assert!(
            catalog.was_dropped(&project),
            "catalog namespace must still be dropped"
        );
    }

    /// T-051: `delete_project_byo_aware` with no BYO bucket must physically
    /// remove all objects under the project prefix via the shared store.
    #[tokio::test]
    async fn delete_shared_project_removes_objects() {
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        // Write two files into the shared store.
        s.write_batch(&project, &table, &part, &small_batch(0, 5, "s-"))
            .await
            .unwrap();
        s.write_batch(&project, &table, &part, &small_batch(5, 5, "s-"))
            .await
            .unwrap();

        // Verify the files exist.
        let files_before = s.list_data_files(&project, &table).await.unwrap();
        assert_eq!(files_before.len(), 2, "expected 2 files before deletion");

        let catalog = ByoCatalog::new();
        // No byo_bucket set — ProjectMetadata::default().
        let deleted = s
            .delete_project_byo_aware(project, catalog.as_ref())
            .await
            .unwrap();

        assert!(deleted >= 2, "at least the 2 data files must be deleted; got {deleted}");
        assert!(
            catalog.was_dropped(&project),
            "catalog namespace must be dropped"
        );

        // After deletion the store should have no objects for this project.
        let root = s.project_root(&project);
        let remaining = s
            .inner
            .object_store
            .list(Some(&root))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            remaining.is_empty(),
            "no objects should remain after shared-project deletion"
        );
    }

    // -----------------------------------------------------------------------
    // Parquet footer cache cap — default + env override
    // -----------------------------------------------------------------------

    /// Single combined test for the cap constant + env-override resolver.
    /// The default must be the audit-driven 16384 and the env var must
    /// supersede it; zero / unparseable values must fall back to the
    /// default. Combined into one test (rather than split) because
    /// `BASIN_STORAGE_PARQUET_META_CACHE_CAP` is a process-global env var
    /// — running each assertion in its own parallel `#[test]` would race.
    #[test]
    fn parquet_meta_cache_cap_resolver() {
        let key = "BASIN_STORAGE_PARQUET_META_CACHE_CAP";
        let prior = std::env::var(key).ok();

        std::env::remove_var(key);
        assert_eq!(resolve_parquet_meta_cache_cap(), 16_384);
        assert_eq!(DEFAULT_PARQUET_META_CACHE_CAP, 16_384);

        // Default propagates through Storage::new.
        {
            let dir = TempDir::new().unwrap();
            let s = storage_in(&dir);
            assert_eq!(s.inner.parquet_meta_cache.cap(), 16_384);
        }

        // Explicit override is honored.
        std::env::set_var(key, "4096");
        assert_eq!(resolve_parquet_meta_cache_cap(), 4096);
        {
            let dir = TempDir::new().unwrap();
            let s = storage_in(&dir);
            assert_eq!(
                s.inner.parquet_meta_cache.cap(),
                4096,
                "override must thread through Storage::new",
            );
        }

        // Zero falls back to default.
        std::env::set_var(key, "0");
        assert_eq!(
            resolve_parquet_meta_cache_cap(),
            DEFAULT_PARQUET_META_CACHE_CAP,
        );

        // Unparseable falls back to default.
        std::env::set_var(key, "not-a-number");
        assert_eq!(
            resolve_parquet_meta_cache_cap(),
            DEFAULT_PARQUET_META_CACHE_CAP,
        );

        // Restore.
        match prior {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }
}
