//! `ObjectStoreCatalog` — a Basin-native **shared** catalog backed entirely by
//! the object store (Tigris / S3 / any `object_store::ObjectStore`), with **no
//! external database**.
//!
//! N engine nodes can share one project's table metadata + partition leases by
//! pointing this catalog at the same bucket/prefix. Correctness rests on a
//! single primitive the object store gives us: **create-if-absent**
//! (`PutMode::Create`, i.e. HTTP `If-None-Match: *`). For the real S3/Tigris
//! store this is only atomic when the `AmazonS3Builder` has conditional-put
//! enabled — in `object_store` 0.13 that is the **default**
//! (`S3ConditionalPut::ETagMatch`), and `basin-storage`'s builder pins it
//! explicitly so a future edit can't silently disable it. The in-memory
//! `object_store::memory::InMemory` store honours `PutMode::Create` atomically,
//! which is what the concurrency tests in this module exercise.
//!
//! ## Design: "append the next monotonic version via create-if-absent"
//!
//! Everything is modelled as appending the next monotonically-versioned object
//! and letting the store reject the loser of a race.
//!
//! ### Table metadata as a versioned manifest log
//!
//! Per `(project, schema, table)` the full table state is serialised as a JSON
//! [`TableManifest`] at a monotonically-versioned key:
//!
//! ```text
//!   {root}{project}/{schema}/{table}/v{N:020}.json   # immutable manifest, N = catalog version
//!   {root}{project}/{schema}/{table}/HEAD            # best-effort pointer to max N
//! ```
//!
//! - "Current version" = highest `N` present. Resolved via the small `HEAD`
//!   pointer (one GET), with a LIST fallback (take max `N`) when `HEAD` is
//!   missing/stale.
//! - A commit loads version `N`, applies the mutation to build manifest `N+1`,
//!   and `put_opts(v{N+1}, …, PutMode::Create)`. If the store says
//!   `AlreadyExists`, another node already wrote `N+1`: reload and either retry
//!   transparently (idempotent DDL) or surface [`BasinError::CommitConflict`]
//!   (append/replace — the engine already retries on that error).
//! - Old version objects are **never deleted**: they are the history (enabling
//!   time-travel) and let a reader mid-commit keep reading a consistent older
//!   manifest.
//!
//! ### Shared lease registry on the same primitive
//!
//! [`ObjectStoreLeaseRegistry`] models leases as a monotonic **epoch log**:
//!
//! ```text
//!   {lease_root}{project}/{partition}/e{EPOCH:020}.json
//! ```
//!
//! Acquisition only ever *creates a higher epoch*. Two racers can never both
//! win the same epoch — the create-if-absent loser is fenced. The epoch is the
//! same monotonic fencing token the shard records and the WAL appends carry.
//!
//! ## Freshness model
//!
//! Readers see the latest **committed** manifest. A reader mid-flight sees a
//! consistent older version (manifests are immutable per version). A small
//! in-process cache keyed by `(project, table)` stores the resolved manifest
//! plus its version; it is validated against `HEAD` (a cheap GET) so the hot
//! `current_snapshot_id` read does not re-download the whole manifest on every
//! statement. Correctness never depends on the cache — a stale cache only ever
//! causes an extra reload or a benign `CommitConflict` retry.

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::Schema;
use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, QualifiedTableName, Result, SchemaName, TableName};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use lru::LruCache;
use object_store::{path::Path as OsPath, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::leases::{Lease, LeaseRegistry};
use crate::metadata::{
    CheckConstraint, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy, ProjectMetadata,
    PromotedJsonbPath, SecondaryIndex, TableFileFormat, TableMetadata, UniqueConstraint,
};
use crate::functions::SqlFunctionDef;
use crate::sequences::{compute_next, SequenceDef, SequenceError};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::Catalog;

/// Default catalog root prefix under the bucket.
pub const DEFAULT_CATALOG_PREFIX: &str = "_catalog/";
/// Default lease-registry root prefix under the bucket.
pub const DEFAULT_LEASE_PREFIX: &str = "_leases/";

/// Bound on transparent retries for an idempotent DDL mutation that lost the
/// create-if-absent race. After this many losses we surface the conflict.
const MAX_DDL_RETRIES: usize = 8;

/// Capacity (number of distinct chunk objects) of the content-addressed baseline
/// chunk read cache. Each entry is one `Vec<DataFileRef>` (~TARGET files), so a
/// few thousand entries cover many large partitions' frozen chunk sets while
/// keeping memory bounded. Overridable via `BASIN_CHUNK_CACHE_CAP`.
const DEFAULT_CHUNK_CACHE_CAP: usize = 4096;

/// Default bounded-staleness read-snapshot TTL (ms). A metadata read under
/// active ingest serves the last resolved unioned view for up to this long
/// without re-LISTing / re-HEADing / re-folding.
///
/// DEFAULT IS `0` (DISABLED) — a non-zero TTL trades bounded read staleness for
/// read throughput under ingest, and because `load_table` feeds BOTH the
/// metadata fast-aggregate path AND the scan path (and a per-partition data
/// commit does not bump the META version that gates the snapshot), a non-zero
/// default would let a scan miss just-inserted rows for up to the TTL — a
/// read-your-writes regression. So it is OPT-IN via `BASIN_READ_SNAPSHOT_TTL_MS`
/// for analytics-heavy-under-ingest deployments that accept bounded staleness.
/// (Follow-up: scope the snapshot to the count/max fast-aggregate path only —
/// which is already eventually-consistent under ingest — so it can default on
/// without affecting scan freshness.)
const DEFAULT_READ_SNAPSHOT_TTL_MS: u64 = 0;

fn chunk_cache_cap() -> std::num::NonZeroUsize {
    let n = std::env::var("BASIN_CHUNK_CACHE_CAP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_CHUNK_CACHE_CAP);
    std::num::NonZeroUsize::new(n).unwrap_or(std::num::NonZeroUsize::new(DEFAULT_CHUNK_CACHE_CAP).unwrap())
}

/// The full, serialisable state of one table at one catalog version.
///
/// This captures everything the in-memory `TableState` carries so the manifest
/// is the single source of truth: a fresh node can reconstruct the complete
/// [`TableMetadata`] (including the entire snapshot chain) from the latest
/// manifest object alone. Every field uses `#[serde(default)]` so a manifest
/// written by an older build deserialises cleanly.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableManifest {
    /// Monotonic catalog version of this manifest (the `N` in `v{N}.json`).
    version: u64,
    /// Arrow schema (serde-serialisable directly, same as the Postgres backend).
    schema: Schema,
    current_snapshot: SnapshotId,
    snapshots: Vec<Snapshot>,
    #[serde(default)]
    format_version: u8,
    #[serde(default)]
    partition_spec: PartitionSpec,
    #[serde(default)]
    rls_enabled: bool,
    #[serde(default)]
    policies: Vec<Policy>,
    #[serde(default)]
    cold_after_seconds: Option<u64>,
    #[serde(default)]
    cold_age_column: Option<String>,
    #[serde(default)]
    bloom_filter_columns: Vec<String>,
    #[serde(default)]
    row_group_rows: Option<usize>,
    #[serde(default)]
    continuous_aggregate: Option<CvDef>,
    #[serde(default)]
    cluster_columns: Vec<String>,
    #[serde(default)]
    file_format: TableFileFormat,
    #[serde(default)]
    row_block_size: Option<u32>,
    #[serde(default)]
    home_region: Option<String>,
    #[serde(default)]
    indexes: Vec<SecondaryIndex>,
    #[serde(default)]
    pk_columns: Vec<String>,
    #[serde(default)]
    check_constraints: Vec<CheckConstraint>,
    #[serde(default)]
    foreign_keys: Vec<ForeignKeyDef>,
    #[serde(default)]
    unique_constraints: Vec<UniqueConstraint>,
    #[serde(default)]
    global_sort_order: Option<Vec<String>>,
    #[serde(default)]
    adaptive_sort_override: Option<bool>,
    #[serde(default)]
    gc_orphan_paths: Vec<String>,
    #[serde(default)]
    promoted_jsonb_paths: Vec<PromotedJsonbPath>,
    /// Tombstone: set when the table has been dropped. A dropped table keeps a
    /// final manifest version so concurrent readers resolve it deterministically
    /// (rather than racing a delete); `load_table` treats it as `NotFound`.
    #[serde(default)]
    dropped: bool,
    /// Per-create GENERATION of this table's partition (`parts/`) segment tree.
    ///
    /// A table's META manifest prefix is keyed only on `(project, schema, name)`
    /// with no generation, so a CREATE TABLE of the SAME name reuses the prefix.
    /// The partition segment chains live under `parts/g{generation}/{pid}/…`, so
    /// each create stamps a fresh generation: a DROP's best-effort purge LISTs
    /// (and deletes) only the generation that was current AT DROP TIME, so it can
    /// NEVER delete a segment that a later same-name recreate (a higher
    /// generation) wrote — closing the drop-purge-vs-recreate race that tore a
    /// delta chain (a delta whose baseline segment had been concurrently purged).
    ///
    /// `0` for genesis tables AND for every manifest written before this field
    /// existed (serde default), so legacy partition trees at the un-prefixed
    /// `parts/{pid}/…` layout are read transparently (see `gen_subdir`).
    #[serde(default)]
    parts_generation: u64,
}

impl TableManifest {
    fn genesis(schema: Schema) -> Self {
        let genesis = Snapshot {
            id: SnapshotId::GENESIS,
            parent: None,
            committed_at: Utc::now(),
            data_files: Vec::new(),
            removed_paths: Vec::new(),
            summary: SnapshotSummary {
                operation: SnapshotOperation::Genesis,
                added_files: 0,
                added_rows: 0,
                added_bytes: 0,
                removed_files: 0,
            },
        };
        Self {
            version: 0,
            schema,
            current_snapshot: SnapshotId::GENESIS,
            snapshots: vec![genesis],
            format_version: 2,
            partition_spec: PartitionSpec::Unpartitioned,
            rls_enabled: false,
            policies: Vec::new(),
            cold_after_seconds: None,
            cold_age_column: None,
            bloom_filter_columns: Vec::new(),
            row_group_rows: None,
            continuous_aggregate: None,
            cluster_columns: Vec::new(),
            file_format: TableFileFormat::default(),
            row_block_size: None,
            home_region: None,
            indexes: Vec::new(),
            pk_columns: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            global_sort_order: None,
            adaptive_sort_override: None,
            gc_orphan_paths: Vec::new(),
            promoted_jsonb_paths: Vec::new(),
            dropped: false,
            parts_generation: 0,
        }
    }

    fn to_metadata(&self, project: &ProjectId, table: &TableName) -> TableMetadata {
        TableMetadata {
            project: *project,
            table: table.clone(),
            schema: Arc::new(self.schema.clone()),
            current_snapshot: self.current_snapshot,
            snapshots: self.snapshots.clone(),
            format_version: if self.format_version == 0 {
                2
            } else {
                self.format_version
            },
            partition_spec: self.partition_spec.clone(),
            rls_enabled: self.rls_enabled,
            policies: self.policies.clone(),
            cold_after_seconds: self.cold_after_seconds,
            cold_age_column: self.cold_age_column.clone(),
            bloom_filter_columns: self.bloom_filter_columns.clone(),
            row_group_rows: self.row_group_rows,
            continuous_aggregate: self.continuous_aggregate.clone(),
            cluster_columns: self.cluster_columns.clone(),
            file_format: self.file_format,
            row_block_size: self.row_block_size,
            home_region: self.home_region.clone(),
            indexes: self.indexes.clone(),
            pk_columns: self.pk_columns.clone(),
            check_constraints: self.check_constraints.clone(),
            foreign_keys: self.foreign_keys.clone(),
            unique_constraints: self.unique_constraints.clone(),
            global_sort_order: self.global_sort_order.clone(),
            adaptive_sort_override: self.adaptive_sort_override,
            gc_orphan_paths: self.gc_orphan_paths.clone(),
            promoted_jsonb_paths: self.promoted_jsonb_paths.clone(),
        }
    }
}

/// One versioned object in a partition's segment chain. This is the on-disk
/// unit that the hot ingest path CASes — sharding it by partition is the
/// multi-node scaling fix: each partition has a single owner/writer, so
/// concurrent writers CAS different keys and never contend.
///
/// Layout (per `(project, schema, table, partition_id)`):
/// ```text
///   {root}{project}/{schema}/{table}/parts/{partition_id}/v{M:020}.json  # immutable segment object, M = per-partition version
///   {root}{project}/{schema}/{table}/parts/{partition_id}/HEAD           # best-effort max-M pointer
/// ```
///
/// FLAT-SCALE FORMAT (delta log + periodic snapshot): each versioned object is
/// EITHER a small DELTA — only the change for THIS commit (`delta`) plus a
/// `base_version` pointer to the prior object — OR a consolidated SNAPSHOT/
/// baseline (`baseline = Some(full live set)`, `base_version = None`). A commit
/// writes ONE delta object whose size is O(files added/removed in THIS commit),
/// independent of how many files the partition already holds — so per-commit
/// work (and thus ingest throughput) is flat in table size. Reads FOLD the
/// chain from the latest baseline forward (bounded by the compaction threshold
/// K, see [`ObjectStoreCatalog::part_segment_compact_every`]).
///
/// Reads UNION every partition's live data files (see `load_unioned`); the
/// table META manifest (the legacy `v{N}.json` chain) keeps schema/DDL/spec.
///
/// Single-owner-per-partition + the create-if-absent CAS on `version` make the
/// chain race-free: only the owning writer ever appends to a partition's chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartSegmentObject {
    /// Monotonic per-partition version (`M` in `parts/{pid}/v{M}.json`).
    version: u64,
    /// This partition's OCC token (advances on every data-file commit here).
    current_snapshot: SnapshotId,
    /// The single snapshot (delta) committed at THIS version: its `data_files`
    /// are the files ADDED here and `removed_paths` the files removed here. Even
    /// for a baseline object this carries the OCC bookkeeping (id/parent/time).
    delta: Snapshot,
    /// For a DELTA object: the version of the prior object to fold from
    /// (`version - 1`). `None` marks a BASELINE/genesis object that needs no
    /// predecessor — its `baseline` holds the full live set.
    base_version: Option<u64>,
    /// For a BASELINE object (genesis or a compaction consolidation): the FULL
    /// live data-file set as of this version. `None` for a pure delta object.
    ///
    /// LEGACY inline baseline. Kept for BACKWARD COMPATIBILITY: objects written
    /// before the #27 chunked-baseline fix carry the entire live set inline here,
    /// and they must still fold/commit correctly. New baselines instead populate
    /// `chunk_baseline` (content-addressed chunk refs) so a baseline PUT is
    /// O(files-added-since-last-baseline), not O(total-files). A baseline object
    /// sets EITHER `chunk_baseline` (new, default) OR `baseline` (legacy / the
    /// `BASIN_BASELINE_CHUNKING=0` escape hatch). Genesis is a baseline too.
    #[serde(default)]
    baseline: Option<Vec<DataFileRef>>,
    /// For a CHUNKED BASELINE object (the #27 flat-scale format): the live set is
    /// the union of the referenced immutable chunk objects MINUS `tombstones`.
    /// `None` for a delta object or a legacy inline baseline. See
    /// [`ChunkedBaseline`].
    #[serde(default)]
    chunk_baseline: Option<ChunkedBaseline>,
}

/// The #27 chunked-baseline descriptor carried by a BASELINE segment object.
///
/// Instead of serialising the entire live `Vec<DataFileRef>` on every baseline
/// (O(total-files), the observed ingest decay), a baseline references a list of
/// IMMUTABLE, content-addressed chunk objects plus a tombstone set. Each chunk
/// object is `Vec<DataFileRef>` stored at `{parts_root}{pid}/chunks/{hash}.json`
/// and is written exactly once (create-if-absent, content-addressed → idempotent
/// and ambiguous-PUT-safe). A steady-state baseline seals only the files added
/// since the previous baseline into ONE new chunk and REUSES every prior chunk
/// ref, so the baseline PUT bytes are O(files-added-since-last-baseline) =
/// bounded, independent of total table size.
///
/// Reconstruction: live = (⋃ chunk.files) − tombstones, then deltas on top.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct ChunkedBaseline {
    /// Ordered list of referenced chunk objects (by content hash). The FIRST
    /// `frozen` entries are immutable, ~TARGET-file chunks reused verbatim on
    /// every later baseline; any trailing entry (at most one) is the GROWING
    /// open-tail chunk that gets re-sealed (bigger) on the next baseline until
    /// it reaches TARGET and itself becomes frozen.
    chunks: Vec<BaselineChunkRef>,
    /// Count of leading `chunks` that are FROZEN (immutable, full). The
    /// remaining `chunks[frozen..]` (0 or 1 entry) is the open tail. Stored
    /// explicitly so a fold reconstructs the frozen/tail split exactly even if
    /// `TARGET_CHUNK_FILES` changed across restarts.
    #[serde(default)]
    frozen: u32,
    /// Paths present in some referenced chunk that have since been removed from
    /// the live set (a `Replace` removed a file already sealed into a chunk).
    /// Subtracted from the chunk union when reconstructing the baseline.
    tombstones: Vec<String>,
}

/// A reference to one immutable, content-addressed baseline chunk object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BaselineChunkRef {
    /// Content hash of the canonical serialised `Vec<DataFileRef>` bytes; also
    /// the chunk object's key suffix (`chunks/{hash}.json`). Identical content →
    /// identical hash → create-if-absent dedupes/reuses.
    hash: String,
    /// Number of files sealed into this chunk (diagnostic / re-chunk sizing).
    file_count: u32,
}

/// A partition's FOLDED current state: its OCC token, version, and the full
/// live data-file set produced by folding the delta chain from the latest
/// baseline forward. This is what `load_part_current` returns and what every
/// reader consumes; it preserves the old `PartitionSegment` read API
/// (`current_snapshot` / `live_data_files()`) so callers are unchanged.
#[derive(Clone, Debug)]
struct PartitionLive {
    /// The HEAD version this view was folded at (diagnostic / parity with the
    /// cached segment version).
    #[allow(dead_code)]
    version: u64,
    current_snapshot: SnapshotId,
    /// The folded live set, keyed by file path, as a PERSISTENT map so a
    /// commit derives the next version by `insert`/`remove` on the delta only
    /// (O(delta·log n) with structural sharing) instead of cloning the whole
    /// Vec each commit. In-memory only — never serialized (the on-disk format
    /// is `PartSegmentObject.baseline: Vec<DataFileRef>`, unchanged).
    live: rpds::HashTrieMapSync<String, DataFileRef>,
    /// Latest commit time across the folded chain (used to stamp unioned reads).
    latest_committed_at: DateTime<Utc>,
    /// Number of delta objects applied on top of the latest baseline (i.e. the
    /// read fold depth). Used to decide when to write a fresh baseline.
    deltas_since_baseline: u64,
    /// #27 chunked-baseline structure carried forward incrementally so a baseline
    /// PUT is O(TARGET_CHUNK_FILES) (a bounded CONSTANT), never O(total-files):
    ///   * `frozen_chunks` — immutable, FULL chunk refs (~TARGET files each).
    ///     REUSED by hash on every later baseline; never re-sealed.
    ///   * `open_tail` — the paths in the single GROWING tail chunk (< TARGET
    ///     files). Each baseline re-seals `open_tail` (cost ≤ O(TARGET)) into a
    ///     fresh content-addressed chunk and references it; once it reaches
    ///     TARGET the tail is FROZEN (moved into `frozen_chunks`) and a new empty
    ///     tail begins. So the chunk-ref count a baseline carries is
    ///     `ceil(live / TARGET)` — bounded by table size / TARGET, and each
    ///     baseline's NEW bytes are at most one TARGET-sized chunk = constant.
    ///   * `tombstones` — paths sealed into some chunk that have since been
    ///     removed (subtracted at reconstruction). The re-chunk valve drains
    ///     these when they grow large.
    /// `open_tail` order is preserved (Vec) so re-sealing is deterministic and
    /// content-addressing dedupes a re-seal of the identical tail. All derived
    /// from the same chain the `live` map folds → a cold fold reproduces them.
    frozen_chunks: Vec<BaselineChunkRef>,
    open_tail: Vec<String>,
    tombstones: std::collections::HashSet<String>,
}

impl PartitionLive {
    fn genesis() -> Self {
        Self {
            version: 0,
            current_snapshot: SnapshotId::GENESIS,
            live: rpds::HashTrieMapSync::new_sync(),
            latest_committed_at: Utc::now(),
            deltas_since_baseline: 0,
            frozen_chunks: Vec::new(),
            open_tail: Vec::new(),
            tombstones: std::collections::HashSet::new(),
        }
    }

    fn live_data_files(&self) -> Vec<DataFileRef> {
        // Materialize the persistent map's values for callers that want a Vec.
        // Order is unspecified (set semantics) — same contract as before, when
        // this was a Vec collected from a HashMap.
        self.live.values().cloned().collect()
    }
}

/// One cached, resolved manifest plus the version it was read at.
#[derive(Clone)]
struct CacheEntry {
    version: u64,
    manifest: Arc<TableManifest>,
}

/// One cached per-partition FOLDED live view plus the version it was read at.
#[derive(Clone)]
struct PartCacheEntry {
    version: u64,
    segment: Arc<PartitionLive>,
}

/// One bounded-staleness read snapshot: a fully-resolved unioned
/// [`TableMetadata`] (the exact value `load_table_q` would return), the META
/// manifest version it was resolved against, and the instant it expires.
///
/// The metadata is internally consistent — it is the output of a single
/// `load_unioned` over one committed META manifest plus a coherent pass over
/// the partition segments — so it is never a torn mix. The META `meta_version`
/// gate means any DDL / schema-evolution / drop (all of which advance the META
/// head and run through `after_commit` / `invalidate`) forces a refresh
/// regardless of the TTL, so schema and table identity are never served stale.
/// Only the *data-file* live-set is allowed to lag, and only up to `expires_at`.
#[derive(Clone)]
struct ReadSnapshotEntry {
    /// META manifest version this snapshot was resolved against; a head move
    /// (DDL) invalidates the snapshot even before the TTL elapses.
    meta_version: u64,
    /// The resolved unioned metadata (a real committed view).
    meta: Arc<TableMetadata>,
    /// Monotonic deadline; past this the entry is stale and must be refreshed.
    expires_at: std::time::Instant,
}

/// Basin-native shared catalog backed by an object store.
pub struct ObjectStoreCatalog {
    store: Arc<dyn ObjectStore>,
    root: String,
    /// Monotonic mutation epoch for session-cache validation (same contract as
    /// `InMemoryCatalog::epoch`). Bumped on every successful mutation.
    epoch: AtomicU64,
    /// Per-`(project, schema.table)` resolved-manifest cache. Keyed by the
    /// fully-qualified `schema.table` string so the same bare name in two
    /// schemas (e.g. `public.users` vs `auth.users`) never shares a cache slot.
    cache: Mutex<HashMap<(ProjectId, String), CacheEntry>>,
    /// Per-`(project, schema.table, partition_id)` resolved-segment cache. The
    /// hot ingest path revalidates a partition's segment against its per-
    /// partition `HEAD`; a stale entry only ever costs an extra reload or a
    /// benign per-partition `CommitConflict` retry. Keyed separately from the
    /// META `cache` so DDL and data-file commits never invalidate each other.
    part_cache: Mutex<HashMap<(ProjectId, String, String), PartCacheEntry>>,
    /// Per-`(project, schema.table)` cache of the *resolved META manifest head
    /// version*. The table manifest head only advances on a manifest write
    /// (DDL / schema evolution / single-node META-chain append) — NEVER on a
    /// per-partition data-file commit (`append_data_files_in_partition`), which
    /// is the sustained-ingest hot path. Caching the resolved head lets a hot
    /// commit skip the `resolve_head_version` GET(HEAD)+HEAD(manifest) RTT pair
    /// it would otherwise pay just to confirm a head that did not move. Every
    /// manifest mutation updates this entry through `after_commit`, and every
    /// invalidation (DDL, lost-race reload) removes it through `invalidate`, so
    /// it is filled/cleared in lockstep with the `cache` manifest-body entry.
    /// A miss simply falls back to the authoritative store resolve — never a
    /// stale hit, because the populating writes are the same ones that move the
    /// manifest.
    meta_head_cache: Mutex<HashMap<(ProjectId, String), u64>>,
    /// Process-wide, content-addressed cache of immutable #27 baseline CHUNK
    /// objects, keyed by content hash. A baseline chunk object lives at
    /// `parts/{pid}/chunks/{hash}.json` and is IMMUTABLE + CONTENT-ADDRESSED:
    /// the hash IS its identity, so its bytes can be cached forever with zero
    /// correctness risk — a cache hit returns exactly the bytes the store holds.
    ///
    /// This is the read-latency win under sustained ingest. The per-partition
    /// folded-view `part_cache` is keyed by partition VERSION, which advances on
    /// EVERY commit, so a read concurrent with heavy ingest constantly MISSES it
    /// and cold-folds via `fold_part_chain` → `load_baseline_chunks`, issuing one
    /// GET per referenced chunk against a store already saturated with ingest
    /// PUTs. But successive baselines REUSE every frozen chunk verbatim (only the
    /// open tail re-seals), so those chunk GETs are repeated reads of the SAME
    /// immutable objects. Caching them by hash collapses the per-read GET count to
    /// just the (one) changed tail chunk, with NO staleness (immutable identity).
    ///
    /// Size-capped LRU so memory stays bounded regardless of table count/size;
    /// the worst case on a cold/evicted entry is the pre-existing store GET.
    chunk_cache: Mutex<LruCache<String, Arc<Vec<DataFileRef>>>>,
    /// #30 bounded-staleness read-snapshot cache, keyed by `(project,
    /// schema.table)`. Under sustained ingest the per-partition VERSION advances
    /// on EVERY commit, so the version-keyed `part_cache` misses on every read
    /// and each `load_table` re-pays a `list_partition_ids` LIST plus, per
    /// partition, a `resolve_part_head_version` (GET HEAD + HEAD seg + HEAD
    /// seg+1) and a fold — object-store round-trips that contend with the heavy
    /// PUT traffic and push `count(*)`/`max` to multiple seconds.
    ///
    /// This caches the fully-resolved unioned [`TableMetadata`] for a short TTL
    /// (`BASIN_READ_SNAPSHOT_TTL_MS`, default [`DEFAULT_READ_SNAPSHOT_TTL_MS`];
    /// `0` disables → exact-every-read legacy). A read within the TTL is served
    /// from here with ZERO LIST / HEAD / fold round-trips. Correctness: the
    /// cached metadata is a real committed view (never torn), the META-version
    /// gate forces a refresh on any DDL before the TTL elapses, and a quiet
    /// table converges to exact within one tiny TTL. A `count(*)` answered from
    /// a ≤TTL-old snapshot is correct metadata-read semantics — it already
    /// excludes the uncompacted WAL tail, and the quiesce-drain converges once
    /// ingest idles.
    read_snapshot_cache: Mutex<HashMap<(ProjectId, String), ReadSnapshotEntry>>,
    /// Per-catalog override for the read-snapshot TTL in ms (bypasses
    /// `BASIN_READ_SNAPSHOT_TTL_MS`, for deterministic tests that must not race
    /// on the process-global env var). `None` = read the env / default.
    read_snapshot_ttl_override: Option<u64>,
    /// Per-instance ("session") sequence state: the locally-reserved block and
    /// the last value handed out by *this* node. Durable disjointness across
    /// nodes comes from the persisted high-water mark (see the module docs on
    /// sequences); this map only tracks the in-memory cursor inside the block
    /// this node reserved, plus the `currval` session value. Never shared
    /// across nodes — two `ObjectStoreCatalog` instances over one store keep
    /// independent maps and reserve disjoint blocks.
    seq_local: Mutex<HashMap<(ProjectId, String), SeqLocal>>,
    /// Explicit sequence-block-size override. `None` means "read
    /// `BASIN_SEQ_BLOCK` (default 64)"; `Some(n)` pins it (used by tests to
    /// avoid racing on a shared process env var, and available to callers that
    /// want a per-catalog block size). See [`ObjectStoreCatalog::seq_block`].
    seq_block_override: Option<u64>,
    /// Explicit per-partition segment-compaction threshold override. `None`
    /// means "read `BASIN_PART_SEGMENT_COMPACT_EVERY` (default 32)"; `Some(k)`
    /// pins it (used by tests to avoid racing on a shared process env var). See
    /// [`ObjectStoreCatalog::part_segment_compact_every`].
    part_compact_override: Option<u64>,
    /// #27 chunked-baseline test overrides (avoid racing on process env vars
    /// across parallel tests). `None` = read the corresponding `BASIN_*` env /
    /// default. `(enabled, chunk_files, chunk_cap)`.
    baseline_chunking_override: Option<bool>,
    baseline_chunk_files_override: Option<u64>,
    baseline_chunk_cap_override: Option<u64>,
}

/// Node-local cursor over a reserved sequence block. `next` is the value the
/// next `nextval` will hand out; once `next` would step past `block_last`
/// (in the increment's direction) the node must reserve a fresh block by
/// CAS-advancing the persisted high-water mark. `last_returned` backs the
/// per-instance `currval`.
#[derive(Clone, Debug)]
struct SeqLocal {
    /// Inclusive: the values `[.. block_last]` (in increment direction) are
    /// reserved by this node and safe to hand out without touching the store.
    block_values: std::collections::VecDeque<i64>,
    /// Last value returned by `nextval` on this instance; `None` until the
    /// first `nextval` this session (drives `currval`'s not-advanced error).
    last_returned: Option<i64>,
}

impl ObjectStoreCatalog {
    /// Construct over `store` with the default `_catalog/` prefix.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_prefix(store, DEFAULT_CATALOG_PREFIX)
    }

    /// Construct over `store` with an explicit root prefix (must end with `/`
    /// for a clean key layout; a missing trailing slash is added).
    pub fn with_prefix(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        let mut root = prefix.to_string();
        if !root.is_empty() && !root.ends_with('/') {
            root.push('/');
        }
        Self {
            store,
            root,
            epoch: AtomicU64::new(0),
            cache: Mutex::new(HashMap::new()),
            part_cache: Mutex::new(HashMap::new()),
            meta_head_cache: Mutex::new(HashMap::new()),
            chunk_cache: Mutex::new(LruCache::new(chunk_cache_cap())),
            read_snapshot_cache: Mutex::new(HashMap::new()),
            read_snapshot_ttl_override: None,
            seq_local: Mutex::new(HashMap::new()),
            seq_block_override: None,
            part_compact_override: None,
            baseline_chunking_override: None,
            baseline_chunk_files_override: None,
            baseline_chunk_cap_override: None,
        }
    }

    /// Construct with the default `_catalog/` prefix and an explicit sequence
    /// block size (bypasses `BASIN_SEQ_BLOCK`). Mainly for tests that need a
    /// deterministic block size without touching the shared process env.
    #[cfg(test)]
    pub fn with_seq_block(store: Arc<dyn ObjectStore>, block: u64) -> Self {
        let mut c = Self::with_prefix(store, DEFAULT_CATALOG_PREFIX);
        c.seq_block_override = Some(block.max(1));
        c
    }

    /// Construct with an explicit per-partition segment-compaction threshold
    /// (bypasses `BASIN_PART_SEGMENT_COMPACT_EVERY`). Mainly for tests that need
    /// a deterministic K without touching the shared process env.
    #[cfg(test)]
    pub fn with_part_compact_every(store: Arc<dyn ObjectStore>, k: u64) -> Self {
        let mut c = Self::with_prefix(store, DEFAULT_CATALOG_PREFIX);
        c.part_compact_override = Some(k.max(1));
        c
    }

    /// Construct with explicit per-partition compaction K AND #27 chunked-baseline
    /// knobs (bypasses the `BASIN_BASELINE_*` env), so parallel tests don't race
    /// on process-global env vars.
    #[cfg(test)]
    fn with_chunk_config(
        store: Arc<dyn ObjectStore>,
        k: u64,
        chunking: bool,
        chunk_files: u64,
        chunk_cap: u64,
    ) -> Self {
        let mut c = Self::with_prefix(store, DEFAULT_CATALOG_PREFIX);
        c.part_compact_override = Some(k.max(1));
        c.baseline_chunking_override = Some(chunking);
        c.baseline_chunk_files_override = Some(chunk_files.max(1));
        c.baseline_chunk_cap_override = Some(chunk_cap.max(1));
        c
    }

    /// Construct with an explicit #30 read-snapshot TTL (ms), bypassing
    /// `BASIN_READ_SNAPSHOT_TTL_MS` so parallel tests don't race on the
    /// process-global env var. `0` disables the snapshot cache.
    #[cfg(test)]
    fn with_read_snapshot_ttl(store: Arc<dyn ObjectStore>, ttl_ms: u64) -> Self {
        let mut c = Self::with_prefix(store, DEFAULT_CATALOG_PREFIX);
        // Pin a huge compaction K so seeding stays in deltas (no baseline-write
        // noise on the partition chains during the test's measured reads).
        c.part_compact_override = Some(1_000_000);
        c.read_snapshot_ttl_override = Some(ttl_ms);
        c
    }

    #[inline]
    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, SeqCst);
    }

    fn table_dir(&self, project: &ProjectId, qtable: &QualifiedTableName) -> String {
        // Schema-qualified keying, mirroring `PostgresCatalog`'s
        // `(project, schema, table)` primary key: the same bare table name in
        // two different schemas (e.g. `public.users` vs `auth.users`) lands at
        // distinct manifest prefixes and never collides.
        format!(
            "{}{}/{}/{}/",
            self.root, project, qtable.schema, qtable.name
        )
    }

    fn manifest_key(&self, project: &ProjectId, qtable: &QualifiedTableName, version: u64) -> OsPath {
        OsPath::from(format!(
            "{}v{version:020}.json",
            self.table_dir(project, qtable)
        ))
    }

    fn head_key(&self, project: &ProjectId, qtable: &QualifiedTableName) -> OsPath {
        OsPath::from(format!("{}HEAD", self.table_dir(project, qtable)))
    }

    /// Sub-directory component for a partition-tree GENERATION.
    ///
    /// Generation `0` (genesis tables + every pre-generation manifest, via the
    /// serde default) maps to the EMPTY string so the layout stays byte-identical
    /// to the historical `parts/{pid}/…` keys — existing on-disk trees are read
    /// and written unchanged, no migration. Generations `>0` (written only by a
    /// same-name RECREATE) nest under `g{gen}/`, an isolated namespace a prior
    /// drop's purge (scoped to the dropped generation) can never enumerate.
    fn gen_subdir(generation: u64) -> String {
        if generation == 0 {
            String::new()
        } else {
            format!("g{generation}/")
        }
    }

    /// Directory holding one partition's data-file segment chain, for a given
    /// partition-tree generation.
    fn part_dir(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> String {
        format!(
            "{}parts/{}{}/",
            self.table_dir(project, qtable),
            Self::gen_subdir(generation),
            sanitize(partition_id)
        )
    }

    /// Prefix under which a table's partition segment dirs for ONE generation
    /// live. The drop-time purge lists exactly this prefix (the generation that
    /// was current at drop) so it cannot reach a later recreate's tree.
    fn parts_root(&self, project: &ProjectId, qtable: &QualifiedTableName, generation: u64) -> String {
        format!(
            "{}parts/{}",
            self.table_dir(project, qtable),
            Self::gen_subdir(generation)
        )
    }

    fn part_segment_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        version: u64,
    ) -> OsPath {
        OsPath::from(format!(
            "{}v{version:020}.json",
            self.part_dir(project, qtable, generation, partition_id)
        ))
    }

    fn part_head_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> OsPath {
        OsPath::from(format!("{}HEAD", self.part_dir(project, qtable, generation, partition_id)))
    }

    fn part_cache_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> (ProjectId, String, String) {
        // Fold the generation into the partition component so a recreated
        // same-name table (higher generation) can never serve a folded view
        // cached against the dropped generation's segment chain.
        (
            *project,
            qtable.to_string(),
            format!("{generation}/{}", sanitize(partition_id)),
        )
    }

    fn cache_key(&self, project: &ProjectId, qtable: &QualifiedTableName) -> (ProjectId, String) {
        (*project, qtable.to_string())
    }

    /// Resolve a bare [`TableName`] to a [`QualifiedTableName`], mirroring
    /// [`InMemoryCatalog::resolve_qtable`]: try the `public` schema first
    /// (fast path for the common case), then LIST across all schemas for a
    /// unique bare-name match (so system-schema tables like `auth.users` or
    /// `cron.job` resolve by bare name after DataFusion schema stripping). If
    /// not found in any schema — or ambiguous across schemas — fall back to
    /// `public` so the downstream lookup produces the expected NotFound error.
    async fn resolve_qtable(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> QualifiedTableName {
        let pub_qtable = QualifiedTableName::in_public(table.clone());
        // Fast path: a live `public` manifest exists. During sustained ingest the
        // table is overwhelmingly in `public` and its manifest head is already in
        // the META-head cache (filled on resolve/commit, cleared on DDL/drop), so
        // serve the existence check from cache with NO store round trip — the same
        // contract `load_current` relies on. A miss falls back to the
        // authoritative store resolve. (This is the table-head GET+HEAD pair the
        // hot commit would otherwise pay TWICE: once here in `resolve_qtable`,
        // again in `load_current`.)
        if self
            .meta_head_cache
            .lock()
            .await
            .contains_key(&self.cache_key(project, &pub_qtable))
        {
            return pub_qtable;
        }
        if self.resolve_head_version(project, &pub_qtable).await.ok().flatten().is_some() {
            return pub_qtable;
        }
        // Search non-public schemas for a table with this bare name.
        match self.list_schema_table_names(project).await {
            Ok(found) => {
                let mut candidate: Option<QualifiedTableName> = None;
                for qt in found {
                    if qt.name == *table {
                        if candidate.is_some() {
                            // Ambiguous across schemas — fall back to public.
                            return pub_qtable;
                        }
                        candidate = Some(qt);
                    }
                }
                candidate.unwrap_or(pub_qtable)
            }
            Err(_) => pub_qtable,
        }
    }

    /// Enumerate every `(schema, table)` directory present under
    /// `{root}{project}/`, regardless of tombstone state. Used by
    /// `resolve_qtable` and `list_tables`. Each returned name is built from the
    /// `{schema}/{table}` path segments.
    async fn list_schema_table_names(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<QualifiedTableName>> {
        use futures::StreamExt;
        // `OsPath` normalises away the trailing slash, so build the string
        // prefix ourselves and match against `meta.location.as_ref()`.
        let prefix_str = format!("{}{}/", self.root, project);
        let list_prefix = OsPath::from(prefix_str.clone());
        let mut stream = self.store.list(Some(&list_prefix));
        // Distinct (schema, table) pairs; BTreeSet gives deterministic order.
        let mut pairs: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        let trimmed = prefix_str.trim_end_matches('/');
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list tables", e))?;
            let key = meta.location.as_ref();
            // key = {root}{project}/{schema}/{table}/v{N}.json  (or /HEAD)
            // The project-scoped `_project/…` metadata keys are NOT table dirs;
            // skip them so they are never mistaken for a `{schema}/{table}`.
            let Some(rest) = key.strip_prefix(trimmed) else {
                continue;
            };
            let rest = rest.trim_start_matches('/');
            let mut segs = rest.split('/');
            let (Some(schema), Some(table)) = (segs.next(), segs.next()) else {
                continue;
            };
            if schema.is_empty() || table.is_empty() || schema == "_project" {
                continue;
            }
            pairs.insert((schema.to_string(), table.to_string()));
        }
        let mut out = Vec::with_capacity(pairs.len());
        for (schema, table) in pairs {
            let (Ok(schema), Ok(table)) = (SchemaName::new(schema), TableName::new(table)) else {
                continue;
            };
            out.push(QualifiedTableName::new(schema, table));
        }
        Ok(out)
    }

    /// Resolve the highest manifest version `N` for a table. Tries `HEAD`
    /// first (one GET), then LISTs the table dir and takes the max `v{N}`.
    /// Returns `None` if the table has no manifest at all.
    async fn resolve_head_version(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Option<u64>> {
        // HEAD fast path.
        match self.store.get(&self.head_key(project, qtable)).await {
            Ok(res) => {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        if let Ok(v) = s.trim().parse::<u64>() {
                            // Trust HEAD only if that manifest actually exists;
                            // a torn/stale HEAD falls through to the LIST scan.
                            if self
                                .store
                                .head(&self.manifest_key(project, qtable, v))
                                .await
                                .is_ok()
                            {
                                return Ok(Some(v));
                            }
                        }
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(storage_err("get HEAD", e)),
        }
        // LIST fallback: scan the table dir for the max v{N}.json.
        self.list_max_version(project, qtable).await
    }

    async fn list_max_version(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Option<u64>> {
        use futures::StreamExt;
        let prefix = OsPath::from(self.table_dir(project, qtable));
        let mut stream = self.store.list(Some(&prefix));
        let mut max: Option<u64> = None;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list manifests", e))?;
            let key = meta.location.as_ref();
            if let Some(file) = key.rsplit('/').next() {
                if let Some(num) = file.strip_prefix('v').and_then(|s| s.strip_suffix(".json")) {
                    if let Ok(v) = num.parse::<u64>() {
                        max = Some(max.map_or(v, |m| m.max(v)));
                    }
                }
            }
        }
        Ok(max)
    }

    async fn get_manifest(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        version: u64,
    ) -> Result<TableManifest> {
        let key = self.manifest_key(project, qtable, version);
        let res = self.store.get(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                BasinError::not_found(format!("{project}/{qtable}@v{version}"))
            }
            other => storage_err("get manifest", other),
        })?;
        let bytes = res.bytes().await.map_err(|e| storage_err("read manifest", e))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| BasinError::catalog(format!("decode manifest {project}/{qtable}: {e}")))
    }

    /// Load the current manifest (highest version), using the cache when its
    /// version matches the resolved HEAD. Returns `NotFound` when the table has
    /// no manifest or its latest manifest is a tombstone.
    async fn load_current(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<(u64, Arc<TableManifest>)> {
        let ck = self.cache_key(project, qtable);
        // HOT-PATH FAST RESOLVE: the table manifest head moves only on a manifest
        // write (DDL / schema evolution / single-node META-chain append), all of
        // which run through `after_commit` (fills both caches) or `invalidate`
        // (clears both). A per-partition data-file commit — the sustained-ingest
        // path — never touches the manifest, so during ingest the cached head is
        // exactly current. When BOTH the meta-head version AND the manifest body
        // at that version are cached, serve them with zero store round-trips,
        // skipping the `resolve_head_version` GET(HEAD)+HEAD(manifest) pair. The
        // two caches are written together, so a head hit always has a body hit at
        // the same version; if either is absent we fall through to the
        // authoritative store resolve below. This is a strict subset of the
        // existing version-pinned cache contract: it can only ever return a
        // manifest the store resolve would also have returned (same version key).
        if let Some(version) = self.meta_head_cache.lock().await.get(&ck).copied() {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&ck) {
                if entry.version == version {
                    if entry.manifest.dropped {
                        return Err(BasinError::not_found(format!("{project}/{qtable}")));
                    }
                    return Ok((version, entry.manifest.clone()));
                }
            }
        }
        let version = self
            .resolve_head_version(project, qtable)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}/{qtable}")))?;
        // Record the freshly-resolved head so the next hot commit can skip the
        // resolve. Safe because every manifest mutation rewrites this entry
        // (after_commit) or clears it (invalidate).
        self.meta_head_cache.lock().await.insert(ck.clone(), version);
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&ck) {
                if entry.version == version {
                    if entry.manifest.dropped {
                        return Err(BasinError::not_found(format!("{project}/{qtable}")));
                    }
                    return Ok((version, entry.manifest.clone()));
                }
            }
        }
        let manifest = Arc::new(self.get_manifest(project, qtable, version).await?);
        {
            let mut cache = self.cache.lock().await;
            cache.insert(
                ck,
                CacheEntry {
                    version,
                    manifest: manifest.clone(),
                },
            );
        }
        if manifest.dropped {
            return Err(BasinError::not_found(format!("{project}/{qtable}")));
        }
        Ok((version, manifest))
    }

    /// Write manifest version `version` via create-if-absent. Returns `true` if
    /// we won the race, `false` on `AlreadyExists` (another node got there).
    async fn put_manifest_create(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        version: u64,
        manifest: &TableManifest,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|e| BasinError::catalog(format!("serialise manifest: {e}")))?;
        let key = self.manifest_key(project, qtable, version);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&key, Bytes::from(bytes).into(), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(storage_err("put manifest", e)),
        }
    }

    /// Best-effort HEAD pointer update + cache fill after a winning commit.
    async fn after_commit(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        version: u64,
        manifest: TableManifest,
    ) {
        // HEAD is an optimisation only; an Overwrite that loses to a concurrent
        // higher commit is fine — readers fall back to the LIST scan.
        let _ = self
            .store
            .put_opts(
                &self.head_key(project, qtable),
                Bytes::from(version.to_string()).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await;
        let ck = self.cache_key(project, qtable);
        // Keep the META-head cache in lockstep with the manifest-body cache: this
        // is a manifest write, so the head now points at `version`.
        self.meta_head_cache.lock().await.insert(ck.clone(), version);
        let mut cache = self.cache.lock().await;
        cache.insert(
            ck,
            CacheEntry {
                version,
                manifest: Arc::new(manifest),
            },
        );
        drop(cache);
        // This is a META manifest write (DDL, schema evolution, or a single-node
        // META-chain data-file append). The new `version` already invalidates
        // any read snapshot via the META-version gate, but drop it explicitly so
        // a META-chain commit converges to the new live set immediately rather
        // than after the TTL.
        self.invalidate_read_snapshot(project, qtable).await;
        self.bump_epoch();
    }

    /// Generic OCC-on-data-files commit shared by append + replace.
    ///
    /// `expected_snapshot` is the OCC token: the new snapshot is appended only
    /// if the current manifest's `current_snapshot` still equals it. A racing
    /// committer that already advanced the table → [`BasinError::CommitConflict`]
    /// (the engine retries). The create-if-absent loser on the manifest write
    /// is also surfaced as `CommitConflict`.
    /// Commit a data-file delta to the table META chain (the legacy single
    /// chain, retained for the non-partitioned back-compat path: the engine
    /// executor's single-node OLTP commits and other callers that don't carry a
    /// partition id). Sharded multi-node ingest uses `commit_part_snapshot`
    /// instead, which never touches this chain.
    ///
    /// The caller's `expected_snapshot` is the synthetic table-level union token
    /// (GENESIS / 1; see `load_unioned`), not the META chain's internal id — so
    /// OCC here is resolved INTERNALLY by read-modify-write against the META
    /// manifest's real `current_snapshot`, retrying the create-if-absent race
    /// transparently (bounded). `expected_snapshot` is therefore informational;
    /// per-write isolation in the multi-node path comes from per-partition OCC.
    async fn commit_snapshot(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        _expected_snapshot: SnapshotId,
        operation: SnapshotOperation,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let table = &qtable.name;
        for _ in 0..MAX_DDL_RETRIES {
            let (version, manifest) = self.load_current(project, qtable).await?;

            // For Replace, validate removed_paths are in the META chain's live
            // set (the back-compat path keeps all its data on the META chain).
            if operation == SnapshotOperation::Replace {
                let live: std::collections::HashSet<String> = manifest
                    .to_metadata(project, table)
                    .live_data_files()
                    .into_iter()
                    .map(|f| f.path)
                    .collect();
                for p in &removed_paths {
                    if !live.contains(p) {
                        return Err(BasinError::catalog(format!(
                            "{project}/{qtable}: replace_data_files removed path {p:?} not in live set"
                        )));
                    }
                }
            }

            let added_files_count = added_files.len() as u64;
            let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
            let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
            let removed_files = removed_paths.len() as u64;

            let parent = manifest.current_snapshot;
            let new_id = parent.next();
            let snap = Snapshot {
                id: new_id,
                parent: Some(parent),
                committed_at: Utc::now(),
                data_files: added_files.clone(),
                removed_paths: removed_paths.clone(),
                summary: SnapshotSummary {
                    operation,
                    added_files: added_files_count,
                    added_rows,
                    added_bytes,
                    removed_files,
                },
            };

            let mut next = (*manifest).clone();
            next.version = version + 1;
            next.current_snapshot = new_id;
            next.snapshots.push(snap);

            if self
                .put_manifest_create(project, qtable, next.version, &next)
                .await?
            {
                let committed = next.clone();
                self.after_commit(project, qtable, next.version, next).await;
                return self.load_unioned(project, qtable, &committed).await;
            }
            // Lost the create race: reload and retry transparently.
            self.invalidate(project, qtable).await;
        }
        Err(BasinError::CommitConflict(format!(
            "{project}/{qtable}: exhausted commit retries under contention"
        )))
    }

    async fn invalidate(&self, project: &ProjectId, qtable: &QualifiedTableName) {
        let ck = self.cache_key(project, qtable);
        // Clear the META-head cache alongside the manifest body so a stale head
        // can never outlive its manifest entry (they are always written and
        // cleared together).
        self.meta_head_cache.lock().await.remove(&ck);
        self.cache.lock().await.remove(&ck);
        // A META mutation (or lost-race reload) can change schema/identity; drop
        // the bounded-staleness read snapshot so the next read re-resolves
        // immediately rather than serving a pre-mutation view until the TTL.
        self.invalidate_read_snapshot(project, qtable).await;
    }

    // -----------------------------------------------------------------------
    // Per-partition data-file segments (the multi-writer scaling fix).
    //
    // Each partition owns its own monotonic segment chain under
    // `{table}/parts/{partition_id}/`. The owner of a partition CASes only ITS
    // chain, so concurrent writers on different partitions never contend. Reads
    // UNION every partition's live data files into one `TableMetadata`.
    // -----------------------------------------------------------------------

    /// Resolve the highest segment version `M` for one partition. HEAD fast
    /// path, then LIST fallback. `None` when the partition has no segment yet.
    async fn resolve_part_head_version(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> Result<Option<u64>> {
        match self.store.get(&self.part_head_key(project, qtable, generation, partition_id)).await {
            Ok(res) => {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        if let Ok(v) = s.trim().parse::<u64>() {
                            if self
                                .store
                                .head(&self.part_segment_key(project, qtable, generation, partition_id, v))
                                .await
                                .is_ok()
                            {
                                // The head pointer is only a HINT — its write
                                // (`after_part_commit`) is a best-effort overwrite
                                // that can be lost if the process dies or the store
                                // PUT fails right after a segment was created. If
                                // the pointer lags behind an already-committed
                                // higher segment, trusting it blindly makes every
                                // commit recompute the SAME `new_version` and lose
                                // the create-if-absent race against the existing
                                // object forever ("lost commit race at version N"
                                // livelock — the tail can never drain). Cheaply
                                // probe the very next version: if `v+1` does NOT
                                // exist (the overwhelming common case) the pointer
                                // is current, so return it with no LIST. If it DOES
                                // exist the pointer is stale — fall through to the
                                // LIST scan below to recover the true max version.
                                if self
                                    .store
                                    .head(&self.part_segment_key(project, qtable, generation, partition_id, v + 1))
                                    .await
                                    .is_err()
                                {
                                    return Ok(Some(v));
                                }
                            }
                        }
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(storage_err("get partition HEAD", e)),
        }
        // LIST fallback: max v{M}.json directly under the partition dir.
        use futures::StreamExt;
        let dir = self.part_dir(project, qtable, generation, partition_id);
        let prefix = OsPath::from(dir.clone());
        let trimmed = dir.trim_end_matches('/');
        let mut stream = self.store.list(Some(&prefix));
        let mut max: Option<u64> = None;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list partition segments", e))?;
            let key = meta.location.as_ref();
            // Only count segments DIRECTLY under this partition dir (the file
            // name is the only remaining path component after the prefix).
            let Some(rest) = key.strip_prefix(trimmed) else { continue };
            let rest = rest.trim_start_matches('/');
            if rest.contains('/') {
                continue;
            }
            if let Some(num) = rest.strip_prefix('v').and_then(|s| s.strip_suffix(".json")) {
                if let Ok(v) = num.parse::<u64>() {
                    max = Some(max.map_or(v, |m| m.max(v)));
                }
            }
        }
        Ok(max)
    }

    async fn get_part_segment(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        version: u64,
    ) -> Result<PartSegmentObject> {
        let key = self.part_segment_key(project, qtable, generation, partition_id, version);
        let res = self.store.get(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => BasinError::not_found(format!(
                "{project}/{qtable}/parts/{partition_id}@v{version}"
            )),
            other => storage_err("get partition segment", other),
        })?;
        let bytes = res.bytes().await.map_err(|e| storage_err("read partition segment", e))?;
        serde_json::from_slice(&bytes).map_err(|e| {
            BasinError::catalog(format!("decode partition segment {project}/{qtable}/{partition_id}: {e}"))
        })
    }

    /// Fold a partition's delta chain at `head_version` into its full live set.
    ///
    /// Reads the HEAD object, then walks back along `base_version` collecting
    /// delta objects until it hits a BASELINE (which carries the full live set
    /// at its version) or genesis. It then applies the collected deltas
    /// oldest-first on top of the baseline's live set. Cost is
    /// `O(baseline_size + deltas_since_baseline)` GETs of `≤ K + 1` objects,
    /// bounded by the compaction threshold K — NOT O(total commits).
    async fn fold_part_chain(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        head_version: u64,
    ) -> Result<PartitionLive> {
        // Collect delta objects from HEAD back to (and including) the baseline.
        let head = self
            .get_part_segment(project, qtable, generation, partition_id, head_version)
            .await?;
        let current_snapshot = head.current_snapshot;
        // Commits to a partition are monotonic (single owner), so the HEAD
        // object's commit time is the partition's latest.
        let latest_committed_at = head.delta.committed_at;
        let mut deltas: Vec<Snapshot> = Vec::new();
        let mut deltas_since_baseline: u64 = 0;
        let mut live: rpds::HashTrieMapSync<String, DataFileRef> =
            rpds::HashTrieMapSync::new_sync();

        // The baseline structure reconstructed at the baseline boundary, then
        // carried forward through the replayed deltas so the in-memory view can
        // seal the NEXT baseline in O(TARGET) without re-reading the chain.
        // `frozen_chunks` are immutable full chunk refs; `open_tail` is the
        // ordered list of currently-live paths in the growing tail chunk.
        let mut frozen_chunks: Vec<BaselineChunkRef> = Vec::new();
        let mut open_tail: Vec<String> = Vec::new();
        let mut tombstones: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Membership of `open_tail` for O(1) removal classification during replay.
        let mut tail_set: std::collections::HashSet<String> = std::collections::HashSet::new();

        let mut cur = head;
        loop {
            if let Some(cb) = cur.chunk_baseline.take() {
                // #27 chunked baseline: live = (⋃ chunk files) − tombstones.
                // Load every referenced chunk object (concurrently), union, and
                // reconstruct the frozen/tail split. The first `cb.frozen` refs
                // are immutable; a trailing ref (if any) is the open tail.
                let per_chunk = self
                    .load_baseline_chunks(project, qtable, generation, partition_id, &cb.chunks)
                    .await?;
                let tomb: std::collections::HashSet<&str> =
                    cb.tombstones.iter().map(String::as_str).collect();
                let frozen_n = (cb.frozen as usize).min(cb.chunks.len());
                for (idx, files) in per_chunk.into_iter().enumerate() {
                    let is_tail = idx >= frozen_n;
                    for f in files {
                        if tomb.contains(f.path.as_str()) {
                            continue;
                        }
                        if is_tail {
                            open_tail.push(f.path.clone());
                            tail_set.insert(f.path.clone());
                        }
                        live.insert_mut(f.path.clone(), f);
                    }
                }
                frozen_chunks = cb.chunks[..frozen_n].to_vec();
                tombstones = cb.tombstones.into_iter().collect();
                break;
            }
            if let Some(files) = cur.baseline.take() {
                // LEGACY inline baseline (or genesis): it carries the full live
                // set inline. Treat the whole set as the OPEN TAIL for the next
                // chunked baseline so a mixed chain (old inline baseline + new
                // deltas) seals correctly into chunks.
                for f in files {
                    open_tail.push(f.path.clone());
                    tail_set.insert(f.path.clone());
                    live.insert_mut(f.path.clone(), f);
                }
                break;
            }
            // A delta object: stash its change, follow the base pointer.
            deltas_since_baseline += 1;
            let base = cur.base_version.ok_or_else(|| {
                BasinError::catalog(format!(
                    "{project}/{qtable}[{partition_id}]: delta v{} has no base_version",
                    cur.version
                ))
            })?;
            deltas.push(cur.delta);
            cur = self
                .get_part_segment(project, qtable, generation, partition_id, base)
                .await?;
        }

        // Apply deltas oldest-first on top of the baseline (we pushed them
        // newest-first while walking back, so iterate in reverse). Maintain the
        // open-tail/tombstone bookkeeping in lockstep so the folded view matches
        // a freshly-committed warm view exactly.
        for snap in deltas.into_iter().rev() {
            for p in &snap.removed_paths {
                live.remove_mut(p);
                apply_removal_to_tail(p, &mut open_tail, &mut tail_set, &mut tombstones);
            }
            for f in &snap.data_files {
                live.insert_mut(f.path.clone(), f.clone());
                // A (re-)added path joins the open tail (it is not in any FROZEN
                // chunk's effective set); clear any tombstone for it.
                tombstones.remove(&f.path);
                if tail_set.insert(f.path.clone()) {
                    open_tail.push(f.path.clone());
                }
            }
        }

        Ok(PartitionLive {
            version: head_version,
            current_snapshot,
            live,
            latest_committed_at,
            deltas_since_baseline,
            frozen_chunks,
            open_tail,
            tombstones,
        })
    }

    /// Key for an immutable, content-addressed baseline chunk object.
    fn baseline_chunk_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        hash: &str,
    ) -> OsPath {
        OsPath::from(format!(
            "{}chunks/{hash}.json",
            self.part_dir(project, qtable, generation, partition_id)
        ))
    }

    /// Load every referenced baseline chunk object, returning their file lists
    /// IN THE SAME ORDER as `refs` (so the caller can split frozen vs tail by
    /// index). Loads concurrently. Chunk objects are immutable + content-
    /// addressed, so a missing chunk means a corrupt/torn baseline; we error
    /// CLEARLY (never silently lose files).
    async fn load_baseline_chunks(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        refs: &[BaselineChunkRef],
    ) -> Result<Vec<Vec<DataFileRef>>> {
        use futures::stream::{FuturesUnordered, StreamExt};

        // Slots filled either from the process-wide content-addressed chunk cache
        // (zero store reads) or by GETting the misses below. Chunk objects are
        // immutable + content-addressed, so a cache hit on `hash` is ALWAYS the
        // exact bytes the store holds — no version/staleness reasoning needed.
        let mut cached: Vec<Option<Arc<Vec<DataFileRef>>>> = (0..refs.len()).map(|_| None).collect();
        {
            let mut cache = self.chunk_cache.lock().await;
            for (idx, r) in refs.iter().enumerate() {
                if let Some(hit) = cache.get(&r.hash) {
                    cached[idx] = Some(hit.clone());
                }
            }
        }

        let mut futs = FuturesUnordered::new();
        for (idx, r) in refs.iter().enumerate() {
            if cached[idx].is_some() {
                continue; // served from the immutable-chunk cache; no store read.
            }
            let key = self.baseline_chunk_key(project, qtable, generation, partition_id, &r.hash);
            let store = self.store.clone();
            let hash = r.hash.clone();
            futs.push(async move {
                let res = store.get(&key).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => BasinError::catalog(format!(
                        "baseline chunk {hash} missing for partition (torn baseline; refusing to fold a baseline that references a missing chunk)"
                    )),
                    other => storage_err("get baseline chunk", other),
                })?;
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| storage_err("read baseline chunk", e))?;
                let files: Vec<DataFileRef> = serde_json::from_slice(&bytes)
                    .map_err(|e| BasinError::catalog(format!("decode baseline chunk {hash}: {e}")))?;
                Ok::<(usize, String, Vec<DataFileRef>), BasinError>((idx, hash, files))
            });
        }
        let mut fetched: Vec<(usize, String, Arc<Vec<DataFileRef>>)> = Vec::new();
        while let Some(item) = futs.next().await {
            let (idx, hash, files) = item?;
            fetched.push((idx, hash, Arc::new(files)));
        }
        // Populate the cache with the freshly-fetched (immutable) chunks.
        if !fetched.is_empty() {
            let mut cache = self.chunk_cache.lock().await;
            for (idx, hash, files) in &fetched {
                cache.put(hash.clone(), files.clone());
                cached[*idx] = Some(files.clone());
            }
        }

        Ok(cached
            .into_iter()
            .map(|s| s.map(|a| (*a).clone()).unwrap_or_default())
            .collect())
    }

    /// Content hash of a chunk's canonical serialised bytes (hex SHA-256). The
    /// chunk's serialised `Vec<DataFileRef>` is itself the canonical form, so two
    /// chunks with identical file sets in identical order hash identically and
    /// dedupe via create-if-absent.
    fn chunk_hash(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(bytes);
        let d = h.finalize();
        let mut s = String::with_capacity(d.len() * 2);
        for b in d {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Seal `files` into ONE immutable, content-addressed chunk object via
    /// create-if-absent and return its ref. Idempotent + ambiguous-PUT-safe:
    /// identical content → identical hash/key → `AlreadyExists` is a benign
    /// no-op (the bytes already there ARE our bytes). MUST complete (chunk
    /// confirmed durable) BEFORE the referencing segment object is written, so a
    /// crash never leaves a baseline pointing at a missing chunk.
    async fn seal_baseline_chunk(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        files: &[DataFileRef],
    ) -> Result<BaselineChunkRef> {
        let bytes = serde_json::to_vec(files)
            .map_err(|e| BasinError::catalog(format!("serialise baseline chunk: {e}")))?;
        let hash = Self::chunk_hash(&bytes);
        let key = self.baseline_chunk_key(project, qtable, generation, partition_id, &hash);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, Bytes::from(bytes).into(), opts)
            .await
        {
            // Wrote it, or it already existed with the SAME content (content-
            // addressed): either way the chunk is durable.
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(e) => {
                // Ambiguous PUT: confirm the chunk is present before referencing
                // it. Content-addressed, so presence = our exact bytes.
                match self.store.head(&key).await {
                    Ok(_) => {}
                    Err(object_store::Error::NotFound { .. }) => {
                        return Err(storage_err("put baseline chunk", e));
                    }
                    Err(he) => return Err(storage_err("verify baseline chunk after ambiguous put", he)),
                }
            }
        }
        Ok(BaselineChunkRef {
            hash,
            file_count: files.len() as u32,
        })
    }

    /// Load a partition's current FOLDED live view (highest version), via cache
    /// when the cached version matches HEAD. Returns a genesis view (version 0)
    /// when the partition has no segment yet, so a first append starts cleanly.
    async fn load_part_current(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> Result<(u64, Arc<PartitionLive>)> {
        let Some(version) = self
            .resolve_part_head_version(project, qtable, generation, partition_id)
            .await?
        else {
            return Ok((0, Arc::new(PartitionLive::genesis())));
        };
        let ck = self.part_cache_key(project, qtable, generation, partition_id);
        {
            let cache = self.part_cache.lock().await;
            if let Some(entry) = cache.get(&ck) {
                if entry.version == version {
                    return Ok((version, entry.segment.clone()));
                }
            }
        }
        let live = Arc::new(
            self.fold_part_chain(project, qtable, generation, partition_id, version)
                .await?,
        );
        let mut cache = self.part_cache.lock().await;
        cache.insert(
            ck,
            PartCacheEntry {
                version,
                segment: live.clone(),
            },
        );
        Ok((version, live))
    }

    /// Hot-path partition resolve that AVOIDS the redundant store round-trip.
    ///
    /// The commit hot path resolves a partition's head TWICE per commit today:
    /// once in the caller's `current_snapshot_id_in_partition` (which folds and
    /// caches `(version, PartitionLive)` in `part_cache`), then AGAIN inside
    /// `commit_part_snapshot` via `load_part_current`. The second resolve issues
    /// `resolve_part_head_version`'s GET(HEAD)+HEAD(seg)+HEAD(seg+1) probe purely
    /// to re-confirm a head the caller just read — pure latency waste on the
    /// PUT-budget-idle, RTT-bound commit path.
    ///
    /// This serves the already-folded `(version, segment)` straight from
    /// `part_cache` with ZERO store reads when present. On a miss it falls back
    /// to the authoritative `load_part_current` (full resolve). Correctness does
    /// NOT depend on the cached state being current: the caller's
    /// `expected_snapshot` check and, decisively, the create-if-absent
    /// `put_part_segment_create` CAS remain the sole authoritative arbiters. A
    /// stale cache can only make us compute a `new_version` that already exists,
    /// LOSE the create-if-absent race, and surface `CommitConflict` — which the
    /// engine's `commit_with_retry` re-resolves and retries, exactly as today.
    /// It can never produce a double-commit or drop a write.
    async fn load_part_current_cached(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) -> Result<(u64, Arc<PartitionLive>)> {
        let ck = self.part_cache_key(project, qtable, generation, partition_id);
        if let Some(entry) = self.part_cache.lock().await.get(&ck) {
            return Ok((entry.version, entry.segment.clone()));
        }
        // Cold cache (first touch / post-invalidation): authoritative resolve.
        self.load_part_current(project, qtable, generation, partition_id).await
    }

    /// Write partition segment `version` via create-if-absent. `true` = won.
    ///
    /// AMBIGUOUS-PUT SAFETY (exactly-once under a flaky object store): a PUT can
    /// LAND on the store yet surface a non-`AlreadyExists` error to us — e.g. a
    /// `408 Request Timeout` (Tigris throttling under sustained load) whose
    /// response we never see, or one that exhausts the `object_store` retry
    /// budget. `object_store` retries a 408 transparently; the retried Create
    /// then hits the just-landed object and returns `AlreadyExists`, but if the
    /// budget is exhausted first it returns the raw timeout error. Treating that
    /// error as a hard failure is UNSAFE: the caller (`compact_one`) leaves the
    /// WAL untruncated and the next tick re-flushes the SAME rows into a NEW
    /// data file (a fresh ULID path) committed on top of our already-landed
    /// segment — so the rows are referenced TWICE and `count(*)` over-reports.
    ///
    /// To make the create idempotent we RESOLVE the ambiguity: on a store error
    /// we read back the object at `version`. The segment object is fully
    /// self-describing (its `version`, `current_snapshot`, and `delta` encode
    /// exactly this commit's intent), so:
    ///   * absent              → the PUT genuinely did not land → propagate the
    ///                            error so the caller retries safely;
    ///   * present == our bytes → OUR write landed (ambiguous PUT) → `Ok(true)`,
    ///                            converging exactly-once with no re-flush;
    ///   * present != our bytes → another writer won the version → `Ok(false)`,
    ///                            identical to the `AlreadyExists` CAS-loss path.
    /// Single-owner-per-partition means a byte-identical object at our version
    /// can only be our own landed write, so this never mistakes a peer's commit
    /// for ours. The create-if-absent CAS stays the authoritative arbiter; this
    /// only disambiguates an error whose outcome the store left uncertain.
    async fn put_part_segment_create(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        version: u64,
        segment: &PartSegmentObject,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(segment)
            .map_err(|e| BasinError::catalog(format!("serialise partition segment: {e}")))?;
        let key = self.part_segment_key(project, qtable, generation, partition_id, version);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, Bytes::from(bytes.clone()).into(), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => {
                // Ambiguous PUT: disambiguate by reading back the object.
                match self.store.get(&key).await {
                    Ok(res) => {
                        let landed = res
                            .bytes()
                            .await
                            .map_err(|ge| storage_err("read partition segment after ambiguous put", ge))?;
                        if landed.as_ref() == bytes.as_slice() {
                            // Our write landed despite the error — converge.
                            Ok(true)
                        } else {
                            // A different object occupies this version: lost the
                            // create-if-absent race, same as `AlreadyExists`.
                            Ok(false)
                        }
                    }
                    // Nothing landed: the PUT truly failed — surface the original
                    // error so the caller retries (never silently drops a write).
                    Err(object_store::Error::NotFound { .. }) => {
                        Err(storage_err("put partition segment", e))
                    }
                    Err(ge) => Err(storage_err("verify partition segment after ambiguous put", ge)),
                }
            }
        }
    }

    async fn after_part_commit(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
        version: u64,
        live: PartitionLive,
    ) {
        // The head pointer is a best-effort hint, but a LOST write here is what
        // lets the pointer lag behind an already-committed segment and (without
        // the self-healing probe in `resolve_part_head_version`) wedges the
        // partition in a "lost commit race" livelock. On flaky object stores a
        // single PUT can transiently fail, so retry a few times with a short
        // backoff and `warn!` if it never lands — the resolver recovers the true
        // version via LIST regardless, but a persistently stale pointer forces an
        // O(segments) LIST on every resolve, so we want it observed, not silent.
        let head_key = self.part_head_key(project, qtable, generation, partition_id);
        let mut head_written = false;
        for attempt in 0..3u32 {
            match self
                .store
                .put_opts(
                    &head_key,
                    Bytes::from(version.to_string()).into(),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    head_written = true;
                    break;
                }
                Err(e) => {
                    if attempt + 1 < 3 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            20u64 << attempt,
                        ))
                        .await;
                    } else {
                        tracing::warn!(
                            %project, %qtable, partition_id, version, error = %e,
                            "partition head-pointer write failed after retries; \
                             resolver will recover via LIST (extra cost until repaired)"
                        );
                    }
                }
            }
        }
        let _ = head_written;
        let ck = self.part_cache_key(project, qtable, generation, partition_id);
        self.part_cache.lock().await.insert(
            ck,
            PartCacheEntry {
                version,
                segment: Arc::new(live),
            },
        );
        self.bump_epoch();
    }

    async fn invalidate_part(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
        partition_id: &str,
    ) {
        let ck = self.part_cache_key(project, qtable, generation, partition_id);
        self.part_cache.lock().await.remove(&ck);
    }

    /// Drop every `part_cache` entry belonging to `(project, qtable)`,
    /// regardless of partition. The folded `PartitionLive` views the entries
    /// hold are pinned to a segment-chain version; once that chain is purged
    /// (see [`Self::purge_part_segments`]) any cached fold is stale and MUST be
    /// evicted so a recreated same-name table never serves the old live set.
    async fn invalidate_all_parts(&self, project: &ProjectId, qtable: &QualifiedTableName) {
        let qname = qtable.to_string();
        let mut cache = self.part_cache.lock().await;
        cache.retain(|(p, q, _pid), _| !(*p == *project && *q == qname));
    }

    /// Hard-delete every per-partition segment object under ONE GENERATION of
    /// the table's `parts/` tree (`parts/{g}/{pid}/v*.json` + `HEAD` + #27 chunk
    /// objects).
    ///
    /// These segment objects are the catalog's record of partition-sharded
    /// data files and are NOT part of the META manifest chain — so the
    /// drop-tombstone on the manifest leaves them intact. Because a table's
    /// object-store prefix is keyed only on `(project, schema, name)`, a CREATE
    /// TABLE of the SAME name reuses this exact prefix; a surviving segment would
    /// then be re-resolved by `load_unioned` and re-counted (its `row_count` is
    /// summed by the metadata fast-aggregate path even though the underlying data
    /// bytes were already purged by the engine's DROP-time `delete_table_prefix`,
    /// so a bare `count(*)` would over-report rows a scan can no longer see).
    /// Purging the tree here makes the recreated same-name table resolve to an
    /// EMPTY live set.
    ///
    /// GENERATION SCOPING (drop-purge-vs-recreate race fix): the purge is bound
    /// to `generation` — the generation that was current at DROP time. A same-
    /// name recreate stamps a FRESH higher generation (`create_table_q`), whose
    /// segments live under a distinct `parts/g{n}/…` prefix that this purge can
    /// NEVER enumerate. So a lagging/concurrent drop purge can no longer delete a
    /// successor table's segments (which previously left a delta whose baseline
    /// was purged → torn chain → a project-wide session-open FATAL). For the
    /// genesis layout (`generation == 0`, prefix `…/parts/`), the LIST would also
    /// return higher-generation keys under `parts/g{n}/…`; those are filtered out
    /// here so generation-0 purge stays scoped to the un-prefixed segments only.
    ///
    /// Best-effort, like the engine's object-store purge: a delete failure
    /// leaves reclaimable bytes but never corrupts the (already-tombstoned)
    /// catalog. List/delete is O(segment objects under the prefix).
    async fn purge_part_segments(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
    ) {
        use futures::StreamExt;
        let root = self.parts_root(project, qtable, generation);
        let prefix = OsPath::from(root.clone());
        // `gen == 0` lists `…/parts/`, which ALSO returns higher-generation keys
        // (`…/parts/g{n}/…`). Keep only keys whose first path component after the
        // prefix is NOT a `g{n}` generation dir, so a genesis-generation purge
        // never reaches a recreate's tree. For `gen > 0` the prefix is exact and
        // isolated, so every listed key belongs to this generation.
        let trimmed = root.trim_end_matches('/');
        let mut stream = self.store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => {
                    if generation == 0 {
                        let key = meta.location.as_ref();
                        if let Some(rest) = key.strip_prefix(trimmed) {
                            let rest = rest.trim_start_matches('/');
                            if let Some(seg) = rest.split('/').next() {
                                // A `g<digits>/` first component is a higher
                                // generation's subtree — leave it untouched.
                                if seg.starts_with('g')
                                    && seg.len() > 1
                                    && seg[1..].chars().all(|c| c.is_ascii_digit())
                                {
                                    continue;
                                }
                            }
                        }
                    }
                    keys.push(meta.location);
                }
                Err(_) => {
                    // A transient list error leaves the tree in place; the
                    // tombstone already makes the table unresolvable, and a
                    // later drop/recreate retries the purge.
                    return;
                }
            }
        }
        for k in keys {
            let _ = self.store.delete(&k).await;
        }
    }

    /// Commit a data-file delta into ONE partition's segment chain. OCC is the
    /// PARTITION's own `current_snapshot`. Loser of the per-partition CAS gets
    /// `CommitConflict` (engine reloads + retries) — but only the same partition
    /// can ever race it, never another partition (the whole point).
    async fn commit_part_snapshot(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
        expected_snapshot: SnapshotId,
        operation: SnapshotOperation,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        // The table META manifest must exist (schema/DDL). NotFound if absent.
        // Served from the META-head cache during ingest (no store RTT) — the
        // manifest head never moves on a data-file commit.
        let (_mv, manifest) = self.load_current(project, qtable).await?;
        // This commit lands in the manifest's CURRENT partition generation, so a
        // prior drop's generation-scoped purge can never reach the segment we
        // write (or its baseline) here.
        let generation = manifest.parts_generation;
        // Resolve the partition's current segment from `part_cache` WITHOUT a
        // redundant store head-resolution when the caller already warmed it via
        // `current_snapshot_id_in_partition`. The `expected_snapshot` check and
        // the create-if-absent CAS below stay authoritative, so a stale cached
        // version simply loses the CAS and falls into the engine's retry.
        let (version, segment) = self
            .load_part_current_cached(project, qtable, generation, partition_id)
            .await?;

        if segment.current_snapshot != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{project}/{qtable}[{partition_id}]: expected snapshot {expected_snapshot}, current is {}",
                segment.current_snapshot
            )));
        }

        if operation == SnapshotOperation::Replace {
            let live: std::collections::HashSet<String> = segment
                .live_data_files()
                .into_iter()
                .map(|f| f.path)
                .collect();
            for p in &removed_paths {
                if !live.contains(p) {
                    return Err(BasinError::catalog(format!(
                        "{project}/{qtable}[{partition_id}]: replace_data_files removed path {p:?} not in partition live set"
                    )));
                }
            }
        }

        let added_files_count = added_files.len() as u64;
        let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
        let removed_files = removed_paths.len() as u64;

        let new_id = expected_snapshot.next();
        let new_version = version + 1;
        let snap = Snapshot {
            id: new_id,
            parent: Some(expected_snapshot),
            committed_at: Utc::now(),
            data_files: added_files.clone(),
            removed_paths: removed_paths.clone(),
            summary: SnapshotSummary {
                operation,
                added_files: added_files_count,
                added_rows,
                added_bytes,
                removed_files,
            },
        };

        // Compute the new folded live set incrementally from the prior folded
        // view + this delta — O(delta), no re-read of the chain AND no O(files)
        // clone of the prior set: cloning the persistent map is O(1) structural
        // sharing, and each removed/added path is an O(log n) update. This keeps
        // per-commit cost independent of the partition's existing file count.
        let new_live: rpds::HashTrieMapSync<String, DataFileRef> = {
            let mut live = segment.live.clone();
            for p in &removed_paths {
                live.remove_mut(p);
            }
            for f in &added_files {
                live.insert_mut(f.path.clone(), f.clone());
            }
            live
        };

        // #27 baseline-structure bookkeeping, advanced in O(Δ) from the prior
        // folded view. `frozen_chunks` are immutable full chunk refs (carried,
        // reused). `open_tail` is the ordered list of currently-live paths in the
        // growing tail chunk. A removal of a tail path drops it from the tail; a
        // removal of an already-frozen path becomes a tombstone. An (re-)added
        // path clears any tombstone and joins the tail.
        let frozen_chunks = segment.frozen_chunks.clone();
        let mut open_tail = segment.open_tail.clone();
        let mut tombstones = segment.tombstones.clone();
        let mut tail_set: std::collections::HashSet<String> = open_tail.iter().cloned().collect();
        for p in &removed_paths {
            apply_removal_to_tail(p, &mut open_tail, &mut tail_set, &mut tombstones);
        }
        for f in &added_files {
            tombstones.remove(&f.path);
            if tail_set.insert(f.path.clone()) {
                open_tail.push(f.path.clone());
            }
        }

        // SEGMENT COMPACTION: fold into a fresh BASELINE every K commits so the
        // read fold depth stays bounded by K. Otherwise write a small DELTA
        // object whose serialized size is O(this commit's files) — independent
        // of how many files the partition already holds. This is the flat-scale
        // property: per-commit PUT cost does not grow with table size.
        let compact_every = self.part_segment_compact_every();
        // `deltas_since_baseline` counts deltas already on top of the latest
        // baseline; this commit would make it `+1`. Snapshot when it reaches K.
        // The FIRST commit (no prior object at `version` to fold from) MUST be a
        // self-contained baseline — there is no predecessor to point a delta at.
        let write_baseline = version == 0 || segment.deltas_since_baseline + 1 >= compact_every;

        // Carried forward into the in-memory view after a successful commit.
        let mut next_frozen = frozen_chunks.clone();
        let mut next_open_tail = open_tail.clone();
        let mut next_tombstones = tombstones.clone();

        let obj = if write_baseline {
            if self.baseline_chunking_enabled() {
                let target = self.baseline_chunk_files();
                let chunk_cap = self.baseline_chunk_cap();
                let live_count = new_live.size();
                // RE-CHUNK SAFETY VALVE (the ONLY O(n) baseline path, RARE by
                // construction): when tombstones bloat the chunk union OR the
                // frozen-chunk count hits the cap, re-seal the ENTIRE live set
                // into fresh ~TARGET-sized chunks, dropping tombstoned files and
                // collapsing the chunk-ref list back down.
                let rechunk = (tombstones.len() * 4 > live_count.max(1))
                    || (frozen_chunks.len() as u64 >= chunk_cap);

                let cb = if rechunk {
                    let all: Vec<DataFileRef> = new_live.values().cloned().collect();
                    let mut fresh: Vec<BaselineChunkRef> = Vec::new();
                    for batch in all.chunks(target as usize) {
                        fresh.push(
                            self.seal_baseline_chunk(project, qtable, generation, partition_id, batch)
                                .await?,
                        );
                    }
                    // After a full re-chunk every chunk is ~TARGET and FROZEN;
                    // the open tail is empty (next appends start a new tail).
                    next_frozen = fresh.clone();
                    next_open_tail = Vec::new();
                    next_tombstones = std::collections::HashSet::new();
                    ChunkedBaseline {
                        frozen: fresh.len() as u32,
                        chunks: fresh,
                        tombstones: Vec::new(),
                    }
                } else {
                    // STEADY-STATE seal: (re-)seal ONLY the open tail (≤ TARGET
                    // files) into a fresh content-addressed chunk and reference
                    // it after the reused frozen chunks. New PUT bytes are
                    // O(tail) ≤ O(TARGET) = a bounded constant, independent of
                    // total table size. If the tail has reached TARGET it is
                    // FROZEN (kept) and a new empty tail begins.
                    let mut chunks = frozen_chunks.clone();
                    let mut frozen_n = frozen_chunks.len() as u32;
                    if !open_tail.is_empty() {
                        let tail_files: Vec<DataFileRef> = open_tail
                            .iter()
                            .filter_map(|p| new_live.get(p).cloned())
                            .collect();
                        let cref = self
                            .seal_baseline_chunk(project, qtable, generation, partition_id, &tail_files)
                            .await?;
                        let tail_full = tail_files.len() as u64 >= target;
                        chunks.push(cref.clone());
                        if tail_full {
                            // Freeze the tail: it joins the immutable set, a new
                            // empty tail starts next baseline.
                            next_frozen = chunks.clone();
                            next_open_tail = Vec::new();
                            frozen_n = chunks.len() as u32;
                        } else {
                            // Tail stays open (re-sealed bigger next baseline).
                            next_frozen = frozen_chunks.clone();
                            next_open_tail = open_tail.clone();
                            frozen_n = frozen_chunks.len() as u32;
                        }
                    } else {
                        next_frozen = frozen_chunks.clone();
                        next_open_tail = Vec::new();
                    }
                    next_tombstones = tombstones.clone();
                    ChunkedBaseline {
                        frozen: frozen_n,
                        chunks,
                        tombstones: tombstones.iter().cloned().collect(),
                    }
                };
                PartSegmentObject {
                    version: new_version,
                    current_snapshot: new_id,
                    delta: snap,
                    base_version: None,
                    baseline: None,
                    chunk_baseline: Some(cb),
                }
            } else {
                // Escape hatch (BASIN_BASELINE_CHUNKING=0): legacy inline
                // baseline — O(files), the pre-#27 behavior. The whole live set
                // becomes the open tail for a future (re-enabled) chunked seal.
                next_frozen = Vec::new();
                next_open_tail = new_live.keys().cloned().collect();
                next_tombstones = std::collections::HashSet::new();
                PartSegmentObject {
                    version: new_version,
                    current_snapshot: new_id,
                    delta: snap,
                    base_version: None,
                    baseline: Some(new_live.values().cloned().collect()),
                    chunk_baseline: None,
                }
            }
        } else {
            PartSegmentObject {
                version: new_version,
                current_snapshot: new_id,
                delta: snap,
                base_version: Some(version),
                baseline: None,
                chunk_baseline: None,
            }
        };

        let committed_at = obj.delta.committed_at;
        if self
            .put_part_segment_create(project, qtable, generation, partition_id, new_version, &obj)
            .await?
        {
            let next_live = PartitionLive {
                version: new_version,
                current_snapshot: new_id,
                live: new_live,
                latest_committed_at: committed_at,
                deltas_since_baseline: if write_baseline {
                    0
                } else {
                    segment.deltas_since_baseline + 1
                },
                frozen_chunks: next_frozen,
                open_tail: next_open_tail,
                tombstones: next_tombstones,
            };
            self.after_part_commit(project, qtable, generation, partition_id, new_version, next_live)
                .await;
            // FLAT-SCALE: do NOT build the unioned table metadata here. The
            // sustained-ingest hot path (`Shard::commit_with_retry`) discards
            // this return value entirely — it commits per partition and reads
            // back through `load_table`/`load_unioned` only when a query needs
            // the complete set. Calling `load_unioned` on every commit would
            // re-materialise EVERY live data file across EVERY partition
            // (O(total-files-in-table)) on each flush, an amortised-O(files)
            // cost that grows with table size — the dominant residual
            // ingest-rate slope at scale. Returning the cheap META manifest
            // metadata (its own live set only, never the per-partition union)
            // keeps the commit O(1) in table size. Readers still see the
            // complete unioned set via the read path; the meta-cache test
            // (`load_table_meta_cached_for_ingest`) explicitly requires that a
            // META load must NOT union per-partition files, so this is the
            // intended contract for the cheap commit return.
            Ok(manifest.to_metadata(project, &qtable.name))
        } else {
            self.invalidate_part(project, qtable, generation, partition_id).await;
            Err(BasinError::CommitConflict(format!(
                "{project}/{qtable}[{partition_id}]: lost commit race at partition version {new_version}"
            )))
        }
    }

    /// Number of delta objects allowed on top of a partition's latest baseline
    /// before a fresh consolidated baseline is written (segment compaction).
    /// Bounds read fold depth to `O(baseline + ≤K deltas)` while keeping every
    /// commit O(1). Override via `BASIN_PART_SEGMENT_COMPACT_EVERY` (clamped
    /// `>= 1`); defaults to 32.
    fn part_segment_compact_every(&self) -> u64 {
        if let Some(k) = self.part_compact_override {
            return k.max(1);
        }
        std::env::var("BASIN_PART_SEGMENT_COMPACT_EVERY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&k| k >= 1)
            .unwrap_or(32)
    }

    /// #30 bounded-staleness read-snapshot TTL. `0` disables the cache
    /// (exact-every-read legacy). Override via `BASIN_READ_SNAPSHOT_TTL_MS`;
    /// defaults to [`DEFAULT_READ_SNAPSHOT_TTL_MS`].
    fn read_snapshot_ttl_ms(&self) -> u64 {
        if let Some(ms) = self.read_snapshot_ttl_override {
            return ms;
        }
        std::env::var("BASIN_READ_SNAPSHOT_TTL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_READ_SNAPSHOT_TTL_MS)
    }

    /// Drop the bounded-staleness read snapshot for one table. Called whenever
    /// the table's META manifest mutates (DDL / drop / rename / fork) so a
    /// schema/identity change converges immediately rather than after the TTL.
    async fn invalidate_read_snapshot(&self, project: &ProjectId, qtable: &QualifiedTableName) {
        let ck = self.cache_key(project, qtable);
        self.read_snapshot_cache.lock().await.remove(&ck);
    }

    /// #27: whether to write CHUNKED (content-addressed, flat-scale) baselines.
    /// Default ON. `BASIN_BASELINE_CHUNKING=0` falls back to the legacy inline
    /// baseline (the pre-#27 O(files) PUT) — an escape hatch for safety, not a
    /// path the steady state should take.
    fn baseline_chunking_enabled(&self) -> bool {
        if let Some(b) = self.baseline_chunking_override {
            return b;
        }
        !matches!(
            std::env::var("BASIN_BASELINE_CHUNKING").ok().as_deref(),
            Some("0") | Some("off") | Some("false")
        )
    }

    /// #27: target maximum files sealed into one baseline chunk during a FULL
    /// re-chunk (the rare O(n) valve). Steady-state append seals exactly the
    /// pending set regardless of this. Override via `BASIN_BASELINE_CHUNK_FILES`
    /// (clamped `>= 1`); defaults to 1024.
    fn baseline_chunk_files(&self) -> u64 {
        if let Some(n) = self.baseline_chunk_files_override {
            return n.max(1);
        }
        std::env::var("BASIN_BASELINE_CHUNK_FILES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(1024)
    }

    /// #27: maximum number of chunk refs a baseline may carry before a FULL
    /// re-chunk consolidates them (bounds BOTH the segment-object PUT size —
    /// O(chunk_refs) — and the fold's chunk-GET fan-out). Each steady-state
    /// baseline appends ONE tiny chunk, so without this cap the chunk-ref list
    /// (and thus the segment PUT) would grow O(total_files / K). The cap forces
    /// a periodic consolidation into `ceil(live / TARGET_CHUNK_FILES)` large
    /// chunks, so the chunk count oscillates within `[ceil(live/TARGET), cap]`
    /// and the segment PUT stays bounded by `cap` refs regardless of table size.
    /// Override via `BASIN_BASELINE_CHUNK_CAP` (clamped `>= 1`); defaults to 64.
    fn baseline_chunk_cap(&self) -> u64 {
        if let Some(n) = self.baseline_chunk_cap_override {
            return n.max(1);
        }
        std::env::var("BASIN_BASELINE_CHUNK_CAP")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(64)
    }

    /// Enumerate every partition id that has at least one segment under
    /// `{table}/parts/`. Used by the unioned read.
    async fn list_partition_ids(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        generation: u64,
    ) -> Result<Vec<String>> {
        use futures::StreamExt;
        let root = self.parts_root(project, qtable, generation);
        let prefix = OsPath::from(root.clone());
        let trimmed = root.trim_end_matches('/');
        let mut stream = self.store.list(Some(&prefix));
        let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list partitions", e))?;
            let key = meta.location.as_ref();
            // key = {parts_root}{partition_id}/v{M}.json (or /HEAD).
            let Some(rest) = key.strip_prefix(trimmed) else { continue };
            let rest = rest.trim_start_matches('/');
            let Some(seg) = rest.split('/').next() else { continue };
            if seg.is_empty() {
                continue;
            }
            // `gen == 0` lists `…/parts/`, which ALSO returns higher-generation
            // keys under `…/parts/g{n}/…`; their first component is a `g{n}` dir,
            // never a real partition id — skip them so a genesis-generation read
            // never folds a recreate's partitions.
            if generation == 0
                && seg.starts_with('g')
                && seg.len() > 1
                && seg[1..].chars().all(|c| c.is_ascii_digit())
            {
                continue;
            }
            ids.insert(seg.to_string());
        }
        Ok(ids.into_iter().collect())
    }

    /// Copy every per-partition segment of `src` into `dst` as a fresh genesis
    /// segment (version 0) carrying the source partition's CURRENT live file
    /// set. Used by rename / fork so data committed through the partition path
    /// (sharded ingest) is preserved across the table-identity change. Files are
    /// shared by reference (no bytes copied), matching `fork`'s contract.
    async fn copy_partition_segments(
        &self,
        src_project: &ProjectId,
        src: &QualifiedTableName,
        dst_project: &ProjectId,
        dst: &QualifiedTableName,
    ) -> Result<()> {
        // Each side reads/writes its own current partition generation: a rename
        // (or fork) copies the live segments of the source generation into the
        // destination's generation, leaving any older dropped generations behind.
        let src_gen = self
            .load_current(src_project, src)
            .await?
            .1
            .parts_generation;
        let dst_gen = self
            .load_current(dst_project, dst)
            .await?
            .1
            .parts_generation;
        for pid in self.list_partition_ids(src_project, src, src_gen).await? {
            let (_v, segment) = self.load_part_current(src_project, src, src_gen, &pid).await?;
            let live = segment.live_data_files();
            if live.is_empty() {
                continue;
            }
            // Represent the source's live set as a fresh BASELINE segment in the
            // destination partition (a self-contained consolidated snapshot
            // carrying every live file). A baseline needs no predecessor, so the
            // dst chain starts clean.
            let added_files = live.len() as u64;
            let added_rows: u64 = live.iter().map(|f| f.row_count).sum();
            let added_bytes: u64 = live.iter().map(|f| f.size_bytes).sum();
            let make_baseline = |version: u64| PartSegmentObject {
                version,
                current_snapshot: SnapshotId(1),
                delta: Snapshot {
                    id: SnapshotId(1),
                    parent: Some(SnapshotId::GENESIS),
                    committed_at: Utc::now(),
                    data_files: live.clone(),
                    removed_paths: Vec::new(),
                    summary: SnapshotSummary {
                        operation: SnapshotOperation::Append,
                        added_files,
                        added_rows,
                        added_bytes,
                        removed_files: 0,
                    },
                },
                base_version: None,
                // Genesis copy keeps a legacy inline baseline (simplest valid
                // baseline the new fold reads directly); subsequent commits to
                // the dst partition switch to chunked baselines as usual.
                baseline: Some(live.clone()),
                chunk_baseline: None,
            };
            let mut obj = make_baseline(0);
            if !self
                .put_part_segment_create(dst_project, dst, dst_gen, &pid, 0, &obj)
                .await?
            {
                // Destination partition already has a segment — place at next.
                let v = self
                    .resolve_part_head_version(dst_project, dst, dst_gen, &pid)
                    .await?
                    .unwrap_or(0);
                obj = make_baseline(v + 1);
                let _ = self
                    .put_part_segment_create(dst_project, dst, dst_gen, &pid, obj.version, &obj)
                    .await?;
            }
            let fresh_live = PartitionLive {
                version: obj.version,
                current_snapshot: SnapshotId(1),
                // `live` here is the Vec from live_data_files(); fold it into
                // the persistent map for the in-memory cached view.
                live: live.iter().map(|f| (f.path.clone(), f.clone())).collect(),
                latest_committed_at: obj.delta.committed_at,
                deltas_since_baseline: 0,
                // Legacy inline baseline → the whole set is the OPEN TAIL for the
                // next (chunked) baseline, mirroring fold_part_chain's handling
                // of an inline baseline boundary.
                frozen_chunks: Vec::new(),
                open_tail: live.iter().map(|f| f.path.clone()).collect(),
                tombstones: std::collections::HashSet::new(),
            };
            self.after_part_commit(dst_project, dst, dst_gen, &pid, obj.version, fresh_live)
                .await;
        }
        Ok(())
    }

    /// Build the COMPLETE [`TableMetadata`] for a table: schema/DDL from the
    /// META `manifest`, plus a UNION of every partition's live data files.
    ///
    /// The unioned snapshot chain is the per-partition chains concatenated and
    /// **renumbered** into one contiguous id space (ordered by commit time),
    /// with `current_snapshot` set to the last. Because `removed_paths` are
    /// path-scoped and data-file paths are globally unique, the renumbered
    /// chain reduces (via `live_data_files`) to exactly the union of every
    /// partition's live set — no loss, no double-count. Time-travel to an
    /// arbitrary historical cut across partitions is NOT preserved (the synthetic
    /// ids are read-time-derived); `load_table_at_snapshot` falls back to a
    /// current read for any id it can't find, which is the documented behaviour.
    async fn load_unioned(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        manifest: &TableManifest,
    ) -> Result<TableMetadata> {
        let mut meta = manifest.to_metadata(project, &qtable.name);

        // Start the union with the META manifest's OWN live data files. The
        // non-partitioned `append_data_files`/`replace_data_files` (the engine
        // executor's single-node OLTP path, and other back-compat callers)
        // still commit to the META chain; sharded multi-node ingest commits to
        // the per-partition segments below. Both feed the same unioned read.
        let mut all_live: Vec<DataFileRef> = meta.live_data_files();

        let generation = manifest.parts_generation;
        let partition_ids = self.list_partition_ids(project, qtable, generation).await?;
        // Track the latest commit time across partitions to stamp the union.
        let mut latest_commit = manifest
            .snapshots
            .iter()
            .map(|s| s.committed_at)
            .max()
            .unwrap_or_else(Utc::now);
        for pid in &partition_ids {
            let (_v, segment) = self.load_part_current(project, qtable, generation, pid).await?;
            for f in segment.live_data_files() {
                all_live.push(f);
            }
            if segment.latest_committed_at > latest_commit {
                latest_commit = segment.latest_committed_at;
            }
        }

        // Represent the unioned live set as a single contiguous chain: a
        // genesis (id 0) plus one Append (id 1) carrying every live file. This
        // keeps `live_data_files()` correct and gives a deterministic, valid
        // `current_snapshot` that callers can pin / advance against.
        let genesis = Snapshot {
            id: SnapshotId::GENESIS,
            parent: None,
            committed_at: meta
                .snapshots
                .iter()
                .find(|s| s.id == SnapshotId::GENESIS)
                .map(|s| s.committed_at)
                .unwrap_or(latest_commit),
            data_files: Vec::new(),
            removed_paths: Vec::new(),
            summary: SnapshotSummary {
                operation: SnapshotOperation::Genesis,
                added_files: 0,
                added_rows: 0,
                added_bytes: 0,
                removed_files: 0,
            },
        };
        if all_live.is_empty() {
            meta.current_snapshot = SnapshotId::GENESIS;
            meta.snapshots = vec![genesis];
        } else {
            let added_files = all_live.len() as u64;
            let added_rows: u64 = all_live.iter().map(|f| f.row_count).sum();
            let added_bytes: u64 = all_live.iter().map(|f| f.size_bytes).sum();
            let union_snap = Snapshot {
                id: SnapshotId(1),
                parent: Some(SnapshotId::GENESIS),
                committed_at: latest_commit,
                data_files: all_live,
                removed_paths: Vec::new(),
                summary: SnapshotSummary {
                    operation: SnapshotOperation::Append,
                    added_files,
                    added_rows,
                    added_bytes,
                    removed_files: 0,
                },
            };
            meta.current_snapshot = SnapshotId(1);
            meta.snapshots = vec![genesis, union_snap];
        }
        Ok(meta)
    }

    /// Read-modify-write a metadata field with transparent retry on the
    /// create-if-absent race. `mutate` applies the DDL change to a fresh clone
    /// of the current manifest; idempotent DDL re-applies cleanly after a lost
    /// race, so we retry up to [`MAX_DDL_RETRIES`] before surfacing a conflict.
    async fn mutate_manifest<F>(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        mut mutate: F,
    ) -> Result<TableManifest>
    where
        F: FnMut(&mut TableManifest),
    {
        for _ in 0..MAX_DDL_RETRIES {
            let (version, manifest) = self.load_current(project, qtable).await?;
            let mut next = (*manifest).clone();
            next.version = version + 1;
            mutate(&mut next);
            if self
                .put_manifest_create(project, qtable, next.version, &next)
                .await?
            {
                let out = next.clone();
                self.after_commit(project, qtable, next.version, next).await;
                return Ok(out);
            }
            // Lost the race — reload and retry the (idempotent) mutation.
            self.invalidate(project, qtable).await;
        }
        Err(BasinError::CommitConflict(format!(
            "{project}/{qtable}: exhausted DDL retries under contention"
        )))
    }

    // -----------------------------------------------------------------------
    // Schema-qualified workers. Both the bare-`TableName` trait methods (which
    // resolve a schema via `resolve_qtable`) and the `*_qualified` trait
    // overrides (which carry an explicit schema) delegate here so the keying
    // logic lives in exactly one place.
    // -----------------------------------------------------------------------

    async fn create_table_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: &Schema,
    ) -> Result<TableMetadata> {
        let table = &qtable.name;
        // Genesis manifest at v0 via create-if-absent. AlreadyExists => the
        // table already has a v0 manifest. If that v0 is a tombstone (dropped),
        // a recreate is allowed by writing a fresh genesis at the next version.
        let manifest = TableManifest::genesis(schema.clone());
        if self.put_manifest_create(project, qtable, 0, &manifest).await? {
            let meta = manifest.to_metadata(project, table);
            self.after_commit(project, qtable, 0, manifest).await;
            return Ok(meta);
        }
        // v0 exists. Resolve the live state.
        match self.load_current(project, qtable).await {
            Ok(_) => Err(BasinError::catalog(format!(
                "table {project}/{qtable} already exists"
            ))),
            Err(BasinError::NotFound(_)) => {
                // Latest manifest is a tombstone — recreate at next version.
                let version = self
                    .resolve_head_version(project, qtable)
                    .await?
                    .unwrap_or(0);
                let mut genesis = TableManifest::genesis(schema.clone());
                genesis.version = version + 1;
                // BUMP the partition generation past the dropped manifest's so
                // the recreated table writes its segment chains under a FRESH
                // `parts/g{n}/…` prefix. A prior/concurrent drop's purge is scoped
                // to the OLD generation (see `drop_table_q` / `purge_part_segments`)
                // and can never enumerate — let alone delete — these segments, so
                // a recreate's delta chain can never reference a baseline the purge
                // removed (the torn-chain root cause). Reading the dropped
                // manifest's generation is cheap (the tombstone version we just
                // resolved). Genesis defaulted `parts_generation = 0`; default the
                // dropped read to 0 too so a legacy un-prefixed tree becomes g1.
                let dropped_generation = self
                    .get_manifest(project, qtable, version)
                    .await
                    .map(|m| m.parts_generation)
                    .unwrap_or(0);
                genesis.parts_generation = dropped_generation + 1;
                if self
                    .put_manifest_create(project, qtable, genesis.version, &genesis)
                    .await?
                {
                    let meta = genesis.to_metadata(project, table);
                    self.after_commit(project, qtable, genesis.version, genesis)
                        .await;
                    Ok(meta)
                } else {
                    Err(BasinError::catalog(format!(
                        "table {project}/{qtable} recreate lost race"
                    )))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn load_table_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        // Resolve the current META manifest. Under sustained ingest the META
        // head never moves (ingest commits to per-partition segments, not the
        // META chain), so `load_current` serves both head + body from cache with
        // ZERO store round-trips — making it a safe, cheap gate for the
        // bounded-staleness snapshot below.
        let (meta_version, manifest) = self.load_current(project, qtable).await?;

        // #30 bounded-staleness read snapshot: if enabled and a non-expired
        // entry exists for the SAME META version, serve the already-resolved
        // unioned view without re-LISTing partitions, re-HEAD-probing segment
        // heads, or re-folding chains — the round-trips that contend with heavy
        // ingest PUT traffic. The META-version gate guarantees a DDL forces a
        // refresh even before the TTL elapses (a manifest mutation runs through
        // `after_commit` / `invalidate`, which also drops the snapshot).
        let ttl_ms = self.read_snapshot_ttl_ms();
        if ttl_ms > 0 {
            let ck = self.cache_key(project, qtable);
            {
                let cache = self.read_snapshot_cache.lock().await;
                if let Some(entry) = cache.get(&ck) {
                    if entry.meta_version == meta_version
                        && entry.expires_at > std::time::Instant::now()
                    {
                        return Ok((*entry.meta).clone());
                    }
                }
            }
            // Miss / expired / stale META version: do the authoritative resolve
            // and refresh the snapshot with a fresh deadline.
            let meta = self.load_unioned(project, qtable, &manifest).await?;
            let entry = ReadSnapshotEntry {
                meta_version,
                meta: Arc::new(meta.clone()),
                expires_at: std::time::Instant::now()
                    + std::time::Duration::from_millis(ttl_ms),
            };
            self.read_snapshot_cache.lock().await.insert(ck, entry);
            return Ok(meta);
        }

        // TTL disabled: exact-every-read legacy path. UNION every partition's
        // data files into the returned metadata on every call.
        self.load_unioned(project, qtable, &manifest).await
    }

    /// META-only metadata: schema + DDL + constraints + RLS policies +
    /// partition spec + write tunables, with an EMPTY snapshot/data-file set.
    ///
    /// Reads ONLY the single META manifest chain via [`load_current`] (one HEAD
    /// + one cached manifest GET) — it does NOT call [`list_partition_ids`] or
    /// [`load_part_current`], so it stays O(1) however many partition segments
    /// the table has accumulated. The data-file fields are deliberately blanked
    /// (genesis-only chain) so a caller can never mistake this for a live file
    /// set; the ingest constraint-prep path that consumes it sources existing
    /// rows from the storage LIST, not from metadata (see `load_table_meta`).
    async fn load_table_meta_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        let (_v, manifest) = self.load_current(project, qtable).await?;
        let mut meta = manifest.to_metadata(project, &qtable.name);
        // Blank the data-file set: META-only by contract. Present a valid
        // genesis-only chain so `live_data_files()` is empty (not a panic) for
        // any caller that probes it.
        meta.current_snapshot = SnapshotId::GENESIS;
        meta.snapshots = vec![Snapshot {
            id: SnapshotId::GENESIS,
            parent: None,
            committed_at: manifest
                .snapshots
                .iter()
                .find(|s| s.id == SnapshotId::GENESIS)
                .map(|s| s.committed_at)
                .unwrap_or_else(Utc::now),
            data_files: Vec::new(),
            removed_paths: Vec::new(),
            summary: SnapshotSummary {
                operation: SnapshotOperation::Genesis,
                added_files: 0,
                added_rows: 0,
                added_bytes: 0,
                removed_files: 0,
            },
        }];
        Ok(meta)
    }

    async fn drop_table_q(&self, project: &ProjectId, qtable: &QualifiedTableName) -> Result<()> {
        // Tombstone: append a manifest version with `dropped = true`. Keeps the
        // history immutable and lets concurrent readers resolve deterministically.
        // Capture the partition GENERATION that is live at drop time BEFORE the
        // tombstone so the purge below is scoped to exactly this generation's
        // segment tree — a concurrent/subsequent same-name recreate stamps a
        // HIGHER generation under a distinct prefix the purge can never reach.
        let (_v, manifest) = self.load_current(project, qtable).await?; // NotFound if absent.
        let dropped_generation = manifest.parts_generation;
        self.mutate_manifest(project, qtable, |m| m.dropped = true)
            .await?;
        // The META tombstone alone does NOT empty a recreated same-name table:
        // partition-sharded data lives in the `parts/` segment tree, which is
        // outside the manifest chain and reuses the table's prefix verbatim on
        // recreate. Purge that tree so `load_unioned` resolves an empty live set
        // (otherwise `count(*)` re-sums stale `row_count`s — see
        // `purge_part_segments`). The purge is scoped to `dropped_generation`, so
        // even if a same-name CREATE + heavy ingest races this purge, the
        // recreated table's segments (a fresh higher generation) are untouched —
        // no delta whose baseline got purged → no torn chain → no project-wide
        // session-open FATAL. Then evict ALL per-node caches for the key so a
        // warm catalog can't serve the pre-drop folded views: the manifest body +
        // META head (`invalidate`) and every partition's folded segment
        // (`invalidate_all_parts`).
        self.purge_part_segments(project, qtable, dropped_generation).await;
        self.invalidate(project, qtable).await;
        self.invalidate_all_parts(project, qtable).await;
        Ok(())
    }

    async fn rename_table_q(
        &self,
        project: &ProjectId,
        old: &QualifiedTableName,
        new: &QualifiedTableName,
    ) -> Result<()> {
        let (_v, manifest) = self.load_current(project, old).await?;
        // Destination must not already exist.
        match self.load_current(project, new).await {
            Ok(_) => {
                return Err(BasinError::catalog(format!(
                    "rename_table: {project}/{new} already exists"
                )))
            }
            Err(BasinError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        // Write a fresh genesis-versioned manifest for `new` carrying old state,
        // then tombstone `old`. (Snapshot history + all fields are preserved.)
        // `old` and `new` may live in different schemas — the keys differ, so
        // this correctly moves the manifest across schema dirs.
        let mut dst = (*manifest).clone();
        dst.version = 0;
        dst.dropped = false;
        if !self.put_manifest_create(project, new, 0, &dst).await? {
            // new dir had a stale/tombstoned manifest; place at next version.
            let v = self.resolve_head_version(project, new).await?.unwrap_or(0);
            dst.version = v + 1;
            if !self
                .put_manifest_create(project, new, dst.version, &dst)
                .await?
            {
                return Err(BasinError::catalog(format!(
                    "rename_table: {project}/{new} lost create race"
                )));
            }
        }
        self.after_commit(project, new, dst.version, dst).await;
        // Carry over any per-partition data-file segments (sharded ingest data).
        self.copy_partition_segments(project, old, project, new).await?;
        self.mutate_manifest(project, old, |m| m.dropped = true)
            .await?;
        Ok(())
    }

    async fn current_snapshot_id_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<SnapshotId> {
        // The table-level current snapshot is the synthetic union id: GENESIS
        // when nothing has data, else `SnapshotId(1)` (see `load_unioned`).
        // Confirm the table exists (NotFound if its META manifest is absent or
        // tombstoned), then report based on whether the META chain or any
        // partition holds live files.
        let (_v, manifest) = self.load_current(project, qtable).await?;
        if !manifest
            .to_metadata(project, &qtable.name)
            .live_data_files()
            .is_empty()
        {
            return Ok(SnapshotId(1));
        }
        let generation = manifest.parts_generation;
        let partition_ids = self.list_partition_ids(project, qtable, generation).await?;
        for pid in &partition_ids {
            let (_pv, segment) = self.load_part_current(project, qtable, generation, pid).await?;
            if !segment.live_data_files().is_empty() {
                return Ok(SnapshotId(1));
            }
        }
        Ok(SnapshotId::GENESIS)
    }

    async fn list_snapshots_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Vec<Snapshot>> {
        // Surface the unioned (synthetic) chain so callers see a complete,
        // consistent file set per snapshot. Per-partition history is not
        // collapsed into a single global timeline (see `load_unioned`).
        let (_v, manifest) = self.load_current(project, qtable).await?;
        let meta = self.load_unioned(project, qtable, &manifest).await?;
        let mut snaps = meta.snapshots.clone();
        snaps.sort_by_key(|s| s.id);
        Ok(snaps)
    }

    async fn rollback_to_snapshot_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let (_v, manifest) = self.load_current(project, qtable).await?;
        if !manifest.snapshots.iter().any(|s| s.id == snapshot_id) {
            return Err(BasinError::not_found(format!(
                "{project}/{qtable}: snapshot {snapshot_id} not in history"
            )));
        }
        let next = self
            .mutate_manifest(project, qtable, |m| {
                let mut orphans: Vec<String> = Vec::new();
                for s in m.snapshots.iter().filter(|s| s.id > snapshot_id) {
                    for f in &s.data_files {
                        orphans.push(f.path.clone());
                    }
                }
                m.gc_orphan_paths.extend(orphans);
                m.snapshots.retain(|s| s.id <= snapshot_id);
                m.current_snapshot = snapshot_id;
            })
            .await?;
        Ok(next.to_metadata(project, &qtable.name))
    }

    async fn create_index_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
        access_method: &str,
        opclass: Option<&str>,
    ) -> Result<()> {
        if columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "create_index: column list cannot be empty".into(),
            ));
        }
        // Validate columns + duplicate name against the current manifest first
        // so we return the right error without burning a version.
        let (_v, manifest) = self.load_current(project, qtable).await?;
        for col in columns {
            if manifest.schema.field_with_name(col).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "create_index: column {col:?} not in table {project}/{qtable} schema"
                )));
            }
        }
        if manifest.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(BasinError::catalog(format!(
                "create_index: {project}/{qtable}: index {name:?} already exists"
            )));
        }
        let idx = SecondaryIndex {
            name: name.to_string(),
            columns: columns.to_vec(),
            access_method: access_method.to_string(),
            opclass: opclass.map(|s| s.to_string()),
        };
        self.mutate_manifest(project, qtable, |m| {
            if !m.indexes.iter().any(|i| i.name == idx.name) {
                m.indexes.push(idx.clone());
            }
        })
        .await?;
        Ok(())
    }

    async fn drop_index_q(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
    ) -> Result<()> {
        let (_v, manifest) = self.load_current(project, qtable).await?;
        if !manifest.indexes.iter().any(|i| i.name == name) {
            return Err(BasinError::not_found(format!(
                "{project}/{qtable}: index {name:?}"
            )));
        }
        self.mutate_manifest(project, qtable, |m| {
            m.indexes.retain(|i| i.name != name);
        })
        .await?;
        Ok(())
    }
}

fn storage_err(ctx: &str, e: object_store::Error) -> BasinError {
    BasinError::catalog(format!("object-store catalog {ctx}: {e}"))
}

#[async_trait]
impl Catalog for ObjectStoreCatalog {
    fn epoch(&self) -> u64 {
        self.epoch.load(SeqCst)
    }

    async fn create_namespace(&self, _project: &ProjectId) -> Result<()> {
        // Namespaces are implicit in the key layout — a table's manifest key
        // carries the project id. Nothing to pre-create.
        self.bump_epoch();
        Ok(())
    }

    async fn create_table(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: &Schema,
    ) -> Result<TableMetadata> {
        // Old API: always creates in the public schema (mirrors InMemory).
        let qtable = QualifiedTableName::in_public(table.clone());
        self.create_table_q(project, &qtable, schema).await
    }

    async fn load_table(&self, project: &ProjectId, table: &TableName) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.load_table_q(project, &qtable).await
    }

    async fn load_table_meta(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.load_table_meta_q(project, &qtable).await
    }

    async fn meta_version(&self, project: &ProjectId, table: &TableName) -> u64 {
        // The META manifest version IS the meta-epoch: it advances only on a
        // manifest write (DDL / schema evolution / single-node META-chain
        // appends), never on `append_data_files_in_partition` (which writes a
        // per-partition segment, not the manifest). A failure to resolve the
        // version (missing/tombstoned table, transient store error) maps to 0,
        // which is always-stale → the ingest cache misses and re-loads; that is
        // the safe degradation, never a stale hit.
        let qtable = self.resolve_qtable(project, table).await;
        match self.resolve_head_version(project, &qtable).await {
            Ok(Some(v)) => v.wrapping_add(1), // +1 so version 0 is distinguishable from the "unknown" 0
            _ => 0,
        }
    }

    async fn current_snapshot_id(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<SnapshotId> {
        let qtable = self.resolve_qtable(project, table).await;
        self.current_snapshot_id_q(project, &qtable).await
    }

    async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.drop_table_q(project, &qtable).await
    }

    async fn rename_table(
        &self,
        project: &ProjectId,
        old: &TableName,
        new: &TableName,
    ) -> Result<()> {
        let qold = self.resolve_qtable(project, old).await;
        // New tables go to public unless qualified (mirrors InMemory).
        let qnew = QualifiedTableName::in_public(new.clone());
        self.rename_table_q(project, &qold, &qnew).await
    }

    async fn list_tables(&self, project: &ProjectId) -> Result<Vec<TableName>> {
        // Back-compat: return only the bare names of live public-schema tables.
        let all = self.list_schema_table_names(project).await?;
        let public = SchemaName::public();
        let mut out = Vec::new();
        for qt in all {
            if qt.schema != public {
                continue;
            }
            // Filter out tombstoned tables.
            match self.load_current(project, &qt).await {
                Ok(_) => out.push(qt.name),
                Err(BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        out.sort();
        Ok(out)
    }

    async fn append_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.commit_snapshot(
            project,
            &qtable,
            expected_snapshot,
            SnapshotOperation::Append,
            Vec::new(),
            files,
        )
        .await
    }

    async fn replace_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        // Removed paths may live on the META chain (back-compat single-node
        // writes) OR in a per-partition segment (sharded ingest). A non-
        // partitioned caller (tiering / stripe-merge / COW table sweeps) does
        // not know which — so route the replace to the chain that actually
        // holds the removed path(s). All removed paths in one call belong to
        // one physical file group, so they resolve to a single owning chain.
        // Group removed paths by the chain that currently holds each one: a
        // single UPDATE/DELETE may touch files committed via the META chain
        // (single-node OLTP) AND files in per-partition segments (sharded
        // ingest). Each chain's removes must be committed to THAT chain.
        if !removed_paths.is_empty() {
            use std::collections::{HashMap, HashSet};
            // Resolve the current partition generation once; every partition
            // probe below reads only this generation's segment chains (the
            // per-partition `commit_part_snapshot` re-resolves it itself).
            let generation = self.load_current(project, &qtable).await?.1.parts_generation;
            let mut by_partition: HashMap<String, Vec<String>> = HashMap::new();
            let mut remaining: HashSet<String> = removed_paths.iter().cloned().collect();
            for pid in self.list_partition_ids(project, &qtable, generation).await? {
                if remaining.is_empty() {
                    break;
                }
                let (_pv, segment) = self.load_part_current(project, &qtable, generation, &pid).await?;
                let live: HashSet<String> = segment
                    .live_data_files()
                    .into_iter()
                    .map(|f| f.path)
                    .collect();
                let here: Vec<String> = remaining
                    .iter()
                    .filter(|p| live.contains(*p))
                    .cloned()
                    .collect();
                for p in &here {
                    remaining.remove(p);
                }
                if !here.is_empty() {
                    by_partition.insert(pid, here);
                }
            }

            // Paths not found in any partition segment belong to the META chain.
            // Attach the new added_files to the META commit when the META chain
            // is involved; otherwise attach them to one partition commit so they
            // land exactly once.
            let meta_removed: Vec<String> = remaining.into_iter().collect();

            if !meta_removed.is_empty() || by_partition.is_empty() {
                // META chain owns some removed paths (or there were none in any
                // partition) → commit the adds here together with META removes.
                self.commit_snapshot(
                    project,
                    &qtable,
                    expected_snapshot,
                    SnapshotOperation::Replace,
                    meta_removed,
                    added_files,
                )
                .await?;
                // Partition removes (if any) are committed without re-adding.
                for (pid, paths) in by_partition {
                    let (_pv, segment) = self.load_part_current(project, &qtable, generation, &pid).await?;
                    self.commit_part_snapshot(
                        project,
                        &qtable,
                        &pid,
                        segment.current_snapshot,
                        SnapshotOperation::Replace,
                        paths,
                        Vec::new(),
                    )
                    .await?;
                }
            } else {
                // All removed paths live in partition segments. Attach the adds
                // to the FIRST partition commit, the rest are pure removes.
                let mut iter = by_partition.into_iter();
                let (first_pid, first_paths) = iter.next().expect("non-empty by_partition");
                let (_pv, seg0) = self.load_part_current(project, &qtable, generation, &first_pid).await?;
                self.commit_part_snapshot(
                    project,
                    &qtable,
                    &first_pid,
                    seg0.current_snapshot,
                    SnapshotOperation::Replace,
                    first_paths,
                    added_files,
                )
                .await?;
                for (pid, paths) in iter {
                    let (_pv, segment) = self.load_part_current(project, &qtable, generation, &pid).await?;
                    self.commit_part_snapshot(
                        project,
                        &qtable,
                        &pid,
                        segment.current_snapshot,
                        SnapshotOperation::Replace,
                        paths,
                        Vec::new(),
                    )
                    .await?;
                }
            }
            // Return a fresh unioned read so the caller sees the complete set.
            let (_v, manifest) = self.load_current(project, &qtable).await?;
            return self.load_unioned(project, &qtable, &manifest).await;
        }

        // Pure add (no removals) → META chain.
        self.commit_snapshot(
            project,
            &qtable,
            expected_snapshot,
            SnapshotOperation::Replace,
            removed_paths,
            added_files,
        )
        .await
    }

    async fn current_snapshot_id_in_partition(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
    ) -> Result<SnapshotId> {
        let qtable = self.resolve_qtable(project, table).await;
        // Confirm the table exists (NotFound if absent/tombstoned) and read its
        // current partition generation.
        let generation = self.load_current(project, &qtable).await?.1.parts_generation;
        let (_v, segment) = self.load_part_current(project, &qtable, generation, partition_id).await?;
        Ok(segment.current_snapshot)
    }

    async fn append_data_files_in_partition(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.commit_part_snapshot(
            project,
            &qtable,
            partition_id,
            expected_snapshot,
            SnapshotOperation::Append,
            Vec::new(),
            files,
        )
        .await
    }

    async fn replace_data_files_in_partition(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.commit_part_snapshot(
            project,
            &qtable,
            partition_id,
            expected_snapshot,
            SnapshotOperation::Replace,
            removed_paths,
            added_files,
        )
        .await
    }

    async fn list_merge_partitions(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<String>> {
        let qtable = self.resolve_qtable(project, table).await;
        // The unbounded-file-count growth lives in the per-partition SEGMENT
        // chains (the sharded ingest path appends one file per flush there).
        // Report exactly those partitions that hold live segment files; the
        // merge sweep commits each via `replace_data_files_in_partition`, the
        // matching OCC chain. META-chain (single-node OLTP) files are not
        // enumerated here — they are coalesced by copy-on-write / stripe-merge.
        let mut out = Vec::new();
        let generation = self.load_current(project, &qtable).await?.1.parts_generation;
        for pid in self.list_partition_ids(project, &qtable, generation).await? {
            let (_v, segment) = self.load_part_current(project, &qtable, generation, &pid).await?;
            if !segment.live_data_files().is_empty() {
                out.push(pid);
            }
        }
        Ok(out)
    }

    async fn live_data_files_in_partition(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
    ) -> Result<(SnapshotId, Vec<DataFileRef>)> {
        let qtable = self.resolve_qtable(project, table).await;
        let generation = self.load_current(project, &qtable).await?.1.parts_generation;
        let (_v, segment) = self.load_part_current(project, &qtable, generation, partition_id).await?;
        Ok((segment.current_snapshot, segment.live_data_files()))
    }

    async fn list_snapshots(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>> {
        let qtable = self.resolve_qtable(project, table).await;
        self.list_snapshots_q(project, &qtable).await
    }

    async fn rollback_to_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.rollback_to_snapshot_q(project, &qtable, snapshot_id)
            .await
    }

    async fn set_partition_spec(
        &self,
        project: &ProjectId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.partition_spec = spec.clone())
            .await?;
        Ok(())
    }

    async fn set_rls_state(
        &self,
        project: &ProjectId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            m.rls_enabled = rls_enabled;
            m.policies = policies.clone();
        })
        .await?;
        Ok(())
    }

    async fn set_tier_policy(
        &self,
        project: &ProjectId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            m.cold_after_seconds = cold_after_seconds;
            m.cold_age_column = cold_age_column.clone();
        })
        .await?;
        Ok(())
    }

    async fn set_bloom_filter_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.bloom_filter_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_row_group_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.row_group_rows = rows)
            .await?;
        Ok(())
    }

    async fn set_schema(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: Schema,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.schema = schema.clone())
            .await?;
        Ok(())
    }

    async fn set_continuous_aggregate(
        &self,
        project: &ProjectId,
        table: &TableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.continuous_aggregate = def.clone())
            .await?;
        Ok(())
    }

    async fn set_cluster_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.cluster_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_file_format(
        &self,
        project: &ProjectId,
        table: &TableName,
        format: TableFileFormat,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.file_format = format)
            .await?;
        Ok(())
    }

    async fn set_global_sort_order(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            m.global_sort_order = if columns.is_empty() {
                None
            } else {
                Some(columns.clone())
            };
        })
        .await?;
        Ok(())
    }

    async fn set_row_block_size(
        &self,
        project: &ProjectId,
        table: &TableName,
        size: Option<u32>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.row_block_size = size)
            .await?;
        Ok(())
    }

    async fn set_adaptive_sort_override(
        &self,
        project: &ProjectId,
        table: &TableName,
        value: Option<bool>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.adaptive_sort_override = value)
            .await?;
        Ok(())
    }

    async fn set_home_region(
        &self,
        project: &ProjectId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| m.home_region = region.clone())
            .await?;
        Ok(())
    }

    async fn set_table_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        pk_columns: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            m.pk_columns = pk_columns.clone();
            m.check_constraints = check_constraints.clone();
            m.foreign_keys = foreign_keys.clone();
        })
        .await?;
        Ok(())
    }

    async fn set_unique_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            m.unique_constraints = unique_constraints.clone();
        })
        .await?;
        Ok(())
    }

    async fn create_index(
        &self,
        project: &ProjectId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        self.create_index_with_method(project, table, name, columns, if_not_exists, "btree", None)
            .await
    }

    async fn create_index_with_method(
        &self,
        project: &ProjectId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
        access_method: &str,
        opclass: Option<&str>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.create_index_q(
            project,
            &qtable,
            name,
            columns,
            if_not_exists,
            access_method,
            opclass,
        )
        .await
    }

    async fn drop_index(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.drop_index_q(project, &qtable, name).await
    }

    async fn promote_jsonb_path(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.mutate_manifest(project, &qtable, |m| {
            let exists = m
                .promoted_jsonb_paths
                .iter()
                .any(|p| p.source_col == source_col && p.json_key == json_key);
            if !exists {
                m.promoted_jsonb_paths.push(PromotedJsonbPath {
                    source_col: source_col.to_string(),
                    json_key: json_key.to_string(),
                });
            }
        })
        .await?;
        Ok(())
    }

    // ---- Project-scoped metadata (stored as small JSON objects under the
    // project prefix; Overwrite is fine — last writer wins, mirrors the
    // in-memory backend's replace-on-set semantics). ----

    async fn set_project_metadata(
        &self,
        project: &ProjectId,
        meta: ProjectMetadata,
    ) -> Result<()> {
        self.put_project_json(project, "metadata.json", &meta).await
    }

    async fn get_project_metadata(&self, project: &ProjectId) -> Result<ProjectMetadata> {
        Ok(self
            .get_project_json::<ProjectMetadata>(project, "metadata.json")
            .await?
            .unwrap_or_default())
    }

    async fn set_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
        watermark_lsn: u64,
    ) -> Result<()> {
        // Monotonic: never lower a stored watermark.
        let key = format!("watermark_{}.json", sanitize(partition_id));
        let existing = self
            .get_project_json::<u64>(project, &key)
            .await?
            .unwrap_or(0);
        let next = existing.max(watermark_lsn);
        self.put_project_json(project, &key, &next).await
    }

    async fn get_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<u64>> {
        let key = format!("watermark_{}.json", sanitize(partition_id));
        self.get_project_json::<u64>(project, &key).await
    }

    async fn set_project_max_connections(
        &self,
        project: &ProjectId,
        max_connections: u32,
    ) -> Result<()> {
        self.put_project_json(project, "max_connections.json", &max_connections)
            .await
    }

    async fn get_project_max_connections(&self, project: &ProjectId) -> Result<Option<u32>> {
        self.get_project_json::<u32>(project, "max_connections.json")
            .await
    }

    async fn set_project_rate_limit_qps(&self, project: &ProjectId, qps: u32) -> Result<()> {
        self.put_project_json(project, "rate_limit_qps.json", &qps)
            .await
    }

    async fn get_project_rate_limit_qps(&self, project: &ProjectId) -> Result<Option<u32>> {
        self.get_project_json::<u32>(project, "rate_limit_qps.json")
            .await
    }

    async fn set_project_storage_config(
        &self,
        project: &ProjectId,
        config: crate::project_storage_config::ProjectStorageConfig,
    ) -> Result<()> {
        self.put_project_json(project, "storage_config.json", &config)
            .await
    }

    async fn get_project_storage_config(
        &self,
        project: &ProjectId,
    ) -> Result<Option<crate::project_storage_config::ProjectStorageConfig>> {
        self.get_project_json::<crate::project_storage_config::ProjectStorageConfig>(
            project,
            "storage_config.json",
        )
        .await
    }

    // ---- Multi-bucket storage pool (#36, Stage 1) ----
    //
    // The registry is a single GLOBAL object (`{root}_bucket_pool/registry.json`)
    // so any node reads the full pool topology in one GET. Each project's
    // assignment is a per-project record written create-if-absent so the FIRST
    // write wins and the assignment is stable thereafter (the linearization
    // point). Credentials are referenced by name in the registry, never inlined.

    async fn get_bucket_registry(&self) -> Result<crate::bucket_pool::BucketRegistry> {
        let key = OsPath::from(format!("{}_bucket_pool/registry.json", self.root));
        match self.store.get(&key).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| storage_err("read bucket registry", e))?;
                let v = serde_json::from_slice(&bytes)
                    .map_err(|e| BasinError::catalog(format!("decode bucket registry: {e}")))?;
                Ok(v)
            }
            Err(object_store::Error::NotFound { .. }) => {
                Ok(crate::bucket_pool::BucketRegistry::default())
            }
            Err(e) => Err(storage_err("get bucket registry", e)),
        }
    }

    async fn put_bucket_registry(
        &self,
        registry: &crate::bucket_pool::BucketRegistry,
    ) -> Result<()> {
        let key = OsPath::from(format!("{}_bucket_pool/registry.json", self.root));
        let bytes = serde_json::to_vec(registry)
            .map_err(|e| BasinError::catalog(format!("serialise bucket registry: {e}")))?;
        self.store
            .put_opts(
                &key,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| storage_err("put bucket registry", e))?;
        self.bump_epoch();
        Ok(())
    }

    async fn get_bucket_assignment(
        &self,
        project: &ProjectId,
    ) -> Result<Option<crate::bucket_pool::BucketAssignment>> {
        self.get_project_json::<crate::bucket_pool::BucketAssignment>(
            project,
            "bucket_assignment.json",
        )
        .await
    }

    async fn assign_bucket_if_absent(
        &self,
        project: &ProjectId,
        proposed: &crate::bucket_pool::BucketAssignment,
    ) -> Result<crate::bucket_pool::BucketAssignment> {
        let key = self.project_meta_key(project, "bucket_assignment.json");
        let bytes = serde_json::to_vec(proposed)
            .map_err(|e| BasinError::catalog(format!("serialise bucket assignment: {e}")))?;
        // Create-if-absent is the CAS: only the first writer's PUT succeeds;
        // the loser gets AlreadyExists and re-reads the winning assignment, so
        // the assignment is stable and identical on every node.
        match self
            .store
            .put_opts(
                &key,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                self.bump_epoch();
                Ok(proposed.clone())
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                // Lost the race: read back the durable winner.
                self.get_bucket_assignment(project).await?.ok_or_else(|| {
                    BasinError::catalog(
                        "assign_bucket_if_absent: AlreadyExists but assignment not readable",
                    )
                })
            }
            Err(e) => Err(storage_err("assign bucket if absent", e)),
        }
    }

    // ---- Online consolidation / migration (#37) ----
    //
    // The migration intent is a single per-project object under a GLOBAL
    // prefix (`{root}_bucket_pool/migrations/{project}.json`) so any node can
    // LIST every in-flight migration for resume + bounded-concurrency. Cutover
    // overwrites the per-project `bucket_assignment.json` — a single atomic PUT
    // (the linearization point).

    async fn set_bucket_assignment(
        &self,
        project: &ProjectId,
        assignment: &crate::bucket_pool::BucketAssignment,
    ) -> Result<()> {
        // A single Overwrite PUT — the atomic cutover. After it returns the
        // assignment durably points at the target; a crash either left the old
        // value (re-run cutover) or the new one (done).
        self.put_project_json(project, "bucket_assignment.json", assignment)
            .await
    }

    async fn get_migration_intent(
        &self,
        project: &ProjectId,
    ) -> Result<Option<crate::bucket_pool::MigrationIntent>> {
        let key = self.migration_intent_key(project);
        match self.store.get(&key).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| storage_err("read migration intent", e))?;
                let v = serde_json::from_slice(&bytes)
                    .map_err(|e| BasinError::catalog(format!("decode migration intent: {e}")))?;
                Ok(Some(v))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err("get migration intent", e)),
        }
    }

    async fn put_migration_intent(
        &self,
        intent: &crate::bucket_pool::MigrationIntent,
    ) -> Result<()> {
        let key = self.migration_intent_key(&intent.project);
        let bytes = serde_json::to_vec(intent)
            .map_err(|e| BasinError::catalog(format!("serialise migration intent: {e}")))?;
        self.store
            .put_opts(
                &key,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| storage_err("put migration intent", e))?;
        self.bump_epoch();
        Ok(())
    }

    async fn delete_migration_intent(&self, project: &ProjectId) -> Result<()> {
        let key = self.migration_intent_key(project);
        match self.store.delete(&key).await {
            Ok(()) => {
                self.bump_epoch();
                Ok(())
            }
            // Idempotent: deleting an absent intent is success.
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(storage_err("delete migration intent", e)),
        }
    }

    async fn list_migration_intents(
        &self,
    ) -> Result<Vec<crate::bucket_pool::MigrationIntent>> {
        use futures::StreamExt;
        let prefix = OsPath::from(format!("{}_bucket_pool/migrations/", self.root));
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list migration intents", e))?;
            let res = self
                .store
                .get(&meta.location)
                .await
                .map_err(|e| storage_err("read migration intent", e))?;
            let bytes = res
                .bytes()
                .await
                .map_err(|e| storage_err("read migration intent", e))?;
            if let Ok(intent) =
                serde_json::from_slice::<crate::bucket_pool::MigrationIntent>(&bytes)
            {
                out.push(intent);
            }
        }
        Ok(out)
    }

    // ---- SQL functions (durable; mirrors InMemory semantics) ----

    async fn register_sql_function(&self, mut def: SqlFunctionDef) -> Result<()> {
        // REPLACE bumps `version` (same contract as InMemory) so downstream
        // caches can detect a redeploy without diffing the body.
        if let Some(existing) = self.lookup_sql_function(&def.project, &def.name).await {
            def.version = existing.version.saturating_add(1);
        }
        let bytes = serde_json::to_vec(&def)
            .map_err(|e| BasinError::catalog(format!("serialise sql function: {e}")))?;
        self.store
            .put_opts(
                &self.sql_function_key(&def.project, &def.name),
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| storage_err("put sql function", e))?;
        self.bump_epoch();
        Ok(())
    }

    async fn drop_sql_function(&self, project: &ProjectId, name: &str) -> Result<()> {
        let key = self.sql_function_key(project, name);
        match self.store.delete(&key).await {
            Ok(_) => {
                self.bump_epoch();
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => Err(BasinError::not_found(format!(
                "{project}: sql function {name:?}"
            ))),
            Err(e) => Err(storage_err("delete sql function", e)),
        }
    }

    async fn lookup_sql_function(&self, project: &ProjectId, name: &str) -> Option<SqlFunctionDef> {
        match self.store.get(&self.sql_function_key(project, name)).await {
            Ok(res) => {
                let bytes = res.bytes().await.ok()?;
                serde_json::from_slice(&bytes).ok()
            }
            _ => None,
        }
    }

    async fn list_sql_functions(&self, project: &ProjectId) -> Vec<SqlFunctionDef> {
        use futures::StreamExt;
        let prefix = OsPath::from(self.sql_function_dir(project));
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let Ok(meta) = item else { continue };
            let Ok(res) = self.store.get(&meta.location).await else {
                continue;
            };
            let Ok(bytes) = res.bytes().await else { continue };
            if let Ok(def) = serde_json::from_slice::<SqlFunctionDef>(&bytes) {
                out.push(def);
            }
        }
        out
    }

    // ---- Sequences (durable, multi-node-safe block allocation) ----

    async fn create_sequence(&self, def: SequenceDef) -> Result<()> {
        if def.increment == 0 {
            return Err(BasinError::InvalidSchema(
                "sequence increment must be non-zero".into(),
            ));
        }
        // Resurrect a tombstoned name: clearing the tombstone lets the new def
        // take over. A live def with this name is a conflict (PG semantics).
        if self.seq_exists(&def.project, &def.name).await? {
            return Err(BasinError::catalog(format!(
                "sequence {}/{} already exists",
                def.project, def.name,
            )));
        }
        let tombstone = self.seq_tombstone_key(&def.project, &def.name);
        if self.seq_is_tombstoned(&def.project, &def.name).await? {
            let _ = self.store.delete(&tombstone).await;
        }
        // Reset any stale hwm log from a prior drop so the resurrected sequence
        // starts from genesis.
        self.seq_purge(&def.project, &def.name).await?;
        let bytes = serde_json::to_vec(&def)
            .map_err(|e| BasinError::catalog(format!("serialise sequence def: {e}")))?;
        match self
            .store
            .put_opts(
                &self.seq_def_key(&def.project, &def.name),
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                return Err(BasinError::catalog(format!(
                    "sequence {}/{} already exists",
                    def.project, def.name,
                )))
            }
            Err(e) => return Err(storage_err("put sequence def", e)),
        }
        self.bump_epoch();
        Ok(())
    }

    async fn drop_sequence(&self, project: &ProjectId, name: &str) -> Result<()> {
        if !self.seq_exists(project, name).await? {
            return Err(BasinError::not_found(format!(
                "{project}: sequence {name:?}"
            )));
        }
        // Tombstone first (makes the sequence invisible at once), then best-
        // effort purge of the def + hwm log objects.
        self.store
            .put_opts(
                &self.seq_tombstone_key(project, name),
                Bytes::from_static(b"1").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| storage_err("put sequence tombstone", e))?;
        let _ = self.store.delete(&self.seq_def_key(project, name)).await;
        self.seq_purge(project, name).await?;
        // Forget any node-local cursor.
        self.seq_local.lock().await.remove(&(*project, name.to_string()));
        self.bump_epoch();
        Ok(())
    }

    async fn lookup_sequence(&self, project: &ProjectId, name: &str) -> Option<SequenceDef> {
        self.seq_load_def(project, name).await.ok().flatten()
    }

    async fn nextval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        let def = self
            .seq_load_def(project, name)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?;
        let block = self.seq_block();
        let key = (*project, name.to_string());
        // Fast path: hand out from the locally-reserved block if any remains.
        {
            let mut local = self.seq_local.lock().await;
            if let Some(entry) = local.get_mut(&key) {
                if let Some(v) = entry.block_values.pop_front() {
                    entry.last_returned = Some(v);
                    return Ok(v);
                }
            }
        }
        // Block empty / unseen: reserve a fresh block via durable CAS.
        let mut values = self.seq_reserve_block(&def, block).await?;
        let v = values
            .pop_front()
            .expect("seq_reserve_block returns a non-empty block on Ok");
        let mut local = self.seq_local.lock().await;
        let entry = local.entry(key).or_insert_with(|| SeqLocal {
            block_values: std::collections::VecDeque::new(),
            last_returned: None,
        });
        entry.block_values = values;
        entry.last_returned = Some(v);
        Ok(v)
    }

    async fn currval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        if !self.seq_exists(project, name).await? {
            return Err(BasinError::not_found(format!(
                "{project}: sequence {name:?}"
            )));
        }
        let local = self.seq_local.lock().await;
        match local
            .get(&(*project, name.to_string()))
            .and_then(|e| e.last_returned)
        {
            Some(v) => Ok(v),
            None => Err(BasinError::not_found(format!(
                "{project}: sequence {name:?} has not been advanced"
            ))),
        }
    }

    async fn setval(
        &self,
        project: &ProjectId,
        name: &str,
        value: i64,
        advance: bool,
    ) -> Result<i64> {
        let def = self
            .seq_load_def(project, name)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?;
        // PG: setval(seq, n, true) => next nextval returns n+increment, so the
        // persisted "last" is n. setval(seq, n, false) => next nextval returns
        // n exactly, so store n-increment (the started-advance lands on n).
        let stored_last = if advance {
            value
        } else {
            value.wrapping_sub(def.increment)
        };
        const MAX_RETRIES: u32 = 64;
        for _ in 0..MAX_RETRIES {
            let (version, _hwm) = self.seq_read_hwm(project, name).await?;
            let next_hwm = SeqHwm {
                last: stored_last,
                started: true,
            };
            if self.seq_put_hwm_create(project, name, version + 1, &next_hwm).await? {
                self.bump_epoch();
                // Drop any node-local reserved block: it may straddle the new
                // mark. The next nextval re-reserves from the persisted mark.
                let mut local = self.seq_local.lock().await;
                if let Some(entry) = local.get_mut(&(*project, name.to_string())) {
                    entry.block_values.clear();
                    entry.last_returned = Some(value);
                } else {
                    local.insert(
                        (*project, name.to_string()),
                        SeqLocal {
                            block_values: std::collections::VecDeque::new(),
                            last_returned: Some(value),
                        },
                    );
                }
                return Ok(value);
            }
        }
        Err(BasinError::catalog(format!(
            "{project}: sequence {name:?} setval contention exceeded retry budget"
        )))
    }

    async fn advance_sequence_floor(
        &self,
        project: &ProjectId,
        name: &str,
        floor: i64,
    ) -> Result<()> {
        // Confirm the sequence exists (and isn't tombstoned) before touching
        // the hwm log; a missing sequence is a NotFound, mirroring setval.
        let _def = self
            .seq_load_def(project, name)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?;
        const MAX_RETRIES: u32 = 64;
        for _ in 0..MAX_RETRIES {
            let (version, hwm) = self.seq_read_hwm(project, name).await?;
            // Monotonic floor: the new persisted "last handed out" is the
            // greater of what is already durably reserved and the recovered
            // floor. Crucially this NEVER lowers the persisted high-water mark
            // — a reserved-but-unused block ceiling (hwm.last > floor) wins, so
            // we never re-issue a value an earlier instance already handed out.
            let new_last = if hwm.started {
                hwm.last.max(floor)
            } else {
                floor
            };
            // Already at or above the floor and started — nothing to persist.
            if hwm.started && new_last == hwm.last {
                return Ok(());
            }
            let next_hwm = SeqHwm {
                last: new_last,
                started: true,
            };
            if self
                .seq_put_hwm_create(project, name, version + 1, &next_hwm)
                .await?
            {
                self.bump_epoch();
                // Drop any node-local reserved block: it may straddle the new
                // mark. The next nextval re-reserves from the persisted mark.
                let mut local = self.seq_local.lock().await;
                if let Some(entry) = local.get_mut(&(*project, name.to_string())) {
                    entry.block_values.clear();
                }
                return Ok(());
            }
            // Lost the CAS race — another instance advanced the mark. Re-read
            // and retry; the next attempt observes the now-higher mark.
        }
        Err(BasinError::catalog(format!(
            "{project}: sequence {name:?} advance_sequence_floor contention exceeded retry budget"
        )))
    }

    async fn list_sequences(&self, project: &ProjectId) -> Vec<SequenceDef> {
        use futures::StreamExt;
        // Enumerate `{root}{project}/_sequences/{name}/` directories, decode the
        // live def for each (tombstoned ones are skipped by `seq_load_def`).
        let prefix_str = format!("{}{}/_sequences/", self.root, project);
        let list_prefix = OsPath::from(prefix_str.clone());
        let mut stream = self.store.list(Some(&list_prefix));
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let trimmed = prefix_str.trim_end_matches('/');
        while let Some(item) = stream.next().await {
            let Ok(meta) = item else { continue };
            let key = meta.location.as_ref();
            if let Some(rest) = key.strip_prefix(trimmed) {
                let rest = rest.trim_start_matches('/');
                if let Some(seg) = rest.split('/').next() {
                    if !seg.is_empty() {
                        names.insert(seg.to_string());
                    }
                }
            }
        }
        let mut out = Vec::new();
        for n in names {
            if let Ok(Some(def)) = self.seq_load_def(project, &n).await {
                out.push(def);
            }
        }
        out
    }

    // ---- Schemas (durable; explicit set + table-implied schemas) ----

    async fn list_schemas(&self, project: &ProjectId) -> Result<Vec<SchemaName>> {
        // Union of: the explicitly-created set (so an empty schema survives),
        // `public` (always present), and any schema implied by a live table.
        let mut set: std::collections::BTreeSet<SchemaName> =
            std::collections::BTreeSet::new();
        set.insert(SchemaName::public());
        if let Some(stored) = self
            .get_project_json::<Vec<String>>(project, "schemas.json")
            .await?
        {
            for s in stored {
                if let Ok(name) = SchemaName::new(s) {
                    set.insert(name);
                }
            }
        }
        // Schemas implied by existing tables (covers reserved schemas created
        // implicitly via `create_table_qualified` without an explicit
        // `create_schema`).
        for qt in self.list_tables_qualified(project).await? {
            set.insert(qt.schema);
        }
        Ok(set.into_iter().collect())
    }

    async fn create_schema(&self, project: &ProjectId, schema: &SchemaName) -> Result<()> {
        // Idempotent; `public` is implicit.
        if schema == &SchemaName::public() {
            return Ok(());
        }
        let mut stored = self
            .get_project_json::<Vec<String>>(project, "schemas.json")
            .await?
            .unwrap_or_default();
        let s = schema.to_string();
        if !stored.iter().any(|e| e == &s) {
            stored.push(s);
            self.put_project_json(project, "schemas.json", &stored).await?;
        }
        Ok(())
    }

    async fn drop_schema(
        &self,
        project: &ProjectId,
        schema: &SchemaName,
        cascade: bool,
    ) -> Result<()> {
        if schema == &SchemaName::public() {
            return Err(BasinError::catalog("cannot drop the public schema"));
        }
        let stored = self
            .get_project_json::<Vec<String>>(project, "schemas.json")
            .await?
            .unwrap_or_default();
        let tables_in_schema: Vec<QualifiedTableName> = self
            .list_tables_qualified(project)
            .await?
            .into_iter()
            .filter(|qt| &qt.schema == schema)
            .collect();
        // Exist iff in the explicit set or implied by a live table.
        let exists = stored.iter().any(|e| e == &schema.to_string())
            || !tables_in_schema.is_empty();
        if !exists {
            return Err(BasinError::not_found(format!(
                "{project}: schema {schema:?}"
            )));
        }
        if !cascade && !tables_in_schema.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop schema {schema}: {} table(s) still exist (use CASCADE to drop them)",
                tables_in_schema.len()
            )));
        }
        if cascade {
            for qt in &tables_in_schema {
                self.drop_table_q(project, qt).await?;
            }
        }
        let remaining: Vec<String> = stored
            .into_iter()
            .filter(|e| e != &schema.to_string())
            .collect();
        self.put_project_json(project, "schemas.json", &remaining).await?;
        Ok(())
    }

    // Bare `fork_table`: resolve the source schema, fork into `public`,
    // mirroring `InMemoryCatalog::fork_table`.
    async fn fork_table(
        &self,
        project: &ProjectId,
        src_table: &TableName,
        dst_table: &TableName,
    ) -> Result<TableMetadata> {
        let qsrc = self.resolve_qtable(project, src_table).await;
        let qdst = QualifiedTableName::in_public(dst_table.clone());
        self.fork_table_qualified(project, &qsrc, &qdst).await
    }

    // -----------------------------------------------------------------------
    // Schema-qualified API (ADR 0022). Unlike the trait defaults (which reject
    // any non-public schema), these honour the caller's schema directly so
    // reserved-schema tables (e.g. `auth.users`) are first-class. Each forwards
    // to the same `*_q` worker the bare methods use, keying by `(project,
    // schema, table)`.
    // -----------------------------------------------------------------------

    async fn create_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: Arc<Schema>,
    ) -> Result<TableMetadata> {
        self.create_table_q(project, qtable, schema.as_ref()).await
    }

    async fn load_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        self.load_table_q(project, qtable).await
    }

    async fn drop_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<()> {
        self.drop_table_q(project, qtable).await
    }

    async fn rename_table_qualified(
        &self,
        project: &ProjectId,
        old: &QualifiedTableName,
        new: &QualifiedTableName,
    ) -> Result<()> {
        self.rename_table_q(project, old, new).await
    }

    async fn list_tables_qualified(&self, project: &ProjectId) -> Result<Vec<QualifiedTableName>> {
        // Enumerate every schema under the project prefix; drop tombstoned
        // tables. Returns correctly-qualified names (schema carried verbatim),
        // matching `InMemoryCatalog::list_tables_qualified`.
        let all = self.list_schema_table_names(project).await?;
        let mut out = Vec::new();
        for qt in all {
            match self.load_current(project, &qt).await {
                Ok(_) => out.push(qt),
                Err(BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        out.sort();
        Ok(out)
    }

    async fn append_data_files_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        self.commit_snapshot(
            project,
            qtable,
            expected_snapshot,
            SnapshotOperation::Append,
            Vec::new(),
            files,
        )
        .await
    }

    async fn replace_data_files_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        self.commit_snapshot(
            project,
            qtable,
            expected_snapshot,
            SnapshotOperation::Replace,
            removed_paths,
            added_files,
        )
        .await
    }

    async fn list_snapshots_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Vec<Snapshot>> {
        self.list_snapshots_q(project, qtable).await
    }

    async fn fork_table_qualified(
        &self,
        project: &ProjectId,
        src: &QualifiedTableName,
        dst: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        // Copy the source manifest verbatim (paths included; no bytes copied)
        // into a fresh genesis-versioned manifest at the destination key.
        let (_v, manifest) = self.load_current(project, src).await?;
        match self.load_current(project, dst).await {
            Ok(_) => {
                return Err(BasinError::catalog(format!(
                    "fork_table: {project}/{dst} already exists"
                )))
            }
            Err(BasinError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let mut forked = (*manifest).clone();
        forked.version = 0;
        forked.dropped = false;
        // A fork starts with a clean GC orphan list.
        forked.gc_orphan_paths = Vec::new();
        if !self.put_manifest_create(project, dst, 0, &forked).await? {
            let v = self.resolve_head_version(project, dst).await?.unwrap_or(0);
            forked.version = v + 1;
            if !self
                .put_manifest_create(project, dst, forked.version, &forked)
                .await?
            {
                return Err(BasinError::catalog(format!(
                    "fork_table: {project}/{dst} lost create race"
                )));
            }
        }
        self.after_commit(project, dst, forked.version, forked.clone()).await;
        // Carry over any per-partition data-file segments (sharded ingest data).
        self.copy_partition_segments(project, src, project, dst).await?;
        self.load_unioned(project, dst, &forked).await
    }

    async fn fork_table_to_project(
        &self,
        src_project: &ProjectId,
        src_table: &TableName,
        dst_project: &ProjectId,
        dst_table: &TableName,
    ) -> Result<TableMetadata> {
        let qsrc = self.resolve_qtable(src_project, src_table).await;
        let qdst = QualifiedTableName::in_public(dst_table.clone());
        let (_v, manifest) = self.load_current(src_project, &qsrc).await?;
        match self.load_current(dst_project, &qdst).await {
            Ok(_) => {
                return Err(BasinError::catalog(format!(
                    "fork_table_to_project: {dst_project}/{qdst} already exists"
                )))
            }
            Err(BasinError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let mut forked = (*manifest).clone();
        forked.version = 0;
        forked.dropped = false;
        forked.gc_orphan_paths = Vec::new();
        if !self
            .put_manifest_create(dst_project, &qdst, 0, &forked)
            .await?
        {
            let v = self
                .resolve_head_version(dst_project, &qdst)
                .await?
                .unwrap_or(0);
            forked.version = v + 1;
            if !self
                .put_manifest_create(dst_project, &qdst, forked.version, &forked)
                .await?
            {
                return Err(BasinError::catalog(format!(
                    "fork_table_to_project: {dst_project}/{qdst} lost create race"
                )));
            }
        }
        self.after_commit(dst_project, &qdst, forked.version, forked.clone())
            .await;
        // Carry over any per-partition data-file segments (sharded ingest data).
        self.copy_partition_segments(src_project, &qsrc, dst_project, &qdst)
            .await?;
        self.load_unioned(dst_project, &qdst, &forked).await
    }

    async fn rollback_to_snapshot_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        self.rollback_to_snapshot_q(project, qtable, snapshot_id)
            .await
    }

    async fn set_partition_spec_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.partition_spec = spec.clone())
            .await?;
        Ok(())
    }

    async fn set_rls_state_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| {
            m.rls_enabled = rls_enabled;
            m.policies = policies.clone();
        })
        .await?;
        Ok(())
    }

    async fn set_tier_policy_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| {
            m.cold_after_seconds = cold_after_seconds;
            m.cold_age_column = cold_age_column.clone();
        })
        .await?;
        Ok(())
    }

    async fn set_bloom_filter_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.bloom_filter_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_row_group_rows_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rows: Option<usize>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.row_group_rows = rows)
            .await?;
        Ok(())
    }

    async fn set_schema_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: Schema,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.schema = schema.clone())
            .await?;
        Ok(())
    }

    async fn set_continuous_aggregate_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.continuous_aggregate = def.clone())
            .await?;
        Ok(())
    }

    async fn set_cluster_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.cluster_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_home_region_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        region: Option<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, qtable, |m| m.home_region = region.clone())
            .await?;
        Ok(())
    }

    async fn create_index_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        self.create_index_q(project, qtable, name, columns, if_not_exists, "btree", None)
            .await
    }

    async fn drop_index_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
    ) -> Result<()> {
        self.drop_index_q(project, qtable, name).await
    }
}

impl ObjectStoreCatalog {
    fn project_meta_key(&self, project: &ProjectId, name: &str) -> OsPath {
        OsPath::from(format!("{}{}/_project/{}", self.root, project, name))
    }

    /// Global key for a project's in-flight migration intent (#37). Lives under
    /// a single global prefix so `list_migration_intents` LISTs every in-flight
    /// migration in one shot (resume-on-restart + bounded-concurrency).
    fn migration_intent_key(&self, project: &ProjectId) -> OsPath {
        OsPath::from(format!("{}_bucket_pool/migrations/{}.json", self.root, project))
    }

    async fn put_project_json<T: Serialize>(
        &self,
        project: &ProjectId,
        name: &str,
        value: &T,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| BasinError::catalog(format!("serialise {name}: {e}")))?;
        self.store
            .put_opts(
                &self.project_meta_key(project, name),
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| storage_err("put project meta", e))?;
        self.bump_epoch();
        Ok(())
    }

    async fn get_project_json<T: for<'de> Deserialize<'de>>(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Result<Option<T>> {
        match self.store.get(&self.project_meta_key(project, name)).await {
            Ok(res) => {
                let bytes = res.bytes().await.map_err(|e| storage_err("read project meta", e))?;
                let v = serde_json::from_slice(&bytes)
                    .map_err(|e| BasinError::catalog(format!("decode {name}: {e}")))?;
                Ok(Some(v))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err("get project meta", e)),
        }
    }

    // ---- SQL functions (durable, last-writer-wins like project metadata) ----

    fn sql_function_key(&self, project: &ProjectId, name: &str) -> OsPath {
        OsPath::from(format!(
            "{}{}/_functions/{}.json",
            self.root,
            project,
            sanitize(name)
        ))
    }

    fn sql_function_dir(&self, project: &ProjectId) -> String {
        format!("{}{}/_functions/", self.root, project)
    }

    // ---- Sequences (durable high-water mark + block allocation) ----
    //
    // Layout under `{root}{project}/_sequences/{name}/`:
    //   def.json                  — the immutable `SequenceDef`, written once
    //                               via PutMode::Create at create time.
    //   hwm/v{N:020}.json         — high-water-mark log. Each object holds a
    //                               `SeqHwm { last, started }` record. "Current"
    //                               = highest N present; advancing reserves a
    //                               block by writing v{N+1} via PutMode::Create
    //                               (the create-if-absent CAS the manifest log
    //                               already uses). The loser of a race re-reads
    //                               and retries (bounded). Old versions are kept
    //                               as history; the high-water mark only ever
    //                               moves forward, so a restart resumes from the
    //                               persisted max and never reuses a value.
    //   TOMBSTONE                 — present iff the sequence was dropped.

    fn seq_dir(&self, project: &ProjectId, name: &str) -> String {
        format!("{}{}/_sequences/{}/", self.root, project, sanitize(name))
    }

    fn seq_def_key(&self, project: &ProjectId, name: &str) -> OsPath {
        OsPath::from(format!("{}def.json", self.seq_dir(project, name)))
    }

    fn seq_tombstone_key(&self, project: &ProjectId, name: &str) -> OsPath {
        OsPath::from(format!("{}TOMBSTONE", self.seq_dir(project, name)))
    }

    fn seq_hwm_key(&self, project: &ProjectId, name: &str, version: u64) -> OsPath {
        OsPath::from(format!(
            "{}hwm/v{version:020}.json",
            self.seq_dir(project, name)
        ))
    }

    /// Block size for sequence reservation. Configurable via `BASIN_SEQ_BLOCK`
    /// (clamped to `>= 1`); defaults to 64. A larger block means fewer CAS
    /// round-trips for SERIAL bulk inserts at the cost of larger gaps on
    /// crash/restart; the auth-provisioning path (a handful of values) keeps
    /// gaps tiny even at the default.
    fn seq_block(&self) -> u64 {
        if let Some(b) = self.seq_block_override {
            return b.max(1);
        }
        std::env::var("BASIN_SEQ_BLOCK")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&b| b >= 1)
            .unwrap_or(64)
    }

    /// Is this sequence present (created and not tombstoned)?
    async fn seq_exists(&self, project: &ProjectId, name: &str) -> Result<bool> {
        if self.seq_is_tombstoned(project, name).await? {
            return Ok(false);
        }
        match self.store.head(&self.seq_def_key(project, name)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(storage_err("head sequence def", e)),
        }
    }

    /// Best-effort delete of every hwm object under a sequence's `hwm/`
    /// prefix. Used on drop and on resurrect-after-drop to clear stale state.
    async fn seq_purge(&self, project: &ProjectId, name: &str) -> Result<()> {
        use futures::StreamExt;
        let prefix = OsPath::from(format!("{}hwm/", self.seq_dir(project, name)));
        let mut stream = self.store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => keys.push(meta.location),
                Err(e) => return Err(storage_err("list sequence hwm for purge", e)),
            }
        }
        for k in keys {
            let _ = self.store.delete(&k).await;
        }
        Ok(())
    }

    async fn seq_is_tombstoned(&self, project: &ProjectId, name: &str) -> Result<bool> {
        match self.store.head(&self.seq_tombstone_key(project, name)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(storage_err("head sequence tombstone", e)),
        }
    }

    async fn seq_load_def(&self, project: &ProjectId, name: &str) -> Result<Option<SequenceDef>> {
        if self.seq_is_tombstoned(project, name).await? {
            return Ok(None);
        }
        match self.store.get(&self.seq_def_key(project, name)).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| storage_err("read sequence def", e))?;
                let def = serde_json::from_slice(&bytes).map_err(|e| {
                    BasinError::catalog(format!("decode sequence def {project}/{name}: {e}"))
                })?;
                Ok(Some(def))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err("get sequence def", e)),
        }
    }

    /// Highest high-water-mark version present, with its decoded record.
    /// Returns `(version, hwm)`; `version == 0` with `started == false`
    /// genesis when no hwm object exists yet.
    async fn seq_read_hwm(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Result<(u64, SeqHwm)> {
        use futures::StreamExt;
        let prefix = OsPath::from(format!("{}hwm/", self.seq_dir(project, name)));
        let mut stream = self.store.list(Some(&prefix));
        let mut max: Option<u64> = None;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list sequence hwm", e))?;
            let key = meta.location.as_ref();
            if let Some(file) = key.rsplit('/').next() {
                if let Some(num) = file.strip_prefix('v').and_then(|s| s.strip_suffix(".json")) {
                    if let Ok(v) = num.parse::<u64>() {
                        max = Some(max.map_or(v, |m| m.max(v)));
                    }
                }
            }
        }
        match max {
            None => Ok((0, SeqHwm::genesis())),
            Some(v) => {
                let res = self
                    .store
                    .get(&self.seq_hwm_key(project, name, v))
                    .await
                    .map_err(|e| storage_err("get sequence hwm", e))?;
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| storage_err("read sequence hwm", e))?;
                let hwm = serde_json::from_slice(&bytes).map_err(|e| {
                    BasinError::catalog(format!("decode sequence hwm {project}/{name}: {e}"))
                })?;
                Ok((v, hwm))
            }
        }
    }

    /// CAS-write hwm version `version` via create-if-absent. `true` = we won,
    /// `false` = another node already wrote that version (re-read + retry).
    async fn seq_put_hwm_create(
        &self,
        project: &ProjectId,
        name: &str,
        version: u64,
        hwm: &SeqHwm,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(hwm)
            .map_err(|e| BasinError::catalog(format!("serialise sequence hwm: {e}")))?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.seq_hwm_key(project, name, version), Bytes::from(bytes).into(), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(storage_err("put sequence hwm", e)),
        }
    }

    /// Reserve a contiguous block of up to `block` values by CAS-advancing the
    /// persisted high-water mark. Returns the reserved values in hand-out
    /// order (`VecDeque`), plus the new persisted `last`. Disjoint across nodes
    /// because the winner is the only one that advances a given hwm version.
    async fn seq_reserve_block(
        &self,
        def: &SequenceDef,
        block: u64,
    ) -> Result<std::collections::VecDeque<i64>> {
        const MAX_RETRIES: u32 = 64;
        for _ in 0..MAX_RETRIES {
            let (version, hwm) = self.seq_read_hwm(&def.project, &def.name).await?;
            // Compute up to `block` successive values starting from the current
            // persisted (last, started), mirroring `compute_next` exactly so
            // increment / min / max / cycle / exhaustion all match InMemory.
            let mut values = std::collections::VecDeque::new();
            let mut last = hwm.last;
            let mut started = hwm.started;
            for _ in 0..block {
                match compute_next(def, last, started) {
                    Ok(v) => {
                        values.push_back(v);
                        last = v;
                        started = true;
                    }
                    Err(SequenceError::Exhausted) => break,
                    Err(SequenceError::InvalidIncrement) => {
                        return Err(BasinError::InvalidSchema(format!(
                            "{}: sequence {:?} has zero increment",
                            def.project, def.name
                        )))
                    }
                }
            }
            if values.is_empty() {
                // No room left in the value space and the very first step is
                // already exhausted.
                return Err(BasinError::catalog(format!(
                    "{}: sequence {:?} exhausted",
                    def.project, def.name
                )));
            }
            let next_hwm = SeqHwm { last, started };
            if self
                .seq_put_hwm_create(&def.project, &def.name, version + 1, &next_hwm)
                .await?
            {
                self.bump_epoch();
                return Ok(values);
            }
            // Lost the CAS race — another node reserved version+1. Re-read and
            // retry; the next attempt starts from the now-higher mark, so the
            // blocks are disjoint.
        }
        Err(BasinError::catalog(format!(
            "{}: sequence {:?} hwm contention exceeded retry budget",
            def.project, def.name
        )))
    }
}

/// Persisted high-water mark for a sequence. `last` is the last value logically
/// allocated (handed out to some node's block); `started` distinguishes "never
/// advanced" (next is `start`) from "advanced" (next is `compute_next(last)`).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeqHwm {
    last: i64,
    started: bool,
}

impl SeqHwm {
    fn genesis() -> Self {
        // `last` is unused while `started == false` (first hand-out is `start`).
        SeqHwm {
            last: 0,
            started: false,
        }
    }
}

/// Environment variable selecting the catalog backend. The **default**
/// (unset, or any value other than `object_store`) is unchanged — callers keep
/// using `InMemoryCatalog` / `PostgresCatalog` exactly as before. Setting
/// `BASIN_CATALOG_BACKEND=object_store` opts into the Basin-native shared
/// catalog built from the storage layer's own object store (no external DB).
pub const CATALOG_BACKEND_ENV: &str = "BASIN_CATALOG_BACKEND";

/// Returns `true` when the object-store catalog backend is selected via
/// [`CATALOG_BACKEND_ENV`]. The server's catalog-wiring point calls this to
/// decide whether to construct an [`ObjectStoreCatalog`] +
/// [`ObjectStoreLeaseRegistry`] (from `Storage::object_store_handle()`), and to
/// wire the shared `ObjectStoreLeaseRegistry` into the shard config. The
/// default branch (this returning `false`) leaves all existing behaviour and
/// tests untouched.
pub fn object_store_backend_selected() -> bool {
    std::env::var(CATALOG_BACKEND_ENV)
        .map(|v| v.eq_ignore_ascii_case("object_store"))
        .unwrap_or(false)
}

/// Build the Basin-native shared catalog **and** lease registry from one
/// shared object store. The two share the same store but live under distinct
/// prefixes (`_catalog/` and `_leases/`). The server wires the returned
/// `Arc<dyn LeaseRegistry>` into the shard config so per-partition ownership is
/// shared across nodes — replacing the per-process in-memory `LeaseRegistry`
/// that gives false safety across nodes.
pub fn build_object_store_backend(
    store: Arc<dyn ObjectStore>,
) -> (Arc<ObjectStoreCatalog>, Arc<ObjectStoreLeaseRegistry>) {
    let catalog = Arc::new(ObjectStoreCatalog::new(store.clone()));
    let leases = Arc::new(ObjectStoreLeaseRegistry::new(store));
    (catalog, leases)
}

/// Make a partition id safe for an object key segment.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// #27 removal bookkeeping shared by fold-replay and the commit hot path: a
/// removed path that is in the OPEN TAIL (not yet frozen) is dropped from the
/// tail in O(tail) (and never needs a tombstone); a path already sealed into a
/// FROZEN chunk becomes a tombstone (subtracted at reconstruction). Keeps the
/// folded view byte-identical to a freshly-committed warm view.
fn apply_removal_to_tail(
    path: &str,
    open_tail: &mut Vec<String>,
    tail_set: &mut std::collections::HashSet<String>,
    tombstones: &mut std::collections::HashSet<String>,
) {
    if tail_set.remove(path) {
        if let Some(pos) = open_tail.iter().position(|p| p == path) {
            open_tail.remove(pos);
        }
    } else {
        tombstones.insert(path.to_string());
    }
}

// ===========================================================================
// Shared lease registry on the same create-if-absent primitive.
// ===========================================================================

/// One lease epoch record, serialised at `{lease_root}{project}/{partition}/e{EPOCH:020}.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseRecord {
    holder: String,
    epoch: i64,
    granted_at_ms: i64,
    ttl_ms: i64,
}

impl LeaseRecord {
    fn expires_at_ms(&self) -> i64 {
        self.granted_at_ms.saturating_add(self.ttl_ms)
    }
}

/// Injectable clock so lease-expiry tests are deterministic. Defaults to real
/// wall-clock (`SystemTime`), matching how the rest of the shard code reads time.
pub trait LeaseClock: Send + Sync {
    /// Current time in Unix epoch milliseconds.
    fn now_ms(&self) -> i64;
}

/// Real wall-clock implementation.
pub struct SystemClock;

impl LeaseClock for SystemClock {
    fn now_ms(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Object-store-backed shared lease registry. Two engine nodes pointed at the
/// same store + prefix contend over the same epoch log; create-if-absent
/// guarantees exactly one winner per epoch.
pub struct ObjectStoreLeaseRegistry {
    store: Arc<dyn ObjectStore>,
    root: String,
    clock: Arc<dyn LeaseClock>,
}

impl ObjectStoreLeaseRegistry {
    /// Construct with the default `_leases/` prefix and the system clock.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_prefix(store, DEFAULT_LEASE_PREFIX)
    }

    /// Construct with an explicit prefix and the system clock.
    pub fn with_prefix(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self::with_prefix_and_clock(store, prefix, Arc::new(SystemClock))
    }

    /// Construct with an explicit prefix and an injected clock (tests).
    pub fn with_prefix_and_clock(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        clock: Arc<dyn LeaseClock>,
    ) -> Self {
        let mut root = prefix.to_string();
        if !root.is_empty() && !root.ends_with('/') {
            root.push('/');
        }
        Self { store, root, clock }
    }

    fn partition_dir(&self, project: &ProjectId, partition: &str) -> String {
        format!("{}{}/{}/", self.root, project, sanitize(partition))
    }

    fn epoch_key(&self, project: &ProjectId, partition: &str, epoch: i64) -> OsPath {
        OsPath::from(format!(
            "{}e{:020}.json",
            self.partition_dir(project, partition),
            epoch
        ))
    }

    /// Read the highest epoch record for `(project, partition)`, or `None`.
    async fn read_max(
        &self,
        project: &ProjectId,
        partition: &str,
    ) -> Result<Option<LeaseRecord>> {
        use futures::StreamExt;
        let prefix = OsPath::from(self.partition_dir(project, partition));
        let mut stream = self.store.list(Some(&prefix));
        let mut max_epoch: Option<i64> = None;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list leases", e))?;
            let key = meta.location.as_ref();
            if let Some(file) = key.rsplit('/').next() {
                if let Some(num) = file.strip_prefix('e').and_then(|s| s.strip_suffix(".json")) {
                    if let Ok(ep) = num.parse::<i64>() {
                        max_epoch = Some(max_epoch.map_or(ep, |m| m.max(ep)));
                    }
                }
            }
        }
        let Some(ep) = max_epoch else { return Ok(None) };
        let res = self
            .store
            .get(&self.epoch_key(project, partition, ep))
            .await
            .map_err(|e| storage_err("get lease record", e))?;
        let bytes = res.bytes().await.map_err(|e| storage_err("read lease", e))?;
        let rec: LeaseRecord = serde_json::from_slice(&bytes)
            .map_err(|e| BasinError::catalog(format!("decode lease: {e}")))?;
        Ok(Some(rec))
    }

    async fn put_epoch_create(
        &self,
        project: &ProjectId,
        partition: &str,
        rec: &LeaseRecord,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(rec)
            .map_err(|e| BasinError::catalog(format!("serialise lease: {e}")))?;
        let key = self.epoch_key(project, partition, rec.epoch);
        match self
            .store
            .put_opts(
                &key,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(storage_err("put lease", e)),
        }
    }

    fn to_lease(&self, project: &ProjectId, partition: &str, rec: &LeaseRecord) -> Lease {
        Lease {
            project: *project,
            partition_id: partition.to_string(),
            holder: rec.holder.clone(),
            epoch: rec.epoch,
            granted_at: chrono::DateTime::from_timestamp_millis(rec.granted_at_ms)
                .unwrap_or_else(Utc::now),
            expires_at: chrono::DateTime::from_timestamp_millis(rec.expires_at_ms())
                .unwrap_or_else(Utc::now),
        }
    }
}

#[async_trait]
impl LeaseRegistry for ObjectStoreLeaseRegistry {
    async fn acquire(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<Lease>> {
        let now = self.clock.now_ms();
        let ttl_ms = ttl.as_millis() as i64;
        // Retry the read→create loop a bounded number of times: a create-if-absent
        // loss means another racer advanced the epoch; we re-read and re-evaluate.
        for _ in 0..MAX_DDL_RETRIES {
            let current = self.read_max(project, partition_id).await?;
            let (target_epoch, may_grant) = match &current {
                None => (1, true),
                Some(rec) => {
                    let expired = rec.expires_at_ms() <= now;
                    if rec.holder == holder || expired {
                        (rec.epoch + 1, true)
                    } else {
                        (0, false)
                    }
                }
            };
            if !may_grant {
                return Ok(None);
            }
            let rec = LeaseRecord {
                holder: holder.to_string(),
                epoch: target_epoch,
                granted_at_ms: now,
                ttl_ms,
            };
            if self.put_epoch_create(project, partition_id, &rec).await? {
                return Ok(Some(self.to_lease(project, partition_id, &rec)));
            }
            // Lost the race for this epoch — someone else created it. Re-read.
        }
        // Heavy contention: report the current owner state.
        match self.read_max(project, partition_id).await? {
            Some(rec) if rec.expires_at_ms() > now && rec.holder != holder => Ok(None),
            _ => Ok(None),
        }
    }

    async fn renew(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        epoch: i64,
        ttl: Duration,
    ) -> Result<bool> {
        let now = self.clock.now_ms();
        let ttl_ms = ttl.as_millis() as i64;
        let Some(current) = self.read_max(project, partition_id).await? else {
            return Ok(false);
        };
        // Must still be us, at exactly this epoch, not yet expired.
        if current.holder != holder || current.epoch != epoch || current.expires_at_ms() <= now {
            return Ok(false);
        }
        // Renew does NOT bump the epoch (steady-state heartbeat). We refresh by
        // writing a new epoch object that advances the log but records the SAME
        // logical fencing epoch in the body, so owner_of still reports `epoch`.
        // To keep "one winner per object key" we advance the key counter while
        // preserving the body epoch.
        let next_key_epoch = current.epoch + 1;
        let rec = LeaseRecord {
            holder: holder.to_string(),
            epoch,
            granted_at_ms: now,
            ttl_ms,
        };
        // Write at a fresh key counter so the create-if-absent invariant holds.
        let bytes = serde_json::to_vec(&rec)
            .map_err(|e| BasinError::catalog(format!("serialise lease: {e}")))?;
        let key = self.epoch_key(project, partition_id, next_key_epoch);
        match self
            .store
            .put_opts(
                &key,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            // Lost the renew race: someone stole the lease at a higher key epoch.
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(storage_err("renew lease", e)),
        }
    }

    async fn release(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
    ) -> Result<bool> {
        // Best-effort: write a tombstone epoch (expired) so owner_of reports no
        // live owner. We do not delete the history (immutable epoch log).
        let Some(current) = self.read_max(project, partition_id).await? else {
            return Ok(false);
        };
        if current.holder != holder {
            return Ok(false);
        }
        let now = self.clock.now_ms();
        let rec = LeaseRecord {
            holder: holder.to_string(),
            epoch: current.epoch + 1,
            granted_at_ms: now - 1,
            ttl_ms: 0, // already expired
        };
        // Best-effort create; a concurrent steal at the same key is fine.
        let _ = self.put_epoch_create(project, partition_id, &rec).await?;
        Ok(true)
    }

    async fn owner_of(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<(String, i64)>> {
        let now = self.clock.now_ms();
        match self.read_max(project, partition_id).await? {
            Some(rec) if rec.expires_at_ms() > now => Ok(Some((rec.holder, rec.epoch))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnStats;
    use arrow_schema::{DataType, Field};
    use object_store::memory::InMemory;
    use std::sync::atomic::AtomicI64;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn file(path: &str, rows: u64) -> DataFileRef {
        DataFileRef {
            path: path.to_string(),
            size_bytes: rows * 10,
            row_count: rows,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        }
    }

    /// A file carrying per-column `id` stats (min/max as 8-byte LE i64, plus a
    /// null count). Used to assert the #27 chunked-baseline path preserves
    /// `column_stats` byte-for-byte through chunk serialize → fold — the
    /// stats the metadata range-aggregate fast path classifies straddlers by.
    fn file_with_stats(path: &str, rows: u64, id_min: i64, id_max: i64, nulls: u64) -> DataFileRef {
        let mut cs = std::collections::BTreeMap::new();
        cs.insert(
            "id".to_string(),
            ColumnStats {
                null_count: Some(nulls),
                min_bytes: Some(id_min.to_le_bytes().to_vec()),
                max_bytes: Some(id_max.to_le_bytes().to_vec()),
                sum_bytes: None,
            },
        );
        DataFileRef {
            path: path.to_string(),
            size_bytes: rows * 10,
            row_count: rows,
            column_stats: cs,
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        }
    }

    fn cat() -> ObjectStoreCatalog {
        ObjectStoreCatalog::new(Arc::new(InMemory::new()))
    }

    // --- Test 1: basic round-trip -----------------------------------------

    #[tokio::test]
    async fn basic_round_trip_append_chain() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("events").unwrap();
        c.create_namespace(&p).await.unwrap();
        let meta = c.create_table(&p, &t, &schema()).await.unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId::GENESIS);

        // Non-partitioned append (the back-compat path): commits land on the
        // table META chain and the unioned read presents them. With the
        // partition-sharded layout the table-level snapshot id is SYNTHETIC
        // (GENESIS when empty, else `SnapshotId(1)` — see `load_unioned`), so a
        // commit's returned `current_snapshot` is the synthetic union id, not a
        // per-commit monotonic id.
        let mut expected = SnapshotId::GENESIS;
        for i in 0..5 {
            let m = c
                .append_data_files(&p, &t, expected, vec![file(&format!("f{i}.parquet"), 100)])
                .await
                .unwrap();
            expected = m.current_snapshot;
        }

        let loaded = c.load_table(&p, &t).await.unwrap();
        assert_eq!(loaded.current_snapshot, SnapshotId(1));
        // Correctness that matters: every committed file is present (no loss,
        // no double-count) in the unioned read.
        let live = loaded.live_data_files();
        assert_eq!(live.len(), 5);

        // `list_snapshots` reflects the synthetic union chain (genesis + a
        // single union snapshot). Fine-grained per-commit time-travel collapses
        // for the object-store catalog; this is the documented reduction.
        let snaps = c.list_snapshots(&p, &t).await.unwrap();
        assert_eq!(snaps.len(), 2); // genesis + union
        assert_eq!(c.current_snapshot_id(&p, &t).await.unwrap(), SnapshotId(1));

        // Time-travel to a fine-grained historical id is no longer retained:
        // `load_table_at_snapshot` falls back to FeatureNotSupported, and the
        // caller serves a current read instead of the wrong point-in-time.
        let at2 = c.load_table_at_snapshot(&p, &t, SnapshotId(2)).await;
        assert!(matches!(at2, Err(BasinError::FeatureNotSupported(_))), "got {at2:?}");
    }

    // --- Test 2: same-CHAIN contention (META chain, back-compat path) ------
    //
    // The non-partitioned `append_data_files` now resolves the META-chain OCC
    // INTERNALLY (read-modify-write with bounded retry), so concurrent racers
    // on the same table all LAND rather than all-but-one conflicting. The
    // load-bearing property is no-loss / no-double-count: every file committed
    // appears exactly once in the unioned read. (The hard CommitConflict for
    // the SAME-partition data-file chain is exercised by
    // `same_partition_occ_one_winner` below.)
    #[tokio::test]
    async fn same_chain_contention_all_land_no_loss() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = Arc::new(ObjectStoreCatalog::new(store));
        let p = ProjectId::new();
        let t = TableName::new("hot").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        const ROUNDS: usize = 25;
        const RACERS: usize = 6;
        let mut total_committed_files = 0u64;

        for round in 0..ROUNDS {
            let expected = c.current_snapshot_id(&p, &t).await.unwrap();
            let mut handles = Vec::new();
            for r in 0..RACERS {
                let c = c.clone();
                let p = p;
                let t = t.clone();
                handles.push(tokio::spawn(async move {
                    c.append_data_files(
                        &p,
                        &t,
                        expected,
                        vec![file(&format!("r{round}_w{r}.parquet"), 1)],
                    )
                    .await
                }));
            }
            for h in handles {
                h.await.unwrap().expect("internal RMW retry lands every commit");
            }
            total_committed_files += RACERS as u64;
        }

        // Every file landed exactly once; the unioned read is complete.
        let meta = c.load_table(&p, &t).await.unwrap();
        assert_eq!(meta.live_data_files().len() as u64, total_committed_files);
        // Synthetic union id once data exists.
        assert_eq!(meta.current_snapshot, SnapshotId(1));
    }

    // --- Test 2b: contention with retry-to-completion ---------------------

    #[tokio::test]
    async fn split_brain_with_retry_all_land() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = Arc::new(ObjectStoreCatalog::new(store));
        let p = ProjectId::new();
        let t = TableName::new("retry").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        const RACERS: usize = 8;
        let mut handles = Vec::new();
        for r in 0..RACERS {
            let c = c.clone();
            let p = p;
            let t = t.clone();
            handles.push(tokio::spawn(async move {
                // Each racer retries on conflict until its single file lands.
                loop {
                    let expected = c.current_snapshot_id(&p, &t).await.unwrap();
                    match c
                        .append_data_files(&p, &t, expected, vec![file(&format!("w{r}.parquet"), 1)])
                        .await
                    {
                        Ok(_) => break,
                        Err(BasinError::CommitConflict(_)) => continue,
                        Err(e) => panic!("unexpected: {e:?}"),
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let meta = c.load_table(&p, &t).await.unwrap();
        // Genesis + RACERS commits; all files present (no loss), synthetic id.
        assert_eq!(meta.current_snapshot, SnapshotId(1));
        assert_eq!(meta.live_data_files().len(), RACERS);
    }

    // --- Test 2c: MULTI-WRITER, NO CONTENTION (the scaling fix) -----------
    //
    // Two ObjectStoreCatalog instances (= two nodes) over ONE shared store
    // commit data files to DIFFERENT partitions of the SAME table, heavily
    // interleaved. Because each partition owns its own segment chain, NO commit
    // ever raises a CommitConflict against another partition. A fresh third
    // instance then reads the table and sees the UNION of every partition's
    // files (correct total, no loss, no dup).
    #[tokio::test]
    async fn multi_writer_cross_partition_no_contention() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = Arc::new(ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX));
        let node_b = Arc::new(ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX));
        let p = ProjectId::new();
        let t = TableName::new("ingest").unwrap();
        node_a.create_namespace(&p).await.unwrap();
        node_a.create_table(&p, &t, &schema()).await.unwrap();

        const ROUNDS: usize = 40;
        // Node A owns partitions s0/s2; node B owns s1/s3 — deterministic,
        // disjoint ownership (exactly the forwarding model).
        let a_parts = ["s0", "s2"];
        let b_parts = ["s1", "s3"];

        let conflicts = Arc::new(AtomicI64::new(0));
        let committed = Arc::new(AtomicI64::new(0));

        let mut handles = Vec::new();
        for (node, parts) in [(node_a.clone(), a_parts), (node_b.clone(), b_parts)] {
            for pid in parts {
                let node = node.clone();
                let p = p;
                let t = t.clone();
                let conflicts = conflicts.clone();
                let committed = committed.clone();
                handles.push(tokio::spawn(async move {
                    for round in 0..ROUNDS {
                        let exp = node
                            .current_snapshot_id_in_partition(&p, &t, pid)
                            .await
                            .unwrap();
                        match node
                            .append_data_files_in_partition(
                                &p,
                                &t,
                                pid,
                                exp,
                                vec![file(&format!("{pid}_r{round}.parquet"), 1)],
                            )
                            .await
                        {
                            Ok(_) => {
                                committed.fetch_add(1, SeqCst);
                            }
                            Err(BasinError::CommitConflict(_)) => {
                                conflicts.fetch_add(1, SeqCst);
                            }
                            Err(e) => panic!("unexpected: {e:?}"),
                        }
                    }
                }));
            }
        }
        for h in handles {
            h.await.unwrap();
        }

        let total_partitions = a_parts.len() + b_parts.len();
        let expected_total = (total_partitions * ROUNDS) as i64;
        assert_eq!(
            conflicts.load(SeqCst),
            0,
            "cross-partition commits must NEVER contend"
        );
        assert_eq!(committed.load(SeqCst), expected_total, "all commits succeed");

        // Fresh third instance: unioned read sees every partition's files.
        let node_c = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let meta = node_c.load_table(&p, &t).await.unwrap();
        let live = meta.live_data_files();
        assert_eq!(
            live.len() as i64,
            expected_total,
            "union total correct: no loss, no double-count"
        );
        // No duplicate paths.
        let mut paths: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
        paths.sort();
        let n = paths.len();
        paths.dedup();
        assert_eq!(paths.len(), n, "no duplicate paths in unioned set");
    }

    // --- Test 2d: SAME-partition OCC still gives exactly one winner --------
    #[tokio::test]
    async fn same_partition_occ_one_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = Arc::new(ObjectStoreCatalog::new(store));
        let p = ProjectId::new();
        let t = TableName::new("onepart").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Two commits to the SAME partition with the SAME expected version:
        // exactly one wins, the other gets a per-partition CommitConflict.
        let expected = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        let c1 = c.clone();
        let c2 = c.clone();
        let (p1, p2) = (p, p);
        let (t1, t2) = (t.clone(), t.clone());
        let h1 = tokio::spawn(async move {
            c1.append_data_files_in_partition(&p1, &t1, "p0", expected, vec![file("a.parquet", 1)])
                .await
        });
        let h2 = tokio::spawn(async move {
            c2.append_data_files_in_partition(&p2, &t2, "p0", expected, vec![file("b.parquet", 1)])
                .await
        });
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        let wins = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(BasinError::CommitConflict(_))))
            .count();
        assert_eq!(wins, 1, "exactly one same-partition winner");
        assert_eq!(conflicts, 1, "the other gets a per-partition conflict");

        // The loser retries against the now-current partition version and lands.
        let exp2 = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp2, vec![file("c.parquet", 1)])
            .await
            .unwrap();
        assert_eq!(c.load_table(&p, &t).await.unwrap().live_data_files().len(), 2);
    }

    // --- Test 2c-flat-1: COMMIT IS O(1) — delta size independent of file count
    //
    // The flat-scale regression guard. Seed a partition with MANY existing files
    // (one per commit), then assert the object written by the NEXT commit
    // contains only THIS commit's added file(s) — NOT the cumulative set. This is
    // what keeps per-commit PUT cost (and thus ingest throughput) constant as a
    // partition accumulates files.
    #[tokio::test]
    async fn part_commit_is_o1_delta_independent_of_file_count() {
        // Big K so the seeding commits stay deltas (no baseline interferes with
        // the size assertion at the measured commit).
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_part_compact_every(store.clone(), 100_000);
        let p = ProjectId::new();
        let t = TableName::new("flat").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let qt = c.resolve_qtable(&p, &t).await;

        // Seed 200 files via 200 single-file commits to one partition.
        const SEED: usize = 200;
        for i in 0..SEED {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("seed{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // The folded live set has all 200 files (correctness).
        assert_eq!(c.load_table(&p, &t).await.unwrap().live_data_files().len(), SEED);

        // The NEXT commit writes a delta object carrying ONLY the 1 added file.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("hot.parquet", 1)])
            .await
            .unwrap();
        let head_v = c.resolve_part_head_version(&p, &qt, 0, "p0").await.unwrap().unwrap();
        let key = c.part_segment_key(&p, &qt, 0, "p0", head_v);
        let bytes = c.store.get(&key).await.unwrap().bytes().await.unwrap();
        let obj: PartSegmentObject = serde_json::from_slice(&bytes).unwrap();

        assert!(obj.base_version.is_some(), "hot commit is a delta, not a baseline");
        assert!(obj.baseline.is_none(), "delta carries no cumulative baseline");
        assert_eq!(
            obj.delta.data_files.len(),
            1,
            "delta object holds ONLY this commit's file, not the {SEED}+ cumulative set"
        );
        assert_eq!(obj.delta.data_files[0].path, "hot.parquet");

        // The serialized object is small and its size does NOT scale with SEED:
        // bound it well under what 201 files would require.
        assert!(
            bytes.len() < 4096,
            "delta object stays tiny ({} bytes) regardless of {SEED} existing files",
            bytes.len()
        );
    }

    // --- Regression: stale head pointer must not livelock commits -------------
    //
    // Reproduces the "lost commit race at partition version N" wedge observed at
    // ~143M rows on dev: a partition segment was committed (create-if-absent PUT
    // succeeded) but the best-effort head-pointer overwrite that should have
    // advanced the pointer was lost — process killed or the store PUT failed
    // right after. With the old resolver, the stale pointer pinned EVERY future
    // commit to recompute the same `new_version`, lose the create race against
    // the already-existing segment, and conflict forever (the shard tail could
    // never drain; restarts replayed the tail and re-wedged identically). The
    // resolver must detect the lag and recover the true max version.
    #[tokio::test]
    async fn stale_head_pointer_self_heals_no_commit_livelock() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_part_compact_every(store.clone(), 100_000);
        let p = ProjectId::new();
        let t = TableName::new("heal").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let qt = c.resolve_qtable(&p, &t).await;

        // Build a segment chain of several commits on partition p0.
        for i in 0..4 {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("f{i}.parquet"), 1)])
                .await
                .unwrap();
        }
        let head_v = c.resolve_part_head_version(&p, &qt, 0, "p0").await.unwrap().unwrap();
        assert!(head_v >= 4, "seeded at least 4 segment versions, got {head_v}");

        // SIMULATE THE LOST HEAD WRITE: roll the on-store head pointer back to an
        // earlier version whose segment still exists, exactly as if the PUT that
        // should have advanced it never landed. Drop the in-memory cache so the
        // resolver must read the store cold (as it would after a restart).
        let stale = head_v - 2;
        c.store
            .put_opts(
                &c.part_head_key(&p, &qt, 0, "p0"),
                Bytes::from(stale.to_string()).into(),
                PutOptions { mode: PutMode::Overwrite, ..Default::default() },
            )
            .await
            .unwrap();
        c.invalidate_part(&p, &qt, 0, "p0").await;

        // The resolver must NOT trust the stale pointer: `stale+1` exists, so it
        // recovers the true max segment via the LIST fallback.
        let resolved = c.resolve_part_head_version(&p, &qt, 0, "p0").await.unwrap().unwrap();
        assert_eq!(resolved, head_v, "resolver heals a stale head pointer to the true max segment");

        // The decisive assertion: a subsequent commit SUCCEEDS instead of looping
        // on "lost commit race at partition version N".
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("after-heal.parquet", 1)])
            .await
            .expect("commit must succeed after head-pointer heal, not livelock");

        // No rows lost or shadowed: every committed file is in the folded live set.
        let live: std::collections::BTreeSet<String> = c
            .load_table(&p, &t).await.unwrap()
            .live_data_files().into_iter().map(|f| f.path).collect();
        for i in 0..4 {
            assert!(live.contains(&format!("f{i}.parquet")), "f{i}.parquet present after heal");
        }
        assert!(live.contains("after-heal.parquet"), "post-heal commit present");
    }

    // --- Ambiguous-PUT exactly-once: a 408-after-landed must NOT double-count --
    //
    // REGRESSION (engine v114, dev 100M COPY under a sustained Tigris 408 storm):
    // a partition segment-create PUT LANDED on the store but surfaced a timeout
    // error to us (object_store exhausted its 408 retry budget, or the response
    // was lost). The OLD `put_part_segment_create` propagated that error as a
    // hard failure, so the commit failed even though the segment was durable.
    // The shard's `compact_one` then left the WAL untruncated and the next tick
    // re-flushed the SAME rows into a NEW data file (a fresh ULID path) on top of
    // the already-landed segment — both references live, so `count(*)` over-
    // reported (dev: 100M rows read back as 100,120,000, ~0.12% double-counted).
    //
    // The fix disambiguates the landed-but-errored PUT by reading the object
    // back: if our exact bytes are present the create WON (converge, no re-
    // flush); if absent the error stands (safe retry). This test injects that
    // ambiguous PUT and asserts the commit SUCCEEDS (so the caller truncates the
    // WAL and never re-flushes) with the wave's file referenced exactly once.

    /// Wraps a store and, for the FIRST `PutMode::Create`, writes the object
    /// through to the inner store and THEN returns a generic error — exactly the
    /// "408 after the write landed" ambiguity. All other ops pass through.
    #[derive(Debug)]
    struct AmbiguousCreateStore {
        inner: Arc<dyn ObjectStore>,
        armed: std::sync::atomic::AtomicBool,
    }
    impl std::fmt::Display for AmbiguousCreateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AmbiguousCreateStore")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for AmbiguousCreateStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if matches!(opts.mode, PutMode::Create)
                && self
                    .armed
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                // Land the write, then report a timeout as if the response were
                // lost — the ambiguous PUT. Overwrite-mode so the inner write
                // actually persists regardless of conditional-put support.
                self.inner
                    .put_opts(
                        location,
                        payload,
                        PutOptions { mode: PutMode::Overwrite, ..Default::default() },
                    )
                    .await?;
                return Err(object_store::Error::Generic {
                    store: "AmbiguousCreateStore",
                    source: "injected 408 Request Timeout after the object landed".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn ambiguous_put_segment_create_converges_exactly_once() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(AmbiguousCreateStore {
            inner: inner.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let store: Arc<dyn ObjectStore> = wrapper.clone();
        let c = ObjectStoreCatalog::with_part_compact_every(store, 100_000);
        let p = ProjectId::new();
        let t = TableName::new("ambig").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Seed one clean commit so the next is a normal delta (the create whose
        // PUT we will make ambiguous), exercising the steady-state ingest path.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("seed.parquet", 10)])
            .await
            .unwrap();

        // ARM the ambiguous PUT: the NEXT segment-create lands but errors.
        wrapper.armed.store(true, std::sync::atomic::Ordering::SeqCst);

        // The wave's file (rows the loader sent exactly once). Under the OLD
        // code this commit returns the injected storage error → the shard never
        // truncates the WAL → re-flush of these rows under a new path → double
        // count. The FIX makes `put_part_segment_create` read the landed object
        // back and converge, so the commit SUCCEEDS exactly once.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        let wave = vec![file("wave.parquet", 25)];
        c.append_data_files_in_partition(&p, &t, "p0", exp, wave.clone())
            .await
            .expect(
                "ambiguous PUT that LANDED must converge to a successful commit, \
                 not surface a hard error that triggers a re-flush (double count)",
            );

        // A FRESH catalog (cold caches, like a peer / restart) must read back
        // exactly the seeded + wave rows once each — no duplicate reference.
        let fresh = ObjectStoreCatalog::with_prefix(inner.clone(), DEFAULT_CATALOG_PREFIX);
        let live = fresh.load_table(&p, &t).await.unwrap().live_data_files();
        let total_rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 35, "count(*) must equal seed(10)+wave(25), not double-count");
        let mut paths: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
        paths.sort();
        let n = paths.len();
        paths.dedup();
        assert_eq!(paths.len(), n, "no duplicate file references in the live set");
        assert_eq!(n, 2, "exactly the two committed files are live");
    }

    // --- Test 2c-flat-2: FOLD CORRECTNESS across adds + interleaved removes ----
    #[tokio::test]
    async fn part_delta_fold_matches_reference_replay() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_part_compact_every(store.clone(), 5);
        let p = ProjectId::new();
        let t = TableName::new("fold").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Reference model: replay the same ops over a HashMap of path -> present.
        use std::collections::BTreeSet;
        let mut reference: BTreeSet<String> = BTreeSet::new();

        // 30 commits: mostly appends, with periodic replaces that remove an
        // earlier file and add a compacted one. Crosses the K=5 baseline several
        // times, so the fold must traverse baselines + deltas correctly.
        for i in 0..30usize {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            if i > 0 && i % 4 == 0 {
                // Replace: remove the two oldest present files, add one merged.
                let to_remove: Vec<String> = reference.iter().take(2).cloned().collect();
                let added = file(&format!("merged{i}.parquet"), 5);
                c.replace_data_files_in_partition(&p, &t, "p0", exp, to_remove.clone(), vec![added.clone()])
                    .await
                    .unwrap();
                for r in &to_remove {
                    reference.remove(r);
                }
                reference.insert(added.path);
            } else {
                let added = file(&format!("f{i}.parquet"), 1);
                c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                    .await
                    .unwrap();
                reference.insert(added.path);
            }
        }

        let live: BTreeSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(live, reference, "folded live set must equal the replay reference");
    }

    // --- Test 2c-flat-3: SEGMENT COMPACTION writes a baseline every K ----------
    //
    // After >K deltas a baseline is written; subsequent reads fold from it with
    // bounded depth, and the folded set stays exactly correct.
    #[tokio::test]
    async fn part_segment_compaction_writes_bounded_baseline() {
        const K: u64 = 8;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_part_compact_every(store.clone(), K);
        let p = ProjectId::new();
        let t = TableName::new("compact").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let qt = c.resolve_qtable(&p, &t).await;

        const N: usize = 30; // > several K cycles
        for i in 0..N {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("c{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // Correct total after compaction.
        assert_eq!(c.load_table(&p, &t).await.unwrap().live_data_files().len(), N);

        // Read fold depth is bounded by K: a fresh instance folding the chain
        // walks at most K-1 deltas back to a baseline.
        let head_v = c.resolve_part_head_version(&p, &qt, 0, "p0").await.unwrap().unwrap();
        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let folded = fresh.fold_part_chain(&p, &qt, 0, "p0", head_v).await.unwrap();
        assert_eq!(folded.live.size(), N, "compacted fold is still exactly correct");
        assert!(
            folded.deltas_since_baseline < K,
            "fold depth {} must stay < K={K} (bounded read cost)",
            folded.deltas_since_baseline
        );

        // At least one BASELINE object exists in the chain (compaction ran).
        // Post-#27 a baseline carries `chunk_baseline` (the default chunked form);
        // a legacy inline `baseline` also counts.
        let mut saw_baseline = false;
        for v in 1..=head_v {
            let obj = c.get_part_segment(&p, &qt, 0, "p0", v).await.unwrap();
            if obj.baseline.is_some() || obj.chunk_baseline.is_some() {
                saw_baseline = true;
                break;
            }
        }
        assert!(saw_baseline, "segment compaction wrote at least one baseline");
    }

    // --- Test 2c-flat-4: per-commit store-GET count is BOUNDED in file count --
    //
    // The regression guard for the RESIDUAL ingest-rate decline at scale. A
    // sustained-ingest commit (`append_data_files_in_partition`) must do an
    // amount of object-store work that is INDEPENDENT of how many files the
    // table already holds. Before the fix, every partition commit called
    // `load_unioned`, which `load_part_current`-folds EVERY partition and
    // re-materialises EVERY live data file across the whole table — an
    // amortised-O(total-files) cost per flush that shows up as a gentle
    // throughput slope as the table grows. After the fix the commit returns the
    // cheap META metadata and never unions, so the GET/LIST count per commit is
    // flat in N.
    //
    // We count store reads (GET via `get_opts` non-head, plus `list`) issued by
    // ONE commit when the table holds N files vs 2N files; the counts must be
    // equal (no growth term in N). Caches are warm (the same catalog instance
    // seeded the files), mirroring the steady-state ingest hot path.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: std::sync::atomic::AtomicUsize,
        lists: std::sync::atomic::AtomicUsize,
        /// Non-head GETs against a content-addressed baseline CHUNK object
        /// (`.../chunks/{hash}.json`). The chunk-cache test asserts this drops to
        /// zero on a repeated fold of the same immutable chunks.
        chunk_gets: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                gets: std::sync::atomic::AtomicUsize::new(0),
                lists: std::sync::atomic::AtomicUsize::new(0),
                chunk_gets: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn reset(&self) {
            self.gets.store(0, std::sync::atomic::Ordering::Relaxed);
            self.lists.store(0, std::sync::atomic::Ordering::Relaxed);
            self.chunk_gets.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        fn reads(&self) -> usize {
            self.gets.load(std::sync::atomic::Ordering::Relaxed)
                + self.lists.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn chunk_gets(&self) -> usize {
            self.chunk_gets.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if !options.head {
                self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let k = location.as_ref();
                if k.contains("/chunks/") && k.ends_with(".json") {
                    self.chunk_gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Drive `seed` single-file commits across a few partitions, then measure the
    /// store reads issued by ONE more commit. Returns that per-commit read count.
    async fn per_commit_reads_after_seeding(seed: usize) -> usize {
        // Big K so seeding stays in deltas and a periodic baseline write (itself
        // O(files-in-partition)) doesn't land on the MEASURED commit and muddy
        // the N-independence signal — the baseline path is a separate concern.
        let counting = CountingStore::new(Arc::new(InMemory::new()));
        let c = ObjectStoreCatalog::with_part_compact_every(counting.clone(), 1_000_000);
        let p = ProjectId::new();
        let t = TableName::new("scale").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Spread the seed over 4 partitions so the union (if it ran) would touch
        // multiple partition chains — i.e. a real O(total-files) shape.
        const PARTS: usize = 4;
        for i in 0..seed {
            let pid = (i % PARTS).to_string();
            let exp = c
                .current_snapshot_id_in_partition(&p, &t, &pid)
                .await
                .unwrap();
            c.append_data_files_in_partition(&p, &t, &pid, exp, vec![file(&format!("s{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // Measure exactly one steady-state commit (caches warm from seeding).
        let pid = "0";
        let exp = c
            .current_snapshot_id_in_partition(&p, &t, pid)
            .await
            .unwrap();
        counting.reset();
        c.append_data_files_in_partition(&p, &t, pid, exp, vec![file("measured.parquet", 1)])
            .await
            .unwrap();
        counting.reads()
    }

    #[tokio::test]
    async fn part_commit_store_reads_independent_of_file_count() {
        let small = per_commit_reads_after_seeding(40).await;
        let large = per_commit_reads_after_seeding(400).await; // 10x the files
        assert_eq!(
            small, large,
            "per-commit object-store reads must NOT grow with table file count \
             (40-file table: {small} reads, 400-file table: {large} reads) — \
             a growth term here is the residual ingest-rate slope at scale"
        );
    }

    // --- PROBE (#27): per-commit PUT BYTES must not grow with file count -------
    //
    // The per-commit object-store READ count is already flat in N (the test
    // above). The residual ingest-rate slope at scale is the per-commit PUT
    // *bytes*: every `compact_every` commits the partition writes a BASELINE
    // segment that serializes the ENTIRE live `Vec<DataFileRef>` (each ref also
    // carrying column_stats + bloom/hll/tdigest sketches) into one JSON object.
    // That PUT is O(files-in-partition) and grows as the table grows, so a
    // steadily larger blob is uploaded on the commit path — the decay observed
    // on dev (34k -> 18k r/s as a single table grew). This probe records the
    // bytes of every part-segment PUT and reports the largest, which is the
    // most-recent baseline. On the CURRENT code the largest baseline PUT grows
    // ~linearly with N; the #27 fix must make it bounded (chunk reuse), at which
    // point the assertion below flips to require flatness.
    #[derive(Debug)]
    struct SegPutSizeProbe {
        inner: Arc<dyn ObjectStore>,
        seg_put_bytes: std::sync::Mutex<Vec<usize>>,
    }
    impl std::fmt::Display for SegPutSizeProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SegPutSizeProbe")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SegPutSizeProbe {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            // Record bytes for partition SEGMENT objects only (…/parts/<pid>/v<M>.json),
            // not the best-effort HEAD pointer, data files, or the #27 immutable
            // baseline CHUNK objects (…/parts/<pid>/chunks/<hash>.json). The probe
            // measures the per-baseline SEGMENT (metadata) PUT slope; a chunk's
            // bytes are the rare re-chunk O(n) valve, a separate documented path.
            let k = location.as_ref();
            if k.contains("/parts/")
                && !k.contains("/chunks/")
                && k.ends_with(".json")
                && !k.ends_with("HEAD")
            {
                self.seg_put_bytes
                    .lock()
                    .unwrap()
                    .push(payload.content_length());
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Drive `n` single-file commits to ONE partition and return the largest
    /// part-segment PUT (bytes) — i.e. the most-recent, biggest baseline.
    async fn max_segment_put_bytes_after(n: usize, k: u64) -> usize {
        let probe = Arc::new(SegPutSizeProbe {
            inner: Arc::new(InMemory::new()),
            seg_put_bytes: std::sync::Mutex::new(Vec::new()),
        });
        let store: Arc<dyn ObjectStore> = probe.clone();
        let c = ObjectStoreCatalog::with_part_compact_every(store, k);
        let p = ProjectId::new();
        let t = TableName::new("decay").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        for i in 0..n {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("f{i}.parquet"), 1)])
                .await
                .unwrap();
        }
        let max = probe.seg_put_bytes.lock().unwrap().iter().copied().max().unwrap_or(0);
        max
    }

    #[tokio::test]
    async fn probe_27_baseline_put_bytes_slope() {
        const K: u64 = 32;
        let small = max_segment_put_bytes_after(400, K).await;
        let large = max_segment_put_bytes_after(4000, K).await; // 10x the files
        let ratio = large as f64 / small.max(1) as f64;
        eprintln!(
            "#27 PROBE baseline PUT bytes: 400-file={small}B  4000-file={large}B  ratio={ratio:.2}x \
             (flat-scale target: ratio < 2; chunked-baseline reuses prior chunks)"
        );
        // POST-FIX: the chunked baseline seals only the files appended since the
        // previous baseline into one new chunk and REUSES prior chunk refs, so
        // the largest baseline-time SEGMENT PUT is O(files-added-since-last-
        // baseline) — bounded by ~K, independent of total table size. The 10x
        // file count must NOT produce a ~10x baseline PUT.
        assert!(
            ratio < 2.0,
            "chunked baseline PUT bytes must be bounded (flat in file count); \
             got {ratio:.2}x for 10x files (400-file={small}B, 4000-file={large}B) \
             — a growth term here means the #27 chunking regressed"
        );
    }

    // --- #27: chunked-baseline correctness, reuse, crash-safety, re-chunk -----
    //
    // A store that records, per PUT, the key + content length and counts PUTs to
    // chunk objects (…/chunks/<hash>.json). Lets a test assert that a baseline
    // with only-appends seals exactly ONE new bounded chunk and reuses the rest.
    #[derive(Debug)]
    struct ChunkPutProbe {
        inner: Arc<dyn ObjectStore>,
        chunk_puts: std::sync::Mutex<Vec<(String, usize)>>,
    }
    impl std::fmt::Display for ChunkPutProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ChunkPutProbe")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for ChunkPutProbe {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let k = location.as_ref();
            if k.contains("/chunks/") && k.ends_with(".json") {
                self.chunk_puts
                    .lock()
                    .unwrap()
                    .push((k.to_string(), payload.content_length()));
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    // BOUNDED SEAL + FROZEN-CHUNK REUSE: with a small TARGET, the open tail
    // FREEZES once full and is never re-sealed; only the growing (≤ TARGET) tail
    // is re-sealed each baseline. So (1) EVERY chunk seal serializes ≤ TARGET
    // files (a bounded constant, NEVER the whole live set), and (2) once a tail
    // freezes its exact content-addressed object is never PUT again — the count
    // of FROZEN (TARGET-sized) chunk objects equals floor(N / TARGET) and each
    // such hash appears exactly once across all PUTs.
    #[tokio::test]
    async fn baseline_seals_bounded_tail_and_freezes_chunks() {
        const K: u64 = 4;
        const TARGET: u64 = 8;
        let probe = Arc::new(ChunkPutProbe {
            inner: Arc::new(InMemory::new()),
            chunk_puts: std::sync::Mutex::new(Vec::new()),
        });
        let store: Arc<dyn ObjectStore> = probe.clone();
        let c = ObjectStoreCatalog::with_chunk_config(store, K, true, TARGET, 1024);
        let p = ProjectId::new();
        let t = TableName::new("reuse").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        const N: usize = 80; // several frozen chunks at TARGET=8
        for i in 0..N {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("r{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        let puts = probe.chunk_puts.lock().unwrap().clone();
        // Bytes of a single ~TARGET-file chunk (realistic long filenames) vs the
        // whole live set: the seal bound sits clearly between them, so passing it
        // proves seals are O(TARGET), not O(N).
        let target_files: Vec<DataFileRef> = (0..TARGET)
            .map(|j| file(&format!("r{}.parquet", j + (N as u64 - TARGET)), 1))
            .collect();
        let target_bytes = serde_json::to_vec(&target_files).unwrap().len();
        let whole_files: Vec<DataFileRef> =
            (0..N).map(|j| file(&format!("r{j}.parquet"), 1)).collect();
        let whole_bytes = serde_json::to_vec(&whole_files).unwrap().len();
        // Bound: <= 1.5 TARGET chunks, and comfortably under a quarter of a
        // whole-live-set seal.
        let seal_bound = (target_bytes * 3 / 2).min(whole_bytes / 2);
        let max_seal = puts.iter().map(|(_, s)| *s).max().unwrap_or(0);
        for (k, sz) in &puts {
            assert!(
                *sz <= seal_bound,
                "chunk seal {k} = {sz}B exceeds the O(TARGET) bound (~{seal_bound}B; \
                 one TARGET chunk ~{target_bytes}B, whole live set ~{whole_bytes}B) — \
                 a seal must never serialize O(total files)"
            );
        }
        // FROZEN chunks (full, ~TARGET-byte seals) are each PUT exactly once — a
        // frozen chunk is never re-uploaded (content-addressed reuse). A frozen
        // seal is one whose byte size is within the TARGET-chunk band.
        use std::collections::HashMap as Map;
        let mut counts: Map<&String, usize> = Map::new();
        for (k, sz) in &puts {
            if *sz as f64 >= target_bytes as f64 * 0.85 {
                *counts.entry(k).or_default() += 1;
            }
        }
        assert!(
            counts.values().all(|&c| c == 1),
            "each FROZEN (full) chunk must be PUT exactly once (content-addressed reuse)"
        );
        eprintln!(
            "#27 seal bound: {} chunk PUTs, {} distinct full chunks; max seal {max_seal}B (bound {seal_bound}B, whole-set {whole_bytes}B)",
            puts.len(),
            counts.len(),
        );
    }

    // CONTENT-ADDRESSED CHUNK CACHE: a fold under churning version (the read-
    // latency-under-ingest case) must NOT re-GET the immutable baseline chunk
    // objects it already read on an earlier fold. We seed several FROZEN chunks,
    // then force two cold folds against a fresh catalog (empty part_cache, so each
    // fold cold-folds via `fold_part_chain` → `load_baseline_chunks`). The FIRST
    // cold fold reads the chunk objects from the store; the SECOND cold fold —
    // which references the SAME immutable chunk hashes — must serve every repeated
    // chunk from the process-wide cache, issuing ZERO chunk-object GETs. This is
    // exactly the win under ingest: the partition version advances on every commit
    // (part_cache misses), but the frozen chunks are reused, so the per-read GET
    // count collapses to the (one) changed tail with no staleness (immutable
    // content-addressed identity).
    #[tokio::test]
    async fn fold_reuses_cached_immutable_chunks_zero_gets_on_second_fold() {
        const K: u64 = 4;
        const TARGET: u64 = 8;
        let counting = CountingStore::new(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = counting.clone();
        // Big chunk cap so we stay with several FROZEN chunks (no re-chunk valve).
        let c = ObjectStoreCatalog::with_chunk_config(store, K, true, TARGET, 1024);
        let p = ProjectId::new();
        let t = TableName::new("chunkcache").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Enough appends to seal several FROZEN chunks at TARGET=8.
        const N: usize = 80;
        for i in 0..N {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("r{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // The reference live set, from the warm catalog.
        use std::collections::BTreeSet;
        let warm: BTreeSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(warm.len(), N, "warm live set should hold all N files");

        // FIRST cold fold: fresh catalog, EMPTY chunk cache. It must GET the
        // baseline chunk objects from the store. Measure the chunk-object GETs.
        let fresh = ObjectStoreCatalog::with_chunk_config(counting.clone(), K, true, TARGET, 1024);
        counting.reset();
        let cold1: BTreeSet<String> = fresh
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        let chunk_gets_first = counting.chunk_gets();
        assert_eq!(cold1, warm, "first cold fold must reconstruct the exact live set");
        assert!(
            chunk_gets_first >= 2,
            "first cold fold must GET the frozen baseline chunk objects (cold chunk cache); \
             got {chunk_gets_first} chunk GETs — expected several frozen chunks"
        );

        // SECOND cold fold on the SAME catalog: clear the part_cache to force a
        // cold re-fold (emulating the version churn under ingest that constantly
        // misses the version-keyed part_cache), but the process-wide chunk cache
        // stays WARM. The fold must reuse the cached immutable chunks: ZERO
        // chunk-object GETs.
        fresh.part_cache.lock().await.clear();
        counting.reset();
        let cold2: BTreeSet<String> = fresh
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        let chunk_gets_second = counting.chunk_gets();
        assert_eq!(cold2, warm, "second cold fold must reconstruct the exact live set");

        // The decisive assertion: a re-fold that references the SAME immutable
        // content-addressed chunk objects performs ZERO chunk-object GETs — every
        // chunk is served from the process-wide cache. This is the read-latency
        // win under ingest with NO staleness (immutable identity).
        assert_eq!(
            chunk_gets_second, 0,
            "second cold fold must issue ZERO baseline-chunk GETs (content-addressed cache \
             reuse); first fold did {chunk_gets_first} chunk GETs, second did {chunk_gets_second}"
        );
    }

    // #30 BOUNDED-STALENESS READ SNAPSHOT: under simulated partition-version
    // CHURN (a fresh part commit between reads, which advances the partition
    // version and so MISSES the version-keyed part_cache), repeated `load_table`
    // reads WITHIN the TTL must issue ZERO new object-store round-trips — no
    // `list_partition_ids` LIST, no `resolve_part_head_version` GET(HEAD), no
    // fold GET — serving the last resolved unioned view. A read AFTER the TTL
    // refreshes (re-resolves, paying round-trips again), and the refreshed view
    // is exact. This is the read-latency-under-ingest win.
    #[tokio::test]
    async fn read_snapshot_zero_round_trips_within_ttl_and_refreshes_after() {
        let counting = CountingStore::new(Arc::new(InMemory::new()));
        // 200ms TTL — long enough to do several reads inside it deterministically,
        // short enough to expire within the test.
        let c = ObjectStoreCatalog::with_read_snapshot_ttl(counting.clone(), 200);
        let p = ProjectId::new();
        let t = TableName::new("snap").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Seed a few partitions so a re-resolve would touch multiple chains.
        const PARTS: usize = 3;
        for i in 0..PARTS {
            let pid = i.to_string();
            let exp = c.current_snapshot_id_in_partition(&p, &t, &pid).await.unwrap();
            c.append_data_files_in_partition(&p, &t, &pid, exp, vec![file(&format!("seed{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // First read PRIMES the snapshot (pays the full resolve).
        let first = c.load_table(&p, &t).await.unwrap();
        assert_eq!(first.live_data_files().len(), PARTS, "primed view = seed set");

        // CHURN: commit a fresh file to partition 0. This advances partition 0's
        // version (version-keyed part_cache now MISSES) but does NOT touch the
        // META manifest, so the bounded-staleness snapshot stays valid.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "0", exp, vec![file("churn.parquet", 1)])
            .await
            .unwrap();

        // Now measure: repeated reads WITHIN the TTL must touch the store ZERO
        // times (no LIST, no GET(HEAD), no fold GET). They serve the PRIMED
        // (pre-churn) snapshot — bounded staleness by design.
        counting.reset();
        for _ in 0..5 {
            let m = c.load_table(&p, &t).await.unwrap();
            // Still the pre-churn view: the churn file is intentionally not yet
            // visible (staleness bounded by the TTL), but the snapshot is a real
            // internally-consistent committed view of exactly PARTS files.
            assert_eq!(m.live_data_files().len(), PARTS, "within-TTL read = primed snapshot");
        }
        assert_eq!(
            counting.reads(),
            0,
            "repeated metadata reads within the TTL must issue ZERO object-store \
             round-trips (no LIST / GET(HEAD) / fold GET) — got {} reads",
            counting.reads()
        );

        // After the TTL expires the next read REFRESHES: it re-resolves (paying
        // round-trips) and now reflects the churn commit exactly.
        tokio::time::sleep(std::time::Duration::from_millis(260)).await;
        counting.reset();
        let refreshed = c.load_table(&p, &t).await.unwrap();
        assert!(
            counting.reads() > 0,
            "a read after the TTL must re-resolve (touch the store), not serve a stale snapshot"
        );
        assert_eq!(
            refreshed.live_data_files().len(),
            PARTS + 1,
            "the post-TTL refresh must reflect the churn commit exactly (converges to exact)"
        );
    }

    // #30 QUIET-TABLE EXACTNESS: with the snapshot enabled, a table that is NOT
    // being mutated still converges to the exact live set within one TTL — and a
    // DDL/commit forces an immediate refresh (META-version gate) rather than
    // waiting out the TTL. Also covers the TTL=0 disable: every read is exact.
    #[tokio::test]
    async fn read_snapshot_quiet_table_is_exact_and_disable_works() {
        // ---- TTL enabled: a commit (META-chain append) forces immediate refresh.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_read_snapshot_ttl(store, 5_000);
        let p = ProjectId::new();
        let t = TableName::new("quiet").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Prime an (empty) snapshot.
        assert_eq!(c.load_table(&p, &t).await.unwrap().live_data_files().len(), 0);

        // A single-node META-chain append advances the META version and runs
        // through `after_commit`, which drops the snapshot. Despite the long
        // 5s TTL the very next read sees the new file (no stale snapshot).
        c.append_data_files(&p, &t, SnapshotId::GENESIS, vec![file("m1.parquet", 1)])
            .await
            .unwrap();
        assert_eq!(
            c.load_table(&p, &t).await.unwrap().live_data_files().len(),
            1,
            "a META commit must force an immediate refresh (META-version gate), not wait the TTL"
        );

        // ---- TTL = 0 disables the snapshot: exact-every-read legacy.
        let store2: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c0 = ObjectStoreCatalog::with_read_snapshot_ttl(store2, 0);
        let p0 = ProjectId::new();
        let t0 = TableName::new("disabled").unwrap();
        c0.create_namespace(&p0).await.unwrap();
        c0.create_table(&p0, &t0, &schema()).await.unwrap();
        let exp = c0.current_snapshot_id_in_partition(&p0, &t0, "0").await.unwrap();
        c0.append_data_files_in_partition(&p0, &t0, "0", exp, vec![file("d0.parquet", 1)])
            .await
            .unwrap();
        assert_eq!(c0.load_table(&p0, &t0).await.unwrap().live_data_files().len(), 1);
        // A churn commit is visible on the VERY NEXT read (no snapshot at all).
        let exp = c0.current_snapshot_id_in_partition(&p0, &t0, "0").await.unwrap();
        c0.append_data_files_in_partition(&p0, &t0, "0", exp, vec![file("d1.parquet", 1)])
            .await
            .unwrap();
        assert_eq!(
            c0.load_table(&p0, &t0).await.unwrap().live_data_files().len(),
            2,
            "with the snapshot disabled (TTL=0) every read must be exact"
        );
    }

    // FOLD CORRECTNESS across MANY baselines + re-chunks + removals: drive
    // thousands of appends interleaved with replace removals, then assert a FRESH
    // cold catalog folds to the EXACT live set (count + path set) the warm
    // in-memory view reports. This is the exactly-once core property.
    #[tokio::test]
    async fn cold_fold_matches_warm_across_chunked_baselines_and_removals() {
        const K: u64 = 16;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Small chunk target + cap so the re-chunk valve fires within the run.
        let c = ObjectStoreCatalog::with_chunk_config(store.clone(), K, true, 64, 8);
        let p = ProjectId::new();
        let t = TableName::new("coldfold").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        use std::collections::BTreeSet;
        let mut reference: BTreeSet<String> = BTreeSet::new();

        const N: usize = 2000;
        for i in 0..N {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            // Every 7th commit (after warmup) is a Replace that removes a few of
            // the OLDEST live files (some sealed into chunks → tombstones) and
            // adds a merged one — drives tombstone growth → eventual re-chunk.
            if i > 0 && i % 7 == 0 && reference.len() >= 4 {
                let to_remove: Vec<String> = reference.iter().take(4).cloned().collect();
                let added = file(&format!("m{i}.parquet"), 5);
                c.replace_data_files_in_partition(&p, &t, "p0", exp, to_remove.clone(), vec![added.clone()])
                    .await
                    .unwrap();
                for r in &to_remove {
                    reference.remove(r);
                }
                reference.insert(added.path);
            } else {
                let added = file(&format!("f{i}.parquet"), 1);
                c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                    .await
                    .unwrap();
                reference.insert(added.path);
            }
        }

        // Warm view.
        let warm: BTreeSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(warm, reference, "warm folded live set must equal the replay reference");

        // Cold catalog (fresh instance, empty caches) must fold to the identical set.
        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let cold: BTreeSet<String> = fresh
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(cold.len(), reference.len(), "cold fold count must match reference");
        assert_eq!(cold, reference, "cold fold path set must EXACTLY match the reference");
    }

    // COLUMN-STATS SURVIVE the chunked-baseline round-trip. The #41 metadata
    // range-aggregate classifies a file as fully-in-range / straddling /
    // outside purely from its per-file `column_stats` (min/max). If the #27
    // chunked baseline (commit a5fc5082) dropped or zeroed those stats through
    // chunk serialize → fold, a wide straddling file would mis-classify as
    // fully-in-range and the range count would over-report. This drives enough
    // appends + removals to force several frozen chunks AND the re-chunk valve,
    // then asserts the cold-folded live set carries the EXACT min/max/null_count
    // of every file — byte-for-byte.
    #[tokio::test]
    async fn chunked_baseline_preserves_column_stats() {
        const K: u64 = 4;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Small TARGET + cap so files freeze into chunks and the re-chunk valve
        // (the O(n) full re-seal) fires within the run.
        let c = ObjectStoreCatalog::with_chunk_config(store.clone(), K, true, 16, 8);
        let p = ProjectId::new();
        let t = TableName::new("statsrt").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        use std::collections::BTreeMap;
        // path -> (row_count, id_min, id_max, null_count) reference.
        let mut reference: BTreeMap<String, (u64, i64, i64, u64)> = BTreeMap::new();

        const N: usize = 400;
        for i in 0..N {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            // Give each file a DISTINCT, wide-ish id range so a dropped/zeroed
            // stat would be obvious (every file's stats differ).
            let lo = (i as i64) * 1000;
            let hi = lo + 999;
            if i > 0 && i % 7 == 0 && reference.len() >= 4 {
                let victims: Vec<String> = reference.keys().take(3).cloned().collect();
                let added = file_with_stats(&format!("m{i}.parquet"), 5, lo, hi, 0);
                c.replace_data_files_in_partition(
                    &p, &t, "p0", exp, victims.clone(), vec![added.clone()],
                )
                .await
                .unwrap();
                for v in &victims {
                    reference.remove(v);
                }
                reference.insert(added.path, (5, lo, hi, 0));
            } else {
                let nulls = (i % 3) as u64;
                let added = file_with_stats(&format!("f{i}.parquet"), 10, lo, hi, nulls);
                c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                    .await
                    .unwrap();
                reference.insert(added.path, (10, lo, hi, nulls));
            }
        }

        // Cold catalog (fresh instance, empty caches): fold purely from objects.
        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let live = fresh.load_table(&p, &t).await.unwrap().live_data_files();
        assert_eq!(live.len(), reference.len(), "cold fold file count must match");

        for f in &live {
            let (rows, lo, hi, nulls) = *reference
                .get(&f.path)
                .unwrap_or_else(|| panic!("unexpected file {} in cold fold", f.path));
            assert_eq!(f.row_count, rows, "row_count drifted for {}", f.path);
            let cs = f
                .column_stats
                .get("id")
                .unwrap_or_else(|| panic!("column_stats for `id` DROPPED on {} through the chunk round-trip", f.path));
            assert_eq!(cs.null_count, Some(nulls), "null_count drifted for {}", f.path);
            assert_eq!(
                cs.min_bytes,
                Some(lo.to_le_bytes().to_vec()),
                "min_bytes drifted for {} (straddle classification would break)",
                f.path
            );
            assert_eq!(
                cs.max_bytes,
                Some(hi.to_le_bytes().to_vec()),
                "max_bytes drifted for {} (straddle classification would break)",
                f.path
            );
        }
    }

    // RE-CHUNK VALVE fires under heavy removal churn and the result is exactly
    // correct. We force a tiny chunk cap so it trips quickly, churn heavily, and
    // assert (a) a chunked baseline with FEWER chunks than the pre-rechunk count
    // eventually appears (compaction happened) and (b) the cold fold is exact.
    #[tokio::test]
    async fn rechunk_valve_fires_and_is_exact() {
        const K: u64 = 4;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // TARGET=32 (so the live set freezes into ~6 chunks, giving frozen files
        // to tombstone), cap=64 (NOT hit — so the valve fires via the TOMBSTONE
        // fraction path under heavy churn, not the chunk-count cap).
        let c = ObjectStoreCatalog::with_chunk_config(store.clone(), K, true, 32, 64);
        let p = ProjectId::new();
        let t = TableName::new("rechunk").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let qt = c.resolve_qtable(&p, &t).await;

        use std::collections::BTreeSet;
        let mut reference: BTreeSet<String> = BTreeSet::new();

        // Build up a healthy live set first.
        for i in 0..200usize {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            let added = file(&format!("a{i}.parquet"), 1);
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                .await
                .unwrap();
            reference.insert(added.path);
        }
        // Now churn heavily: remove an old file and add a new one each commit, so
        // tombstones pile up and trip the rechunk valve (tomb*4 > live).
        for i in 0..400usize {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            let victim = reference.iter().next().cloned().unwrap();
            let added = file(&format!("c{i}.parquet"), 1);
            c.replace_data_files_in_partition(&p, &t, "p0", exp, vec![victim.clone()], vec![added.clone()])
                .await
                .unwrap();
            reference.remove(&victim);
            reference.insert(added.path);
        }

        // Scan the baseline trajectory: a RE-CHUNK consolidates the live set into
        // fresh ~TARGET chunks, so it (a) DROPS the tombstone set to empty after
        // tombstones had accumulated, and/or (b) reduces the chunk-ref count vs
        // the previous baseline. We detect either signal.
        let head_v = c.resolve_part_head_version(&p, &qt, 0, "p0").await.unwrap().unwrap();
        let mut saw_rechunk = false;
        let mut max_tomb = 0usize;
        let mut prev_chunks: Option<usize> = None;
        let mut prev_tomb = 0usize;
        for v in 1..=head_v {
            let obj = c.get_part_segment(&p, &qt, 0, "p0", v).await.unwrap();
            if let Some(cb) = obj.chunk_baseline {
                max_tomb = max_tomb.max(cb.tombstones.len());
                let now_chunks = cb.chunks.len();
                let dropped_chunks = prev_chunks.is_some_and(|pc| now_chunks < pc);
                let absorbed_tomb = prev_tomb >= 4 && cb.tombstones.is_empty();
                if dropped_chunks || absorbed_tomb {
                    saw_rechunk = true;
                }
                prev_chunks = Some(now_chunks);
                prev_tomb = cb.tombstones.len();
            }
        }
        eprintln!("#27 rechunk valve: head_v={head_v} max_tombstones_seen={max_tomb} saw_rechunk={saw_rechunk}");
        assert!(saw_rechunk, "re-chunk valve must fire under heavy churn (chunk-count drop or absorbed tombstones)");

        // Exactly correct from a cold catalog.
        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let cold: BTreeSet<String> = fresh
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(cold, reference, "cold fold after re-chunk must be exactly correct");
    }

    // CRASH / AMBIGUOUS-PUT safety for CHUNK objects: a chunk Create that LANDS
    // but surfaces an error (mirror of `AmbiguousCreateStore`) must converge —
    // the seal sees the content-addressed object already present and proceeds,
    // and a fresh cold catalog folds the referencing baseline correctly.
    #[tokio::test]
    async fn ambiguous_chunk_put_converges_and_cold_fold_is_correct() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(AmbiguousCreateStore {
            inner: inner.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let store: Arc<dyn ObjectStore> = wrapper.clone();
        // K=2 so the second commit writes a baseline (and seals a chunk) — we arm
        // the ambiguous PUT just before it so the CHUNK create lands-but-errors.
        let c = ObjectStoreCatalog::with_part_compact_every(store, 2);
        let p = ProjectId::new();
        let t = TableName::new("ambigchunk").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Commit 1 = genesis baseline (seals a chunk). Disarmed.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("g0.parquet", 10)])
            .await
            .unwrap();

        // ARM: the NEXT Create PUT lands but errors. The baseline commit seals a
        // chunk FIRST (that is the create we make ambiguous), then writes the
        // segment object. The seal must converge (chunk is content-addressed and
        // already present), so the whole commit succeeds.
        wrapper.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("g1.parquet", 25)])
            .await
            .expect("ambiguous CHUNK put that LANDED must converge, not fail the commit");

        // Cold catalog reads back both files once each — the baseline references
        // a chunk that is durably present.
        let fresh = ObjectStoreCatalog::with_prefix(inner.clone(), DEFAULT_CATALOG_PREFIX);
        let live = fresh.load_table(&p, &t).await.unwrap().live_data_files();
        let total_rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 35, "cold fold must see g0(10)+g1(25) exactly once");
        let mut paths: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
        paths.sort();
        let n = paths.len();
        paths.dedup();
        assert_eq!(paths.len(), n, "no duplicate file refs after ambiguous chunk put");
        assert_eq!(n, 2, "exactly the two committed files are live");
    }

    // BACKWARD COMPAT: a chain that STARTS with a legacy inline baseline (written
    // with chunking off) then continues with chunked baselines + deltas must fold
    // correctly from a cold catalog. We force the genesis baseline inline via the
    // escape hatch, then re-enable chunking for the rest.
    #[tokio::test]
    async fn legacy_inline_baseline_then_chunked_folds_correctly() {
        const K: u64 = 4;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = ProjectId::new();
        let t = TableName::new("compat").unwrap();

        use std::collections::BTreeSet;
        let mut reference: BTreeSet<String> = BTreeSet::new();

        // Phase 1: chunking OFF → genesis + early baselines are LEGACY inline.
        {
            let c = ObjectStoreCatalog::with_chunk_config(store.clone(), K, false, 1024, 1024);
            c.create_namespace(&p).await.unwrap();
            c.create_table(&p, &t, &schema()).await.unwrap();
            for i in 0..10usize {
                let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
                let added = file(&format!("legacy{i}.parquet"), 1);
                c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                    .await
                    .unwrap();
                reference.insert(added.path);
            }
        }

        // Phase 2: chunking ON (default) → deltas + chunked baselines on top of
        // the legacy inline baseline chain. Fresh instance (cold caches) so it
        // must fold the legacy inline boundary correctly.
        {
            let c = ObjectStoreCatalog::with_chunk_config(store.clone(), K, true, 1024, 1024);
            for i in 10..40usize {
                let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
                if i % 5 == 0 {
                    let victim = reference.iter().next().cloned().unwrap();
                    let added = file(&format!("mix{i}.parquet"), 1);
                    c.replace_data_files_in_partition(&p, &t, "p0", exp, vec![victim.clone()], vec![added.clone()])
                        .await
                        .unwrap();
                    reference.remove(&victim);
                    reference.insert(added.path);
                } else {
                    let added = file(&format!("new{i}.parquet"), 1);
                    c.append_data_files_in_partition(&p, &t, "p0", exp, vec![added.clone()])
                        .await
                        .unwrap();
                    reference.insert(added.path);
                }
            }
        }

        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let cold: BTreeSet<String> = fresh
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(cold, reference, "mixed legacy-inline + chunked chain must fold exactly");
    }

    // --- Redundant-resolve elimination: per-commit ROUND TRIPS drop ----------
    //
    // The throughput lift removes the SECOND head-resolution per partition
    // commit: `commit_part_snapshot` no longer re-runs `resolve_part_head_version`
    // (GET HEAD + HEAD seg + HEAD seg+1) that the caller's
    // `current_snapshot_id_in_partition` already paid, and `load_current` serves
    // the table manifest head from the META-head cache instead of GET HEAD + HEAD
    // manifest. We count ALL store round trips (GET + HEAD + LIST + PUT) issued by
    // a single steady-state commit and assert the resolve-side reads collapse,
    // while the authoritative PUTs (segment create + best-effort head pointer)
    // are unchanged.
    #[derive(Debug)]
    struct RtCountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: std::sync::atomic::AtomicUsize,
        heads: std::sync::atomic::AtomicUsize,
        lists: std::sync::atomic::AtomicUsize,
        puts: std::sync::atomic::AtomicUsize,
    }
    impl std::fmt::Display for RtCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RtCountingStore")
        }
    }
    impl RtCountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                gets: std::sync::atomic::AtomicUsize::new(0),
                heads: std::sync::atomic::AtomicUsize::new(0),
                lists: std::sync::atomic::AtomicUsize::new(0),
                puts: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn reset(&self) {
            use std::sync::atomic::Ordering::Relaxed;
            self.gets.store(0, Relaxed);
            self.heads.store(0, Relaxed);
            self.lists.store(0, Relaxed);
            self.puts.store(0, Relaxed);
        }
        fn read_rts(&self) -> usize {
            use std::sync::atomic::Ordering::Relaxed;
            self.gets.load(Relaxed) + self.heads.load(Relaxed) + self.lists.load(Relaxed)
        }
        fn put_rts(&self) -> usize {
            self.puts.load(std::sync::atomic::Ordering::Relaxed)
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for RtCountingStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                self.heads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn commit_eliminates_redundant_head_resolution_round_trips() {
        let counting = RtCountingStore::new(Arc::new(InMemory::new()));
        let c = ObjectStoreCatalog::with_part_compact_every(counting.clone(), 1_000_000);
        let p = ProjectId::new();
        let t = TableName::new("rt").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Warm the caches the way the engine hot path does: a few prior commits
        // so the META-head and partition caches are populated (steady state).
        for i in 0..3 {
            let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
            c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file(&format!("w{i}.parquet"), 1)])
                .await
                .unwrap();
        }

        // Measure ONE steady-state commit cycle exactly as `commit_with_retry`
        // drives it: resolve the partition snapshot, then append.
        counting.reset();
        let exp = c.current_snapshot_id_in_partition(&p, &t, "p0").await.unwrap();
        let read_rts_after_resolve = counting.read_rts();
        c.append_data_files_in_partition(&p, &t, "p0", exp, vec![file("hot.parquet", 1)])
            .await
            .unwrap();
        let total_read_rts = counting.read_rts();
        let append_read_rts = total_read_rts - read_rts_after_resolve;
        let put_rts = counting.put_rts();

        // The APPEND must do NO resolve-side reads of its own: the META head is
        // served from the meta-head cache and the partition segment from the
        // part_cache the caller's resolve just warmed. (Before the fix the append
        // re-ran resolve_head_version + resolve_part_head_version = ~5 reads.)
        assert_eq!(
            append_read_rts, 0,
            "append_data_files_in_partition must issue ZERO redundant resolve reads \
             (META + partition heads served from cache); got {append_read_rts}"
        );

        // Authoritative writes are unchanged: exactly the segment create + the
        // best-effort head-pointer overwrite (2 PUTs).
        assert_eq!(
            put_rts, 2,
            "commit still issues exactly the 2 authoritative PUTs (segment create + head pointer)"
        );

        // Sanity: the whole cycle's read round trips collapsed to roughly the
        // single (first, anti-livelock-probe-bearing) resolve, well under the
        // ~10 the doubled-resolve path used to issue.
        assert!(
            total_read_rts <= 5,
            "one commit cycle now costs <= 5 read round trips (was ~10 with the \
             duplicate resolve); got {total_read_rts}"
        );

        // Correctness preserved: every file is live, none lost/dup.
        let live: std::collections::BTreeSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        for i in 0..3 {
            assert!(live.contains(&format!("w{i}.parquet")));
        }
        assert!(live.contains("hot.parquet"));
        assert_eq!(live.len(), 4, "no over/under count after redundant-resolve removal");
    }

    // --- Exactly-once gate: concurrent multi-partition ingest + compaction ----
    //
    // The bar for the redundant-resolve removal. Many writers hammer several
    // partitions concurrently (appends) while a compaction sweep concurrently
    // REPLACES files in those same partitions. With the second head-resolution
    // removed, the per-partition CAS must STILL admit exactly one winner per
    // version: the final folded row count must equal the rows that actually
    // committed — never over- or under-counted — and the chain must converge with
    // no livelock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_multipart_ingest_plus_compaction_exactly_once() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Small K so compaction baselines fire frequently during the run.
        let c = Arc::new(ObjectStoreCatalog::with_part_compact_every(store.clone(), 4));
        let p = ProjectId::new();
        let t = TableName::new("xo").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        const PARTS: usize = 6;
        const PER_PART: usize = 25;
        const ROWS_PER_FILE: u64 = 7;

        // One appender task per partition: each commits PER_PART single-file
        // appends, retrying its own per-partition CAS on conflict (exactly the
        // engine's `commit_with_retry` loop). Tracks the paths it durably landed.
        let mut appenders = Vec::new();
        for part in 0..PARTS {
            let c = c.clone();
            let t = t.clone();
            appenders.push(tokio::spawn(async move {
                let pid = part.to_string();
                let mut landed: Vec<String> = Vec::new();
                for i in 0..PER_PART {
                    let path = format!("p{part}-f{i}.parquet");
                    loop {
                        let exp = c
                            .current_snapshot_id_in_partition(&p, &t, &pid)
                            .await
                            .unwrap();
                        match c
                            .append_data_files_in_partition(
                                &p,
                                &t,
                                &pid,
                                exp,
                                vec![file(&path, ROWS_PER_FILE)],
                            )
                            .await
                        {
                            Ok(_) => {
                                landed.push(path.clone());
                                break;
                            }
                            Err(BasinError::CommitConflict(_)) => {
                                // Re-resolve (authoritative, via cold-cache path)
                                // and retry — no double commit, no loss.
                                continue;
                            }
                            Err(e) => panic!("unexpected append error: {e}"),
                        }
                    }
                }
                landed
            }));
        }

        // A concurrent compaction sweep: repeatedly merge each partition's two
        // oldest live files into one, via the partition replace CAS — racing the
        // appenders on the SAME chains. Replaces preserve row counts (merged file
        // carries the summed rows), so the row total is invariant under merges.
        let merger = {
            let c = c.clone();
            let t = t.clone();
            tokio::spawn(async move {
                let qt = c.resolve_qtable(&p, &t).await;
                for _ in 0..40 {
                    for part in 0..PARTS {
                        let pid = part.to_string();
                        let (_v, seg) = match c.load_part_current(&p, &qt, 0, &pid).await {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        let mut files = seg.live_data_files();
                        if files.len() < 2 {
                            continue;
                        }
                        files.sort_by(|a, b| a.path.cmp(&b.path));
                        let a = files[0].clone();
                        let b = files[1].clone();
                        let merged = file(
                            &format!("{}+{}.merged", a.path, b.path),
                            a.row_count + b.row_count,
                        );
                        // Best-effort: a conflict means an appender advanced the
                        // chain; just skip this round (the appender's retry and
                        // the next sweep make progress). Never panic on conflict.
                        let _ = c
                            .replace_data_files_in_partition(
                                &p,
                                &t,
                                &pid,
                                seg.current_snapshot,
                                vec![a.path.clone(), b.path.clone()],
                                vec![merged],
                            )
                            .await;
                        tokio::task::yield_now().await;
                    }
                }
            })
        };

        let mut all_landed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for h in appenders {
            for path in h.await.unwrap() {
                assert!(all_landed.insert(path.clone()), "a path was committed twice: {path}");
            }
        }
        merger.await.unwrap();

        // The decisive count: total live rows must equal exactly the rows the
        // appenders durably landed. Merges fold pairs but preserve summed rows,
        // so the invariant is: every appended file's rows are present exactly
        // once, whether still standalone or rolled into a merged file.
        let expected_rows = (all_landed.len() as u64) * ROWS_PER_FILE;

        // Read through a COLD instance: forces a full store re-resolve + fold of
        // every partition, so the assertion reflects durable state, not caches.
        let cold = ObjectStoreCatalog::with_part_compact_every(store.clone(), 4);
        let meta = cold.load_table(&p, &t).await.unwrap();
        let live = meta.live_data_files();
        let total_rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(
            total_rows, expected_rows,
            "EXACTLY-ONCE: folded row count must equal rows committed (no over/under count)"
        );

        // No duplicate paths survive in the live set (CAS admitted one winner).
        let mut paths: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
        let n = paths.len();
        paths.sort();
        paths.dedup();
        assert_eq!(paths.len(), n, "no duplicate paths in the folded live set");

        // Heads converged: a fresh resolve of every partition succeeds and a
        // final append lands first try (no livelock / wedged chain).
        for part in 0..PARTS {
            let pid = part.to_string();
            let exp = cold.current_snapshot_id_in_partition(&p, &t, &pid).await.unwrap();
            cold.append_data_files_in_partition(&p, &t, &pid, exp, vec![file(&format!("final-{part}.parquet"), 1)])
                .await
                .expect("post-run commit lands without livelock");
        }
    }

    // --- Test 2e: non-partitioned replace resolves the owning chain --------
    //
    // A file committed to a PARTITION segment can be replaced via the bare
    // (non-partitioned) `replace_data_files` — the catalog locates the owning
    // chain and commits the swap there, and a mixed replace (one path on the
    // META chain, one in a partition) routes each removed path to its own chain.
    #[tokio::test]
    async fn non_partitioned_replace_resolves_owning_chain() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("mix").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // META-chain file (single-node OLTP style append).
        c.append_data_files(&p, &t, SnapshotId::GENESIS, vec![file("meta.parquet", 3)])
            .await
            .unwrap();
        // Partition-segment file (sharded ingest style).
        let exp = c.current_snapshot_id_in_partition(&p, &t, "s0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "s0", exp, vec![file("part.parquet", 3)])
            .await
            .unwrap();

        // Sanity: union sees both.
        let live: std::collections::HashSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert!(live.contains("meta.parquet") && live.contains("part.parquet"));

        // Replace the PARTITION file via the bare API → routed to partition s0.
        let head = c.current_snapshot_id(&p, &t).await.unwrap();
        c.replace_data_files(
            &p,
            &t,
            head,
            vec!["part.parquet".into()],
            vec![file("part2.parquet", 3)],
        )
        .await
        .unwrap();

        // Mixed replace: remove one META path + one partition path at once.
        let head2 = c.current_snapshot_id(&p, &t).await.unwrap();
        c.replace_data_files(
            &p,
            &t,
            head2,
            vec!["meta.parquet".into(), "part2.parquet".into()],
            vec![file("merged.parquet", 6)],
        )
        .await
        .unwrap();

        let final_live: std::collections::HashSet<String> = c
            .load_table(&p, &t)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        // Only the merged file survives; both removed paths are gone, no dup.
        assert_eq!(final_live.len(), 1, "exactly one live file after the swaps");
        assert!(final_live.contains("merged.parquet"));
    }

    // --- Test 3: double-lease-acquire -------------------------------------

    struct ManualClock(AtomicI64);
    impl LeaseClock for ManualClock {
        fn now_ms(&self) -> i64 {
            self.0.load(SeqCst)
        }
    }

    #[tokio::test]
    async fn double_lease_exactly_one_holder_per_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reg = Arc::new(ObjectStoreLeaseRegistry::new(store));
        let p = ProjectId::new();
        let part = "part-0";

        const RACERS: usize = 8;
        let mut handles = Vec::new();
        for r in 0..RACERS {
            let reg = reg.clone();
            let p = p;
            handles.push(tokio::spawn(async move {
                reg.acquire(&p, part, &format!("holder-{r}"), Duration::from_secs(30))
                    .await
            }));
        }
        let mut granted = Vec::new();
        for h in handles {
            if let Some(lease) = h.await.unwrap().unwrap() {
                granted.push(lease);
            }
        }
        // Exactly one holder is live at epoch 1 (the others raced and lost).
        let owner = reg.owner_of(&p, part).await.unwrap();
        assert!(owner.is_some(), "someone owns the partition");
        let (owner_holder, owner_epoch) = owner.unwrap();
        assert_eq!(owner_epoch, 1, "first grant is epoch 1");

        // No two distinct holders ever share an epoch: among granted leases,
        // each epoch maps to exactly one holder.
        let mut by_epoch: HashMap<i64, String> = HashMap::new();
        for l in &granted {
            if let Some(prev) = by_epoch.insert(l.epoch, l.holder.clone()) {
                assert_eq!(prev, l.holder, "epoch {} had two holders", l.epoch);
            }
        }
        // Whoever owns it now must be one of the granted holders.
        assert!(granted.iter().any(|l| l.holder == owner_holder));
    }

    #[tokio::test]
    async fn double_lease_expiry_steal_and_fence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let clock = Arc::new(ManualClock(AtomicI64::new(1_000)));
        let reg = Arc::new(ObjectStoreLeaseRegistry::with_prefix_and_clock(
            store,
            DEFAULT_LEASE_PREFIX,
            clock.clone(),
        ));
        let p = ProjectId::new();
        let part = "p";

        // A acquires at epoch 1, ttl 10s.
        let a = reg
            .acquire(&p, part, "A", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.epoch, 1);

        // B cannot acquire while A's lease is live.
        clock.0.store(5_000, SeqCst);
        assert!(reg
            .acquire(&p, part, "B", Duration::from_secs(10))
            .await
            .unwrap()
            .is_none());

        // Advance past A's expiry; B steals at epoch 2 (strictly increasing).
        clock.0.store(20_000, SeqCst);
        let b = reg
            .acquire(&p, part, "B", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.epoch, 2, "stolen epoch strictly increases");

        // A is now fenced: it observes B as the owner at the higher epoch, and
        // its renew at the stale epoch 1 fails.
        let (owner, epoch) = reg.owner_of(&p, part).await.unwrap().unwrap();
        assert_eq!(owner, "B");
        assert_eq!(epoch, 2);
        let renewed = reg
            .renew(&p, part, "A", 1, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(!renewed, "fenced loser cannot renew the stale epoch");

        // B can renew at its own epoch without bumping the fencing epoch.
        clock.0.store(22_000, SeqCst);
        let ok = reg
            .renew(&p, part, "B", 2, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(ok);
        let (_, epoch_after) = reg.owner_of(&p, part).await.unwrap().unwrap();
        assert_eq!(epoch_after, 2, "renew does not bump the fencing epoch");
    }

    // --- Test 4: DDL set_* survives a concurrent appender -----------------

    #[tokio::test]
    async fn ddl_and_append_both_land_no_lost_update() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = Arc::new(ObjectStoreCatalog::new(store));
        let p = ProjectId::new();
        let t = TableName::new("mixed").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Interleave a set_bloom_filter_columns (transparent-retry DDL) with an
        // appender (OCC retry). Both must land; neither clobbers the other.
        let c1 = c.clone();
        let p1 = p;
        let t1 = t.clone();
        let ddl = tokio::spawn(async move {
            c1.set_bloom_filter_columns(&p1, &t1, vec!["name".into()])
                .await
                .unwrap();
        });
        let c2 = c.clone();
        let p2 = p;
        let t2 = t.clone();
        let appender = tokio::spawn(async move {
            loop {
                let exp = c2.current_snapshot_id(&p2, &t2).await.unwrap();
                match c2
                    .append_data_files(&p2, &t2, exp, vec![file("a.parquet", 10)])
                    .await
                {
                    Ok(_) => break,
                    Err(BasinError::CommitConflict(_)) => continue,
                    Err(e) => panic!("{e:?}"),
                }
            }
        });
        ddl.await.unwrap();
        appender.await.unwrap();

        let meta = c.load_table(&p, &t).await.unwrap();
        assert_eq!(
            meta.bloom_filter_columns,
            vec!["name".to_string()],
            "DDL landed"
        );
        assert_eq!(meta.live_data_files().len(), 1, "append landed");
        assert_eq!(meta.current_snapshot, SnapshotId(1));
    }

    // --- Test 5: replace_data_files OCC + conflict ------------------------

    #[tokio::test]
    async fn replace_data_files_occ_and_conflict() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("cow").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Seed two files.
        let m = c
            .append_data_files(&p, &t, SnapshotId::GENESIS, vec![file("a.parquet", 5), file("b.parquet", 5)])
            .await
            .unwrap();
        let head = m.current_snapshot;

        // Replace a.parquet -> c.parquet (the META-chain back-compat path now
        // resolves its OCC internally, so the passed `head` is informational).
        let m2 = c
            .replace_data_files(&p, &t, head, vec!["a.parquet".into()], vec![file("c.parquet", 5)])
            .await
            .unwrap();
        let live: Vec<String> = m2.live_data_files().into_iter().map(|f| f.path).collect();
        assert!(live.contains(&"b.parquet".to_string()));
        assert!(live.contains(&"c.parquet".to_string()));
        assert!(!live.contains(&"a.parquet".to_string()));

        // A second replace lands too (internal RMW); the live set tracks it.
        let m3 = c
            .replace_data_files(&p, &t, head, vec!["b.parquet".into()], vec![file("d.parquet", 5)])
            .await
            .unwrap();
        let live3: Vec<String> = m3.live_data_files().into_iter().map(|f| f.path).collect();
        assert!(live3.contains(&"c.parquet".to_string()));
        assert!(live3.contains(&"d.parquet".to_string()));
        assert!(!live3.contains(&"b.parquet".to_string()));

        // Removing a non-live path -> Catalog error (path is in neither the META
        // chain nor any partition segment).
        let head2 = c.current_snapshot_id(&p, &t).await.unwrap();
        let err2 = c
            .replace_data_files(&p, &t, head2, vec!["does-not-exist".into()], vec![])
            .await
            .unwrap_err();
        assert!(matches!(err2, BasinError::Catalog(_)), "got {err2:?}");
    }

    // --- create/drop/rename basics ----------------------------------------

    #[tokio::test]
    async fn create_drop_rename_and_list() {
        let c = cat();
        let p = ProjectId::new();
        let t1 = TableName::new("t1").unwrap();
        let t2 = TableName::new("t2").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t1, &schema()).await.unwrap();

        // Duplicate create rejected.
        assert!(c.create_table(&p, &t1, &schema()).await.is_err());

        // List sees t1.
        assert_eq!(c.list_tables(&p).await.unwrap(), vec![t1.clone()]);

        // Rename t1 -> t2, history preserved.
        c.append_data_files(&p, &t1, SnapshotId::GENESIS, vec![file("x.parquet", 1)])
            .await
            .unwrap();
        c.rename_table(&p, &t1, &t2).await.unwrap();
        assert!(c.load_table(&p, &t1).await.is_err());
        let m = c.load_table(&p, &t2).await.unwrap();
        assert_eq!(m.live_data_files().len(), 1);
        assert_eq!(c.list_tables(&p).await.unwrap(), vec![t2.clone()]);

        // Drop t2.
        c.drop_table(&p, &t2).await.unwrap();
        assert!(c.load_table(&p, &t2).await.is_err());
        assert!(c.list_tables(&p).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn project_metadata_and_watermark_round_trip() {
        let c = cat();
        let p = ProjectId::new();
        // max connections
        assert_eq!(c.get_project_max_connections(&p).await.unwrap(), None);
        c.set_project_max_connections(&p, 42).await.unwrap();
        assert_eq!(c.get_project_max_connections(&p).await.unwrap(), Some(42));
        // monotonic watermark
        c.set_compaction_watermark(&p, "part-1", 100).await.unwrap();
        c.set_compaction_watermark(&p, "part-1", 50).await.unwrap(); // lower ignored
        assert_eq!(
            c.get_compaction_watermark(&p, "part-1").await.unwrap(),
            Some(100)
        );
    }

    // --- Test: two NODES share one project's catalog over one store ---------
    //
    // This is the multi-node property the object-store backend exists for: two
    // engine nodes, each with its OWN in-process `ObjectStoreCatalog` instance
    // (and thus its own session cache), pointed at one shared bucket + prefix.
    // Node A creates a table and commits a chain; node B — which never wrote —
    // must see the full state through HEAD/version refresh, not stale/empty
    // cached data. Then the same for the lease registry: A holds, B is fenced.
    #[tokio::test]
    async fn two_nodes_share_catalog_over_one_store() {
        // ONE shared bucket; TWO independent catalog instances (= two nodes).
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let node_b = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);

        let p = ProjectId::new();
        let t = TableName::new("shared").unwrap();

        // Node A creates the table and commits 3 append snapshots.
        node_a.create_namespace(&p).await.unwrap();
        node_a.create_table(&p, &t, &schema()).await.unwrap();
        let mut expected = SnapshotId::GENESIS;
        for i in 0..3 {
            let m = node_a
                .append_data_files(&p, &t, expected, vec![file(&format!("a{i}.parquet"), 10)])
                .await
                .unwrap();
            expected = m.current_snapshot;
        }
        let a_snapshot = node_a.current_snapshot_id(&p, &t).await.unwrap();
        // Synthetic union id once data exists (was SnapshotId(3) under the old
        // single-chain layout; the count assertion below is what matters).
        assert_eq!(a_snapshot, SnapshotId(1));

        // KEY ASSERTION: node B — independent in-process cache, never wrote —
        // sees the table, all 3 commits' files, and the SAME current snapshot.
        // If B served a stale/empty cache this would fail (cross-node staleness
        // bug); it passes because reads revalidate against HEAD/version.
        let b_loaded = node_b.load_table(&p, &t).await.unwrap();
        assert_eq!(
            b_loaded.live_data_files().len(),
            3,
            "node B sees all 3 of node A's committed files (cross-node visibility)"
        );
        let b_snapshot = node_b.current_snapshot_id(&p, &t).await.unwrap();
        assert_eq!(
            b_snapshot, a_snapshot,
            "node B's current snapshot matches node A's (no stale cache)"
        );

        // Now the lease registry across the same two nodes. Use a manual clock
        // so the expired-steal branch is deterministic.
        let clock = Arc::new(ManualClock(AtomicI64::new(1_000)));
        let lease_a = ObjectStoreLeaseRegistry::with_prefix_and_clock(
            store.clone(),
            DEFAULT_LEASE_PREFIX,
            clock.clone(),
        );
        let lease_b = ObjectStoreLeaseRegistry::with_prefix_and_clock(
            store.clone(),
            DEFAULT_LEASE_PREFIX,
            clock.clone(),
        );
        let part = "partition-0";

        // Node A acquires the writer lease.
        let granted = lease_a
            .acquire(&p, part, "nodeA", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("node A acquires the lease");
        assert_eq!(granted.epoch, 1);

        // Node B cannot acquire while A's lease is live: single-writer ACROSS
        // nodes through the shared store.
        let b_attempt = lease_b
            .acquire(&p, part, "nodeB", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            b_attempt.is_none(),
            "node B is refused while node A holds the lease (single-writer across nodes)"
        );

        // Node B sees node A as the owner at the granted epoch.
        let (owner, epoch) = lease_b.owner_of(&p, part).await.unwrap().unwrap();
        assert_eq!(owner, "nodeA");
        assert_eq!(epoch, 1);

        // Advance past A's TTL: node B can steal the EXPIRED lease at a strictly
        // higher epoch (the WAL fencing token increments across nodes).
        clock.0.store(40_000, SeqCst);
        let stolen = lease_b
            .acquire(&p, part, "nodeB", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("node B steals the expired lease");
        assert_eq!(stolen.epoch, 2, "stolen epoch strictly increases across nodes");
        let (owner2, epoch2) = lease_a.owner_of(&p, part).await.unwrap().unwrap();
        assert_eq!(owner2, "nodeB");
        assert_eq!(epoch2, 2);
    }

    // --- Test: schema-qualified isolation (ADR 0022) ----------------------
    //
    // The same bare table name in two different schemas must be completely
    // independent: separate manifests, separate snapshot chains, no collision.
    // This guards the deploy-blocker case where `BASIN_AUTH_ENABLED=1` keeps
    // its system tables in the reserved `auth` schema while user tables live in
    // `public`.
    #[tokio::test]
    async fn public_and_auth_same_name_are_independent() {
        let c = cat();
        let p = ProjectId::new();
        c.create_namespace(&p).await.unwrap();

        let users = TableName::new("users").unwrap();
        let pub_users = QualifiedTableName::in_public(users.clone());
        let auth_users =
            QualifiedTableName::new(SchemaName::new("auth").unwrap(), users.clone());

        // Two tables, SAME bare name, DIFFERENT schemas, over ONE store.
        c.create_table_qualified(&p, &pub_users, Arc::new(schema()))
            .await
            .unwrap();
        c.create_table_qualified(&p, &auth_users, Arc::new(schema()))
            .await
            .unwrap();

        // Drive their snapshot chains to DIFFERENT lengths so a collision would
        // be immediately visible.
        let mut expected = SnapshotId::GENESIS;
        for i in 0..3 {
            let m = c
                .append_data_files_qualified(
                    &p,
                    &pub_users,
                    expected,
                    vec![file(&format!("pub{i}.parquet"), 10)],
                )
                .await
                .unwrap();
            expected = m.current_snapshot;
        }
        let mut expected_auth = SnapshotId::GENESIS;
        let m = c
            .append_data_files_qualified(
                &p,
                &auth_users,
                expected_auth,
                vec![file("auth0.parquet", 99)],
            )
            .await
            .unwrap();
        expected_auth = m.current_snapshot;
        let _ = expected_auth;

        // Each resolves to ITS OWN current snapshot and live file set.
        let pub_meta = c.load_table_qualified(&p, &pub_users).await.unwrap();
        let auth_meta = c.load_table_qualified(&p, &auth_users).await.unwrap();
        // Both report the synthetic union id (1) since both have data; the
        // independence proof is the DISTINCT live file sets + paths below, not
        // the (now synthetic) snapshot id.
        assert_eq!(pub_meta.current_snapshot, SnapshotId(1));
        assert_eq!(auth_meta.current_snapshot, SnapshotId(1));
        assert_eq!(
            pub_meta.live_data_files().len(),
            3,
            "public.users advanced 3 commits independently"
        );
        assert_eq!(
            auth_meta.live_data_files().len(),
            1,
            "auth.users advanced exactly 1 commit — NOT contaminated by public"
        );
        // The file paths prove the manifests are physically distinct.
        assert!(pub_meta
            .live_data_files()
            .iter()
            .all(|f| f.path.starts_with("pub")));
        assert_eq!(auth_meta.live_data_files()[0].path, "auth0.parquet");

        // list_tables_qualified returns BOTH, correctly schema-qualified.
        let mut listed = c.list_tables_qualified(&p).await.unwrap();
        listed.sort();
        assert_eq!(
            listed,
            vec![auth_users.clone(), pub_users.clone()],
            "both schemas enumerated with correct qualification"
        );
        // The back-compat bare list_tables returns ONLY the public-schema name.
        assert_eq!(
            c.list_tables(&p).await.unwrap(),
            vec![users.clone()],
            "bare list_tables stays public-only for back-compat"
        );

        // Dropping the auth-schema table leaves the public one untouched.
        c.drop_table_qualified(&p, &auth_users).await.unwrap();
        assert!(matches!(
            c.load_table_qualified(&p, &auth_users).await,
            Err(BasinError::NotFound(_))
        ));
        let pub_after = c.load_table_qualified(&p, &pub_users).await.unwrap();
        assert_eq!(
            pub_after.live_data_files().len(),
            3,
            "dropping auth.users did not affect public.users"
        );
        // After the auth drop, only public.users remains in the qualified list.
        assert_eq!(
            c.list_tables_qualified(&p).await.unwrap(),
            vec![pub_users.clone()]
        );
    }

    // Bare-name resolution finds a unique non-public table (mirrors the
    // InMemory `resolve_qtable` fallback) — so the executor can address
    // `auth.users` by its stripped bare name when no `public.users` exists.
    #[tokio::test]
    async fn bare_name_resolves_unique_non_public_schema() {
        let c = cat();
        let p = ProjectId::new();
        c.create_namespace(&p).await.unwrap();
        let t = TableName::new("sessions").unwrap();
        let auth_t = QualifiedTableName::new(SchemaName::new("auth").unwrap(), t.clone());
        c.create_table_qualified(&p, &auth_t, Arc::new(schema()))
            .await
            .unwrap();
        // No public.sessions exists → bare load resolves to auth.sessions.
        let loaded = c.load_table(&p, &t).await.unwrap();
        assert_eq!(loaded.current_snapshot, SnapshotId::GENESIS);
    }

    // fork_table_qualified copies across schemas without aliasing the source.
    #[tokio::test]
    async fn fork_qualified_across_schemas_is_independent() {
        let c = cat();
        let p = ProjectId::new();
        c.create_namespace(&p).await.unwrap();
        let src = QualifiedTableName::in_public(TableName::new("src").unwrap());
        let dst = QualifiedTableName::new(
            SchemaName::new("staging").unwrap(),
            TableName::new("src").unwrap(),
        );
        c.create_table_qualified(&p, &src, Arc::new(schema()))
            .await
            .unwrap();
        let m = c
            .append_data_files_qualified(&p, &src, SnapshotId::GENESIS, vec![file("a.parquet", 5)])
            .await
            .unwrap();
        c.fork_table_qualified(&p, &src, &dst).await.unwrap();
        // Fork carries the source's live files.
        let forked = c.load_table_qualified(&p, &dst).await.unwrap();
        assert_eq!(forked.live_data_files().len(), 1);
        // Mutating the fork does not touch the source.
        c.append_data_files_qualified(&p, &dst, m.current_snapshot, vec![file("b.parquet", 7)])
            .await
            .unwrap();
        let src_after = c.load_table_qualified(&p, &src).await.unwrap();
        assert_eq!(
            src_after.live_data_files().len(),
            1,
            "source unchanged after fork mutation"
        );
        let dst_after = c.load_table_qualified(&p, &dst).await.unwrap();
        assert_eq!(dst_after.live_data_files().len(), 2);
    }

    // --- Sequences --------------------------------------------------------

    fn seq_def(project: ProjectId, name: &str, start: i64, increment: i64) -> SequenceDef {
        SequenceDef {
            project,
            name: name.to_string(),
            start,
            increment,
            min_value: if increment > 0 { 1 } else { i64::MIN + 1 },
            max_value: if increment > 0 { i64::MAX } else { -1 },
            cache_size: 1,
            cycle: false,
        }
    }

    #[tokio::test]
    async fn sequence_basics_match_increment_and_currval_setval_drop() {
        // BLOCK=1 exercises the per-value CAS path (the auth-provisioning shape).
        let c = ObjectStoreCatalog::with_seq_block(Arc::new(InMemory::new()), 1);
        let p = ProjectId::new();
        let d = seq_def(p, "s", 5, 2);
        c.create_sequence(d.clone()).await.unwrap();
        // First nextval returns `start`, then steps by `increment`.
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 5);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 7);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 9);
        // currval reflects the last value returned this session.
        assert_eq!(c.currval(&p, "s").await.unwrap(), 9);
        // lookup + list see the def.
        assert_eq!(c.lookup_sequence(&p, "s").await.unwrap(), d);
        assert_eq!(c.list_sequences(&p).await.len(), 1);
        // setval(advance=true): next nextval is value+increment.
        assert_eq!(c.setval(&p, "s", 100, true).await.unwrap(), 100);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 102);
        // setval(advance=false): next nextval is value exactly.
        assert_eq!(c.setval(&p, "s", 200, false).await.unwrap(), 200);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 200);
        // drop: gone everywhere.
        c.drop_sequence(&p, "s").await.unwrap();
        assert!(c.lookup_sequence(&p, "s").await.is_none());
        assert!(c.nextval(&p, "s").await.is_err());
        assert!(c.drop_sequence(&p, "s").await.is_err());
    }

    #[tokio::test]
    async fn sequence_matches_in_memory_value_stream() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let os = ObjectStoreCatalog::with_seq_block(store, 7);
        let im = crate::InMemoryCatalog::new();
        let p = ProjectId::new();
        let d = seq_def(p, "s", 1, 3);
        os.create_sequence(d.clone()).await.unwrap();
        im.create_sequence(d).await.unwrap();
        // The same op stream must produce identical values across backends,
        // even though ObjectStore reserves blocks of 7 under the hood.
        for _ in 0..50 {
            assert_eq!(
                os.nextval(&p, "s").await.unwrap(),
                im.nextval(&p, "s").await.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn sequence_no_cycle_exhausts_like_in_memory() {
        let c = ObjectStoreCatalog::with_seq_block(Arc::new(InMemory::new()), 4);
        let p = ProjectId::new();
        let mut d = seq_def(p, "s", 1, 1);
        d.max_value = 3;
        c.create_sequence(d).await.unwrap();
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 1);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 2);
        assert_eq!(c.nextval(&p, "s").await.unwrap(), 3);
        // NO CYCLE past max -> exhausted error.
        assert!(c.nextval(&p, "s").await.is_err());
    }

    // HEADLINE: two nodes over one store never hand out a duplicate value.
    #[tokio::test]
    async fn sequence_two_nodes_disjoint_no_duplicates() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::with_seq_block(store.clone(), 8);
        let node_b = ObjectStoreCatalog::with_seq_block(store.clone(), 8);
        let p = ProjectId::new();
        node_a.create_sequence(seq_def(p, "s", 1, 1)).await.unwrap();

        let mut seen = std::collections::HashSet::new();
        // Interleave nextval across both nodes, spanning many block allocations.
        for _ in 0..200 {
            let va = node_a.nextval(&p, "s").await.unwrap();
            let vb = node_b.nextval(&p, "s").await.unwrap();
            assert!(va >= 1, "in range");
            assert!(vb >= 1, "in range");
            assert!(seen.insert(va), "no duplicate values across two nodes: {va}");
            assert!(seen.insert(vb), "no duplicate values across two nodes: {vb}");
        }
        assert_eq!(seen.len(), 400, "every value distinct across both nodes");
    }

    // CRASH RECOVERY: a fresh instance resumes strictly above any value the
    // crashed instance handed out (gap allowed, never reused).
    #[tokio::test]
    async fn sequence_crash_recovery_resumes_strictly_greater() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = ProjectId::new();
        let mut max_a = i64::MIN;
        {
            // Instance A reserves a block of 64 and hands out only a few, then
            // "crashes" (dropped) leaving an unused tail.
            let node_a = ObjectStoreCatalog::with_seq_block(store.clone(), 64);
            node_a.create_sequence(seq_def(p, "s", 1, 1)).await.unwrap();
            for _ in 0..3 {
                max_a = max_a.max(node_a.nextval(&p, "s").await.unwrap());
            }
        }
        // Fresh instance B over the same store.
        let node_b = ObjectStoreCatalog::new(store.clone());
        let first_b = node_b.nextval(&p, "s").await.unwrap();
        assert!(
            first_b > max_a,
            "B resumes strictly greater than A: first_b={first_b} max_a={max_a}"
        );
    }

    // CRASH RECOVERY (durable hwm must NOT regress): a reserved-but-unused
    // block ceiling sits ABOVE the highest committed id. Recovery's
    // `advance_sequence_floor(max_committed)` must keep the persisted ceiling,
    // not clobber down to it — otherwise the next `nextval` re-issues values
    // an earlier instance already handed out (the regression bug).
    #[tokio::test]
    async fn advance_floor_never_regresses_below_persisted_hwm() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let p = ProjectId::new();
        let mut handed_out = Vec::new();
        {
            // Instance A reserves a block of 64 (hwm.last == 64) and hands out
            // 1..=10, then crashes leaving the unused tail 11..=64 reserved.
            let node_a = ObjectStoreCatalog::with_seq_block(store.clone(), 64);
            node_a.create_sequence(seq_def(p, "s", 1, 1)).await.unwrap();
            for _ in 0..10 {
                handed_out.push(node_a.nextval(&p, "s").await.unwrap());
            }
        }
        // Recovery on a fresh instance: max committed id is 10 (rows 1..=10),
        // but the persisted hwm is 64. The floor must NOT lower the hwm.
        let node_b = ObjectStoreCatalog::new(store.clone());
        node_b.advance_sequence_floor(&p, "s", 10).await.unwrap();
        let next = node_b.nextval(&p, "s").await.unwrap();
        assert_eq!(
            next, 65,
            "must resume above the persisted block ceiling (64), not re-issue 11"
        );
        assert!(
            handed_out.iter().all(|&v| v < next),
            "no previously-handed-out value is re-issued"
        );

        // Conversely, when the recovered max EXCEEDS the persisted hwm (e.g. a
        // rows-loaded-then-catalog-lost scenario), the floor raises it.
        let node_c = ObjectStoreCatalog::new(store.clone());
        node_c.advance_sequence_floor(&p, "s", 1000).await.unwrap();
        assert_eq!(node_c.nextval(&p, "s").await.unwrap(), 1001);
    }

    // Cross-node visibility for sequences + SQL functions (mirrors
    // two_nodes_share_catalog): write on A, read on B over one store.
    #[tokio::test]
    async fn sequence_and_sql_function_visible_across_nodes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::new(store.clone());
        let node_b = ObjectStoreCatalog::new(store.clone());
        let p = ProjectId::new();
        // Sequence def created on A is looked up on B.
        node_a.create_sequence(seq_def(p, "s", 10, 5)).await.unwrap();
        assert_eq!(
            node_b.lookup_sequence(&p, "s").await.unwrap().start,
            10,
            "sequence def visible on the other node"
        );
        assert_eq!(node_b.list_sequences(&p).await.len(), 1);

        // SQL function registered on A is visible + listable on B.
        use crate::functions::{SqlArgType, SqlFunctionLanguage, SqlReturnType};
        let f = SqlFunctionDef {
            project: p,
            name: "f".to_string(),
            args: vec![],
            return_type: SqlReturnType::Scalar(SqlArgType::BigInt),
            body: "SELECT 1".to_string(),
            language: SqlFunctionLanguage::Sql,
            version: 1,
            source: None,
        };
        node_a.register_sql_function(f.clone()).await.unwrap();
        let got = node_b.lookup_sql_function(&p, "f").await.unwrap();
        assert_eq!(got.body, "SELECT 1");
        assert_eq!(node_b.list_sql_functions(&p).await.len(), 1);
        // drop on B is observed on A.
        node_b.drop_sql_function(&p, "f").await.unwrap();
        assert!(node_a.lookup_sql_function(&p, "f").await.is_none());
    }

    // Schemas: create (empty), list, drop — visible across nodes.
    #[tokio::test]
    async fn schemas_durable_and_visible_across_nodes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::new(store.clone());
        let node_b = ObjectStoreCatalog::new(store.clone());
        let p = ProjectId::new();
        node_a.create_namespace(&p).await.unwrap();
        // Default: only public.
        assert_eq!(
            node_b.list_schemas(&p).await.unwrap(),
            vec![SchemaName::public()]
        );
        // Empty schema survives (no tables in it) and is visible on B.
        let analytics = SchemaName::new("analytics").unwrap();
        node_a.create_schema(&p, &analytics).await.unwrap();
        let schemas = node_b.list_schemas(&p).await.unwrap();
        assert!(schemas.contains(&analytics));
        assert!(schemas.contains(&SchemaName::public()));
        // Idempotent.
        node_a.create_schema(&p, &analytics).await.unwrap();
        // RESTRICT drop of empty schema succeeds; then it's gone.
        node_b.drop_schema(&p, &analytics, false).await.unwrap();
        assert!(!node_a.list_schemas(&p).await.unwrap().contains(&analytics));
        // Dropping public is rejected.
        assert!(node_a
            .drop_schema(&p, &SchemaName::public(), false)
            .await
            .is_err());
        // Unknown schema -> NotFound.
        assert!(node_a
            .drop_schema(&p, &SchemaName::new("nope").unwrap(), false)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn project_storage_config_round_trip_across_nodes() {
        use crate::project_storage_config::ProjectStorageConfig;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::new(store.clone());
        let node_b = ObjectStoreCatalog::new(store.clone());
        let p = ProjectId::new();
        // Default before any set.
        assert!(node_b.get_project_storage_config(&p).await.unwrap().is_none());
        let cfg = ProjectStorageConfig::default();
        node_a.set_project_storage_config(&p, cfg.clone()).await.unwrap();
        assert_eq!(
            node_b.get_project_storage_config(&p).await.unwrap(),
            Some(cfg)
        );
    }

    #[tokio::test]
    async fn bucket_registry_and_assignment_persist_and_cas() {
        use crate::bucket_pool::{
            BucketAssignment, BucketRegistry, BucketRegistryEntry, BucketTier,
        };
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::new(store.clone());
        let node_b = ObjectStoreCatalog::new(store.clone());

        // Empty registry by default; persisted registry round-trips to a peer.
        assert!(node_b.get_bucket_registry().await.unwrap().buckets.is_empty());
        let registry = BucketRegistry {
            buckets: vec![BucketRegistryEntry {
                bucket_id: "pool-0000".into(),
                bucket_name: "pool-0000".into(),
                endpoint: "https://example".into(),
                region: "auto".into(),
                credentials_ref: Some("AWS".into()),
                assigned_count: 1,
            }],
        };
        node_a.put_bucket_registry(&registry).await.unwrap();
        assert_eq!(node_b.get_bucket_registry().await.unwrap(), registry);

        // First assignment wins; a racing assignment to a DIFFERENT bucket
        // returns the durable winner (create-if-absent CAS), so the assignment
        // is stable.
        let p = ProjectId::new();
        assert!(node_a.get_bucket_assignment(&p).await.unwrap().is_none());
        let first = BucketAssignment {
            bucket_id: "pool-0000".into(),
            tier: BucketTier::Pooled,
            stripe: Vec::new(),
        };
        let won = node_a.assign_bucket_if_absent(&p, &first).await.unwrap();
        assert_eq!(won, first);

        let second = BucketAssignment {
            bucket_id: "pool-0001".into(),
            tier: BucketTier::Pooled,
            stripe: Vec::new(),
        };
        let still = node_b.assign_bucket_if_absent(&p, &second).await.unwrap();
        assert_eq!(still, first, "CAS must keep the first durable assignment");
        // A fresh node re-reads the same stable assignment.
        let node_c = ObjectStoreCatalog::new(store.clone());
        assert_eq!(
            node_c.get_bucket_assignment(&p).await.unwrap(),
            Some(first.clone())
        );

        // Cutover: set_bucket_assignment OVERWRITES (the atomic flip). Visible
        // to a peer node.
        let flipped = BucketAssignment {
            bucket_id: "pool-0001".into(),
            tier: BucketTier::Pooled,
            stripe: vec!["pool-0001".into()],
        };
        node_a.set_bucket_assignment(&p, &flipped).await.unwrap();
        assert_eq!(
            node_b.get_bucket_assignment(&p).await.unwrap(),
            Some(flipped),
            "cutover overwrite is visible to a peer (atomic flip)"
        );
    }

    #[tokio::test]
    async fn migration_intent_persist_list_resume_delete() {
        use crate::bucket_pool::{MigrationIntent, MigrationPhase};
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = ObjectStoreCatalog::new(store.clone());
        let node_b = ObjectStoreCatalog::new(store.clone());

        let p1 = ProjectId::new();
        let p2 = ProjectId::new();
        // No intents to start.
        assert!(node_a.get_migration_intent(&p1).await.unwrap().is_none());
        assert!(node_a.list_migration_intents().await.unwrap().is_empty());

        let i1 = MigrationIntent {
            project: p1,
            from: "A".into(),
            to: "B".into(),
            phase: MigrationPhase::Copy,
        };
        let i2 = MigrationIntent {
            project: p2,
            from: "C".into(),
            to: "B".into(),
            phase: MigrationPhase::Cutover,
        };
        node_a.put_migration_intent(&i1).await.unwrap();
        node_a.put_migration_intent(&i2).await.unwrap();

        // A peer node reads the same intent (resume-on-restart) and LISTs both.
        assert_eq!(node_b.get_migration_intent(&p1).await.unwrap(), Some(i1.clone()));
        let mut listed = node_b.list_migration_intents().await.unwrap();
        listed.sort_by_key(|m| m.project);
        let mut expected = vec![i1.clone(), i2.clone()];
        expected.sort_by_key(|m| m.project);
        assert_eq!(listed, expected, "LIST must return every in-flight intent");

        // Overwrite advances the phase (the state-machine advance step).
        let advanced = MigrationIntent { phase: MigrationPhase::Verify, ..i1.clone() };
        node_a.put_migration_intent(&advanced).await.unwrap();
        assert_eq!(
            node_b.get_migration_intent(&p1).await.unwrap(),
            Some(advanced),
            "phase advance is a durable overwrite"
        );

        // Delete is idempotent (delete-if-exists).
        node_a.delete_migration_intent(&p1).await.unwrap();
        node_a.delete_migration_intent(&p1).await.unwrap();
        assert!(node_b.get_migration_intent(&p1).await.unwrap().is_none());
        assert_eq!(
            node_b.list_migration_intents().await.unwrap(),
            vec![i2],
            "only the remaining intent is listed after delete"
        );
    }

    // --- cheap META-only load + meta-version (multi-node ingest fix) -------

    #[tokio::test]
    async fn load_table_meta_returns_schema_and_constraints_without_partition_segments() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("ingest_meta").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Register META: pk + a check constraint.
        let check = CheckConstraint {
            name: "ingest_meta_id_check".into(),
            predicate: "id > 0".into(),
        };
        c.set_table_constraints(
            &p,
            &t,
            vec!["id".into()],
            vec![check.clone()],
            Vec::new(),
        )
        .await
        .unwrap();

        // Append a pile of data files across MANY distinct partitions: a full
        // `load_table` would LIST `parts/` and GET each of these segments.
        for pid in 0..8u32 {
            let part = pid.to_string();
            let exp = c
                .current_snapshot_id_in_partition(&p, &t, &part)
                .await
                .unwrap();
            c.append_data_files_in_partition(
                &p,
                &t,
                &part,
                exp,
                vec![file(&format!("part-{pid}.parquet"), 100)],
            )
            .await
            .unwrap();
        }

        // The cheap META load returns the schema + constraints…
        let meta = c.load_table_meta(&p, &t).await.unwrap();
        assert_eq!(meta.pk_columns, vec!["id".to_string()]);
        assert_eq!(meta.check_constraints, vec![check]);
        assert_eq!(meta.schema.fields().len(), 2);
        // …but surfaces NO data files (META-only by contract): it never unioned
        // the 8 partition segments.
        assert!(
            meta.live_data_files().is_empty(),
            "load_table_meta must not union per-partition data files"
        );
        assert_eq!(meta.current_snapshot, SnapshotId::GENESIS);

        // The full load DOES see all 8 partition files (sanity: the data exists).
        let full = c.load_table(&p, &t).await.unwrap();
        assert_eq!(full.live_data_files().len(), 8);
        assert_eq!(full.pk_columns, vec!["id".to_string()]);
    }

    #[tokio::test]
    async fn meta_version_stable_across_data_appends_bumps_on_ddl() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("mv").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        let v0 = c.meta_version(&p, &t).await;
        assert_ne!(v0, 0, "a live table resolves a non-zero meta-version");

        // Per-partition DATA appends MUST NOT bump the meta-version — this is
        // what keeps the ingest cache valid across a bulk COPY.
        for pid in 0..5u32 {
            let part = pid.to_string();
            let exp = c
                .current_snapshot_id_in_partition(&p, &t, &part)
                .await
                .unwrap();
            c.append_data_files_in_partition(
                &p,
                &t,
                &part,
                exp,
                vec![file(&format!("d-{pid}.parquet"), 10)],
            )
            .await
            .unwrap();
            assert_eq!(
                c.meta_version(&p, &t).await,
                v0,
                "data append to partition {pid} must not bump meta_version"
            );
        }

        // A DDL/constraint change MUST bump the meta-version (the ingest cache
        // re-loads and observes the new constraint on the next batch).
        c.set_table_constraints(
            &p,
            &t,
            vec!["id".into()],
            vec![CheckConstraint {
                name: "mv_chk".into(),
                predicate: "id >= 0".into(),
            }],
            Vec::new(),
        )
        .await
        .unwrap();
        let v_after_ddl = c.meta_version(&p, &t).await;
        assert_ne!(v_after_ddl, v0, "DDL must bump meta_version");

        // And a fresh cheap META load reflects the new constraint.
        let meta = c.load_table_meta(&p, &t).await.unwrap();
        assert_eq!(meta.check_constraints.len(), 1);
    }

    // --- DROP + recreate-SAME-name starts clean --------------------------
    //
    // A DROP TABLE followed by CREATE TABLE of the SAME name must present an
    // EMPTY table. The leak: partition-sharded data lives in the `parts/`
    // segment tree, which is outside the META manifest chain and reuses the
    // table prefix verbatim on recreate. Before the fix, the manifest
    // tombstone left those segments behind, so `load_unioned` re-summed their
    // `row_count` — a bare `count(*)` (the metadata fast-aggregate path) saw
    // the stale rows even though a scan saw none. The regression target is
    // the catalog-visible symptom: `live_data_files()` and its summed
    // `row_count` must both be empty on the recreated table.

    /// WARM, same instance: drop + recreate-same-name → empty live set
    /// (zero files, zero rows). Fails before the fix (stale partition segment
    /// re-counted), passes after.
    #[tokio::test]
    async fn drop_recreate_same_name_warm_is_empty() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("t").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Land a partition-sharded data file (mirrors the engine's INSERT
        // route on dev). One file, 1 row.
        let exp = c.current_snapshot_id_in_partition(&p, &t, "s0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "s0", exp, vec![file("pre_drop.parquet", 1)])
            .await
            .unwrap();
        let before = c.load_table(&p, &t).await.unwrap();
        assert_eq!(before.live_data_files().len(), 1, "precondition: 1 file live");

        c.drop_table(&p, &t).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        let after = c.load_table(&p, &t).await.unwrap();
        let live = after.live_data_files();
        let rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(after.current_snapshot, SnapshotId::GENESIS, "recreated table is genesis");
        assert_eq!(live.len(), 0, "no stale data file survives drop+recreate (warm)");
        assert_eq!(rows, 0, "count(*) fast-aggregate sees zero rows (warm)");
    }

    /// COLD, fresh instance: a brand-new catalog over the SAME store reads the
    /// recreated table as empty. Proves the leak was PERSISTED (the `parts/`
    /// segment objects), not merely a stale per-node cache.
    #[tokio::test]
    async fn drop_recreate_same_name_cold_is_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let p = ProjectId::new();
        let t = TableName::new("t").unwrap();
        writer.create_namespace(&p).await.unwrap();
        writer.create_table(&p, &t, &schema()).await.unwrap();
        let exp = writer.current_snapshot_id_in_partition(&p, &t, "s0").await.unwrap();
        writer
            .append_data_files_in_partition(&p, &t, "s0", exp, vec![file("pre_drop.parquet", 7)])
            .await
            .unwrap();

        writer.drop_table(&p, &t).await.unwrap();
        writer.create_table(&p, &t, &schema()).await.unwrap();

        // Fresh catalog: empty caches, must resolve purely from the store.
        let cold = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let after = cold.load_table(&p, &t).await.unwrap();
        let live = after.live_data_files();
        let rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(live.len(), 0, "no stale segment persisted under reused prefix");
        assert_eq!(rows, 0, "cold count(*) sees zero rows");
    }

    /// After recreate, a NEW insert is counted exactly once — the recreated
    /// table accrues only its own rows, with no stale add from the prior life.
    #[tokio::test]
    async fn drop_recreate_then_insert_counts_only_new() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("t").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let exp = c.current_snapshot_id_in_partition(&p, &t, "s0").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "s0", exp, vec![file("old.parquet", 999)])
            .await
            .unwrap();

        c.drop_table(&p, &t).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Insert into the recreated table (same partition id reused).
        let exp = c.current_snapshot_id_in_partition(&p, &t, "s0").await.unwrap();
        assert_eq!(exp, SnapshotId::GENESIS, "recreated partition starts at genesis");
        c.append_data_files_in_partition(&p, &t, "s0", exp, vec![file("new.parquet", 3)])
            .await
            .unwrap();

        let after = c.load_table(&p, &t).await.unwrap();
        let live = after.live_data_files();
        let rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(live.len(), 1, "only the post-recreate file is live");
        assert_eq!(rows, 3, "count(*) == exactly the new rows, no stale add");
    }

    /// Control: a FRESH (never-reused) table name is unaffected by the fix —
    /// its data is read back intact.
    #[tokio::test]
    async fn fresh_named_table_unaffected_by_drop_recreate() {
        let c = cat();
        let p = ProjectId::new();
        let dropped = TableName::new("t").unwrap();
        let fresh = TableName::new("never_reused").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &dropped, &schema()).await.unwrap();
        c.create_table(&p, &fresh, &schema()).await.unwrap();
        let exp = c.current_snapshot_id_in_partition(&p, &fresh, "s0").await.unwrap();
        c.append_data_files_in_partition(&p, &fresh, "s0", exp, vec![file("keep.parquet", 5)])
            .await
            .unwrap();

        // Dropping/recreating the OTHER table must not perturb `fresh`.
        c.drop_table(&p, &dropped).await.unwrap();
        c.create_table(&p, &dropped, &schema()).await.unwrap();

        let after = c.load_table(&p, &fresh).await.unwrap();
        let live = after.live_data_files();
        let rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(live.len(), 1, "fresh-named table keeps its data");
        assert_eq!(rows, 5, "fresh-named table row count intact");
    }

    /// REGRESSION (drop-purge-vs-same-name-recreate race): a DROP's best-effort
    /// `parts/` purge, lagging or racing the recreate's writes, MUST NOT delete a
    /// segment the recreated same-name table just wrote. Before the per-create
    /// GENERATION fix, the recreate reused the dropped table's `parts/{pid}/…`
    /// prefix verbatim, so a lagging purge LISTed + deleted the recreate's
    /// segments — leaving a delta whose baseline segment was gone (a torn chain).
    /// Any later fold then errored `not found: …/parts/{pid}@v{N}`, which (eager
    /// session-open warm) FATAL'd every pgwire connection to the whole project.
    ///
    /// We reproduce the race deterministically: drop (which makes the next
    /// create bump the partition generation), recreate, write enough to the
    /// recreated table to build a delta chain (baseline + deltas), THEN re-run
    /// the DROPPED generation's purge directly — simulating the stale/lagging
    /// purge landing AFTER the recreate's writes. The recreated chain must be
    /// untouched (no torn fold, exact row count). A COLD catalog confirms it
    /// reads the same intact set from the store.
    #[tokio::test]
    async fn drop_recreate_purge_race_does_not_corrupt_recreated_chain() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let p = ProjectId::new();
        let t = TableName::new("p100m").unwrap();
        let qt = QualifiedTableName::in_public(t.clone());
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // Original life: a few partition-sharded commits (generation 0).
        let mut exp = c.current_snapshot_id_in_partition(&p, &t, "s5").await.unwrap();
        for i in 0..3 {
            c.append_data_files_in_partition(&p, &t, "s5", exp, vec![file(&format!("old{i}.parquet"), 10)])
                .await
                .unwrap();
            // The per-table union id is synthetic; re-resolve the PARTITION head.
            exp = c.current_snapshot_id_in_partition(&p, &t, "s5").await.unwrap();
        }

        // The dropped table is generation 0.
        let dropped_gen = c.load_current(&p, &qt).await.unwrap().1.parts_generation;
        assert_eq!(dropped_gen, 0, "first life is the genesis generation");

        c.drop_table(&p, &t).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        // The recreate stamped a FRESH higher generation.
        let new_gen = c.load_current(&p, &qt).await.unwrap().1.parts_generation;
        assert_eq!(new_gen, dropped_gen + 1, "recreate bumps the partition generation");

        // Recreated life: build a delta chain (baseline + several deltas) under
        // the new generation, reusing the SAME partition id `s5`.
        let mut exp = c.current_snapshot_id_in_partition(&p, &t, "s5").await.unwrap();
        assert_eq!(exp, SnapshotId::GENESIS, "recreated partition starts at genesis");
        for i in 0..6 {
            c.append_data_files_in_partition(&p, &t, "s5", exp, vec![file(&format!("new{i}.parquet"), 4)])
                .await
                .unwrap();
            exp = c.current_snapshot_id_in_partition(&p, &t, "s5").await.unwrap();
        }
        let before_race = c.load_table(&p, &t).await.unwrap();
        let n_files = before_race.live_data_files().len();
        let n_rows: u64 = before_race.live_data_files().iter().map(|f| f.row_count).sum();
        assert_eq!(n_files, 6, "precondition: recreated table has its 6 files");
        assert_eq!(n_rows, 24, "precondition: recreated table has its 24 rows");

        // THE RACE: the dropped table's purge lands NOW, after the recreate's
        // writes. Scoped to the dropped generation, it can only enumerate the OLD
        // generation's prefix — never the recreate's.
        c.purge_part_segments(&p, &qt, dropped_gen).await;
        c.invalidate_all_parts(&p, &qt).await;

        // The recreated chain is intact: the fold does NOT error (no torn chain)
        // and the row count is exactly the recreate's own.
        let after_race = c.load_table(&p, &t).await.expect("recreated chain still folds (not torn)");
        let live = after_race.live_data_files();
        let rows: u64 = live.iter().map(|f| f.row_count).sum();
        assert_eq!(live.len(), 6, "recreated table's files survive the stale purge");
        assert_eq!(rows, 24, "recreated table's rows survive the stale purge");

        // COLD: a fresh catalog over the same store reads the same intact set.
        let cold = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let cold_after = cold.load_table(&p, &t).await.expect("cold fold of recreated chain succeeds");
        assert_eq!(cold_after.live_data_files().len(), 6, "cold read: 6 files intact");
    }

    /// The other half of the #44 contract still holds WITH generations: after a
    /// drop+recreate, the recreated same-name table reads EMPTY (the prior life's
    /// segments are not unioned in), because the recreate reads its own (higher)
    /// generation whose tree the drop purged / never populated.
    #[tokio::test]
    async fn drop_recreate_with_generation_still_starts_empty() {
        let c = cat();
        let p = ProjectId::new();
        let t = TableName::new("p100m").unwrap();
        c.create_namespace(&p).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();
        let exp = c.current_snapshot_id_in_partition(&p, &t, "s5").await.unwrap();
        c.append_data_files_in_partition(&p, &t, "s5", exp, vec![file("pre.parquet", 100)])
            .await
            .unwrap();

        c.drop_table(&p, &t).await.unwrap();
        c.create_table(&p, &t, &schema()).await.unwrap();

        let after = c.load_table(&p, &t).await.unwrap();
        assert_eq!(after.current_snapshot, SnapshotId::GENESIS, "recreated table is genesis");
        assert_eq!(after.live_data_files().len(), 0, "recreated table starts empty");
    }
}
