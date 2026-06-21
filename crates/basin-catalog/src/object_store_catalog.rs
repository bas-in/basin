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
    baseline: Option<Vec<DataFileRef>>,
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
    live: Vec<DataFileRef>,
    /// Latest commit time across the folded chain (used to stamp unioned reads).
    latest_committed_at: DateTime<Utc>,
    /// Number of delta objects applied on top of the latest baseline (i.e. the
    /// read fold depth). Used to decide when to write a fresh baseline.
    deltas_since_baseline: u64,
}

impl PartitionLive {
    fn genesis() -> Self {
        Self {
            version: 0,
            current_snapshot: SnapshotId::GENESIS,
            live: Vec::new(),
            latest_committed_at: Utc::now(),
            deltas_since_baseline: 0,
        }
    }

    fn live_data_files(&self) -> Vec<DataFileRef> {
        self.live.clone()
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
            seq_local: Mutex::new(HashMap::new()),
            seq_block_override: None,
            part_compact_override: None,
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

    /// Directory holding one partition's data-file segment chain.
    fn part_dir(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
    ) -> String {
        format!(
            "{}parts/{}/",
            self.table_dir(project, qtable),
            sanitize(partition_id)
        )
    }

    /// Prefix under which ALL of a table's partition segment dirs live.
    fn parts_root(&self, project: &ProjectId, qtable: &QualifiedTableName) -> String {
        format!("{}parts/", self.table_dir(project, qtable))
    }

    fn part_segment_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
        version: u64,
    ) -> OsPath {
        OsPath::from(format!(
            "{}v{version:020}.json",
            self.part_dir(project, qtable, partition_id)
        ))
    }

    fn part_head_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
    ) -> OsPath {
        OsPath::from(format!("{}HEAD", self.part_dir(project, qtable, partition_id)))
    }

    fn part_cache_key(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
    ) -> (ProjectId, String, String) {
        (*project, qtable.to_string(), sanitize(partition_id))
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
        // Fast path: a live `public` manifest exists.
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
        let version = self
            .resolve_head_version(project, qtable)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}/{qtable}")))?;
        let ck = self.cache_key(project, qtable);
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
        let mut cache = self.cache.lock().await;
        cache.insert(
            ck,
            CacheEntry {
                version,
                manifest: Arc::new(manifest),
            },
        );
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
        self.cache.lock().await.remove(&ck);
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
        partition_id: &str,
    ) -> Result<Option<u64>> {
        match self.store.get(&self.part_head_key(project, qtable, partition_id)).await {
            Ok(res) => {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        if let Ok(v) = s.trim().parse::<u64>() {
                            if self
                                .store
                                .head(&self.part_segment_key(project, qtable, partition_id, v))
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
            Err(e) => return Err(storage_err("get partition HEAD", e)),
        }
        // LIST fallback: max v{M}.json directly under the partition dir.
        use futures::StreamExt;
        let dir = self.part_dir(project, qtable, partition_id);
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
        partition_id: &str,
        version: u64,
    ) -> Result<PartSegmentObject> {
        let key = self.part_segment_key(project, qtable, partition_id, version);
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
        partition_id: &str,
        head_version: u64,
    ) -> Result<PartitionLive> {
        use std::collections::HashMap;
        // Collect delta objects from HEAD back to (and including) the baseline.
        let head = self
            .get_part_segment(project, qtable, partition_id, head_version)
            .await?;
        let current_snapshot = head.current_snapshot;
        // Commits to a partition are monotonic (single owner), so the HEAD
        // object's commit time is the partition's latest.
        let latest_committed_at = head.delta.committed_at;
        let mut deltas: Vec<Snapshot> = Vec::new();
        let mut deltas_since_baseline: u64 = 0;
        let mut live: HashMap<String, DataFileRef> = HashMap::new();

        let mut cur = head;
        loop {
            if let Some(files) = cur.baseline.take() {
                // Reached a baseline (or genesis): it carries the full live set.
                for f in files {
                    live.insert(f.path.clone(), f);
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
                .get_part_segment(project, qtable, partition_id, base)
                .await?;
        }

        // Apply deltas oldest-first on top of the baseline (we pushed them
        // newest-first while walking back, so iterate in reverse).
        for snap in deltas.into_iter().rev() {
            for p in &snap.removed_paths {
                live.remove(p);
            }
            for f in &snap.data_files {
                live.insert(f.path.clone(), f.clone());
            }
        }

        Ok(PartitionLive {
            version: head_version,
            current_snapshot,
            live: live.into_values().collect(),
            latest_committed_at,
            deltas_since_baseline,
        })
    }

    /// Load a partition's current FOLDED live view (highest version), via cache
    /// when the cached version matches HEAD. Returns a genesis view (version 0)
    /// when the partition has no segment yet, so a first append starts cleanly.
    async fn load_part_current(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
    ) -> Result<(u64, Arc<PartitionLive>)> {
        let Some(version) = self
            .resolve_part_head_version(project, qtable, partition_id)
            .await?
        else {
            return Ok((0, Arc::new(PartitionLive::genesis())));
        };
        let ck = self.part_cache_key(project, qtable, partition_id);
        {
            let cache = self.part_cache.lock().await;
            if let Some(entry) = cache.get(&ck) {
                if entry.version == version {
                    return Ok((version, entry.segment.clone()));
                }
            }
        }
        let live = Arc::new(
            self.fold_part_chain(project, qtable, partition_id, version)
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

    /// Write partition segment `version` via create-if-absent. `true` = won.
    async fn put_part_segment_create(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
        version: u64,
        segment: &PartSegmentObject,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(segment)
            .map_err(|e| BasinError::catalog(format!("serialise partition segment: {e}")))?;
        let key = self.part_segment_key(project, qtable, partition_id, version);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&key, Bytes::from(bytes).into(), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(storage_err("put partition segment", e)),
        }
    }

    async fn after_part_commit(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        partition_id: &str,
        version: u64,
        live: PartitionLive,
    ) {
        let _ = self
            .store
            .put_opts(
                &self.part_head_key(project, qtable, partition_id),
                Bytes::from(version.to_string()).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await;
        let ck = self.part_cache_key(project, qtable, partition_id);
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
        partition_id: &str,
    ) {
        let ck = self.part_cache_key(project, qtable, partition_id);
        self.part_cache.lock().await.remove(&ck);
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
        let (_mv, manifest) = self.load_current(project, qtable).await?;
        let (version, segment) = self
            .load_part_current(project, qtable, partition_id)
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
        // view + this delta — O(delta), no re-read of the chain.
        let new_live = {
            use std::collections::HashMap;
            let mut live: HashMap<String, DataFileRef> = segment
                .live
                .iter()
                .map(|f| (f.path.clone(), f.clone()))
                .collect();
            for p in &removed_paths {
                live.remove(p);
            }
            for f in &added_files {
                live.insert(f.path.clone(), f.clone());
            }
            live.into_values().collect::<Vec<_>>()
        };

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

        let obj = if write_baseline {
            PartSegmentObject {
                version: new_version,
                current_snapshot: new_id,
                delta: snap,
                base_version: None,
                baseline: Some(new_live.clone()),
            }
        } else {
            PartSegmentObject {
                version: new_version,
                current_snapshot: new_id,
                delta: snap,
                base_version: Some(version),
                baseline: None,
            }
        };

        let committed_at = obj.delta.committed_at;
        if self
            .put_part_segment_create(project, qtable, partition_id, new_version, &obj)
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
            };
            self.after_part_commit(project, qtable, partition_id, new_version, next_live)
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
            self.invalidate_part(project, qtable, partition_id).await;
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

    /// Enumerate every partition id that has at least one segment under
    /// `{table}/parts/`. Used by the unioned read.
    async fn list_partition_ids(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Vec<String>> {
        use futures::StreamExt;
        let root = self.parts_root(project, qtable);
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
            if let Some(seg) = rest.split('/').next() {
                if !seg.is_empty() {
                    ids.insert(seg.to_string());
                }
            }
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
        for pid in self.list_partition_ids(src_project, src).await? {
            let (_v, segment) = self.load_part_current(src_project, src, &pid).await?;
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
                baseline: Some(live.clone()),
            };
            let mut obj = make_baseline(0);
            if !self
                .put_part_segment_create(dst_project, dst, &pid, 0, &obj)
                .await?
            {
                // Destination partition already has a segment — place at next.
                let v = self
                    .resolve_part_head_version(dst_project, dst, &pid)
                    .await?
                    .unwrap_or(0);
                obj = make_baseline(v + 1);
                let _ = self
                    .put_part_segment_create(dst_project, dst, &pid, obj.version, &obj)
                    .await?;
            }
            let fresh_live = PartitionLive {
                version: obj.version,
                current_snapshot: SnapshotId(1),
                live: live.clone(),
                latest_committed_at: obj.delta.committed_at,
                deltas_since_baseline: 0,
            };
            self.after_part_commit(dst_project, dst, &pid, obj.version, fresh_live)
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

        let partition_ids = self.list_partition_ids(project, qtable).await?;
        // Track the latest commit time across partitions to stamp the union.
        let mut latest_commit = manifest
            .snapshots
            .iter()
            .map(|s| s.committed_at)
            .max()
            .unwrap_or_else(Utc::now);
        for pid in &partition_ids {
            let (_v, segment) = self.load_part_current(project, qtable, pid).await?;
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
        let (_v, manifest) = self.load_current(project, qtable).await?;
        // UNION every partition's data files into the returned metadata.
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
        self.load_current(project, qtable).await?; // NotFound if absent.
        self.mutate_manifest(project, qtable, |m| m.dropped = true)
            .await?;
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
        let partition_ids = self.list_partition_ids(project, qtable).await?;
        for pid in &partition_ids {
            let (_pv, segment) = self.load_part_current(project, qtable, pid).await?;
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
            let mut by_partition: HashMap<String, Vec<String>> = HashMap::new();
            let mut remaining: HashSet<String> = removed_paths.iter().cloned().collect();
            for pid in self.list_partition_ids(project, &qtable).await? {
                if remaining.is_empty() {
                    break;
                }
                let (_pv, segment) = self.load_part_current(project, &qtable, &pid).await?;
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
                    let (_pv, segment) = self.load_part_current(project, &qtable, &pid).await?;
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
                let (_pv, seg0) = self.load_part_current(project, &qtable, &first_pid).await?;
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
                    let (_pv, segment) = self.load_part_current(project, &qtable, &pid).await?;
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
        // Confirm the table exists (NotFound if absent/tombstoned).
        self.load_current(project, &qtable).await?;
        let (_v, segment) = self.load_part_current(project, &qtable, partition_id).await?;
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
        for pid in self.list_partition_ids(project, &qtable).await? {
            let (_v, segment) = self.load_part_current(project, &qtable, &pid).await?;
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
        let (_v, segment) = self.load_part_current(project, &qtable, partition_id).await?;
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
        let head_v = c.resolve_part_head_version(&p, &qt, "p0").await.unwrap().unwrap();
        let key = c.part_segment_key(&p, &qt, "p0", head_v);
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
        let head_v = c.resolve_part_head_version(&p, &qt, "p0").await.unwrap().unwrap();
        let fresh = ObjectStoreCatalog::with_prefix(store.clone(), DEFAULT_CATALOG_PREFIX);
        let folded = fresh.fold_part_chain(&p, &qt, "p0", head_v).await.unwrap();
        assert_eq!(folded.live.len(), N, "compacted fold is still exactly correct");
        assert!(
            folded.deltas_since_baseline < K,
            "fold depth {} must stay < K={K} (bounded read cost)",
            folded.deltas_since_baseline
        );

        // At least one BASELINE object exists in the chain (compaction ran).
        let mut saw_baseline = false;
        for v in 1..=head_v {
            let obj = c.get_part_segment(&p, &qt, "p0", v).await.unwrap();
            if obj.baseline.is_some() {
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
            })
        }
        fn reset(&self) {
            self.gets.store(0, std::sync::atomic::Ordering::Relaxed);
            self.lists.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        fn reads(&self) -> usize {
            self.gets.load(std::sync::atomic::Ordering::Relaxed)
                + self.lists.load(std::sync::atomic::Ordering::Relaxed)
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
}
