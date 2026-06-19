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
use basin_common::{BasinError, ProjectId, Result, TableName};
use bytes::Bytes;
use chrono::Utc;
use object_store::{path::Path as OsPath, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::leases::{Lease, LeaseRegistry};
use crate::metadata::{
    CheckConstraint, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy, ProjectMetadata,
    PromotedJsonbPath, SecondaryIndex, TableFileFormat, TableMetadata, UniqueConstraint,
};
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

/// One cached, resolved manifest plus the version it was read at.
#[derive(Clone)]
struct CacheEntry {
    version: u64,
    manifest: Arc<TableManifest>,
}

/// Basin-native shared catalog backed by an object store.
pub struct ObjectStoreCatalog {
    store: Arc<dyn ObjectStore>,
    root: String,
    /// Monotonic mutation epoch for session-cache validation (same contract as
    /// `InMemoryCatalog::epoch`). Bumped on every successful mutation.
    epoch: AtomicU64,
    /// Per-`(project, table)` resolved-manifest cache. Keyed by the same
    /// `(project, public.table)` shape the rest of the catalog uses.
    cache: Mutex<HashMap<(ProjectId, String), CacheEntry>>,
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
        }
    }

    #[inline]
    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, SeqCst);
    }

    fn table_dir(&self, project: &ProjectId, table: &TableName) -> String {
        // Single-schema (public) keying for v0.1, mirroring the other backends'
        // public-schema fast path. Schema-qualified manifests are a follow-up.
        format!("{}{}/public/{}/", self.root, project, table)
    }

    fn manifest_key(&self, project: &ProjectId, table: &TableName, version: u64) -> OsPath {
        OsPath::from(format!(
            "{}v{version:020}.json",
            self.table_dir(project, table)
        ))
    }

    fn head_key(&self, project: &ProjectId, table: &TableName) -> OsPath {
        OsPath::from(format!("{}HEAD", self.table_dir(project, table)))
    }

    fn cache_key(&self, project: &ProjectId, table: &TableName) -> (ProjectId, String) {
        (*project, table.as_str().to_string())
    }

    /// Resolve the highest manifest version `N` for a table. Tries `HEAD`
    /// first (one GET), then LISTs the table dir and takes the max `v{N}`.
    /// Returns `None` if the table has no manifest at all.
    async fn resolve_head_version(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Option<u64>> {
        // HEAD fast path.
        match self.store.get(&self.head_key(project, table)).await {
            Ok(res) => {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        if let Ok(v) = s.trim().parse::<u64>() {
                            // Trust HEAD only if that manifest actually exists;
                            // a torn/stale HEAD falls through to the LIST scan.
                            if self
                                .store
                                .head(&self.manifest_key(project, table, v))
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
        self.list_max_version(project, table).await
    }

    async fn list_max_version(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Option<u64>> {
        use futures::StreamExt;
        let prefix = OsPath::from(self.table_dir(project, table));
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
        table: &TableName,
        version: u64,
    ) -> Result<TableManifest> {
        let key = self.manifest_key(project, table, version);
        let res = self.store.get(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                BasinError::not_found(format!("{project}/{table}@v{version}"))
            }
            other => storage_err("get manifest", other),
        })?;
        let bytes = res.bytes().await.map_err(|e| storage_err("read manifest", e))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| BasinError::catalog(format!("decode manifest {project}/{table}: {e}")))
    }

    /// Load the current manifest (highest version), using the cache when its
    /// version matches the resolved HEAD. Returns `NotFound` when the table has
    /// no manifest or its latest manifest is a tombstone.
    async fn load_current(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<(u64, Arc<TableManifest>)> {
        let version = self
            .resolve_head_version(project, table)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("{project}/{table}")))?;
        let ck = self.cache_key(project, table);
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&ck) {
                if entry.version == version {
                    if entry.manifest.dropped {
                        return Err(BasinError::not_found(format!("{project}/{table}")));
                    }
                    return Ok((version, entry.manifest.clone()));
                }
            }
        }
        let manifest = Arc::new(self.get_manifest(project, table, version).await?);
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
            return Err(BasinError::not_found(format!("{project}/{table}")));
        }
        Ok((version, manifest))
    }

    /// Write manifest version `version` via create-if-absent. Returns `true` if
    /// we won the race, `false` on `AlreadyExists` (another node got there).
    async fn put_manifest_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        version: u64,
        manifest: &TableManifest,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|e| BasinError::catalog(format!("serialise manifest: {e}")))?;
        let key = self.manifest_key(project, table, version);
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
        table: &TableName,
        version: u64,
        manifest: TableManifest,
    ) {
        // HEAD is an optimisation only; an Overwrite that loses to a concurrent
        // higher commit is fine — readers fall back to the LIST scan.
        let _ = self
            .store
            .put_opts(
                &self.head_key(project, table),
                Bytes::from(version.to_string()).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await;
        let ck = self.cache_key(project, table);
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
    async fn commit_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        operation: SnapshotOperation,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let (version, manifest) = self.load_current(project, table).await?;
        if manifest.current_snapshot != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{project}/{table}: expected snapshot {expected_snapshot}, current is {}",
                manifest.current_snapshot
            )));
        }

        // For Replace, validate removed_paths are in the current live set.
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
                        "{project}/{table}: replace_data_files removed path {p:?} not in live set"
                    )));
                }
            }
        }

        let added_files_count = added_files.len() as u64;
        let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
        let removed_files = removed_paths.len() as u64;

        let new_id = expected_snapshot.next();
        let snap = Snapshot {
            id: new_id,
            parent: Some(expected_snapshot),
            committed_at: Utc::now(),
            data_files: added_files,
            removed_paths,
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
            .put_manifest_create(project, table, next.version, &next)
            .await?
        {
            let meta = next.to_metadata(project, table);
            self.after_commit(project, table, next.version, next).await;
            Ok(meta)
        } else {
            // Lost the create race: someone else committed v{N+1}. Invalidate
            // cache and surface CommitConflict — the engine reloads + retries.
            self.invalidate(project, table).await;
            Err(BasinError::CommitConflict(format!(
                "{project}/{table}: lost commit race at version {}",
                next.version
            )))
        }
    }

    async fn invalidate(&self, project: &ProjectId, table: &TableName) {
        let ck = self.cache_key(project, table);
        self.cache.lock().await.remove(&ck);
    }

    /// Read-modify-write a metadata field with transparent retry on the
    /// create-if-absent race. `mutate` applies the DDL change to a fresh clone
    /// of the current manifest; idempotent DDL re-applies cleanly after a lost
    /// race, so we retry up to [`MAX_DDL_RETRIES`] before surfacing a conflict.
    async fn mutate_manifest<F>(
        &self,
        project: &ProjectId,
        table: &TableName,
        mut mutate: F,
    ) -> Result<TableManifest>
    where
        F: FnMut(&mut TableManifest),
    {
        for _ in 0..MAX_DDL_RETRIES {
            let (version, manifest) = self.load_current(project, table).await?;
            let mut next = (*manifest).clone();
            next.version = version + 1;
            mutate(&mut next);
            if self
                .put_manifest_create(project, table, next.version, &next)
                .await?
            {
                let out = next.clone();
                self.after_commit(project, table, next.version, next).await;
                return Ok(out);
            }
            // Lost the race — reload and retry the (idempotent) mutation.
            self.invalidate(project, table).await;
        }
        Err(BasinError::CommitConflict(format!(
            "{project}/{table}: exhausted DDL retries under contention"
        )))
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
        // Genesis manifest at v0 via create-if-absent. AlreadyExists => the
        // table already has a v0 manifest. If that v0 is a tombstone (dropped),
        // a recreate is allowed by writing a fresh genesis at the next version.
        let manifest = TableManifest::genesis(schema.clone());
        if self.put_manifest_create(project, table, 0, &manifest).await? {
            let meta = manifest.to_metadata(project, table);
            self.after_commit(project, table, 0, manifest).await;
            return Ok(meta);
        }
        // v0 exists. Resolve the live state.
        match self.load_current(project, table).await {
            Ok(_) => Err(BasinError::catalog(format!(
                "table {project}/{table} already exists"
            ))),
            Err(BasinError::NotFound(_)) => {
                // Latest manifest is a tombstone — recreate at next version.
                let version = self
                    .resolve_head_version(project, table)
                    .await?
                    .unwrap_or(0);
                let mut genesis = TableManifest::genesis(schema.clone());
                genesis.version = version + 1;
                if self
                    .put_manifest_create(project, table, genesis.version, &genesis)
                    .await?
                {
                    let meta = genesis.to_metadata(project, table);
                    self.after_commit(project, table, genesis.version, genesis)
                        .await;
                    Ok(meta)
                } else {
                    Err(BasinError::catalog(format!(
                        "table {project}/{table} recreate lost race"
                    )))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn load_table(&self, project: &ProjectId, table: &TableName) -> Result<TableMetadata> {
        let (_v, manifest) = self.load_current(project, table).await?;
        Ok(manifest.to_metadata(project, table))
    }

    async fn current_snapshot_id(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<SnapshotId> {
        let (_v, manifest) = self.load_current(project, table).await?;
        Ok(manifest.current_snapshot)
    }

    async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        // Tombstone: append a manifest version with `dropped = true`. Keeps the
        // history immutable and lets concurrent readers resolve deterministically.
        self.load_current(project, table).await?; // NotFound if absent.
        self.mutate_manifest(project, table, |m| m.dropped = true)
            .await?;
        Ok(())
    }

    async fn rename_table(
        &self,
        project: &ProjectId,
        old: &TableName,
        new: &TableName,
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
        self.mutate_manifest(project, old, |m| m.dropped = true)
            .await?;
        Ok(())
    }

    async fn list_tables(&self, project: &ProjectId) -> Result<Vec<TableName>> {
        use futures::StreamExt;
        // List the project prefix and collect distinct table dirs whose latest
        // manifest is not a tombstone.
        // `OsPath` normalises away the trailing slash, so build the string
        // prefix ourselves and match against `meta.location.as_ref()` (which is
        // the normalised, slash-separated key with no leading/trailing slash).
        let prefix_str = format!("{}{}/public/", self.root, project);
        let list_prefix = OsPath::from(prefix_str.clone());
        let mut stream = self.store.list(Some(&list_prefix));
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| storage_err("list tables", e))?;
            let key = meta.location.as_ref();
            // key = {root}{project}/public/{table}/v{N}.json  (or /HEAD)
            if let Some(rest) = key.strip_prefix(prefix_str.trim_end_matches('/')) {
                let rest = rest.trim_start_matches('/');
                if let Some(table_name) = rest.split('/').next() {
                    if !table_name.is_empty() {
                        names.insert(table_name.to_string());
                    }
                }
            }
        }
        let mut out = Vec::new();
        for name in names {
            let Ok(tn) = TableName::new(name) else { continue };
            // Filter out tombstoned tables.
            match self.load_current(project, &tn).await {
                Ok(_) => out.push(tn),
                Err(BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    async fn append_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        self.commit_snapshot(
            project,
            table,
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
        self.commit_snapshot(
            project,
            table,
            expected_snapshot,
            SnapshotOperation::Replace,
            removed_paths,
            added_files,
        )
        .await
    }

    async fn list_snapshots(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>> {
        let (_v, manifest) = self.load_current(project, table).await?;
        let mut snaps = manifest.snapshots.clone();
        snaps.sort_by_key(|s| s.id);
        Ok(snaps)
    }

    async fn rollback_to_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let (_v, manifest) = self.load_current(project, table).await?;
        if !manifest.snapshots.iter().any(|s| s.id == snapshot_id) {
            return Err(BasinError::not_found(format!(
                "{project}/{table}: snapshot {snapshot_id} not in history"
            )));
        }
        // Collect orphan paths added by discarded snapshots before pruning.
        let next = self
            .mutate_manifest(project, table, |m| {
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
        Ok(next.to_metadata(project, table))
    }

    async fn set_partition_spec(
        &self,
        project: &ProjectId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.partition_spec = spec.clone())
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
        self.mutate_manifest(project, table, |m| {
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
        self.mutate_manifest(project, table, |m| {
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
        self.mutate_manifest(project, table, |m| m.bloom_filter_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_row_group_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.row_group_rows = rows)
            .await?;
        Ok(())
    }

    async fn set_schema(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: Schema,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.schema = schema.clone())
            .await?;
        Ok(())
    }

    async fn set_continuous_aggregate(
        &self,
        project: &ProjectId,
        table: &TableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.continuous_aggregate = def.clone())
            .await?;
        Ok(())
    }

    async fn set_cluster_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.cluster_columns = columns.clone())
            .await?;
        Ok(())
    }

    async fn set_file_format(
        &self,
        project: &ProjectId,
        table: &TableName,
        format: TableFileFormat,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.file_format = format)
            .await?;
        Ok(())
    }

    async fn set_global_sort_order(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| {
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
        self.mutate_manifest(project, table, |m| m.row_block_size = size)
            .await?;
        Ok(())
    }

    async fn set_adaptive_sort_override(
        &self,
        project: &ProjectId,
        table: &TableName,
        value: Option<bool>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.adaptive_sort_override = value)
            .await?;
        Ok(())
    }

    async fn set_home_region(
        &self,
        project: &ProjectId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| m.home_region = region.clone())
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
        self.mutate_manifest(project, table, |m| {
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
        self.mutate_manifest(project, table, |m| {
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
        if columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "create_index: column list cannot be empty".into(),
            ));
        }
        // Validate columns + duplicate name against the current manifest first
        // so we return the right error without burning a version.
        let (_v, manifest) = self.load_current(project, table).await?;
        for col in columns {
            if manifest.schema.field_with_name(col).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "create_index: column {col:?} not in table {project}/{table} schema"
                )));
            }
        }
        if manifest.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(BasinError::catalog(format!(
                "create_index: {project}/{table}: index {name:?} already exists"
            )));
        }
        let idx = SecondaryIndex {
            name: name.to_string(),
            columns: columns.to_vec(),
            access_method: access_method.to_string(),
            opclass: opclass.map(|s| s.to_string()),
        };
        self.mutate_manifest(project, table, |m| {
            if !m.indexes.iter().any(|i| i.name == idx.name) {
                m.indexes.push(idx.clone());
            }
        })
        .await?;
        Ok(())
    }

    async fn drop_index(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        let (_v, manifest) = self.load_current(project, table).await?;
        if !manifest.indexes.iter().any(|i| i.name == name) {
            return Err(BasinError::not_found(format!(
                "{project}/{table}: index {name:?}"
            )));
        }
        self.mutate_manifest(project, table, |m| {
            m.indexes.retain(|i| i.name != name);
        })
        .await?;
        Ok(())
    }

    async fn promote_jsonb_path(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> Result<()> {
        self.mutate_manifest(project, table, |m| {
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

        let mut expected = SnapshotId::GENESIS;
        for i in 0..5 {
            let m = c
                .append_data_files(&p, &t, expected, vec![file(&format!("f{i}.parquet"), 100)])
                .await
                .unwrap();
            assert_eq!(m.current_snapshot, expected.next());
            expected = m.current_snapshot;
        }

        let loaded = c.load_table(&p, &t).await.unwrap();
        assert_eq!(loaded.current_snapshot, SnapshotId(5));
        let live = loaded.live_data_files();
        assert_eq!(live.len(), 5);

        let snaps = c.list_snapshots(&p, &t).await.unwrap();
        assert_eq!(snaps.len(), 6); // genesis + 5
        assert_eq!(c.current_snapshot_id(&p, &t).await.unwrap(), SnapshotId(5));

        // Time-travel rewind.
        let at2 = c
            .load_table_at_snapshot(&p, &t, SnapshotId(2))
            .await
            .unwrap();
        assert_eq!(at2.current_snapshot, SnapshotId(2));
        assert_eq!(at2.live_data_files().len(), 2);
    }

    // --- Test 2: split-brain / double-committer ---------------------------

    #[tokio::test]
    async fn split_brain_exactly_one_winner_per_round() {
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
            // All racers read the SAME expected_snapshot then race to commit.
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
            let mut winners = 0;
            let mut conflicts = 0;
            for h in handles {
                match h.await.unwrap() {
                    Ok(_) => winners += 1,
                    Err(BasinError::CommitConflict(_)) => conflicts += 1,
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
            assert_eq!(winners, 1, "round {round}: exactly one winner");
            assert_eq!(conflicts, RACERS - 1, "round {round}: rest conflict");
            total_committed_files += 1;
        }

        // Chain must be linear and consistent: current == ROUNDS, ids 0..=ROUNDS.
        let meta = c.load_table(&p, &t).await.unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(ROUNDS as u64));
        let snaps = c.list_snapshots(&p, &t).await.unwrap();
        assert_eq!(snaps.len(), ROUNDS + 1); // genesis + one per round
        let ids: Vec<u64> = snaps.iter().map(|s| s.id.0).collect();
        let expected_ids: Vec<u64> = (0..=ROUNDS as u64).collect();
        assert_eq!(ids, expected_ids, "linear, no gaps/dupes");
        // One file committed per round.
        assert_eq!(meta.live_data_files().len() as u64, total_committed_files);
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
        // Genesis + RACERS commits; all files present, ids contiguous.
        assert_eq!(meta.current_snapshot, SnapshotId(RACERS as u64));
        assert_eq!(meta.live_data_files().len(), RACERS);
        let snaps = c.list_snapshots(&p, &t).await.unwrap();
        let ids: Vec<u64> = snaps.iter().map(|s| s.id.0).collect();
        assert_eq!(ids, (0..=RACERS as u64).collect::<Vec<_>>());
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

        // Replace a.parquet -> c.parquet at the correct expected snapshot.
        let m2 = c
            .replace_data_files(&p, &t, head, vec!["a.parquet".into()], vec![file("c.parquet", 5)])
            .await
            .unwrap();
        let live: Vec<String> = m2.live_data_files().into_iter().map(|f| f.path).collect();
        assert!(live.contains(&"b.parquet".to_string()));
        assert!(live.contains(&"c.parquet".to_string()));
        assert!(!live.contains(&"a.parquet".to_string()));

        // Stale expected snapshot -> CommitConflict.
        let err = c
            .replace_data_files(&p, &t, head, vec!["b.parquet".into()], vec![file("d.parquet", 5)])
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::CommitConflict(_)), "got {err:?}");

        // Removing a non-live path -> Catalog error.
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
        assert_eq!(a_snapshot, SnapshotId(3));

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
}
