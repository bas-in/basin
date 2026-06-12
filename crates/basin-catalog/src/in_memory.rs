//! Volatile in-process catalog for tests and the Phase 1 integration.
//!
//! **Not durable.** Process restart loses everything. The whole point of
//! Lakekeeper / a REST catalog is durability; this implementation exists so
//! we can unit-test the wiring without standing up a real catalog.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, QualifiedTableName, Result, SchemaName, TableName};
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};
use tracing::instrument;

use basin_common::ChangeOp;

use crate::cdc_webhooks::{CdcWebhookDef, CdcWebhookRow, CdcWebhookState};
use crate::domains::{self, DomainDef, DomainError};
use crate::enums::{self, EnumError, EnumTypeDef};
use crate::functions::SqlFunctionDef;
use crate::metadata::{
    CheckConstraint, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy, ProjectMetadata,
    PromotedJsonbPath, SecondaryIndex, TableFileFormat, TableMetadata, UniqueConstraint,
};
use crate::inbound_webhooks::{self, InboundWebhookDef, InboundWebhookError};
use crate::procedures::{self, ProcedureError, SqlProcedureDef};
use crate::project_storage_config::ProjectStorageConfig;
use crate::reactors::{self, ReactorDef, ReactorError};
use crate::sequences::{advance_one, SequenceDef, SequenceError, SequenceState};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::views::ViewDef;
use crate::Catalog;

/// One table's mutable state. The per-table mutex serializes commits so
/// `append_data_files` is atomic without blocking commits to *other* tables.
struct TableState {
    schema: Arc<Schema>,
    current: SnapshotId,
    snapshots: Vec<Snapshot>,
    partition_spec: PartitionSpec,
    rls_enabled: bool,
    policies: Vec<Policy>,
    cold_after_seconds: Option<u64>,
    cold_age_column: Option<String>,
    bloom_filter_columns: Vec<String>,
    row_group_rows: Option<usize>,
    continuous_aggregate: Option<CvDef>,
    cluster_columns: Vec<String>,
    file_format: TableFileFormat,
    row_block_size: Option<u32>,
    home_region: Option<String>,
    indexes: Vec<SecondaryIndex>,
    pk_columns: Vec<String>,
    check_constraints: Vec<CheckConstraint>,
    foreign_keys: Vec<ForeignKeyDef>,
    unique_constraints: Vec<UniqueConstraint>,
    global_sort_order: Option<Vec<String>>,
    adaptive_sort_override: Option<bool>,
    /// Paths collected by `rollback_to_snapshot` that were written by
    /// now-discarded snapshots.  These are catalog-orphans: they do not
    /// appear in any live snapshot but were once recorded in the table's
    /// history.  The GC sweep includes this list in the "universe" so
    /// `gc_orphaned_files` can identify them as physically deletable even
    /// though the snapshot records that added them have been pruned.
    gc_orphan_paths: Vec<String>,
    /// ADR 0027 Phase 4 — promoted JSONB top-level paths.
    promoted_jsonb_paths: Vec<PromotedJsonbPath>,
}

impl TableState {
    fn genesis(schema: Arc<Schema>) -> Self {
        let now = Utc::now();
        let genesis = Snapshot {
            id: SnapshotId::GENESIS,
            parent: None,
            committed_at: now,
            data_files: Vec::new(),
            removed_paths: vec![],
            summary: SnapshotSummary {
                operation: SnapshotOperation::Genesis,
                added_files: 0,
                added_rows: 0,
                added_bytes: 0,
                removed_files: 0,
            },
        };
        Self {
            schema,
            current: SnapshotId::GENESIS,
            snapshots: vec![genesis],
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
        }
    }
}

/// Internal storage key. All table operations are keyed by
/// `(ProjectId, QualifiedTableName)` so the same table name in different
/// schemas does not collide.
type TableMap = HashMap<(ProjectId, QualifiedTableName), Arc<Mutex<TableState>>>;

/// In-memory implementation of [`Catalog`]. Cheap to clone via `Arc`.
///
/// Concurrency model:
/// * `tables` is a `tokio::sync::RwLock<HashMap<...>>`. Reads (every SELECT's
///   load_table) take the shared lock so concurrent readers never queue on
///   each other — with the old Mutex, 16 concurrent point reads serialized
///   through this one lock and queueing dominated their latency. Writers
///   (DDL) take the exclusive lock; they are rare. The guard is held only
///   long enough to look up (or insert) the per-table `Arc<Mutex<TableState>>`,
///   then dropped before doing any real work.
/// * Commits and reads on a single `(project, table)` serialize on that table's
///   own mutex. Two projects — or two different tables in the same project —
///   never block each other.
///
/// We use `tokio::sync::Mutex` (not `std::sync::Mutex`) because async code
/// holds the per-table guard across `.await` points and a poisoned `std`
/// mutex would force every method to handle poisoning.
pub struct InMemoryCatalog {
    /// Monotonically increasing mutation counter. Bumped by SeqCst fetch_add
    /// on every method that changes catalog state. Session caches store the
    /// epoch at fill time and compare against this on each read; a mismatch
    /// causes an immediate cache-miss refetch.
    epoch: AtomicU64,
    tables: RwLock<TableMap>,
    namespaces: Mutex<HashSet<ProjectId>>,
    /// Per-project registered SQL functions. One shared `HashMap` keyed by
    /// `(ProjectId, name)` so per-project cost stays `O(bytes)` — no
    /// per-project heavy resource.
    sql_functions: Mutex<HashMap<(ProjectId, String), SqlFunctionDef>>,
    /// Per-project registered sequences. Same shape rule as `sql_functions`:
    /// one shared `HashMap` keyed by `(ProjectId, name)`. Each value is a
    /// pair of (immutable definition, per-sequence Mutex around the
    /// counter state). The outer mutex is held only long enough to look
    /// up (or insert) the per-sequence handle; concurrent `nextval` calls
    /// on different sequences never block each other.
    sequences: Mutex<HashMap<(ProjectId, String), Arc<SequenceEntry>>>,
    /// Per-project registered reactors. Same shape rule as
    /// `sql_functions`: one shared `HashMap` keyed by `(ProjectId,
    /// composite)` where `composite = "<table>:<reactor_name>"` —
    /// reactor names are unique per `(project, table)`, not per project.
    /// Per-project cost stays `O(bytes)` with no per-project heavy
    /// resource. Each value carries a monotonic `seq` so
    /// `lookup_reactors_for` can replay reactors in registration order.
    reactors: Mutex<ReactorState>,
    /// Per-project `CREATE TYPE … AS ENUM` declarations. Same shape
    /// rule as `sql_functions`.
    enum_types: Mutex<HashMap<(ProjectId, String), EnumTypeDef>>,
    /// Per-project `CREATE DOMAIN` declarations.
    domains: Mutex<HashMap<(ProjectId, String), DomainDef>>,
    /// Per-project `CREATE PROCEDURE … LANGUAGE sql` declarations.
    /// Same shape rule as `sql_functions`: one shared `HashMap` keyed
    /// by `(ProjectId, name)` so per-project cost stays `O(bytes)` with
    /// no per-project heavy resource.
    procedures: Mutex<HashMap<(ProjectId, String), SqlProcedureDef>>,
    /// Per-project storage config (KMS routing + provider extras).
    /// Single shared `HashMap` keyed by `ProjectId`; lazy entry creation;
    /// per-project cost stays `O(bytes)` with no per-project heavy
    /// resource. Cleared in `drop_namespace`.
    project_storage_config: Mutex<HashMap<ProjectId, ProjectStorageConfig>>,
    /// Per-project plain-view definitions (`CREATE VIEW … AS SELECT …`).
    /// Same shape as `sql_functions`: keyed by `(ProjectId, lower-name)`.
    views: Mutex<HashMap<(ProjectId, String), ViewDef>>,
    /// Per-project schema set. `public` is always pre-seeded on
    /// `create_namespace`. Used by `list_schemas`, `create_schema`, and
    /// `drop_schema`.
    schemas: Mutex<HashMap<ProjectId, HashSet<SchemaName>>>,
    /// Per-project inbound webhook definitions (Phase 5.11.N, ADR 0019).
    /// One shared `HashMap` keyed by `(ProjectId, name)` — per-project cost
    /// stays `O(bytes)` with no per-project heavy resource.
    inbound_webhooks: Mutex<HashMap<(ProjectId, String), InboundWebhookDef>>,
    /// Phase 6.X.A — partition leases (ADR 0023). One shared `HashMap` keyed
    /// by `(ProjectId, partition_id)`. The outer mutex serialises the
    /// acquire / renew / steal CAS so two contending replicas can't both win;
    /// it is held only for the brief in-map arithmetic. Per-project cost stays
    /// `O(partitions)` with no per-replica heavy resource.
    leases: Mutex<HashMap<(ProjectId, String), LeaseRow>>,
    /// Per-project metadata (BYO-bucket config, etc.). Single shared
    /// `HashMap` keyed by `ProjectId`; lazy entry creation; per-project
    /// cost stays `O(bytes)` with no per-project heavy resource.
    /// Cleared in `drop_namespace`. Implements `set_project_metadata` /
    /// `get_project_metadata` on the `Catalog` trait (T-048).
    project_metadata: Mutex<HashMap<ProjectId, ProjectMetadata>>,
    /// Per-project pgwire connection ceiling. Written by the admin route;
    /// read by the pgwire startup handler. Cleared in `drop_namespace`.
    project_max_connections: Mutex<HashMap<ProjectId, u32>>,
    /// Per-project home region (Fly.io region code). Written by the admin
    /// placement route; consumed by the multi-region router (future). Cleared
    /// in `drop_namespace`. `None` means "no placement pin".
    project_home_regions: Mutex<HashMap<ProjectId, String>>,
    /// Per-`(project, partition)` compaction watermark — the highest WAL LSN
    /// whose tail batch has been compacted into a catalog-committed cold file.
    /// Mirrors the Postgres `compaction_watermarks` table. Monotonic: writes
    /// take `max(existing, new)`. Read by shard cold-start replay to skip
    /// already-committed entries (the commit→truncate crash-window dedup).
    compaction_watermarks: Mutex<HashMap<(ProjectId, String), u64>>,
    /// ADR 0028 Phase 2 — per-project CDC webhook subscriptions + delivery
    /// cursor. One shared `HashMap` keyed by `(ProjectId, webhook_id)`; the
    /// value bundles the subscription `def` with its mutable delivery `state`.
    /// Per-project cost stays `O(bytes)`. Cleared in `drop_namespace`.
    cdc_webhooks: Mutex<HashMap<(ProjectId, String), (CdcWebhookDef, CdcWebhookState)>>,
}

/// In-memory lease row. Mirrors the `partition_leases` Postgres table.
#[derive(Clone)]
struct LeaseRow {
    holder: String,
    epoch: i64,
    /// Recorded for parity with the Postgres `granted_at` column / future
    /// observability (lease age). Not read on the in-memory hot path.
    #[allow(dead_code)]
    granted_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
}

/// Aggregate reactor state. The seq counter assigns each newly-
/// registered reactor a strictly-increasing index; `lookup_reactors_for`
/// sorts matching reactors by it so callers see PG-shaped registration
/// order.
#[derive(Default)]
struct ReactorState {
    /// Composite key = `(project, "<table>:<name>")`.
    map: HashMap<(ProjectId, String), ReactorEntry>,
    next_seq: u64,
}

/// One reactor catalog row plus its registration index.
#[derive(Clone)]
struct ReactorEntry {
    def: ReactorDef,
    seq: u64,
}

/// One sequence's catalog row plus its per-sequence Mutex-guarded state.
/// The fields are independently locked — definitions are immutable after
/// `create_sequence`, so we don't need to synchronise reads of `def`
/// against writes of `state`.
struct SequenceEntry {
    def: SequenceDef,
    state: Mutex<SequenceState>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            tables: RwLock::new(HashMap::new()),
            namespaces: Mutex::new(HashSet::new()),
            sql_functions: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
            reactors: Mutex::new(ReactorState::default()),
            enum_types: Mutex::new(HashMap::new()),
            domains: Mutex::new(HashMap::new()),
            procedures: Mutex::new(HashMap::new()),
            project_storage_config: Mutex::new(HashMap::new()),
            views: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            inbound_webhooks: Mutex::new(HashMap::new()),
            leases: Mutex::new(HashMap::new()),
            project_metadata: Mutex::new(HashMap::new()),
            project_max_connections: Mutex::new(HashMap::new()),
            project_home_regions: Mutex::new(HashMap::new()),
            compaction_watermarks: Mutex::new(HashMap::new()),
            cdc_webhooks: Mutex::new(HashMap::new()),
        }
    }

    /// Bump the mutation epoch. Called at the start of every method that
    /// changes catalog state so session caches can detect staleness via a
    /// single atomic load rather than a full catalog round-trip.
    #[inline(always)]
    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, SeqCst);
    }

    async fn get_table(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Arc<Mutex<TableState>>> {
        // Phase 5.18.C (ADR 0022): try the public schema first (fast path),
        // then fall back to searching all schemas for a unique bare-name match.
        // This allows catalog operations to resolve system-schema tables
        // (cron.job, auth.users, net._http_response, etc.) by bare name.
        let pub_qtable = QualifiedTableName::in_public(table.clone());
        {
            let tables = self.tables.read().await;
            if tables.contains_key(&(*project, pub_qtable.clone())) {
                drop(tables);
                return self.get_table_qualified(project, &pub_qtable).await;
            }
            // Search non-public schemas for a table with the same bare name.
            let candidates: Vec<QualifiedTableName> = tables
                .keys()
                .filter(|key| key.0 == *project && key.1.name == *table)
                .map(|key| key.1.clone())
                .collect();
            drop(tables);
            if candidates.len() == 1 {
                return self.get_table_qualified(project, &candidates[0]).await;
            }
        }
        // Not found in any schema, or ambiguous — return the public-schema
        // NotFound so callers get the expected error format.
        self.get_table_qualified(project, &pub_qtable).await
    }

    /// Internal helper that persists an index with full access_method + opclass
    /// metadata. Both `create_index_qualified` and `create_index_with_method`
    /// delegate here so the column-validation and duplicate-name logic lives in
    /// one place.
    async fn create_index_qualified_with_method(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
        access_method: &str,
        opclass: Option<&str>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        if columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "create_index: column list cannot be empty".into(),
            ));
        }
        // For GIN indexes the column is a JSONB column; its Arrow type is Utf8
        // with a BASIN_TYPE=JSONB marker.  The schema field_with_name check
        // validates the column exists regardless of type.
        for col in columns {
            if state.schema.field_with_name(col).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "create_index: column {col:?} not in table {project}/{qtable} schema"
                )));
            }
        }
        if state.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(BasinError::catalog(format!(
                "create_index: {project}/{qtable}: index {name:?} already exists"
            )));
        }
        state.indexes.push(SecondaryIndex {
            name: name.to_string(),
            columns: columns.to_vec(),
            access_method: access_method.to_string(),
            opclass: opclass.map(|s| s.to_string()),
        });
        Ok(())
    }

    async fn get_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Arc<Mutex<TableState>>> {
        let key = (*project, qtable.clone());
        let guard = self.tables.read().await;
        guard
            .get(&key)
            .cloned()
            .ok_or_else(|| BasinError::not_found(format!("{project}/{qtable}")))
    }

    fn build_metadata(project: &ProjectId, table: &TableName, state: &TableState) -> TableMetadata {
        TableMetadata {
            project: *project,
            table: table.clone(),
            schema: state.schema.clone(),
            current_snapshot: state.current,
            snapshots: state.snapshots.clone(),
            format_version: 2,
            partition_spec: state.partition_spec.clone(),
            rls_enabled: state.rls_enabled,
            policies: state.policies.clone(),
            cold_after_seconds: state.cold_after_seconds,
            cold_age_column: state.cold_age_column.clone(),
            bloom_filter_columns: state.bloom_filter_columns.clone(),
            row_group_rows: state.row_group_rows,
            continuous_aggregate: state.continuous_aggregate.clone(),
            cluster_columns: state.cluster_columns.clone(),
            file_format: state.file_format,
            row_block_size: state.row_block_size,
            home_region: state.home_region.clone(),
            indexes: state.indexes.clone(),
            pk_columns: state.pk_columns.clone(),
            check_constraints: state.check_constraints.clone(),
            foreign_keys: state.foreign_keys.clone(),
            unique_constraints: state.unique_constraints.clone(),
            global_sort_order: state.global_sort_order.clone(),
            adaptive_sort_override: state.adaptive_sort_override,
            gc_orphan_paths: state.gc_orphan_paths.clone(),
            promoted_jsonb_paths: state.promoted_jsonb_paths.clone(),
        }
    }

    /// Phase 5.18.C (ADR 0022): resolve a bare table name to a
    /// [`QualifiedTableName`] for `project`.
    ///
    /// Strategy:
    /// 1. Try `public.{table}` first (fast path for the common case).
    /// 2. If not found in `public`, search all schemas for a unique bare-name
    ///    match. This allows catalog operations to resolve system-schema tables
    ///    (`cron.job`, `auth.users`, `net._http_response`, etc.) by bare name.
    /// 3. If not found in any schema, return `public.{table}` as the qualified
    ///    name (so `get_table_qualified` will produce the expected NotFound error).
    ///
    /// This method is SYNCHRONOUS after lock acquisition (no await inside the
    /// critical section) to avoid holding the tables mutex across awaits.
    async fn resolve_qtable(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> QualifiedTableName {
        let pub_qtable = QualifiedTableName::in_public(table.clone());
        let tables = self.tables.read().await;
        if tables.contains_key(&(*project, pub_qtable.clone())) {
            return pub_qtable;
        }
        // Search non-public schemas.
        let mut candidate: Option<QualifiedTableName> = None;
        for key in tables.keys() {
            if key.0 == *project && key.1.name == *table {
                if candidate.is_some() {
                    // Ambiguous: more than one non-public schema has this table.
                    // Fall back to public so get_table_qualified returns NotFound.
                    return pub_qtable;
                }
                candidate = Some(key.1.clone());
            }
        }
        candidate.unwrap_or(pub_qtable)
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Catalog for InMemoryCatalog {
    fn epoch(&self) -> u64 {
        self.epoch.load(SeqCst)
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn create_namespace(&self, project: &ProjectId) -> Result<()> {
        self.bump_epoch();
        self.namespaces.lock().await.insert(*project);
        // Phase 5.18.A: pre-seed ALL reserved schemas so that schema-qualified
        // catalog operations on any reserved schema work without an explicit
        // `create_schema` call. Mirrors the PostgresCatalog behaviour.
        let mut schemas = self.schemas.lock().await;
        let schema_set = schemas.entry(*project).or_insert_with(HashSet::new);
        for &reserved in crate::reserved_schema::ReservedSchema::ALL {
            schema_set.insert(reserved.to_schema_name());
        }
        Ok(())
    }

    #[instrument(skip(self, schema), fields(project = %project, table = %table))]
    async fn create_table(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: &Schema,
    ) -> Result<TableMetadata> {
        // Old API: always creates in the public schema.
        let qtable = QualifiedTableName::in_public(table.clone());
        self.create_table_qualified(project, &qtable, Arc::new(schema.clone()))
            .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn load_table(&self, project: &ProjectId, table: &TableName) -> Result<TableMetadata> {
        // Phase 5.18.C (ADR 0022): try the public schema first (fast path), then
        // fall back to searching all schemas for a unique bare-name match. This
        // allows the executor's DML operations (INSERT/UPDATE/DELETE) to resolve
        // system-schema tables (cron.job, auth.users, net._http_response, etc.)
        // that are referenced by their bare name after DataFusion schema stripping.
        let pub_qtable = QualifiedTableName::in_public(table.clone());
        {
            let tables = self.tables.read().await;
            if tables.contains_key(&(*project, pub_qtable.clone())) {
                drop(tables);
                return self.load_table_qualified(project, &pub_qtable).await;
            }
            // Search non-public schemas for a table with the same bare name.
            let candidates: Vec<QualifiedTableName> = tables
                .keys()
                .filter(|key| key.0 == *project && key.1.name == *table)
                .map(|key| key.1.clone())
                .collect();
            drop(tables);
            if candidates.len() == 1 {
                return self.load_table_qualified(project, &candidates[0]).await;
            }
            if candidates.len() > 1 {
                // Ambiguous bare name — multiple schemas have a table with this
                // name. Fall through to the NotFound error so callers that care
                // can use `load_table_qualified` with an explicit schema.
                return Err(basin_common::BasinError::not_found(format!(
                    "{project}/{table}: ambiguous bare name (found in schemas: {})",
                    candidates
                        .iter()
                        .map(|qt| qt.schema.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        // Not found in any schema.
        Err(basin_common::BasinError::not_found(format!(
            "{project}/{pub_qtable}"
        )))
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.drop_table_qualified(project, &qtable).await
    }

    #[instrument(skip(self), fields(project = %project, old = %old, new = %new))]
    async fn rename_table(
        &self,
        project: &ProjectId,
        old: &TableName,
        new: &TableName,
    ) -> Result<()> {
        let qold = self.resolve_qtable(project, old).await;
        // For dst, use public schema (new tables always go to public unless qualified)
        let qnew = QualifiedTableName::in_public(new.clone());
        self.rename_table_qualified(project, &qold, &qnew).await
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_tables(&self, project: &ProjectId) -> Result<Vec<TableName>> {
        // Back-compat: return only the bare table names from the public schema.
        let tables = self.tables.read().await;
        let public = SchemaName::public();
        let mut out: Vec<TableName> = tables
            .keys()
            .filter(|(t, qtable)| t == project && qtable.schema == public)
            .map(|(_, qtable)| qtable.name.clone())
            .collect();
        out.sort();
        Ok(out)
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn drop_namespace(&self, project: &ProjectId) -> Result<()> {
        self.bump_epoch();
        // Single-pass: hold the table-map mutex once, drop every entry whose
        // project matches. Cheaper than the default-impl loop (N small awaits)
        // and atomic w.r.t. concurrent list_tables on the same in-memory map.
        let mut tables = self.tables.write().await;
        tables.retain(|(t, _), _| t != project);
        let mut namespaces = self.namespaces.lock().await;
        namespaces.remove(project);
        let mut funcs = self.sql_functions.lock().await;
        funcs.retain(|(t, _), _| t != project);
        let mut seqs = self.sequences.lock().await;
        seqs.retain(|(t, _), _| t != project);
        let mut reactors = self.reactors.lock().await;
        reactors.map.retain(|(t, _), _| t != project);
        let mut enums = self.enum_types.lock().await;
        enums.retain(|(t, _), _| t != project);
        let mut doms = self.domains.lock().await;
        doms.retain(|(t, _), _| t != project);
        let mut procs = self.procedures.lock().await;
        procs.retain(|(t, _), _| t != project);
        let mut storage_cfg = self.project_storage_config.lock().await;
        storage_cfg.remove(project);
        let mut schemas = self.schemas.lock().await;
        schemas.remove(project);
        let mut iwhs = self.inbound_webhooks.lock().await;
        iwhs.retain(|(t, _), _| t != project);
        let mut leases = self.leases.lock().await;
        leases.retain(|(t, _), _| t != project);
        let mut pm = self.project_metadata.lock().await;
        pm.remove(project);
        // Clear per-project connection ceiling.
        let mut mc = self.project_max_connections.lock().await;
        mc.remove(project);
        // Clear per-project home region pin.
        let mut hr = self.project_home_regions.lock().await;
        hr.remove(project);
        // Clear per-project CDC webhook subscriptions + cursors.
        let mut cwh = self.cdc_webhooks.lock().await;
        cwh.retain(|(t, _), _| t != project);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_project_data_files(&self, project: &ProjectId) -> Result<Vec<DataFileRef>> {
        // Walk every table's full snapshot history under one map-lock-free
        // pass: clone the per-table Arc<Mutex> handles up front, then drain
        // each snapshot list while holding only that table's mutex. Avoids
        // re-acquiring the top-level map mutex per table the way the default
        // impl (list_tables → load_table loop) would.
        let handles: Vec<Arc<Mutex<TableState>>> = {
            let tables = self.tables.read().await;
            tables
                .iter()
                .filter(|((proj, _), _)| proj == project)
                .map(|(_, state)| state.clone())
                .collect()
        };
        let mut out: Vec<DataFileRef> = Vec::new();
        for state in handles {
            let guard = state.lock().await;
            for snap in &guard.snapshots {
                out.extend(snap.data_files.iter().cloned());
            }
        }
        Ok(out)
    }

    #[instrument(
        skip(self, files),
        fields(
            project = %project,
            table = %table,
            expected_snapshot = %expected_snapshot,
            file_count = files.len(),
        ),
    )]
    async fn append_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.append_data_files_qualified(project, &qtable, expected_snapshot, files)
            .await
    }

    #[instrument(
        skip(self, removed_paths, added_files),
        fields(
            project = %project,
            table = %table,
            expected_snapshot = %expected_snapshot,
            removed = removed_paths.len(),
            added = added_files.len(),
        ),
    )]
    async fn replace_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.replace_data_files_qualified(
            project,
            &qtable,
            expected_snapshot,
            removed_paths,
            added_files,
        )
        .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn list_snapshots(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>> {
        let qtable = self.resolve_qtable(project, table).await;
        self.list_snapshots_qualified(project, &qtable).await
    }

    #[instrument(skip(self), fields(project = %project, src = %src_table, dst = %dst_table))]
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

    #[instrument(skip(self), fields(project = %project, table = %table, snapshot = %snapshot_id))]
    async fn rollback_to_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let qtable = self.resolve_qtable(project, table).await;
        self.rollback_to_snapshot_qualified(project, &qtable, snapshot_id)
            .await
    }

    #[instrument(skip(self, spec), fields(project = %project, table = %table))]
    async fn set_partition_spec(
        &self,
        project: &ProjectId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_partition_spec_qualified(project, &qtable, spec)
            .await
    }

    #[instrument(skip(self, policies), fields(project = %project, table = %table))]
    async fn set_rls_state(
        &self,
        project: &ProjectId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_rls_state_qualified(project, &qtable, rls_enabled, policies)
            .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn set_tier_policy(
        &self,
        project: &ProjectId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_tier_policy_qualified(project, &qtable, cold_after_seconds, cold_age_column)
            .await
    }

    #[instrument(skip(self, columns), fields(project = %project, table = %table, n = columns.len()))]
    async fn set_bloom_filter_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_bloom_filter_columns_qualified(project, &qtable, columns)
            .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn set_row_group_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_row_group_rows_qualified(project, &qtable, rows)
            .await
    }

    #[instrument(skip(self, schema), fields(project = %project, table = %table))]
    async fn set_schema(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: Schema,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_schema_qualified(project, &qtable, schema).await
    }

    #[instrument(skip(self, def), fields(project = %project, table = %table))]
    async fn set_continuous_aggregate(
        &self,
        project: &ProjectId,
        table: &TableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_continuous_aggregate_qualified(project, &qtable, def)
            .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn set_cluster_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_cluster_columns_qualified(project, &qtable, columns)
            .await
    }

    async fn set_row_block_size(
        &self,
        project: &ProjectId,
        table: &TableName,
        size: Option<u32>,
    ) -> Result<()> {
        self.bump_epoch();
        let qtable = self.resolve_qtable(project, table).await;
        let state_arc = self.get_table_qualified(project, &qtable).await?;
        let mut state = state_arc.lock().await;
        state.row_block_size = size;
        Ok(())
    }

    async fn set_adaptive_sort_override(
        &self,
        project: &ProjectId,
        table: &TableName,
        value: Option<bool>,
    ) -> Result<()> {
        self.bump_epoch();
        let qtable = self.resolve_qtable(project, table).await;
        let state_arc = self.get_table_qualified(project, &qtable).await?;
        let mut state = state_arc.lock().await;
        state.adaptive_sort_override = value;
        Ok(())
    }

    /// #161: real impl — persist the table's on-disk data-file format
    /// into the in-memory table state so `load_table` returns it.
    /// Mirrors `set_cluster_columns_qualified`'s state-mutation pattern.
    async fn set_file_format(
        &self,
        project: &ProjectId,
        table: &TableName,
        format: TableFileFormat,
    ) -> Result<()> {
        self.bump_epoch();
        let qtable = self.resolve_qtable(project, table).await;
        let state_arc = self.get_table_qualified(project, &qtable).await?;
        let mut state = state_arc.lock().await;
        state.file_format = format;
        Ok(())
    }

    /// W3-R3: persist the user-asserted global sort order so that
    /// `load_table` returns it and the session can wire
    /// `ListingOptions::with_file_sort_order`.
    async fn set_global_sort_order(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.bump_epoch();
        let qtable = self.resolve_qtable(project, table).await;
        let state_arc = self.get_table_qualified(project, &qtable).await?;
        let mut state = state_arc.lock().await;
        state.global_sort_order = if columns.is_empty() {
            None
        } else {
            Some(columns)
        };
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, table = %table))]
    async fn set_home_region(
        &self,
        project: &ProjectId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.set_home_region_qualified(project, &qtable, region)
            .await
    }

    #[instrument(skip(self, check_constraints, foreign_keys), fields(project = %project, table = %table))]
    async fn set_table_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        pk_columns: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table(project, table).await?;
        let mut state = state_arc.lock().await;
        state.pk_columns = pk_columns;
        state.check_constraints = check_constraints;
        state.foreign_keys = foreign_keys;
        Ok(())
    }

    #[instrument(skip(self, unique_constraints), fields(project = %project, table = %table))]
    async fn set_unique_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table(project, table).await?;
        let mut state = state_arc.lock().await;
        state.unique_constraints = unique_constraints;
        Ok(())
    }

    #[instrument(skip(self, columns), fields(project = %project, table = %table, name = %name))]
    async fn create_index(
        &self,
        project: &ProjectId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.create_index_qualified(project, &qtable, name, columns, if_not_exists)
            .await
    }

    #[instrument(skip(self), fields(project = %project, table = %table, name = %name))]
    async fn drop_index(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.drop_index_qualified(project, &qtable, name).await
    }

    /// ADR 0027 Phase 4: declare a promoted JSONB top-level path.
    async fn promote_jsonb_path(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> Result<()> {
        self.bump_epoch();
        let qtable = self.resolve_qtable(project, table).await;
        let tables = self.tables.read().await;
        let entry = tables.get(&(*project, qtable)).ok_or_else(|| {
            BasinError::not_found(format!("{project}: table {table}"))
        })?;
        let mut state = entry.lock().await;
        let already = state.promoted_jsonb_paths.iter().any(|p| {
            p.source_col == source_col && p.json_key == json_key
        });
        if !already {
            state.promoted_jsonb_paths.push(PromotedJsonbPath {
                source_col: source_col.to_string(),
                json_key: json_key.to_string(),
            });
        }
        Ok(())
    }

    #[instrument(skip(self, def), fields(project = %def.project, name = %def.name))]
    async fn register_sql_function(&self, mut def: SqlFunctionDef) -> Result<()> {
        self.bump_epoch();
        let key = (def.project, def.name.clone());
        let mut map = self.sql_functions.lock().await;
        // Phase 5.11.W6: bump `version` on REPLACE so callers (CDN
        // invalidator, basin-fn runtime cache) can detect redeploys
        // without diffing the body bytes. First registration keeps the
        // caller-supplied version (defaults to `1`).
        if let Some(existing) = map.get(&key) {
            def.version = existing.version.saturating_add(1);
        }
        map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn drop_sql_function(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, name.to_string());
        let mut map = self.sql_functions.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{project}: sql function {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn lookup_sql_function(&self, project: &ProjectId, name: &str) -> Option<SqlFunctionDef> {
        let key = (*project, name.to_string());
        let map = self.sql_functions.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_sql_functions(&self, project: &ProjectId) -> Vec<SqlFunctionDef> {
        let map = self.sql_functions.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(project = %def.project, name = %def.name))]
    async fn create_sequence(&self, def: SequenceDef) -> Result<()> {
        self.bump_epoch();
        if def.increment == 0 {
            return Err(BasinError::InvalidSchema(
                "sequence increment must be non-zero".into(),
            ));
        }
        let key = (def.project, def.name.clone());
        let mut map = self.sequences.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "sequence {}/{} already exists",
                def.project, def.name,
            )));
        }
        let state = SequenceState::genesis(&def);
        let entry = SequenceEntry {
            def,
            state: Mutex::new(state),
        };
        map.insert(key, Arc::new(entry));
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn drop_sequence(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, name.to_string());
        let mut map = self.sequences.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{project}: sequence {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn lookup_sequence(&self, project: &ProjectId, name: &str) -> Option<SequenceDef> {
        let key = (*project, name.to_string());
        let map = self.sequences.lock().await;
        map.get(&key).map(|e| e.def.clone())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn nextval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        let entry = {
            let key = (*project, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?
        };
        // Per-sequence mutex serialises increments; concurrent callers
        // see distinct values, but two sequences (or two projects) never
        // block each other beyond the top-level HashMap probe above.
        let mut state = entry.state.lock().await;
        match advance_one(&entry.def, &mut state) {
            Ok(v) => Ok(v),
            Err(SequenceError::Exhausted) => Err(BasinError::catalog(format!(
                "{project}: sequence {name:?} exhausted"
            ))),
            Err(SequenceError::InvalidIncrement) => Err(BasinError::InvalidSchema(format!(
                "{project}: sequence {name:?} has zero increment"
            ))),
        }
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn currval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        let entry = {
            let key = (*project, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?
        };
        let state = entry.state.lock().await;
        if !state.started {
            return Err(BasinError::not_found(format!(
                "{project}: sequence {name:?} has not been advanced"
            )));
        }
        Ok(state.current.load(std::sync::atomic::Ordering::Relaxed))
    }

    #[instrument(skip(self), fields(project = %project, name = %name, value = value, advance = advance))]
    async fn setval(
        &self,
        project: &ProjectId,
        name: &str,
        value: i64,
        advance: bool,
    ) -> Result<i64> {
        let entry = {
            let key = (*project, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{project}: sequence {name:?}")))?
        };
        let mut state = entry.state.lock().await;
        // PG's `setval(seq, n, true)` (the default) makes `n` the most
        // recently handed-out value, so the next `nextval` returns
        // `n + increment`. `setval(seq, n, false)` arms the sequence so
        // the next `nextval` returns `n` directly. We model the latter
        // by storing `n - increment` so the started-state advance step
        // (`current + increment`) lands on `n`. Either way `started`
        // becomes true — both forms imply the sequence has been
        // touched, so a subsequent `currval` is well-defined.
        let stored = if advance {
            value
        } else {
            value.wrapping_sub(entry.def.increment)
        };
        state
            .current
            .store(stored, std::sync::atomic::Ordering::Relaxed);
        state
            .block_end
            .store(stored, std::sync::atomic::Ordering::Relaxed);
        state.started = true;
        Ok(value)
    }

    #[instrument(skip(self, def), fields(project = %def.project, table = %def.table, name = %def.name))]
    async fn register_reactor(&self, def: ReactorDef) -> Result<()> {
        self.bump_epoch();
        if def.ops.is_empty() {
            return Err(BasinError::InvalidSchema(
                "reactor ops bitset is empty".into(),
            ));
        }
        reactors::validate_body(&def.body).map_err(reactor_err_to_basin)?;
        if let Some(p) = &def.when_predicate {
            reactors::validate_predicate(p).map_err(reactor_err_to_basin)?;
        }

        let key = (def.project, format!("{}:{}", def.table, def.name));
        let mut state = self.reactors.lock().await;
        if state.map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "reactor {:?} on {}/{} already exists",
                def.name, def.project, def.table
            )));
        }
        state.next_seq += 1;
        let seq = state.next_seq;
        state.map.insert(key, ReactorEntry { def, seq });
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, table = %table, name = %name))]
    async fn drop_reactor(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, format!("{table}:{name}"));
        let mut state = self.reactors.lock().await;
        if state.map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{project}/{table}: reactor {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, table = %table, op = ?op))]
    async fn lookup_reactors_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        op: ChangeOp,
    ) -> Vec<ReactorDef> {
        let state = self.reactors.lock().await;
        let mut hits: Vec<&ReactorEntry> = state
            .map
            .iter()
            .filter(|((t, _), entry)| {
                t == project && &entry.def.table == table && entry.def.ops.matches(op)
            })
            .map(|(_, entry)| entry)
            .collect();
        hits.sort_by_key(|e| e.seq);
        hits.into_iter().map(|e| e.def.clone()).collect()
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_reactors(&self, project: &ProjectId) -> Vec<ReactorDef> {
        let state = self.reactors.lock().await;
        let mut hits: Vec<&ReactorEntry> = state
            .map
            .iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, entry)| entry)
            .collect();
        hits.sort_by_key(|e| e.seq);
        hits.into_iter().map(|e| e.def.clone()).collect()
    }

    #[instrument(skip(self, def), fields(project = %def.project, name = %def.name))]
    async fn register_enum_type(&self, def: EnumTypeDef) -> Result<()> {
        self.bump_epoch();
        enums::validate_new(&def).map_err(enum_err_to_basin)?;
        let key = (def.project, def.name.clone());
        let mut enums_map = self.enum_types.lock().await;
        if enums_map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "enum type {}/{} already exists",
                def.project, def.name,
            )));
        }
        // Cross-namespace collision: a domain with the same name on the
        // same project is rejected so column resolution stays
        // unambiguous.
        let doms = self.domains.lock().await;
        if doms.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "type {}/{} collides with an existing domain",
                def.project, def.name,
            )));
        }
        drop(doms);
        enums_map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn lookup_enum_type(&self, project: &ProjectId, name: &str) -> Option<EnumTypeDef> {
        let key = (*project, name.to_string());
        let map = self.enum_types.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(project = %project, name = %name, value = %value))]
    async fn add_enum_value(&self, project: &ProjectId, name: &str, value: &str) -> Result<()> {
        self.bump_epoch();
        if value.is_empty() {
            return Err(BasinError::InvalidSchema(
                "ALTER TYPE ADD VALUE: label cannot be empty".into(),
            ));
        }
        let key = (*project, name.to_string());
        let mut map = self.enum_types.lock().await;
        let def = map
            .get_mut(&key)
            .ok_or_else(|| BasinError::not_found(format!("{project}: enum type {name:?}")))?;
        if def.labels.iter().any(|l| l == value) {
            return Err(BasinError::catalog(format!(
                "enum {name:?} already contains value {value:?}"
            )));
        }
        def.labels.push(value.to_string());
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn drop_enum_type(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        // Refcount: scan every table the project owns and reject the
        // drop when any column carries `BASIN_ENUM_TYPE=<name>`. v0.1
        // has no CASCADE; the message tells the caller to drop the
        // columns first.
        let referencing = self.tables_referencing_type(project, name, true).await;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop enum {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let key = (*project, name.to_string());
        let mut map = self.enum_types.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{project}: enum type {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_enum_types(&self, project: &ProjectId) -> Vec<EnumTypeDef> {
        let map = self.enum_types.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(project = %def.project, name = %def.name))]
    async fn register_domain(&self, def: DomainDef) -> Result<()> {
        self.bump_epoch();
        domains::validate_new(&def).map_err(domain_err_to_basin)?;
        let key = (def.project, def.name.clone());
        let mut doms = self.domains.lock().await;
        if doms.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "domain {}/{} already exists",
                def.project, def.name,
            )));
        }
        let enums_map = self.enum_types.lock().await;
        if enums_map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "domain {}/{} collides with an existing enum type",
                def.project, def.name,
            )));
        }
        drop(enums_map);
        doms.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn lookup_domain(&self, project: &ProjectId, name: &str) -> Option<DomainDef> {
        let key = (*project, name.to_string());
        let map = self.domains.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn drop_domain(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        let referencing = self.tables_referencing_type(project, name, false).await;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop domain {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let key = (*project, name.to_string());
        let mut map = self.domains.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!("{project}: domain {name:?}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_domains(&self, project: &ProjectId) -> Vec<DomainDef> {
        let map = self.domains.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_sequences(&self, project: &ProjectId) -> Vec<SequenceDef> {
        let map = self.sequences.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, entry)| entry.def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(project = %def.project, name = %def.name))]
    async fn register_procedure(&self, def: SqlProcedureDef) -> Result<()> {
        self.bump_epoch();
        procedures::validate_new(&def).map_err(procedure_err_to_basin)?;
        let key = (def.project, def.name.clone());
        let mut map = self.procedures.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "procedure {}/{} already exists",
                def.project, def.name,
            )));
        }
        map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn drop_procedure(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, name.to_string());
        let mut map = self.procedures.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{project}: procedure {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, name = %name))]
    async fn lookup_procedure(&self, project: &ProjectId, name: &str) -> Option<SqlProcedureDef> {
        let key = (*project, name.to_string());
        let map = self.procedures.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn list_procedures(&self, project: &ProjectId) -> Vec<SqlProcedureDef> {
        let map = self.procedures.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, config), fields(project = %project))]
    async fn set_project_storage_config(
        &self,
        project: &ProjectId,
        config: ProjectStorageConfig,
    ) -> Result<()> {
        self.bump_epoch();
        let mut map = self.project_storage_config.lock().await;
        map.insert(*project, config);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn get_project_storage_config(
        &self,
        project: &ProjectId,
    ) -> Result<Option<ProjectStorageConfig>> {
        let map = self.project_storage_config.lock().await;
        Ok(map.get(project).cloned())
    }

    #[instrument(skip(self, meta), fields(project = %project))]
    async fn set_project_metadata(
        &self,
        project: &ProjectId,
        meta: ProjectMetadata,
    ) -> Result<()> {
        self.bump_epoch();
        let mut map = self.project_metadata.lock().await;
        map.insert(*project, meta);
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project))]
    async fn get_project_metadata(&self, project: &ProjectId) -> Result<ProjectMetadata> {
        let map = self.project_metadata.lock().await;
        Ok(map.get(project).cloned().unwrap_or_default())
    }

    #[instrument(skip(self), fields(project = %project, partition = %partition_id))]
    async fn set_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
        watermark_lsn: u64,
    ) -> Result<()> {
        // Note: deliberately does NOT bump_epoch — the watermark is shard
        // recovery state, not table metadata the session caches key on.
        let mut map = self.compaction_watermarks.lock().await;
        let slot = map.entry((*project, partition_id.to_string())).or_insert(0);
        // Monotonic: never rewind the replay floor.
        if watermark_lsn > *slot {
            *slot = watermark_lsn;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(project = %project, partition = %partition_id))]
    async fn get_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<u64>> {
        let map = self.compaction_watermarks.lock().await;
        Ok(map.get(&(*project, partition_id.to_string())).copied())
    }

    async fn set_project_max_connections(
        &self,
        project: &ProjectId,
        max_connections: u32,
    ) -> Result<()> {
        let mut m = self.project_max_connections.lock().await;
        m.insert(*project, max_connections);
        Ok(())
    }

    async fn get_project_max_connections(&self, project: &ProjectId) -> Result<Option<u32>> {
        let m = self.project_max_connections.lock().await;
        Ok(m.get(project).copied())
    }

    async fn set_project_home_region(
        &self,
        project: &ProjectId,
        home_region: &str,
    ) -> Result<()> {
        let mut m = self.project_home_regions.lock().await;
        m.insert(*project, home_region.to_owned());
        Ok(())
    }

    async fn get_project_home_region(&self, project: &ProjectId) -> Result<Option<String>> {
        let m = self.project_home_regions.lock().await;
        Ok(m.get(project).cloned())
    }

    async fn register_view(&self, def: ViewDef, or_replace: bool) -> Result<()> {
        self.bump_epoch();
        let key = (def.project, def.name.to_ascii_lowercase());
        let mut map = self.views.lock().await;
        if !or_replace && map.contains_key(&key) {
            return Err(BasinError::Catalog(format!(
                "view {:?} already exists",
                def.name
            )));
        }
        map.insert(key, def);
        Ok(())
    }

    async fn drop_view(&self, project: &ProjectId, name: &str, if_exists: bool) -> Result<()> {
        self.bump_epoch();
        let key = (*project, name.to_ascii_lowercase());
        let mut map = self.views.lock().await;
        if map.remove(&key).is_none() && !if_exists {
            return Err(BasinError::NotFound(format!(
                "view {name:?} does not exist"
            )));
        }
        Ok(())
    }

    async fn lookup_view(&self, project: &ProjectId, name: &str) -> Option<ViewDef> {
        let key = (*project, name.to_ascii_lowercase());
        let map = self.views.lock().await;
        map.get(&key).cloned()
    }

    async fn list_views(&self, project: &ProjectId) -> Vec<ViewDef> {
        let map = self.views.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, v)| v.clone())
            .collect()
    }

    // -----------------------------------------------------------------------
    // Phase 5.11.N — inbound webhook receivers (ADR 0019)
    // -----------------------------------------------------------------------

    async fn register_inbound_webhook(&self, def: InboundWebhookDef) -> Result<()> {
        self.bump_epoch();
        inbound_webhooks::validate_body(&def.body).map_err(inbound_webhook_err_to_basin)?;
        let key = (def.project, def.name.clone());
        let mut map = self.inbound_webhooks.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::Catalog(format!(
                "inbound webhook {:?} already exists for project {}",
                def.name, def.project
            )));
        }
        map.insert(key, def);
        Ok(())
    }

    async fn drop_inbound_webhook(&self, project: &ProjectId, name: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, name.to_string());
        let mut map = self.inbound_webhooks.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "inbound webhook {name:?} does not exist"
            )));
        }
        Ok(())
    }

    async fn lookup_inbound_webhook(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Option<InboundWebhookDef> {
        let key = (*project, name.to_string());
        let map = self.inbound_webhooks.lock().await;
        map.get(&key).cloned()
    }

    async fn list_inbound_webhooks(&self, project: &ProjectId) -> Vec<InboundWebhookDef> {
        let map = self.inbound_webhooks.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, v)| v.clone())
            .collect()
    }

    // ---- ADR 0028 Phase 2: CDC webhooks ---------------------------------

    async fn register_cdc_webhook(&self, def: CdcWebhookDef) -> Result<()> {
        self.bump_epoch();
        let key = (def.project, def.id.clone());
        let mut map = self.cdc_webhooks.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::Catalog(format!(
                "cdc webhook {:?} already exists for project {}",
                def.id, def.project
            )));
        }
        map.insert(key, (def, CdcWebhookState::default()));
        Ok(())
    }

    async fn drop_cdc_webhook(&self, project: &ProjectId, id: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, id.to_string());
        let mut map = self.cdc_webhooks.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "cdc webhook {id:?} does not exist"
            )));
        }
        Ok(())
    }

    async fn list_cdc_webhooks(&self, project: &ProjectId) -> Vec<CdcWebhookRow> {
        let map = self.cdc_webhooks.lock().await;
        let mut rows: Vec<CdcWebhookRow> = map
            .iter()
            .filter(|((t, _), _)| t == project)
            .map(|(_, (def, state))| CdcWebhookRow {
                def: def.clone(),
                state: state.clone(),
            })
            .collect();
        // Stable order for the GET route / deterministic tests.
        rows.sort_by(|a, b| a.def.id.cmp(&b.def.id));
        rows
    }

    async fn record_cdc_webhook_ack(
        &self,
        project: &ProjectId,
        id: &str,
        last_seq: u64,
        last_status: &str,
    ) -> Result<()> {
        let key = (*project, id.to_string());
        let mut map = self.cdc_webhooks.lock().await;
        if let Some((_, state)) = map.get_mut(&key) {
            // Monotonic cursor: never rewind (mirrors compaction watermark).
            state.last_seq = state.last_seq.max(last_seq);
            state.last_status = Some(last_status.to_string());
            state.retry_count = 0;
        }
        Ok(())
    }

    async fn record_cdc_webhook_failure(
        &self,
        project: &ProjectId,
        id: &str,
        retry_count: u32,
        last_status: &str,
    ) -> Result<()> {
        let key = (*project, id.to_string());
        let mut map = self.cdc_webhooks.lock().await;
        if let Some((_, state)) = map.get_mut(&key) {
            state.retry_count = retry_count;
            state.last_status = Some(last_status.to_string());
        }
        Ok(())
    }

    async fn disable_cdc_webhook(&self, project: &ProjectId, id: &str) -> Result<()> {
        self.bump_epoch();
        let key = (*project, id.to_string());
        let mut map = self.cdc_webhooks.lock().await;
        match map.get_mut(&key) {
            Some((def, state)) => {
                def.active = false;
                state.disabled_at = Some(chrono::Utc::now().to_rfc3339());
                Ok(())
            }
            None => Err(BasinError::not_found(format!(
                "cdc webhook {id:?} does not exist"
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // Phase 6: refcount-aware GC — InMemoryCatalog optimised override.
    //
    // Instead of N list_tables + load_table round-trips (the default-impl
    // path), we snapshot the per-table Arc handles in one map-lock pass,
    // then read each table's snapshots directly.  This stays entirely in
    // memory without any per-table mutex round-trip overhead.
    // -----------------------------------------------------------------------

    async fn gc_orphaned_files(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> basin_common::Result<crate::GcReport> {
        use std::collections::HashSet;

        // Snapshot all per-table Arc handles for this project in one pass.
        let handles: Vec<(QualifiedTableName, std::sync::Arc<tokio::sync::Mutex<TableState>>)> = {
            let tables = self.tables.read().await;
            tables
                .iter()
                .filter(|((p, _), _)| p == project)
                .map(|((_, qt), arc)| (qt.clone(), arc.clone()))
                .collect()
        };

        // Identify the target table handle (for its universe).
        // Phase 5.18.C: resolve bare name across all schemas (system tables may
        // live in non-public schemas like cron.job, auth.users, etc.).
        let resolved_qtable = self.resolve_qtable(project, table).await;
        let target_arc = handles
            .iter()
            .find(|(qt, _)| qt == &resolved_qtable)
            .map(|(_, arc)| arc.clone())
            .ok_or_else(|| BasinError::not_found(format!("{project}/{table}")))?;

        // Build the universe: every path ever recorded in the target table's
        // snapshot history (added files + removed_paths from Replace commits),
        // PLUS paths saved in `gc_orphan_paths` by prior `rollback_to_snapshot`
        // calls (those snapshot records were pruned but the paths are still
        // candidates for physical deletion).
        let universe: HashSet<String> = {
            let guard = target_arc.lock().await;
            let mut u = HashSet::new();
            for snap in &guard.snapshots {
                for f in &snap.data_files {
                    u.insert(f.path.clone());
                }
                for p in &snap.removed_paths {
                    u.insert(p.clone());
                }
            }
            // Include rollback-orphaned paths collected at rollback time.
            for p in &guard.gc_orphan_paths {
                u.insert(p.clone());
            }
            u
        };

        // Build the live set across ALL tables in the project.
        // live_data_files() replays the snapshot chain; a path shared by a
        // fork and its source appears once per table here — but for the GC
        // decision we only need to know "is it in ANY live set?".
        let mut live: HashSet<String> = HashSet::new();
        for (_, arc) in &handles {
            let guard = arc.lock().await;
            // Compute live_data_files inline: replay snapshots in id order.
            let mut ordered: Vec<&crate::snapshot::Snapshot> = guard.snapshots.iter().collect();
            ordered.sort_by_key(|s| s.id);
            let mut live_map: std::collections::HashMap<String, ()> = std::collections::HashMap::new();
            for snap in ordered {
                if snap.id > guard.current {
                    break;
                }
                for p in &snap.removed_paths {
                    live_map.remove(p);
                }
                for f in &snap.data_files {
                    live_map.insert(f.path.clone(), ());
                }
            }
            for path in live_map.into_keys() {
                live.insert(path);
            }
        }

        let mut orphaned_paths: Vec<String> = universe
            .iter()
            .filter(|p| !live.contains(*p))
            .cloned()
            .collect();
        orphaned_paths.sort();

        let mut live_paths: Vec<String> = universe
            .iter()
            .filter(|p| live.contains(*p))
            .cloned()
            .collect();
        live_paths.sort();

        Ok(crate::GcReport {
            orphaned_paths,
            live_paths,
        })
    }

    // -----------------------------------------------------------------------
    // Phase A.2 overrides: schema-qualified operations
    //
    // InMemoryCatalog provides real multi-schema semantics. All `*_qualified`
    // methods operate directly on the `(ProjectId, QualifiedTableName)` key,
    // while the old `*_table` / `*_data_files` methods are kept as thin
    // wrappers that default to the public schema (done above in the old-method
    // overrides).
    // -----------------------------------------------------------------------

    async fn create_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<TableMetadata> {
        self.bump_epoch();
        // Auto-create the namespace on first table; the dedicated
        // `create_namespace` call is for callers that want the explicit step.
        self.namespaces.lock().await.insert(*project);
        // Ensure the schema exists in the schema-set. If it doesn't exist yet
        // we auto-create it here (matches PG behaviour where schemas can be
        // implicitly created by a `CREATE TABLE schema.t`).
        self.schemas
            .lock()
            .await
            .entry(*project)
            .or_insert_with(HashSet::new)
            .insert(qtable.schema.clone());

        let key = (*project, qtable.clone());
        let mut tables = self.tables.write().await;
        if tables.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "table {project}/{qtable} already exists"
            )));
        }
        let state = TableState::genesis(schema);
        let meta = Self::build_metadata(project, &qtable.name, &state);
        tables.insert(key, Arc::new(Mutex::new(state)));
        Ok(meta)
    }

    async fn load_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        let state = self.get_table_qualified(project, qtable).await?;
        let guard = state.lock().await;
        Ok(Self::build_metadata(project, &qtable.name, &guard))
    }

    async fn drop_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<()> {
        self.bump_epoch();
        let key = (*project, qtable.clone());
        let mut tables = self.tables.write().await;
        tables
            .remove(&key)
            .ok_or_else(|| BasinError::not_found(format!("{project}/{qtable}")))?;
        Ok(())
    }

    async fn rename_table_qualified(
        &self,
        project: &ProjectId,
        old: &QualifiedTableName,
        new: &QualifiedTableName,
    ) -> Result<()> {
        self.bump_epoch();
        let old_key = (*project, old.clone());
        let new_key = (*project, new.clone());
        let mut tables = self.tables.write().await;
        if tables.contains_key(&new_key) {
            return Err(BasinError::catalog(format!(
                "rename_table: target {project}/{new} already exists"
            )));
        }
        // Look up the old entry and *alias* the new key to its Arc.
        // v0.1 trade-off: the old name stays as a synonym until DROP.
        let entry = tables
            .get(&old_key)
            .cloned()
            .ok_or_else(|| BasinError::not_found(format!("{project}/{old}")))?;
        tables.insert(new_key, entry);
        Ok(())
    }

    async fn list_tables_qualified(&self, project: &ProjectId) -> Result<Vec<QualifiedTableName>> {
        let tables = self.tables.read().await;
        let mut out: Vec<QualifiedTableName> = tables
            .keys()
            .filter(|(p, _)| p == project)
            .map(|(_, qtable)| qtable.clone())
            .collect();
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
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;

        if state.current != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{project}/{qtable}: expected snapshot {expected_snapshot}, current is {}",
                state.current
            )));
        }

        let added_files = files.len() as u64;
        let added_rows: u64 = files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();

        let parent = state.current;
        let new_id = parent.next();
        let snap = Snapshot {
            id: new_id,
            parent: Some(parent),
            committed_at: Utc::now(),
            data_files: files,
            removed_paths: vec![],
            summary: SnapshotSummary {
                operation: SnapshotOperation::Append,
                added_files,
                added_rows,
                added_bytes,
                removed_files: 0,
            },
        };
        state.snapshots.push(snap);
        state.current = new_id;
        Ok(Self::build_metadata(project, &qtable.name, &state))
    }

    async fn replace_data_files_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;

        if state.current != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{project}/{qtable}: expected snapshot {expected_snapshot}, current is {}",
                state.current
            )));
        }

        let parent = state.current;
        let added_files_count = added_files.len() as u64;
        let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
        let removed_files_count = removed_paths.len() as u64;

        let new_id = parent.next();
        let snap = Snapshot {
            id: new_id,
            parent: Some(parent),
            committed_at: Utc::now(),
            data_files: added_files,
            removed_paths,
            summary: SnapshotSummary {
                operation: SnapshotOperation::Replace,
                added_files: added_files_count,
                added_rows,
                added_bytes,
                removed_files: removed_files_count,
            },
        };
        state.snapshots.push(snap);
        state.current = new_id;
        Ok(Self::build_metadata(project, &qtable.name, &state))
    }

    async fn list_snapshots_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Vec<Snapshot>> {
        let state = self.get_table_qualified(project, qtable).await?;
        let guard = state.lock().await;
        Ok(guard.snapshots.clone())
    }

    async fn fork_table_qualified(
        &self,
        project: &ProjectId,
        src: &QualifiedTableName,
        dst: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        self.bump_epoch();
        // Read source state then drop the per-table guard before grabbing
        // the table-map guard for the insert.
        let cloned_state = {
            let src_arc = self.get_table_qualified(project, src).await?;
            let s = src_arc.lock().await;
            TableState {
                schema: s.schema.clone(),
                current: s.current,
                snapshots: s.snapshots.clone(),
                partition_spec: s.partition_spec.clone(),
                rls_enabled: s.rls_enabled,
                policies: s.policies.clone(),
                cold_after_seconds: s.cold_after_seconds,
                cold_age_column: s.cold_age_column.clone(),
                bloom_filter_columns: s.bloom_filter_columns.clone(),
                row_group_rows: s.row_group_rows,
                continuous_aggregate: s.continuous_aggregate.clone(),
                cluster_columns: s.cluster_columns.clone(),
                file_format: s.file_format,
                row_block_size: s.row_block_size,
                home_region: s.home_region.clone(),
                indexes: s.indexes.clone(),
                pk_columns: s.pk_columns.clone(),
                check_constraints: s.check_constraints.clone(),
                foreign_keys: s.foreign_keys.clone(),
                unique_constraints: s.unique_constraints.clone(),
                global_sort_order: s.global_sort_order.clone(),
                adaptive_sort_override: s.adaptive_sort_override,
                // The fork starts with a clean GC orphan list: any paths
                // that were orphaned in the source before this fork are the
                // source's problem, not the fork's.
                gc_orphan_paths: Vec::new(),
                // Fork inherits the promoted JSONB paths so queries against
                // the fork benefit from the same shadow columns.
                promoted_jsonb_paths: s.promoted_jsonb_paths.clone(),
            }
        };

        let dst_key = (*project, dst.clone());
        let mut tables = self.tables.write().await;
        if tables.contains_key(&dst_key) {
            return Err(BasinError::catalog(format!(
                "fork_table: {project}/{dst} already exists",
            )));
        }
        let dst_arc = Arc::new(Mutex::new(cloned_state));
        tables.insert(dst_key, dst_arc.clone());
        drop(tables);

        let dst_state = dst_arc.lock().await;
        Ok(Self::build_metadata(project, &dst.name, &dst_state))
    }

    async fn rollback_to_snapshot_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;

        if !state.snapshots.iter().any(|s| s.id == snapshot_id) {
            return Err(BasinError::not_found(format!(
                "{project}/{qtable}: snapshot {snapshot_id} not in history",
            )));
        }

        // Collect the paths added by snapshots being discarded (id >
        // snapshot_id) into the persistent orphan list.  This preserves GC
        // visibility after the snapshot records are pruned: `gc_orphaned_files`
        // will find these paths in `gc_orphan_paths` and check whether any
        // other table still has a live reference to them.
        let discarded_paths: Vec<String> = state
            .snapshots
            .iter()
            .filter(|s| s.id > snapshot_id)
            .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
            .collect();
        state.gc_orphan_paths.extend(discarded_paths);
        // Deduplicate — multiple rollbacks can accumulate the same path.
        state.gc_orphan_paths.sort();
        state.gc_orphan_paths.dedup();

        state.snapshots.retain(|s| s.id <= snapshot_id);
        state.current = snapshot_id;

        Ok(Self::build_metadata(project, &qtable.name, &state))
    }

    async fn set_partition_spec_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.partition_spec = spec;
        Ok(())
    }

    async fn set_rls_state_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.rls_enabled = rls_enabled;
        state.policies = policies;
        Ok(())
    }

    async fn set_tier_policy_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.cold_after_seconds = cold_after_seconds;
        state.cold_age_column = cold_age_column;
        Ok(())
    }

    async fn set_bloom_filter_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.bloom_filter_columns = columns;
        Ok(())
    }

    async fn set_row_group_rows_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rows: Option<usize>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.row_group_rows = rows;
        Ok(())
    }

    async fn set_schema_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: arrow_schema::Schema,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.schema = Arc::new(schema);
        Ok(())
    }

    async fn set_continuous_aggregate_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        def: Option<crate::metadata::CvDef>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.continuous_aggregate = def;
        Ok(())
    }

    async fn set_cluster_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.cluster_columns = columns;
        Ok(())
    }

    async fn set_home_region_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        region: Option<String>,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        state.home_region = region;
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
        self.create_index_qualified_with_method(
            project, qtable, name, columns, if_not_exists, "btree", None,
        )
        .await
    }

    async fn create_index_with_method(
        &self,
        project: &ProjectId,
        table: &basin_common::TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
        access_method: &str,
        opclass: Option<&str>,
    ) -> Result<()> {
        let qtable = self.resolve_qtable(project, table).await;
        self.create_index_qualified_with_method(
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

    async fn drop_index_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
    ) -> Result<()> {
        self.bump_epoch();
        let state_arc = self.get_table_qualified(project, qtable).await?;
        let mut state = state_arc.lock().await;
        let before = state.indexes.len();
        state.indexes.retain(|i| i.name != name);
        if state.indexes.len() == before {
            return Err(BasinError::not_found(format!(
                "{project}/{qtable}: index {name:?}"
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Schema-level operations
    // -----------------------------------------------------------------------

    async fn list_schemas(&self, project: &ProjectId) -> Result<Vec<SchemaName>> {
        let schemas = self.schemas.lock().await;
        let mut out: Vec<SchemaName> = schemas
            .get(project)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        out.sort();
        Ok(out)
    }

    async fn create_schema(&self, project: &ProjectId, schema: &SchemaName) -> Result<()> {
        self.bump_epoch();
        self.schemas
            .lock()
            .await
            .entry(*project)
            .or_insert_with(HashSet::new)
            .insert(schema.clone());
        Ok(())
    }

    async fn drop_schema(
        &self,
        project: &ProjectId,
        schema: &SchemaName,
        cascade: bool,
    ) -> Result<()> {
        self.bump_epoch();
        if schema == &SchemaName::public() {
            return Err(BasinError::catalog("cannot drop the public schema"));
        }
        // Check whether the schema exists at all.
        {
            let schemas = self.schemas.lock().await;
            if !schemas
                .get(project)
                .map(|s| s.contains(schema))
                .unwrap_or(false)
            {
                return Err(BasinError::not_found(format!(
                    "{project}: schema {schema:?}"
                )));
            }
        }
        // Collect tables in this schema.
        let tables_in_schema: Vec<QualifiedTableName> = {
            let tables = self.tables.read().await;
            tables
                .keys()
                .filter(|(p, qt)| p == project && &qt.schema == schema)
                .map(|(_, qt)| qt.clone())
                .collect()
        };
        if !cascade && !tables_in_schema.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop schema {schema}: {} table(s) still exist (use CASCADE to drop them)",
                tables_in_schema.len()
            )));
        }
        // CASCADE: drop all tables.
        if cascade {
            let mut tables = self.tables.write().await;
            for qt in &tables_in_schema {
                tables.remove(&(*project, qt.clone()));
            }
        }
        // Remove the schema itself.
        let mut schemas = self.schemas.lock().await;
        if let Some(set) = schemas.get_mut(project) {
            set.remove(schema);
        }
        Ok(())
    }
}

impl InMemoryCatalog {
    /// Walk every table owned by `project`, returning `<table>.<column>`
    /// labels for every column whose Arrow `Field` carries the
    /// requested type metadata. `is_enum == true` checks the
    /// `BASIN_ENUM_TYPE` key; `false` checks `BASIN_DOMAIN`. Used by
    /// `drop_enum_type` / `drop_domain` to reject drops that would
    /// orphan a column type.
    async fn tables_referencing_type(
        &self,
        project: &ProjectId,
        type_name: &str,
        is_enum: bool,
    ) -> Vec<String> {
        let key = if is_enum {
            crate::enums::BASIN_ENUM_TYPE_KEY
        } else {
            crate::domains::BASIN_DOMAIN_KEY
        };
        // Snapshot the per-table handles so we don't hold the
        // top-level table-map lock while inspecting each one's schema.
        let handles: Vec<(QualifiedTableName, Arc<Mutex<TableState>>)> = {
            let tables = self.tables.read().await;
            tables
                .iter()
                .filter(|((t, _), _)| t == project)
                .map(|((_, qtable), state)| (qtable.clone(), state.clone()))
                .collect()
        };
        let mut out = Vec::new();
        for (qtable, state) in handles {
            let guard = state.lock().await;
            for f in guard.schema.fields() {
                if f.metadata().get(key).map(|s| s.as_str()) == Some(type_name) {
                    out.push(format!("{qtable}.{}", f.name()));
                }
            }
        }
        out
    }
}

/// Map [`EnumError`] into the cross-crate [`BasinError`] surface.
fn enum_err_to_basin(e: EnumError) -> BasinError {
    match e {
        EnumError::DuplicateLabel(l) => {
            BasinError::InvalidSchema(format!("enum label {l:?} listed more than once"))
        }
        EnumError::EmptyLabelList => {
            BasinError::InvalidSchema("enum type must have at least one label".into())
        }
        EnumError::EmptyLabel => {
            BasinError::InvalidSchema("enum label must be a non-empty string".into())
        }
        EnumError::Duplicate => BasinError::Catalog("enum type already exists".into()),
        EnumError::NotFound => BasinError::NotFound("enum type not found".into()),
        EnumError::LabelAlreadyExists(l) => {
            BasinError::Catalog(format!("enum already contains value {l:?}"))
        }
    }
}

/// Map [`DomainError`] into the cross-crate [`BasinError`] surface.
fn domain_err_to_basin(e: DomainError) -> BasinError {
    match e {
        DomainError::Duplicate => BasinError::Catalog("domain already exists".into()),
        DomainError::NotFound => BasinError::NotFound("domain not found".into()),
        DomainError::InvalidPredicate(msg) => {
            BasinError::InvalidSchema(format!("domain CHECK predicate: {msg}"))
        }
    }
}

/// Map [`ProcedureError`] into the cross-crate [`BasinError`] surface.
fn procedure_err_to_basin(e: ProcedureError) -> BasinError {
    match e {
        ProcedureError::InvalidBody(msg) => {
            BasinError::InvalidSchema(format!("procedure body: {msg}"))
        }
        ProcedureError::DisallowedStatement(msg) => BasinError::InvalidSchema(msg),
        ProcedureError::DuplicateArgName(name) => {
            BasinError::InvalidSchema(format!("duplicate procedure argument name {name:?}"))
        }
        ProcedureError::InvalidName(msg) => BasinError::InvalidIdent(msg),
    }
}

/// Map [`ReactorError`] into the cross-crate [`BasinError`] surface.
fn inbound_webhook_err_to_basin(e: InboundWebhookError) -> BasinError {
    match e {
        InboundWebhookError::Duplicate => {
            BasinError::Catalog("inbound webhook with the same name already exists".into())
        }
        InboundWebhookError::InvalidBody(msg) => {
            BasinError::InvalidSchema(format!("inbound webhook body: {msg}"))
        }
        InboundWebhookError::MultiStatementBody => BasinError::InvalidSchema(
            "inbound webhook body must be a single SQL statement".into(),
        ),
        InboundWebhookError::NotFound => BasinError::NotFound("inbound webhook not found".into()),
    }
}

fn reactor_err_to_basin(e: ReactorError) -> BasinError {
    match e {
        ReactorError::Duplicate => {
            BasinError::Catalog("reactor with the same name already exists".into())
        }
        ReactorError::InvalidBody(msg) => BasinError::InvalidSchema(format!("reactor body: {msg}")),
        ReactorError::InvalidPredicate(msg) => {
            BasinError::InvalidSchema(format!("reactor when-predicate: {msg}"))
        }
        ReactorError::NoOps => BasinError::InvalidSchema("reactor ops bitset is empty".into()),
        ReactorError::MultiStatementBody => {
            BasinError::InvalidSchema("reactor body must be a single SQL statement".into())
        }
        ReactorError::NotFound => BasinError::NotFound("reactor not found".into()),
    }
}

#[async_trait]
impl crate::leases::LeaseRegistry for InMemoryCatalog {
    #[instrument(skip(self), fields(project = %project, partition = %partition_id, holder = %holder))]
    async fn acquire(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<crate::leases::Lease>> {
        let now = chrono::Utc::now();
        let expires_at = now + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::zero());
        let key = (*project, partition_id.to_string());
        let mut map = self.leases.lock().await;
        let (granted, epoch) = match map.get(&key) {
            // First grant: no row yet. Epoch starts at 1.
            None => (true, 1),
            // We already hold it (or it's expired): (re)grant + bump epoch.
            Some(row) if row.holder == holder || row.expires_at <= now => {
                (true, row.epoch + 1)
            }
            // A different, non-expired holder owns it: lose the race.
            Some(_) => (false, 0),
        };
        if !granted {
            return Ok(None);
        }
        let lease = crate::leases::Lease {
            project: *project,
            partition_id: partition_id.to_string(),
            holder: holder.to_string(),
            epoch,
            granted_at: now,
            expires_at,
        };
        map.insert(
            key,
            LeaseRow {
                holder: holder.to_string(),
                epoch,
                granted_at: now,
                expires_at,
            },
        );
        Ok(Some(lease))
    }

    #[instrument(skip(self), fields(project = %project, partition = %partition_id, holder = %holder, epoch))]
    async fn renew(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        epoch: i64,
        ttl: std::time::Duration,
    ) -> Result<bool> {
        let now = chrono::Utc::now();
        let expires_at = now + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::zero());
        let key = (*project, partition_id.to_string());
        let mut map = self.leases.lock().await;
        match map.get_mut(&key) {
            // Still ours at the exact epoch, and not yet expired.
            Some(row) if row.holder == holder && row.epoch == epoch && row.expires_at > now => {
                row.expires_at = expires_at;
                Ok(true)
            }
            // Lost the lease: stolen, regranted, expired, or never held.
            _ => Ok(false),
        }
    }

    #[instrument(skip(self), fields(project = %project, partition = %partition_id, holder = %holder))]
    async fn release(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
    ) -> Result<bool> {
        let key = (*project, partition_id.to_string());
        let mut map = self.leases.lock().await;
        match map.get(&key) {
            Some(row) if row.holder == holder => {
                map.remove(&key);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    #[instrument(skip(self), fields(project = %project, partition = %partition_id))]
    async fn owner_of(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<(String, i64)>> {
        let now = chrono::Utc::now();
        let key = (*project, partition_id.to_string());
        let map = self.leases.lock().await;
        Ok(map.get(&key).and_then(|row| {
            // Expired rows have no live owner even though the row lingers.
            if row.expires_at > now {
                Some((row.holder.clone(), row.epoch))
            } else {
                None
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{BasinError, ProjectId, QualifiedTableName, SchemaName, TableName};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn file(path: &str, rows: u64, bytes: u64) -> DataFileRef {
        DataFileRef {
            path: path.into(),
            size_bytes: bytes,
            row_count: rows,
            column_stats: std::collections::BTreeMap::new(),
            bloom_filters: ::std::collections::BTreeMap::new(),
            hll_sketches: ::std::collections::BTreeMap::new(),
            tdigest_sketches: ::std::collections::BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn create_load_drop_table() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("users").unwrap();

        cat.create_namespace(&t).await.unwrap();
        let meta = cat.create_table(&t, &tbl, &schema()).await.unwrap();
        assert_eq!(meta.format_version, 2);
        assert_eq!(meta.current_snapshot, SnapshotId::GENESIS);
        assert_eq!(meta.snapshots.len(), 1);
        assert_eq!(
            meta.snapshots[0].summary.operation,
            SnapshotOperation::Genesis
        );

        let loaded = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(loaded.project, t);
        assert_eq!(loaded.table, tbl);
        assert_eq!(loaded.current_snapshot, SnapshotId::GENESIS);

        cat.drop_table(&t, &tbl).await.unwrap();
    }

    #[tokio::test]
    async fn drop_then_load_returns_not_found() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("ghost").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.drop_table(&t, &tbl).await.unwrap();
        let err = cat.load_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");

        let err = cat.drop_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn project_isolation() {
        let cat = InMemoryCatalog::new();
        let a = ProjectId::new();
        let b = ProjectId::new();
        let tbl = TableName::new("orders").unwrap();

        let meta_a = cat.create_table(&a, &tbl, &schema()).await.unwrap();
        let meta_b = cat.create_table(&b, &tbl, &schema()).await.unwrap();
        assert_eq!(meta_a.project, a);
        assert_eq!(meta_b.project, b);
        assert_ne!(meta_a.project, meta_b.project);

        // Independent advance: appending to A does not change B.
        cat.append_data_files(
            &a,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a/data/0.parquet", 10, 100)],
        )
        .await
        .unwrap();
        let only_b = cat.load_table(&b, &tbl).await.unwrap();
        assert_eq!(only_b.current_snapshot, SnapshotId::GENESIS);

        let list_a = cat.list_tables(&a).await.unwrap();
        let list_b = cat.list_tables(&b).await.unwrap();
        assert_eq!(list_a, vec![tbl.clone()]);
        assert_eq!(list_b, vec![tbl.clone()]);

        // Dropping in A doesn't drop in B.
        cat.drop_table(&a, &tbl).await.unwrap();
        cat.load_table(&b, &tbl).await.unwrap();
    }

    #[tokio::test]
    async fn append_advances_snapshot() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("events").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        let meta = cat
            .append_data_files(
                &t,
                &tbl,
                SnapshotId::GENESIS,
                vec![file("p/0.parquet", 42, 1024)],
            )
            .await
            .unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(1));
        assert_eq!(meta.snapshots.len(), 2);
        let head = meta.current().unwrap();
        assert_eq!(head.summary.operation, SnapshotOperation::Append);
        assert_eq!(head.summary.added_rows, 42);
        assert_eq!(head.summary.added_bytes, 1024);
        assert_eq!(head.parent, Some(SnapshotId::GENESIS));

        let snaps = cat.list_snapshots(&t, &tbl).await.unwrap();
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps.first().unwrap().id, SnapshotId::GENESIS);
        assert_eq!(snaps.last().unwrap().id, SnapshotId(1));
    }

    // ── Transaction snapshot-stable reads: load_table_at_snapshot ──────────
    //
    // The InMemoryCatalog retains the full append-only snapshot chain, so the
    // default `load_table_at_snapshot` trait impl reconstructs any historical
    // point-in-time by rewinding `current_snapshot` and replaying
    // `live_data_files()`. This is the catalog primitive behind the engine's
    // REPEATABLE-READ-ish transaction read-view.
    #[tokio::test]
    async fn load_table_at_snapshot_reconstructs_history() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("events").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Snapshot 1: one file (10 rows).
        cat.append_data_files(&t, &tbl, SnapshotId::GENESIS, vec![file("p/1.parquet", 10, 100)])
            .await
            .unwrap();
        // Snapshot 2: a second file (5 rows).
        cat.append_data_files(&t, &tbl, SnapshotId(1), vec![file("p/2.parquet", 5, 50)])
            .await
            .unwrap();

        // Live head sees both files → 15 rows.
        let head = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(head.current_snapshot, SnapshotId(2));
        let head_rows: u64 = head.live_data_files().iter().map(|f| f.row_count).sum();
        assert_eq!(head_rows, 15);

        // Pinned at snapshot 1: only the first file is live → 10 rows.
        let at1 = cat
            .load_table_at_snapshot(&t, &tbl, SnapshotId(1))
            .await
            .unwrap();
        assert_eq!(at1.current_snapshot, SnapshotId(1));
        let at1_rows: u64 = at1.live_data_files().iter().map(|f| f.row_count).sum();
        assert_eq!(at1_rows, 10, "snapshot-1 view must not see the file added at snapshot 2");

        // Pinned at genesis: no files.
        let at0 = cat
            .load_table_at_snapshot(&t, &tbl, SnapshotId::GENESIS)
            .await
            .unwrap();
        assert!(at0.live_data_files().is_empty(), "genesis view has no files");

        // Asking for current head is identity.
        let at_head = cat
            .load_table_at_snapshot(&t, &tbl, SnapshotId(2))
            .await
            .unwrap();
        assert_eq!(at_head.current_snapshot, SnapshotId(2));

        // A snapshot id that was never minted → FeatureNotSupported (caller
        // degrades to a read-committed current read rather than serving wrong
        // point-in-time).
        let missing = cat
            .load_table_at_snapshot(&t, &tbl, SnapshotId(99))
            .await;
        assert!(
            matches!(missing, Err(BasinError::FeatureNotSupported(_))),
            "unminted snapshot must return FeatureNotSupported, got {missing:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_append_one_wins() {
        let cat = Arc::new(InMemoryCatalog::new());
        let t = ProjectId::new();
        let tbl = TableName::new("race").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Both writers start from the same snapshot. One must win, one must
        // observe a CommitConflict; we don't care which.
        let c1 = cat.clone();
        let c2 = cat.clone();
        let t1 = t;
        let t2 = t;
        let tbl1 = tbl.clone();
        let tbl2 = tbl.clone();
        let h1 = tokio::spawn(async move {
            c1.append_data_files(
                &t1,
                &tbl1,
                SnapshotId::GENESIS,
                vec![file("a.parquet", 1, 10)],
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            c2.append_data_files(
                &t2,
                &tbl2,
                SnapshotId::GENESIS,
                vec![file("b.parquet", 1, 10)],
            )
            .await
        });
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();

        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(BasinError::CommitConflict(_))))
            .count();
        let oks = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        assert_eq!(oks, 1, "exactly one append must win: {r1:?} {r2:?}");
        assert_eq!(
            conflicts, 1,
            "exactly one append must conflict: {r1:?} {r2:?}"
        );

        let head = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(head.current_snapshot, SnapshotId(1));
    }

    #[tokio::test]
    async fn optimistic_retry() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("retry").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // First commit succeeds.
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 1, 10)],
        )
        .await
        .unwrap();

        // A stale appender (still pointing at GENESIS) loses.
        let err = cat
            .append_data_files(
                &t,
                &tbl,
                SnapshotId::GENESIS,
                vec![file("b.parquet", 1, 10)],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::CommitConflict(_)));

        // Reload, retry with the fresh snapshot — succeeds.
        let fresh = cat.load_table(&t, &tbl).await.unwrap();
        let meta = cat
            .append_data_files(
                &t,
                &tbl,
                fresh.current_snapshot,
                vec![file("b.parquet", 1, 10)],
            )
            .await
            .unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(2));
        assert_eq!(meta.snapshots.len(), 3);
    }

    #[tokio::test]
    async fn replace_data_files_advances_snapshot() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("rep").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();

        let meta = cat
            .replace_data_files(
                &t,
                &tbl,
                SnapshotId(1),
                vec!["a.parquet".to_string()],
                vec![file("b.parquet", 5, 60)],
            )
            .await
            .unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(2));
        let head = meta.current().unwrap();
        assert_eq!(head.summary.operation, SnapshotOperation::Replace);
        assert_eq!(head.summary.removed_files, 1);
        assert_eq!(head.summary.added_files, 1);
        assert_eq!(head.summary.added_rows, 5);
        assert_eq!(head.summary.added_bytes, 60);
        assert_eq!(head.parent, Some(SnapshotId(1)));
    }

    #[tokio::test]
    async fn replace_data_files_concurrent_one_wins() {
        let cat = Arc::new(InMemoryCatalog::new());
        let t = ProjectId::new();
        let tbl = TableName::new("repmix").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        // Seed a real file so removed_paths references something concrete.
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("seed.parquet", 1, 10)],
        )
        .await
        .unwrap();

        let c1 = cat.clone();
        let c2 = cat.clone();
        let t1 = t;
        let t2 = t;
        let tbl1 = tbl.clone();
        let tbl2 = tbl.clone();
        // Both racers see snapshot 1 and try to swap.
        let h1 = tokio::spawn(async move {
            c1.replace_data_files(
                &t1,
                &tbl1,
                SnapshotId(1),
                vec!["seed.parquet".to_string()],
                vec![file("a.parquet", 1, 10)],
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            c2.replace_data_files(
                &t2,
                &tbl2,
                SnapshotId(1),
                vec!["seed.parquet".to_string()],
                vec![file("b.parquet", 1, 10)],
            )
            .await
        });
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();

        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(BasinError::CommitConflict(_))))
            .count();
        let oks = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        assert_eq!(oks, 1, "exactly one replace must win: {r1:?} {r2:?}");
        assert_eq!(
            conflicts, 1,
            "exactly one replace must conflict: {r1:?} {r2:?}"
        );

        let head = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(head.current_snapshot, SnapshotId(2));
    }

    /// Phase 6 PITR: rollback truncates history and rewinds the head.
    #[tokio::test]
    async fn rollback_to_earlier_snapshot() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("pitr").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Two appends → snapshots 1 and 2 in addition to GENESIS (0).
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        cat.append_data_files(&t, &tbl, SnapshotId(1), vec![file("b.parquet", 20, 200)])
            .await
            .unwrap();
        let pre = cat.list_snapshots(&t, &tbl).await.unwrap();
        assert_eq!(pre.len(), 3);

        // Rewind to snapshot 1.
        let rolled = cat
            .rollback_to_snapshot(&t, &tbl, SnapshotId(1))
            .await
            .unwrap();
        assert_eq!(rolled.current_snapshot, SnapshotId(1));
        assert_eq!(rolled.snapshots.len(), 2);
        assert!(rolled.snapshots.iter().all(|s| s.id <= SnapshotId(1)));

        // History is truncated, not just hidden.
        let post = cat.list_snapshots(&t, &tbl).await.unwrap();
        assert_eq!(post.len(), 2);
        assert!(post.iter().all(|s| s.id <= SnapshotId(1)));

        // A new commit chains off the rolled-back head.
        let after = cat
            .append_data_files(&t, &tbl, SnapshotId(1), vec![file("c.parquet", 30, 300)])
            .await
            .unwrap();
        assert_eq!(after.current_snapshot, SnapshotId(2));
        let head = after
            .snapshots
            .iter()
            .find(|s| s.id == SnapshotId(2))
            .unwrap();
        assert_eq!(head.parent, Some(SnapshotId(1)));
        assert_eq!(head.data_files.len(), 1);
        assert_eq!(head.data_files[0].path, "c.parquet");
    }

    #[tokio::test]
    async fn rollback_to_genesis_is_supported() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("pitr_genesis").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        let rolled = cat
            .rollback_to_snapshot(&t, &tbl, SnapshotId::GENESIS)
            .await
            .unwrap();
        assert_eq!(rolled.current_snapshot, SnapshotId::GENESIS);
        assert_eq!(rolled.snapshots.len(), 1);
    }

    #[tokio::test]
    async fn rollback_to_unknown_snapshot_is_not_found() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("pitr_404").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        let err = cat
            .rollback_to_snapshot(&t, &tbl, SnapshotId(999))
            .await
            .unwrap_err();
        assert!(
            matches!(err, BasinError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    /// Phase 6 fork: dst table inherits source state; subsequent commits
    /// diverge.
    #[tokio::test]
    async fn fork_table_clones_source_state() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let src = TableName::new("src").unwrap();
        let dst = TableName::new("dst").unwrap();
        cat.create_table(&t, &src, &schema()).await.unwrap();

        cat.append_data_files(
            &t,
            &src,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        cat.append_data_files(&t, &src, SnapshotId(1), vec![file("b.parquet", 20, 200)])
            .await
            .unwrap();

        let forked = cat.fork_table(&t, &src, &dst).await.unwrap();
        assert_eq!(forked.current_snapshot, SnapshotId(2));
        assert_eq!(forked.snapshots.len(), 3);
        // Same data file paths — fork is copy-on-write.
        let src_meta = cat.load_table(&t, &src).await.unwrap();
        let src_paths: Vec<&str> = src_meta
            .snapshots
            .iter()
            .flat_map(|s| s.data_files.iter().map(|f| f.path.as_str()))
            .collect();
        let dst_paths: Vec<&str> = forked
            .snapshots
            .iter()
            .flat_map(|s| s.data_files.iter().map(|f| f.path.as_str()))
            .collect();
        assert_eq!(src_paths, dst_paths);
    }

    #[tokio::test]
    async fn fork_then_commit_diverges_from_source() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let src = TableName::new("orig").unwrap();
        let dst = TableName::new("clone").unwrap();
        cat.create_table(&t, &src, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &src,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();

        cat.fork_table(&t, &src, &dst).await.unwrap();

        // Commit only on dst.
        cat.append_data_files(
            &t,
            &dst,
            SnapshotId(1),
            vec![file("dst-only.parquet", 5, 50)],
        )
        .await
        .unwrap();

        let src_after = cat.load_table(&t, &src).await.unwrap();
        let dst_after = cat.load_table(&t, &dst).await.unwrap();
        assert_eq!(src_after.current_snapshot, SnapshotId(1));
        assert_eq!(dst_after.current_snapshot, SnapshotId(2));
        // Source should not see the dst-only file.
        let src_files: Vec<&str> = src_after
            .snapshots
            .iter()
            .flat_map(|s| s.data_files.iter().map(|f| f.path.as_str()))
            .collect();
        assert!(!src_files.contains(&"dst-only.parquet"));
    }

    /// Phase 5.7 B2: cluster columns round-trip through set + load + fork.
    #[tokio::test]
    async fn set_cluster_columns_round_trip() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("clustered").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Default: no cluster columns.
        let pre = cat.load_table(&t, &tbl).await.unwrap();
        assert!(pre.cluster_columns.is_empty());

        cat.set_cluster_columns(&t, &tbl, vec!["id".into(), "ts".into()])
            .await
            .unwrap();
        let after = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after.cluster_columns, vec!["id".to_string(), "ts".into()]);

        // Fork carries the cluster spec forward.
        let dst = TableName::new("forked").unwrap();
        let forked = cat.fork_table(&t, &tbl, &dst).await.unwrap();
        assert_eq!(forked.cluster_columns, vec!["id".to_string(), "ts".into()]);

        // Clearing leaves the spec empty.
        cat.set_cluster_columns(&t, &tbl, Vec::new()).await.unwrap();
        let cleared = cat.load_table(&t, &tbl).await.unwrap();
        assert!(cleared.cluster_columns.is_empty());
    }

    #[tokio::test]
    async fn set_file_format_round_trip() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("fmt_tbl").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // #161: Vortex is the default for every fresh table.
        let pre = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(pre.file_format, TableFileFormat::Vortex);

        cat.set_file_format(&t, &tbl, TableFileFormat::Vortex)
            .await
            .unwrap();
        let after = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after.file_format, TableFileFormat::Vortex);

        // Switch back is also honoured.
        cat.set_file_format(&t, &tbl, TableFileFormat::Parquet)
            .await
            .unwrap();
        let reverted = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(reverted.file_format, TableFileFormat::Parquet);
    }

    #[tokio::test]
    async fn fork_to_existing_dst_errors() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let src = TableName::new("src2").unwrap();
        let dst = TableName::new("dst2").unwrap();
        cat.create_table(&t, &src, &schema()).await.unwrap();
        cat.create_table(&t, &dst, &schema()).await.unwrap();
        let err = cat.fork_table(&t, &src, &dst).await.unwrap_err();
        assert!(
            matches!(err, BasinError::Catalog(_)),
            "expected Catalog, got {err:?}"
        );
    }

    #[tokio::test]
    async fn fork_from_missing_source_is_not_found() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let src = TableName::new("missing").unwrap();
        let dst = TableName::new("clone").unwrap();
        let err = cat.fork_table(&t, &src, &dst).await.unwrap_err();
        assert!(
            matches!(err, BasinError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn rollback_to_current_is_a_noop() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("pitr_noop").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        let pre = cat.load_table(&t, &tbl).await.unwrap();
        let rolled = cat
            .rollback_to_snapshot(&t, &tbl, SnapshotId(1))
            .await
            .unwrap();
        assert_eq!(rolled.current_snapshot, pre.current_snapshot);
        assert_eq!(rolled.snapshots.len(), pre.snapshots.len());
    }

    /// Phase 5.7 B1: secondary index round-trip through create + load + drop.
    #[tokio::test]
    async fn create_drop_secondary_index() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("idx_tbl").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Default: no indexes.
        let pre = cat.load_table(&t, &tbl).await.unwrap();
        assert!(pre.indexes.is_empty());

        cat.create_index(&t, &tbl, "ix_id", &["id".into()], false)
            .await
            .unwrap();
        let after = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after.indexes.len(), 1);
        assert_eq!(after.indexes[0].name, "ix_id");
        assert_eq!(after.indexes[0].columns, vec!["id".to_string()]);

        // Duplicate name on the same table is rejected.
        let err = cat
            .create_index(&t, &tbl, "ix_id", &["name".into()], false)
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");

        // IF NOT EXISTS swallows the duplicate.
        cat.create_index(&t, &tbl, "ix_id", &["name".into()], true)
            .await
            .unwrap();

        // Unknown column is rejected with InvalidSchema.
        let err = cat
            .create_index(&t, &tbl, "ix_bogus", &["ghost_col".into()], false)
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");

        // Fork carries the index forward.
        let dst = TableName::new("idx_forked").unwrap();
        let forked = cat.fork_table(&t, &tbl, &dst).await.unwrap();
        assert_eq!(forked.indexes.len(), 1);
        assert_eq!(forked.indexes[0].name, "ix_id");

        cat.drop_index(&t, &tbl, "ix_id").await.unwrap();
        let dropped = cat.load_table(&t, &tbl).await.unwrap();
        assert!(dropped.indexes.is_empty());

        // Drop a nonexistent index → NotFound.
        let err = cat.drop_index(&t, &tbl, "ghost").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    /// Phase 5.19.B: `create_index_with_method` persists access_method and
    /// opclass correctly (GIN does not silently degrade to btree).
    #[tokio::test]
    async fn create_index_with_method_gin_persists_opclass() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("gin_test").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // GIN with jsonb_path_ops.
        cat.create_index_with_method(
            &t,
            &tbl,
            "gin_test_id_path_ops",
            &["id".into()],
            false,
            "gin",
            Some("jsonb_path_ops"),
        )
        .await
        .unwrap();

        let after = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after.indexes.len(), 1);
        let idx = &after.indexes[0];
        assert_eq!(idx.name, "gin_test_id_path_ops");
        assert_eq!(idx.access_method, "gin");
        assert_eq!(idx.opclass, Some("jsonb_path_ops".to_string()));

        // GIN with jsonb_ops (explicit default).
        cat.create_index_with_method(
            &t,
            &tbl,
            "gin_test_id_ops",
            &["id".into()],
            false,
            "gin",
            Some("jsonb_ops"),
        )
        .await
        .unwrap();

        let after2 = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after2.indexes.len(), 2);
        let idx2 = &after2.indexes[1];
        assert_eq!(idx2.access_method, "gin");
        assert_eq!(idx2.opclass, Some("jsonb_ops".to_string()));

        // IF NOT EXISTS is respected for create_index_with_method.
        cat.create_index_with_method(
            &t,
            &tbl,
            "gin_test_id_ops",
            &["id".into()],
            true,
            "gin",
            Some("jsonb_ops"),
        )
        .await
        .unwrap(); // must not error
        let after3 = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(after3.indexes.len(), 2, "IF NOT EXISTS must not add a duplicate");
    }

    /// Migration Manager v0.2: project-wide list returns every table's
    /// every snapshot, sorted by committed_at.
    #[tokio::test]
    async fn list_snapshots_project_wide_returns_all_tables() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbls = ["alpha", "beta", "gamma"]
            .iter()
            .map(|n| TableName::new(*n).unwrap())
            .collect::<Vec<_>>();
        for tbl in &tbls {
            cat.create_table(&t, tbl, &schema()).await.unwrap();
        }
        // Two appends per table — interleaved so commit timestamps cross
        // table boundaries and sort order is non-trivial.
        for round in 0..2 {
            for tbl in &tbls {
                let parent = if round == 0 {
                    SnapshotId::GENESIS
                } else {
                    SnapshotId(1)
                };
                cat.append_data_files(
                    &t,
                    tbl,
                    parent,
                    vec![file(&format!("{tbl}/r{round}.parquet"), 1, 10)],
                )
                .await
                .unwrap();
                // Bias commit_at ordering between consecutive commits so the
                // sort is deterministic on platforms with coarse clocks.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        let entries = cat.list_snapshots_project_wide(&t).await.unwrap();
        // Genesis (3) + 2 appends × 3 tables = 9 rows.
        assert_eq!(entries.len(), 9, "got {entries:?}");
        // Sorted ascending by committed_at — strictly non-decreasing.
        for w in entries.windows(2) {
            assert!(w[0].committed_at <= w[1].committed_at);
        }
        // Every table appears, and each appears exactly thrice.
        for tbl in &tbls {
            let n = entries.iter().filter(|e| &e.table == tbl).count();
            assert_eq!(n, 3, "table {tbl} count = {n}");
        }
    }

    /// Diff window between two captured wall times groups per-table.
    #[tokio::test]
    async fn diff_snapshots_window() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let a = TableName::new("a").unwrap();
        let b = TableName::new("b").unwrap();
        cat.create_table(&t, &a, &schema()).await.unwrap();
        cat.create_table(&t, &b, &schema()).await.unwrap();

        // First batch — pre-window.
        cat.append_data_files(&t, &a, SnapshotId::GENESIS, vec![file("a1.parquet", 1, 10)])
            .await
            .unwrap();
        cat.append_data_files(&t, &b, SnapshotId::GENESIS, vec![file("b1.parquet", 1, 10)])
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let t1 = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // In-window appends: 2 on a, 1 on b.
        cat.append_data_files(&t, &a, SnapshotId(1), vec![file("a2.parquet", 1, 10)])
            .await
            .unwrap();
        cat.append_data_files(&t, &a, SnapshotId(2), vec![file("a3.parquet", 1, 10)])
            .await
            .unwrap();
        cat.append_data_files(&t, &b, SnapshotId(1), vec![file("b2.parquet", 1, 10)])
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let t2 = chrono::Utc::now();

        let diff = cat.diff_snapshots(&t, t1, t2).await.unwrap();
        let a_entries = diff.per_table.get(&a).expect("a in diff");
        let b_entries = diff.per_table.get(&b).expect("b in diff");
        assert_eq!(a_entries.len(), 2, "expected 2 a-snapshots: {a_entries:?}");
        assert_eq!(b_entries.len(), 1, "expected 1 b-snapshot: {b_entries:?}");
        // Genesis was pre-window, so neither table is flagged as created.
        assert!(diff.created_in_window.is_empty(), "{diff:?}");
    }

    /// Project-wide rollback rewinds every table to its latest pre-cutoff
    /// snapshot.
    #[tokio::test]
    async fn rollback_to_snapshot_project_wide_rewinds_all() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbls: Vec<TableName> = ["x", "y", "z"]
            .iter()
            .map(|n| TableName::new(*n).unwrap())
            .collect();
        for tbl in &tbls {
            cat.create_table(&t, tbl, &schema()).await.unwrap();
            cat.append_data_files(
                &t,
                tbl,
                SnapshotId::GENESIS,
                vec![file(&format!("{tbl}/first.parquet"), 1, 10)],
            )
            .await
            .unwrap();
        }
        // Snapshot the wall time after the first round of commits.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let cutoff = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Second round of appends — these must be rewound by the rollback.
        for tbl in &tbls {
            cat.append_data_files(
                &t,
                tbl,
                SnapshotId(1),
                vec![file(&format!("{tbl}/second.parquet"), 1, 10)],
            )
            .await
            .unwrap();
        }

        let pairs = cat
            .rollback_to_snapshot_project_wide(&t, cutoff)
            .await
            .unwrap();
        assert_eq!(pairs.len(), 3, "{pairs:?}");
        for (_table, head) in &pairs {
            // Each table's head should be the post-first-append snapshot
            // (id 1), not the post-second-append snapshot (id 2).
            assert_eq!(*head, SnapshotId(1));
        }
        for tbl in &tbls {
            let meta = cat.load_table(&t, tbl).await.unwrap();
            assert_eq!(meta.current_snapshot, SnapshotId(1));
            // Second-round file is gone from history.
            let paths: Vec<&str> = meta
                .snapshots
                .iter()
                .flat_map(|s| s.data_files.iter().map(|f| f.path.as_str()))
                .collect();
            assert!(
                !paths.iter().any(|p| p.contains("second.parquet")),
                "post-cutoff file still present: {paths:?}"
            );
        }
    }

    /// Tables created strictly after `as_of` have no eligible target and
    /// must be left untouched.
    #[tokio::test]
    async fn rollback_to_snapshot_project_wide_skips_tables_created_after() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let early = TableName::new("early").unwrap();
        cat.create_table(&t, &early, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &early,
            SnapshotId::GENESIS,
            vec![file("early.parquet", 1, 10)],
        )
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let cutoff = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Created strictly after the cutoff.
        let late = TableName::new("late").unwrap();
        cat.create_table(&t, &late, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &late,
            SnapshotId::GENESIS,
            vec![file("late.parquet", 1, 10)],
        )
        .await
        .unwrap();

        let pairs = cat
            .rollback_to_snapshot_project_wide(&t, cutoff)
            .await
            .unwrap();
        // Only `early` is rolled back; `late` was created after the cutoff
        // and is skipped, not failed.
        assert_eq!(pairs.len(), 1, "{pairs:?}");
        assert_eq!(pairs[0].0, early);

        // `late` is still at its post-create head — no truncation happened.
        let late_meta = cat.load_table(&t, &late).await.unwrap();
        assert_eq!(late_meta.current_snapshot, SnapshotId(1));
        assert_eq!(late_meta.snapshots.len(), 2);
    }

    // ── live_data_files() unit tests (Bug #41) ──────────────────────────────

    /// After inserting two batches and rolling back to the first-batch snapshot,
    /// `live_data_files()` must return only the first batch's files. This is the
    /// catalog-level contract that the engine's read path must honour to fix #41.
    #[tokio::test]
    async fn live_data_files_after_rollback_excludes_post_snapshot_files() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("bug41_live").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        // Batch A: 1 file, 3 rows — mirrors the bug#41 3-row insert.
        // file(path, rows, bytes): rows=3, bytes=10.
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("batch_a.parquet", 3, 10)],
        )
        .await
        .unwrap();
        let target_snap = SnapshotId(1); // post-batch-A snapshot

        // Batch B: 2 more files, 1 row each — mirrors the 2-row second insert.
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId(1),
            vec![
                file("batch_b1.parquet", 1, 5),
                file("batch_b2.parquet", 1, 5),
            ],
        )
        .await
        .unwrap();

        // Before rollback: live set has all three files (5 rows total).
        let before = cat.load_table(&t, &tbl).await.unwrap();
        let mut before_paths: Vec<String> = before
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        before_paths.sort();
        assert_eq!(
            before_paths,
            vec!["batch_a.parquet", "batch_b1.parquet", "batch_b2.parquet"],
            "pre-rollback live set should include both batches",
        );
        let rows_before: u64 = before.live_data_files().iter().map(|f| f.row_count).sum();
        assert_eq!(rows_before, 5, "pre-rollback: 3+1+1 = 5 rows");

        // Rollback to batch-A snapshot.
        let rolled = cat
            .rollback_to_snapshot(&t, &tbl, target_snap)
            .await
            .unwrap();

        // After rollback: live set must contain ONLY batch_a.parquet.
        let mut post_paths: Vec<String> = rolled
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        post_paths.sort();
        assert_eq!(
            post_paths,
            vec!["batch_a.parquet"],
            "post-rollback live set must exclude batch-B files (bug #41)",
        );

        // Row count via live_data_files should be 3 (batch A only).
        let row_count: u64 = rolled.live_data_files().iter().map(|f| f.row_count).sum();
        assert_eq!(
            row_count, 3,
            "post-rollback row count via catalog must be 3"
        );
    }

    /// Genesis snapshot → live_data_files returns empty.
    #[tokio::test]
    async fn live_data_files_at_genesis_is_empty() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("bug41_genesis").unwrap();
        let meta = cat.create_table(&t, &tbl, &schema()).await.unwrap();
        assert!(
            meta.live_data_files().is_empty(),
            "genesis live set must be empty",
        );
    }

    /// Append two files → live_data_files returns both.
    #[tokio::test]
    async fn live_data_files_accumulates_across_appends() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("bug41_append").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("f1.parquet", 1, 10)],
        )
        .await
        .unwrap();
        let meta = cat
            .append_data_files(&t, &tbl, SnapshotId(1), vec![file("f2.parquet", 2, 20)])
            .await
            .unwrap();
        let mut paths: Vec<String> = meta.live_data_files().into_iter().map(|f| f.path).collect();
        paths.sort();
        assert_eq!(paths, vec!["f1.parquet", "f2.parquet"]);
    }

    /// Replace removes the specified files and adds the replacements.
    #[tokio::test]
    async fn live_data_files_replace_removes_old_adds_new() {
        let cat = InMemoryCatalog::new();
        let t = ProjectId::new();
        let tbl = TableName::new("bug41_replace").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("old1.parquet", 1, 10), file("old2.parquet", 1, 10)],
        )
        .await
        .unwrap();

        // Replace old1 with new1, keep old2.
        let meta = cat
            .replace_data_files(
                &t,
                &tbl,
                SnapshotId(1),
                vec!["old1.parquet".to_string()],
                vec![file("new1.parquet", 2, 20)],
            )
            .await
            .unwrap();

        let mut paths: Vec<String> = meta.live_data_files().into_iter().map(|f| f.path).collect();
        paths.sort();
        // old1 removed, new1 added, old2 retained.
        assert_eq!(paths, vec!["new1.parquet", "old2.parquet"]);
    }

    // -----------------------------------------------------------------------
    // Phase A.2 (#118): multi-schema isolation tests
    // -----------------------------------------------------------------------

    fn qtable(schema: &str, table: &str) -> QualifiedTableName {
        QualifiedTableName::new(
            SchemaName::new(schema).unwrap(),
            TableName::new(table).unwrap(),
        )
    }

    #[tokio::test]
    async fn schema_qualified_create_and_load() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let qt = qtable("analytics", "events");
        let meta = cat
            .create_table_qualified(&p, &qt, Arc::new(schema()))
            .await
            .unwrap();
        assert_eq!(meta.table, qt.name);

        let loaded = cat.load_table_qualified(&p, &qt).await.unwrap();
        assert_eq!(loaded.table, qt.name);
    }

    #[tokio::test]
    async fn same_table_name_different_schemas_do_not_collide() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let pub_qt = QualifiedTableName::in_public(TableName::new("orders").unwrap());
        let priv_qt = qtable("private", "orders");

        cat.create_table_qualified(&p, &pub_qt, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &priv_qt, Arc::new(schema()))
            .await
            .unwrap();

        // They are independent entries.
        let pub_meta = cat.load_table_qualified(&p, &pub_qt).await.unwrap();
        let priv_meta = cat.load_table_qualified(&p, &priv_qt).await.unwrap();
        assert_eq!(pub_meta.table, pub_qt.name);
        assert_eq!(priv_meta.table, priv_qt.name);

        // Append to public does not affect private.
        cat.append_data_files_qualified(
            &p,
            &pub_qt,
            SnapshotId::GENESIS,
            vec![file("data.parquet", 1, 10)],
        )
        .await
        .unwrap();
        let priv_reloaded = cat.load_table_qualified(&p, &priv_qt).await.unwrap();
        assert_eq!(priv_reloaded.current_snapshot, SnapshotId::GENESIS);
    }

    #[tokio::test]
    async fn create_duplicate_qualified_table_returns_catalog_error() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let qt = qtable("reporting", "summary");

        cat.create_table_qualified(&p, &qt, Arc::new(schema()))
            .await
            .unwrap();
        let err = cat
            .create_table_qualified(&p, &qt, Arc::new(schema()))
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn drop_table_qualified_removes_only_target_schema() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();

        let pub_qt = QualifiedTableName::in_public(TableName::new("logs").unwrap());
        let other_qt = qtable("other", "logs");

        cat.create_table_qualified(&p, &pub_qt, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &other_qt, Arc::new(schema()))
            .await
            .unwrap();

        cat.drop_table_qualified(&p, &pub_qt).await.unwrap();

        // public.logs gone
        let err = cat.load_table_qualified(&p, &pub_qt).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));

        // other.logs still there
        cat.load_table_qualified(&p, &other_qt).await.unwrap();
    }

    #[tokio::test]
    async fn list_tables_qualified_returns_all_schemas() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();

        let qt1 = qtable("alpha", "t1");
        let qt2 = qtable("beta", "t2");
        let qt3 = QualifiedTableName::in_public(TableName::new("t3").unwrap());

        cat.create_table_qualified(&p, &qt1, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &qt2, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &qt3, Arc::new(schema()))
            .await
            .unwrap();

        let mut all = cat.list_tables_qualified(&p).await.unwrap();
        all.sort();
        assert_eq!(all.len(), 3);
        assert!(all.iter().any(|q| q.schema.as_str() == "alpha"));
        assert!(all.iter().any(|q| q.schema.as_str() == "beta"));
        assert!(all.iter().any(|q| q.schema.as_str() == "public"));
    }

    #[tokio::test]
    async fn list_tables_back_compat_returns_only_public() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();

        let pub_qt = QualifiedTableName::in_public(TableName::new("public_table").unwrap());
        let priv_qt = qtable("private", "private_table");

        cat.create_table_qualified(&p, &pub_qt, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &priv_qt, Arc::new(schema()))
            .await
            .unwrap();

        // Old list_tables returns only public-schema tables.
        let names = cat.list_tables(&p).await.unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str(), "public_table");
    }

    #[tokio::test]
    async fn list_schemas_returns_public_by_default() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let schemas = cat.list_schemas(&p).await.unwrap();
        // Phase 5.18.A: create_namespace now seeds all 8 reserved schemas.
        assert_eq!(
            schemas.len(),
            crate::reserved_schema::ReservedSchema::ALL.len(),
            "expected all reserved schemas to be pre-seeded"
        );
        assert!(
            schemas.iter().any(|s| s.as_str() == "public"),
            "public must be present"
        );
    }

    #[tokio::test]
    async fn create_and_list_schemas() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        cat.create_schema(&p, &SchemaName::new("analytics").unwrap())
            .await
            .unwrap();
        cat.create_schema(&p, &SchemaName::new("staging").unwrap())
            .await
            .unwrap();

        let mut schemas = cat.list_schemas(&p).await.unwrap();
        schemas.sort();
        let names: Vec<&str> = schemas.iter().map(|s| s.as_str()).collect();
        assert!(names.contains(&"analytics"));
        assert!(names.contains(&"staging"));
        assert!(names.contains(&"public"));
    }

    #[tokio::test]
    async fn drop_schema_restrict_fails_when_tables_exist() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();
        cat.create_schema(&p, &SchemaName::new("reporting").unwrap())
            .await
            .unwrap();

        let qt = qtable("reporting", "summary");
        cat.create_table_qualified(&p, &qt, Arc::new(schema()))
            .await
            .unwrap();

        let err = cat
            .drop_schema(&p, &SchemaName::new("reporting").unwrap(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn drop_schema_cascade_removes_tables() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();
        cat.create_schema(&p, &SchemaName::new("staging").unwrap())
            .await
            .unwrap();

        let qt1 = qtable("staging", "t1");
        let qt2 = qtable("staging", "t2");
        cat.create_table_qualified(&p, &qt1, Arc::new(schema()))
            .await
            .unwrap();
        cat.create_table_qualified(&p, &qt2, Arc::new(schema()))
            .await
            .unwrap();

        cat.drop_schema(&p, &SchemaName::new("staging").unwrap(), true)
            .await
            .unwrap();

        // Both tables gone.
        assert!(matches!(
            cat.load_table_qualified(&p, &qt1).await.unwrap_err(),
            BasinError::NotFound(_)
        ));
        assert!(matches!(
            cat.load_table_qualified(&p, &qt2).await.unwrap_err(),
            BasinError::NotFound(_)
        ));

        // Schema itself gone.
        let schemas = cat.list_schemas(&p).await.unwrap();
        assert!(!schemas.iter().any(|s| s.as_str() == "staging"));
    }

    #[tokio::test]
    async fn drop_public_schema_is_rejected() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let err = cat
            .drop_schema(&p, &SchemaName::public(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn drop_nonexistent_schema_returns_not_found() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let err = cat
            .drop_schema(&p, &SchemaName::new("ghost").unwrap(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn rename_table_qualified_across_schemas() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();

        let src = QualifiedTableName::in_public(TableName::new("orders").unwrap());
        let dst = qtable("archive", "old_orders");

        cat.create_table_qualified(&p, &src, Arc::new(schema()))
            .await
            .unwrap();
        cat.rename_table_qualified(&p, &src, &dst).await.unwrap();

        // dst accessible (aliased to same state)
        cat.load_table_qualified(&p, &dst).await.unwrap();
    }

    #[tokio::test]
    async fn fork_table_qualified_cross_schema() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();

        let src = QualifiedTableName::in_public(TableName::new("events").unwrap());
        let dst = qtable("mirror", "events");

        cat.create_table_qualified(&p, &src, Arc::new(schema()))
            .await
            .unwrap();
        cat.append_data_files_qualified(
            &p,
            &src,
            SnapshotId::GENESIS,
            vec![file("e.parquet", 5, 50)],
        )
        .await
        .unwrap();

        let dst_meta = cat.fork_table_qualified(&p, &src, &dst).await.unwrap();
        // Fork inherits source snapshot state.
        assert_eq!(dst_meta.current_snapshot, SnapshotId(1));

        // Subsequent write to src does not affect dst.
        cat.append_data_files_qualified(&p, &src, SnapshotId(1), vec![file("e2.parquet", 3, 30)])
            .await
            .unwrap();
        let dst_reloaded = cat.load_table_qualified(&p, &dst).await.unwrap();
        assert_eq!(dst_reloaded.current_snapshot, SnapshotId(1));
    }

    #[tokio::test]
    async fn old_api_still_works_after_qualified_create() {
        // Regression: old `create_table` / `load_table` / `drop_table` must
        // continue to work against the public schema even after the internal
        // storage migrated to QualifiedTableName keys.
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let tbl = TableName::new("legacy").unwrap();

        cat.create_namespace(&p).await.unwrap();
        cat.create_table(&p, &tbl, &schema()).await.unwrap();
        let meta = cat.load_table(&p, &tbl).await.unwrap();
        assert_eq!(meta.table, tbl);
        cat.append_data_files(
            &p,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("l.parquet", 1, 10)],
        )
        .await
        .unwrap();
        cat.drop_table(&p, &tbl).await.unwrap();
        assert!(matches!(
            cat.load_table(&p, &tbl).await.unwrap_err(),
            BasinError::NotFound(_)
        ));
    }

    // -----------------------------------------------------------------------
    // Phase 6: refcount-aware GC tests
    // -----------------------------------------------------------------------

    /// After a PITR rollback, files written by the discarded snapshots have
    /// refcount 0 and must appear in `gc_orphaned_files` → `orphaned_paths`.
    /// Files referenced by the surviving snapshots must appear in `live_paths`.
    #[tokio::test]
    async fn gc_after_rollback_orphans_discarded_files() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let tbl = TableName::new("gc_pitr").unwrap();
        cat.create_table(&p, &tbl, &schema()).await.unwrap();

        // Snapshot 1: file "a.parquet"
        cat.append_data_files(
            &p,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        // Snapshot 2: file "b.parquet"
        cat.append_data_files(&p, &tbl, SnapshotId(1), vec![file("b.parquet", 20, 200)])
            .await
            .unwrap();
        // Snapshot 3: file "c.parquet"
        cat.append_data_files(&p, &tbl, SnapshotId(2), vec![file("c.parquet", 30, 300)])
            .await
            .unwrap();

        // Rollback to snapshot 1 — snapshots 2 and 3 are pruned from history.
        cat.rollback_to_snapshot(&p, &tbl, SnapshotId(1))
            .await
            .unwrap();

        // GC: "b.parquet" and "c.parquet" are orphans; "a.parquet" is live.
        let report = cat.gc_orphaned_files(&p, &tbl).await.unwrap();
        assert!(
            report.orphaned_paths.contains(&"b.parquet".to_string()),
            "b.parquet should be orphaned: {:?}",
            report.orphaned_paths
        );
        assert!(
            report.orphaned_paths.contains(&"c.parquet".to_string()),
            "c.parquet should be orphaned: {:?}",
            report.orphaned_paths
        );
        assert!(
            report.live_paths.contains(&"a.parquet".to_string()),
            "a.parquet should be live: {:?}",
            report.live_paths
        );
        assert!(
            !report.orphaned_paths.contains(&"a.parquet".to_string()),
            "a.parquet must NOT be orphaned: {:?}",
            report.orphaned_paths
        );
    }

    /// A file referenced by the live snapshot must never appear as an orphan,
    /// even on a freshly-written table.
    #[tokio::test]
    async fn gc_no_orphans_on_live_table() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let tbl = TableName::new("gc_live").unwrap();
        cat.create_table(&p, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &p,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("x.parquet", 5, 50)],
        )
        .await
        .unwrap();

        let report = cat.gc_orphaned_files(&p, &tbl).await.unwrap();
        assert!(
            report.orphaned_paths.is_empty(),
            "no orphans expected on live table: {:?}",
            report.orphaned_paths
        );
        assert!(report.live_paths.contains(&"x.parquet".to_string()));
    }

    /// Forked table shares files with source.  Dropping the source table from
    /// the catalog (simulated by rollback of source to GENESIS so shared files
    /// drop out of source's live set) must NOT orphan files the fork still
    /// references.
    #[tokio::test]
    async fn gc_fork_protects_shared_files() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let src = TableName::new("gc_src").unwrap();
        let dst = TableName::new("gc_dst").unwrap();
        cat.create_table(&p, &src, &schema()).await.unwrap();

        // Two appends into source.
        cat.append_data_files(
            &p,
            &src,
            SnapshotId::GENESIS,
            vec![file("shared_a.parquet", 10, 100)],
        )
        .await
        .unwrap();
        cat.append_data_files(
            &p,
            &src,
            SnapshotId(1),
            vec![file("shared_b.parquet", 20, 200)],
        )
        .await
        .unwrap();

        // Fork: dst inherits the same shared_a and shared_b.
        cat.fork_table(&p, &src, &dst).await.unwrap();

        // Rollback source all the way to GENESIS — shared files drop from
        // source's live set.
        cat.rollback_to_snapshot(&p, &src, SnapshotId::GENESIS)
            .await
            .unwrap();

        // GC on source: shared_a and shared_b should NOT be orphaned because
        // the fork (dst) still references them in its live set.
        let report = cat.gc_orphaned_files(&p, &src).await.unwrap();
        assert!(
            !report.orphaned_paths.contains(&"shared_a.parquet".to_string()),
            "shared_a.parquet must not be orphaned while fork exists: {:?}",
            report.orphaned_paths
        );
        assert!(
            !report.orphaned_paths.contains(&"shared_b.parquet".to_string()),
            "shared_b.parquet must not be orphaned while fork exists: {:?}",
            report.orphaned_paths
        );
    }

    /// Cross-project fork safety: files in project B's table are not visible
    /// to project A's GC sweep — the refcount only counts references within
    /// the same project.
    #[tokio::test]
    async fn gc_cross_project_isolation() {
        let cat = InMemoryCatalog::new();
        let proj_a = ProjectId::new();
        let proj_b = ProjectId::new();
        let tbl_a = TableName::new("gc_iso_a").unwrap();
        let tbl_b = TableName::new("gc_iso_b").unwrap();

        cat.create_table(&proj_a, &tbl_a, &schema()).await.unwrap();
        cat.create_table(&proj_b, &tbl_b, &schema()).await.unwrap();

        // Both projects write a file with the same logical path name.
        cat.append_data_files(
            &proj_a,
            &tbl_a,
            SnapshotId::GENESIS,
            vec![file("shared_name.parquet", 10, 100)],
        )
        .await
        .unwrap();
        cat.append_data_files(
            &proj_b,
            &tbl_b,
            SnapshotId::GENESIS,
            vec![file("shared_name.parquet", 10, 100)],
        )
        .await
        .unwrap();

        // Rollback project A to GENESIS — file becomes orphaned for proj_a.
        cat.rollback_to_snapshot(&proj_a, &tbl_a, SnapshotId::GENESIS)
            .await
            .unwrap();

        // GC for project A must see the path as orphaned — even though
        // project B has a live reference to the same path name.  Projects
        // are isolated; cross-project liveness is not a concern.
        let report_a = cat.gc_orphaned_files(&proj_a, &tbl_a).await.unwrap();
        assert!(
            report_a
                .orphaned_paths
                .contains(&"shared_name.parquet".to_string()),
            "project A: path should be orphaned after rollback: {:?}",
            report_a.orphaned_paths
        );

        // GC for project B must still see the path as live.
        let report_b = cat.gc_orphaned_files(&proj_b, &tbl_b).await.unwrap();
        assert!(
            report_b
                .live_paths
                .contains(&"shared_name.parquet".to_string()),
            "project B: path should still be live: {:?}",
            report_b.live_paths
        );
    }

    /// `file_refcount` returns 0 for a path that is not in any live snapshot,
    /// and ≥ 1 for a live path.  A path shared between two tables (source and
    /// fork) has refcount 2 — once per table.
    #[tokio::test]
    async fn file_refcount_after_rollback_and_fork() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let src = TableName::new("rc_src").unwrap();
        let dst = TableName::new("rc_dst").unwrap();
        cat.create_table(&p, &src, &schema()).await.unwrap();

        cat.append_data_files(
            &p,
            &src,
            SnapshotId::GENESIS,
            vec![file("rc_shared.parquet", 10, 100)],
        )
        .await
        .unwrap();
        cat.append_data_files(
            &p,
            &src,
            SnapshotId(1),
            vec![file("rc_only_src.parquet", 5, 50)],
        )
        .await
        .unwrap();

        // Fork: dst sees both files.
        cat.fork_table(&p, &src, &dst).await.unwrap();

        // rc_shared is in both src and dst live sets → refcount 2.
        let rc = cat.file_refcount(&p, "rc_shared.parquet").await.unwrap();
        assert_eq!(rc, 2, "shared file should have refcount 2 (src + dst)");

        // rc_only_src is also in both live sets → 2.
        let rc2 = cat
            .file_refcount(&p, "rc_only_src.parquet")
            .await
            .unwrap();
        assert_eq!(rc2, 2, "src-only file shared via fork → refcount 2");

        // Rollback source to GENESIS — rc_only_src drops from src's live set
        // but dst still has it.
        cat.rollback_to_snapshot(&p, &src, SnapshotId::GENESIS)
            .await
            .unwrap();

        // rc_only_src now only in dst → refcount 1.
        let rc3 = cat
            .file_refcount(&p, "rc_only_src.parquet")
            .await
            .unwrap();
        assert_eq!(
            rc3, 1,
            "after rollback, rc_only_src only in dst → refcount 1"
        );

        // Drop the fork table entirely — rc_only_src now has refcount 0.
        cat.drop_table(&p, &dst).await.unwrap();
        let rc4 = cat
            .file_refcount(&p, "rc_only_src.parquet")
            .await
            .unwrap();
        assert_eq!(rc4, 0, "after fork drop, rc_only_src has refcount 0");
    }

    /// `create_namespace` must bump the catalog epoch so that any session
    /// cache built before the project was created is invalidated on the next
    /// epoch check.
    #[tokio::test]
    async fn epoch_bumps_on_create_namespace() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        let epoch_before = cat.epoch();
        cat.create_namespace(&p).await.unwrap();
        assert!(
            cat.epoch() > epoch_before,
            "epoch must increase after create_namespace (was {epoch_before}, now {})",
            cat.epoch()
        );
    }

    /// `drop_namespace` wipes all per-project catalog state. Any session that
    /// cached table metadata before the drop must detect the staleness via the
    /// epoch counter rather than serving stale schema.
    #[tokio::test]
    async fn epoch_bumps_on_drop_namespace() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();
        let tbl = TableName::new("t").unwrap();
        cat.create_table(&p, &tbl, &schema()).await.unwrap();
        let epoch_before = cat.epoch();
        cat.drop_namespace(&p).await.unwrap();
        assert!(
            cat.epoch() > epoch_before,
            "epoch must increase after drop_namespace (was {epoch_before}, now {})",
            cat.epoch()
        );
    }
}
