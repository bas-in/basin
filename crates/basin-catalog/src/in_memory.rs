//! Volatile in-process catalog for tests and the Phase 1 integration.
//!
//! **Not durable.** Process restart loses everything. The whole point of
//! Lakekeeper / a REST catalog is durability; this implementation exists so
//! we can unit-test the wiring without standing up a real catalog.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use basin_common::{BasinError, Result, TableName, TenantId};
use chrono::Utc;
use tokio::sync::Mutex;
use tracing::instrument;

use basin_common::ChangeOp;

use crate::domains::{self, DomainDef, DomainError};
use crate::enums::{self, EnumError, EnumTypeDef};
use crate::functions::SqlFunctionDef;
use crate::metadata::{
    CheckConstraint, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy, SecondaryIndex,
    TableMetadata, UniqueConstraint,
};
use crate::procedures::{self, ProcedureError, SqlProcedureDef};
use crate::reactors::{self, ReactorDef, ReactorError};
use crate::sequences::{advance_one, SequenceDef, SequenceError, SequenceState};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::tenant_storage_config::TenantStorageConfig;
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
    home_region: Option<String>,
    indexes: Vec<SecondaryIndex>,
    pk_columns: Vec<String>,
    check_constraints: Vec<CheckConstraint>,
    foreign_keys: Vec<ForeignKeyDef>,
    unique_constraints: Vec<UniqueConstraint>,
}

impl TableState {
    fn genesis(schema: Arc<Schema>) -> Self {
        let now = Utc::now();
        let genesis = Snapshot {
            id: SnapshotId::GENESIS,
            parent: None,
            committed_at: now,
            data_files: Vec::new(),
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
            home_region: None,
            indexes: Vec::new(),
            pk_columns: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
        }
    }
}

type TableMap = HashMap<(TenantId, TableName), Arc<Mutex<TableState>>>;

/// In-memory implementation of [`Catalog`]. Cheap to clone via `Arc`.
///
/// Concurrency model:
/// * `tables` is a `tokio::sync::Mutex<HashMap<...>>`. We hold this lock only
///   long enough to look up (or insert) the per-table `Arc<Mutex<TableState>>`
///   then drop it before doing any real work.
/// * Commits and reads on a single `(tenant, table)` serialize on that table's
///   own mutex. Two tenants — or two different tables in the same tenant —
///   never block each other.
///
/// We use `tokio::sync::Mutex` (not `std::sync::Mutex`) because async code
/// holds the per-table guard across `.await` points and a poisoned `std`
/// mutex would force every method to handle poisoning.
pub struct InMemoryCatalog {
    tables: Mutex<TableMap>,
    namespaces: Mutex<HashSet<TenantId>>,
    /// Per-tenant registered SQL functions. One shared `HashMap` keyed by
    /// `(TenantId, name)` so per-tenant cost stays `O(bytes)` — no
    /// per-tenant heavy resource.
    sql_functions: Mutex<HashMap<(TenantId, String), SqlFunctionDef>>,
    /// Per-tenant registered sequences. Same shape rule as `sql_functions`:
    /// one shared `HashMap` keyed by `(TenantId, name)`. Each value is a
    /// pair of (immutable definition, per-sequence Mutex around the
    /// counter state). The outer mutex is held only long enough to look
    /// up (or insert) the per-sequence handle; concurrent `nextval` calls
    /// on different sequences never block each other.
    sequences: Mutex<HashMap<(TenantId, String), Arc<SequenceEntry>>>,
    /// Per-tenant registered reactors. Same shape rule as
    /// `sql_functions`: one shared `HashMap` keyed by `(TenantId,
    /// composite)` where `composite = "<table>:<reactor_name>"` —
    /// reactor names are unique per `(tenant, table)`, not per tenant.
    /// Per-tenant cost stays `O(bytes)` with no per-tenant heavy
    /// resource. Each value carries a monotonic `seq` so
    /// `lookup_reactors_for` can replay reactors in registration order.
    reactors: Mutex<ReactorState>,
    /// Per-tenant `CREATE TYPE … AS ENUM` declarations. Same shape
    /// rule as `sql_functions`.
    enum_types: Mutex<HashMap<(TenantId, String), EnumTypeDef>>,
    /// Per-tenant `CREATE DOMAIN` declarations.
    domains: Mutex<HashMap<(TenantId, String), DomainDef>>,
    /// Per-tenant `CREATE PROCEDURE … LANGUAGE sql` declarations.
    /// Same shape rule as `sql_functions`: one shared `HashMap` keyed
    /// by `(TenantId, name)` so per-tenant cost stays `O(bytes)` with
    /// no per-tenant heavy resource.
    procedures: Mutex<HashMap<(TenantId, String), SqlProcedureDef>>,
    /// Per-tenant storage config (KMS routing + provider extras).
    /// Single shared `HashMap` keyed by `TenantId`; lazy entry creation;
    /// per-tenant cost stays `O(bytes)` with no per-tenant heavy
    /// resource. Cleared in `drop_namespace`.
    tenant_storage_config: Mutex<HashMap<TenantId, TenantStorageConfig>>,
}

/// Aggregate reactor state. The seq counter assigns each newly-
/// registered reactor a strictly-increasing index; `lookup_reactors_for`
/// sorts matching reactors by it so callers see PG-shaped registration
/// order.
#[derive(Default)]
struct ReactorState {
    /// Composite key = `(tenant, "<table>:<name>")`.
    map: HashMap<(TenantId, String), ReactorEntry>,
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
            tables: Mutex::new(HashMap::new()),
            namespaces: Mutex::new(HashSet::new()),
            sql_functions: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
            reactors: Mutex::new(ReactorState::default()),
            enum_types: Mutex::new(HashMap::new()),
            domains: Mutex::new(HashMap::new()),
            procedures: Mutex::new(HashMap::new()),
            tenant_storage_config: Mutex::new(HashMap::new()),
        }
    }

    async fn get_table(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Arc<Mutex<TableState>>> {
        let key = (*tenant, table.clone());
        let guard = self.tables.lock().await;
        guard
            .get(&key)
            .cloned()
            .ok_or_else(|| BasinError::not_found(format!("{tenant}/{table}")))
    }

    fn build_metadata(tenant: &TenantId, table: &TableName, state: &TableState) -> TableMetadata {
        TableMetadata {
            tenant: *tenant,
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
            home_region: state.home_region.clone(),
            indexes: state.indexes.clone(),
            pk_columns: state.pk_columns.clone(),
            check_constraints: state.check_constraints.clone(),
            foreign_keys: state.foreign_keys.clone(),
            unique_constraints: state.unique_constraints.clone(),
        }
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Catalog for InMemoryCatalog {
    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn create_namespace(&self, tenant: &TenantId) -> Result<()> {
        self.namespaces.lock().await.insert(*tenant);
        Ok(())
    }

    #[instrument(skip(self, schema), fields(tenant = %tenant, table = %table))]
    async fn create_table(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: &Schema,
    ) -> Result<TableMetadata> {
        // Auto-create the namespace on first table; the dedicated
        // `create_namespace` call is for callers that want the explicit step.
        self.namespaces.lock().await.insert(*tenant);

        let key = (*tenant, table.clone());
        let mut tables = self.tables.lock().await;
        if tables.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "table {tenant}/{table} already exists"
            )));
        }
        let state = TableState::genesis(Arc::new(schema.clone()));
        let meta = Self::build_metadata(tenant, table, &state);
        tables.insert(key, Arc::new(Mutex::new(state)));
        Ok(meta)
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn load_table(&self, tenant: &TenantId, table: &TableName) -> Result<TableMetadata> {
        let state = self.get_table(tenant, table).await?;
        let guard = state.lock().await;
        Ok(Self::build_metadata(tenant, table, &guard))
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn drop_table(&self, tenant: &TenantId, table: &TableName) -> Result<()> {
        let key = (*tenant, table.clone());
        let mut tables = self.tables.lock().await;
        tables
            .remove(&key)
            .ok_or_else(|| BasinError::not_found(format!("{tenant}/{table}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, old = %old, new = %new))]
    async fn rename_table(
        &self,
        tenant: &TenantId,
        old: &TableName,
        new: &TableName,
    ) -> Result<()> {
        let old_key = (*tenant, old.clone());
        let new_key = (*tenant, new.clone());
        let mut tables = self.tables.lock().await;
        if tables.contains_key(&new_key) {
            return Err(BasinError::catalog(format!(
                "rename_table: target {tenant}/{new} already exists"
            )));
        }
        let entry = tables
            .remove(&old_key)
            .ok_or_else(|| BasinError::not_found(format!("{tenant}/{old}")))?;
        tables.insert(new_key, entry);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_tables(&self, tenant: &TenantId) -> Result<Vec<TableName>> {
        let tables = self.tables.lock().await;
        let mut out: Vec<TableName> = tables
            .keys()
            .filter(|(t, _)| t == tenant)
            .map(|(_, name)| name.clone())
            .collect();
        out.sort();
        Ok(out)
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn drop_namespace(&self, tenant: &TenantId) -> Result<()> {
        // Single-pass: hold the table-map mutex once, drop every entry whose
        // tenant matches. Cheaper than the default-impl loop (N small awaits)
        // and atomic w.r.t. concurrent list_tables on the same in-memory map.
        let mut tables = self.tables.lock().await;
        tables.retain(|(t, _), _| t != tenant);
        let mut namespaces = self.namespaces.lock().await;
        namespaces.remove(tenant);
        let mut funcs = self.sql_functions.lock().await;
        funcs.retain(|(t, _), _| t != tenant);
        let mut seqs = self.sequences.lock().await;
        seqs.retain(|(t, _), _| t != tenant);
        let mut reactors = self.reactors.lock().await;
        reactors.map.retain(|(t, _), _| t != tenant);
        let mut enums = self.enum_types.lock().await;
        enums.retain(|(t, _), _| t != tenant);
        let mut doms = self.domains.lock().await;
        doms.retain(|(t, _), _| t != tenant);
        let mut procs = self.procedures.lock().await;
        procs.retain(|(t, _), _| t != tenant);
        let mut storage_cfg = self.tenant_storage_config.lock().await;
        storage_cfg.remove(tenant);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_tenant_data_files(&self, tenant: &TenantId) -> Result<Vec<DataFileRef>> {
        // Walk every table's full snapshot history under one map-lock-free
        // pass: clone the per-table Arc<Mutex> handles up front, then drain
        // each snapshot list while holding only that table's mutex. Avoids
        // re-acquiring the top-level map mutex per table the way the default
        // impl (list_tables → load_table loop) would.
        let handles: Vec<Arc<Mutex<TableState>>> = {
            let tables = self.tables.lock().await;
            tables
                .iter()
                .filter(|((t, _), _)| t == tenant)
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
            tenant = %tenant,
            table = %table,
            expected_snapshot = %expected_snapshot,
            file_count = files.len(),
        ),
    )]
    async fn append_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;

        if state.current != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{tenant}/{table}: expected snapshot {expected_snapshot}, current is {}",
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
        Ok(Self::build_metadata(tenant, table, &state))
    }

    #[instrument(
        skip(self, removed_paths, added_files),
        fields(
            tenant = %tenant,
            table = %table,
            expected_snapshot = %expected_snapshot,
            removed = removed_paths.len(),
            added = added_files.len(),
        ),
    )]
    async fn replace_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;

        if state.current != expected_snapshot {
            return Err(BasinError::CommitConflict(format!(
                "{tenant}/{table}: expected snapshot {expected_snapshot}, current is {}",
                state.current
            )));
        }

        let parent = state.current;
        let added_files_count = added_files.len() as u64;
        let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
        let removed_files_count = removed_paths.len() as u64;

        // Per-snapshot data_files mirrors the Append convention: the snapshot
        // records the *delta* (the new files written by this commit). The
        // catalog isn't asked to materialise the full file set; the engine
        // physically deletes the removed files from the object store as part
        // of the same commit so subsequent listings see the new state.
        let new_id = parent.next();
        let snap = Snapshot {
            id: new_id,
            parent: Some(parent),
            committed_at: Utc::now(),
            data_files: added_files,
            summary: SnapshotSummary {
                operation: SnapshotOperation::Replace,
                added_files: added_files_count,
                added_rows,
                added_bytes,
                removed_files: removed_files_count,
            },
        };
        // Suppress the "unused" warning for `removed_paths` — its value is
        // recorded in the summary count above. The path list itself is the
        // engine's responsibility to act on (file deletion); the catalog
        // snapshot history records *that* the swap happened, not which old
        // bytes were thrown away.
        let _ = removed_paths;
        state.snapshots.push(snap);
        state.current = new_id;
        Ok(Self::build_metadata(tenant, table, &state))
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn list_snapshots(&self, tenant: &TenantId, table: &TableName) -> Result<Vec<Snapshot>> {
        let state = self.get_table(tenant, table).await?;
        let guard = state.lock().await;
        Ok(guard.snapshots.clone())
    }

    #[instrument(skip(self), fields(tenant = %tenant, src = %src_table, dst = %dst_table))]
    async fn fork_table(
        &self,
        tenant: &TenantId,
        src_table: &TableName,
        dst_table: &TableName,
    ) -> Result<TableMetadata> {
        // Read source state then drop the per-table guard before grabbing
        // the table-map guard for the insert.
        let cloned_state = {
            let src_arc = self.get_table(tenant, src_table).await?;
            let src = src_arc.lock().await;
            TableState {
                schema: src.schema.clone(),
                current: src.current,
                snapshots: src.snapshots.clone(),
                partition_spec: src.partition_spec.clone(),
                rls_enabled: src.rls_enabled,
                policies: src.policies.clone(),
                cold_after_seconds: src.cold_after_seconds,
                cold_age_column: src.cold_age_column.clone(),
                bloom_filter_columns: src.bloom_filter_columns.clone(),
                row_group_rows: src.row_group_rows,
                continuous_aggregate: src.continuous_aggregate.clone(),
                cluster_columns: src.cluster_columns.clone(),
                home_region: src.home_region.clone(),
                indexes: src.indexes.clone(),
                pk_columns: src.pk_columns.clone(),
                check_constraints: src.check_constraints.clone(),
                foreign_keys: src.foreign_keys.clone(),
                unique_constraints: src.unique_constraints.clone(),
            }
        };

        let dst_key = (*tenant, dst_table.clone());
        let mut tables = self.tables.lock().await;
        if tables.contains_key(&dst_key) {
            return Err(BasinError::catalog(format!(
                "fork_table: {tenant}/{dst_table} already exists",
            )));
        }
        let dst_arc = Arc::new(Mutex::new(cloned_state));
        tables.insert(dst_key, dst_arc.clone());
        drop(tables);

        let dst = dst_arc.lock().await;
        Ok(Self::build_metadata(tenant, dst_table, &dst))
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, snapshot = %snapshot_id))]
    async fn rollback_to_snapshot(
        &self,
        tenant: &TenantId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;

        if !state.snapshots.iter().any(|s| s.id == snapshot_id) {
            return Err(BasinError::not_found(format!(
                "{tenant}/{table}: snapshot {snapshot_id} not in history",
            )));
        }

        // Destructive truncate: drop every snapshot strictly newer than
        // the target. v0.2 will add a non-destructive branch variant.
        state.snapshots.retain(|s| s.id <= snapshot_id);
        state.current = snapshot_id;

        Ok(Self::build_metadata(tenant, table, &state))
    }

    #[instrument(skip(self, spec), fields(tenant = %tenant, table = %table))]
    async fn set_partition_spec(
        &self,
        tenant: &TenantId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.partition_spec = spec;
        Ok(())
    }

    #[instrument(skip(self, policies), fields(tenant = %tenant, table = %table))]
    async fn set_rls_state(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.rls_enabled = rls_enabled;
        state.policies = policies;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_tier_policy(
        &self,
        tenant: &TenantId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.cold_after_seconds = cold_after_seconds;
        state.cold_age_column = cold_age_column;
        Ok(())
    }

    #[instrument(skip(self, columns), fields(tenant = %tenant, table = %table, n = columns.len()))]
    async fn set_bloom_filter_columns(
        &self,
        tenant: &TenantId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.bloom_filter_columns = columns;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_row_group_rows(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.row_group_rows = rows;
        Ok(())
    }

    #[instrument(skip(self, schema), fields(tenant = %tenant, table = %table))]
    async fn set_schema(&self, tenant: &TenantId, table: &TableName, schema: Schema) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.schema = Arc::new(schema);
        Ok(())
    }

    #[instrument(skip(self, def), fields(tenant = %tenant, table = %table))]
    async fn set_continuous_aggregate(
        &self,
        tenant: &TenantId,
        table: &TableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.continuous_aggregate = def;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_cluster_columns(
        &self,
        tenant: &TenantId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.cluster_columns = columns;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_home_region(
        &self,
        tenant: &TenantId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.home_region = region;
        Ok(())
    }

    #[instrument(skip(self, check_constraints, foreign_keys), fields(tenant = %tenant, table = %table))]
    async fn set_table_constraints(
        &self,
        tenant: &TenantId,
        table: &TableName,
        pk_columns: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.pk_columns = pk_columns;
        state.check_constraints = check_constraints;
        state.foreign_keys = foreign_keys;
        Ok(())
    }

    #[instrument(skip(self, unique_constraints), fields(tenant = %tenant, table = %table))]
    async fn set_unique_constraints(
        &self,
        tenant: &TenantId,
        table: &TableName,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        state.unique_constraints = unique_constraints;
        Ok(())
    }

    #[instrument(skip(self, columns), fields(tenant = %tenant, table = %table, name = %name))]
    async fn create_index(
        &self,
        tenant: &TenantId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        if columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "create_index: column list cannot be empty".into(),
            ));
        }
        // Reject indexes on unknown columns up front: a lookup against a
        // missing column would silently always return empty results.
        for col in columns {
            if state.schema.field_with_name(col).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "create_index: column {col:?} not in table {tenant}/{table} schema"
                )));
            }
        }
        if state.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(BasinError::catalog(format!(
                "create_index: {tenant}/{table}: index {name:?} already exists"
            )));
        }
        state.indexes.push(SecondaryIndex {
            name: name.to_string(),
            columns: columns.to_vec(),
        });
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, name = %name))]
    async fn drop_index(&self, tenant: &TenantId, table: &TableName, name: &str) -> Result<()> {
        let state_arc = self.get_table(tenant, table).await?;
        let mut state = state_arc.lock().await;
        let before = state.indexes.len();
        state.indexes.retain(|i| i.name != name);
        if state.indexes.len() == before {
            return Err(BasinError::not_found(format!(
                "{tenant}/{table}: index {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_sql_function(&self, def: SqlFunctionDef) -> Result<()> {
        let key = (def.tenant, def.name.clone());
        let mut map = self.sql_functions.lock().await;
        map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_sql_function(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let key = (*tenant, name.to_string());
        let mut map = self.sql_functions.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}: sql function {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_sql_function(&self, tenant: &TenantId, name: &str) -> Option<SqlFunctionDef> {
        let key = (*tenant, name.to_string());
        let map = self.sql_functions.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_sql_functions(&self, tenant: &TenantId) -> Vec<SqlFunctionDef> {
        let map = self.sql_functions.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn create_sequence(&self, def: SequenceDef) -> Result<()> {
        if def.increment == 0 {
            return Err(BasinError::InvalidSchema(
                "sequence increment must be non-zero".into(),
            ));
        }
        let key = (def.tenant, def.name.clone());
        let mut map = self.sequences.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "sequence {}/{} already exists",
                def.tenant, def.name,
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

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_sequence(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let key = (*tenant, name.to_string());
        let mut map = self.sequences.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_sequence(&self, tenant: &TenantId, name: &str) -> Option<SequenceDef> {
        let key = (*tenant, name.to_string());
        let map = self.sequences.lock().await;
        map.get(&key).map(|e| e.def.clone())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn nextval(&self, tenant: &TenantId, name: &str) -> Result<i64> {
        let entry = {
            let key = (*tenant, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{tenant}: sequence {name:?}")))?
        };
        // Per-sequence mutex serialises increments; concurrent callers
        // see distinct values, but two sequences (or two tenants) never
        // block each other beyond the top-level HashMap probe above.
        let mut state = entry.state.lock().await;
        match advance_one(&entry.def, &mut state) {
            Ok(v) => Ok(v),
            Err(SequenceError::Exhausted) => Err(BasinError::catalog(format!(
                "{tenant}: sequence {name:?} exhausted"
            ))),
            Err(SequenceError::InvalidIncrement) => Err(BasinError::InvalidSchema(format!(
                "{tenant}: sequence {name:?} has zero increment"
            ))),
        }
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn currval(&self, tenant: &TenantId, name: &str) -> Result<i64> {
        let entry = {
            let key = (*tenant, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{tenant}: sequence {name:?}")))?
        };
        let state = entry.state.lock().await;
        if !state.started {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?} has not been advanced"
            )));
        }
        Ok(state.current.load(std::sync::atomic::Ordering::Relaxed))
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name, value = value, advance = advance))]
    async fn setval(
        &self,
        tenant: &TenantId,
        name: &str,
        value: i64,
        advance: bool,
    ) -> Result<i64> {
        let entry = {
            let key = (*tenant, name.to_string());
            let map = self.sequences.lock().await;
            map.get(&key)
                .cloned()
                .ok_or_else(|| BasinError::not_found(format!("{tenant}: sequence {name:?}")))?
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

    #[instrument(skip(self, def), fields(tenant = %def.tenant, table = %def.table, name = %def.name))]
    async fn register_reactor(&self, def: ReactorDef) -> Result<()> {
        if def.ops.is_empty() {
            return Err(BasinError::InvalidSchema(
                "reactor ops bitset is empty".into(),
            ));
        }
        reactors::validate_body(&def.body).map_err(reactor_err_to_basin)?;
        if let Some(p) = &def.when_predicate {
            reactors::validate_predicate(p).map_err(reactor_err_to_basin)?;
        }

        let key = (def.tenant, format!("{}:{}", def.table, def.name));
        let mut state = self.reactors.lock().await;
        if state.map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "reactor {:?} on {}/{} already exists",
                def.name, def.tenant, def.table
            )));
        }
        state.next_seq += 1;
        let seq = state.next_seq;
        state.map.insert(key, ReactorEntry { def, seq });
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, name = %name))]
    async fn drop_reactor(&self, tenant: &TenantId, table: &TableName, name: &str) -> Result<()> {
        let key = (*tenant, format!("{table}:{name}"));
        let mut state = self.reactors.lock().await;
        if state.map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}/{table}: reactor {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, op = ?op))]
    async fn lookup_reactors_for(
        &self,
        tenant: &TenantId,
        table: &TableName,
        op: ChangeOp,
    ) -> Vec<ReactorDef> {
        let state = self.reactors.lock().await;
        let mut hits: Vec<&ReactorEntry> = state
            .map
            .iter()
            .filter(|((t, _), entry)| {
                t == tenant && &entry.def.table == table && entry.def.ops.matches(op)
            })
            .map(|(_, entry)| entry)
            .collect();
        hits.sort_by_key(|e| e.seq);
        hits.into_iter().map(|e| e.def.clone()).collect()
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_reactors(&self, tenant: &TenantId) -> Vec<ReactorDef> {
        let state = self.reactors.lock().await;
        let mut hits: Vec<&ReactorEntry> = state
            .map
            .iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, entry)| entry)
            .collect();
        hits.sort_by_key(|e| e.seq);
        hits.into_iter().map(|e| e.def.clone()).collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_enum_type(&self, def: EnumTypeDef) -> Result<()> {
        enums::validate_new(&def).map_err(enum_err_to_basin)?;
        let key = (def.tenant, def.name.clone());
        let mut enums_map = self.enum_types.lock().await;
        if enums_map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "enum type {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        // Cross-namespace collision: a domain with the same name on the
        // same tenant is rejected so column resolution stays
        // unambiguous.
        let doms = self.domains.lock().await;
        if doms.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "type {}/{} collides with an existing domain",
                def.tenant, def.name,
            )));
        }
        drop(doms);
        enums_map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_enum_type(&self, tenant: &TenantId, name: &str) -> Option<EnumTypeDef> {
        let key = (*tenant, name.to_string());
        let map = self.enum_types.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name, value = %value))]
    async fn add_enum_value(&self, tenant: &TenantId, name: &str, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(BasinError::InvalidSchema(
                "ALTER TYPE ADD VALUE: label cannot be empty".into(),
            ));
        }
        let key = (*tenant, name.to_string());
        let mut map = self.enum_types.lock().await;
        let def = map
            .get_mut(&key)
            .ok_or_else(|| BasinError::not_found(format!("{tenant}: enum type {name:?}")))?;
        if def.labels.iter().any(|l| l == value) {
            return Err(BasinError::catalog(format!(
                "enum {name:?} already contains value {value:?}"
            )));
        }
        def.labels.push(value.to_string());
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_enum_type(&self, tenant: &TenantId, name: &str) -> Result<()> {
        // Refcount: scan every table the tenant owns and reject the
        // drop when any column carries `BASIN_ENUM_TYPE=<name>`. v0.1
        // has no CASCADE; the message tells the caller to drop the
        // columns first.
        let referencing = self.tables_referencing_type(tenant, name, true).await;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop enum {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let key = (*tenant, name.to_string());
        let mut map = self.enum_types.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}: enum type {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_enum_types(&self, tenant: &TenantId) -> Vec<EnumTypeDef> {
        let map = self.enum_types.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_domain(&self, def: DomainDef) -> Result<()> {
        domains::validate_new(&def).map_err(domain_err_to_basin)?;
        let key = (def.tenant, def.name.clone());
        let mut doms = self.domains.lock().await;
        if doms.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "domain {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        let enums_map = self.enum_types.lock().await;
        if enums_map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "domain {}/{} collides with an existing enum type",
                def.tenant, def.name,
            )));
        }
        drop(enums_map);
        doms.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_domain(&self, tenant: &TenantId, name: &str) -> Option<DomainDef> {
        let key = (*tenant, name.to_string());
        let map = self.domains.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_domain(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let referencing = self.tables_referencing_type(tenant, name, false).await;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop domain {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let key = (*tenant, name.to_string());
        let mut map = self.domains.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!("{tenant}: domain {name:?}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_domains(&self, tenant: &TenantId) -> Vec<DomainDef> {
        let map = self.domains.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_procedure(&self, def: SqlProcedureDef) -> Result<()> {
        procedures::validate_new(&def).map_err(procedure_err_to_basin)?;
        let key = (def.tenant, def.name.clone());
        let mut map = self.procedures.lock().await;
        if map.contains_key(&key) {
            return Err(BasinError::catalog(format!(
                "procedure {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        map.insert(key, def);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_procedure(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let key = (*tenant, name.to_string());
        let mut map = self.procedures.lock().await;
        if map.remove(&key).is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}: procedure {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_procedure(&self, tenant: &TenantId, name: &str) -> Option<SqlProcedureDef> {
        let key = (*tenant, name.to_string());
        let map = self.procedures.lock().await;
        map.get(&key).cloned()
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_procedures(&self, tenant: &TenantId) -> Vec<SqlProcedureDef> {
        let map = self.procedures.lock().await;
        map.iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, def)| def.clone())
            .collect()
    }

    #[instrument(skip(self, config), fields(tenant = %tenant))]
    async fn set_tenant_storage_config(
        &self,
        tenant: &TenantId,
        config: TenantStorageConfig,
    ) -> Result<()> {
        let mut map = self.tenant_storage_config.lock().await;
        map.insert(*tenant, config);
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn get_tenant_storage_config(
        &self,
        tenant: &TenantId,
    ) -> Result<Option<TenantStorageConfig>> {
        let map = self.tenant_storage_config.lock().await;
        Ok(map.get(tenant).cloned())
    }
}

impl InMemoryCatalog {
    /// Walk every table owned by `tenant`, returning `<table>.<column>`
    /// labels for every column whose Arrow `Field` carries the
    /// requested type metadata. `is_enum == true` checks the
    /// `BASIN_ENUM_TYPE` key; `false` checks `BASIN_DOMAIN`. Used by
    /// `drop_enum_type` / `drop_domain` to reject drops that would
    /// orphan a column type.
    async fn tables_referencing_type(
        &self,
        tenant: &TenantId,
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
        let handles: Vec<(TableName, Arc<Mutex<TableState>>)> = {
            let tables = self.tables.lock().await;
            tables
                .iter()
                .filter(|((t, _), _)| t == tenant)
                .map(|((_, name), state)| (name.clone(), state.clone()))
                .collect()
        };
        let mut out = Vec::new();
        for (table, state) in handles {
            let guard = state.lock().await;
            for f in guard.schema.fields() {
                if f.metadata().get(key).map(|s| s.as_str()) == Some(type_name) {
                    out.push(format!("{table}.{}", f.name()));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{BasinError, TableName, TenantId};

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
        }
    }

    #[tokio::test]
    async fn create_load_drop_table() {
        let cat = InMemoryCatalog::new();
        let t = TenantId::new();
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
        assert_eq!(loaded.tenant, t);
        assert_eq!(loaded.table, tbl);
        assert_eq!(loaded.current_snapshot, SnapshotId::GENESIS);

        cat.drop_table(&t, &tbl).await.unwrap();
    }

    #[tokio::test]
    async fn drop_then_load_returns_not_found() {
        let cat = InMemoryCatalog::new();
        let t = TenantId::new();
        let tbl = TableName::new("ghost").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.drop_table(&t, &tbl).await.unwrap();
        let err = cat.load_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");

        let err = cat.drop_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let cat = InMemoryCatalog::new();
        let a = TenantId::new();
        let b = TenantId::new();
        let tbl = TableName::new("orders").unwrap();

        let meta_a = cat.create_table(&a, &tbl, &schema()).await.unwrap();
        let meta_b = cat.create_table(&b, &tbl, &schema()).await.unwrap();
        assert_eq!(meta_a.tenant, a);
        assert_eq!(meta_b.tenant, b);
        assert_ne!(meta_a.tenant, meta_b.tenant);

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
        let t = TenantId::new();
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

    #[tokio::test]
    async fn concurrent_append_one_wins() {
        let cat = Arc::new(InMemoryCatalog::new());
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
    async fn fork_to_existing_dst_errors() {
        let cat = InMemoryCatalog::new();
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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

    /// Migration Manager v0.2: project-wide list returns every table's
    /// every snapshot, sorted by committed_at.
    #[tokio::test]
    async fn list_snapshots_project_wide_returns_all_tables() {
        let cat = InMemoryCatalog::new();
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
}
