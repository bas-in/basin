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

use crate::metadata::{CvDef, DataFileRef, PartitionSpec, Policy, TableMetadata};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
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
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            namespaces: Mutex::new(HashSet::new()),
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

    fn build_metadata(
        tenant: &TenantId,
        table: &TableName,
        state: &TableState,
    ) -> TableMetadata {
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
    async fn list_snapshots(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>> {
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
    async fn set_schema(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: Schema,
    ) -> Result<()> {
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
        assert_eq!(conflicts, 1, "exactly one append must conflict: {r1:?} {r2:?}");

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
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId(1),
            vec![file("b.parquet", 20, 200)],
        )
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
            .append_data_files(
                &t,
                &tbl,
                SnapshotId(1),
                vec![file("c.parquet", 30, 300)],
            )
            .await
            .unwrap();
        assert_eq!(after.current_snapshot, SnapshotId(2));
        let head = after.snapshots.iter().find(|s| s.id == SnapshotId(2)).unwrap();
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
        cat.append_data_files(
            &t,
            &src,
            SnapshotId(1),
            vec![file("b.parquet", 20, 200)],
        )
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
}
