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

use crate::metadata::{DataFileRef, TableMetadata};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::Catalog;

/// One table's mutable state. The per-table mutex serializes commits so
/// `append_data_files` is atomic without blocking commits to *other* tables.
struct TableState {
    schema: Arc<Schema>,
    current: SnapshotId,
    snapshots: Vec<Snapshot>,
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
}
