//! Cold-path tombstone filtering for the non-HTAP DELETE fast path.
//!
//! When the process-wide `MemTableRegistry` holds tombstones for a table but
//! the session has **no** live HTAP rows (i.e. `refresh_table` / the plain
//! cold-only path), this module provides a cleaner `TableProvider` +
//! `ExecutionPlan` pair that:
//!
//! * Drives `Storage::read_paths_with_schema` directly — no `ListingTable`
//!   indirection.
//! * Applies per-row tombstone suppression inline inside `execute()`, using the
//!   same `filter_batch` logic already proven in `hot_tombstone`.
//! * Follows the same structural pattern as `GinRowGroupPrunedTable`,
//!   `RTreePrunedTable`, and `JsonbPostingPrunedTable`.
//!
//! The previous `wrap_with_tombstone_filter` helper wrapped a `ListingTable`
//! with `TombstoneFilteringTable` on EVERY single-PK cold-only registration,
//! even when tombstones were absent (zero-overhead only because of the inner
//! `snapshot_tombstones` call in `scan()`). This module replaces that pattern:
//! the new `TombstoneColdTable` is registered ONLY when tombstones are actually
//! present on a cold-only path, eliminating the wrapper entirely for the common
//! tombstone-free case.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use basin_common::{ProjectId, TableName};
use basin_storage::{ReadOptions, Storage};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

use crate::hot_tombstone::{array_value_to_row_key, filter_batch};

// ── TombstoneColdTable ────────────────────────────────────────────────────────

/// A [`datafusion::catalog::TableProvider`] that reads the cold-tier files via
/// Basin's native storage reader and suppresses any row whose PK matches a
/// snapshotted tombstone.
///
/// Registered by `refresh_table` (and its qualified / with-extra variants) when
/// `snapshot_tombstones` returns a non-empty set AND the session has no live
/// HTAP rows for the table — i.e. the pure cold read path after a fast-path
/// DELETE.
///
/// The tombstone set is snapshotted at registration time (same contract as
/// `TombstoneFilteringTable`: bounded by the memtable flush cycle, typically
/// a few thousand keys at most).
pub(crate) struct TombstoneColdTable {
    schema: SchemaRef,
    storage: Storage,
    project: ProjectId,
    table: TableName,
    paths: Vec<ObjectPath>,
    /// Snapshotted tombstone `RowKey` bytes, keyed by raw `Vec<u8>`.
    tombstones: Arc<HashSet<Vec<u8>>>,
    /// Single PK column name — the fast-path DELETE writer only writes to
    /// single-column-PK tables.
    pk_column: String,
    /// Declared Arrow data type of the PK column (from catalog schema).
    pk_dt: DataType,
}

impl TombstoneColdTable {
    pub(crate) fn new(
        schema: SchemaRef,
        storage: Storage,
        project: ProjectId,
        table: TableName,
        paths: Vec<ObjectPath>,
        tombstones: Arc<HashSet<Vec<u8>>>,
        pk_column: String,
        pk_dt: DataType,
    ) -> Self {
        Self {
            schema,
            storage,
            project,
            table,
            paths,
            tombstones,
            pk_column,
            pk_dt,
        }
    }
}

impl fmt::Debug for TombstoneColdTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TombstoneColdTable")
            .field("table", &self.table)
            .field("n_files", &self.paths.len())
            .field("n_tombstones", &self.tombstones.len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for TombstoneColdTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Resolve the PK column index in the catalog schema.
        let pk_idx_in_schema = self.schema.index_of(&self.pk_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "TombstoneColdTable: PK column '{}' not in schema for table '{}'",
                self.pk_column, self.table
            ))
        })?;

        // If the caller's projection omits the PK column, augment it so the
        // tombstone filter has the key bytes to compare. We then strip the
        // extra column back out with a `ProjectionExec` so the surrounding plan
        // sees the originally-requested schema.
        //
        // Limit pushdown is dropped when augmenting because the limit applies to
        // post-filter rows; passing it to the cold scan would truncate before
        // tombstone removal and under-count survivors.
        let (scan_projection, augmented, effective_limit) = match projection {
            Some(p) if !p.contains(&pk_idx_in_schema) => {
                let mut p2 = p.clone();
                p2.push(pk_idx_in_schema);
                (Some(p2), true, None)
            }
            Some(p) => (Some(p.clone()), false, limit),
            None => (None, false, limit),
        };

        let proj_names: Option<Vec<String>> = scan_projection.as_ref().map(|idxs| {
            idxs.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect()
        });

        let opts = ReadOptions {
            projection: proj_names,
            limit: effective_limit,
            ..ReadOptions::default()
        };

        // Resolve the output schema after (possibly augmented) projection.
        let scan_schema: SchemaRef = match &scan_projection {
            Some(idxs) => Arc::new(
                self.schema
                    .project(idxs)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ),
            None => self.schema.clone(),
        };

        let exec = Arc::new(TombstoneColdScanExec::new(
            scan_schema.clone(),
            self.storage.clone(),
            self.project,
            self.table.clone(),
            self.schema.clone(),
            self.paths.clone(),
            opts,
            Arc::clone(&self.tombstones),
            self.pk_column.clone(),
            self.pk_dt.clone(),
        ));

        if !augmented {
            return Ok(exec);
        }

        // Strip the appended PK column so the outer plan sees the schema it
        // originally asked for. The original projection lives in the first
        // `p.len() - 1` output columns of the exec's schema.
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
        let original_len = scan_projection.as_ref().map(|p| p.len() - 1).unwrap_or(0);
        let exprs: Vec<ProjectionExpr> = (0..original_len)
            .map(|i| {
                let field = scan_schema.field(i);
                ProjectionExpr {
                    expr: Arc::new(Column::new(field.name(), i)),
                    alias: field.name().to_owned(),
                }
            })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, exec)?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use datafusion::logical_expr::TableProviderFilterPushDown as Pd;
        // Tombstone suppression commutes with a row filter (a tombstoned row
        // that matches a predicate is still suppressed). Claim `Inexact` so
        // DataFusion can push predicates into storage I/O for pruning while
        // keeping a `FilterExec` above for authoritative evaluation.
        Ok(vec![Pd::Inexact; filters.len()])
    }
}

// ── TombstoneColdScanExec ─────────────────────────────────────────────────────

/// Leaf [`ExecutionPlan`] that drives `Storage::read_paths_with_schema` and
/// drops every row whose encoded PK matches one of the snapshotted tombstones.
///
/// Produces a single output partition.
#[derive(Debug)]
pub(crate) struct TombstoneColdScanExec {
    output_schema: SchemaRef,
    storage: Storage,
    project: ProjectId,
    table: TableName,
    catalog_schema: SchemaRef,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
    tombstones: Arc<HashSet<Vec<u8>>>,
    pk_column: String,
    pk_dt: DataType,
    props: Arc<PlanProperties>,
}

impl TombstoneColdScanExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        output_schema: SchemaRef,
        storage: Storage,
        project: ProjectId,
        table: TableName,
        catalog_schema: SchemaRef,
        paths: Vec<ObjectPath>,
        opts: ReadOptions,
        tombstones: Arc<HashSet<Vec<u8>>>,
        pk_column: String,
        pk_dt: DataType,
    ) -> Self {
        let eq = EquivalenceProperties::new(output_schema.clone());
        let props = Arc::new(PlanProperties::new(
            eq,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            output_schema,
            storage,
            project,
            table,
            catalog_schema,
            paths,
            opts,
            tombstones,
            pk_column,
            pk_dt,
            props,
        }
    }
}

impl DisplayAs for TombstoneColdScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TombstoneColdScanExec: table={} n_files={} suppressed={}",
            self.table,
            self.paths.len(),
            self.tombstones.len()
        )
    }
}

impl ExecutionPlan for TombstoneColdScanExec {
    fn name(&self) -> &str {
        "TombstoneColdScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let storage = self.storage.clone();
        let project = self.project;
        let paths = self.paths.clone();
        let opts = self.opts.clone();
        let catalog_schema = Some(self.catalog_schema.clone());
        let output_schema = self.output_schema.clone();
        let tombstones = Arc::clone(&self.tombstones);
        let pk_column = self.pk_column.clone();
        let pk_dt = self.pk_dt.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<DFResult<arrow_array::RecordBatch>>(64);
        tokio::spawn(async move {
            let result = storage
                .read_paths_with_schema(&project, paths, opts, catalog_schema)
                .await;
            match result {
                Err(e) => {
                    let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                }
                Ok(mut inner) => {
                    while let Some(item) = inner.next().await {
                        let df_item = item.map_err(|e| DataFusionError::External(Box::new(e)));
                        let filtered = df_item.and_then(|batch| {
                            filter_batch(&batch, &pk_column, &pk_dt, &tombstones)
                        });
                        if tx.send(filtered).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use basin_hottier::RowKey;

    fn make_batch(ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(ids.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn tombstone_set(ids: &[i64]) -> Arc<HashSet<Vec<u8>>> {
        Arc::new(
            ids.iter()
                .map(|v| {
                    RowKey::builder()
                        .append_i64(*v)
                        .finish()
                        .as_bytes()
                        .to_vec()
                })
                .collect(),
        )
    }

    /// `filter_batch` (reused by TombstoneColdScanExec) drops tombstoned rows.
    #[test]
    fn tombstone_cold_scan_filters_deleted_rows() {
        let batch = make_batch(&[1, 2, 3, 4, 5]);
        let tombstones = tombstone_set(&[2, 4]);
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 3);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 3);
        assert_eq!(col.value(2), 5);
    }

    /// Empty tombstone set is a pass-through — this case is normally never
    /// registered (the wiring branch only fires on non-empty tombstone sets)
    /// but the exec handles it gracefully.
    #[test]
    fn tombstone_cold_scan_empty_tombstones_passes_through() {
        let batch = make_batch(&[10, 20, 30]);
        let tombstones: Arc<HashSet<Vec<u8>>> = Arc::new(HashSet::new());
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 3);
    }
}
