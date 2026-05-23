//! Read-path merge-on-read tombstone suppression for the DELETE hot-tier fast
//! path.
//!
//! When `dml_mutate::exec_delete` takes the `BASIN_HOTTIER_DELETE_FASTPATH`
//! shortcut it writes `MemRowValue::Tombstone` entries into the process-wide
//! `MemTableRegistry` and skips the cold-tier copy-on-write rewrite. The
//! corresponding read paths must therefore consult the registry to drop any
//! cold-tier rows that have been tombstoned, or follow-up SELECTs would return
//! the now-deleted rows.
//!
//! This module provides:
//!
//! * [`snapshot_tombstones`] — gather the tombstone `RowKey` bytes for a
//!   `(project, table)` pair as a fast-lookup `HashSet`. Empty when the
//!   memtable is missing or contains no tombstones.
//! * [`array_value_to_row_key`] — encode a single column value out of an
//!   Arrow `RecordBatch` into the same `RowKey` form `dml_mutate` uses when
//!   writing tombstones. The encoding mirrors `pk_scalar_to_row_key`.
//! * [`TombstoneFilterExec`] — a thin `ExecutionPlan` wrapper around a cold
//!   scan that drops every row whose encoded PK matches one of the
//!   snapshotted tombstones. No-op when the snapshot is empty.
//! * [`maybe_wrap_with_tombstone_filter`] — convenience helper used by
//!   `refresh_table*` and `HtapUnionTable::scan` to apply the filter only
//!   when at least one tombstone exists.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use basin_common::{ProjectId, TableName};
use basin_hottier::{MemRowValue, MemTableRegistry, RowKey};
use datafusion::common::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

// ── Snapshot helper ──────────────────────────────────────────────────────────

/// Gather every `MemRowValue::Tombstone` key currently registered for
/// `(project, table)` in `registry` into a `HashSet<Vec<u8>>` keyed by raw
/// `RowKey` bytes.
///
/// Returns an empty set when the registry has no entry for the table or when
/// every entry is a live row. Cheap when the table is cold or write-only.
///
/// The snapshot is bounded in size: tombstones live in the memtable only
/// between the DELETE and the next compaction/flush, so the typical OLTP
/// working-set fits comfortably in memory (thousands of keys at most before
/// the flush task drains them).
pub(crate) fn snapshot_tombstones(
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
) -> HashSet<Vec<u8>> {
    let Some(entry) = registry.get(project, table) else {
        return HashSet::new();
    };
    let snap = entry.memtable.snapshot();
    let mut out: HashSet<Vec<u8>> = HashSet::new();
    for (key, value) in snap {
        if matches!(value, MemRowValue::Tombstone) {
            out.insert(key.as_bytes().to_vec());
        }
    }
    out
}

// ── Array → RowKey encoder ───────────────────────────────────────────────────

/// Encode the single value at `row_idx` in `array` (whose declared logical
/// type is `col_dt`) into a `RowKey` whose lexicographic byte order matches
/// the cluster-key sort and the `pk_scalar_to_row_key` encoding used by the
/// fast-path DELETE writer.
///
/// Returns `None` when:
/// * the value is NULL (PK columns are NOT NULL — but we degrade gracefully
///   rather than panic so a single bad row never corrupts the merge),
/// * the column's datatype is not in the supported set (`Int16/Int32/Int64
///   /UInt64/Utf8/LargeUtf8/Utf8View/Boolean`), or
/// * the Arrow array's runtime type does not match the declared `col_dt`.
///
/// Mirrors `dml_mutate::pk_scalar_to_row_key`. Any addition to that function
/// must be reflected here to keep tombstone matching correct.
pub(crate) fn array_value_to_row_key(
    array: &dyn Array,
    row_idx: usize,
    col_dt: &DataType,
) -> Option<RowKey> {
    if array.is_null(row_idx) {
        return None;
    }
    let b = RowKey::builder();
    Some(match col_dt {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int64Array>()?;
            b.append_i64(arr.value(row_idx)).finish()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int32Array>()?;
            b.append_i32(arr.value(row_idx)).finish()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int16Array>()?;
            b.append_i16(arr.value(row_idx)).finish()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<arrow_array::UInt64Array>()?;
            b.append_u64(arr.value(row_idx)).finish()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<arrow_array::StringArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<arrow_array::BooleanArray>()?;
            b.append_u8(if arr.value(row_idx) { 1 } else { 0 }).finish()
        }
        _ => return None,
    })
}

// ── ExecutionPlan wrapper ────────────────────────────────────────────────────

/// Drops every row whose encoded PK matches one of `tombstones`.
///
/// When the PK column is not present in the scan's output schema (e.g. a
/// projection that omits it) the filter degrades to a pass-through: there is
/// no safe way to evaluate "is this row tombstoned?" without the key bytes.
/// In practice this only matters for `SELECT non_pk_col FROM t` issued
/// immediately after a fast-path DELETE — a vanishingly rare shape — and the
/// next compaction flush will reconcile correctness regardless.
///
/// Single-column PKs only. Composite PKs trigger an empty `tombstones` set
/// in `snapshot_tombstones` because the fast-path writer rejects them in
/// `try_resolve_fast_path_pks`, so we never construct a multi-column key
/// to compare against.
#[derive(Debug)]
pub(crate) struct TombstoneFilterExec {
    inner: Arc<dyn ExecutionPlan>,
    /// Name of the single PK column the tombstones key on. Read from the
    /// table's catalog metadata at plan time.
    pk_column: String,
    /// Declared Arrow data type of the PK column. Read from the catalog
    /// schema at plan time so we always encode against the catalog's type,
    /// not whatever the scan happens to emit (e.g. Utf8 vs Utf8View).
    pk_dt: DataType,
    /// Snapshot of tombstone `RowKey` bytes at plan time. Bounded by the
    /// memtable's tombstone count, which the flush task keeps small.
    tombstones: Arc<HashSet<Vec<u8>>>,
    /// Cached `PlanProperties` mirroring `inner`'s (the filter is a pure
    /// row-discarding pass with no schema or ordering change).
    props: Arc<PlanProperties>,
}

impl TombstoneFilterExec {
    pub(crate) fn new(
        inner: Arc<dyn ExecutionPlan>,
        pk_column: String,
        pk_dt: DataType,
        tombstones: Arc<HashSet<Vec<u8>>>,
    ) -> Self {
        let props = Arc::new(PlanProperties::new(
            inner.equivalence_properties().clone(),
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        ));
        Self {
            inner,
            pk_column,
            pk_dt,
            tombstones,
            props,
        }
    }
}

impl DisplayAs for TombstoneFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TombstoneFilterExec: pk={} suppressed={}",
            self.pk_column,
            self.tombstones.len()
        )
    }
}

impl ExecutionPlan for TombstoneFilterExec {
    fn name(&self) -> &str {
        "TombstoneFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TombstoneFilterExec::new(
            children.swap_remove(0),
            self.pk_column.clone(),
            self.pk_dt.clone(),
            Arc::clone(&self.tombstones),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = self.inner.schema();
        let pk_column = self.pk_column.clone();
        let pk_dt = self.pk_dt.clone();
        let tombstones = Arc::clone(&self.tombstones);
        let mapped = inner_stream.map(move |batch_res| {
            batch_res.and_then(|batch| filter_batch(&batch, &pk_column, &pk_dt, &tombstones))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

/// Build a `BooleanArray` mask that drops rows whose PK is in `tombstones`.
/// Returns the input batch unchanged when the PK column is absent (we can't
/// safely evaluate the filter — see `TombstoneFilterExec` docs).
fn filter_batch(
    batch: &RecordBatch,
    pk_column: &str,
    pk_dt: &DataType,
    tombstones: &HashSet<Vec<u8>>,
) -> DFResult<RecordBatch> {
    let Ok(pk_idx) = batch.schema().index_of(pk_column) else {
        return Ok(batch.clone());
    };
    let pk_array = batch.column(pk_idx);
    let n = batch.num_rows();
    let mut keep: Vec<bool> = Vec::with_capacity(n);
    for r in 0..n {
        match array_value_to_row_key(pk_array.as_ref(), r, pk_dt) {
            Some(key) => keep.push(!tombstones.contains(key.as_bytes())),
            // Unsupported type / NULL PK / type mismatch — keep the row.
            // The fast-path writer would have rejected the DELETE in this
            // case, so no live tombstone can match. Conservatively retain.
            None => keep.push(true),
        }
    }
    let mask = BooleanArray::from(keep);
    let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(filtered)
}

// ── Batch-level convenience wrapper ──────────────────────────────────────────

/// Apply the merge-on-read tombstone filter to an already-collected vector of
/// `RecordBatch`es and return the surviving rows.
///
/// Used by the point-lookup fast path in `fast_select::execute_simple_select`,
/// which reads cold-tier files directly into `Vec<RecordBatch>` and never
/// constructs an `ExecutionPlan` we could wrap with `TombstoneFilterExec`.
///
/// Callers should only invoke this when `tombstones` is non-empty (a
/// `snapshot_tombstones` call followed by a `.is_empty()` check) so the happy
/// path stays zero-overhead. When `tombstones` is empty this still returns
/// `batches` unchanged but pays one allocation per batch.
///
/// Mirrors the behaviour of `TombstoneFilterExec::execute`: rows whose PK
/// encodes to one of the snapshotted `RowKey` bytes are dropped; batches
/// missing the PK column pass through unchanged (no safe key to compare).
pub(crate) fn apply_tombstone_filter_to_batches(
    batches: Vec<RecordBatch>,
    tombstones: &HashSet<Vec<u8>>,
    pk_column: &str,
    pk_dt: &DataType,
) -> DFResult<Vec<RecordBatch>> {
    if tombstones.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|b| filter_batch(&b, pk_column, pk_dt, tombstones))
        .collect()
}

// ── Convenience wrapper ──────────────────────────────────────────────────────

/// Wrap `inner` with a `TombstoneFilterExec` when the table has at least one
/// tombstone in the process-wide memtable registry. Returns `inner` unchanged
/// otherwise.
///
/// The PK column is looked up from `pk_columns` (single-column PK only); if
/// the table has a composite PK we skip the wrap because the fast-path writer
/// never writes tombstones for composite-PK tables.
pub(crate) fn maybe_wrap_with_tombstone_filter(
    inner: Arc<dyn ExecutionPlan>,
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
    pk_columns: &[String],
    schema: &arrow_schema::Schema,
) -> Arc<dyn ExecutionPlan> {
    if pk_columns.len() != 1 {
        return inner;
    }
    let pk_col = &pk_columns[0];
    let tombs = snapshot_tombstones(registry, project, table);
    if tombs.is_empty() {
        return inner;
    }
    let Ok(pk_idx) = schema.index_of(pk_col) else {
        return inner;
    };
    let pk_dt = schema.field(pk_idx).data_type().clone();
    Arc::new(TombstoneFilterExec::new(
        inner,
        pk_col.clone(),
        pk_dt,
        Arc::new(tombs),
    ))
}

// ── TableProvider wrapper ────────────────────────────────────────────────────

use crate::Engine;

/// A [`datafusion::catalog::TableProvider`] that wraps the cold-tier provider
/// and inserts a [`TombstoneFilterExec`] above its scan whenever the
/// process-wide `MemTableRegistry` has at least one tombstone for the table.
///
/// Used by `session::refresh_table*` so that any SELECT issued AFTER a
/// fast-path DELETE drops the now-tombstoned cold-tier rows. When the
/// registry has no tombstones for the table the scan is a zero-cost
/// pass-through (one `DashMap::get` + one `BTreeMap::iter`).
pub(crate) struct TombstoneFilteringTable {
    cold: Arc<dyn datafusion::catalog::TableProvider>,
    engine: Engine,
    project: ProjectId,
    table: TableName,
    pk_columns: Vec<String>,
    // Cached catalog schema — used to resolve the PK's declared `DataType`,
    // which may differ from the per-batch schema the scan emits (e.g.
    // Utf8 vs Utf8View after the listing-format promotion).
    catalog_schema: SchemaRef,
}

impl TombstoneFilteringTable {
    pub(crate) fn new(
        cold: Arc<dyn datafusion::catalog::TableProvider>,
        engine: Engine,
        project: ProjectId,
        table: TableName,
        pk_columns: Vec<String>,
        catalog_schema: SchemaRef,
    ) -> Self {
        Self {
            cold,
            engine,
            project,
            table,
            pk_columns,
            catalog_schema,
        }
    }
}

impl fmt::Debug for TombstoneFilteringTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TombstoneFilteringTable")
            .field("table", &self.table)
            .field("pk_columns", &self.pk_columns)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for TombstoneFilteringTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.cold.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.cold.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let registry = self.engine.memtable_registry();
        let tombs = snapshot_tombstones(registry.as_ref(), &self.project, &self.table);

        // No tombstones → zero-overhead pass-through. Hand the cold provider
        // the original projection / filters / limit and skip the wrap.
        if tombs.is_empty() || self.pk_columns.len() != 1 {
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            return Ok(cold_plan);
        }

        let pk_col = &self.pk_columns[0];
        let Ok(pk_idx_in_schema) = self.catalog_schema.index_of(pk_col) else {
            // PK column missing from catalog schema (defensive — would be a
            // catalog corruption). Fall back to the pass-through so we never
            // crash a read just because of bad metadata.
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            return Ok(cold_plan);
        };
        let pk_dt = self.catalog_schema.field(pk_idx_in_schema).data_type().clone();

        // If the caller's projection omits the PK column we must add it so the
        // tombstone filter has the key bytes to compare. We then strip the
        // augmented column back out with a `ProjectionExec` so the surrounding
        // plan sees the originally-requested schema. This is the fix for
        // `SELECT COUNT(*)` / `SELECT non_pk_col` after a fast-path DELETE:
        // without the PK in the cold batch the filter has nothing to key on
        // and tombstoned rows leak into aggregates / non-PK projections.
        //
        // Limit pushdown is also dropped when augmenting because the limit
        // applies to post-filter rows; passing a limit to the cold scan would
        // truncate before tombstone removal and under-count survivors.
        let (cold_projection_owned, augmented, effective_limit) = match projection {
            Some(p) if !p.contains(&pk_idx_in_schema) => {
                let mut p2 = p.clone();
                p2.push(pk_idx_in_schema);
                (Some(p2), true, None)
            }
            Some(p) => (Some(p.clone()), false, limit),
            None => (None, false, limit),
        };
        let cold_projection_ref = cold_projection_owned.as_ref();
        let cold_plan = self
            .cold
            .scan(state, cold_projection_ref, filters, effective_limit)
            .await?;

        // Wrap with the row-filter.
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(TombstoneFilterExec::new(
            cold_plan,
            pk_col.clone(),
            pk_dt,
            Arc::new(tombs),
        ));

        if !augmented {
            return Ok(filtered);
        }

        // Strip the appended PK column so the outer plan sees the schema it
        // originally asked for. The original projection lives in the first
        // `p.len()` output columns of `filtered`.
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
        let filtered_schema = filtered.schema();
        let original_len = cold_projection_owned
            .as_ref()
            .map(|p| p.len() - 1)
            .unwrap_or(0);
        let exprs: Vec<ProjectionExpr> = (0..original_len)
            .map(|i| {
                let field = filtered_schema.field(i);
                ProjectionExpr {
                    expr: Arc::new(Column::new(field.name(), i)),
                    alias: field.name().to_owned(),
                }
            })
            .collect();
        let projected = ProjectionExec::try_new(exprs, filtered)?;
        Ok(Arc::new(projected))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        // When tombstones are present we disable predicate pushdown into the
        // cold scan. Otherwise the cold provider may evaluate predicates
        // BEFORE the tombstone filter runs, and a row that satisfies the
        // predicate but is tombstoned would leak through. Returning
        // `Unsupported` keeps the filter ABOVE this provider's output in the
        // plan tree, which runs AFTER our `TombstoneFilterExec`.
        let registry = self.engine.memtable_registry();
        let tombs = snapshot_tombstones(registry.as_ref(), &self.project, &self.table);
        if !tombs.is_empty() {
            return Ok(vec![
                datafusion::logical_expr::TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        self.cold.supports_filters_pushdown(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    #[test]
    fn array_value_to_row_key_int64_matches_pk_writer() {
        // Mirror `dml_mutate::pk_scalar_to_row_key(Int64(7), Int64)`.
        let arr = Int64Array::from(vec![7i64]);
        let key = array_value_to_row_key(&arr, 0, &DataType::Int64).unwrap();
        let expected = RowKey::builder().append_i64(7).finish();
        assert_eq!(key.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn array_value_to_row_key_utf8_matches_pk_writer() {
        let arr = StringArray::from(vec!["hello"]);
        let key = array_value_to_row_key(&arr, 0, &DataType::Utf8).unwrap();
        let expected = RowKey::builder().append_str("hello").finish();
        assert_eq!(key.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn array_value_to_row_key_null_returns_none() {
        let arr = Int64Array::from(vec![None]);
        assert!(array_value_to_row_key(&arr, 0, &DataType::Int64).is_none());
    }

    #[test]
    fn filter_batch_drops_tombstoned_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let tombstones: HashSet<Vec<u8>> = [2i64, 4]
            .iter()
            .map(|v| RowKey::builder().append_i64(*v).finish().as_bytes().to_vec())
            .collect();
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 3);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 3);
        assert_eq!(col.value(2), 5);
    }

    #[test]
    fn apply_tombstone_filter_to_batches_drops_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
        )
        .unwrap();
        let tombstones: HashSet<Vec<u8>> = [2i64, 5]
            .iter()
            .map(|v| RowKey::builder().append_i64(*v).finish().as_bytes().to_vec())
            .collect();
        let out =
            apply_tombstone_filter_to_batches(vec![b1, b2], &tombstones, "id", &DataType::Int64)
                .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn apply_tombstone_filter_to_batches_empty_snapshot_is_passthrough() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .unwrap();
        let empty: HashSet<Vec<u8>> = HashSet::new();
        let out =
            apply_tombstone_filter_to_batches(vec![b.clone()], &empty, "id", &DataType::Int64)
                .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn filter_batch_passes_through_when_pk_column_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let arr = StringArray::from(vec!["a", "b"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let tombstones: HashSet<Vec<u8>> = [1i64]
            .iter()
            .map(|v| RowKey::builder().append_i64(*v).finish().as_bytes().to_vec())
            .collect();
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 2);
    }
}
