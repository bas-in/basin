//! Engine-side evaluation of `GENERATED ALWAYS AS (<expr>) STORED` columns.
//!
//! The expression text lives on the column's Arrow `Field` metadata under
//! [`crate::types::BASIN_GENERATED_AS`]. INSERT and UPDATE both call
//! [`materialise_generated_columns`] after the user-provided values have
//! been written into a `RecordBatch`; we run each expression as a single
//! DataFusion projection over the batch and swap the result column in.
//!
//! The evaluation context is a fresh `SessionContext` per call so we never
//! mutate the caller's session — the UDFs (vector distance, pgcrypto,
//! pg-compat scalars) are re-registered on the local context. User-defined
//! `LANGUAGE sql` functions are inlined into the projection SQL via the
//! same rewriter the SELECT path uses, so a generated column referencing a
//! user function works without the SELECT-surface plumbing being live.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::DataType;
use basin_catalog::Catalog;
use basin_common::{BasinError, Result, TenantId};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use crate::convert::{batch_df_to_ws, batch_ws_to_df};
use crate::types::field_is_generated;

/// For every `GENERATED ALWAYS AS (...) STORED` column on `batch`'s
/// schema, evaluate the expression against the row data and replace the
/// existing column with the computed values. Columns without the marker
/// pass through untouched.
///
/// Multiple generated columns are evaluated in declaration order; a later
/// generated column is allowed to reference an earlier one (PG semantics).
pub(crate) async fn materialise_generated_columns(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| field_is_generated(f).is_some())
    {
        return Ok(batch);
    }

    let mut current = batch;
    let gen_cols: Vec<(usize, String, DataType)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            field_is_generated(f).map(|e| (i, e.to_string(), f.data_type().clone()))
        })
        .collect();

    for (col_idx, expr_text, col_dt) in gen_cols {
        let computed = eval_expression(catalog, tenant, &current, &expr_text, &col_dt).await?;
        current = swap_column(&current, col_idx, computed)?;
    }

    Ok(current)
}

/// Evaluate `expr_text` over every row in `batch`. Returns an `ArrayRef`
/// of the same length as `batch.num_rows()` whose data type matches
/// `expected_dt` (cast if the projection's natural output type is wider
/// or narrower). Exposed `pub(crate)` so the UPDATE expression-RHS path
/// in `dml_mutate` can reuse it without duplicating the DataFusion setup.
pub(crate) async fn eval_expression(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    batch: &RecordBatch,
    expr_text: &str,
    expected_dt: &DataType,
) -> Result<ArrayRef> {
    // Inline any user-defined SQL functions before handing the projection
    // SQL to DataFusion. Built-ins (`||`, `coalesce`, `lower`, `now()` etc.)
    // pass through unchanged.
    let projection_sql = format!("SELECT {expr_text} AS __basin_gen FROM __basin_gen_src");
    let rewritten =
        crate::sql_functions::rewrite_sql_inlining_functions(catalog, tenant, &projection_sql)
            .await?;

    let ctx = SessionContext::new();
    crate::udf::register_distance_udfs(&ctx);
    crate::udf::register_pg_udfs(&ctx);
    crate::udf::register_pg_compat_udfs(&ctx);

    // Cross the arrow-version bridge: the workspace batch becomes a
    // datafusion-arrow batch for the MemTable, and the projection's
    // result rides back across the bridge after collect().
    let df_batch = batch_ws_to_df(batch)?;
    let df_schema = df_batch.schema();
    let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
        .map_err(|e| BasinError::internal(format!("MemTable for generated col: {e}")))?;
    ctx.register_table("__basin_gen_src", Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register temp table: {e}")))?;

    let df = ctx.sql(&rewritten).await.map_err(|e| {
        BasinError::InvalidSchema(format!("generated column expression failed to plan: {e}"))
    })?;
    let df_collected = df.collect().await.map_err(|e| {
        BasinError::InvalidSchema(format!(
            "generated column expression failed to execute: {e}"
        ))
    })?;

    if df_collected.is_empty() {
        return Err(BasinError::internal(
            "generated column expression produced no output batches",
        ));
    }

    // Translate each result batch back into workspace-arrow types so the
    // arrow-select concat helper can stitch them together.
    let mut ws_batches: Vec<RecordBatch> = Vec::with_capacity(df_collected.len());
    for b in &df_collected {
        ws_batches.push(batch_df_to_ws(b)?);
    }
    let result_schema = ws_batches[0].schema();
    let merged_batch = arrow_select::concat::concat_batches(&result_schema, &ws_batches)
        .map_err(|e| BasinError::internal(format!("concat generated col output: {e}")))?;
    let merged = merged_batch.column(0).clone();
    if merged.len() != batch.num_rows() {
        return Err(BasinError::internal(format!(
            "generated column expression returned {} rows, expected {}",
            merged.len(),
            batch.num_rows()
        )));
    }
    if merged.data_type() != expected_dt {
        let casted = arrow::compute::cast(&merged, expected_dt).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "generated column expression result type {:?} cannot be coerced to declared \
                 column type {:?}: {e}",
                merged.data_type(),
                expected_dt
            ))
        })?;
        Ok(casted)
    } else {
        Ok(merged)
    }
}

/// Replace `batch`'s column at `col_idx` with `new_col` and return a new
/// `RecordBatch` (shared columns are `Arc::clone`'d).
fn swap_column(batch: &RecordBatch, col_idx: usize, new_col: ArrayRef) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    cols[col_idx] = new_col;
    RecordBatch::try_new(batch.schema(), cols)
        .map_err(|e| BasinError::internal(format!("rebuild batch with generated col: {e}")))
}

/// Re-evaluate every generated column on `batch` but only overwrite the
/// values at rows where `mask[i]` is true. Used by UPDATE so unmatched
/// rows keep their stored generated value while matched rows get a fresh
/// computation. NULL mask entries are treated as unmatched (same
/// convention as `apply_assignments`).
pub(crate) async fn materialise_generated_columns_masked(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    batch: RecordBatch,
    mask: &BooleanArray,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| field_is_generated(f).is_some())
    {
        return Ok(batch);
    }
    debug_assert_eq!(mask.len(), batch.num_rows());

    let mut current = batch;
    let gen_cols: Vec<(usize, String, DataType)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            field_is_generated(f).map(|e| (i, e.to_string(), f.data_type().clone()))
        })
        .collect();

    for (col_idx, expr_text, col_dt) in gen_cols {
        let computed = eval_expression(catalog, tenant, &current, &expr_text, &col_dt).await?;
        let original = current.column(col_idx).clone();
        let merged = merge_by_mask(&original, &computed, mask)?;
        current = swap_column(&current, col_idx, merged)?;
    }
    Ok(current)
}

/// Build a column that takes its value from `new_col` at matched rows
/// and from `orig` at unmatched / null-mask rows. Both inputs must share
/// the same `DataType` and length.
fn merge_by_mask(orig: &ArrayRef, new_col: &ArrayRef, mask: &BooleanArray) -> Result<ArrayRef> {
    if orig.len() != new_col.len() || orig.len() != mask.len() {
        return Err(BasinError::internal(format!(
            "merge_by_mask length mismatch: orig={}, new={}, mask={}",
            orig.len(),
            new_col.len(),
            mask.len()
        )));
    }
    if orig.data_type() != new_col.data_type() {
        return Err(BasinError::internal(format!(
            "merge_by_mask data-type mismatch: orig={:?}, new={:?}",
            orig.data_type(),
            new_col.data_type()
        )));
    }
    // `arrow_select::zip::zip(mask, a, b)` returns a where mask is true,
    // b otherwise. Treat NULL mask entries as "unmatched" by transforming
    // them into a `false` boolean cell.
    let normalised = sanitize_mask(mask);
    let merged = arrow_select::zip::zip(&normalised, new_col, orig)
        .map_err(|e| BasinError::internal(format!("zip generated col: {e}")))?;
    Ok(merged)
}

/// Replace any null mask entry with `false` so `arrow_select::zip::zip`
/// always picks the original value for null-mask rows.
fn sanitize_mask(mask: &BooleanArray) -> BooleanArray {
    let mut out = arrow_array::builder::BooleanBuilder::with_capacity(mask.len());
    for i in 0..mask.len() {
        if mask.is_null(i) {
            out.append_value(false);
        } else {
            out.append_value(mask.value(i));
        }
    }
    out.finish()
}
