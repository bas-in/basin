//! UPDATE / DELETE — copy-on-write DML.
//!
//! Iceberg v2 lets a snapshot record either copy-on-write (CoW) or
//! merge-on-read (MoR) row-level changes. v0.1 ships CoW only:
//!
//! 1. List every Parquet file in `(project, table)` *with stats*.
//! 2. For each file, ask the predicate pruner: can we prove this file
//!    has no matching rows? Can we prove every row matches?
//!      - NoMatch: pass through to the new snapshot's `data_files`
//!        unchanged. No read, no write, no object-store delete.
//!      - AllMatch + DELETE: drop the file from the snapshot. Still
//!        no read; still no replacement file.
//!      - AllMatch + UPDATE: read for SET application, write a
//!        replacement, swap.
//!      - Mixed: read, evaluate per row, partition, write a replacement,
//!        swap.
//! 3. Commit a `Replace` snapshot through the catalog
//!    (`replace_data_files`) so optimistic concurrency races land cleanly.
//! 4. Physically `delete` the *replaced* Parquet files (and any HNSW
//!    sidecars that share their ULID) from the object store. Files we
//!    pruned away never get touched.
//! 5. Refresh the engine's per-session listing-table registration so the
//!    next SELECT in this session sees the swap.
//!
//! Predicate evaluation. WHERE supports `<col> OP <literal>` for
//! `=, <, >, <=, >=` plus `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`,
//! and `IN (...)`. Anything else surfaces as `InvalidSchema` so the user
//! sees a clean error rather than a silent partial-DELETE.
//!
//! SET RHS evaluation. `SET col = <literal>` and `SET col = <bind-param>`
//! hit the fast path that coerces straight into a `ScalarValue` and
//! splats it across every matched row. Anything else — `SET col = col + 1`,
//! `SET col = LOWER(col)`, `SET col = NOW()` — falls back to running the
//! expression as a single DataFusion projection over the (pre-update)
//! batch and merging the result into the matched rows via the mask. The
//! expression is evaluated against the OLD row values (PG semantics), so
//! `SET a = b, b = a` swaps correctly.
//!
//! Out of scope for v0.1, marked TODO inline:
//! - Merge-on-read deletion vectors / position deletes. CoW is a fine
//!   default; MoR is a future optimisation.
//! - Multi-table UPDATE/DELETE, ORDER BY, LIMIT.
//! - Correlated scalar subqueries on the SET RHS
//!   (e.g. `SET x = (SELECT u.v FROM u WHERE u.id = t.id)`). Non-correlated
//!   scalar subqueries are supported since #106.
//! - Subquery / function-call WHERE.

use std::sync::Arc;
use crate::pg_ast::ObjectNamePartExt;
use object_store::ObjectStoreExt;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::DataFileRef;
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName, ProjectId};
use basin_storage::{
    evaluate_compound, evaluate_compound_for_pruning, vector_index_segment_key_for_data_file,
    CompoundPredicate, DataFile, Predicate, PruneOutcome, ScalarValue, Storage,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, ObjectName, SelectItem,
    TableFactor, TableWithJoins, UnaryOperator, Value,
};

// ---------------------------------------------------------------------------
// Subquery-IN resolution
// ---------------------------------------------------------------------------

/// Walk an `Expr` tree and replace any `Expr::InSubquery { ... }` nodes with
/// an equivalent `Expr::InList { ... }` by executing the inner SELECT through
/// the session's DataFusion context and extracting the first column.
///
/// This is the v0.1 strategy for `WHERE col IN (SELECT id FROM t)`:
/// materialise the sub-select first (cheap on small tables), then treat the
/// result as a literal list and feed it through the normal predicate path.
/// The function is recursive so nested AND/OR containing IN-subqueries work
/// too.
pub(crate) async fn resolve_subqueries_in_expr(
    sess: &ProjectSession,
    expr: Expr,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: col_expr,
            subquery,
            negated,
        } => {
            let sql = subquery.to_string();
            let df = sess.ctx.sql(&sql).await.map_err(|e| {
                BasinError::internal(format!("IN (SELECT …) – plan failed: {e}"))
            })?;
            let df_batches = df.collect().await.map_err(|e| {
                BasinError::internal(format!("IN (SELECT …) – execute failed: {e}"))
            })?;
            // Convert from DataFusion's internal arrow version to the workspace
            // arrow version so the column type-check in `arrow_col_value_to_expr`
            // sees the correct trait object.
            let mut list: Vec<Expr> = Vec::new();
            for df_batch in &df_batches {
                if df_batch.num_columns() == 0 {
                    continue;
                }
                let batch = crate::convert::batch_df_to_ws(df_batch)?;
                let col = batch.column(0);
                for i in 0..col.len() {
                    if col.is_null(i) {
                        continue; // NULL IN (…) never matches; skip
                    }
                    let lit = arrow_col_value_to_expr(col.as_ref(), i)?;
                    list.push(lit);
                }
            }
            Ok(Expr::InList {
                expr: col_expr,
                list,
                negated,
            })
        }
        // Scalar subquery in expression position: `(SELECT MAX(id) FROM u)`.
        // Materialise the sub-SELECT, enforce PG's ≤1-row / 1-column
        // constraint, and substitute a plain literal (or NULL) in place of the
        // subquery node so the downstream SET/predicate machinery never sees
        // the subquery at all.
        //
        // Semantics (PostgreSQL-compatible):
        //   - 0 rows  → NULL
        //   - 1 row   → the single cell value as a literal
        //   - >1 rows → error (SQLSTATE 21000)
        //
        // This handles NON-CORRELATED scalar subqueries only — the subquery is
        // executed once against the current session context. Correlated scalar
        // subqueries (`SET v = (SELECT u.v FROM u WHERE u.id = t.id)`) require
        // per-outer-row sub-execution and are deferred as a follow-up.
        Expr::Subquery(subquery) => {
            let sql = subquery.to_string();
            let df = sess.ctx.sql(&sql).await.map_err(|e| {
                BasinError::internal(format!("scalar subquery – plan failed: {e}"))
            })?;
            let df_batches = df.collect().await.map_err(|e| {
                BasinError::internal(format!("scalar subquery – execute failed: {e}"))
            })?;
            // Count total rows across batches to enforce the ≤1-row constraint.
            // Collect rows into at most 2 for the check.
            let mut total_rows = 0usize;
            let mut first_row_batch_idx: Option<usize> = None;
            for (bi, b) in df_batches.iter().enumerate() {
                if b.num_rows() > 0 && first_row_batch_idx.is_none() {
                    first_row_batch_idx = Some(bi);
                }
                total_rows += b.num_rows();
                if total_rows > 1 {
                    return Err(BasinError::InvalidSchema(
                        "more than one row returned by a subquery used as an expression"
                            .to_string(),
                    ));
                }
            }
            if total_rows == 0 {
                // 0 rows → NULL (PG semantics).
                return Ok(Expr::Value(ValueWithSpan {
                    value: Value::Null,
                    span: sqlparser::tokenizer::Span::empty(),
                }));
            }
            // Exactly 1 row — extract the single cell.
            let bi = first_row_batch_idx.unwrap();
            let df_batch = &df_batches[bi];
            if df_batch.num_columns() == 0 {
                return Ok(Expr::Value(ValueWithSpan {
                    value: Value::Null,
                    span: sqlparser::tokenizer::Span::empty(),
                }));
            }
            let batch = crate::convert::batch_df_to_ws(df_batch)?;
            let col = batch.column(0);
            // Find the actual row index inside this batch (skip leading empty
            // batches by scanning for the first batch with rows).
            // Since total_rows==1 and first_row_batch_idx points here, there
            // is exactly one row in this batch — it must be at index 0.
            let row_idx = 0usize;
            if col.is_null(row_idx) {
                return Ok(Expr::Value(ValueWithSpan {
                    value: Value::Null,
                    span: sqlparser::tokenizer::Span::empty(),
                }));
            }
            arrow_col_value_to_expr(col.as_ref(), row_idx)
        }
        // Combinators: recurse into children.
        Expr::BinaryOp { left, op, right } => {
            let left = Box::new(Box::pin(resolve_subqueries_in_expr(sess, *left)).await?);
            let right = Box::new(Box::pin(resolve_subqueries_in_expr(sess, *right)).await?);
            Ok(Expr::BinaryOp { left, op, right })
        }
        Expr::UnaryOp { op, expr: inner } => {
            let inner = Box::new(Box::pin(resolve_subqueries_in_expr(sess, *inner)).await?);
            Ok(Expr::UnaryOp { op, expr: inner })
        }
        Expr::Nested(inner) => {
            let inner = Box::new(Box::pin(resolve_subqueries_in_expr(sess, *inner)).await?);
            Ok(Expr::Nested(inner))
        }
        // All other forms are already representable – pass through.
        other => Ok(other),
    }
}

/// Convert a single cell from an Arrow array column into a sqlparser `Expr`
/// literal. Supports Int16/Int32/Int64, Float32/Float64, Utf8, Boolean.
///
/// Narrow integer types (INT2/INT4) and FLOAT4 are widened to their number
/// literal form so the generated `IN (…)` list is type-agnostic — the
/// predicate evaluator will coerce back to the column's declared width.
/// This is necessary because #66 stores INTEGER columns as Int32Array, so
/// `SELECT id FROM u` for an INTEGER `u.id` returns Int32, not Int64.
fn arrow_col_value_to_expr(col: &dyn arrow_array::Array, i: usize) -> Result<Expr> {
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType as Dt;
    match col.data_type() {
        Dt::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("expected Int64Array"))?
                .value(i);
            if v < 0 {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        Value::Number((-v).to_string(), false).into(),
                    )),
                })
            } else {
                Ok(Expr::Value((Value::Number(v.to_string(), false)).into()))
            }
        }
        // INT4 / INTEGER columns are stored as Int32Array after #66.
        // Widen to i64 for the number literal — the predicate evaluator coerces
        // back to the column's declared width when evaluating IN (…).
        Dt::Int32 => {
            let v = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| BasinError::internal("expected Int32Array"))?
                .value(i) as i64;
            if v < 0 {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        Value::Number((-v).to_string(), false).into(),
                    )),
                })
            } else {
                Ok(Expr::Value((Value::Number(v.to_string(), false)).into()))
            }
        }
        // INT2 / SMALLINT columns are stored as Int16Array after #66.
        Dt::Int16 => {
            let v = col
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| BasinError::internal("expected Int16Array"))?
                .value(i) as i64;
            if v < 0 {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        Value::Number((-v).to_string(), false).into(),
                    )),
                })
            } else {
                Ok(Expr::Value((Value::Number(v.to_string(), false)).into()))
            }
        }
        Dt::Utf8 => {
            let v = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("expected StringArray"))?
                .value(i);
            Ok(Expr::Value((Value::SingleQuotedString(v.to_string())).into()))
        }
        Dt::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BasinError::internal("expected Float64Array"))?
                .value(i);
            if v < 0.0 {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        Value::Number((-v).to_string(), false).into(),
                    )),
                })
            } else {
                Ok(Expr::Value((Value::Number(v.to_string(), false)).into()))
            }
        }
        // FLOAT4 / REAL columns are stored as Float32Array after #66.
        Dt::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| BasinError::internal("expected Float32Array"))?
                .value(i) as f64;
            if v < 0.0 {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        Value::Number((-v).to_string(), false).into(),
                    )),
                })
            } else {
                Ok(Expr::Value((Value::Number(v.to_string(), false)).into()))
            }
        }
        Dt::Boolean => {
            use arrow_array::BooleanArray;
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("expected BooleanArray"))?
                .value(i);
            Ok(Expr::Value((Value::Boolean(v)).into()))
        }
        other => Err(BasinError::InvalidSchema(format!(
            "IN (SELECT …): column type {other:?} cannot be used as IN list element"
        ))),
    }
}

use crate::events::{
    build_row_json, dispatch_post_commit, dispatch_pre_commit, make_event, registry_has_any,
};
use crate::lifecycle::AuditRecord;
use crate::session::refresh_table;
use crate::{ExecResult, ProjectSession};

pub(crate) async fn exec_delete(sess: &ProjectSession, delete: Delete) -> Result<ExecResult> {
    let table = single_table_from_delete(&delete)?;
    // Resolve any IN (SELECT …) subqueries in the WHERE clause to literal
    // lists before parsing. This must happen before `pre_mutation_flush` so
    // the subquery SELECT sees the most-recently-committed rows.
    // DELETE FROM t USING u WHERE … — rewrite to plain DELETE using a
    // subquery that materialises the matching target-table PK values.
    // Do this BEFORE consuming delete.selection in the subquery resolution.
    if let Some(using_tables) = delete.using {
        return exec_delete_using(sess, delete.selection, &table, using_tables).await;
    }

    // Correlated EXISTS / NOT EXISTS: route through DataFusion's optimizer
    // (which decorrelates EXISTS to semi/anti join) to get the matching PK
    // set, then issue a plain DELETE WHERE pk IN (…).
    // This must happen before `resolve_subqueries_in_expr` (which only handles
    // non-correlated IN-subqueries) and before `parse_compound_predicate`
    // (which cannot represent EXISTS at all).
    if let Some(ref sel) = delete.selection {
        if has_exists_subquery(sel) {
            // Refuse multi-table first so the error message is clear.
            if !delete.tables.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "multi-table DELETE not supported".into(),
                ));
            }
            // Pass through any alias on the DELETE target table so DataFusion
            // can resolve alias-qualified column references in the WHERE clause
            // (e.g. `DELETE FROM posts p WHERE EXISTS (... WHERE p.author_id = ...)`).
            let table_alias = delete_table_alias(&delete);
            return exec_delete_via_df_rowset(sess, &table, table_alias.as_deref(), sel).await;
        }
    }

    let resolved_selection: Option<Expr> = match delete.selection {
        None => None,
        Some(e) => Some(resolve_subqueries_in_expr(sess, e).await?),
    };
    let predicate_expr = resolved_selection.as_ref();

    // Refuse the easy-foot-gun multi-table DELETE / DELETE with USING for now
    // — they're not on the v0.1 surface and silently picking one would risk
    // wrong results.
    if !delete.tables.is_empty() {
        return Err(BasinError::InvalidSchema(
            "multi-table DELETE not supported".into(),
        ));
    }
    if delete.using.is_some() {
        return Err(BasinError::InvalidSchema(
            "DELETE ... USING not supported".into(),
        ));
    }
    let returning = delete.returning.clone();

    pre_mutation_flush(sess).await?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();

    // SOFT DELETE rewrite: when the table has a SOFT DELETE column the
    // physical operation is an UPDATE that stamps that column with
    // `now()` and skips already-soft-deleted rows. The audit op (when
    // AUDIT TO is also configured) stays `'delete'` even though the
    // underlying write is an UPDATE — the user's intent matters here,
    // not the storage shape.
    if let Some(sd_col) = crate::types::soft_delete_column(schema.as_ref()) {
        return exec_soft_delete(sess, table, schema, predicate_expr, sd_col, returning).await;
    }

    let storage = sess.engine.config().storage.clone();

    let data_files = storage
        .list_data_files_with_stats(&sess.project, &table)
        .await?;
    if data_files.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
    }

    let pred = match predicate_expr {
        None => None,
        Some(e) => Some(parse_compound_predicate(e, schema.as_ref())?),
    };

    let audit_table = crate::types::audit_table_name(schema.as_ref()).map(|s| s.to_string());
    // RETURNING needs the matched rows materialised as Arrow batches so we
    // can run the projection through DataFusion. capture_events folds it
    // in too: anywhere we'd already be reading the file for sinks/audit we
    // also get RETURNING's input for free.
    let want_returning_rows = returning.is_some();
    let capture_events = sinks_attached(sess) || audit_table.is_some() || want_returning_rows;

    // Walk files: deletes can shortcut on AllMatch (drop the file outright).
    let mut deleted: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut dropped_paths: Vec<String> = Vec::new();
    // Only allocated when sinks are attached. Holds (before_row, after=None)
    // pairs for every actually-deleted row.
    let mut event_payloads: Vec<RowChange> = Vec::new();
    // Deleted-row batches for RETURNING. Allocated only when needed.
    let mut returning_input: Vec<RecordBatch> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(pred.as_ref(), f, schema.as_ref());
        match (outcome, &pred) {
            // No predicate, or AllMatch: every row in this file is matched
            // and deleted. Drop the file outright.
            (PruneOutcome::AllMatch, Some(_)) | (_, None) => {
                deleted += f.row_count as usize;
                dropped_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_dropped_file(
                        &storage,
                        &sess.project,
                        &f.path,
                        &mut event_payloads,
                        want_returning_rows.then_some(&mut returning_input),
                    )
                    .await?;
                }
            }
            (PruneOutcome::NoMatch, Some(_)) => {
                // Pass-through: file appears unchanged in the new snapshot.
            }
            (PruneOutcome::Mixed, Some(p)) => {
                let (kept, deleted_rows, deleted_batch) = if capture_events {
                    evaluate_and_partition_delete_capturing(
                        &storage,
                        &sess.project,
                        &f.path,
                        p,
                        want_returning_rows,
                    )
                    .await?
                } else {
                    let kept =
                        evaluate_and_partition_delete(&storage, &sess.project, &f.path, p).await?;
                    (kept, Vec::new(), None)
                };
                let kept_rows: usize = kept.iter().map(|b| b.num_rows()).sum();
                let removed = (f.row_count as usize).saturating_sub(kept_rows);
                if removed == 0 {
                    // Predicate matched no rows in this file even though
                    // stats said it might — pass through.
                    continue;
                }
                deleted += removed;
                replaced_paths.push(f.path.as_ref().to_string());
                replacement_batches.extend(kept);
                if capture_events {
                    for (b, row) in &deleted_rows {
                        event_payloads.push(RowChange {
                            before: Some(build_row_json(b, *row)?),
                            after: None,
                        });
                    }
                    if let Some(b) = deleted_batch {
                        if b.num_rows() > 0 {
                            returning_input.push(b);
                        }
                    }
                }
            }
        }
    }

    if deleted == 0 {
        return Ok(empty_or_returning(
            "DELETE 0",
            schema.clone(),
            returning.as_deref(),
        ));
    }

    // Parent-side FK enforcement on DELETE. Compute the deleted PK
    // tuple set, then for every child table that references this
    // one: NO ACTION rejects when referring rows exist; CASCADE
    // captures rows for a follow-on DELETE.
    let mut cascades: Vec<crate::constraints::CascadeDelete> = Vec::new();
    if !meta.pk_columns.is_empty() {
        let mut deleted_pks: std::collections::HashSet<Vec<String>> = Default::default();
        for p in &dropped_paths {
            let mut stream = sess
                .engine
                .config()
                .storage
                .read_file(&sess.project, &object_store::path::Path::from(p.as_str()))
                .await?;
            while let Some(rb) = stream.next().await {
                let rb = rb?;
                let idx: Vec<usize> = meta
                    .pk_columns
                    .iter()
                    .map(|c| {
                        rb.schema()
                            .index_of(c)
                            .map_err(|_| BasinError::internal(format!("PK column {c:?} missing")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..rb.num_rows() {
                    if let Some(k) = crate::constraints::pk_tuple_for_row(&rb, &idx, row)? {
                        deleted_pks.insert(k);
                    }
                }
            }
        }
        for p in &replaced_paths {
            let mut original: std::collections::HashSet<Vec<String>> = Default::default();
            let mut stream = sess
                .engine
                .config()
                .storage
                .read_file(&sess.project, &object_store::path::Path::from(p.as_str()))
                .await?;
            while let Some(rb) = stream.next().await {
                let rb = rb?;
                let idx: Vec<usize> = meta
                    .pk_columns
                    .iter()
                    .map(|c| {
                        rb.schema()
                            .index_of(c)
                            .map_err(|_| BasinError::internal(format!("PK column {c:?} missing")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..rb.num_rows() {
                    if let Some(k) = crate::constraints::pk_tuple_for_row(&rb, &idx, row)? {
                        original.insert(k);
                    }
                }
            }
            let kept =
                crate::constraints::pk_tuples_from_batches(&replacement_batches, &meta.pk_columns)?;
            for k in original {
                if !kept.contains(&k) {
                    deleted_pks.insert(k);
                }
            }
        }
        cascades = crate::constraints::check_parent_delete(
            &sess.engine.config().catalog,
            &sess.engine.config().storage,
            &sess.project,
            table.as_str(),
            &deleted_pks,
            &meta.pk_columns,
        )
        .await?;
    }

    let audit_rows: Vec<RowChange> = if audit_table.is_some() {
        event_payloads.iter().cloned().collect()
    } else {
        Vec::new()
    };
    let events = build_events(sess, &table, ChangeOp::Delete, event_payloads);
    let mut removed_paths = replaced_paths.clone();
    removed_paths.extend(dropped_paths.iter().cloned());
    // Pre-commit before writing the replacement file so a rejecting
    // sink leaves no orphan parquet on disk.
    dispatch_pre_commit(&sess.engine, &events).await?;
    let added_files = write_replacement(sess, &table, schema.clone(), replacement_batches).await?;
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        removed_paths.clone(),
        added_files,
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);
    delete_objects(sess, &table, schema.as_ref(), &removed_paths).await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    if let Some(audit) = audit_table.as_ref() {
        write_audit_rows(sess, audit, ChangeOp::Delete, audit_rows).await?;
    }

    // Dispatch CASCADE DELETEs on child tables. Each cascade is a
    // standard recursive DELETE call; same code path → triggers any
    // grand-child cascades automatically.
    for cd in cascades {
        let child_meta = sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, &cd.child_table)
            .await?;
        let where_sql = crate::constraints::build_in_predicate_sql(
            &cd.rows,
            &cd.fk_columns,
            child_meta.schema.as_ref(),
        )?;
        let sql = format!("DELETE FROM {} WHERE {where_sql}", cd.child_table.as_str());
        Box::pin(sess.execute(&sql)).await?;
    }

    // RETURNING: project the collected deleted rows and return them.
    if let Some(items) = returning.as_deref() {
        return project_returning(
            &sess.engine.config().catalog,
            &sess.project,
            schema.clone(),
            returning_input,
            items,
        )
        .await;
    }

    Ok(ExecResult::Empty {
        tag: format!("DELETE {deleted}"),
    })
}

pub(crate) async fn exec_update(
    sess: &ProjectSession,
    table_with_joins: TableWithJoins,
    assignments: Vec<Assignment>,
    from: Option<TableWithJoins>,
    selection: Option<Expr>,
    returning: Option<Vec<sqlparser::ast::SelectItem>>,
) -> Result<ExecResult> {
    if !table_with_joins.joins.is_empty() {
        return Err(BasinError::InvalidSchema(
            "UPDATE with JOIN not supported".into(),
        ));
    }
    // UPDATE t SET col = u.col FROM u WHERE t.id = u.id — handled separately.
    // RETURNING is not yet threaded through exec_update_from; reject early so
    // the caller gets a clear error rather than a silent missing-RETURNING.
    if let Some(from_table) = from {
        if returning.is_some() {
            return Err(BasinError::InvalidSchema(
                "UPDATE … FROM … RETURNING not supported".into(),
            ));
        }
        return exec_update_from(sess, table_with_joins, assignments, from_table, selection).await;
    }
    let table_name = match &table_with_joins.relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if alias.is_some() || args.is_some() {
                return Err(BasinError::InvalidSchema(
                    "UPDATE with table alias or function args not supported".into(),
                ));
            }
            single_part_name(name)?.to_string()
        }
        _ => {
            return Err(BasinError::InvalidSchema(
                "UPDATE target must be a simple table name".into(),
            ));
        }
    };
    let table = TableName::new(table_name.clone())?;

    // Correlated EXISTS / NOT EXISTS in WHERE: route through DataFusion's
    // optimizer (decorrelates to semi/anti join) before the custom
    // CompoundPredicate engine sees the expression.
    // RETURNING is not yet threaded through exec_update_via_df_rowset; reject early.
    if let Some(ref sel) = selection {
        if has_exists_subquery(sel) {
            if returning.is_some() {
                return Err(BasinError::InvalidSchema(
                    "UPDATE … EXISTS … RETURNING not supported in this shape".into(),
                ));
            }
            return exec_update_via_df_rowset(sess, &table_name, &assignments, sel).await;
        }
    }

    // Resolve any IN (SELECT …) subqueries to literal lists before the flush
    // so the subquery SELECT sees the committed state.
    let selection = match selection {
        None => None,
        Some(e) => Some(resolve_subqueries_in_expr(sess, e).await?),
    };

    // Resolve scalar subqueries on the SET RHS before parse_assignments so
    // each `(SELECT …)` becomes a plain literal. This must happen before
    // pre_mutation_flush so the subquery SELECT sees the committed state.
    // Only non-correlated scalar subqueries are resolved here; correlated
    // subqueries (referencing outer table columns) are deferred.
    let assignments = {
        let mut resolved = assignments;
        for a in &mut resolved {
            if contains_subquery(&a.value) {
                a.value = resolve_subqueries_in_expr(sess, a.value.clone()).await?;
            }
        }
        resolved
    };

    pre_mutation_flush(sess).await?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();

    // Resolve assignments to (column_index, AssignmentValue).
    // Literals become Scalar; anything else (column refs, arithmetic,
    // function calls) becomes DFExpr for DataFusion evaluation per-batch.
    // Resolve assignments to (column_index, rhs). Literal/bind hits the
    // fast scalar path; anything else (col references, scalar UDFs,
    // arithmetic, NOW()) falls through to DataFusion expression eval.
    let mut assignments = parse_assignments(&assignments, schema.as_ref())?;
    // AUTO_UPDATE injection: any column flagged on the schema that the
    // user didn't explicitly set gets a fresh `now()` micros value.
    inject_auto_update_assignments(schema.as_ref(), &mut assignments);
    let assignments_have_expressions = assignments
        .iter()
        .any(|(_, rhs)| matches!(rhs, AssignmentRhs::Expr(_)));

    let data_files = storage
        .list_data_files_with_stats(&sess.project, &table)
        .await?;
    if data_files.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "UPDATE 0".into(),
        });
    }

    let pred = match &selection {
        None => None,
        Some(e) => Some(parse_compound_predicate(e, schema.as_ref())?),
    };

    let audit_table = crate::types::audit_table_name(schema.as_ref()).map(|s| s.to_string());
    // Generated columns force the same per-batch traversal as the
    // capture-events path: each batch must be re-evaluated with the
    // matched-row mask in hand so the expression is recomputed exactly
    // for the rows the user's UPDATE just changed.
    let has_generated_cols = schema
        .fields()
        .iter()
        .any(|f| crate::types::field_is_generated(f).is_some());
    // Expression-RHS assignments need the pre-update batch in memory to
    // evaluate the expression against the OLD row values (PG semantics),
    // so they fold into capture_events the same way generated columns do.
    let want_returning_rows = returning.is_some();
    let capture_events = sinks_attached(sess)
        || audit_table.is_some()
        || has_generated_cols
        || assignments_have_expressions
        || want_returning_rows;

    // Walk files. Unlike DELETE, an AllMatch UPDATE still has to read the
    // file to apply SET to every row.
    let mut updated_total: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut event_payloads: Vec<RowChange> = Vec::new();
    // Post-update rows that matched (RETURNING input). Each entry is one
    // filtered batch with only the matched rows.
    let mut returning_input: Vec<RecordBatch> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(pred.as_ref(), f, schema.as_ref());
        match (outcome, &pred) {
            (PruneOutcome::NoMatch, Some(_)) => {
                // Pass-through. No read, no write.
            }
            // AllMatch with predicate, or no predicate at all: every row is
            // matched. We still need the file's contents to apply SET.
            (PruneOutcome::AllMatch, _) | (PruneOutcome::Mixed, None) => {
                let catalog = &sess.engine.config().catalog;
                let (mut new_batches, before_batches, all_true_masks) = if capture_events {
                    let befores = read_file_to_batches(&storage, &sess.project, &f.path).await?;
                    let mut all_masks = Vec::with_capacity(befores.len());
                    for b in &befores {
                        all_masks.push(BooleanArray::from(vec![true; b.num_rows()]));
                    }
                    let news = apply_assignments_all(
                        &sess.engine.config().catalog,
                        &sess.project,
                        &befores,
                        &assignments,
                    )
                    .await?;
                    (news, befores, all_masks)
                } else {
                    let news = read_and_apply_assignments(
                        &sess.engine.config().catalog,
                        &storage,
                        &sess.project,
                        &f.path,
                        None,
                        &assignments,
                    )
                    .await?;
                    (news, Vec::new(), Vec::new())
                };
                if has_generated_cols {
                    let mut rebuilt = Vec::with_capacity(new_batches.len());
                    for b in new_batches {
                        rebuilt.push(
                            crate::generated_cols::materialise_generated_columns(
                                catalog,
                                &sess.project,
                                b,
                            )
                            .await?,
                        );
                    }
                    new_batches = rebuilt;
                }
                updated_total += f.row_count as usize;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(
                        &before_batches,
                        &new_batches,
                        None,
                        &mut event_payloads,
                    )?;
                    if want_returning_rows {
                        // AllMatch: every row matches; the unfiltered
                        // post-update batches feed RETURNING directly.
                        for b in &new_batches {
                            returning_input.push(b.clone());
                        }
                    }
                }
                let _ = all_true_masks; // silence unused when capture_events ran
                replacement_batches.extend(new_batches);
            }
            (PruneOutcome::Mixed, Some(p)) => {
                let catalog = &sess.engine.config().catalog;
                let (rows_matched, mut new_batches, before_batches, mask_per_batch) =
                    if capture_events {
                        let befores = read_file_to_batches(&storage, &sess.project, &f.path).await?;
                        let mut masks = Vec::with_capacity(befores.len());
                        let mut news = Vec::with_capacity(befores.len());
                        let mut matched = 0usize;
                        for b in &befores {
                            let mask = evaluate_compound(b, p).map_err(|e| {
                                BasinError::internal(format!("update predicate eval: {e}"))
                            })?;
                            matched += mask.iter().filter(|x| matches!(x, Some(true))).count();
                            news.push(
                                apply_assignments(catalog, &sess.project, b, &mask, &assignments)
                                    .await?,
                            );
                            masks.push(mask);
                        }
                        (matched, news, befores, masks)
                    } else {
                        let (matched, news) = read_and_apply_assignments_mixed(
                            catalog,
                            &storage,
                            &sess.project,
                            &f.path,
                            p,
                            &assignments,
                        )
                        .await?;
                        (matched, news, Vec::new(), Vec::new())
                    };
                if rows_matched == 0 {
                    // Stats said maybe-match but no rows actually matched —
                    // pass through instead of pointlessly rewriting.
                    continue;
                }
                if has_generated_cols {
                    // We took the capture_events branch above, so masks
                    // are populated 1:1 with new_batches.
                    let mut rebuilt = Vec::with_capacity(new_batches.len());
                    for (b, m) in new_batches.into_iter().zip(mask_per_batch.iter()) {
                        rebuilt.push(
                            crate::generated_cols::materialise_generated_columns_masked(
                                &sess.engine.config().catalog,
                                &sess.project,
                                b,
                                m,
                            )
                            .await?,
                        );
                    }
                    new_batches = rebuilt;
                }
                updated_total += rows_matched;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(
                        &before_batches,
                        &new_batches,
                        Some(&mask_per_batch),
                        &mut event_payloads,
                    )?;
                    if want_returning_rows {
                        // Mixed: filter each post-update batch to keep
                        // only matched rows for RETURNING.
                        for (b, m) in new_batches.iter().zip(mask_per_batch.iter()) {
                            let filtered = arrow_select::filter::filter_record_batch(b, m)
                                .map_err(|e| {
                                    BasinError::internal(format!(
                                        "filter returning batch: {e}"
                                    ))
                                })?;
                            if filtered.num_rows() > 0 {
                                returning_input.push(filtered);
                            }
                        }
                    }
                }
                replacement_batches.extend(new_batches);
            }
            // AllMatch + None handled above; this branch is unreachable in
            // practice but kept for the exhaustive match.
            (PruneOutcome::NoMatch, None) => unreachable!(),
        }
    }

    if updated_total == 0 {
        return Ok(empty_or_returning(
            "UPDATE 0",
            schema.clone(),
            returning.as_deref(),
        ));
    }

    // CHECK / FK / PK enforcement on the post-SET batches.
    if !meta.check_constraints.is_empty() {
        for batch in &replacement_batches {
            crate::constraints::enforce_check_constraints(
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
                batch,
            )
            .await?;
        }
    }
    let assignments_touch_pk = meta.pk_columns.iter().any(|p| {
        assignments
            .iter()
            .any(|(idx, _)| meta.schema.field(*idx).name() == p)
    });
    let assignments_touch_fk = meta.foreign_keys.iter().any(|fk| {
        fk.columns.iter().any(|fc| {
            assignments
                .iter()
                .any(|(idx, _)| meta.schema.field(*idx).name() == fc)
        })
    });
    if assignments_touch_pk && !meta.pk_columns.is_empty() {
        check_update_pk(
            &sess.engine.config().storage,
            &sess.project,
            &table,
            table.as_str(),
            &meta.pk_columns,
            &replacement_batches,
            &replaced_paths,
        )
        .await?;
    }
    if assignments_touch_fk {
        for batch in &replacement_batches {
            crate::constraints::enforce_fk_on_insert(
                &sess.engine.config().catalog,
                &sess.engine.config().storage,
                &sess.project,
                table.as_str(),
                &meta.foreign_keys,
                batch,
            )
            .await?;
        }
    }
    // UNIQUE enforcement runs whenever the assignments touch ANY column
    // in ANY UNIQUE constraint. We deliberately scope to "touches" — an
    // UPDATE that only writes columns no UNIQUE constraint cares about
    // can't introduce a duplicate, so we skip the table scan in the
    // common case.
    let assignments_touch_unique = meta.unique_constraints.iter().any(|u| {
        u.columns.iter().any(|uc| {
            assignments
                .iter()
                .any(|(idx, _)| meta.schema.field(*idx).name() == uc)
        })
    });
    if assignments_touch_unique && !meta.unique_constraints.is_empty() {
        crate::constraints::enforce_unique_on_update(
            &sess.engine.config().storage,
            &sess.project,
            &table,
            table.as_str(),
            &meta.unique_constraints,
            &replacement_batches,
            &replaced_paths,
        )
        .await?;
    }

    // Materialise audit rows from the same captured before/after pairs
    // before they're consumed by the event-builder.
    let audit_rows: Vec<RowChange> = if audit_table.is_some() {
        event_payloads.iter().cloned().collect()
    } else {
        Vec::new()
    };
    let events = build_events(sess, &table, ChangeOp::Update, event_payloads);
    // Pre-commit before writing the replacement file so a rejecting
    // sink leaves no orphan parquet on disk.
    dispatch_pre_commit(&sess.engine, &events).await?;
    let added_files = write_replacement(sess, &table, schema.clone(), replacement_batches).await?;
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        replaced_paths.clone(),
        added_files,
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);
    delete_objects(sess, &table, schema.as_ref(), &replaced_paths).await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    if let Some(audit) = audit_table.as_ref() {
        write_audit_rows(sess, audit, ChangeOp::Update, audit_rows).await?;
    }

    if let Some(items) = returning.as_deref() {
        return project_returning(
            &sess.engine.config().catalog,
            &sess.project,
            schema.clone(),
            returning_input,
            items,
        )
        .await;
    }

    Ok(ExecResult::Empty {
        tag: format!("UPDATE {updated_total}"),
    })
}

// ---------------------------------------------------------------------------
// Join-form DML helpers
// ---------------------------------------------------------------------------

/// `DELETE FROM t USING u [, …] WHERE <condition>`
///
/// Strategy (v0.1, copy-on-write):
/// 1. Load the target table's PK columns (required; error if absent).
/// 2. Build and run a SELECT that returns only the target PK columns from
///    the join: `SELECT t.<pk> FROM t INNER JOIN u ON <condition>`.
/// 3. Collect the PK values as string scalars (the existing IN-predicate
///    path handles only string-literal sets in the compound predicate; the
///    schema's real types are still compared correctly by the Arrow
///    evaluation layer because scalar values carry their types).
/// 4. Re-issue a plain `DELETE FROM t WHERE pk IN (...)` via the recursive
///    `sess.execute` path so cascades, soft-delete, and audit all fire.
///
/// Limitation: the target table must have exactly one PK column in v0.1.
/// Multi-column PKs require a composite IN or repeated OR clauses; we
/// emit the latter.
async fn exec_delete_using(
    sess: &ProjectSession,
    selection: Option<Expr>,
    target: &TableName,
    using_tables: Vec<TableWithJoins>,
) -> Result<ExecResult> {
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, target)
        .await?;
    if meta.pk_columns.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "DELETE … USING requires the target table {:?} to have a PRIMARY KEY in v0.1",
            target.as_str()
        )));
    }
    let pk = &meta.pk_columns;
    let target_str = target.as_str();

    // Build the SELECT that materialises matching PK tuples.
    let pk_proj: Vec<String> = pk
        .iter()
        .map(|c| format!("{target_str}.{c}"))
        .collect();

    // USING tables as a comma-separated FROM list.
    let using_str: Vec<String> = using_tables
        .iter()
        .map(|t| t.relation.to_string())
        .collect();

    let where_sql = match &selection {
        Some(e) => format!(" WHERE {e}"),
        None => String::new(),
    };

    let join_select = format!(
        "SELECT {proj} FROM {target} , {using}{wh}",
        proj = pk_proj.join(", "),
        target = target_str,
        using = using_str.join(", "),
        wh = where_sql,
    );

    // Execute the join SELECT.
    let join_result = Box::pin(sess.execute(&join_select)).await?;
    let batches = match join_result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return Ok(ExecResult::Empty { tag: "DELETE 0".into() }),
    };

    // Collect pk tuples.
    let pk_tuples = collect_pk_batches(&batches, pk)?;
    if pk_tuples.is_empty() {
        return Ok(ExecResult::Empty { tag: "DELETE 0".into() });
    }

    // Build a WHERE clause: for single-pk tables use simple IN; for multi-pk
    // emit (pk1=v1 AND pk2=v2) OR … (matches the cascade helper pattern).
    let schema = meta.schema.clone();
    let where_pred = build_pk_in_predicate(&pk_tuples, pk, &schema)?;

    // Execute the plain DELETE.
    let delete_sql = format!("DELETE FROM {target_str} WHERE {where_pred}");
    Box::pin(sess.execute(&delete_sql)).await
}

/// `UPDATE t SET col = expr FROM u [, …] WHERE <condition>`
///
/// Strategy (v0.1):
/// 1. Require that the target table has exactly one PK column.
/// 2. Determine which SET RHS values come from the USING table vs. are
///    literals. For v0.1 we require all RHS to be either:
///    a. A literal / NULL — pass directly to the single-row UPDATE.
///    b. A compound-identifier `u.col` — project that column from the join.
/// 3. Run `SELECT t.pk, rhs_col1, rhs_col2, … FROM t JOIN u ON <cond>`
/// 4. For each result row emit `UPDATE t SET col1=v1, col2=v2, … WHERE pk=pk`.
async fn exec_update_from(
    sess: &ProjectSession,
    target_twj: TableWithJoins,
    assignments: Vec<Assignment>,
    from_table: TableWithJoins,
    selection: Option<Expr>,
) -> Result<ExecResult> {
    let target_name = match &target_twj.relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if alias.is_some() || args.is_some() {
                return Err(BasinError::InvalidSchema(
                    "UPDATE target must be a bare table name".into(),
                ));
            }
            single_part_name(name)?.to_string()
        }
        _ => {
            return Err(BasinError::InvalidSchema(
                "UPDATE target must be a simple table name".into(),
            ))
        }
    };
    let target = TableName::new(target_name.clone())?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &target)
        .await?;
    if meta.pk_columns.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "UPDATE … FROM requires the target table {:?} to have a PRIMARY KEY in v0.1",
            target_name
        )));
    }
    let pk_col = &meta.pk_columns[0];
    let schema = meta.schema.clone();

    // Collect the SET targets and their RHS expressions.
    let mut set_cols: Vec<String> = Vec::new();
    let mut rhs_exprs: Vec<String> = Vec::new();
    for a in &assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => {
                if name.0.len() == 1 {
                    name.0[0].id_val().clone()
                } else {
                    // schema.column — take the last part
                    name.0.last().map(|i| i.id_val().clone()).unwrap_or_default()
                }
            }
            AssignmentTarget::Tuple(_) => {
                return Err(BasinError::InvalidSchema(
                    "UPDATE FROM: tuple assignment targets not supported in v0.1".into(),
                ))
            }
        };
        set_cols.push(col);
        rhs_exprs.push(a.value.to_string());
    }

    // Build the join SELECT: pk + all RHS expressions.
    let where_sql = match &selection {
        Some(e) => format!(" WHERE {e}"),
        None => String::new(),
    };

    // We project: target.pk, and each RHS as an expression. RHS may be
    // `u.col` (compound) or a literal — both are valid SQL projections.
    let rhs_proj: Vec<String> = rhs_exprs
        .iter()
        .enumerate()
        .map(|(i, e)| format!("{e} AS __rhs_{i}"))
        .collect();

    let proj = std::iter::once(format!("{target_name}.{pk_col}"))
        .chain(rhs_proj)
        .collect::<Vec<_>>()
        .join(", ");

    let from_str = from_table.relation.to_string();

    let join_select = format!(
        "SELECT {proj} FROM {target_name} , {from_str}{wh}",
        wh = where_sql,
    );

    let join_result = Box::pin(sess.execute(&join_select)).await?;
    let batches = match join_result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return Ok(ExecResult::Empty { tag: "UPDATE 0".into() }),
    };

    // For each matching row emit an individual UPDATE.
    let pk_field = schema.field_with_name(pk_col).map_err(|_| {
        BasinError::InvalidSchema(format!("PK column {pk_col:?} not in table schema"))
    })?;
    let mut updated = 0usize;
    for batch in &batches {
        let num_rows = batch.num_rows();
        let pk_arr = batch.column(0);
        for row in 0..num_rows {
            let pk_lit = scalar_from_array(pk_arr.as_ref(), row, pk_field.data_type())?;

            // Build SET clause: col1 = rhs_val, col2 = rhs_val, …
            let mut set_parts: Vec<String> = Vec::new();
            for (i, col) in set_cols.iter().enumerate() {
                let rhs_arr = batch.column(i + 1);
                let rhs_field_dt = batch.schema().field(i + 1).data_type().clone();
                let rhs_lit = scalar_from_array(rhs_arr.as_ref(), row, &rhs_field_dt)?;
                set_parts.push(format!("{col} = {rhs_lit}"));
            }

            let update_sql = format!(
                "UPDATE {target_name} SET {sets} WHERE {pk_col} = {pk_lit}",
                sets = set_parts.join(", ")
            );
            match Box::pin(sess.execute(&update_sql)).await? {
                ExecResult::Empty { tag } => {
                    if tag.starts_with("UPDATE ") {
                        let n: usize = tag[7..].trim().parse().unwrap_or(0);
                        updated += n;
                    }
                }
                ExecResult::Rows { .. } => {}
            }
        }
    }

    Ok(ExecResult::Empty {
        tag: format!("UPDATE {updated}"),
    })
}

// ---------------------------------------------------------------------------
// Correlated-subquery path (EXISTS / NOT EXISTS in WHERE)
// ---------------------------------------------------------------------------
//
// DataFusion's optimizer decorrelates EXISTS / NOT EXISTS subqueries when they
// appear inside a SELECT plan (it rewrites them to semi/anti joins).  The
// custom CompoundPredicate engine used by the file-scan layer has no such
// capability.
//
// Strategy when the WHERE clause contains an EXISTS node:
//
// *With PRIMARY KEY*: run `SELECT <pk_cols> FROM t WHERE <selection>` through
// DataFusion (decorrelates EXISTS → semi/anti join), collect the PK set, then
// re-issue `DELETE FROM t WHERE pk IN (…)` / `UPDATE t SET … WHERE pk IN (…)`
// so cascades, soft-delete, and audit still fire.
//
// *Without PRIMARY KEY* (DELETE only): run `SELECT * FROM t WHERE NOT
// (<selection>)` to obtain the rows to *keep*, then replace the table with
// those rows using a direct write+commit.  UPDATE without PK is rejected with
// a clear error.
//
// Correctness invariants preserved by this design:
//  * NULL semantics: DataFusion's anti-join (NOT EXISTS) ignores NULLs in
//    the correlated column — NOT EXISTS finds rows with no match, which is
//    the correct SQL semantic even when u.col has NULLs.
//  * Empty subquery result: NOT EXISTS over an empty table returns every row
//    of the outer table (all match); EXISTS returns nothing.  DataFusion
//    handles both correctly via the anti/semi join rewrite.
//  * EXISTS vs NOT EXISTS: `negated = false` in the AST node → EXISTS (semi);
//    `negated = true` → NOT EXISTS (anti). DataFusion respects this.
//  * If UPDATE is requested on a table with no PK, we fail cleanly with a
//    clear error rather than silently applying wrong mutations.

/// `DELETE FROM t WHERE [NOT] EXISTS (SELECT 1 FROM u WHERE u.x = t.x)` and
/// similar correlated-EXISTS forms.
///
/// *With PK*:
/// 1. Run `SELECT <pk_cols> FROM t WHERE <selection>` through DataFusion
///    so the optimizer decorrelates the EXISTS → semi/anti join.
/// 2. Collect the matching PK tuples.
/// 3. Re-issue `DELETE FROM t WHERE pk IN (…)` through the normal path so
///    cascades, soft-delete, and audit all fire.
///
/// *Without PK*:
/// 1. Run `SELECT * FROM t WHERE NOT (<selection>)` to get rows to KEEP.
/// 2. Replace the entire table with those rows via direct write+commit.
/// 3. Return `DELETE <dropped_count>`.
async fn exec_delete_via_df_rowset(
    sess: &ProjectSession,
    table: &TableName,
    table_alias: Option<&str>,
    selection: &Expr,
) -> Result<ExecResult> {
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let table_str = table.as_str();
    // When the original DELETE used a table alias (e.g. `DELETE FROM posts p WHERE p.col = …`),
    // we must include the alias in the FROM clause of the synthesised SELECT so DataFusion
    // can resolve alias-qualified references in the WHERE predicate.
    let from_clause = match table_alias {
        Some(alias) if alias != table_str => format!("{table_str} {alias}"),
        _ => table_str.to_string(),
    };
    // The qualifier to use in SELECT projections: prefer the alias if present.
    let qualifier = table_alias.unwrap_or(table_str);

    if !meta.pk_columns.is_empty() {
        // --- PK path: get matching row PKs, re-issue plain DELETE ---
        let pk = &meta.pk_columns;

        // Build: SELECT t.pk1, t.pk2, … FROM t [AS alias] WHERE <selection>
        // The table qualifier prevents ambiguity when the subquery also
        // references `t`.
        let pk_proj: Vec<String> = pk
            .iter()
            .map(|c| format!("{qualifier}.{c}"))
            .collect();

        let rowset_sql = format!(
            "SELECT {proj} FROM {from_clause} WHERE {pred}",
            proj = pk_proj.join(", "),
            from_clause = from_clause,
            pred = selection,
        );

        // Execute through the full engine pipeline (refresh_table, optimizer,
        // DataFusion decorrelation) so EXISTS is lowered to semi/anti join.
        let rowset_result = Box::pin(sess.execute(&rowset_sql)).await?;
        let batches = match rowset_result {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { .. } => {
                return Ok(ExecResult::Empty {
                    tag: "DELETE 0".into(),
                })
            }
        };

        let pk_tuples = collect_pk_batches(&batches, pk)?;
        if pk_tuples.is_empty() {
            return Ok(ExecResult::Empty {
                tag: "DELETE 0".into(),
            });
        }

        let schema = meta.schema.clone();
        let where_pred = build_pk_in_predicate(&pk_tuples, pk, &schema)?;
        let delete_sql = format!("DELETE FROM {table_str} WHERE {where_pred}");
        return Box::pin(sess.execute(&delete_sql)).await;
    }

    // --- No-PK path: get rows to KEEP by inverting the predicate ---
    //
    // Rows to keep = those that do NOT match the DELETE predicate:
    //   SELECT * FROM t WHERE NOT (<selection>)
    //
    // We select all columns; DataFusion still decorrelates the EXISTS
    // inside `NOT (EXISTS(...))`.
    let keep_sql = format!(
        "SELECT * FROM {from_clause} WHERE NOT ({pred})",
        from_clause = from_clause,
        pred = selection,
    );

    pre_mutation_flush(sess).await?;

    // Re-load metadata after the flush (snapshot id must be current).
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();

    // Count total rows BEFORE deletion so we can report DELETE <n>.
    let total_rows_before: usize = storage
        .list_data_files_with_stats(&sess.project, table)
        .await?
        .iter()
        .map(|f| f.row_count as usize)
        .sum();

    if total_rows_before == 0 {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
    }

    // Execute the KEEP query through the full engine pipeline (refreshes
    // table registrations, decorrelates EXISTS).
    let keep_result = Box::pin(sess.execute(&keep_sql)).await?;
    let keep_batches: Vec<RecordBatch> = match keep_result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => Vec::new(),
    };
    // keep_batches are already in workspace Arrow format — exec_select
    // converts them via batch_df_to_ws before placing them in ExecResult::Rows.

    let kept_rows: usize = keep_batches.iter().map(|b| b.num_rows()).sum();
    let deleted = total_rows_before.saturating_sub(kept_rows);

    if deleted == 0 {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
    }

    // List ALL current data files so we can remove them all and replace
    // with the kept-rows batch.
    let data_files = storage
        .list_data_files_with_stats(&sess.project, table)
        .await?;
    let all_paths: Vec<String> = data_files
        .iter()
        .map(|f| f.path.as_ref().to_string())
        .collect();

    // Write the kept rows as a new file (empty → no file written).
    let added = write_replacement(sess, table, schema.clone(), keep_batches).await?;

    // Atomically swap old files → new file in the catalog snapshot.
    commit_replace(sess, table, meta.current_snapshot, all_paths.clone(), added).await?;

    // Physical cleanup + session refresh.
    delete_objects(sess, table, schema.as_ref(), &all_paths).await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;

    Ok(ExecResult::Empty {
        tag: format!("DELETE {deleted}"),
    })
}

/// `UPDATE t SET … WHERE [NOT] EXISTS (SELECT 1 FROM u WHERE u.x = t.x)` and
/// similar correlated-EXISTS forms.
///
/// Requires the target table to have a PRIMARY KEY.  Materialises the matching
/// PK set via a DataFusion SELECT (which decorrelates EXISTS), then re-issues
/// `UPDATE t SET … WHERE pk IN (…)` through the normal path.
///
/// UPDATE on a no-PK table with EXISTS is rejected with a clear error; a
/// correct fix would require a full-table scan with per-row identity tracking
/// that is out of scope for v0.1.
async fn exec_update_via_df_rowset(
    sess: &ProjectSession,
    table_str: &str,
    assignments: &[Assignment],
    selection: &Expr,
) -> Result<ExecResult> {
    let table = TableName::new(table_str.to_string())?;
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    if meta.pk_columns.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "UPDATE WHERE EXISTS/NOT EXISTS requires the target table {:?} to have a PRIMARY KEY",
            table_str
        )));
    }
    let pk = &meta.pk_columns;

    // Build: SELECT t.pk1, … FROM t WHERE <selection>
    let pk_proj: Vec<String> = pk
        .iter()
        .map(|c| format!("{table_str}.{c}"))
        .collect();

    let rowset_sql = format!(
        "SELECT {proj} FROM {tbl} WHERE {pred}",
        proj = pk_proj.join(", "),
        tbl = table_str,
        pred = selection,
    );

    let rowset_result = Box::pin(sess.execute(&rowset_sql)).await?;
    let batches = match rowset_result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => {
            return Ok(ExecResult::Empty {
                tag: "UPDATE 0".into(),
            })
        }
    };

    let pk_tuples = collect_pk_batches(&batches, pk)?;
    if pk_tuples.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "UPDATE 0".into(),
        });
    }

    // Build SET clause text.
    let set_parts: Vec<String> = assignments
        .iter()
        .map(|a| format!("{}", a))
        .collect();
    let set_clause = set_parts.join(", ");

    let schema = meta.schema.clone();
    let where_pred = build_pk_in_predicate(&pk_tuples, pk, &schema)?;
    let update_sql = format!(
        "UPDATE {table_str} SET {set_clause} WHERE {where_pred}"
    );
    Box::pin(sess.execute(&update_sql)).await
}

/// Collect `(pk_col1, pk_col2, …)` tuples from a result set as string
/// literals. Each column value is rendered as its SQL literal form
/// (quoted for text, unquoted for numeric types).
fn collect_pk_batches(
    batches: &[RecordBatch],
    pk_cols: &[String],
) -> Result<Vec<Vec<String>>> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut tuple = Vec::with_capacity(pk_cols.len());
            for (i, _) in pk_cols.iter().enumerate() {
                let arr = batch.column(i);
                let dt = batch.schema().field(i).data_type().clone();
                let lit = scalar_from_array(arr.as_ref(), row, &dt)?;
                tuple.push(lit);
            }
            out.push(tuple);
        }
    }
    Ok(out)
}

/// Render an Arrow array element at `row` as a SQL literal string.
/// Quoted for Utf8; bare numeric / bool otherwise.
fn scalar_from_array(
    arr: &dyn arrow_array::Array,
    row: usize,
    dt: &DataType,
) -> Result<String> {
    use arrow_array::cast::AsArray;
    if arr.is_null(row) {
        return Ok("NULL".into());
    }
    match dt {
        DataType::Utf8 => {
            let s = arr.as_string::<i32>().value(row);
            Ok(format!("'{}'", s.replace('\'', "''")))
        }
        DataType::LargeUtf8 => {
            let s = arr.as_string::<i64>().value(row);
            Ok(format!("'{}'", s.replace('\'', "''")))
        }
        DataType::Int8 => Ok(arr.as_primitive::<arrow_array::types::Int8Type>().value(row).to_string()),
        DataType::Int16 => Ok(arr.as_primitive::<arrow_array::types::Int16Type>().value(row).to_string()),
        DataType::Int32 => Ok(arr.as_primitive::<arrow_array::types::Int32Type>().value(row).to_string()),
        DataType::Int64 => Ok(arr.as_primitive::<arrow_array::types::Int64Type>().value(row).to_string()),
        DataType::Float32 => Ok(arr.as_primitive::<arrow_array::types::Float32Type>().value(row).to_string()),
        DataType::Float64 => Ok(arr.as_primitive::<arrow_array::types::Float64Type>().value(row).to_string()),
        DataType::Boolean => {
            let b = arr.as_boolean().value(row);
            Ok(if b { "TRUE".into() } else { "FALSE".into() })
        }
        DataType::Timestamp(_, _) => {
            // Render as a BIGINT literal (microseconds) which is what the
            // predicate evaluator stores timestamps as.
            use arrow_array::types::TimestampMicrosecondType;
            let v = arr.as_primitive::<TimestampMicrosecondType>().value(row);
            Ok(v.to_string())
        }
        other => Err(BasinError::InvalidSchema(format!(
            "UPDATE/DELETE FROM: unsupported PK column type {other:?} in v0.1"
        ))),
    }
}

/// Build a WHERE predicate that matches any of the given PK tuples.
/// Single-column PK → `pk IN (v1, v2, …)`.
/// Multi-column PK → `(pk1=v1 AND pk2=v2) OR …`.
fn build_pk_in_predicate(
    tuples: &[Vec<String>],
    pk_cols: &[String],
    _schema: &Schema,
) -> Result<String> {
    if pk_cols.len() == 1 {
        let values: Vec<String> = tuples.iter().map(|t| t[0].clone()).collect();
        Ok(format!("{} IN ({})", pk_cols[0], values.join(", ")))
    } else {
        let clauses: Vec<String> = tuples
            .iter()
            .map(|tuple| {
                let parts: Vec<String> = pk_cols
                    .iter()
                    .zip(tuple.iter())
                    .map(|(c, v)| format!("{c} = {v}"))
                    .collect();
                format!("({})", parts.join(" AND "))
            })
            .collect();
        Ok(clauses.join(" OR "))
    }
}

/// Resolved SET-clause assignment for one column.
///
/// `Scalar` is the fast path for literal assignments (`SET col = 5`).
/// `DFExpr` is the DataFusion path for expression assignments
/// (`SET col = col + 1`, `SET col = now()`, etc.). The `sql_text` is
/// the raw expression string handed to DataFusion as a `SELECT <sql_text>
/// FROM <src>` projection.
#[derive(Clone)]
#[allow(dead_code)]
enum AssignmentValue {
    Scalar(ScalarValue),
    DFExpr { sql_text: String, col_type: DataType },
}

/// One captured row-level change, lazily materialised only when at
/// least one [`ChangeEventSink`] is attached or `AUDIT TO` is configured.
#[derive(Clone)]
struct RowChange {
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
}

/// Hot-path probe: are any sinks attached on either side? When false,
/// the rest of the mutation path is byte-identical to the no-event
/// baseline.
fn sinks_attached(sess: &ProjectSession) -> bool {
    let guard = sess
        .engine
        .event_sinks()
        .read()
        .expect("event_sinks lock poisoned");
    registry_has_any(&guard)
}

fn build_events(
    sess: &ProjectSession,
    table: &TableName,
    op: ChangeOp,
    rows: Vec<RowChange>,
) -> Vec<ChangeEvent> {
    if rows.is_empty() {
        return Vec::new();
    }
    let user = if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.clone())
    };
    let mut out = Vec::with_capacity(rows.len());
    for RowChange { before, after } in rows {
        let seq = sess.engine.next_event_seq(&sess.project, table);
        out.push(make_event(
            &sess.project,
            table,
            op,
            before,
            after,
            seq,
            user.clone(),
        ));
    }
    out
}

/// DELETE / AllMatch path: read the file (which the no-sink path skips
/// entirely) and emit one `before` per row. `after` is `None` for
/// DELETE. When `returning_out` is `Some`, the unfiltered batches are
/// also captured for the RETURNING projection.
async fn capture_dropped_file(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
    out: &mut Vec<RowChange>,
    mut returning_out: Option<&mut Vec<RecordBatch>>,
) -> Result<()> {
    let mut stream = storage.read_file(project, path).await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            out.push(RowChange {
                before: Some(build_row_json(&batch, row)?),
                after: None,
            });
        }
        if let Some(target) = returning_out.as_deref_mut() {
            target.push(batch);
        }
    }
    Ok(())
}

/// Like [`evaluate_and_partition_delete`] but also returns the matched
/// rows (the ones being deleted). Each entry in the second vec is
/// `(batch, row_idx)` for sink/audit JSON lazily; the third element is
/// the matched-rows batch for RETURNING (only built when requested).
async fn evaluate_and_partition_delete_capturing(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
    want_returning: bool,
) -> Result<(
    Vec<RecordBatch>,
    Vec<(RecordBatch, usize)>,
    Option<RecordBatch>,
)> {
    let mut stream = storage.read_file(project, path).await?;
    let mut kept = Vec::new();
    let mut deleted = Vec::new();
    let mut deleted_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("delete predicate eval: {e}")))?;
        for i in 0..batch.num_rows() {
            if !mask.is_null(i) && mask.value(i) {
                deleted.push((batch.clone(), i));
            }
        }
        if want_returning {
            let kept_mask = sanitize_mask_local(&mask);
            let deleted_only = arrow_select::filter::filter_record_batch(&batch, &kept_mask)
                .map_err(|e| {
                    BasinError::internal(format!("filter deleted batch for RETURNING: {e}"))
                })?;
            if deleted_only.num_rows() > 0 {
                deleted_batches.push(deleted_only);
            }
        }
        let inverse = invert_mask(&mask);
        let kb = arrow_select::filter::filter_record_batch(&batch, &inverse)
            .map_err(|e| BasinError::internal(format!("delete filter batch: {e}")))?;
        if kb.num_rows() > 0 {
            kept.push(kb);
        }
    }
    let deleted_batch = if want_returning && !deleted_batches.is_empty() {
        // Concat all matched-row batches into one for cleaner downstream
        // handling. The schema is uniform within a file.
        let s = deleted_batches[0].schema();
        Some(
            arrow_select::concat::concat_batches(&s, &deleted_batches)
                .map_err(|e| BasinError::internal(format!("concat deleted batches: {e}")))?,
        )
    } else {
        None
    };
    Ok((kept, deleted, deleted_batch))
}

/// Normalise NULL mask cells to `false`. Used by RETURNING capture so a
/// NULL predicate result for an unrelated reason doesn't accidentally
/// pull that row into the deleted set.
fn sanitize_mask_local(mask: &BooleanArray) -> BooleanArray {
    let mut b = BooleanBuilder::with_capacity(mask.len());
    for i in 0..mask.len() {
        if mask.is_null(i) {
            b.append_value(false);
        } else {
            b.append_value(mask.value(i));
        }
    }
    b.finish()
}

/// Read a file into in-memory batches. Used by the event-capturing
/// UPDATE path so we can pair before/after row-by-row without re-reading.
async fn read_file_to_batches(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(project, path).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// AllMatch UPDATE: every row in `befores` is matched. Returns the
/// post-SET batches.
async fn apply_assignments_all(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &basin_common::ProjectId,
    befores: &[RecordBatch],
    assignments: &[(usize, AssignmentRhs)],
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(befores.len());
    for b in befores {
        let mask = BooleanArray::from(vec![true; b.num_rows()]);
        out.push(apply_assignments(catalog, project, b, &mask, assignments).await?);
    }
    Ok(out)
}

/// Pair before/after rows into [`RowChange`] entries. When `masks` is
/// `Some`, only matched rows produce events (the Mixed branch); when
/// `None`, every row produces one (the AllMatch / no-predicate branch).
fn capture_update_events(
    befores: &[RecordBatch],
    afters: &[RecordBatch],
    masks: Option<&[BooleanArray]>,
    out: &mut Vec<RowChange>,
) -> Result<()> {
    debug_assert_eq!(befores.len(), afters.len());
    for (i, (b, a)) in befores.iter().zip(afters.iter()).enumerate() {
        let mask = masks.map(|m| &m[i]);
        for row in 0..b.num_rows() {
            let matched = mask.is_none_or(|m| !m.is_null(row) && m.value(row));
            if !matched {
                continue;
            }
            out.push(RowChange {
                before: Some(build_row_json(b, row)?),
                after: Some(build_row_json(a, row)?),
            });
        }
    }
    Ok(())
}

/// Wrap the pruning evaluator: returns `Mixed` if the predicate is `None`
/// so callers can branch uniformly.
fn file_outcome(
    pred: Option<&CompoundPredicate>,
    file: &DataFile,
    schema: &Schema,
) -> PruneOutcome {
    match pred {
        Some(p) => evaluate_compound_for_pruning(p, &file.column_stats, schema, file.row_count),
        None => PruneOutcome::AllMatch,
    }
}

/// Read a single Parquet file and return only the rows that do NOT match
/// `pred`. Used by DELETE.
async fn evaluate_and_partition_delete(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(project, path).await?;
    let mut kept = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("delete predicate eval: {e}")))?;
        let inverse = invert_mask(&mask);
        let kb = arrow_select::filter::filter_record_batch(&batch, &inverse)
            .map_err(|e| BasinError::internal(format!("delete filter batch: {e}")))?;
        if kb.num_rows() > 0 {
            kept.push(kb);
        }
    }
    Ok(kept)
}

/// Read a single Parquet file and apply SET to every row (when `pred` is
/// None) or every matching row (when `pred` is Some). Caller decides
/// matched-row count externally — used in the AllMatch branch where
/// every row is updated.
async fn read_and_apply_assignments(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    storage: &Storage,
    project: &ProjectId,
    path: &object_store::path::Path,
    pred: Option<&CompoundPredicate>,
    assignments: &[(usize, AssignmentRhs)],
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(project, path).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = match pred {
            Some(p) => evaluate_compound(&batch, p)
                .map_err(|e| BasinError::internal(format!("update predicate eval: {e}")))?,
            None => BooleanArray::from(vec![true; batch.num_rows()]),
        };
        out.push(apply_assignments(catalog, project, &batch, &mask, assignments).await?);
    }
    Ok(out)
}

/// Same as `read_and_apply_assignments` but also returns the count of
/// rows that actually matched the predicate. Used by the Mixed branch
/// where we need the count for the row tag and also have to detect the
/// "stats said maybe but nothing actually matched" no-op case.
async fn read_and_apply_assignments_mixed(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    storage: &Storage,
    project: &ProjectId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
    assignments: &[(usize, AssignmentRhs)],
) -> Result<(usize, Vec<RecordBatch>)> {
    let mut stream = storage.read_file(project, path).await?;
    let mut matched = 0usize;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("update predicate eval: {e}")))?;
        matched += mask.iter().filter(|b| matches!(b, Some(true))).count();
        out.push(apply_assignments(catalog, project, &batch, &mask, assignments).await?);
    }
    Ok((matched, out))
}

/// Force-flush any in-RAM tail rows in the shard before we list data files.
/// Without this, a DELETE / UPDATE issued shortly after an INSERT through
/// the shard owner would silently skip rows that are still in the WAL.
/// PK enforcement on UPDATE. Build the "existing PK set" from data
/// files NOT in `replaced_paths` and validate the post-SET batches
/// against that plus their own intra-batch duplicates.
async fn check_update_pk(
    storage: &Storage,
    project: &basin_common::ProjectId,
    table: &TableName,
    table_name_str: &str,
    pk_columns: &[String],
    batches: &[RecordBatch],
    replaced_paths: &[String],
) -> Result<()> {
    if pk_columns.is_empty() {
        return Ok(());
    }
    use std::collections::HashSet;
    let replaced: HashSet<&str> = replaced_paths.iter().map(|s| s.as_str()).collect();
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    let mut existing: HashSet<Vec<String>> = HashSet::new();
    for f in &data_files {
        if replaced.contains(f.path.as_ref()) {
            continue;
        }
        let mut stream = storage.read_file(project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let idx: Vec<usize> = pk_columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!("PK column {c:?} missing from data file"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rb.num_rows() {
                if let Some(k) = crate::constraints::pk_tuple_for_row(&rb, &idx, row)? {
                    existing.insert(k);
                }
            }
        }
    }
    let mut seen: HashSet<Vec<String>> = HashSet::new();
    for b in batches {
        let idx: Vec<usize> = pk_columns
            .iter()
            .map(|c| {
                b.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("PK column {c:?} missing from update batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..b.num_rows() {
            let Some(k) = crate::constraints::pk_tuple_for_row(b, &idx, row)? else {
                return Err(BasinError::CheckViolation(format!(
                    "null value in column violates not-null constraint on PRIMARY KEY of \
                     \"{table_name_str}\""
                )));
            };
            if existing.contains(&k) || !seen.insert(k.clone()) {
                return Err(BasinError::UniqueViolation(format!(
                    "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                     Key ({})=({}) already exists.",
                    pk_columns.join(", "),
                    k.join(", ")
                )));
            }
        }
    }
    Ok(())
}

async fn pre_mutation_flush(sess: &ProjectSession) -> Result<()> {
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    Ok(())
}

/// Write the replacement batches as one new Parquet file (none if `batches`
/// is empty). We concat first because `Storage::write_batch` is one-batch-
/// per-call; a multi-batch table would otherwise produce N replacement files
/// per UPDATE which fragments the Parquet base needlessly.
async fn write_replacement(
    sess: &ProjectSession,
    table: &TableName,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFileRef>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let merged = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow_select::concat::concat_batches(&schema, refs)
            .map_err(|e| BasinError::internal(format!("concat batches for rewrite: {e}")))?
    };
    if merged.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let part = PartitionKey::default_key();
    // Honour the per-table writer overrides (bloom-filter columns,
    // row-group size) on the rewrite path too; otherwise the
    // copy-on-write replacement file would silently lose the table's
    // configured pruning aids.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let opts = basin_storage::WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        cluster_columns: meta.cluster_columns.clone(),
    };
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, table, &part, &merged, &opts)
        .await?;
    Ok(vec![DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
    }])
}

/// Commit the swap with one optimistic-conflict retry. Mirrors
/// [`crate::executor::exec_insert`]'s retry shape — the only difference is
/// that on retry we re-validate the snapshot id but reuse the same already-
/// written `added_files`. We do NOT re-read or re-rewrite the table on
/// conflict: in copy-on-write the rewrite is not re-driven by a stale read,
/// it's just stamped against the new snapshot. If a competing INSERT slipped
/// in between our list and our commit it would still appear in the next
/// SELECT (the new INSERT lands as a separate Parquet file).
async fn commit_replace(
    sess: &ProjectSession,
    table: &TableName,
    expected: basin_catalog::SnapshotId,
    removed: Vec<String>,
    added: Vec<DataFileRef>,
) -> Result<()> {
    match sess
        .engine
        .config()
        .catalog
        .replace_data_files(
            &sess.project,
            table,
            expected,
            removed.clone(),
            added.clone(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BasinError::CommitConflict(_)) => {
            let fresh = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, table)
                .await?;
            sess.engine
                .config()
                .catalog
                .replace_data_files(&sess.project, table, fresh.current_snapshot, removed, added)
                .await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Best-effort physical removal of the old Parquet files plus any HNSW
/// sidecar segments that share their ULID. Object stores occasionally
/// hiccup on individual deletes, which is fine: the catalog commit
/// already advanced the snapshot, and any straggler files will be cleaned
/// up by a future maintenance pass. Logging-only on per-file errors keeps
/// the user-facing UPDATE/DELETE result tied to the catalog commit (the
/// source of truth) rather than to deferred cleanup.
///
/// HNSW cleanup is intentionally best-effort: stale sidecars are an
/// inefficiency, not a correctness bug. Vector search merges across all
/// segments and re-checks each, so a leftover segment pointing at a
/// deleted Parquet ULID just contributes hits we discard.
async fn delete_objects(
    sess: &ProjectSession,
    table: &TableName,
    schema: &Schema,
    paths: &[String],
) -> Result<()> {
    let storage = &sess.engine.config().storage;
    let store = storage.project_object_store(&sess.project);
    let root = storage.root_prefix_handle();
    let vector_columns: Vec<String> = schema
        .fields()
        .iter()
        .filter_map(|f| match f.data_type() {
            DataType::FixedSizeList(child, _) if *child.data_type() == DataType::Float32 => {
                Some(f.name().clone())
            }
            _ => None,
        })
        .collect();

    for p in paths {
        let obj = object_store::path::Path::from(p.as_str());
        if let Err(e) = store.delete(&obj).await {
            tracing::warn!(path = %p, error = %e, "post-replace object delete failed");
        }
        // Best-effort sidecar sweep. We don't probe the store first; a
        // missing sidecar (most files don't have one) returns NotFound,
        // which we swallow.
        for column in &vector_columns {
            if let Some(sidecar) = vector_index_segment_key_for_data_file(
                root.as_ref(),
                &sess.project,
                table,
                column,
                p,
            ) {
                if let Err(e) = store.delete(&sidecar).await {
                    // NotFound is the normal case for files that never had
                    // an index sidecar; demote to debug.
                    let msg = format!("{e}");
                    if msg.contains("NotFound") || msg.contains("not found") {
                        tracing::debug!(path = %sidecar, "no hnsw sidecar to delete");
                    } else {
                        tracing::warn!(path = %sidecar, error = %e, "hnsw sidecar delete failed");
                    }
                }
            }
        }
    }
    Ok(())
}

fn invert_mask(mask: &BooleanArray) -> BooleanArray {
    // Fresh BooleanArray with the same nullability shape. Treat NULL as
    // false on the kept side — a NULL predicate evaluation means "we don't
    // know if it matched", so for DELETE we conservatively keep it.
    let mut out = BooleanBuilder::with_capacity(mask.len());
    for i in 0..mask.len() {
        if mask.is_null(i) {
            out.append_value(true);
        } else {
            out.append_value(!mask.value(i));
        }
    }
    out.finish()
}

/// Apply the parsed SET clause to one batch. Matched rows have their
/// assigned columns swapped out for the new value; unmatched rows pass
/// through untouched. NULL mask entries are treated as unmatched (same
/// rationale as `invert_mask`).
///
/// For `AssignmentValue::Scalar` the assignment is synchronous.
/// For `AssignmentValue::DFExpr` the expression is evaluated via DataFusion
/// over the entire batch, producing a new column array, and then blended
/// into matched rows only using `arrow_select::zip`.
/// Expression RHSs are evaluated once per batch against the OLD row
/// values (PG semantics), then merged into matched rows via the mask.
async fn apply_assignments(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &basin_common::ProjectId,
    batch: &RecordBatch,
    mask: &BooleanArray,
    assignments: &[(usize, AssignmentRhs)],
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let original = batch.column(col_idx).clone();
        let assignment = assignments.iter().find(|(idx, _)| *idx == col_idx);
        let new_col = match assignment {
            None => original,
            Some((_, AssignmentRhs::Scalar(scalar))) => {
                let field = schema.field(col_idx);
                build_assigned_column(field.data_type(), &original, mask, scalar)?
            }
            Some((_, AssignmentRhs::Expr(text))) => {
                let field = schema.field(col_idx);
                let computed = crate::generated_cols::eval_expression(
                    catalog,
                    project,
                    batch,
                    text,
                    field.data_type(),
                )
                .await?;
                crate::generated_cols::merge_by_mask(&original, &computed, mask)?
            }
        };
        new_columns.push(new_col);
    }
    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| BasinError::internal(format!("rebuild batch after SET: {e}")))
}

/// Blend two arrays: take `new_col` at rows where `mask` is true, `orig`
/// at rows where mask is false or null. Both arrays must be the same length.
#[allow(dead_code)]
fn blend_by_mask(orig: &ArrayRef, new_col: &ArrayRef, mask: &BooleanArray) -> Result<ArrayRef> {
    if orig.len() != new_col.len() || orig.len() != mask.len() {
        return Err(BasinError::internal(format!(
            "blend_by_mask length mismatch: orig={}, new={}, mask={}",
            orig.len(),
            new_col.len(),
            mask.len()
        )));
    }
    // Cast new_col to orig's type if needed (e.g. DataFusion may produce
    // Int32 for an Int64 column).
    let new_col_cast = if new_col.data_type() != orig.data_type() {
        arrow::compute::cast(new_col, orig.data_type()).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "UPDATE SET expression result type {:?} cannot be coerced to column type {:?}: {e}",
                new_col.data_type(),
                orig.data_type()
            ))
        })?
    } else {
        new_col.clone()
    };
    // Sanitise: treat null mask entries as false (keep original).
    let mut sanitised = arrow_array::builder::BooleanBuilder::with_capacity(mask.len());
    for i in 0..mask.len() {
        sanitised.append_value(!mask.is_null(i) && mask.value(i));
    }
    let sanitised = sanitised.finish();
    arrow_select::zip::zip(&sanitised, &new_col_cast, orig)
        .map_err(|e| BasinError::internal(format!("blend_by_mask zip: {e}")))
}

/// Produce an empty RETURNING result with the right projected schema
/// when no rows matched the UPDATE/DELETE, or fall back to the standard
/// `ExecResult::Empty` when RETURNING wasn't requested. The empty-schema
/// route still surfaces column names so a downstream `psql \d` doesn't
/// see a 0-column result set.
fn empty_or_returning(
    tag: &str,
    table_schema: Arc<Schema>,
    returning: Option<&[SelectItem]>,
) -> ExecResult {
    match returning {
        None => ExecResult::Empty { tag: tag.into() },
        Some(items) => {
            let projected = projected_returning_schema(table_schema.as_ref(), items);
            ExecResult::Rows {
                schema: Arc::new(projected),
                batches: Vec::new(),
            }
        }
    }
}

/// Build the projected schema for a RETURNING clause without actually
/// running the projection. `*` keeps every field; identifier items pick
/// individual fields; everything else (aliases, expressions) falls back
/// to the raw column type — sufficient for the zero-row case where the
/// caller only needs column names.
fn projected_returning_schema(schema: &Schema, items: &[SelectItem]) -> Schema {
    use sqlparser::ast::SelectItem as SI;
    let mut fields: Vec<arrow_schema::FieldRef> = Vec::new();
    for it in items {
        match it {
            SI::Wildcard(_) | SI::QualifiedWildcard(_, _) => {
                for f in schema.fields() {
                    fields.push(f.clone());
                }
            }
            SI::UnnamedExpr(Expr::Identifier(ident)) => {
                if let Ok(idx) = schema.index_of(&ident.value) {
                    fields.push(schema.field(idx).clone().into());
                }
            }
            SI::ExprWithAlias {
                expr: Expr::Identifier(ident),
                alias,
            } => {
                if let Ok(idx) = schema.index_of(&ident.value) {
                    let orig = schema.field(idx);
                    fields.push(Arc::new(arrow_schema::Field::new(
                        alias.value.as_str(),
                        orig.data_type().clone(),
                        orig.is_nullable(),
                    )));
                }
            }
            _ => {
                // Best-effort: stamp an unknown-aliased Utf8 placeholder.
                // The Rows path always rebuilds the real schema from the
                // first batch, so this only matters when the result is
                // empty.
                fields.push(Arc::new(arrow_schema::Field::new(
                    "?column?",
                    DataType::Utf8,
                    true,
                )));
            }
        }
    }
    Schema::new(fields)
}

/// Run the RETURNING projection over the captured input batches. Items
/// are rendered back to SQL and pushed into one DataFusion SELECT, so
/// any expression DataFusion understands (column refs, arithmetic,
/// function calls) works without a bespoke evaluator here.
async fn project_returning(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &basin_common::ProjectId,
    table_schema: Arc<Schema>,
    inputs: Vec<RecordBatch>,
    items: &[SelectItem],
) -> Result<ExecResult> {
    if inputs.is_empty() {
        let projected = projected_returning_schema(table_schema.as_ref(), items);
        return Ok(ExecResult::Rows {
            schema: Arc::new(projected),
            batches: Vec::new(),
        });
    }
    // Concat upstream batches so DataFusion sees one MemTable partition
    // — matches the generated-cols helper.
    let merged = if inputs.len() == 1 {
        inputs.into_iter().next().unwrap()
    } else {
        let refs: Vec<&RecordBatch> = inputs.iter().collect();
        arrow_select::concat::concat_batches(&inputs[0].schema(), refs)
            .map_err(|e| BasinError::internal(format!("concat returning batches: {e}")))?
    };
    // Build the projection list as comma-separated SQL.
    let projection = items
        .iter()
        .map(|it| it.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let projection_sql =
        format!("SELECT {projection} FROM __basin_returning_src");
    let rewritten =
        crate::sql_functions::rewrite_sql_inlining_functions(catalog, project, &projection_sql)
            .await?;

    let ctx = SessionContext::new();
    crate::udf::register_distance_udfs(&ctx);
    crate::udf::register_pg_udfs(&ctx);
    crate::udf::register_pg_compat_udfs(&ctx);
    let df_batch = crate::convert::batch_ws_to_df(&merged)?;
    let df_schema = df_batch.schema();
    let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
        .map_err(|e| BasinError::internal(format!("MemTable for RETURNING: {e}")))?;
    ctx.register_table("__basin_returning_src", Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register RETURNING table: {e}")))?;
    let df = ctx.sql(&rewritten).await.map_err(|e| {
        BasinError::InvalidSchema(format!("RETURNING expression failed to plan: {e}"))
    })?;
    let df_collected = df.collect().await.map_err(|e| {
        BasinError::InvalidSchema(format!("RETURNING expression failed to execute: {e}"))
    })?;
    let mut ws_batches: Vec<RecordBatch> = Vec::with_capacity(df_collected.len());
    for b in &df_collected {
        ws_batches.push(crate::convert::batch_df_to_ws(b)?);
    }
    let schema = if let Some(first) = ws_batches.first() {
        first.schema()
    } else {
        Arc::new(projected_returning_schema(table_schema.as_ref(), items))
    };
    Ok(ExecResult::Rows {
        schema,
        batches: ws_batches,
    })
}

/// Build a column where matched rows take the new scalar value and
/// unmatched rows keep their original value. Per-column dispatch on data
/// type matches the literal coercion the INSERT path uses.
fn build_assigned_column(
    dt: &DataType,
    original: &ArrayRef,
    mask: &BooleanArray,
    scalar: &ScalarValue,
) -> Result<ArrayRef> {
    let n = original.len();
    let matched = |i: usize| -> bool { !mask.is_null(i) && mask.value(i) };
    match (dt, scalar) {
        (DataType::Int64, ScalarValue::Int64(v)) => {
            let arr = original
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("expected Int64Array for SET"))?;
            let mut b = Int64Builder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(*v);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // INT4 column — scalar is Int64 (from literal parsing), narrowed to i32.
        (DataType::Int32, ScalarValue::Int64(v)) => {
            let set_val = i32::try_from(*v).map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "UPDATE SET: value {v} out of range for INT4 column"
                ))
            })?;
            let arr = original
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| BasinError::internal("expected Int32Array for SET"))?;
            let mut b = Int32Builder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(set_val);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // INT2 column — scalar is Int64, narrowed to i16.
        (DataType::Int16, ScalarValue::Int64(v)) => {
            let set_val = i16::try_from(*v).map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "UPDATE SET: value {v} out of range for INT2 column"
                ))
            })?;
            let arr = original
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| BasinError::internal("expected Int16Array for SET"))?;
            let mut b = Int16Builder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(set_val);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // FLOAT4 column — scalar is Float64 (from literal parsing), narrowed to f32.
        (DataType::Float32, ScalarValue::Float64(v)) => {
            let set_val = *v as f32;
            let arr = original
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| BasinError::internal("expected Float32Array for SET"))?;
            let mut b = Float32Builder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(set_val);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        (DataType::Utf8, ScalarValue::Utf8(v)) => {
            let arr = original
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("expected StringArray for SET"))?;
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for i in 0..n {
                if matched(i) {
                    b.append_value(v.as_str());
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        (DataType::Boolean, ScalarValue::Boolean(v)) => {
            let arr = original
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("expected BooleanArray for SET"))?;
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(*v);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        (DataType::Float64, ScalarValue::Float64(v)) => {
            let arr = original
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BasinError::internal("expected Float64Array for SET"))?;
            let mut b = Float64Builder::with_capacity(n);
            for i in 0..n {
                if matched(i) {
                    b.append_value(*v);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // TIMESTAMPTZ assignment, used by AUTO_UPDATE / SOFT DELETE
        // injection. The scalar is i64 microseconds since epoch.
        (DataType::Timestamp(TimeUnit::Microsecond, _), ScalarValue::Int64(v)) => {
            let arr = original
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    BasinError::internal("expected TimestampMicrosecondArray for SET")
                })?;
            let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_data_type(dt.clone());
            for i in 0..n {
                if matched(i) {
                    b.append_value(*v);
                } else if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(arr.value(i));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Cross-type assignments (e.g. SET id = '5') aren't supported in
        // v0.1. Mention both sides in the error so debugging is easy.
        (col_type, scalar) => Err(BasinError::InvalidSchema(format!(
            "UPDATE SET: cannot assign {scalar:?} to column of type {col_type:?}"
        ))),
    }
}

/// Right-hand side of an UPDATE assignment.
///
/// - `Scalar` — a literal or bind-param the user wrote (`SET x = 5`,
///   `SET x = $1`). Fast path: coerced once, splatted across matched rows.
/// - `Expr` — anything else (`SET x = x + 1`, `SET x = LOWER(x)`,
///   `SET x = NOW()`). The SQL text is re-rendered from the AST and run
///   as a DataFusion projection over the pre-update batch; the result
///   column is merged into matched rows via the predicate mask.
#[derive(Debug, Clone)]
pub(crate) enum AssignmentRhs {
    Scalar(ScalarValue),
    /// SQL text of the expression (e.g. `"id + 1"`). Re-rendered from the
    /// AST so the original user formatting / casing isn't preserved, but
    /// the semantics are.
    Expr(String),
}

fn parse_assignments(
    assignments: &[Assignment],
    schema: &Schema,
) -> Result<Vec<(usize, AssignmentRhs)>> {
    let mut out = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col_name = match &a.target {
            AssignmentTarget::ColumnName(name) => single_part_name(name)?.to_string(),
            AssignmentTarget::Tuple(_) => {
                return Err(BasinError::InvalidSchema(
                    "UPDATE SET (a, b) = ... not supported".into(),
                ));
            }
        };
        let idx = schema
            .index_of(&col_name)
            .map_err(|_| BasinError::InvalidSchema(format!("unknown column {col_name}")))?;
        // Direct writes to a generated column are forbidden — match PG's
        // SQLSTATE 42601 wording so ORM clients that key off that string
        // detect Basin generated columns identically.
        if crate::types::field_is_generated(schema.field(idx)).is_some() {
            return Err(BasinError::InvalidSchema(format!(
                "cannot insert into generated column {:?}",
                schema.field(idx).name()
            )));
        }
        // `GENERATED ALWAYS AS IDENTITY` columns reject direct UPDATE.
        // PG: SQLSTATE 428C9 ("column cannot be updated"). v0.1 doesn't
        // surface OVERRIDING on UPDATE (PG doesn't either — OVERRIDING
        // is INSERT-only), so an ALWAYS IDENTITY column is immutable
        // through UPDATE.
        if matches!(
            crate::types::field_identity_mode(schema.field(idx)),
            Some(crate::types::IdentityMode::Always)
        ) {
            return Err(BasinError::InvalidSchema(format!(
                "column {:?} can only be updated to DEFAULT (GENERATED ALWAYS AS IDENTITY)",
                schema.field(idx).name()
            )));
        }
        let dt = schema.field(idx).data_type().clone();
        // Try the fast literal path first. If the expression isn't a plain
        // literal (e.g. `col + 1`, `now()`, `upper(name)`) fall through to
        // the DataFusion expression-eval path.
        // Fast path: try to coerce the RHS as a literal. If `literal_to_scalar`
        // succeeds, we splat the scalar across matched rows directly. If it
        // returns `NotALiteral`, fall back to expression evaluation via
        // DataFusion. We deliberately don't surface coercion errors from
        // the fast path here — those would mask the expression fallback for
        // things like `SET id = (SELECT MAX(id) FROM u)`.
        let rhs = match try_literal_to_scalar(&a.value, &dt, &col_name)? {
            Some(scalar) => AssignmentRhs::Scalar(scalar),
            None => {
                // Scalar subqueries on the SET RHS are resolved to literals
                // before `parse_assignments` is called (see the pre-flush
                // resolution loop in exec_update). If a subquery node somehow
                // survives to this point it is correlated or unsupported — let
                // the DataFusion expression path surface a helpful error.
                AssignmentRhs::Expr(a.value.to_string())
            }
        };
        out.push((idx, rhs));
    }
    Ok(out)
}

/// Walk an expression and report whether any subquery node lurks inside.
/// Conservative: a free-standing `Subquery`, `Exists`, or `InSubquery`
/// counts. Used to gate the SET RHS fallback so the user gets a clear
/// error instead of DataFusion's "table __basin_gen_src missing"
/// noise.
fn contains_subquery(expr: &Expr) -> bool {
    use sqlparser::ast::Expr as E;
    match expr {
        E::Subquery(_) | E::Exists { .. } | E::InSubquery { .. } => true,
        E::Nested(inner) => contains_subquery(inner),
        E::UnaryOp { expr: inner, .. } => contains_subquery(inner),
        E::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        E::Cast { expr: inner, .. } => contains_subquery(inner),
        E::IsNull(inner) | E::IsNotNull(inner) => contains_subquery(inner),
        E::Function(_) | E::Identifier(_) | E::CompoundIdentifier(_) | E::Value(_) => false,
        _ => false,
    }
}

/// Return `true` when `expr` (a WHERE clause) contains at least one
/// `EXISTS (…)` or `NOT EXISTS (…)` node.  Used to route DELETE/UPDATE
/// through the DataFusion-decorrelation path before the custom predicate
/// engine (which can't represent correlated subqueries) sees the expression.
fn has_exists_subquery(expr: &Expr) -> bool {
    use sqlparser::ast::Expr as E;
    match expr {
        E::Exists { .. } => true,
        E::Nested(inner) => has_exists_subquery(inner),
        E::UnaryOp { expr: inner, .. } => has_exists_subquery(inner),
        E::BinaryOp { left, right, .. } => has_exists_subquery(left) || has_exists_subquery(right),
        _ => false,
    }
}

/// Translate a SQL literal expression into a `ScalarValue` matching the
/// destination column's data type. Mirrors the INSERT path's coercion
/// rules so `SET col = 5` and `INSERT (col) VALUES (5)` accept the same
/// input forms.
fn literal_to_scalar(expr: &Expr, dt: &DataType, col: &str) -> Result<ScalarValue> {
    match try_literal_to_scalar(expr, dt, col)? {
        Some(s) => Ok(s),
        None => Err(BasinError::InvalidSchema(format!(
            "UPDATE SET {col}: expected literal of type {dt:?}, got {expr}"
        ))),
    }
}

/// Like [`literal_to_scalar`] but returns `Ok(None)` instead of an error
/// when `expr` isn't a recognised literal form. Used by the UPDATE-SET
/// parser to decide between the fast scalar path and the DataFusion
/// expression fallback. NOTE: `try_*` still returns `Err(...)` for
/// malformed literals (bad timestamp string, etc.) — those would be
/// errors on the expression path too, so we don't paper them over.
fn try_literal_to_scalar(expr: &Expr, dt: &DataType, col: &str) -> Result<Option<ScalarValue>> {
    let (negated, inner) = peel_unary(expr);
    match (dt, inner) {
        // BIGINT / INT8 column — 64-bit signed.
        (DataType::Int64, Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(ScalarValue::Int64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        // INT / INTEGER / INT4 column — 32-bit. Store as Int64 so the SET
        // path can range-check and narrow back to i32.
        (DataType::Int32, Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(ScalarValue::Int64(if negated { -parsed } else { parsed })))
        }
        // SMALLINT / INT2 column — 16-bit. Store as Int64 for the same reason.
        (DataType::Int16, Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(ScalarValue::Int64(if negated { -parsed } else { parsed })))
        }
        // DOUBLE PRECISION / FLOAT8 column.
        (DataType::Float64, Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(Some(ScalarValue::Float64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        // REAL / FLOAT4 column — 32-bit. Store as Float64 so the SET path
        // can narrow to f32.
        (DataType::Float32, Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(Some(ScalarValue::Float64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        (DataType::Utf8, Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }))
        | (DataType::Utf8, Expr::Value(ValueWithSpan { value: Value::DoubleQuotedString(s), .. }))
        | (DataType::Utf8, Expr::Value(ValueWithSpan { value: Value::EscapedStringLiteral(s), .. }))
        | (DataType::Utf8, Expr::Value(ValueWithSpan { value: Value::NationalStringLiteral(s), .. })) => {
            if negated {
                Err(BasinError::InvalidSchema(format!(
                    "cannot negate string literal in SET {col} = {expr}"
                )))
            } else {
                Ok(Some(ScalarValue::Utf8(s.clone())))
            }
        }
        (DataType::Boolean, Expr::Value(ValueWithSpan { value: Value::Boolean(b), .. })) => {
            if negated {
                Err(BasinError::InvalidSchema(format!(
                    "cannot negate boolean literal in SET {col} = {expr}"
                )))
            } else {
                Ok(Some(ScalarValue::Boolean(*b)))
            }
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), _) => {
            // Literal timestamp forms only here. `now()` /
            // `current_timestamp` and the rest flow through the
            // expression fallback below, where DataFusion evaluates them
            // natively — basin-auth's email-verify, magic-link,
            // refresh-token-rotate, and password-reset flows all rely on
            // this round trip.
            let micros: i64 = match inner {
                Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. }) => s.parse::<i64>().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad timestamp literal {s:?}: {e}"))
                })?,
                Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })
                | Expr::Value(ValueWithSpan { value: Value::DoubleQuotedString(s), .. })
                | Expr::Value(ValueWithSpan { value: Value::EscapedStringLiteral(s), .. })
                | Expr::Value(ValueWithSpan { value: Value::NationalStringLiteral(s), .. }) => {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
                        .map_err(|e| {
                            BasinError::InvalidSchema(format!(
                                "bad RFC3339 timestamp {s:?} in SET {col}: {e}"
                            ))
                        })?
                }
                _ => return Ok(None),
            };
            Ok(Some(ScalarValue::Int64(if negated {
                -micros
            } else {
                micros
            })))
        }
        // Not a recognised literal form for this column type. Caller falls
        // back to expression evaluation.
        _ => Ok(None),
    }
}

/// Parse a WHERE clause into the [`CompoundPredicate`] tree the storage
/// layer evaluates. Atoms are `<col> OP <literal>` for `=, <, >, <=, >=`;
/// `<=` and `>=` synthesise to `Lt OR Eq` / `Gt OR Eq` respectively
/// because the storage atom enum doesn't carry them yet. Combinators:
/// `AND`, `OR`, `NOT`. Plus `IS NULL`, `IS NOT NULL`, `IN (lit, ...)`.
///
/// Anything we can't represent (function calls, subqueries, expressions
/// on both sides) becomes `InvalidSchema` so the user sees a clean error
/// rather than a partial mutation.
fn parse_compound_predicate(expr: &Expr, schema: &Schema) -> Result<CompoundPredicate> {
    match expr {
        Expr::Nested(inner) => parse_compound_predicate(inner, schema),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(CompoundPredicate::And(vec![
                parse_compound_predicate(left, schema)?,
                parse_compound_predicate(right, schema)?,
            ])),
            BinaryOperator::Or => Ok(CompoundPredicate::Or(vec![
                parse_compound_predicate(left, schema)?,
                parse_compound_predicate(right, schema)?,
            ])),
            BinaryOperator::Eq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::LtEq
            | BinaryOperator::GtEq => parse_atom(left, op, right, schema),
            BinaryOperator::NotEq => {
                let atom = parse_atom(left, &BinaryOperator::Eq, right, schema)?;
                Ok(CompoundPredicate::Not(Box::new(atom)))
            }
            other => Err(BasinError::InvalidSchema(format!(
                "WHERE operator {other:?} not supported"
            ))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(CompoundPredicate::Not(Box::new(parse_compound_predicate(
            inner, schema,
        )?))),
        Expr::IsNull(col) => {
            let name = identifier_or_err(col)?;
            schema
                .index_of(&name)
                .map_err(|_| BasinError::InvalidSchema(format!("unknown column {name}")))?;
            Ok(CompoundPredicate::IsNull(name))
        }
        Expr::IsNotNull(col) => {
            let name = identifier_or_err(col)?;
            schema
                .index_of(&name)
                .map_err(|_| BasinError::InvalidSchema(format!("unknown column {name}")))?;
            Ok(CompoundPredicate::IsNotNull(name))
        }
        Expr::InList {
            expr: col_expr,
            list,
            negated,
        } => {
            let name = identifier_or_err(col_expr)?;
            let idx = schema
                .index_of(&name)
                .map_err(|_| BasinError::InvalidSchema(format!("unknown column {name}")))?;
            let dt = schema.field(idx).data_type().clone();
            let mut values = Vec::with_capacity(list.len());
            for e in list {
                let v = literal_to_scalar(e, &dt, &name)?;
                values.push(v);
            }
            let inner = CompoundPredicate::In(name, values);
            if *negated {
                Ok(CompoundPredicate::Not(Box::new(inner)))
            } else {
                Ok(inner)
            }
        }
        other => Err(BasinError::InvalidSchema(format!(
            "WHERE clause not representable in v0.1: {other}"
        ))),
    }
}

fn parse_atom(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &Schema,
) -> Result<CompoundPredicate> {
    // `<col> OP <literal>` or the swapped `<literal> OP <col>`.
    let (col, op, lit) = if let (Some(c), Ok(Some(l))) = (as_identifier(left), as_literal(right)) {
        (c, op.clone(), l)
    } else if let (Some(c), Ok(Some(l))) = (as_identifier(right), as_literal(left)) {
        (c, mirror_op(op)?, l)
    } else {
        return Err(BasinError::InvalidSchema(format!(
            "WHERE atom must be `<col> OP <literal>`; got {left} {op:?} {right}"
        )));
    };
    schema
        .index_of(&col)
        .map_err(|_| BasinError::InvalidSchema(format!("unknown column {col}")))?;
    Ok(build_atom(&col, &op, lit))
}

fn build_atom(col: &str, op: &BinaryOperator, lit: ScalarValue) -> CompoundPredicate {
    match op {
        BinaryOperator::Eq => CompoundPredicate::Atom(Predicate::Eq(col.to_string(), lit)),
        BinaryOperator::Lt => CompoundPredicate::Atom(Predicate::Lt(col.to_string(), lit)),
        BinaryOperator::Gt => CompoundPredicate::Atom(Predicate::Gt(col.to_string(), lit)),
        // `<=` and `>=` synthesise as a disjunction so the storage atom
        // enum doesn't have to grow new variants for this path.
        BinaryOperator::LtEq => CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Lt(col.to_string(), lit.clone())),
            CompoundPredicate::Atom(Predicate::Eq(col.to_string(), lit)),
        ]),
        BinaryOperator::GtEq => CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Gt(col.to_string(), lit.clone())),
            CompoundPredicate::Atom(Predicate::Eq(col.to_string(), lit)),
        ]),
        // parse_atom only forwards the comparison operators; anything else
        // is a programmer error in this module.
        other => unreachable!("build_atom received non-comparison operator {other:?}"),
    }
}

fn mirror_op(op: &BinaryOperator) -> Result<BinaryOperator> {
    Ok(match op {
        BinaryOperator::Eq => BinaryOperator::Eq,
        BinaryOperator::NotEq => BinaryOperator::NotEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "unsupported operator {other:?} in WHERE atom"
            )));
        }
    })
}

fn as_identifier(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::Nested(inner) => as_identifier(inner),
        _ => None,
    }
}

fn identifier_or_err(e: &Expr) -> Result<String> {
    match e {
        Expr::Identifier(i) => Ok(i.value.clone()),
        Expr::Nested(inner) => identifier_or_err(inner),
        other => Err(BasinError::InvalidSchema(format!(
            "WHERE expects column identifier, got {other}"
        ))),
    }
}

/// Parse a literal RHS of a comparison in WHERE. Returns Ok(None) when
/// the expression isn't a recognisable literal so the caller can decide
/// whether to try the swapped form.
fn as_literal(e: &Expr) -> Result<Option<ScalarValue>> {
    let (negated, inner) = peel_unary(e);
    Ok(match inner {
        Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. }) => {
            // Try integer first; fall back to float if the literal has a `.`.
            if s.contains('.') || s.contains('e') || s.contains('E') {
                let v: f64 = s.parse().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}"))
                })?;
                Some(ScalarValue::Float64(if negated { -v } else { v }))
            } else {
                let v: i64 = s.parse().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
                })?;
                Some(ScalarValue::Int64(if negated { -v } else { v }))
            }
        }
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::DoubleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::EscapedStringLiteral(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::NationalStringLiteral(s), .. }) => {
            if negated {
                return Err(BasinError::InvalidSchema(
                    "cannot negate string literal in WHERE".into(),
                ));
            }
            Some(ScalarValue::Utf8(s.clone()))
        }
        Expr::Value(ValueWithSpan { value: Value::Boolean(b), .. }) => {
            if negated {
                return Err(BasinError::InvalidSchema(
                    "cannot negate boolean literal in WHERE".into(),
                ));
            }
            Some(ScalarValue::Boolean(*b))
        }
        _ => None,
    })
}

fn peel_unary(expr: &Expr) -> (bool, &Expr) {
    if let Expr::UnaryOp { op, expr: inner } = expr {
        match op {
            UnaryOperator::Minus => return (true, inner.as_ref()),
            UnaryOperator::Plus => return (false, inner.as_ref()),
            _ => {}
        }
    }
    (false, expr)
}

fn single_table_from_delete(d: &Delete) -> Result<TableName> {
    let tables = match &d.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    if tables.len() != 1 {
        return Err(BasinError::InvalidSchema("single-table DELETE only".into()));
    }
    let twj = &tables[0];
    if !twj.joins.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DELETE with JOIN not supported".into(),
        ));
    }
    let name = match &twj.relation {
        TableFactor::Table {
            name, args, ..
        } => {
            if args.is_some() {
                return Err(BasinError::InvalidSchema(
                    "DELETE target must be a bare table name".into(),
                ));
            }
            // Aliases are permitted: `DELETE FROM posts p WHERE p.col = ...`
            // The base table name is what we use for the actual delete; the
            // alias is used in the WHERE clause predicate and is passed through
            // to DataFusion via the SQL we construct in exec_delete_via_df_rowset.
            single_part_name(name)?.to_string()
        }
        _ => {
            return Err(BasinError::InvalidSchema(
                "DELETE target must be a simple table name".into(),
            ));
        }
    };
    TableName::new(name)
}

/// Extract the optional alias from a DELETE statement's FROM clause.
/// Returns the alias string if present, otherwise None.
fn delete_table_alias(d: &Delete) -> Option<String> {
    let tables = match &d.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let twj = tables.first()?;
    if let TableFactor::Table { alias, .. } = &twj.relation {
        alias.as_ref().map(|a| a.name.value.clone())
    } else {
        None
    }
}

fn single_part_name(name: &ObjectName) -> Result<&str> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified names not supported: {name}"
        )));
    }
    Ok(&name.0[0].id_val())
}

/// Append `(idx, AssignmentValue::Scalar(now_micros))` to `assignments` for
/// any AUTO_UPDATE column on `schema` the user didn't already explicitly set.
/// `now_micros` is captured once per UPDATE so every AUTO_UPDATE column
/// stamped in the same statement gets the same timestamp.
/// Append `(idx, AssignmentRhs::Scalar(Int64(now_micros)))` to
/// `assignments` for any AUTO_UPDATE column on `schema` the user didn't
/// already explicitly set. `now_micros` is captured once per UPDATE so
/// every AUTO_UPDATE column stamped in the same statement gets the same
/// timestamp.
fn inject_auto_update_assignments(
    schema: &Schema,
    assignments: &mut Vec<(usize, AssignmentRhs)>,
) {
    let now_micros = chrono::Utc::now().timestamp_micros();
    for (idx, field) in schema.fields().iter().enumerate() {
        if !crate::types::field_is_auto_update(field) {
            continue;
        }
        if assignments.iter().any(|(i, _)| *i == idx) {
            continue;
        }
        assignments.push((idx, AssignmentRhs::Scalar(ScalarValue::Int64(now_micros))));
    }
}

/// Translate `RowChange` (engine-internal pair) into the lifecycle
/// crate's [`AuditRecord`] so `lifecycle::write_audit_rows` is the
/// single place that knows the audit-table physical schema.
async fn write_audit_rows(
    sess: &ProjectSession,
    audit_table: &str,
    op: ChangeOp,
    rows: Vec<RowChange>,
) -> Result<()> {
    let records: Vec<AuditRecord> = rows
        .into_iter()
        .map(|r| AuditRecord {
            before: r.before,
            after: r.after,
        })
        .collect();
    crate::lifecycle::write_audit_rows(sess, audit_table, op, records).await
}

/// Soft-delete rewrite. Behaves like `UPDATE foo SET <sd_col> = now()
/// WHERE <user-pred> AND <sd_col> IS NULL`. The audit op (when
/// configured) is `'delete'` because that's what the user asked for —
/// the underlying write being an UPDATE is an implementation detail.
async fn exec_soft_delete(
    sess: &ProjectSession,
    table: TableName,
    schema: Arc<Schema>,
    predicate_expr: Option<&Expr>,
    sd_col: String,
    returning: Option<Vec<SelectItem>>,
) -> Result<ExecResult> {
    let storage = sess.engine.config().storage.clone();
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;

    // Build the assignments: just `<sd_col> = now()`. AUTO_UPDATE stamps
    // are skipped — soft-delete is a single-purpose stamp.
    let sd_idx = schema.index_of(&sd_col).map_err(|_| {
        BasinError::internal(format!("soft-delete column {sd_col:?} missing from schema"))
    })?;
    let now_micros = chrono::Utc::now().timestamp_micros();
    let assignments = vec![(sd_idx, AssignmentRhs::Scalar(ScalarValue::Int64(now_micros)))];

    // Compose the effective predicate: `<user-pred> AND <sd_col> IS NULL`.
    // Without the IS NULL guard a re-DELETE on already-soft-deleted rows
    // would re-stamp the column and re-emit an audit row.
    let mut pred = match predicate_expr {
        None => CompoundPredicate::IsNull(sd_col.clone()),
        Some(e) => {
            let user_pred = parse_compound_predicate(e, schema.as_ref())?;
            CompoundPredicate::And(vec![user_pred, CompoundPredicate::IsNull(sd_col.clone())])
        }
    };
    // Strip the trivial single-leg AND so pruning sees the same shape.
    if let CompoundPredicate::And(ref mut legs) = pred {
        if legs.len() == 1 {
            pred = legs.remove(0);
        }
    }

    let data_files = storage
        .list_data_files_with_stats(&sess.project, &table)
        .await?;
    if data_files.is_empty() {
        return Ok(empty_or_returning(
            "DELETE 0",
            schema.clone(),
            returning.as_deref(),
        ));
    }

    let audit_table = crate::types::audit_table_name(schema.as_ref()).map(|s| s.to_string());
    let want_returning_rows = returning.is_some();
    let capture_events = sinks_attached(sess) || audit_table.is_some() || want_returning_rows;

    let mut updated_total: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut event_payloads: Vec<RowChange> = Vec::new();
    // RETURNING input. For SOFT DELETE the row is conceptually deleted,
    // so we project the BEFORE state (pre-stamp) — matches what hard
    // DELETE RETURNING would expose.
    let mut returning_input: Vec<RecordBatch> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(Some(&pred), f, schema.as_ref());
        match outcome {
            PruneOutcome::NoMatch => {}
            PruneOutcome::AllMatch => {
                let catalog = &sess.engine.config().catalog;
                let befores = read_file_to_batches(&storage, &sess.project, &f.path).await?;
                let news =
                    apply_assignments_all(catalog, &sess.project, &befores, &assignments).await?;
                updated_total += f.row_count as usize;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(&befores, &news, None, &mut event_payloads)?;
                    if want_returning_rows {
                        for b in &befores {
                            returning_input.push(b.clone());
                        }
                    }
                }
                replacement_batches.extend(news);
            }
            PruneOutcome::Mixed => {
                let catalog = &sess.engine.config().catalog;
                let befores = read_file_to_batches(&storage, &sess.project, &f.path).await?;
                let mut masks = Vec::with_capacity(befores.len());
                let mut news = Vec::with_capacity(befores.len());
                let mut matched = 0usize;
                for b in &befores {
                    let mask = evaluate_compound(b, &pred).map_err(|e| {
                        BasinError::internal(format!("soft-delete predicate eval: {e}"))
                    })?;
                    matched += mask.iter().filter(|x| matches!(x, Some(true))).count();
                    news.push(
                        apply_assignments(catalog, &sess.project, b, &mask, &assignments).await?,
                    );
                    masks.push(mask);
                }
                if matched == 0 {
                    continue;
                }
                updated_total += matched;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(&befores, &news, Some(&masks), &mut event_payloads)?;
                    if want_returning_rows {
                        for (b, m) in befores.iter().zip(masks.iter()) {
                            let filtered = arrow_select::filter::filter_record_batch(b, m)
                                .map_err(|e| {
                                    BasinError::internal(format!(
                                        "filter soft-delete returning batch: {e}"
                                    ))
                                })?;
                            if filtered.num_rows() > 0 {
                                returning_input.push(filtered);
                            }
                        }
                    }
                }
                replacement_batches.extend(news);
            }
        }
    }

    if updated_total == 0 {
        return Ok(empty_or_returning(
            "DELETE 0",
            schema.clone(),
            returning.as_deref(),
        ));
    }

    // Audit row payloads need to look like a DELETE (before=row,
    // after=None) per the task contract, even though the underlying
    // write was an UPDATE.
    let audit_rows: Vec<RowChange> = if audit_table.is_some() {
        event_payloads
            .iter()
            .map(|r| RowChange {
                before: r.before.clone(),
                after: None,
            })
            .collect()
    } else {
        Vec::new()
    };

    let events = build_events(sess, &table, ChangeOp::Delete, event_payloads);
    dispatch_pre_commit(&sess.engine, &events).await?;
    let added_files = write_replacement(sess, &table, schema.clone(), replacement_batches).await?;
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        replaced_paths.clone(),
        added_files,
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);
    delete_objects(sess, &table, schema.as_ref(), &replaced_paths).await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    if let Some(audit) = audit_table.as_ref() {
        write_audit_rows(sess, audit, ChangeOp::Delete, audit_rows).await?;
    }

    if let Some(items) = returning.as_deref() {
        return project_returning(
            &sess.engine.config().catalog,
            &sess.project,
            schema.clone(),
            returning_input,
            items,
        )
        .await;
    }

    Ok(ExecResult::Empty {
        tag: format!("DELETE {updated_total}"),
    })
}

// ---------------------------------------------------------------------------
// RETURNING helpers
// ---------------------------------------------------------------------------

/// Build the projected schema for an empty RETURNING result.
#[allow(dead_code)]
fn schema_for_returning(table_schema: &Schema, items: &[SelectItem]) -> Result<Schema> {
    let col_indices = returning_column_indices(items, table_schema)?;
    let fields: Vec<Field> = col_indices
        .iter()
        .map(|&i| table_schema.field(i).clone())
        .collect();
    Ok(Schema::new(fields))
}

/// Resolve `RETURNING` items to column indices into `table_schema`.
#[allow(dead_code)]
fn returning_column_indices(items: &[SelectItem], schema: &Schema) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                // `*` — return all columns.
                indices.extend(0..schema.fields().len());
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let name = &ident.value;
                let idx = schema.index_of(name).map_err(|_| {
                    BasinError::InvalidSchema(format!("RETURNING: unknown column {name:?}"))
                })?;
                indices.push(idx);
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                ..
            } => {
                let name = &ident.value;
                let idx = schema.index_of(name).map_err(|_| {
                    BasinError::InvalidSchema(format!("RETURNING: unknown column {name:?}"))
                })?;
                indices.push(idx);
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "RETURNING: unsupported item {other}; only * and plain column names are \
                     supported"
                )));
            }
        }
    }
    Ok(indices)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── arrow_col_value_to_expr narrow-width regression (#66×#56) ────────────
    //
    // Before this fix, `arrow_col_value_to_expr` only handled Int64 / Float64 /
    // Utf8 / Boolean. After #66 started storing INTEGER columns as Int32Array
    // and SMALLINT as Int16Array, `resolve_subqueries_in_expr` would call
    // `arrow_col_value_to_expr` on the subquery result column and hit the
    // catch-all `other` arm, producing:
    //
    //   "IN (SELECT …): column type Int32 cannot be used as IN list element"
    //
    // This caused `UPDATE t SET id=1 WHERE id IN (SELECT id FROM u)` and
    // the NOT IN variant to be rejected as planner-errors (📜) even though
    // they were green (✅) before #66.

    /// Int32 column (INT4 / INTEGER after #66) round-trips through
    /// `arrow_col_value_to_expr` as a Number literal, not an error.
    #[test]
    fn arrow_col_value_to_expr_int32_positive() {
        use arrow_array::Int32Array;
        let arr = Int32Array::from(vec![42i32]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Int32 must not error");
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan { value: Value::Number(ref s, _), .. } = *v else {
                    panic!("expected Number literal, got {expr:?}");
                };
                assert_eq!(s, "42");
            }
            other => panic!("expected Value(Number), got {other:?}"),
        }
    }

    /// Negative Int32 renders as UnaryOp(Minus, Number(…)).
    #[test]
    fn arrow_col_value_to_expr_int32_negative() {
        use arrow_array::Int32Array;
        let arr = Int32Array::from(vec![-7i32]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("negative Int32 must not error");
        match expr {
            Expr::UnaryOp { op: UnaryOperator::Minus, ref expr } => {
                let Expr::Value(ref v) = **expr else {
                    panic!("expected Number inside UnaryOp, got {expr:?}");
                };
                let ValueWithSpan { value: Value::Number(ref s, _), .. } = *v else {
                    panic!("expected Number literal, got {v:?}");
                };
                assert_eq!(s, "7");
            }
            other => panic!("expected UnaryOp(Minus, Number), got {other:?}"),
        }
    }

    /// Int16 column (SMALLINT / INT2 after #66) round-trips correctly.
    #[test]
    fn arrow_col_value_to_expr_int16_positive() {
        use arrow_array::Int16Array;
        let arr = Int16Array::from(vec![100i16]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Int16 must not error");
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan { value: Value::Number(ref s, _), .. } = *v else {
                    panic!("expected Number literal, got {expr:?}");
                };
                assert_eq!(s, "100");
            }
            other => panic!("expected Value(Number), got {other:?}"),
        }
    }

    /// Float32 column (REAL / FLOAT4 after #66) round-trips correctly.
    #[test]
    fn arrow_col_value_to_expr_float32_positive() {
        use arrow_array::Float32Array;
        let arr = Float32Array::from(vec![3.14f32]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Float32 must not error");
        // The value is widened to f64 — just verify it's a Number and parses
        // back to something close to 3.14.
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan { value: Value::Number(ref s, _), .. } = *v else {
                    panic!("expected Number literal, got {expr:?}");
                };
                let parsed: f64 = s.parse().expect("must be numeric string");
                assert!((parsed - 3.14f64).abs() < 0.01, "got {parsed}");
            }
            other => panic!("expected Value(Number), got {other:?}"),
        }
    }

    /// Int64 still works as before (regression guard for the pre-#66 path).
    #[test]
    fn arrow_col_value_to_expr_int64_unchanged() {
        use arrow_array::Int64Array;
        let arr = Int64Array::from(vec![9_000_000_000i64]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Int64 must not error");
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan { value: Value::Number(ref s, _), .. } = *v else {
                    panic!("expected Number literal, got {expr:?}");
                };
                assert_eq!(s, "9000000000");
            }
            other => panic!("expected Value(Number), got {other:?}"),
        }
    }

    /// Unknown column type returns an error (the catch-all must still fire).
    #[test]
    fn arrow_col_value_to_expr_unsupported_type_errors() {
        use arrow_array::Date32Array;
        let arr = Date32Array::from(vec![0i32]);
        let err = arrow_col_value_to_expr(&arr, 0).expect_err("Date32 must error");
        let msg = err.to_string();
        assert!(
            msg.contains("cannot be used as IN list element"),
            "unexpected error: {msg}"
        );
    }

    // ── try_literal_to_scalar narrow-width regression (#66) ──────────────────
    //
    // Verify that the literal-to-scalar path added by #66 for INT4/INT2/FLOAT4
    // columns still produces the correct ScalarValue for SET col = <literal>.

    fn parse_expr(sql: &str) -> Expr {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let full = format!("SELECT {sql}");
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, &full).unwrap();
        match stmts.pop().unwrap() {
            sqlparser::ast::Statement::Query(q) => {
                match q.body.as_ref() {
                    sqlparser::ast::SetExpr::Select(s) => {
                        match &s.projection[0] {
                            sqlparser::ast::SelectItem::UnnamedExpr(e) => e.clone(),
                            other => panic!("not an expr: {other:?}"),
                        }
                    }
                    other => panic!("not a SELECT: {other:?}"),
                }
            }
            other => panic!("not a query: {other:?}"),
        }
    }

    /// `SET col = 5` where col is INT4 → ScalarValue::Int64(5).
    #[test]
    fn literal_to_scalar_int32_col_positive() {
        let expr = parse_expr("5");
        let sv = try_literal_to_scalar(&expr, &DataType::Int32, "id")
            .expect("should parse")
            .expect("should be Some");
        assert_eq!(sv, ScalarValue::Int64(5), "Int32 col literal → Int64(5)");
    }

    /// `SET col = -3` where col is INT2 → ScalarValue::Int64(-3).
    #[test]
    fn literal_to_scalar_int16_col_negative() {
        let expr = parse_expr("-3");
        let sv = try_literal_to_scalar(&expr, &DataType::Int16, "n")
            .expect("should parse")
            .expect("should be Some");
        assert_eq!(sv, ScalarValue::Int64(-3), "Int16 col literal → Int64(-3)");
    }

    /// `SET col = 1.5` where col is FLOAT4 → ScalarValue::Float64(1.5).
    #[test]
    fn literal_to_scalar_float32_col() {
        let expr = parse_expr("1.5");
        let sv = try_literal_to_scalar(&expr, &DataType::Float32, "x")
            .expect("should parse")
            .expect("should be Some");
        assert!(
            matches!(sv, ScalarValue::Float64(v) if (v - 1.5).abs() < 1e-6),
            "Float32 col literal → Float64(1.5), got {sv:?}"
        );
    }

    // ── scalar subquery resolution (#106) ────────────────────────────────────
    //
    // Unit tests that verify the helper utilities used by the new
    // `Expr::Subquery` arm in `resolve_subqueries_in_expr`.  The full
    // end-to-end path (executing a real sub-SELECT and substituting the result
    // into a SET assignment) is covered by the integration-test matrix; here
    // we verify the smaller building blocks that are independently testable.

    /// `contains_subquery` detects `Expr::Subquery` nodes.
    #[test]
    fn contains_subquery_detects_subquery_expr() {
        // Build a synthetic `Expr::Subquery(...)` via sqlparser to be sure we
        // match the right variant.  Parse `(SELECT 1)` as an expression — the
        // parser produces `Expr::Subquery(_)` for a standalone scalar subquery
        // in expression position.
        let expr = parse_expr("(SELECT 1)");
        assert!(
            contains_subquery(&expr),
            "Expr::Subquery should be detected by contains_subquery"
        );
    }

    /// `contains_subquery` returns false for plain literals and identifiers.
    #[test]
    fn contains_subquery_ignores_plain_literal() {
        let expr = parse_expr("42");
        assert!(
            !contains_subquery(&expr),
            "plain Number literal should not count as subquery"
        );
    }

    /// `contains_subquery` returns false for arithmetic expressions without
    /// subqueries.
    #[test]
    fn contains_subquery_ignores_arithmetic() {
        let expr = parse_expr("id + 1");
        assert!(
            !contains_subquery(&expr),
            "arithmetic expr should not count as subquery"
        );
    }

    /// `arrow_col_value_to_expr` handles the Utf8 (VARCHAR/TEXT) type —
    /// required for `SET name = (SELECT MAX(name) FROM u)` style queries.
    #[test]
    fn arrow_col_value_to_expr_utf8_string() {
        use arrow_array::StringArray;
        let arr = StringArray::from(vec!["hello"]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Utf8 must not error");
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan {
                    value: Value::SingleQuotedString(ref s),
                    ..
                } = *v
                else {
                    panic!("expected SingleQuotedString, got {expr:?}");
                };
                assert_eq!(s, "hello");
            }
            other => panic!("expected Value(SingleQuotedString), got {other:?}"),
        }
    }

    /// `arrow_col_value_to_expr` handles Boolean — required for
    /// `SET active = (SELECT bool_col FROM u LIMIT 1)`.
    #[test]
    fn arrow_col_value_to_expr_boolean_true() {
        use arrow_array::BooleanArray;
        let arr = BooleanArray::from(vec![true]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Boolean must not error");
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan {
                    value: Value::Boolean(b),
                    ..
                } = *v
                else {
                    panic!("expected Boolean literal, got {expr:?}");
                };
                assert!(b);
            }
            other => panic!("expected Value(Boolean), got {other:?}"),
        }
    }

    /// `contains_subquery` recursively detects subqueries inside `IN (SELECT …)`.
    #[test]
    fn contains_subquery_detects_in_subquery() {
        let expr = parse_expr("id IN (SELECT id FROM u)");
        assert!(
            contains_subquery(&expr),
            "Expr::InSubquery should be detected by contains_subquery"
        );
    }
}
