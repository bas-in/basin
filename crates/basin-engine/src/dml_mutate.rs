//! UPDATE / DELETE — copy-on-write DML.
//!
//! Iceberg v2 lets a snapshot record either copy-on-write (CoW) or
//! merge-on-read (MoR) row-level changes. v0.1 ships CoW only:
//!
//! 1. List every Parquet file in `(tenant, table)` *with stats*.
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
//! Out of scope for v0.1, marked TODO inline:
//! - Merge-on-read deletion vectors / position deletes. CoW is a fine
//!   default; MoR is a future optimisation.
//! - Multi-table UPDATE/DELETE, RETURNING, ORDER BY, LIMIT.
//! - Subquery / function-call WHERE.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use basin_catalog::DataFileRef;
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName};
use basin_storage::{
    evaluate_compound, evaluate_compound_for_pruning, vector_index_segment_key_for_data_file,
    CompoundPredicate, DataFile, Predicate, PruneOutcome, ScalarValue, Storage,
};
use futures::StreamExt;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, ObjectName, TableFactor,
    TableWithJoins, UnaryOperator, Value,
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
    sess: &TenantSession,
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
/// literal. Supports Int64, Utf8, Float64, Boolean.
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
                    expr: Box::new(Expr::Value(Value::Number(
                        (-v).to_string(),
                        false,
                    ))),
                })
            } else {
                Ok(Expr::Value(Value::Number(v.to_string(), false)))
            }
        }
        Dt::Utf8 => {
            let v = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("expected StringArray"))?
                .value(i);
            Ok(Expr::Value(Value::SingleQuotedString(v.to_string())))
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
                    expr: Box::new(Expr::Value(Value::Number(
                        (-v).to_string(),
                        false,
                    ))),
                })
            } else {
                Ok(Expr::Value(Value::Number(v.to_string(), false)))
            }
        }
        Dt::Boolean => {
            use arrow_array::BooleanArray;
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("expected BooleanArray"))?
                .value(i);
            Ok(Expr::Value(Value::Boolean(v)))
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
use crate::{ExecResult, TenantSession};

pub(crate) async fn exec_delete(sess: &TenantSession, delete: Delete) -> Result<ExecResult> {
    let table = single_table_from_delete(&delete)?;
    // Resolve any IN (SELECT …) subqueries in the WHERE clause to literal
    // lists before parsing. This must happen before `pre_mutation_flush` so
    // the subquery SELECT sees the most-recently-committed rows.
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
    if delete.returning.is_some() {
        return Err(BasinError::InvalidSchema(
            "DELETE ... RETURNING not supported".into(),
        ));
    }

    pre_mutation_flush(sess).await?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await?;
    let schema = meta.schema.clone();

    // SOFT DELETE rewrite: when the table has a SOFT DELETE column the
    // physical operation is an UPDATE that stamps that column with
    // `now()` and skips already-soft-deleted rows. The audit op (when
    // AUDIT TO is also configured) stays `'delete'` even though the
    // underlying write is an UPDATE — the user's intent matters here,
    // not the storage shape.
    if let Some(sd_col) = crate::types::soft_delete_column(schema.as_ref()) {
        return exec_soft_delete(sess, table, schema, predicate_expr, sd_col).await;
    }

    let storage = sess.engine.config().storage.clone();

    let data_files = storage
        .list_data_files_with_stats(&sess.tenant, &table)
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
    let capture_events = sinks_attached(sess) || audit_table.is_some();

    // Walk files: deletes can shortcut on AllMatch (drop the file outright).
    let mut deleted: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut dropped_paths: Vec<String> = Vec::new();
    // Only allocated when sinks are attached. Holds (before_row, after=None)
    // pairs for every actually-deleted row.
    let mut event_payloads: Vec<RowChange> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(pred.as_ref(), f, schema.as_ref());
        match (outcome, &pred) {
            // No predicate, or AllMatch: every row in this file is matched
            // and deleted. Drop the file outright.
            (PruneOutcome::AllMatch, Some(_)) | (_, None) => {
                deleted += f.row_count as usize;
                dropped_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_dropped_file(&storage, &sess.tenant, &f.path, &mut event_payloads)
                        .await?;
                }
            }
            (PruneOutcome::NoMatch, Some(_)) => {
                // Pass-through: file appears unchanged in the new snapshot.
            }
            (PruneOutcome::Mixed, Some(p)) => {
                let (kept, deleted_rows) = if capture_events {
                    evaluate_and_partition_delete_capturing(&storage, &sess.tenant, &f.path, p)
                        .await?
                } else {
                    let kept =
                        evaluate_and_partition_delete(&storage, &sess.tenant, &f.path, p).await?;
                    (kept, Vec::new())
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
                    for (b, row) in deleted_rows {
                        event_payloads.push(RowChange {
                            before: Some(build_row_json(&b, row)?),
                            after: None,
                        });
                    }
                }
            }
        }
    }

    if deleted == 0 {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
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
                .read_file(&sess.tenant, &object_store::path::Path::from(p.as_str()))
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
                .read_file(&sess.tenant, &object_store::path::Path::from(p.as_str()))
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
            &sess.tenant,
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
    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

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
            .load_table(&sess.tenant, &cd.child_table)
            .await?;
        let where_sql = crate::constraints::build_in_predicate_sql(
            &cd.rows,
            &cd.fk_columns,
            child_meta.schema.as_ref(),
        )?;
        let sql = format!("DELETE FROM {} WHERE {where_sql}", cd.child_table.as_str());
        Box::pin(sess.execute(&sql)).await?;
    }

    Ok(ExecResult::Empty {
        tag: format!("DELETE {deleted}"),
    })
}

pub(crate) async fn exec_update(
    sess: &TenantSession,
    table_with_joins: TableWithJoins,
    assignments: Vec<Assignment>,
    from: Option<TableWithJoins>,
    selection: Option<Expr>,
    returning: Option<Vec<sqlparser::ast::SelectItem>>,
) -> Result<ExecResult> {
    if from.is_some() {
        return Err(BasinError::InvalidSchema(
            "UPDATE ... FROM not supported".into(),
        ));
    }
    if returning.is_some() {
        return Err(BasinError::InvalidSchema(
            "UPDATE ... RETURNING not supported".into(),
        ));
    }
    if !table_with_joins.joins.is_empty() {
        return Err(BasinError::InvalidSchema(
            "UPDATE with JOIN not supported".into(),
        ));
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
    let table = TableName::new(table_name)?;

    // Resolve any IN (SELECT …) subqueries to literal lists before the flush
    // so the subquery SELECT sees the committed state.
    let selection = match selection {
        None => None,
        Some(e) => Some(resolve_subqueries_in_expr(sess, e).await?),
    };

    pre_mutation_flush(sess).await?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await?;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();

    // Resolve assignments to (column_index, scalar). Reject anything that
    // isn't a single-column = literal pair: UPDATE ... SET col = col + 1
    // requires expression evaluation we don't ship in v0.1.
    let mut assignments = parse_assignments(&assignments, schema.as_ref())?;
    // AUTO_UPDATE injection: any column flagged on the schema that the
    // user didn't explicitly set gets a fresh `now()` micros value.
    inject_auto_update_assignments(schema.as_ref(), &mut assignments);

    let data_files = storage
        .list_data_files_with_stats(&sess.tenant, &table)
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
    let capture_events = sinks_attached(sess) || audit_table.is_some() || has_generated_cols;

    // Walk files. Unlike DELETE, an AllMatch UPDATE still has to read the
    // file to apply SET to every row.
    let mut updated_total: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut event_payloads: Vec<RowChange> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(pred.as_ref(), f, schema.as_ref());
        match (outcome, &pred) {
            (PruneOutcome::NoMatch, Some(_)) => {
                // Pass-through. No read, no write.
            }
            // AllMatch with predicate, or no predicate at all: every row is
            // matched. We still need the file's contents to apply SET.
            (PruneOutcome::AllMatch, _) | (PruneOutcome::Mixed, None) => {
                let (mut new_batches, before_batches) = if capture_events {
                    let befores = read_file_to_batches(&storage, &sess.tenant, &f.path).await?;
                    let news = apply_assignments_all(&befores, &assignments)?;
                    (news, befores)
                } else {
                    let news = read_and_apply_assignments(
                        &storage,
                        &sess.tenant,
                        &f.path,
                        None,
                        &assignments,
                    )
                    .await?;
                    (news, Vec::new())
                };
                if has_generated_cols {
                    let mut rebuilt = Vec::with_capacity(new_batches.len());
                    for b in new_batches {
                        rebuilt.push(
                            crate::generated_cols::materialise_generated_columns(
                                &sess.engine.config().catalog,
                                &sess.tenant,
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
                }
                replacement_batches.extend(new_batches);
            }
            (PruneOutcome::Mixed, Some(p)) => {
                let (rows_matched, mut new_batches, before_batches, mask_per_batch) =
                    if capture_events {
                        let befores = read_file_to_batches(&storage, &sess.tenant, &f.path).await?;
                        let mut masks = Vec::with_capacity(befores.len());
                        let mut news = Vec::with_capacity(befores.len());
                        let mut matched = 0usize;
                        for b in &befores {
                            let mask = evaluate_compound(b, p).map_err(|e| {
                                BasinError::internal(format!("update predicate eval: {e}"))
                            })?;
                            matched += mask.iter().filter(|x| matches!(x, Some(true))).count();
                            news.push(apply_assignments(b, &mask, &assignments)?);
                            masks.push(mask);
                        }
                        (matched, news, befores, masks)
                    } else {
                        let (matched, news) = read_and_apply_assignments_mixed(
                            &storage,
                            &sess.tenant,
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
                                &sess.tenant,
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
                }
                replacement_batches.extend(new_batches);
            }
            // AllMatch + None handled above; this branch is unreachable in
            // practice but kept for the exhaustive match.
            (PruneOutcome::NoMatch, None) => unreachable!(),
        }
    }

    if updated_total == 0 {
        return Ok(ExecResult::Empty {
            tag: "UPDATE 0".into(),
        });
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
            &sess.tenant,
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
                &sess.tenant,
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
            &sess.tenant,
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
    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    if let Some(audit) = audit_table.as_ref() {
        write_audit_rows(sess, audit, ChangeOp::Update, audit_rows).await?;
    }

    Ok(ExecResult::Empty {
        tag: format!("UPDATE {updated_total}"),
    })
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
fn sinks_attached(sess: &TenantSession) -> bool {
    let guard = sess
        .engine
        .event_sinks()
        .read()
        .expect("event_sinks lock poisoned");
    registry_has_any(&guard)
}

fn build_events(
    sess: &TenantSession,
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
        let seq = sess.engine.next_event_seq(&sess.tenant, table);
        out.push(make_event(
            &sess.tenant,
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
/// DELETE.
async fn capture_dropped_file(
    storage: &Storage,
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
    out: &mut Vec<RowChange>,
) -> Result<()> {
    let mut stream = storage.read_file(tenant, path).await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            out.push(RowChange {
                before: Some(build_row_json(&batch, row)?),
                after: None,
            });
        }
    }
    Ok(())
}

/// Like [`evaluate_and_partition_delete`] but also returns the matched
/// rows (the ones being deleted). Each entry is `(batch, row_idx)` —
/// the caller materialises JSON lazily.
async fn evaluate_and_partition_delete_capturing(
    storage: &Storage,
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
) -> Result<(Vec<RecordBatch>, Vec<(RecordBatch, usize)>)> {
    let mut stream = storage.read_file(tenant, path).await?;
    let mut kept = Vec::new();
    let mut deleted = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("delete predicate eval: {e}")))?;
        for i in 0..batch.num_rows() {
            if !mask.is_null(i) && mask.value(i) {
                deleted.push((batch.clone(), i));
            }
        }
        let inverse = invert_mask(&mask);
        let kb = arrow_select::filter::filter_record_batch(&batch, &inverse)
            .map_err(|e| BasinError::internal(format!("delete filter batch: {e}")))?;
        if kb.num_rows() > 0 {
            kept.push(kb);
        }
    }
    Ok((kept, deleted))
}

/// Read a file into in-memory batches. Used by the event-capturing
/// UPDATE path so we can pair before/after row-by-row without re-reading.
async fn read_file_to_batches(
    storage: &Storage,
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(tenant, path).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// AllMatch UPDATE: every row in `befores` is matched. Returns the
/// post-SET batches.
fn apply_assignments_all(
    befores: &[RecordBatch],
    assignments: &[(usize, ScalarValue)],
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(befores.len());
    for b in befores {
        let mask = BooleanArray::from(vec![true; b.num_rows()]);
        out.push(apply_assignments(b, &mask, assignments)?);
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
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(tenant, path).await?;
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
    storage: &Storage,
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
    pred: Option<&CompoundPredicate>,
    assignments: &[(usize, ScalarValue)],
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(tenant, path).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = match pred {
            Some(p) => evaluate_compound(&batch, p)
                .map_err(|e| BasinError::internal(format!("update predicate eval: {e}")))?,
            None => BooleanArray::from(vec![true; batch.num_rows()]),
        };
        out.push(apply_assignments(&batch, &mask, assignments)?);
    }
    Ok(out)
}

/// Same as `read_and_apply_assignments` but also returns the count of
/// rows that actually matched the predicate. Used by the Mixed branch
/// where we need the count for the row tag and also have to detect the
/// "stats said maybe but nothing actually matched" no-op case.
async fn read_and_apply_assignments_mixed(
    storage: &Storage,
    tenant: &basin_common::TenantId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
    assignments: &[(usize, ScalarValue)],
) -> Result<(usize, Vec<RecordBatch>)> {
    let mut stream = storage.read_file(tenant, path).await?;
    let mut matched = 0usize;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("update predicate eval: {e}")))?;
        matched += mask.iter().filter(|b| matches!(b, Some(true))).count();
        out.push(apply_assignments(&batch, &mask, assignments)?);
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
    tenant: &basin_common::TenantId,
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
    let data_files = storage.list_data_files_with_stats(tenant, table).await?;
    let mut existing: HashSet<Vec<String>> = HashSet::new();
    for f in &data_files {
        if replaced.contains(f.path.as_ref()) {
            continue;
        }
        let mut stream = storage.read_file(tenant, &f.path).await?;
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

async fn pre_mutation_flush(sess: &TenantSession) -> Result<()> {
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
    sess: &TenantSession,
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
        .load_table(&sess.tenant, table)
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
        .write_batch_with_options(&sess.tenant, table, &part, &merged, &opts)
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
    sess: &TenantSession,
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
            &sess.tenant,
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
                .load_table(&sess.tenant, table)
                .await?;
            sess.engine
                .config()
                .catalog
                .replace_data_files(&sess.tenant, table, fresh.current_snapshot, removed, added)
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
    sess: &TenantSession,
    table: &TableName,
    schema: &Schema,
    paths: &[String],
) -> Result<()> {
    let storage = &sess.engine.config().storage;
    let store = storage.tenant_object_store(&sess.tenant);
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
                &sess.tenant,
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
/// assigned columns swapped out for the literal value; unmatched rows pass
/// through untouched. NULL mask entries are treated as unmatched (same
/// rationale as `invert_mask`).
fn apply_assignments(
    batch: &RecordBatch,
    mask: &BooleanArray,
    assignments: &[(usize, ScalarValue)],
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let original = batch.column(col_idx).clone();
        let assignment = assignments.iter().find(|(idx, _)| *idx == col_idx);
        let new_col = match assignment {
            None => original,
            Some((_, scalar)) => {
                let field = schema.field(col_idx);
                build_assigned_column(field.data_type(), &original, mask, scalar)?
            }
        };
        new_columns.push(new_col);
    }
    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| BasinError::internal(format!("rebuild batch after SET: {e}")))
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

fn parse_assignments(
    assignments: &[Assignment],
    schema: &Schema,
) -> Result<Vec<(usize, ScalarValue)>> {
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
        let dt = schema.field(idx).data_type().clone();
        let scalar = literal_to_scalar(&a.value, &dt, &col_name)?;
        out.push((idx, scalar));
    }
    Ok(out)
}

/// Translate a SQL literal expression into a `ScalarValue` matching the
/// destination column's data type. Mirrors the INSERT path's coercion
/// rules so `SET col = 5` and `INSERT (col) VALUES (5)` accept the same
/// input forms.
fn literal_to_scalar(expr: &Expr, dt: &DataType, col: &str) -> Result<ScalarValue> {
    let (negated, inner) = peel_unary(expr);
    match (dt, inner) {
        (DataType::Int64, Expr::Value(Value::Number(s, _))) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(ScalarValue::Int64(if negated { -parsed } else { parsed }))
        }
        (DataType::Float64, Expr::Value(Value::Number(s, _))) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(ScalarValue::Float64(if negated { -parsed } else { parsed }))
        }
        (DataType::Utf8, Expr::Value(Value::SingleQuotedString(s)))
        | (DataType::Utf8, Expr::Value(Value::DoubleQuotedString(s)))
        | (DataType::Utf8, Expr::Value(Value::EscapedStringLiteral(s)))
        | (DataType::Utf8, Expr::Value(Value::NationalStringLiteral(s))) => {
            if negated {
                Err(BasinError::InvalidSchema(format!(
                    "cannot negate string literal in SET {col} = {expr}"
                )))
            } else {
                Ok(ScalarValue::Utf8(s.clone()))
            }
        }
        (DataType::Boolean, Expr::Value(Value::Boolean(b))) => {
            if negated {
                Err(BasinError::InvalidSchema(format!(
                    "cannot negate boolean literal in SET {col} = {expr}"
                )))
            } else {
                Ok(ScalarValue::Boolean(*b))
            }
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), _) => {
            let micros = match inner {
                Expr::Value(Value::Number(s, _)) => s.parse::<i64>().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad timestamp literal {s:?}: {e}"))
                })?,
                Expr::Value(Value::SingleQuotedString(s))
                | Expr::Value(Value::DoubleQuotedString(s))
                | Expr::Value(Value::EscapedStringLiteral(s))
                | Expr::Value(Value::NationalStringLiteral(s)) => {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
                        .map_err(|e| {
                            BasinError::InvalidSchema(format!(
                                "bad RFC3339 timestamp {s:?} in SET {col}: {e}"
                            ))
                        })?
                }
                // `now()` / `current_timestamp` / `transaction_timestamp()`
                // — match the set we accept on the INSERT side
                // (`coerce_timestamp_micros` in `dml.rs`). Without this,
                // `UPDATE t SET ts = now()` fails — which basin-auth's
                // email-verify, magic-link, refresh-token-rotate, and
                // password-reset flows all need.
                Expr::Function(f) => {
                    let fname = f
                        .name
                        .0
                        .last()
                        .map(|i| i.value.to_ascii_lowercase())
                        .unwrap_or_default();
                    let args_empty = match &f.args {
                        sqlparser::ast::FunctionArguments::None => true,
                        sqlparser::ast::FunctionArguments::List(list) => list.args.is_empty(),
                        _ => false,
                    };
                    if !args_empty {
                        return Err(BasinError::InvalidSchema(format!(
                            "UPDATE SET {col}: timestamp function {fname}(...) takes no args"
                        )));
                    }
                    match fname.as_str() {
                        "now"
                        | "current_timestamp"
                        | "transaction_timestamp"
                        | "statement_timestamp"
                        | "clock_timestamp" => chrono::Utc::now().timestamp_micros(),
                        _ => {
                            return Err(BasinError::InvalidSchema(format!(
                                "UPDATE SET {col}: unsupported timestamp function {fname}()"
                            )));
                        }
                    }
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "UPDATE SET {col}: expected timestamp literal, got {other}"
                    )));
                }
            };
            Ok(ScalarValue::Int64(if negated { -micros } else { micros }))
        }
        (col_type, other) => Err(BasinError::InvalidSchema(format!(
            "UPDATE SET {col}: expected literal of type {col_type:?}, got {other}"
        ))),
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
        Expr::Value(Value::Number(s, _)) => {
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
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => {
            if negated {
                return Err(BasinError::InvalidSchema(
                    "cannot negate string literal in WHERE".into(),
                ));
            }
            Some(ScalarValue::Utf8(s.clone()))
        }
        Expr::Value(Value::Boolean(b)) => {
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
            name, alias, args, ..
        } => {
            if alias.is_some() || args.is_some() {
                return Err(BasinError::InvalidSchema(
                    "DELETE target must be a bare table name".into(),
                ));
            }
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

fn single_part_name(name: &ObjectName) -> Result<&str> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified names not supported: {name}"
        )));
    }
    Ok(&name.0[0].value)
}

/// Append `(idx, ScalarValue::Int64(now_micros))` to `assignments` for any
/// AUTO_UPDATE column on `schema` the user didn't already explicitly set.
/// `now_micros` is captured once per UPDATE so every AUTO_UPDATE column
/// stamped in the same statement gets the same timestamp.
fn inject_auto_update_assignments(schema: &Schema, assignments: &mut Vec<(usize, ScalarValue)>) {
    let now_micros = chrono::Utc::now().timestamp_micros();
    for (idx, field) in schema.fields().iter().enumerate() {
        if !crate::types::field_is_auto_update(field) {
            continue;
        }
        if assignments.iter().any(|(i, _)| *i == idx) {
            continue;
        }
        assignments.push((idx, ScalarValue::Int64(now_micros)));
    }
}

/// Translate `RowChange` (engine-internal pair) into the lifecycle
/// crate's [`AuditRecord`] so `lifecycle::write_audit_rows` is the
/// single place that knows the audit-table physical schema.
async fn write_audit_rows(
    sess: &TenantSession,
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
    sess: &TenantSession,
    table: TableName,
    schema: Arc<Schema>,
    predicate_expr: Option<&Expr>,
    sd_col: String,
) -> Result<ExecResult> {
    let storage = sess.engine.config().storage.clone();
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await?;

    // Build the assignments: just `<sd_col> = now()`. AUTO_UPDATE stamps
    // are skipped — soft-delete is a single-purpose stamp.
    let sd_idx = schema.index_of(&sd_col).map_err(|_| {
        BasinError::internal(format!("soft-delete column {sd_col:?} missing from schema"))
    })?;
    let now_micros = chrono::Utc::now().timestamp_micros();
    let assignments = vec![(sd_idx, ScalarValue::Int64(now_micros))];

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
        .list_data_files_with_stats(&sess.tenant, &table)
        .await?;
    if data_files.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
    }

    let audit_table = crate::types::audit_table_name(schema.as_ref()).map(|s| s.to_string());
    let capture_events = sinks_attached(sess) || audit_table.is_some();

    let mut updated_total: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    let mut event_payloads: Vec<RowChange> = Vec::new();

    for f in &data_files {
        let outcome = file_outcome(Some(&pred), f, schema.as_ref());
        match outcome {
            PruneOutcome::NoMatch => {}
            PruneOutcome::AllMatch => {
                let befores = read_file_to_batches(&storage, &sess.tenant, &f.path).await?;
                let news = apply_assignments_all(&befores, &assignments)?;
                updated_total += f.row_count as usize;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(&befores, &news, None, &mut event_payloads)?;
                }
                replacement_batches.extend(news);
            }
            PruneOutcome::Mixed => {
                let befores = read_file_to_batches(&storage, &sess.tenant, &f.path).await?;
                let mut masks = Vec::with_capacity(befores.len());
                let mut news = Vec::with_capacity(befores.len());
                let mut matched = 0usize;
                for b in &befores {
                    let mask = evaluate_compound(b, &pred).map_err(|e| {
                        BasinError::internal(format!("soft-delete predicate eval: {e}"))
                    })?;
                    matched += mask.iter().filter(|x| matches!(x, Some(true))).count();
                    news.push(apply_assignments(b, &mask, &assignments)?);
                    masks.push(mask);
                }
                if matched == 0 {
                    continue;
                }
                updated_total += matched;
                replaced_paths.push(f.path.as_ref().to_string());
                if capture_events {
                    capture_update_events(&befores, &news, Some(&masks), &mut event_payloads)?;
                }
                replacement_batches.extend(news);
            }
        }
    }

    if updated_total == 0 {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
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
    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    if let Some(audit) = audit_table.as_ref() {
        write_audit_rows(sess, audit, ChangeOp::Delete, audit_rows).await?;
    }

    Ok(ExecResult::Empty {
        tag: format!("DELETE {updated_total}"),
    })
}
