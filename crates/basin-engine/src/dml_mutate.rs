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

use crate::pg_ast::ObjectNamePartExt;
use object_store::ObjectStoreExt;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::DataFileRef;
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, ProjectId, Result, TableName};
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

/// Maximum number of rows we'll materialise from an `IN (SELECT …)` sub-SELECT
/// before refusing the rewrite. Beyond this, the literal-list rewrite is
/// O(N) memory both for the AST and for the per-row predicate eval, so we
/// stop the user with a typed error pointing at the JOIN form (which is
/// streamed by DataFusion) before they OOM the engine. Picked at 100k because
/// `IN (lit, lit, …)` lists under that size still parse + plan in <100ms in
/// our benchmarks; above that the predicate engine starts dominating.
pub(crate) const MAX_IN_SUBQUERY_ROWS: usize = 100_000;

pub(crate) async fn resolve_subqueries_in_expr(sess: &ProjectSession, expr: Expr) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: col_expr,
            subquery,
            negated,
        } => {
            let sql = subquery.to_string();
            let df =
                sess.ctx.sql(&sql).await.map_err(|e| {
                    crate::executor::map_df_plan_error("IN (SELECT …) – plan failed", &e)
                })?;
            let df_batches = df.collect().await.map_err(|e| {
                BasinError::internal(format!("IN (SELECT …) – execute failed: {e}"))
            })?;
            // Convert from DataFusion's internal arrow version to the workspace
            // arrow version so the column type-check in `arrow_col_value_to_expr`
            // sees the correct trait object.
            //
            // Bound the materialisation at `MAX_IN_SUBQUERY_ROWS` so a pathological
            // sub-SELECT (`WHERE id IN (SELECT id FROM huge_table)`) refuses early
            // with a clean message instead of OOM'ing the engine. The JOIN form
            // (`UPDATE t SET … FROM (SELECT id FROM huge_table) src WHERE t.id = src.id`)
            // streams through DataFusion and is the right shape for large inputs.
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
                    if list.len() >= MAX_IN_SUBQUERY_ROWS {
                        return Err(BasinError::InvalidSchema(format!(
                            "IN (SELECT …) sub-SELECT returned more than {} rows; \
                             rewrite as a JOIN (e.g. `UPDATE t SET … FROM (<subquery>) src \
                             WHERE t.<col> = src.<col>`) so DataFusion can stream the match",
                            MAX_IN_SUBQUERY_ROWS
                        )));
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
            let df =
                sess.ctx.sql(&sql).await.map_err(|e| {
                    crate::executor::map_df_plan_error("scalar subquery – plan failed", &e)
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

/// Parse a SQL boolean expression fragment (no leading `WHERE`) into a
/// sqlparser `Expr` by wrapping it in `SELECT <fragment>` and extracting the
/// projection. Used by the RLS USING-merge path in `exec_delete` to fold a
/// policy-built predicate string into the user's WHERE before per-file
/// predicate evaluation. Returns `InvalidSchema` if the fragment fails to
/// parse — that would indicate a malformed policy USING clause stored in the
/// catalog, which is a user-facing schema issue, not an internal bug.
fn parse_sql_expr_fragment(fragment: &str) -> Result<Expr> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let probe = format!("SELECT {fragment}");
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, &probe).map_err(|e| {
        BasinError::InvalidSchema(format!("could not parse RLS USING fragment {fragment:?}: {e}"))
    })?;
    let stmt = stmts.pop().ok_or_else(|| {
        BasinError::internal(format!("empty parse result for RLS fragment {fragment:?}"))
    })?;
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        other => {
            return Err(BasinError::internal(format!(
                "RLS fragment did not parse as Query: {other:?}"
            )));
        }
    };
    let select = match *query.body {
        sqlparser::ast::SetExpr::Select(s) => s,
        other => {
            return Err(BasinError::internal(format!(
                "RLS fragment body not a SELECT: {other:?}"
            )));
        }
    };
    let item = select.projection.into_iter().next().ok_or_else(|| {
        BasinError::internal(format!("RLS fragment {fragment:?} produced no projection"))
    })?;
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(e) => Ok(e),
        sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => Ok(expr),
        other => Err(BasinError::internal(format!(
            "RLS fragment projection not an Expr: {other:?}"
        ))),
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
                    expr: Box::new(Expr::Value(Value::Number((-v).to_string(), false).into())),
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
                    expr: Box::new(Expr::Value(Value::Number((-v).to_string(), false).into())),
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
                    expr: Box::new(Expr::Value(Value::Number((-v).to_string(), false).into())),
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
            Ok(Expr::Value(
                (Value::SingleQuotedString(v.to_string())).into(),
            ))
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
                    expr: Box::new(Expr::Value(Value::Number((-v).to_string(), false).into())),
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
                    expr: Box::new(Expr::Value(Value::Number((-v).to_string(), false).into())),
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

// ---------------------------------------------------------------------------
// Phase 5.19.E — GIN posting-list maintenance trait impl
// ---------------------------------------------------------------------------

/// Implement the storage-layer `GinRegistry` trait for `GinIndexRegistry` so
/// the `GinPostingListMaintainer` in `basin-storage` can drive maintenance
/// without a compile-time dependency on `basin-engine` types.
impl basin_storage::index::index_maint::GinRegistry
    for crate::index_probe::GinIndexRegistry
{
    fn remove_file(
        &self,
        project: &basin_common::ProjectId,
        table: &basin_common::TableName,
        col: &str,
        file_path: &str,
    ) {
        crate::index_probe::GinIndexRegistry::remove_file(self, project, table, col, file_path);
    }

    fn rebuild_file_entries(
        &self,
        project: &basin_common::ProjectId,
        table: &basin_common::TableName,
        col: &str,
        opclass: &str,
        batches: &[arrow_array::RecordBatch],
        new_file_path: &str,
    ) {
        crate::index_probe::GinIndexRegistry::rebuild_file_entries(
            self, project, table, col, opclass, batches, new_file_path,
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Hot-tier DELETE fast path (PR1 — bulk-DELETE-WHERE-IN write-amp fix)
// ─────────────────────────────────────────────────────────────────────────────
//
// For DELETE statements shaped as `pk_col = lit` or `pk_col IN (lit, …)` on
// tables with a single-column PRIMARY KEY and no surrounding feature usage
// (RLS, soft-delete, audit, sinks, secondary indexes, FK children), the
// engine writes `MemRowValue::Tombstone` entries into the process-wide
// `MemTableRegistry` keyed by encoded PK bytes — skipping the cold
// copy-on-write Parquet rewrite (`pre_mutation_flush` →
// `list_data_files_with_stats` → `evaluate_and_partition_delete` →
// `write_replacement` → `commit_replace` → `delete_objects` →
// `refresh_table`) that scales linearly with file size.
//
// Read-path semantics. The tombstones are recorded via
// `MemTable::delete` (the same path INSERTs / UPDATEs would use). The
// merge-on-read code in `basin_hottier::merge` already understands them
// (`tombstone_suppresses_cold_row`), but the fast-path point-lookup at
// `fast_select.rs:1167` currently ignores the `has_tombstone` flag
// returned by `probe_memtable`. Until that flag is acted upon (separate
// follow-up: wire `merge_scan` into `HtapUnionTable::scan`), a SELECT
// issued AFTER this fast-path DELETE may still observe the cold-tier
// row. The bulk-DELETE perf bench (`compare_postgres_10k`) measures only
// the DELETE latency and does not depend on the subsequent read; the
// post-flush compaction path will eventually drain tombstones into a new
// cold file, at which point reads become correct.
//
// Atomicity. The fast path runs only OUTSIDE an explicit transaction
// (`tx_is_active == false`). Inside a `BEGIN; … COMMIT/ROLLBACK` block
// we fall through to the slow copy-on-write path so ROLLBACK still
// works. PR2 (UPDATE-via-hot-tier) is expected to refactor `TxState` to
// carry pending tombstones; until then, in-tx DELETE keeps its existing
// semantics.

/// Try to express `expr` as `Some(values)` where `values` is the literal
/// list of `pk_col` values the predicate matches. Returns `None` when the
/// predicate isn't shaped as a pure PK equality / IN-list lookup.
///
/// Handled shapes (all on `pk_col` only):
///   * `pk_col = <lit>` / `<lit> = pk_col`              → `Some(vec![lit])`
///   * `pk_col IN (<lit>, …)`                            → `Some(vec![lits…])`
///   * `pk_col IN (<lit>, …) NEGATED` (i.e. NOT IN)      → `None` (slow path)
///   * `Expr::Nested(inner)`                             → recurse
///
/// Any other shape (composite atoms, AND/OR, range comparisons, non-PK
/// column references, function calls, sub-selects) yields `None` so the
/// caller falls back to the existing copy-on-write rewrite.
fn predicate_resolves_to_pk_list(
    expr: &Expr,
    pk_col: &str,
    table_name: &str,
) -> Option<Vec<Expr>> {
    match expr {
        Expr::Nested(inner) => predicate_resolves_to_pk_list(inner, pk_col, table_name),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            // `pk = lit` or `lit = pk` — only when the identifier resolves
            // to the table's single PK column.
            let lit_expr = if as_identifier(left, table_name)
                .map(|c| c.eq_ignore_ascii_case(pk_col))
                .unwrap_or(false)
                && is_simple_literal(right)
            {
                right.as_ref().clone()
            } else if as_identifier(right, table_name)
                .map(|c| c.eq_ignore_ascii_case(pk_col))
                .unwrap_or(false)
                && is_simple_literal(left)
            {
                left.as_ref().clone()
            } else {
                return None;
            };
            Some(vec![lit_expr])
        }
        Expr::InList {
            expr: col_expr,
            list,
            negated,
        } => {
            if *negated {
                return None;
            }
            let col = as_identifier(col_expr, table_name)?;
            if !col.eq_ignore_ascii_case(pk_col) {
                return None;
            }
            // Every list element must be a recognisable literal — bail
            // otherwise so the slow path can attempt richer expression
            // evaluation.
            for lit in list {
                if !is_simple_literal(lit) {
                    return None;
                }
            }
            Some(list.clone())
        }
        _ => None,
    }
}

/// Recognise the literal shapes `predicate_resolves_to_pk_list` accepts.
/// Mirrors the subset of `as_literal` (Number / String / Boolean, with an
/// optional leading unary `-` / `+`).
fn is_simple_literal(expr: &Expr) -> bool {
    let (_neg, inner) = peel_unary(expr);
    matches!(
        inner,
        Expr::Value(ValueWithSpan {
            value: Value::Number(_, _)
                | Value::SingleQuotedString(_)
                | Value::DoubleQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::Boolean(_),
            ..
        })
    )
}

/// Encode a single PK column value (already coerced to a `ScalarValue`)
/// into a `RowKey` whose lexicographic byte order matches the PK column
/// sort order — same encoding the cold-tier cluster sort uses, so a
/// future merge-on-read pass can match cold rows by encoded key.
///
/// Supported types: `Int64`, `Int32` (widened from `ScalarValue::Int64`
/// Decide whether the named hot-tier fast-path env var is active.
///
/// Phase 5.14 closure flipped these from default-OFF to default-ON. The
/// resolution order, highest precedence first:
///
///   1. **Global kill-switch** `BASIN_HOTTIER_FASTPATH_DISABLE=1` — forces
///      *every* hot-tier fast path off regardless of per-shape settings.
///      Operators use this to roll back without a redeploy.
///   2. **Per-shape override** (`BASIN_HOTTIER_DELETE_FASTPATH` or
///      `BASIN_HOTTIER_UPDATE_FASTPATH`):
///        * `0` — fast path off
///        * `1` — fast path on (the historical opt-in value still works)
///        * unset — fast path on (Phase 5.14 default)
///   3. **Default** — on.
///
/// Why an env var and not a config struct: shard processes spawn from a
/// single binary and the kill-switch needs to take effect on the next
/// SQL statement after the var is set, without restarting any process.
/// The lookup is cheap (a syscall returning a small string), and only
/// runs on the fast-path eligibility check — not in tight loops.
pub(crate) fn hottier_fastpath_enabled(per_shape_key: &str) -> bool {
    if std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").as_deref() == Ok("1") {
        return false;
    }
    match std::env::var(per_shape_key).as_deref() {
        Ok("0") => false,
        _ => true,
    }
}

/// Decide whether the in-transaction hot-tier UPDATE/DELETE fast path is
/// eligible for `table` in the current transaction.
///
/// Conservative gates (all must hold; any failure routes the statement to the
/// cold copy-on-write path, which is the correctness oracle):
///   * **No savepoint active.** `TxState::tx_overlay` cannot be rewound to a
///     savepoint watermark (RMW overwrites keys, so there is no length-based
///     snapshot like `pending_files`/`htap_rows`), so the overlay would not
///     honour `ROLLBACK TO SAVEPOINT`. See `tx_overlay_fastpath_blocked`.
///   * **No pending (cold-path) files for this table this tx.** A multi-row /
///     cold-path UPDATE/DELETE earlier in the same tx staged a rewritten file
///     in `pending_files`; the overlay's read-before-write does NOT consult
///     those pending files, so mixing the two on one table could read a stale
///     pre-image. Bail to cold for a consistent single materialisation.
///   * **No tx-buffered INSERT batches for this table this tx.** An in-tx
///     INSERT lands in `htap_rows` (not the shared memtable or cold tier), so
///     the overlay's read-before-write would not see a just-inserted row.
///     Bail so a same-tx INSERT-then-UPDATE/DELETE stays correct on the cold
///     path (which merges the htap tail).
///
/// Caller must already have checked `tx_is_active`.
pub(crate) fn tx_fastpath_eligible_for_table(
    sess: &ProjectSession,
    table: &TableName,
) -> bool {
    if crate::session::tx_overlay_fastpath_blocked(&sess.state) {
        return false;
    }
    if !crate::session::tx_pending_files_for(&sess.state, table).is_empty() {
        return false;
    }
    if !crate::session::tx_htap_batches_for(&sess.state, table).is_empty() {
        return false;
    }
    true
}

/// when the column type is Int32), `UInt64`, `Utf8`, `Boolean`. Anything
/// else returns `None` and the caller falls back to the slow path.
pub(crate) fn pk_scalar_to_row_key(
    val: &basin_storage::ScalarValue,
    col_dt: &DataType,
) -> Option<basin_hottier::RowKey> {
    use basin_storage::ScalarValue as S;
    let b = basin_hottier::RowKey::builder();
    Some(match (val, col_dt) {
        (S::Int64(v), DataType::Int64) => b.append_i64(*v).finish(),
        (S::Int64(v), DataType::Int32) => {
            let narrow = i32::try_from(*v).ok()?;
            b.append_i32(narrow).finish()
        }
        (S::Int64(v), DataType::Int16) => {
            let narrow = i16::try_from(*v).ok()?;
            b.append_i16(narrow).finish()
        }
        (S::UInt64(v), DataType::UInt64) => b.append_u64(*v).finish(),
        (S::Utf8(s), DataType::Utf8) | (S::Utf8(s), DataType::LargeUtf8) => {
            b.append_str(s).finish()
        }
        (S::Boolean(v), DataType::Boolean) => b.append_u8(if *v { 1 } else { 0 }).finish(),
        _ => return None,
    })
}

/// Decide whether the DELETE call site is eligible for the hot-tier fast
/// path. Returns `Ok(Some(pk_literals))` when every gate passes; returns
/// `Ok(None)` to signal the caller should run the existing slow path.
///
/// Gates (all must hold):
///   * Not inside an active explicit transaction (ROLLBACK semantics).
///   * Table has exactly one PK column (composite PKs fall through).
///   * Table has zero secondary indexes (would need maintenance on delete).
///   * Table has zero CHECK / FK / UNIQUE constraints (FK ON DELETE CASCADE
///     / NO ACTION semantics require the slow path).
///   * No child table references this one as a FK parent.
///   * RLS is disabled (USING-clause enforcement needs the row image).
///   * No soft-delete column, no audit table, no attached sinks.
///   * No RETURNING clause (we don't have the row image to project).
///   * The WHERE predicate is `pk = lit` or `pk IN (lits)` only.
///   * Every PK literal encodes to a `RowKey` (supported PK types).
/// Table-level gates shared by both DELETE hot-tier fast paths (the point /
/// `pk IN (…)` path in [`try_resolve_fast_path_pks`] and the `DELETE … USING`
/// join path in [`exec_delete_using`]). Returns true when writing tombstones
/// by PK is correctness-safe for THIS table, independent of the WHERE shape:
/// single-column PK, no RLS / soft-delete / audit / DELETE reactor, only
/// overlay-guarded secondary indexes (GIN and single-column btree are admitted
/// — see the index gate; GIST / vector / multi-col / expression decline), and
/// no other table FK-referencing this one. The predicate-shape and
/// context gates (fast-path enabled, in-tx eligibility, RETURNING) are checked
/// by each caller, since they are not table properties.
///
/// Keep this in lock-step with the inline gates the callers no longer duplicate
/// — relaxing a gate here relaxes it for every tombstone-by-PK path.
async fn delete_fastpath_table_eligible(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
) -> Result<bool> {
    // Single-column PK only — the RowKey encoders and the tombstone overlay key
    // on one column.
    if meta.pk_columns.len() != 1 {
        return Ok(false);
    }
    // RLS, soft-delete, audit all need the slow read+rewrite.
    if meta.rls_enabled {
        return Ok(false);
    }
    if crate::types::soft_delete_column(meta.schema.as_ref()).is_some() {
        return Ok(false);
    }
    if crate::types::audit_table_name(meta.schema.as_ref()).is_some() {
        return Ok(false);
    }
    // Per-table DELETE reactor subscribers need before/after row images
    // dispatched through the event pipeline; the fast path discards them. (We
    // use the per-table catalog list, not `sinks_attached`, because the engine
    // always attaches a `ReactorSink` dispatcher.)
    {
        let catalog = &sess.engine.config().catalog;
        let reactors = catalog.list_reactors(&sess.project).await;
        let has_reactor = reactors.iter().any(|r| {
            r.table.as_str().eq_ignore_ascii_case(table.as_str())
                && r.ops.contains(basin_catalog::ReactorOps::DELETE)
        });
        if has_reactor {
            return Ok(false);
        }
    }
    // Secondary indexes: ADMIT GIN-only and single-column B-tree tables,
    // exactly as the UPDATE twin does (see its `meta.indexes` gate). Both
    // read consumers now have an overlay-emptiness guard:
    //   * GIN/FTS: `apply_gin_pruning_for_query` /
    //     `apply_jsonb_posting_pruning_for_query` + the posting-probe `Empty`
    //     short-circuit fall back to the overlay-aware (TombstoneFilter) scan
    //     while `table_has_live_overlay`.
    //   * B-tree: `fast_select`'s secondary-index allowlist probe declines
    //     entirely while `table_has_live_overlay`, so a HIT never prunes to a
    //     cold-file set that an override/tombstone could escape.
    // And `materialize_overlay_for_table` re-maintains BOTH families on drain
    // (purge replaced files + re-register the replacement), so pruning
    // re-engages after a drain instead of leaking. GIST / vector (hnsw) still
    // keep the cold path — their readers have no overlay guard. Oracles:
    // `gin_overlay_delete.rs`, `btree_overlay_delete.rs`.
    if !meta.indexes.is_empty()
        && !meta.indexes.iter().all(|idx| {
            idx.access_method == "gin"
                || (idx.access_method == "btree"
                    && idx.columns.len() == 1
                    && !idx.columns[0].starts_with("expr:"))
        })
    {
        return Ok(false);
    }
    // Any *other* table referencing THIS one means CASCADE / NO ACTION must run
    // on the slow path.
    let referencing = crate::constraints::fks_referencing(
        &sess.engine.config().catalog,
        &sess.project,
        table.as_str(),
    )
    .await?;
    if !referencing.is_empty() {
        return Ok(false);
    }
    Ok(true)
}

async fn try_resolve_fast_path_pks(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    predicate: Option<&Expr>,
    returning: Option<&[SelectItem]>,
) -> Result<Option<Vec<basin_hottier::RowKey>>> {
    // Gate: hot-tier DELETE fast path. **Default ON** since Phase 5.14
    // closure (HtapUnionTable Update overlay landed in d8020f7; gate-matrix
    // and round-trip tests landed in 6cbe224). All read paths apply the
    // tombstone + UPDATE-override overlay so post-DELETE/UPDATE SELECT sees the
    // correct logical state:
    //   * the simple-SELECT point-lookup fast path
    //     (`fast_select::execute_simple_select`) merges overrides inline;
    //   * the transactional projection-scan path wraps the cold scan in
    //     `TombstoneFilterExec` + `UpdateOverlayExec` via `HtapUnionTable::scan`;
    //   * the non-transactional DataFusion read path reaches the SAME two
    //     overlay exec nodes through `session::register_cold_with_overlay`,
    //     which registers an empty-hot `HtapUnionTable` when the registry holds
    //     tombstones or overrides for the table.
    //
    // Operator overrides:
    //   * `BASIN_HOTTIER_FASTPATH_DISABLE=1` — global kill-switch, forces
    //     every hot-tier fast path off without a redeploy. Use this if
    //     production tracing surfaces a correctness regression.
    //   * `BASIN_HOTTIER_DELETE_FASTPATH=0` — disable just the DELETE
    //     fast path, leaving UPDATE on.
    //   * `BASIN_HOTTIER_DELETE_FASTPATH=1` (or unset) — DELETE fast path
    //     active. The historical opt-in value is still respected for
    //     operators with existing pinned configs.
    if !crate::dml_mutate::hottier_fastpath_enabled("BASIN_HOTTIER_DELETE_FASTPATH") {
        return Ok(None);
    }
    // Gate: explicit transaction.
    //
    // Auto-commit (`tx_is_active == false`): the fast path writes tombstones
    // straight to the shared `MemTableRegistry` (durable, no rollback needed).
    //
    // In-transaction (`tx_is_active == true`): we route to the tx-overlay
    // variant (`hot_tier_delete_by_pk_tx`) which writes to `TxState::tx_overlay`
    // instead, so ROLLBACK can drop it. But only when the in-tx fast path is
    // eligible — see `tx_fastpath_eligible_for_table`: savepoint-free AND no
    // multi-row / cold-path mutation already staged for this table this tx.
    if crate::session::tx_is_active(&sess.state) {
        if !tx_fastpath_eligible_for_table(sess, table) {
            return Ok(None);
        }
    }
    // Gate: RETURNING needs the deleted row's content; fast path discards it.
    if returning.is_some() {
        return Ok(None);
    }
    // Gate: table-level fast-path eligibility — single-column PK, no RLS /
    // soft-delete / audit / DELETE reactor / secondary index, no FK referencing
    // this table. Shared with the DELETE … USING join fast path so the two can
    // never diverge (`delete_fastpath_table_eligible`).
    if !delete_fastpath_table_eligible(sess, table, meta).await? {
        return Ok(None);
    }
    let pk_col = &meta.pk_columns[0];
    // Gate: WHERE must resolve to a pure pk = lit / pk IN (lits) shape.
    let Some(expr) = predicate else {
        // `DELETE FROM t` with no WHERE — would delete everything; that's a
        // perfectly valid fast-path shape but not one a typical bulk-DELETE
        // benchmark uses, and walking every cold-tier PK to tombstone it
        // would defeat the purpose. Stay on the slow path (which can drop
        // every file outright).
        return Ok(None);
    };
    let Some(lit_exprs) = predicate_resolves_to_pk_list(expr, pk_col, table.as_str()) else {
        return Ok(None);
    };
    // Empty IN-list → trivially zero matches.
    if lit_exprs.is_empty() {
        return Ok(Some(Vec::new()));
    }
    // Encode every PK literal to its RowKey form. Bail on any unsupported
    // type so the slow path can handle exotic PKs (e.g. UUID, INTERVAL).
    let pk_idx = meta
        .schema
        .index_of(pk_col)
        .map_err(|_| BasinError::internal(format!("PK column {pk_col:?} missing from schema")))?;
    let pk_dt = meta.schema.field(pk_idx).data_type().clone();
    let mut keys: Vec<basin_hottier::RowKey> = Vec::with_capacity(lit_exprs.len());
    for lit in &lit_exprs {
        let scalar = match try_literal_to_scalar(lit, &pk_dt, pk_col)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let Some(key) = pk_scalar_to_row_key(&scalar, &pk_dt) else {
            return Ok(None);
        };
        keys.push(key);
    }
    Ok(Some(keys))
}

/// Execute the resolved hot-tier DELETE: write a `Tombstone` per PK into
/// the process-wide `MemTableRegistry`. Returns the number of tombstones
/// written (which equals `keys.len()`).
///
/// Affected-row semantics. Postgres reports the number of rows that
/// *actually existed and were deleted*. The caller (`exec_delete`) now passes
/// ONLY the PKs that `resolve_present_pk_keys` confirmed resolve to a live row,
/// so `keys.len()` here is exactly that count — an absent / already-tombstoned
/// PK never reaches this function and is never tombstoned or counted. This
/// keeps the affected-row tag and the metadata `COUNT(*)` correction (which
/// subtracts one per live tombstone) both exact.
fn hot_tier_delete_by_pk(
    sess: &ProjectSession,
    table: &TableName,
    keys: Vec<basin_hottier::RowKey>,
) -> usize {
    let registry = sess.engine.memtable_registry();
    let entry = registry.get_or_create(sess.project, table.clone());
    let count = keys.len();
    for key in keys {
        entry.memtable.delete(key);
    }
    count
}

/// In-transaction variant of [`hot_tier_delete_by_pk`]: write tombstones into
/// this session's `TxState::tx_overlay` instead of the process-wide
/// `MemTableRegistry`, so ROLLBACK can drop them. On COMMIT the executor drains
/// the overlay into the shared registry (`tx_overlay_take_all` →
/// `entry.memtable.insert`), exactly mirroring the durability of the auto-commit
/// path (which is also registry-only — no WAL).
///
/// Affected-row semantics match the auto-commit path: the caller passes only
/// PKs confirmed live by `resolve_present_pk_keys`, so `keys.len()` is the true
/// number of rows deleted (an absent / already-tombstoned PK never reaches
/// here — see `hot_tier_delete_by_pk`'s doc-comment).
fn hot_tier_delete_by_pk_tx(
    sess: &ProjectSession,
    table: &TableName,
    keys: Vec<basin_hottier::RowKey>,
) -> usize {
    let count = keys.len();
    for key in keys {
        crate::session::tx_overlay_put(
            &sess.state,
            table,
            key,
            basin_hottier::MemRowValue::Tombstone,
        );
    }
    count
}

/// Read the FULL current row image for each `key`, in tier precedence (tx
/// overlay when `tx_mode` > shared memtable > cold), returning a map keyed by
/// encoded PK bytes. Used by the hot-tier DELETE fast path to capture
/// before-images for CDC / realtime change events WITHOUT promoting the row —
/// it is called only when a sink is attached (the zero-sink DELETE hot path
/// skips it). Keys that resolve to a tombstone (already deleted) or to no live
/// row are simply absent from the returned map (no before-image → no event).
///
/// This mirrors the read precedence and normalisation of the UPDATE fast path's
/// pre-image gather (`hot_tier_update_by_pk` §§1a/1b/2b): images are
/// `reattach_catalog_metadata`-normalised so `build_row_json` sees catalog-typed
/// arrays, and the cold tier is reached via the same per-file catalog probe +
/// single-key equality pushdown so a point DELETE pays O(matching files), not a
/// full scan.
/// Resolve which of `keys` correspond to a row that is currently LIVE, in tier
/// precedence (tx overlay when `tx_mode` > shared memtable > cold). A key that
/// resolves to a `Tombstone` (already deleted) or to no row at all is NOT live.
/// Returns the live subset in the original `keys` order, deduplicated.
///
/// This is the DELETE fast path's correctness gate: the path writes one
/// tombstone per requested PK and (historically) reported `keys.len()` and
/// subtracted one from `COUNT(*)` per tombstone — but a PK that matches no live
/// row must affect 0 rows and leave the count unchanged. By tombstoning ONLY
/// live keys, both the affected-row tag (`= live.len()`) and the metadata
/// `COUNT(*)` correction (which subtracts the tombstone count) become exact:
/// every tombstone written now shadows exactly one real row. Postgres reports
/// the number of rows that actually existed and were deleted; this matches it.
///
/// Cost mirrors `capture_pre_images_for_keys`: O(1) overlay/memtable probes
/// plus, for any key not resolved in RAM, a cold per-file PK-point probe
/// (`pk_point_probe` pruning + single-key equality pushdown) — O(matching
/// files), not a full scan. Re-uses that function's exact tier-precedence and
/// cold-probe machinery; keep the two in lock-step.
async fn resolve_present_pk_keys(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    keys: &[basin_hottier::RowKey],
    tx_mode: bool,
) -> Result<Vec<basin_hottier::RowKey>> {
    use std::collections::{HashMap, HashSet};
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();
    let registry = sess.engine.memtable_registry();
    let entry = registry.get_or_create(sess.project, table.clone());

    // Dedup the requested keys while preserving first-seen order: a repeated
    // `DELETE WHERE id = 5` issued twice in one IN-list must still tombstone /
    // count that key only once (it is a single live row).
    let mut order: Vec<basin_hottier::RowKey> = Vec::with_capacity(keys.len());
    let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(keys.len());
    for k in keys {
        if seen.insert(k.as_bytes().to_vec()) {
            order.push(k.clone());
        }
    }

    let want: HashSet<Vec<u8>> = seen;
    // Per requested-key liveness: `Some(true)` live, `Some(false)` definitively
    // tombstoned/absent (a higher tier answered), `None` still unresolved.
    let mut live: HashMap<Vec<u8>, bool> = HashMap::with_capacity(order.len());

    // 1a. Tx overlay (highest precedence) — only inside a tx.
    if tx_mode {
        for k in &order {
            let kb = k.as_bytes().to_vec();
            let Some(v) = crate::session::tx_overlay_get(&sess.state, table, k) else {
                continue;
            };
            let is_live = matches!(
                v,
                basin_hottier::MemRowValue::Row { .. } | basin_hottier::MemRowValue::Update { .. }
            );
            live.insert(kb, is_live);
        }
    }
    // 1b. Shared memtable for any key not resolved above.
    {
        let snap = entry.memtable.snapshot();
        for (k, v) in snap {
            let kb = k.as_bytes().to_vec();
            if !want.contains(&kb) || live.contains_key(&kb) {
                continue;
            }
            let is_live = matches!(
                v,
                basin_hottier::MemRowValue::Row { .. } | basin_hottier::MemRowValue::Update { .. }
            );
            live.insert(kb, is_live);
        }
    }

    // 2. Cold-tier fill for keys still unresolved (no overlay/memtable entry).
    let missing: Vec<&basin_hottier::RowKey> =
        order.iter().filter(|k| !live.contains_key(k.as_bytes())).collect();
    if !missing.is_empty() {
        let pk_col = &meta.pk_columns[0];
        let pk_idx = schema.index_of(pk_col).map_err(|_| {
            BasinError::internal(format!("PK column {pk_col:?} missing from schema"))
        })?;
        let pk_dt = schema.field(pk_idx).data_type().clone();

        let probe_scalars: Option<Vec<basin_storage::ScalarValue>> = missing
            .iter()
            .map(|k| pk_row_key_to_scalar(k, &pk_dt))
            .collect();

        let catalog_files = meta.live_data_files();
        let pruned_paths: Option<std::collections::HashSet<String>> = match probe_scalars.as_ref() {
            Some(scalars) => {
                let mut paths: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for scalar in scalars {
                    if let crate::index_probe::PkProbeOutcome::Candidates { paths: cands, .. } =
                        crate::index_probe::pk_point_probe(
                            pk_col,
                            scalar,
                            &catalog_files,
                            schema.as_ref(),
                        )
                    {
                        for p in cands {
                            paths.insert(p.to_string());
                        }
                    }
                }
                Some(paths)
            }
            None => None,
        };
        let single_eq: Option<basin_storage::Predicate> = match probe_scalars.as_deref() {
            Some([scalar]) => Some(basin_storage::Predicate::Eq(pk_col.clone(), scalar.clone())),
            _ => None,
        };
        // Keys still unresolved after the cold scan resolve to "absent".
        let missing_bytes: HashSet<Vec<u8>> =
            missing.iter().map(|k| k.as_bytes().to_vec()).collect();
        let mut found_cold: HashSet<Vec<u8>> = HashSet::new();

        let data_files = storage.list_data_files_with_stats(&sess.project, table).await?;
        let data_files = filter_to_live_data_files(sess, table, data_files).await?;
        'files: for f in &data_files {
            if found_cold.len() == missing_bytes.len() {
                break 'files;
            }
            if let Some(ref allow) = pruned_paths {
                if !allow.contains(f.path.as_ref()) {
                    continue;
                }
            }
            let mut stream = match single_eq.as_ref() {
                Some(pred) => {
                    let opts = basin_storage::ReadOptions {
                        filters: vec![pred.clone()],
                        ..Default::default()
                    };
                    storage
                        .read_file_with_options(&sess.project, &f.path, opts)
                        .await?
                }
                None => storage.read_file(&sess.project, &f.path).await?,
            };
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let pk_array = batch.column(pk_idx);
                for row in 0..batch.num_rows() {
                    let Some(rk) =
                        crate::hot_tombstone::array_value_to_row_key(pk_array.as_ref(), row, &pk_dt)
                    else {
                        continue;
                    };
                    let kb = rk.as_bytes().to_vec();
                    if missing_bytes.contains(&kb) {
                        found_cold.insert(kb);
                    }
                }
            }
        }
        for k in &missing {
            let kb = k.as_bytes().to_vec();
            live.insert(kb.clone(), found_cold.contains(&kb));
        }
    }

    Ok(order
        .into_iter()
        .filter(|k| *live.get(k.as_bytes()).unwrap_or(&false))
        .collect())
}

async fn capture_pre_images_for_keys(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    keys: &[basin_hottier::RowKey],
    tx_mode: bool,
) -> Result<std::collections::HashMap<Vec<u8>, RecordBatch>> {
    use std::collections::HashMap;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();
    let registry = sess.engine.memtable_registry();
    let entry = registry.get_or_create(sess.project, table.clone());

    let want: std::collections::HashSet<Vec<u8>> =
        keys.iter().map(|k| k.as_bytes().to_vec()).collect();
    let mut current: HashMap<Vec<u8>, RecordBatch> = HashMap::new();

    // 1a. Tx overlay (highest precedence) — only inside a tx. A Tombstone means
    //     the row was already deleted earlier in this tx: record an empty-row
    //     sentinel so the lower tiers don't resurrect a pre-image for it (and
    //     the caller emits no event for it).
    if tx_mode {
        for key in keys {
            let kb = key.as_bytes().to_vec();
            let Some(v) = crate::session::tx_overlay_get(&sess.state, table, key) else {
                continue;
            };
            match v {
                basin_hottier::MemRowValue::Row { bytes, .. }
                | basin_hottier::MemRowValue::Update { bytes, .. } => {
                    if let Some(rb) = decode_ipc_single_row(&bytes) {
                        current.insert(kb, reattach_catalog_metadata(schema.as_ref(), rb)?);
                    }
                }
                basin_hottier::MemRowValue::Tombstone => {
                    current.insert(kb, RecordBatch::new_empty(schema.clone()));
                }
            }
        }
    }
    // 1b. Shared memtable for any PK not resolved above.
    {
        let snap = entry.memtable.snapshot();
        for (k, v) in snap {
            let kb = k.as_bytes().to_vec();
            if !want.contains(&kb) || current.contains_key(&kb) {
                continue;
            }
            match v {
                basin_hottier::MemRowValue::Row { bytes, .. }
                | basin_hottier::MemRowValue::Update { bytes, .. } => {
                    if let Some(rb) = decode_ipc_single_row(&bytes) {
                        current.insert(kb, reattach_catalog_metadata(schema.as_ref(), rb)?);
                    }
                }
                basin_hottier::MemRowValue::Tombstone => {
                    current.insert(kb, RecordBatch::new_empty(schema.clone()));
                }
            }
        }
    }

    // 2. Cold-tier fill for PKs still unresolved, using the same per-file
    //    catalog probe + single-key pushdown the UPDATE fast path uses.
    let missing: Vec<&basin_hottier::RowKey> = keys
        .iter()
        .filter(|k| !current.contains_key(k.as_bytes()))
        .collect();
    if !missing.is_empty() {
        let pk_col = &meta.pk_columns[0];
        let pk_idx = schema.index_of(pk_col).map_err(|_| {
            BasinError::internal(format!("PK column {pk_col:?} missing from schema"))
        })?;
        let pk_dt = schema.field(pk_idx).data_type().clone();

        let probe_scalars: Option<Vec<basin_storage::ScalarValue>> = missing
            .iter()
            .map(|k| pk_row_key_to_scalar(k, &pk_dt))
            .collect();

        let catalog_files = meta.live_data_files();
        let pruned_paths: Option<std::collections::HashSet<String>> = match probe_scalars.as_ref() {
            Some(scalars) => {
                let mut paths: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for scalar in scalars {
                    if let crate::index_probe::PkProbeOutcome::Candidates { paths: cands, .. } =
                        crate::index_probe::pk_point_probe(
                            pk_col,
                            scalar,
                            &catalog_files,
                            schema.as_ref(),
                        )
                    {
                        for p in cands {
                            paths.insert(p.to_string());
                        }
                    }
                }
                Some(paths)
            }
            None => None,
        };
        let single_eq: Option<basin_storage::Predicate> = match probe_scalars.as_deref() {
            Some([scalar]) => Some(basin_storage::Predicate::Eq(pk_col.clone(), scalar.clone())),
            _ => None,
        };

        let data_files = storage.list_data_files_with_stats(&sess.project, table).await?;
        let data_files = filter_to_live_data_files(sess, table, data_files).await?;
        'files: for f in &data_files {
            if current.len() == want.len() {
                break 'files;
            }
            if let Some(ref allow) = pruned_paths {
                if !allow.contains(f.path.as_ref()) {
                    continue;
                }
            }
            let mut stream = match single_eq.as_ref() {
                Some(pred) => {
                    let opts = basin_storage::ReadOptions {
                        filters: vec![pred.clone()],
                        ..Default::default()
                    };
                    storage
                        .read_file_with_options(&sess.project, &f.path, opts)
                        .await?
                }
                None => storage.read_file(&sess.project, &f.path).await?,
            };
            while let Some(batch) = stream.next().await {
                let batch = reattach_catalog_metadata(schema.as_ref(), batch?)?;
                let pk_array = batch.column(pk_idx);
                for row in 0..batch.num_rows() {
                    let Some(rk) =
                        crate::hot_tombstone::array_value_to_row_key(pk_array.as_ref(), row, &pk_dt)
                    else {
                        continue;
                    };
                    let kb = rk.as_bytes().to_vec();
                    if want.contains(&kb) && !current.contains_key(&kb) {
                        current.insert(kb, batch.slice(row, 1));
                    }
                }
            }
        }
    }

    Ok(current)
}

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

    // ── Hot-tier fast path ──────────────────────────────────────────────
    //
    // For pk = lit / pk IN (lits) DELETEs on a table that satisfies every
    // gate in `try_resolve_fast_path_pks`, write tombstones directly to
    // the process-wide MemTableRegistry and skip the copy-on-write rewrite
    // (`pre_mutation_flush`, `list_data_files_with_stats`,
    // `evaluate_and_partition_delete`, `write_replacement`,
    // `commit_replace`, `delete_objects`, `refresh_table`). This is the
    // bulk-DELETE-WHERE-IN write-amp fix (decisions.md 2026-05-23) — was
    // 116-18000x slower than PG; closes the gap to ~point-write latency.
    //
    // Flush the INSERT tail only when one exists, BEFORE the fast-path gate
    // so a just-INSERTed-same-row is visible to BOTH the fast-path read and
    // the cold read. The cold `materialize_hot_overlay_into_cold` is moved to
    // the cold fall-through below — the fast path writes tombstones by PK that
    // shadow any prior overlay on read, so it does not need materializing (#205).
    pre_mutation_flush_if_tail(sess, &table).await?;

    // Load the catalog metadata once here so the gates can inspect PK /
    // RLS / indexes / FKs. On the slow path the same load happens below.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();
    if let Some(pk_keys) = try_resolve_fast_path_pks(
        sess,
        &table,
        &meta,
        resolved_selection.as_ref(),
        returning.as_deref(),
    )
    .await?
    {
        let tx_mode = crate::session::tx_is_active(&sess.state);
        // CORRECTNESS: tombstone (and count) ONLY the requested PKs that actually
        // resolve to a LIVE row right now, in tier precedence (tx overlay >
        // shared memtable > cold). A PK that matches no live row — including one
        // far outside the seeded range, or one already deleted — must affect 0
        // rows and leave `COUNT(*)` unchanged. Writing a tombstone for an absent
        // key both over-reported the affected-row tag (it returned the requested
        // count) and corrupted the metadata `COUNT(*)` fast path (which subtracts
        // one per live tombstone): a phantom tombstone dropped the count by one
        // even though no real row was removed. Resolving liveness first makes
        // every tombstone we write shadow exactly one real row, so the tag and
        // the count correction are both exact (mirrors the UPDATE fast path,
        // which already reports only PKs that resolved to an existing row).
        let live_keys = resolve_present_pk_keys(sess, &table, &meta, &pk_keys, tx_mode).await?;

        // Change-event capture: the DELETE fast path writes tombstones by PK and
        // never reads the row — so to emit before-images we read them HERE, but
        // ONLY when a sink is attached (lazy: the zero-sink OLTP DELETE hot path
        // skips this read entirely and stays at point-write latency). The read
        // is tier-precedence (tx overlay > shared memtable > cold), overlay-aware
        // so a prior hot-tier UPDATE on the same PK is reflected in the captured
        // before-image. Done BEFORE the tombstone write so the row is still live.
        // Only the live keys can have a before-image (absent keys contribute no
        // event), so we capture for those.
        let delete_pre_images: std::collections::HashMap<Vec<u8>, RecordBatch> =
            if post_commit_sink_attached(sess) && !live_keys.is_empty() {
                capture_pre_images_for_keys(sess, &table, &meta, &live_keys, tx_mode).await?
            } else {
                std::collections::HashMap::new()
            };

        // In-tx → write to the tx overlay (rollback-able); auto-commit → write
        // to the shared registry. The gate (`try_resolve_fast_path_pks`) only
        // admits the in-tx case when `tx_fastpath_eligible_for_table` held.
        let n = if tx_mode {
            hot_tier_delete_by_pk_tx(sess, &table, live_keys)
        } else {
            hot_tier_delete_by_pk(sess, &table, live_keys)
        };
        // Empty IN-list / zero-row matches still return DELETE 0.
        if n == 0 {
            return Ok(empty_or_returning(
                "DELETE 0",
                schema.clone(),
                returning.as_deref(),
            ));
        }
        // Post-commit (the tombstone write IS this fast path's commit) emit /
        // buffer one DELETE event per captured before-image (after = None). The
        // map is empty unless a sink was attached, so this is a single
        // `is_empty` branch on the hot path. A captured PK that had no live row
        // contributes no event (it had no before-image), which matches the cold
        // path's "events only for rows that existed" semantics even though the
        // affected-row tag counts requested PKs.
        if !delete_pre_images.is_empty() {
            let mut rows: Vec<RowChange> = Vec::with_capacity(delete_pre_images.len());
            for pre in delete_pre_images.values() {
                if pre.num_rows() == 0 {
                    continue; // PK matched no live row → no event.
                }
                rows.push(RowChange {
                    before: Some(build_row_json(pre, 0)?),
                    after: None,
                });
            }
            emit_or_buffer_change_events(sess, &table, ChangeOp::Delete, rows, tx_mode);
        }
        return Ok(ExecResult::Empty {
            tag: format!("DELETE {n}"),
        });
    }

    // ── Cold copy-on-write path ─────────────────────────────────────────
    //
    // The cold rewrite reads the RAW base via `list_data_files_with_stats`,
    // which is NOT overlay-aware: a prior hot-tier UPDATE/DELETE override
    // would not be seen, so without materializing it first the rewrite would
    // operate on stale cold data while the un-cleared overlay shadows the
    // result on read (#94/#95). Materialize the overlay into cold BEFORE we
    // list the base files. This runs only on the cold fall-through (the fast
    // path above never reaches here), so a delete-heavy fast-path loop no
    // longer pays the eager re-encode + commit on every DELETE (#205). The
    // INSERT-tail flush already happened above (gated on `has_pending_tail`).
    materialize_hot_overlay_into_cold(sess, &table).await?;
    // Re-load metadata: materialize advances the table snapshot, so `meta`
    // (and the schema derived from it) must reflect the post-materialize state
    // before any cold read/RLS/commit decision.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();

    // RLS USING enforcement on DELETE (P0 #56 fix). UPDATE's exec_update
    // already enforces RLS WITH CHECK on the post-SET image, which trips
    // when an UPDATE-without-WHERE tries to overwrite a row the current
    // user doesn't own. DELETE has no post-image, so without this guard a
    // `DELETE FROM t` issued by user A would drop every row in the file,
    // including ones policy-owned by B — bypassing RLS entirely.
    //
    // We mirror the SELECT path semantically: load the table's policies,
    // combine the applicable USING predicates (permissive → OR), and AND
    // the result into the user's WHERE before parsing the compound
    // predicate. With no applicable policy under rls_enabled, the helper
    // returns `(FALSE)` (Postgres default-deny). When rls is disabled it
    // returns `None` and the existing fast path is untouched.
    let rls_using_sql = crate::rls::build_using_predicate_sql_for_kind(
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Delete,
    );
    let effective_selection: Option<Expr> = match (resolved_selection, rls_using_sql) {
        (sel, None) => sel,
        (None, Some(rls_sql)) => Some(parse_sql_expr_fragment(&rls_sql)?),
        (Some(user), Some(rls_sql)) => {
            let rls_expr = parse_sql_expr_fragment(&rls_sql)?;
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(user))),
                op: BinaryOperator::And,
                right: Box::new(Expr::Nested(Box::new(rls_expr))),
            })
        }
    };
    let predicate_expr = effective_selection.as_ref();

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
    // Defense-in-depth (#94/#95): a fire-and-forget physical delete may
    // not have completed by the time we re-list, but the catalog is
    // authoritative — intersect with `live_data_files()` so any stale
    // on-disk file the lister returned is filtered out here, before any
    // read decision is made on it. Re-load the meta because earlier
    // helpers (pre_mutation_flush / materialize_hot_overlay_into_cold)
    // may have advanced the snapshot since `meta` was first loaded.
    let data_files = filter_to_live_data_files(sess, &table, data_files).await?;
    if data_files.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
    }

    let pred = match predicate_expr {
        None => None,
        Some(e) => Some(parse_compound_predicate(e, schema.as_ref(), table.as_str())?),
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
                        schema.as_ref(),
                    )
                    .await?
                } else {
                    let (kept, decoded_total) = evaluate_and_partition_delete(
                        &storage,
                        &sess.project,
                        &f.path,
                        p,
                        schema.as_ref(),
                    )
                    .await?;
                    // ROW-CONSERVATION TRIPWIRE (1B-audit finding, CoW
                    // variant): `removed` below is INFERRED as
                    // catalog_rows − kept_rows, so a read that decodes this
                    // file as empty/partial (poisoned cached decode /
                    // truncated GET — under investigation) silently counts
                    // every unseen row as "deleted" and the replace destroys
                    // them (observed: a 3.9M-row file rewritten to nothing
                    // by a 100k-key purge DELETE). The decode must account
                    // for every catalog row before we may subtract.
                    if decoded_total != f.row_count {
                        return Err(BasinError::internal(format!(
                            "DELETE CoW: file {} decoded {decoded_total} rows but catalog \
                             says {} — refusing rewrite (would destroy rows); retry",
                            f.path, f.row_count
                        )));
                    }
                    (kept, Vec::new(), None)
                };
                let kept_rows: usize = kept.iter().map(|b| b.num_rows()).sum();
                // Same conservation tripwire for the CAPTURING path: its
                // deleted rows are enumerated, so kept + deleted must account
                // for every catalog row (the non-capturing path verified its
                // decode total inline above).
                if capture_events && kept_rows + deleted_rows.len() != f.row_count as usize {
                    return Err(BasinError::internal(format!(
                        "DELETE CoW (capturing): file {} decoded {} kept + {} deleted rows \
                         but catalog says {} — refusing rewrite (would destroy rows); retry",
                        f.path,
                        kept_rows,
                        deleted_rows.len(),
                        f.row_count
                    )));
                }
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
    //
    // Gate the whole thing on there actually being a child table that
    // references THIS one: building `deleted_pks` re-reads every rewritten /
    // dropped data file (a second full pass over the rewritten rows), so for
    // the common no-inbound-FK table it is pure waste — a cold DELETE of a few
    // rows from a large table re-read all its rewritten files for nothing. The
    // `fks_referencing` probe is a cheap catalog-metadata lookup (no file I/O).
    let referencing_children = if meta.pk_columns.is_empty() {
        Vec::new()
    } else {
        crate::constraints::fks_referencing(
            &sess.engine.config().catalog,
            &sess.project,
            table.as_str(),
        )
        .await?
    };
    let mut cascades: Vec<crate::constraints::CascadeDelete> = Vec::new();
    if !meta.pk_columns.is_empty() && !referencing_children.is_empty() {
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
    // Phase 5.19.E: clone replacement batches for GIN posting-list maintenance
    // BEFORE write_replacement consumes them. Arrow RecordBatch clones are
    // cheap (data lives in Arc-backed buffers).
    let gin_replacement_batches: Vec<RecordBatch> = replacement_batches.iter().cloned().collect();
    // Pre-commit before writing the replacement file so a rejecting
    // sink leaves no orphan parquet on disk.
    dispatch_pre_commit(&sess.engine, &events).await?;
    let added_files = write_replacement(sess, &table, schema.clone(), replacement_batches).await?;
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        removed_paths.clone(),
        added_files.clone(),
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);

    // Phase 5.19.E — GIN posting-list maintenance: remove dropped/replaced
    // file entries and rebuild for any new replacement file.
    {
        use basin_storage::index::index_maint::GinPostingListMaintainer;
        if let Some(maint) = GinPostingListMaintainer::new(
            sess.engine.gin_index_registry().as_ref(),
            &sess.project,
            &table,
            &meta,
        ) {
            // Files that were fully dropped (AllMatch delete).
            for path in &dropped_paths {
                maint.on_file_removed(path);
            }
            // Files that were rewritten (Mixed delete — kept rows written
            // to a new file). added_files[0].path is the replacement.
            if !replaced_paths.is_empty() {
                if let Some(new_file) = added_files.first() {
                    for old_path in &replaced_paths {
                        maint.on_file_replaced(old_path, &new_file.path, &gin_replacement_batches);
                    }
                }
            }
        }
    }

    // Phase 5.24.D — GIST interval-tree maintenance for DELETE.
    // Remove stale entries for all files that are being dropped or replaced.
    {
        let gist_cols: Vec<String> = meta
            .indexes
            .iter()
            .filter(|idx| idx.access_method == "gist" && idx.columns.len() == 1)
            .map(|idx| idx.columns[0].clone())
            .collect();
        if !gist_cols.is_empty() {
            let ireg = sess.engine.interval_registry();
            for col in &gist_cols {
                for path in &dropped_paths {
                    ireg.remove_file(&sess.project, &table, col, path);
                }
                for path in &replaced_paths {
                    ireg.remove_file(&sess.project, &table, col, path);
                }
                // Rebuild from the replacement batches for replaced files.
                if let Some(new_file) = added_files.first() {
                    for batch in &gin_replacement_batches {
                        use arrow_array::Array;
                        if let Ok(col_idx) = batch.schema().index_of(col) {
                            let col_arr = batch.column(col_idx);
                            if let Some(arr) = col_arr.as_any().downcast_ref::<arrow_array::StringArray>() {
                                for row in 0..arr.len() {
                                    if arr.is_null(row) { continue; }
                                    ireg.index_row(&sess.project, &table, col, arr.value(row), &new_file.path, 0);
                                }
                            }
                        }
                    }
                    if !replaced_paths.is_empty() {
                        ireg.mark_file_indexed(&sess.project, &table, col, &new_file.path);
                    }
                }
            }
        }
    }

    // B-tree secondary-index maintenance for DELETE: purge every dropped AND
    // replaced file's stale locations, then re-register the replacement file
    // (kept rows) when one was written. See
    // `maintain_btree_secondary_on_replace` for the soundness argument.
    {
        let rewrites: Vec<(String, Vec<RecordBatch>)> = match added_files.first() {
            Some(new_file) if !replaced_paths.is_empty() => {
                vec![(new_file.path.clone(), gin_replacement_batches.clone())]
            }
            _ => Vec::new(),
        };
        // Box::pin: exec_delete recurses through FK CASCADE chains; keep the
        // maintenance future's locals on the heap so the debug-profile stack
        // budget of the recursive poll chain is unchanged.
        Box::pin(maintain_btree_secondary_on_replace(
            sess,
            &table,
            &meta,
            &removed_paths,
            &rewrites,
        ))
        .await;
    }

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

// ─────────────────────────────────────────────────────────────────────────────
// Hot-tier UPDATE fast path (PR2 — mirrors the DELETE fast path)
// ─────────────────────────────────────────────────────────────────────────────
//
// For UPDATE statements shaped as `SET col = lit[, …] WHERE pk = lit` or
// `… WHERE pk IN (lit, …)` on tables with a single-column PRIMARY KEY and no
// surrounding feature usage (RLS, audit, generated columns, secondary indexes,
// CHECK/FK/UNIQUE constraints, FK children, reactors), the engine
// reads the matched row image (hot tier first, then cold tier), applies the
// SET, and writes a `MemRowValue::Update` (full-row replacement keyed by the
// encoded PK) into the process-wide `MemTableRegistry` — skipping the cold-tier
// copy-on-write Parquet rewrite that scales linearly with file size.
//
// Read-path semantics. The override row wins over the stale cold-tier row on
// every read path: the merge-on-read overlay (`hot_tombstone::UpdateOverlayExec`
// on the DataFusion path, `apply_update_overlay_to_batches` on the fast-select
// cold path, and the `probe_memtable` point-lookup) suppress the cold row by PK
// and surface the new image. See `hot_tombstone.rs`.
//
// Atomicity. Like the DELETE fast path, this runs only OUTSIDE an explicit
// transaction (`tx_is_active == false`) so ROLLBACK semantics are unaffected.
//
// Assignment support. Literal / bind-param SET RHS (the `AssignmentRhs::
// Scalar` path) is always fast-path eligible. Expression RHS (`AssignmentRhs::
// Expr`) is eligible only for the row-local, deterministic read-modify-write
// allowlist in `rmw_rhs_is_fast_path_eligible` (`col + 1`, `CASE …`, `a = b`,
// simple casts); non-allowlisted expressions (`now()`, `upper(col)`, division,
// subqueries, …) fall through to the cold CoW path. Both paths share the same
// per-batch DataFusion evaluator (`apply_assignments` →
// `generated_cols::eval_expression`), so an allowlisted RMW post-image is
// byte-identical hot vs cold.

/// Return `true` when every item in a RETURNING list can be projected directly
/// from the post-image `RecordBatch` without a DataFusion evaluation context.
///
/// Allowlist (fast path):
///   * `*` / `tbl.*` / `schema.tbl.*` — wildcard expands all columns.
///   * `col` (bare identifier, `UnnamedExpr(Identifier)`) — plain column ref.
///   * `col AS alias` (`ExprWithAlias { expr: Identifier, .. }`) — column ref
///     with an output alias; the alias is carried through in the projected schema.
///
/// Anything else (arithmetic, function calls, casts, literals, compound
/// expressions) returns `false` → the caller falls through to the cold path
/// which runs the full `project_returning` DataFusion round-trip.
fn returning_is_fast_path_eligible(items: &[SelectItem]) -> bool {
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
            SelectItem::UnnamedExpr(Expr::Identifier(_)) => {}
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                ..
            } => {}
            _ => return false,
        }
    }
    true
}

/// Return `true` when a SET-RHS expression is on the read-modify-write
/// allowlist for the single-row PK-eq UPDATE fast path.
///
/// The post-image is produced by `apply_assignments` →
/// `generated_cols::eval_expression`, the SAME DataFusion evaluator the cold
/// path uses, run against the pre-image row batch. So the only safety
/// requirement for the hot path is that the expression be **row-local and
/// deterministic**: its value for a row must depend only on that row's column
/// values, never on other rows, external state, or evaluation time. Every
/// shape below satisfies that, so the fast-path result is byte-identical to
/// cold (matching NULL propagation, integer overflow, and type coercion,
/// because it is the same evaluator on the same base row).
///
/// Allowlist (recursive):
///   * a literal `Value` (incl. NULL) — also handled by the Scalar path, but
///     harmless if it reaches here inside a larger expression;
///   * a bare `Identifier` / two-part `CompoundIdentifier` that resolves to a
///     real column in `schema` (an unknown ident → cold path, which produces
///     the canonical error);
///   * `+`, `-`, `*` arithmetic where both operands are allowlisted;
///   * unary `+` / `-` of an allowlisted operand;
///   * a `CAST(expr AS ty)` (plain `CAST` kind only) of an allowlisted operand;
///   * a parenthesized (`Nested`) allowlisted expression;
///   * `CASE` (searched or simple) whose operand (if any), every WHEN
///     condition, every THEN result, and the ELSE result are allowlisted, and
///     whose conditions use only comparison / boolean connectives.
///
/// Function allowlist (recursive on arguments — see `RMW_FN_ALLOWLIST`):
///   * `jsonb_set`, `jsonb_insert`, `jsonb_strip_nulls`, `coalesce` — every
///     one is registered `Volatility::Immutable` and its result depends only
///     on its argument values (and a row's column values when an argument IS a
///     column ref), never on wall-clock time, other rows, or external state.
///     `jsonb_set(col, '{a,b}', '…')` rewriting one row's JSONB blob is exactly
///     the row-local/deterministic shape the overlay can serve byte-identically
///     to cold (same `generated_cols::eval_expression` evaluator). Every
///     argument must itself be allowlisted, so `jsonb_set(col, path, now())`
///     stays on the cold path.
///
/// Deliberately EXCLUDED (kept on the cold path):
///   * any function NOT in `RMW_FN_ALLOWLIST` (`now()`, `random()`, `upper()`,
///     arbitrary UDFs) — a time/volatile/non-row-local function must stay on
///     the cold oracle. `now()` in particular is volatile and is the canonical
///     "do not fast-path" case;
///   * `/` (Divide) and `%` (Modulo) — division-by-zero and integer-vs-float
///     coercion behaviour is subtle; "when in doubt, exclude";
///   * `TRY_CAST` / `SAFE_CAST` and casts with a FORMAT clause;
///   * subqueries, EXISTS, IN, window/aggregate constructs, and anything else.
fn rmw_rhs_is_fast_path_eligible(expr: &Expr, schema: &Schema) -> bool {
    use sqlparser::ast::CastKind;
    match expr {
        // Literals (including NULL) are row-local constants.
        Expr::Value(_) => true,
        // Bare column reference — must resolve to a real column.
        Expr::Identifier(ident) => schema.index_of(&ident.value).is_ok(),
        // `tbl.col` / `schema.col` — resolve on the final part.
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| schema.index_of(&ident.value).is_ok()),
        // Parentheses.
        Expr::Nested(inner) => rmw_rhs_is_fast_path_eligible(inner, schema),
        // Unary +/- (e.g. `-v`, `+amount`).
        Expr::UnaryOp { op, expr: inner } => {
            matches!(op, UnaryOperator::Minus | UnaryOperator::Plus)
                && rmw_rhs_is_fast_path_eligible(inner, schema)
        }
        // Arithmetic: +, -, * only (no /, %).
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
            ) && rmw_rhs_is_fast_path_eligible(left, schema)
                && rmw_rhs_is_fast_path_eligible(right, schema)
        }
        // Plain CAST of an allowlisted operand (no TRY_CAST / SAFE_CAST /
        // FORMAT / multi-valued ARRAY cast).
        Expr::Cast {
            kind: CastKind::Cast,
            expr: inner,
            array: false,
            format: None,
            ..
        } => rmw_rhs_is_fast_path_eligible(inner, schema),
        // CASE — every sub-expression must be allowlisted. Searched CASE
        // (`CASE WHEN a > 1 THEN …`) requires each WHEN condition to be a
        // comparison / boolean connective tree; simple CASE
        // (`CASE col WHEN lit THEN …`) stores each WHEN arm's *value*
        // expression as the condition (sqlparser keeps the implicit
        // `operand = value` equality structural), so an arm is eligible iff
        // that value expression is itself allowlisted — exactly the searched
        // equivalent `operand = value`, which the comparison rule admits.
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                if !rmw_rhs_is_fast_path_eligible(op, schema) {
                    return false;
                }
            }
            for when in conditions {
                let cond_ok = if operand.is_some() {
                    rmw_rhs_is_fast_path_eligible(&when.condition, schema)
                } else {
                    rmw_case_condition_is_eligible(&when.condition, schema)
                };
                if !cond_ok {
                    return false;
                }
                if !rmw_rhs_is_fast_path_eligible(&when.result, schema) {
                    return false;
                }
            }
            match else_result {
                Some(e) => rmw_rhs_is_fast_path_eligible(e, schema),
                None => true,
            }
        }
        // Allowlisted, Immutable, row-local function call: every positional
        // argument must itself be allowlisted. Named args / wildcard args /
        // DISTINCT / ORDER BY / FILTER / OVER take it off the fast path.
        Expr::Function(f) => rmw_function_is_fast_path_eligible(f, schema),
        _ => false,
    }
}

/// Functions whose result is `Volatility::Immutable` AND row-local: it depends
/// only on the (literal or column-ref) argument values, never on wall-clock
/// time, randomness, other rows, or external state. The overlay can serve their
/// post-image byte-identically to the cold path because both run the SAME
/// `generated_cols::eval_expression` evaluator on the SAME pre-image row.
///
/// Conservative by construction: this is an explicit closed list, not a
/// volatility lookup, so adding a function is a deliberate, reviewed change.
/// `now()` / `random()` / `gen_random_uuid()` are NOT here (volatile);
/// `upper()` etc. are omitted not because they're unsafe but because they
/// aren't in the hot-shape benchmark battery and "when in doubt, exclude".
const RMW_FN_ALLOWLIST: &[&str] = &[
    "jsonb_set",
    "jsonb_insert",
    "jsonb_strip_nulls",
    "coalesce",
];

/// Return `true` when `f` is an allowlisted, Immutable, row-local function call
/// (name in `RMW_FN_ALLOWLIST`) whose every positional argument is itself
/// fast-path-eligible. Any non-plain call shape (named args, `*`, DISTINCT,
/// ORDER BY, FILTER, WITHIN GROUP, OVER) bails to the cold path.
fn rmw_function_is_fast_path_eligible(f: &sqlparser::ast::Function, schema: &Schema) -> bool {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    // Reject any window (`OVER`), FILTER, WITHIN GROUP, NULL-treatment, ODBC
    // `{fn …}`, or ClickHouse parametric (`f(a)(b)`) decoration — none is the
    // plain scalar shape `eval_expression` projects.
    if f.over.is_some()
        || f.filter.is_some()
        || !f.within_group.is_empty()
        || f.null_treatment.is_some()
        || f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
    {
        return false;
    }
    let name = f
        .name
        .0
        .last()
        .map(|i| i.id_val().to_ascii_lowercase())
        .unwrap_or_default();
    if !RMW_FN_ALLOWLIST.contains(&name.as_str()) {
        return false;
    }
    let list = match &f.args {
        FunctionArguments::List(list) => list,
        // `f()` with no args (none of the allowlist take zero args) → reject.
        _ => return false,
    };
    // No DISTINCT / ALL, no ORDER BY / other in-arglist clauses.
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return false;
    }
    for a in &list.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                if !rmw_rhs_is_fast_path_eligible(e, schema) {
                    return false;
                }
            }
            // Named args (`=> `), `*`, and qualified-`*` are not the plain
            // positional shape; bail to cold.
            _ => return false,
        }
    }
    true
}

/// Return `true` when a CASE-WHEN condition is a comparison or boolean
/// combination of allowlisted (row-local, deterministic) sub-expressions.
/// Used only by `rmw_rhs_is_fast_path_eligible`.
fn rmw_case_condition_is_eligible(cond: &Expr, schema: &Schema) -> bool {
    match cond {
        Expr::Nested(inner) => rmw_case_condition_is_eligible(inner, schema),
        Expr::BinaryOp { left, op, right } => match op {
            // Comparisons of two allowlisted value expressions.
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                rmw_rhs_is_fast_path_eligible(left, schema)
                    && rmw_rhs_is_fast_path_eligible(right, schema)
            }
            // Boolean connectives of two allowlisted conditions.
            BinaryOperator::And | BinaryOperator::Or => {
                rmw_case_condition_is_eligible(left, schema)
                    && rmw_case_condition_is_eligible(right, schema)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Resolve the matched PK keys + scalar assignments when the UPDATE call site
/// is eligible for the hot-tier fast path. Returns
/// `Ok(Some((keys, assigns, probe_pre_images)))` when every gate passes
/// (`probe_pre_images` is non-empty only for probe-resolved shapes — see
/// [`probe_matched_pk_keys`]); `Ok(None)` to fall through to the cold path.
///
/// Gates mirror `try_resolve_fast_path_pks` (DELETE) plus the UPDATE-specific
/// ones: scalar OR allowlisted-RMW assignments (see
/// `rmw_rhs_is_fast_path_eligible`), no generated columns, no CHECK/FK/UNIQUE,
/// PK columns not touched by the SET (a PK rewrite needs the slow path's PK
/// uniqueness check + key re-encoding).
async fn try_resolve_fast_path_update(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    assignments: &[Assignment],
    predicate: Option<&Expr>,
    returning: Option<&[sqlparser::ast::SelectItem]>,
) -> Result<
    Option<(
        Vec<basin_hottier::RowKey>,
        Vec<(usize, AssignmentRhs)>,
        std::collections::HashMap<Vec<u8>, RecordBatch>,
    )>,
> {
    // Gate: hot-tier UPDATE fast path. **Default ON** since Phase 5.14
    // closure. See `try_hot_tier_delete` for the kill-switch semantics —
    // `BASIN_HOTTIER_FASTPATH_DISABLE=1` (global) and
    // `BASIN_HOTTIER_UPDATE_FASTPATH=0` (per-shape) override the default.
    if !crate::dml_mutate::hottier_fastpath_enabled("BASIN_HOTTIER_UPDATE_FASTPATH") {
        return Ok(None);
    }
    // Gate: explicit transaction.
    //
    // Auto-commit: `hot_tier_update_by_pk(tx_mode=false)` writes the `Update`
    // override straight to the shared `MemTableRegistry`. In-transaction:
    // `hot_tier_update_by_pk(tx_mode=true)` writes it to `TxState::tx_overlay`
    // so ROLLBACK can drop it — but only when the in-tx fast path is eligible
    // for this table (no savepoint, no prior multi-row / cold-path mutation
    // staged this tx). See `tx_fastpath_eligible_for_table`.
    if crate::session::tx_is_active(&sess.state) {
        if !tx_fastpath_eligible_for_table(sess, table) {
            return Ok(None);
        }
    }
    // Gate: RETURNING with non-trivial expressions (anything beyond plain
    // column references and `*`) cannot be projected from the post-image
    // batch without a DataFusion context — fall through to the cold path.
    // Plain column refs and wildcards are handled inline after the fast-path
    // write (see `hot_tier_update_by_pk` caller).
    if let Some(items) = returning {
        if !returning_is_fast_path_eligible(items) {
            return Ok(None);
        }
    }
    // Gate: single-column PK only.
    if meta.pk_columns.len() != 1 {
        return Ok(None);
    }
    let pk_col = &meta.pk_columns[0];
    // Gate: RLS / audit need the slow read+rewrite (row images, WITH CHECK).
    if meta.rls_enabled {
        return Ok(None);
    }
    if crate::types::soft_delete_column(meta.schema.as_ref()).is_some() {
        return Ok(None);
    }
    if crate::types::audit_table_name(meta.schema.as_ref()).is_some() {
        return Ok(None);
    }
    // Gate: generated columns must be recomputed per-row on the slow path.
    if meta
        .schema
        .fields()
        .iter()
        .any(|f| crate::types::field_is_generated(f).is_some())
    {
        return Ok(None);
    }
    // Gate: AUTO_UPDATE columns inject a fresh now() on the slow path.
    {
        let mut probe: Vec<(usize, AssignmentRhs)> = Vec::new();
        inject_auto_update_assignments(meta.schema.as_ref(), &mut probe);
        if !probe.is_empty() {
            return Ok(None);
        }
    }
    // Gate: constraints (CHECK / FK / UNIQUE) need post-image enforcement.
    if !meta.check_constraints.is_empty()
        || !meta.foreign_keys.is_empty()
        || !meta.unique_constraints.is_empty()
    {
        return Ok(None);
    }
    // Gate: secondary indexes — dispatch on the catalog access method
    // (`SecondaryIndex::access_method`: "btree" is the default; "gin" covers
    // jsonb_ops / jsonb_path_ops containment GIN and tsvector_ops FTS GIN;
    // "gist" is the interval/range index; vector indexes persist as "hnsw" —
    // IVFFlat is stored under "hnsw" with an `ivfflat:` opclass).
    //
    // GIN-ONLY tables (EVERY declared index has `access_method == "gin"`)
    // are ADMITTED to the overlay fast path. The historical blanket decline
    // documented three read-path blockers; all three are now closed in
    // committed code (pinned by tests/integration/tests/gin_overlay_update.rs):
    //
    //   1. Executor posting-probe `Empty` short-circuits (`@>`/`<@`,
    //      `?`/`?&`/`?|`, and the tsvector `@@` twin) fire only when
    //      `executor::gin_empty_probe_is_trustworthy` /
    //      `fts_empty_probe_is_trustworthy` hold: NO live overlay for the
    //      table (`session::table_has_live_overlay`, O(1) counter reads) AND
    //      every live file is in the registry's indexed-files completeness
    //      set. A live override row therefore always falls through to the
    //      overlay-aware scan (`TombstoneFilterExec` + `UpdateOverlayExec`).
    //
    //   2. The session pruned re-registrations
    //      (`session::apply_gin_pruning_for_query`,
    //      `apply_jsonb_posting_pruning_for_query`,
    //      `apply_gin_fts_pruning_for_query`) skip swapping the overlay-aware
    //      provider for a bare cold reader while `table_has_live_overlay` is
    //      true — override rows are appended and their stale cold images
    //      suppressed for every containment/FTS SELECT during the overlay
    //      window (correct-but-unpruned; pruning resumes after the drain).
    //
    //   3. CREATE INDEX settles any live overlay
    //      (`materialize_overlay_for_table`) BEFORE backfilling, so a fresh
    //      index never seals pre-update cold images as "complete"; and the
    //      materialize path itself now performs GIN-family registry
    //      maintenance on its replacement files (purge replaced paths,
    //      rebuild + completeness-seal the replacement — see the maintenance
    //      block in `materialize_overlay_for_table`), so a drained overlay
    //      leaves the posting lists complete and pruning RE-ENGAGES instead
    //      of degrading to full scans forever.
    //
    // ALSO ADMITTED — single-column "btree" (the default access method):
    //   * `fast_select`'s secondary-index allowlist probe now DECLINES while
    //     `table_has_live_overlay` (the overlay-emptiness guard equivalent to
    //     (1)/(2) above), so a HIT never prunes to a cold-file set that an
    //     overlay override / tombstone row could escape; and
    //   * `materialize_overlay_for_table` re-maintains the B-tree location
    //     registry on drain (purge replaced files + re-register the
    //     replacement via `backfill_btree_batch`), mirroring the cold CoW
    //     `maintain_btree_secondary_on_replace`, so pruning RE-ENGAGES after a
    //     drain. Oracle: `btree_overlay_delete.rs`.
    //
    // STILL DECLINED — these readers have no overlay guard:
    //   * "gist": the interval-tree registry has its own consumers
    //     (`rtree_rowgroup_scan` / range probes) with no overlay guards.
    //   * "hnsw"/vector: ANN sidecars have their own readers and rebuild
    //     lifecycle; overlay-merging a graph index is out of scope.
    //   * multi-column / expression btree: the registry indexes single
    //     non-expression columns only.
    //
    // Mixed tables decline UNLESS every index is GIN or single-column btree:
    // one unguarded consumer is enough to leak a stale read.
    if !meta.indexes.is_empty()
        && !meta.indexes.iter().all(|idx| {
            idx.access_method == "gin"
                || (idx.access_method == "btree"
                    && idx.columns.len() == 1
                    && !idx.columns[0].starts_with("expr:"))
        })
    {
        return Ok(None);
    }
    // Gate: any child table referencing this one by FK (cascade handling), or a
    // per-table UPDATE reactor (needs before/after row images) → slow path.
    //
    // perf-w-pointops: these were TWO uncached awaited catalog round-trips
    // (`fks_referencing` + `list_reactors`) on EVERY fast-path UPDATE
    // (~120µs/statement on the warm OLTP loop). They are now served from the
    // per-session, catalog-epoch-validated `dml_flags_cache` — a single warm
    // `Mutex` lock on the steady-state loop, refetched on any catalog mutation
    // (FK/reactor DDL bumps the epoch). Correctness is unchanged: the cached
    // flags carry the identical FK-presence and UPDATE-reactor-presence
    // verdicts the inline calls computed.
    let dml_flags = crate::session::load_dml_flags_cached(sess, table).await?;
    if dml_flags.has_referencing_fk || dml_flags.has_update_reactor {
        return Ok(None);
    }
    // NOTE: no-WHERE is NOT a gate. `UPDATE t SET …` with no predicate routes
    // through the resolve-by-probe machinery below (`SELECT * FROM t LIMIT
    // cap+1`): a small table's full row set is exactly a small-bulk key set,
    // while a table with more than `delta_update_max_keys()` live rows blows
    // the probe LIMIT and falls to the cold rewrite exactly as today. Every
    // semantic gate above (single PK, indexes, constraints, reactors, RLS,
    // audit, generated, AUTO_UPDATE) and below (RHS allowlist, PK unassigned)
    // applies identically; the probe itself enforces auto-commit-only.
    //
    // Gate: every assignment must be either a plain scalar OR an allowlisted
    // read-modify-write expression; reject anything else, and reject any
    // assignment that touches the PK column (key re-encoding / uniqueness).
    //
    // We resolve the assignment eligibility BEFORE the (potentially I/O-bound)
    // multi-key probe so an ineligible SET never pays for a probe SELECT.
    //
    // Read-modify-write (`SET col = col + 1`, `SET col = CASE …`, `SET a = b`,
    // `SET payload = jsonb_set(payload, …)`) is the single worst-performing
    // UPDATE shape: at 1M rows the cold copy-on-write rewrite is 1.7–16.5s vs
    // ~20ms on the hot overlay. Routing it through the overlay is safe because:
    //   * `apply_assignments` already evaluates `AssignmentRhs::Expr` against
    //     the pre-image batch via the SAME `generated_cols::eval_expression`
    //     DataFusion projection the cold path uses — so for any row-local,
    //     deterministic expression the post-image is byte-identical to cold
    //     (NULL propagation, integer overflow, and type coercion all match
    //     because it is literally the same evaluator on the same base row);
    //   * `hot_tier_update_by_pk` reads the LATEST value (memtable Update
    //     overlay > PK row cache > cold), so repeated `SET v = v + 1`
    //     accumulates rather than re-reading the stale cold base;
    //   * the overlay-vs-cold-rewrite data-loss hazard is closed by
    //     `materialize_hot_overlay_into_cold` (see rmw_update_correctness.rs).
    //
    // We therefore admit an EXPLICIT allowlist of expression shapes whose
    // evaluation is row-local and deterministic (column refs, +/-/* arithmetic,
    // comparison-based CASE, simple casts, Immutable row-local functions —
    // `jsonb_set`/`jsonb_insert`/`jsonb_strip_nulls`/`coalesce` — and nestings
    // thereof; see `rmw_rhs_is_fast_path_eligible`). Anything outside the
    // allowlist (volatile functions, division/modulo, subqueries, NOW(), …)
    // still falls to the cold path, which remains the semantics oracle. Do NOT
    // widen the allowlist without re-running rmw_update_correctness +
    // update_hottier.
    let parsed = parse_assignments(assignments, meta.schema.as_ref())?;
    debug_assert_eq!(parsed.len(), assignments.len());
    for ((_, rhs), assignment) in parsed.iter().zip(assignments.iter()) {
        if matches!(rhs, AssignmentRhs::Expr(_))
            && !rmw_rhs_is_fast_path_eligible(&assignment.value, meta.schema.as_ref())
        {
            return Ok(None);
        }
    }
    let pk_idx = meta
        .schema
        .index_of(pk_col)
        .map_err(|_| BasinError::internal(format!("PK column {pk_col:?} missing from schema")))?;
    if parsed.iter().any(|(idx, _)| *idx == pk_idx) {
        return Ok(None);
    }
    let pk_dt = meta.schema.field(pk_idx).data_type().clone();

    // ── WHERE → matched-PK set resolution ───────────────────────────────────
    //
    // Two strategies, cheapest first:
    //
    //   (A) Literal pk-list — `WHERE pk = lit` / `pk IN (lits)`. Resolved with
    //       ZERO reads by `predicate_resolves_to_pk_list`: the matched key set
    //       IS the literal list. This is the historical single-/few-key shape.
    //
    //   (B) Resolve-by-probe — any OTHER predicate the fast SELECT machinery
    //       understands (`pk < lit`, `pk <= lit`, `pk BETWEEN a AND b`,
    //       `pk > a AND pk < b`, even a NON-pk equality like `status = 'x'`),
    //       AND the no-WHERE statement (every live row matches). Such matched
    //       sets aren't enumerable without a read, so we PROBE: run
    //       `SELECT * FROM t [WHERE <original predicate>] LIMIT N+1` through
    //       the EXISTING `execute_simple_select` fast path. If ≤ N keys come
    //       back we have the EXACT matched set (plus each key's pre-image,
    //       carried from the probe rows) and route them through
    //       `hot_tier_update_by_pk`; if N+1 rows come back (too many for a
    //       small-bulk overlay write) or the probe can't represent the
    //       predicate, we return `Ok(None)` and the caller falls to cold CoW —
    //       exactly as today.
    if let Some(expr) = predicate {
        if let Some(lit_exprs) = predicate_resolves_to_pk_list(expr, pk_col, table.as_str()) {
            // (A) Literal list. Empty IN-list → zero matches (UPDATE 0).
            if lit_exprs.is_empty() {
                return Ok(Some((Vec::new(), parsed, std::collections::HashMap::new())));
            }
            let mut keys: Vec<basin_hottier::RowKey> = Vec::with_capacity(lit_exprs.len());
            for lit in &lit_exprs {
                let scalar = match try_literal_to_scalar(lit, &pk_dt, pk_col)? {
                    Some(s) => s,
                    None => return Ok(None),
                };
                let Some(key) = pk_scalar_to_row_key(&scalar, &pk_dt) else {
                    return Ok(None);
                };
                keys.push(key);
            }
            return Ok(Some((keys, parsed, std::collections::HashMap::new())));
        }
    }

    // (B) Resolve-by-probe (`predicate = None` ⇒ the no-WHERE whole-table
    // probe; the cap decides delta-vs-cold).
    match probe_matched_pk_keys(sess, table, meta, pk_col, &pk_dt, predicate).await? {
        Some((keys, pre_images)) => Ok(Some((keys, parsed, pre_images))),
        None => Ok(None),
    }
}

/// Default delta-UPDATE fast-path cap: the maximum number of matched PKs a
/// probed UPDATE will route through the hot-tier overlay. A WHERE matching
/// more than this many rows falls to the cold copy-on-write path (whose
/// write-amp is amortised over a whole-file rewrite anyway).
///
/// History: the original cap was 64, chosen when the write step re-read every
/// pre-image from cold per key. Two things since removed that bound: (a) the
/// probe now carries the full pre-image batches itself (one read total, no
/// second cold pass), and (b) the overlay write is budget-guarded — after the
/// pre-images are gathered `hot_tier_update_by_pk` reserves their exact bytes
/// against the project memtable budget and declines to cold on
/// `HardCapReached`. With per-key cost flat and memory capped by the budget
/// (not the key count), the cap's job is only to keep a single statement's
/// overlay write "small bulk" rather than a table rewrite; 10k covers the
/// `WHERE status = 'x'` / project-slice shapes while a full-table UPDATE on a
/// large table still takes the amortised cold rewrite.
const DELTA_UPDATE_MAX_KEYS_DEFAULT: usize = 10_000;

/// Resolve the delta-UPDATE cardinality cap: `BASIN_DELTA_UPDATE_MAX_KEYS`
/// when set to a positive integer, else [`DELTA_UPDATE_MAX_KEYS_DEFAULT`].
///
/// Read per statement (same rationale as `hottier_fastpath_enabled`): shard
/// processes spawn from one binary and an operator clamping the cap must take
/// effect on the next statement without a restart. `0` / unparseable values
/// fall back to the default rather than disabling the path — use
/// `BASIN_HOTTIER_UPDATE_FASTPATH=0` to turn the fast path off.
fn delta_update_max_keys() -> usize {
    std::env::var("BASIN_DELTA_UPDATE_MAX_KEYS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DELTA_UPDATE_MAX_KEYS_DEFAULT)
}

/// Probe result: the matched `RowKey`s (statement order) plus, keyed by the
/// encoded key bytes, each key's full single-row pre-image batch as the probe
/// read it (overlay-aware). The map is empty for the literal pk-list path,
/// which performs no read — those keys gather their pre-images through the
/// memtable / PK-cache / cold tiers in `hot_tier_update_by_pk` as before.
type ProbedUpdateKeys = (
    Vec<basin_hottier::RowKey>,
    std::collections::HashMap<Vec<u8>, RecordBatch>,
);

/// Resolve the EXACT set of matched PK `RowKey`s for an UPDATE whose WHERE is
/// NOT a literal pk-list — or that has NO WHERE at all — by probing the
/// existing fast-SELECT machinery.
///
/// Returns:
///   * `Ok(Some((keys, pre_images)))` — the predicate (or, for a no-WHERE
///     UPDATE, the whole table) matched ≤ `delta_update_max_keys()` rows and
///     every matched PK encoded to a `RowKey`; route them through the overlay.
///     `pre_images` maps each key's encoded bytes to its full single-row
///     pre-image batch, harvested from the probe itself (design 1a — see
///     below).
///   * `Ok(None)` — too many matches (> cap), an un-probe-able predicate shape,
///     a NULL / unsupported-type PK, or any other reason to fall to cold CoW.
///
/// ── Probe design ─────────────────────────────────────────────────────────
/// We build `SELECT * FROM <table> [WHERE <predicate>] LIMIT N+1` (N = cap)
/// by round-tripping the ORIGINAL predicate `Expr` back to SQL text (its
/// `Display` is faithful for sqlparser ASTs and subqueries were already
/// resolved to literals upstream in `exec_update`), parse it, and feed it to
/// `match_simple_select`. That matcher is the gate on which predicates we
/// accept: anything it can't represent (OR, function predicates, IS NOT NULL,
/// >3 AND atoms, …) yields `None` here and we fall to cold. A no-WHERE UPDATE
/// (predicate = `None`) probes the zero-predicate select, which
/// `match_simple_select` also admits — so a small-table `UPDATE t SET …`
/// routes through the same machinery and a big-table one falls to cold at
/// the cap exactly like a wide WHERE. Because the probe evaluates the FULL
/// original predicate and returns the matching PKs, the UPDATE then applies
/// to EXACTLY those keys — the predicate is fully consumed by the probe, so a
/// non-pk residual (`status = 'x'`) needs no post-probe re-evaluation.
///
/// ── Pre-image carry (design 1a) ──────────────────────────────────────────
/// The probe selects `*`, not just the PK: each matched row's FULL image is
/// returned anyway by the overlay-aware fast SELECT, so we slice it into
/// per-key single-row batches and hand them to `hot_tier_update_by_pk` as the
/// pre-image source. Historically the probe projected only the PK and the
/// write step re-read every pre-image from cold WITHOUT predicate pushdown
/// for >1 key — at the raised 10k-key cap that second unfiltered pass is the
/// dominant cost, and carrying the probe batches eliminates it entirely.
/// Precedence is preserved: the memtable / tx-overlay tiers in
/// `hot_tier_update_by_pk` still win per key; the probe image only fills keys
/// those tiers don't hold (and the probe itself already merged the overlay,
/// so the image is never staler than the cold base).
///
/// `execute_simple_select` is overlay-aware (it merges tombstones + UPDATE
/// overrides on read), so the probe sees the same logical state a user SELECT
/// would — prior hot-tier mutations included.
///
/// ── Race argument (auto-commit, no surrounding tx) ───────────────────────
/// Between the probe SELECT and the overlay write a concurrent session could
/// INSERT a row that ALSO matches the predicate; our overlay then misses it.
/// This is the SAME class of race the cold CoW path already has: the cold path
/// lists + reads the base files at one instant and rewrites them, re-evaluating
/// the predicate over WHAT IT READ — a row inserted to the tail AFTER that read
/// is likewise missed. Both paths take a read snapshot, evaluate the predicate
/// over it, and write; neither holds a predicate lock. So the probe path is
/// race-EQUIVALENT to the cold path, not weaker.
///
/// Ordering equivalence: the cold path flushes the INSERT tail before it lists
/// base files (`pre_mutation_flush_if_tail` in `exec_update`, which runs BEFORE
/// this gate). The probe inherits that exact ordering — the tail flush already
/// happened — and `execute_simple_select` additionally flushes the shard tail
/// itself, so the probe reads a post-flush head identical to the one the cold
/// rewrite would list. No additional flush is required here.
async fn probe_matched_pk_keys(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    pk_col: &str,
    pk_dt: &DataType,
    predicate: Option<&Expr>,
) -> Result<Option<ProbedUpdateKeys>> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Gate: probe only in auto-commit. `execute_simple_select` is the no-pin /
    // live-head entrypoint; inside an explicit tx it would not read at this
    // session's pinned read-view (or see this tx's own overlay writes), so a
    // probed key set could diverge from what the tx should mutate. The literal
    // pk-list path (which performs no read) stays in-tx-eligible above; only the
    // resolve-by-probe shapes (including no-WHERE) are restricted to
    // auto-commit. In a tx these predicates fall to the cold CoW path, which
    // honours the tx read-view.
    if crate::session::tx_is_active(&sess.state) {
        return Ok(None);
    }

    let cap = delta_update_max_keys();

    // Build the probe SQL. Quote the table identifier so reserved-word /
    // mixed-case table names round-trip. The predicate Display is faithful for
    // the sqlparser AST we hold (subqueries already resolved to literals
    // upstream). `SELECT *` (not just the PK): the full row images double as
    // the write step's pre-image source — see "Pre-image carry" above. A
    // no-WHERE UPDATE omits the WHERE clause entirely; `match_simple_select`
    // admits the zero-predicate select.
    let tbl = table.as_str().replace('"', "\"\"");
    let lim = cap + 1;
    let probe_sql = match predicate {
        Some(pred) => format!("SELECT * FROM \"{tbl}\" WHERE {pred} LIMIT {lim}"),
        None => format!("SELECT * FROM \"{tbl}\" LIMIT {lim}"),
    };
    let mut stmts = match Parser::parse_sql(&PostgreSqlDialect {}, &probe_sql) {
        Ok(s) => s,
        // A predicate that won't re-parse (shouldn't happen for an AST we just
        // held) → fall to cold rather than erroring the user's UPDATE.
        Err(_) => return Ok(None),
    };
    let Some(stmt) = stmts.pop() else {
        return Ok(None);
    };
    // The fast-SELECT matcher is the gate on which predicates we accept; an
    // un-representable shape (OR, function predicate, IS NOT NULL, …) → cold.
    let Some(plan) = crate::fast_select::match_simple_select(&stmt) else {
        return Ok(None);
    };

    // Run the probe through the overlay-aware fast-SELECT path. `include_deleted
    // = false` so tombstoned rows are not returned (they must not be re-updated).
    let result = crate::fast_select::execute_simple_select(
        sess,
        plan,
        Some(meta.clone()),
        &probe_sql,
        false,
    )
    .await?;
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        // An `Empty` result for a row-returning probe is unexpected; be safe.
        ExecResult::Empty { .. } => return Ok(None),
    };

    // Collect + cap. The LIMIT already bounds the probe to N+1 rows, so seeing
    // strictly more than N means "too many to small-bulk" → cold.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total > cap {
        return Ok(None);
    }
    if total == 0 {
        // Zero matches: a valid fast-path outcome (UPDATE 0). Returning an empty
        // key set keeps us off the cold path for a no-op WHERE (or a no-WHERE
        // UPDATE on an empty table).
        return Ok(Some((Vec::new(), std::collections::HashMap::new())));
    }

    let mut keys: Vec<basin_hottier::RowKey> = Vec::with_capacity(total);
    let mut pre_images: std::collections::HashMap<Vec<u8>, RecordBatch> =
        std::collections::HashMap::with_capacity(total);
    for batch in &batches {
        // `SELECT *` projection: locate the PK column BY NAME per batch — a
        // hot-tier memtable image carries the write-time column order, so the
        // PK is not guaranteed to sit at the same index in every batch.
        let Ok(pk_idx) = batch.schema().index_of(pk_col) else {
            // A probe batch without the PK column is unexpected; be safe.
            return Ok(None);
        };
        // Normalize ONCE per batch (not per row): `reattach_catalog_metadata`
        // re-applies catalog field metadata and the Binary↔LargeBinary /
        // Utf8↔LargeUtf8 physical widenings a raw Vortex decode drops — the
        // same normalization the cold fill in `hot_tier_update_by_pk`
        // performs, so the merge sees an identical image either way.
        // (Column-order drift is handled later by `pad_batch_to_schema`,
        // which emits catalog order by name.) The KEY is still extracted from
        // the RAW column, exactly as the PK-only probe did — a type the
        // encoder doesn't support returns `None` and we fall to cold.
        let normalized = reattach_catalog_metadata(meta.schema.as_ref(), batch.clone())?;
        let col = batch.column(pk_idx);
        for row in 0..batch.num_rows() {
            if col.is_null(row) {
                // A NULL PK can't encode to a RowKey and shouldn't exist for a
                // PK column; bail to cold for safety.
                return Ok(None);
            }
            let Some(key) = pk_array_value_to_row_key(col.as_ref(), row, pk_dt) else {
                return Ok(None);
            };
            // Carry the full row image as the pre-image for this key.
            pre_images.insert(key.as_bytes().to_vec(), normalized.slice(row, 1));
            keys.push(key);
        }
    }
    Ok(Some((keys, pre_images)))
}

/// Encode the PK value at `row` of `arr` into a `RowKey`, mirroring the type
/// support of [`pk_scalar_to_row_key`] (Int64 / Int32 / Int16 / UInt64 / Utf8 /
/// Boolean). Returns `None` for an unsupported column type so the caller falls
/// back to the cold path. The Arrow array type must match the PK column's
/// declared `DataType` (it comes straight from the probe projection of that
/// column).
fn pk_array_value_to_row_key(
    arr: &dyn arrow_array::Array,
    row: usize,
    col_dt: &DataType,
) -> Option<basin_hottier::RowKey> {
    use arrow_array::{
        BooleanArray, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        UInt64Array,
    };
    let b = basin_hottier::RowKey::builder();
    Some(match col_dt {
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>()?;
            b.append_i64(a.value(row)).finish()
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>()?;
            b.append_i32(a.value(row)).finish()
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>()?;
            b.append_i16(a.value(row)).finish()
        }
        DataType::UInt64 => {
            let a = arr.as_any().downcast_ref::<UInt64Array>()?;
            b.append_u64(a.value(row)).finish()
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>()?;
            b.append_str(a.value(row)).finish()
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>()?;
            b.append_str(a.value(row)).finish()
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>()?;
            b.append_u8(if a.value(row) { 1 } else { 0 }).finish()
        }
        _ => return None,
    })
}

/// Execute the resolved hot-tier UPDATE: for each matched PK read the current
/// row image (tx overlay first when in a tx, then memtable override, then the
/// probe-carried pre-image, else cold-tier), apply the scalar assignments, and
/// write a `MemRowValue::Update` keyed by the encoded PK. Returns
/// `Ok(Some((updated_count, returning_batches)))` on success, or `Ok(None)` —
/// the budget-decline sentinel — when the project's memtable hard cap cannot
/// admit the pre-image bytes: the caller (`exec_update`) must then fall
/// through to the cold copy-on-write path, which re-evaluates the predicate
/// itself (the probe's key set is simply discarded) and whose
/// `materialize_hot_overlay_into_cold` prologue actively DRAINS overlay
/// memory, the correct response to a full budget. The decline happens BEFORE
/// any overlay/tx-overlay write and before assignment evaluation, so a
/// declined statement leaves zero partial state.
///
/// `tx_mode`:
///   * `false` (auto-commit) — read precedence is shared memtable > PK cache >
///     cold; the post-image override is written to the shared `MemTableRegistry`.
///   * `true` (in an explicit tx, eligibility already checked by
///     `tx_fastpath_eligible_for_table`) — read precedence prepends this
///     session's `TxState::tx_overlay` (so repeated RMW within the tx
///     accumulates: `UPDATE v=v+1` twice → +2), and the post-image override is
///     written to `tx_overlay` (rollback-able) instead of the shared registry.
///
/// When `returning` is `Some(items)` (pre-validated by
/// `returning_is_fast_path_eligible`), `returning_batches` contains the
/// post-update row images projected to the requested columns, in key order.
/// When `returning` is `None` the second element is always empty.
///
/// Affected-row semantics. Unlike the DELETE fast path (which reports the
/// number of requested PKs), UPDATE reports the number of PKs that resolved to
/// an existing row — a PK that matches no live row contributes nothing, exactly
/// like Postgres.
async fn hot_tier_update_by_pk(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    keys: &[basin_hottier::RowKey],
    assignments: &[(usize, AssignmentRhs)],
    probe_pre_images: &std::collections::HashMap<Vec<u8>, RecordBatch>,
    returning: Option<&[sqlparser::ast::SelectItem]>,
    tx_mode: bool,
) -> Result<Option<(usize, Vec<RecordBatch>)>> {
    use std::collections::HashMap;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();
    let catalog = &sess.engine.config().catalog;
    let registry = sess.engine.memtable_registry();
    let entry = registry.get_or_create(sess.project, table.clone());

    // ── PK row cache context for the read-before-write ───────────────────────
    //
    // The merge below needs the FULL current row image so unset columns are
    // preserved. The always-on `PkRowCache` already holds full-schema cold-row
    // images populated by the canonical `SELECT * WHERE pk = X` fast path (and
    // by this function's own cold-read populate below). Consulting it lets a
    // single-row UPDATE skip the cold point-lookup entirely on a warm hit,
    // which is the scale-dependent cost that made the 1M-row UPDATE ~10× slower
    // than the 100k case (the cold read grows with file count).
    //
    // We mirror the SELECT-path guard exactly:
    //   * RLS DISABLED — a cached row is the RAW row, never an RLS-filtered
    //     view, so it must never seed an RLS-table merge (bug family #159);
    //   * single-column PK only — multi-column / non-single PK tables don't
    //     share the `RowKey` point shape and are left on the cold path;
    //   * `proj_hash = hash_read_cols(None)` — we need the FULL row, so we only
    //     match (and only populate) the all-columns cache shape; a SELECT that
    //     cached a column subset has a different `proj_hash` and is ignored.
    //
    // Watermarks are captured HERE, before this function writes any overlay, so
    // a concurrent mutation that raced our read advances the live watermark past
    // the captured one and the next reader rejects the (now stale) entry. We do
    // NOT re-populate with the POST-update image: the UPDATE writes a
    // `MemRowValue::Update` overlay that the memtable point-lookup probe serves
    // directly (fast_select returns early on that hit before ever consulting
    // this cache), and the overlay bumps `hot_tier_epoch` so any cold entry we
    // read/populated auto-invalidates on its next GET — never a stale serve.
    let pk_cache_full_proj = crate::pk_row_cache::hash_read_cols(None);
    let pk_cache_enabled = !meta.rls_enabled && meta.pk_columns.len() == 1;
    let pk_cache_hot_epoch = registry.hot_tier_epoch(&sess.project, table);
    let pk_cache_snapshot = meta.current_snapshot.0;

    // Build the set of target PK keys for fast membership + a stable order.
    let want: std::collections::HashSet<Vec<u8>> =
        keys.iter().map(|k| k.as_bytes().to_vec()).collect();

    // 1. Gather current row images keyed by PK bytes. Read precedence:
    //    tx overlay (in-tx only) > shared memtable > [PK cache > cold below].
    //    A higher-precedence hit for a PK wins; lower tiers only fill PKs not
    //    yet present. This is what makes in-tx RMW accumulate: a prior
    //    `UPDATE v=v+1` wrote an override into `tx_overlay`, and this read sees
    //    it (not the stale cold base) so the second `+1` lands on v+1 → v+2.
    let mut current: HashMap<Vec<u8>, RecordBatch> = HashMap::new();
    // 1a. Tx overlay (highest precedence) — only when running inside a tx.
    if tx_mode {
        for key in keys {
            let kb = key.as_bytes().to_vec();
            let Some(v) = crate::session::tx_overlay_get(&sess.state, table, key) else {
                continue;
            };
            match v {
                basin_hottier::MemRowValue::Row { bytes, .. }
                | basin_hottier::MemRowValue::Update { bytes, .. } => {
                    if let Some(rb) = decode_ipc_single_row(&bytes) {
                        current.insert(kb, reattach_catalog_metadata(schema.as_ref(), rb)?);
                    }
                }
                // Tombstoned earlier in this tx → present-but-dead sentinel so
                // the lower tiers skip it and the UPDATE does not resurrect it.
                basin_hottier::MemRowValue::Tombstone => {
                    current.insert(kb, RecordBatch::new_empty(schema.clone()));
                }
            }
        }
    }
    // 1b. Shared memtable (a prior auto-commit fast-path Update/Insert wins over
    //     cold for the same PK), for any PK not already resolved by 1a.
    {
        let snap = entry.memtable.snapshot();
        for (k, v) in snap {
            let kb = k.as_bytes().to_vec();
            if !want.contains(&kb) || current.contains_key(&kb) {
                continue;
            }
            match v {
                basin_hottier::MemRowValue::Row { bytes, .. }
                | basin_hottier::MemRowValue::Update { bytes, .. } => {
                    if let Some(rb) = decode_ipc_single_row(&bytes) {
                        current.insert(kb, reattach_catalog_metadata(schema.as_ref(), rb)?);
                    }
                }
                // A tombstone for this PK means the row was deleted; an UPDATE
                // must not resurrect it. Mark it present-but-dead by inserting
                // a zero-row sentinel so the cold read below skips it.
                basin_hottier::MemRowValue::Tombstone => {
                    current.insert(kb, RecordBatch::new_empty(schema.clone()));
                }
            }
        }
    }
    // 1c. Probe-carried pre-images (design 1a): the resolve-by-probe SELECT
    //     already materialized every matched row's FULL image, so keys not
    //     overridden by the tx overlay / shared memtable take their pre-image
    //     straight from the probe — eliminating the second cold read the
    //     write step used to pay per statement (unfiltered for >1 key, the
    //     dominant cost at the raised key cap). The probe ran through the
    //     overlay-aware `execute_simple_select`, so these images are never
    //     staler than the cold base; tiers 1a/1b still win because a
    //     concurrent overlay write that landed after the probe is fresher.
    //     Empty for the literal pk-list shapes, which fall to 2a/2b below.
    for (kb, rb) in probe_pre_images {
        if want.contains(kb) && !current.contains_key(kb) {
            current.insert(kb.clone(), rb.clone());
        }
    }
    // 2a. PK row cache fill: for keys not satisfied by the memtable, consult the
    //     always-on PK-row cache for a full-schema cold-row image. On a valid
    //     dual-watermark hit we use it as the merge base and skip the cold
    //     point-lookup for that key. Only the all-columns shape
    //     (`hash_read_cols(None)`) is reused — anything else is a different
    //     projection and would lose columns the merge must preserve.
    if pk_cache_enabled {
        let cache = sess.engine.pk_row_cache();
        for k in keys {
            let kb = k.as_bytes().to_vec();
            if current.contains_key(&kb) {
                continue;
            }
            if let Some(rows) = cache.get(
                &sess.project,
                table,
                k,
                pk_cache_hot_epoch,
                pk_cache_snapshot,
                pk_cache_full_proj,
            ) {
                // The cache stores 0- or 1-row full-schema batches. A 1-row hit
                // is the live cold row; a 0-row hit means the cold tier had no
                // such PK (the merge loop will skip it, exactly as a cold read
                // returning nothing would). Reattach catalog metadata so the
                // image matches the schema the merge expects.
                let row_count: usize = rows.iter().map(|b| b.num_rows()).sum();
                if row_count == 0 {
                    continue; // no live cold row — leave it for the (empty) cold pass.
                }
                if row_count == 1 {
                    let rb = rows
                        .iter()
                        .find(|b| b.num_rows() == 1)
                        .expect("row_count==1 implies one non-empty batch")
                        .clone();
                    current.insert(kb, reattach_catalog_metadata(schema.as_ref(), rb)?);
                }
            }
        }
    }

    // 2b. Cold-tier fill for PKs still not resolved (memtable miss + cache miss).
    //
    // Scalable file prune: for each missing key we run `pk_point_probe`
    // against the catalog's per-file zone-map + bloom and union the candidate
    // file paths. Pre-fix the loop scanned EVERY live data file for the
    // target PK (single-row UPDATE was 166x slower than PG at 100k because
    // `list_data_files_with_stats` returned ~50-100 files and each read every
    // row). With the probe the file set typically collapses to 0 or 1 file
    // per key — making the cold-tier read O(matching files) rather than
    // O(total files). Falls back to the unpruned scan for non-Int64/Utf8
    // PK types where decoding the RowKey back to a ScalarValue isn't
    // supported (the probe is purely an optimisation; the per-row mask
    // below still enforces correctness).
    let missing: Vec<&basin_hottier::RowKey> = keys
        .iter()
        .filter(|k| !current.contains_key(k.as_bytes()))
        .collect();
    if !missing.is_empty() {
        let pk_col = &meta.pk_columns[0];
        let pk_idx = schema.index_of(pk_col).map_err(|_| {
            BasinError::internal(format!("PK column {pk_col:?} missing from schema"))
        })?;
        let pk_dt = schema.field(pk_idx).data_type().clone();

        // Decode each missing RowKey back to a ScalarValue so we can run the
        // catalog probe. None means "this type isn't supported by the probe"
        // → bail to the full-scan path for correctness (we can't lose rows).
        let probe_scalars: Option<Vec<basin_storage::ScalarValue>> = missing
            .iter()
            .map(|k| pk_row_key_to_scalar(k, &pk_dt))
            .collect();

        let catalog_files = meta.live_data_files();
        let pruned_paths: Option<std::collections::HashSet<String>> = match probe_scalars.as_ref() {
            Some(scalars) => {
                let mut paths: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for scalar in scalars {
                    match crate::index_probe::pk_point_probe(
                        pk_col,
                        scalar,
                        &catalog_files,
                        schema.as_ref(),
                    ) {
                        crate::index_probe::PkProbeOutcome::Absent { .. } => {
                            // No live file can contain this PK — the
                            // assignment loop below will skip it as
                            // "PK matched no live row".
                        }
                        crate::index_probe::PkProbeOutcome::Candidates {
                            paths: cands, ..
                        } => {
                            for p in cands {
                                paths.insert(p.to_string());
                            }
                        }
                    }
                }
                Some(paths)
            }
            None => None,
        };

        // #212: single-key UPDATEs push an exact PK equality predicate into
        // the cold file read. The reader turns it into within-file pruning
        // (Parquet row-group stats/bloom/page prune, Vortex native zone
        // maps), so a PK-sorted stripe file costs O(row-group) instead of
        // O(file) — the dominant term at 1M rows where round-robin striping
        // makes every candidate file ~10× bigger than at 100k. Multi-key
        // UPDATEs keep the unfiltered scan: `ReadOptions::filters` is a
        // conjunction, so per-key equality cannot express an OR across keys.
        // The per-row PK match below remains the source of truth either way.
        let single_eq: Option<basin_storage::Predicate> = match probe_scalars.as_deref() {
            Some([scalar]) => {
                Some(basin_storage::Predicate::Eq(pk_col.clone(), scalar.clone()))
            }
            _ => None,
        };

        let data_files = storage.list_data_files_with_stats(&sess.project, table).await?;
        // Defense-in-depth (#94/#95): drop any files the cold-path
        // lister returned that the catalog already considers removed.
        let data_files = filter_to_live_data_files(sess, table, data_files).await?;
        'files: for f in &data_files {
            if current.len() == want.len() {
                break 'files;
            }
            // Skip files that the catalog probe ruled out for every missing
            // PK. When the probe couldn't run (`None`) we fall through to
            // the full scan as before.
            if let Some(ref allow) = pruned_paths {
                if !allow.contains(f.path.as_ref()) {
                    continue;
                }
            }
            // No projection: the merge needs the FULL row image, so only the
            // predicate (row-group / zone-map prune) is pushed down.
            let mut stream = match single_eq.as_ref() {
                Some(pred) => {
                    let opts = basin_storage::ReadOptions {
                        filters: vec![pred.clone()],
                        ..Default::default()
                    };
                    storage
                        .read_file_with_options(&sess.project, &f.path, opts)
                        .await?
                }
                None => storage.read_file(&sess.project, &f.path).await?,
            };
            while let Some(batch) = stream.next().await {
                let batch = reattach_catalog_metadata(schema.as_ref(), batch?)?;
                let pk_array = batch.column(pk_idx);
                for row in 0..batch.num_rows() {
                    let Some(rk) =
                        crate::hot_tombstone::array_value_to_row_key(pk_array.as_ref(), row, &pk_dt)
                    else {
                        continue;
                    };
                    let kb = rk.as_bytes().to_vec();
                    if want.contains(&kb) && !current.contains_key(&kb) {
                        let one = batch.slice(row, 1);
                        // Populate the PK row cache with the full-schema cold
                        // image (all columns → `hash_read_cols(None)`) so a
                        // later UPDATE (or `SELECT *`) for this PK skips the
                        // cold point-lookup. Watermarks were captured at entry,
                        // before this function writes any overlay. Same gate as
                        // the read above: single-PK, RLS-disabled tables only.
                        if pk_cache_enabled {
                            sess.engine.pk_row_cache().insert(
                                &sess.project,
                                table,
                                rk.clone(),
                                pk_cache_hot_epoch,
                                pk_cache_snapshot,
                                pk_cache_full_proj,
                                vec![one.clone()],
                            );
                        }
                        current.insert(kb, one);
                    }
                }
            }
        }
    }

    // 3. Apply the SET to each present, live row image; write the override.
    //
    // Pre-images are gathered (padded to the catalog schema) in key order
    // FIRST so a multi-key expression SET can be evaluated in ONE
    // `apply_assignments` call over a concatenated n-row batch. Each
    // `AssignmentRhs::Expr` evaluation builds a fresh DataFusion
    // SessionContext, registers every UDF family, and plans SQL
    // (`generated_cols::eval_expression`), so the old per-key loop paid n
    // context builds + n plans — the dominant cost of the
    // ≤`delta_update_max_keys()` bulk-RMW shape. The batched call is the SAME
    // evaluator with an all-true mask over all matched rows, exactly how the
    // cold path feeds whole batches to `apply_assignments`, so each row's
    // post-image stays byte-identical to the cold path (and to the previous
    // per-key behavior) for the row-local, deterministic expressions the RMW
    // allowlist admits.
    //
    // Error semantics: the per-key loop applied overrides as it went, so an
    // eval error at key k left keys before k already updated (partial
    // application). The batched eval runs BEFORE any memtable/tx-overlay
    // write, so an expression error now fails the statement atomically with
    // ZERO keys applied — strictly tighter. Single-key and scalar-only
    // statements keep the per-key path (see below) and are unaffected.

    // Live (non-tombstoned) pre-images in key order. Schema evolution: a
    // pre-image sourced from a file older than an ALTER ADD COLUMN lacks the
    // new column entirely; apply_assignments merges per batch-column, so an
    // assignment to the missing column would be silently DROPPED. Pad to the
    // full catalog schema first.
    let mut live: Vec<(&basin_hottier::RowKey, RecordBatch)> = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(row_batch) = current.get(key.as_bytes()) else {
            continue; // PK matched no live row.
        };
        if row_batch.num_rows() == 0 {
            continue; // tombstoned (deleted) — UPDATE skips it.
        }
        live.push((
            key,
            crate::hot_tombstone::pad_batch_to_schema(row_batch.clone(), &schema)?,
        ));
    }

    // Post-images, one single-row batch per live key, in key order (RETURNING
    // depends on this order). Batched eval fires only when (a) at least one
    // assignment is an expression — scalar-only assignment is pure array
    // construction with no DataFusion context, so per-key is already cheap —
    // and (b) more than one key matched (single-key short-circuit: n==1 keeps
    // the historical one-row path with zero concat/slice overhead). If the
    // pre-images can't be stitched into one batch (defensive: physical type
    // drift the pad/normalize pass doesn't cover), fall back to the per-key
    // path rather than failing the statement.
    let has_expr_assignment = assignments
        .iter()
        .any(|(_, rhs)| matches!(rhs, AssignmentRhs::Expr(_)));
    let mut post_images: Option<Vec<RecordBatch>> = None;
    if has_expr_assignment && live.len() > 1 {
        if let Some(joined) = concat_pre_images_for_batch_eval(&live, &schema) {
            let mask = BooleanArray::from(vec![true; joined.num_rows()]);
            let new_batch =
                apply_assignments(catalog, &sess.project, &joined, &mask, assignments).await?;
            post_images = Some(
                (0..new_batch.num_rows())
                    .map(|i| new_batch.slice(i, 1))
                    .collect(),
            );
        }
    }
    let post_images = match post_images {
        Some(rows) => rows,
        None => {
            let mut rows: Vec<RecordBatch> = Vec::with_capacity(live.len());
            for (_, row_batch) in &live {
                let mask = BooleanArray::from(vec![true; row_batch.num_rows()]);
                let new_batch =
                    apply_assignments(catalog, &sess.project, row_batch, &mask, assignments)
                        .await?;
                rows.push(if new_batch.num_rows() == 1 {
                    new_batch
                } else {
                    new_batch.slice(0, 1)
                });
            }
            rows
        }
    };

    // Pair each live key with its post-image and hand the whole set to the
    // shared overlay-write helper, which stages the promoted-shadow encode,
    // reserves the budget (returning the `Ok(None)` decline sentinel on
    // `HardCapReached`), and writes every override to the memtable (auto-
    // commit) or the rollback-able tx overlay (`tx_mode`). The helper is also
    // the write tail for `exec_update_from`, whose per-key RHS values come
    // from a join rather than a single uniform assignment set.
    let key_post_images: Vec<(basin_hottier::RowKey, RecordBatch)> = live
        .iter()
        .map(|(k, _)| (*k).clone())
        .zip(post_images.into_iter())
        .collect();
    // Change-event pre-images: capture the FULL prior row per key ONLY when a
    // sink is attached (lazy — the zero-sink OLTP hot path leaves this empty and
    // `write_overlay_post_images` skips event work entirely, preserving the
    // 1M-row benchmark shape). `live` already holds each key's padded pre-image
    // batch, so this is a borrow-clone, not an extra read.
    let event_pre_images: std::collections::HashMap<Vec<u8>, RecordBatch> =
        if post_commit_sink_attached(sess) {
            live.iter()
                .map(|(k, b)| (k.as_bytes().to_vec(), b.clone()))
                .collect()
        } else {
            std::collections::HashMap::new()
        };
    let Some(()) = write_overlay_post_images(
        sess,
        table,
        meta,
        &key_post_images,
        tx_mode,
        &event_pre_images,
    )?
    else {
        return Ok(None);
    };

    let updated = key_post_images.len();
    // Post-update row images collected for RETURNING (allocated only when needed).
    let mut returning_rows: Vec<RecordBatch> = Vec::new();
    if let Some(items) = returning {
        for (_, one_row) in &key_post_images {
            returning_rows.push(project_post_image_for_returning(
                one_row,
                schema.as_ref(),
                items,
            )?);
        }
    }
    Ok(Some((updated, returning_rows)))
}

/// Stage + budget-guard + write a set of `(key, post_image)` overrides to the
/// hot-tier overlay. Factored out of [`hot_tier_update_by_pk`] so the
/// UPDATE…FROM path (whose RHS values are per-row, from a join, and so cannot
/// be expressed as one uniform [`AssignmentRhs`] set) reuses the IDENTICAL
/// staging / promoted-shadow / memory-budget / overlay-write logic instead of
/// a per-row SQL re-execution loop.
///
/// Each `post_image` is the full single-row catalog-schema batch the row will
/// have AFTER the update (the caller evaluated the assignments). We:
///   * materialise the promoted-JSONB shadow column(s) on each row (ADR 0027
///     Phase 4 — without it the overlay row null-fills `__promoted$col$key`
///     and the promoted-column read fast path returns a wrong NULL);
///   * encode the exact IPC blob the memtable retains;
///   * reserve the summed blob bytes against the project memtable budget
///     BEFORE the first write (auto-commit only — `tx_mode` writes go to the
///     session-local, non-registry-accounted `TxState::tx_overlay`).
///
/// Returns:
///   * `Ok(Some(()))` — every override written.
///   * `Ok(None)` — budget decline (`HardCapReached`): NOTHING was written
///     (staging is pure computation, the reservation fires before the first
///     insert), so the caller may fall back to a cold rewrite with zero
///     partial state.
fn write_overlay_post_images(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    key_post_images: &[(basin_hottier::RowKey, RecordBatch)],
    tx_mode: bool,
    // Per-key FULL pre-image (keyed by encoded PK bytes) for CHANGE-EVENT
    // capture. EMPTY when no change-event sink is attached — the caller
    // (`hot_tier_update_by_pk` / `exec_update_from`) gathers these lazily,
    // gated on `sinks_attached`, so the zero-sink OLTP hot path passes an empty
    // map and pays nothing here beyond the staged-budget logic it always ran.
    // When non-empty we emit one UPDATE change event per `(pre, post)` pair
    // AFTER the overlay write lands (post-commit semantics), covering BOTH the
    // single-table fast path and UPDATE…FROM through this one seam.
    event_pre_images: &std::collections::HashMap<Vec<u8>, RecordBatch>,
) -> Result<Option<()>> {
    let registry = sess.engine.memtable_registry();
    let entry = registry.get_or_create(sess.project, table.clone());

    // Stage every override BEFORE writing any of them: per key, materialise
    // the promoted-JSONB shadow columns and encode the exact IPC blob the
    // memtable will retain. Staging first serves the memory guard below —
    // the statement's overlay footprint is the sum of these blob lengths,
    // known exactly before the first insert.
    let mut staged: Vec<(&basin_hottier::RowKey, Vec<u8>)> =
        Vec::with_capacity(key_post_images.len());
    for (key, one_row) in key_post_images {
        let override_row = crate::promoted_columns::materialize_promoted_columns(
            one_row,
            &meta.promoted_jsonb_paths,
        )?;
        let bytes = encode_single_row_ipc(&override_row);
        staged.push((key, bytes));
    }

    // ── Memory guard (budget reservation) ───────────────────────────────────
    //
    // Reserve the statement's exact overlay footprint against the project's
    // memtable budget BEFORE the first overlay write. This is what makes the
    // raised `delta_update_max_keys()` cap safe: the budget — not the key
    // count — bounds resident memory. `HardCapReached` → `Ok(None)` decline;
    // nothing has been written (staging is pure computation), so the decline
    // leaves zero partial state. The summed IPC blob lengths are EXACTLY the
    // `heap_bytes` the memtable charges and exactly what `release_bytes`
    // returns on drain, so reserve-units == release-units. `tx_mode` is
    // exempt: the override goes to the session-local `TxState::tx_overlay`,
    // which is not registry-accounted.
    if !tx_mode {
        let staged_bytes: u64 = staged.iter().map(|(_, b)| b.len() as u64).sum();
        if staged_bytes > 0
            && registry.try_reserve_bytes(&sess.project, staged_bytes)
                == basin_hottier::ReservationOutcome::HardCapReached
        {
            return Ok(None);
        }
    }

    for (key, bytes) in staged {
        if tx_mode {
            // In-tx: write the override to the rollback-able tx overlay. On
            // COMMIT it is drained into this same shared memtable; on ROLLBACK
            // it is dropped.
            crate::session::tx_overlay_put(
                &sess.state,
                table,
                key.clone(),
                basin_hottier::MemRowValue::update(bytes, 0),
            );
        } else {
            entry
                .memtable
                .insert(key.clone(), basin_hottier::MemRowValue::update(bytes, 0));
        }
    }

    // ── Post-commit change-event capture (CDC / realtime) ────────────────────
    //
    // The overlay write above IS this fast path's commit, so we dispatch (or,
    // in-tx, buffer) AFTER it. `event_pre_images` is empty unless a sink is
    // attached (the caller's lazy gate), so the no-sink hot path skips this
    // entire block on the first `is_empty` check inside
    // `emit_or_buffer_change_events`. Each `(pre, post)` pair becomes one
    // UPDATE event; the post-image is the catalog-schema single-row batch we
    // just wrote, the pre-image the caller's full prior row image.
    if !event_pre_images.is_empty() {
        let mut rows: Vec<RowChange> = Vec::with_capacity(key_post_images.len());
        for (key, post_row) in key_post_images {
            let Some(pre_row) = event_pre_images.get(key.as_bytes()) else {
                // No captured pre-image for this key (e.g. it was sourced from a
                // tier the caller didn't snapshot). Skip rather than emit a
                // half-formed event — correctness over completeness.
                continue;
            };
            let before = if pre_row.num_rows() >= 1 {
                Some(build_row_json(pre_row, 0)?)
            } else {
                None
            };
            let after = if post_row.num_rows() >= 1 {
                Some(build_row_json(post_row, 0)?)
            } else {
                None
            };
            rows.push(RowChange { before, after });
        }
        emit_or_buffer_change_events(sess, table, ChangeOp::Update, rows, tx_mode);
    }

    Ok(Some(()))
}

/// Stitch the per-key single-row pre-images into ONE n-row batch (key order
/// preserved) for the batched `apply_assignments` call in
/// [`hot_tier_update_by_pk`]. Every input was already padded/normalized to
/// the catalog `schema` by `pad_batch_to_schema`; each row is rebuilt against
/// the exact same `SchemaRef` so the concat can't trip over per-batch
/// metadata/nullability drift. Returns `None` (the caller falls back to the
/// per-key eval path) when any row's physical column types still diverge from
/// the catalog schema — defensive; not expected after the pad pass.
fn concat_pre_images_for_batch_eval(
    live: &[(&basin_hottier::RowKey, RecordBatch)],
    schema: &Arc<Schema>,
) -> Option<RecordBatch> {
    let mut rows: Vec<RecordBatch> = Vec::with_capacity(live.len());
    for (_, b) in live {
        rows.push(RecordBatch::try_new(schema.clone(), b.columns().to_vec()).ok()?);
    }
    arrow_select::concat::concat_batches(schema, &rows).ok()
}

/// Decode one Arrow IPC stream blob into the first `RecordBatch`.
fn decode_ipc_single_row(bytes: &[u8]) -> Option<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

/// Project a single post-update row batch to the columns requested in a
/// RETURNING list, for use in the hot-tier UPDATE fast path.
///
/// The items slice must have passed `returning_is_fast_path_eligible` —
/// only `*`, `col`, and `col AS alias` are handled; anything else is a
/// logic error that returns an `InvalidSchema` error rather than panicking.
///
/// Output schema: columns appear in the order the RETURNING list specifies.
/// `*` expands to all columns in left-to-right schema order. `col AS alias`
/// renames the output field to `alias`.
fn project_post_image_for_returning(
    row: &RecordBatch,
    schema: &Schema,
    items: &[SelectItem],
) -> Result<RecordBatch> {
    use arrow_schema::FieldRef;
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<FieldRef> = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                for (i, f) in schema.fields().iter().enumerate() {
                    columns.push(row.column(i).clone());
                    fields.push(f.clone());
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = schema.index_of(&ident.value).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "RETURNING: unknown column {:?}",
                        ident.value
                    ))
                })?;
                columns.push(row.column(idx).clone());
                fields.push(schema.field(idx).clone().into());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                alias,
            } => {
                let idx = schema.index_of(&ident.value).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "RETURNING: unknown column {:?}",
                        ident.value
                    ))
                })?;
                let orig = schema.field(idx);
                let renamed = Arc::new(arrow_schema::Field::new(
                    alias.value.as_str(),
                    orig.data_type().clone(),
                    orig.is_nullable(),
                ));
                columns.push(row.column(idx).clone());
                fields.push(renamed);
            }
            other => {
                return Err(BasinError::internal(format!(
                    "project_post_image_for_returning: unexpected item {other} (should have \
                     been rejected by returning_is_fast_path_eligible)"
                )));
            }
        }
    }
    let out_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(out_schema, columns)
        .map_err(|e| BasinError::internal(format!("RETURNING fast-path projection: {e}")))
}

/// Decode the `RowKey` encoding back to a `ScalarValue` for the UPDATE
/// fastpath's catalog probe. The `RowKey` wire format is the order-preserving
/// encoding from `basin_hottier::RowKeyBuilder` — Int64 is bias-flipped
/// big-endian (XOR sign bit), Utf8 is raw bytes with `0x00` escaping (`0x00`
/// → `0x00 0xFF`) and a final `0x00` terminator.
///
/// Returns `None` for unsupported column types so the caller can degrade
/// gracefully to a full data-file scan rather than mis-decode and skip
/// rows. This is purely an optimisation hook — the per-row PK match in
/// the cold-tier loop remains the source of truth.
fn pk_row_key_to_scalar(
    key: &basin_hottier::RowKey,
    dt: &arrow_schema::DataType,
) -> Option<basin_storage::ScalarValue> {
    use arrow_schema::DataType as Dt;
    let bytes = key.as_bytes();
    match dt {
        Dt::Int64 => {
            if bytes.len() != 8 {
                return None;
            }
            let arr: [u8; 8] = bytes.try_into().ok()?;
            let u = u64::from_be_bytes(arr);
            let v = (u ^ 0x8000_0000_0000_0000u64) as i64;
            Some(basin_storage::ScalarValue::Int64(v))
        }
        Dt::Utf8 | Dt::LargeUtf8 => {
            // Reverse the NUL-escape encoding: `0x00 0xFF` → `0x00`; an
            // unescaped `0x00` is the terminator.
            let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
            let mut i = 0;
            while i < bytes.len() {
                let b = bytes[i];
                if b == 0x00 {
                    if i + 1 < bytes.len() && bytes[i + 1] == 0xFF {
                        out.push(0x00);
                        i += 2;
                    } else {
                        // Unescaped 0x00 → terminator.
                        break;
                    }
                } else {
                    out.push(b);
                    i += 1;
                }
            }
            String::from_utf8(out)
                .ok()
                .map(basin_storage::ScalarValue::Utf8)
        }
        // Other types (UUID, Decimal, etc.) — the probe would need a
        // type-specific bloom encoding that may not yet exist; return None
        // so the caller falls back to the unpruned scan.
        _ => None,
    }
}

/// Encode a single-row `RecordBatch` to Arrow IPC stream bytes (memtable wire
/// format — mirrors `executor::encode_batch_to_ipc`).
fn encode_single_row_ipc(batch: &RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
        .expect("IPC StreamWriter init");
    writer.write(batch).expect("IPC write");
    writer.finish().expect("IPC finish");
    buf
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

    // Flush the INSERT tail only when one exists, BEFORE the fast-path gate
    // so a just-INSERTed-same-row is visible to BOTH the fast-path read and
    // the cold read. The cold `materialize_hot_overlay_into_cold` is moved to
    // the cold fall-through below — the fast path (`hot_tier_update_by_pk`)
    // reads memtable-override-first, so a prior overlay is already visible and
    // does not need materializing (#205).
    pre_mutation_flush_if_tail(sess, &table).await?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();
    let storage = sess.engine.config().storage.clone();

    // RLS USING enforcement on UPDATE (mirrors the DELETE P0 #56 fix and the
    // SELECT path). WITH CHECK on the post-SET image only catches an UPDATE
    // that moves a row OUT of the policy; it does NOT stop an UPDATE that
    // leaves the policy columns unchanged (e.g. `SET v = 999` on a row owned
    // by another user, including via the ON CONFLICT DO UPDATE upsert path).
    // Without a USING filter the cold rewrite would match — and silently
    // overwrite — rows the current user cannot see. AND the policies' USING
    // predicates into the user's WHERE so the update only ever touches rows
    // the policy makes visible: combine the applicable USING predicates
    // (permissive → OR) and AND the result in. With RLS on but no applicable
    // policy the helper returns `(FALSE)` (Postgres default-deny); with RLS
    // off it returns `None` and the path is untouched.
    let selection: Option<Expr> = {
        let rls_using_sql = crate::rls::build_using_predicate_sql_for_kind(
            meta.rls_enabled,
            &meta.policies,
            &sess.current_user,
            basin_catalog::PolicyCommand::Update,
        )
        // Resolve auth_uid()/auth_role() to literals: the cold UPDATE filters
        // rows through the compound-predicate engine, which cannot evaluate
        // UDFs, so a policy like `owner_id = auth_uid()` must become
        // `owner_id = '<uuid>'` before parse_compound_predicate sees it.
        .map(|sql| crate::rls::substitute_auth_functions(&sql, &sess.auth_context));
        match (selection, rls_using_sql) {
            (sel, None) => sel,
            (None, Some(rls_sql)) => Some(parse_sql_expr_fragment(&rls_sql)?),
            (Some(user), Some(rls_sql)) => {
                let rls_expr = parse_sql_expr_fragment(&rls_sql)?;
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(user))),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::Nested(Box::new(rls_expr))),
                })
            }
        }
    };

    // ── Hot-tier fast path ──────────────────────────────────────────────
    //
    // For `SET col = lit` (scalar) OR an allowlisted read-modify-write
    // `SET col = <expr>` (`col + 1`, `CASE …`, `a = b`, simple casts — see
    // `rmw_rhs_is_fast_path_eligible`), with `WHERE pk = lit / pk IN (lits)`,
    // any probe-resolvable predicate matching ≤ `delta_update_max_keys()`
    // rows, or NO WHERE at all on a ≤cap table, on a table that satisfies
    // every gate in `try_resolve_fast_path_update`, read the matched row
    // image (hot, probe-carried, then cold), apply SET, and write a
    // `MemRowValue::Update` override to the process-wide MemTableRegistry —
    // skipping the copy-on-write rewrite (`list_data_files_with_stats` →
    // per-file read+SET → `write_replacement` → `commit_replace` →
    // `delete_objects` → `refresh_table`). Mirrors the DELETE fast path.
    if let Some((pk_keys, fp_assigns, probe_pre_images)) = try_resolve_fast_path_update(
        sess,
        &table,
        &meta,
        &assignments,
        selection.as_ref(),
        returning.as_deref(),
    )
    .await?
    {
        // In-tx → tx-overlay variant (rollback-able); auto-commit → shared
        // registry. The gate only admitted the in-tx case when
        // `tx_fastpath_eligible_for_table` held.
        //
        // `hot_tier_update_by_pk` returns `Ok(None)` — the budget-decline
        // sentinel — when the project memtable hard cap cannot admit the
        // gathered pre-image bytes. In that case we DON'T return: control
        // falls through this block to the cold copy-on-write path below,
        // which discards the probe's key set and re-evaluates the original
        // predicate itself (it never looks at `pk_keys`), and whose
        // `materialize_hot_overlay_into_cold` prologue drains overlay memory
        // — the correct remedy for a full budget. No partial state exists at
        // decline time (the sentinel fires before any overlay write).
        let tx_mode = crate::session::tx_is_active(&sess.state);
        if let Some((n, ret_batches)) = hot_tier_update_by_pk(
            sess,
            &table,
            &meta,
            &pk_keys,
            &fp_assigns,
            &probe_pre_images,
            returning.as_deref(),
            tx_mode,
        )
        .await?
        {
            if n == 0 {
                return Ok(empty_or_returning(
                    "UPDATE 0",
                    schema.clone(),
                    returning.as_deref(),
                ));
            }
            // Fast-path RETURNING: the post-image batches were already projected
            // inside `hot_tier_update_by_pk`; just wrap them in ExecResult::Rows.
            if let Some(items) = returning.as_deref() {
                let ret_schema = if let Some(first) = ret_batches.first() {
                    first.schema()
                } else {
                    Arc::new(projected_returning_schema(schema.as_ref(), items))
                };
                return Ok(ExecResult::Rows {
                    schema: ret_schema,
                    batches: ret_batches,
                });
            }
            return Ok(ExecResult::Empty {
                tag: format!("UPDATE {n}"),
            });
        }
    }

    // ── Cold copy-on-write path ─────────────────────────────────────────
    //
    // The cold rewrite reads the RAW base via `list_data_files_with_stats`,
    // which is NOT overlay-aware: a prior hot-tier UPDATE/DELETE override
    // would not be seen, so without materializing it first the rewrite would
    // operate on stale cold data while the un-cleared overlay shadows the
    // result on read (#94/#95). Materialize the overlay into cold BEFORE we
    // list the base files. This runs only on the cold fall-through (a fast
    // path that RAN returned above; a budget-declined fast path reaches here
    // having written nothing), so an update-heavy fast-path loop no
    // longer pays the eager re-encode + commit on every UPDATE (#205).
    // Only the shapes the fast path declined (un-probe-able WHERE, >cap
    // matched sets, non-allowlisted RMW expressions, constrained tables,
    // memtable-budget HardCapReached, …) reach this materialization — and for
    // the budget decline it is also the remedy: materializing drains the
    // overlay and releases its reserved bytes.
    materialize_hot_overlay_into_cold(sess, &table).await?;
    // Re-load metadata: materialize advances the table snapshot, so `meta`
    // (and the schema/live-files derived from it) must reflect the post-
    // materialize state before any cold read/commit decision.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();

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

    let data_files = storage
        .list_data_files_with_stats(&sess.project, &table)
        .await?;
    // Defense-in-depth (#94/#95): filter out files the catalog
    // already removed even if the async cleanup hasn't unlinked them.
    let data_files = filter_to_live_data_files(sess, &table, data_files).await?;
    if data_files.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "UPDATE 0".into(),
        });
    }

    let pred = match &selection {
        None => None,
        Some(e) => Some(parse_compound_predicate(e, schema.as_ref(), table.as_str())?),
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
    // Note: expression-RHS assignments do NOT force capture. Both the serial
    // and the parallel branch evaluate SET expressions through the same
    // `apply_assignments` → `eval_expression` call against the pre-update
    // batch (PG semantics: the RHS sees the OLD row values), so an
    // expression-only UPDATE with no sinks / audit / generated columns /
    // RETURNING takes the parallel branch and skips the per-row
    // before/after RowChange JSON that nothing would consume.
    let want_returning_rows = returning.is_some();
    let capture_events = sinks_attached(sess)
        || audit_table.is_some()
        || has_generated_cols
        || want_returning_rows;

    // Walk files. Unlike DELETE, an AllMatch UPDATE still has to read the
    // file to apply SET to every row.
    let mut updated_total: usize = 0;
    let mut replaced_paths: Vec<String> = Vec::new();
    let mut replacement_batches: Vec<RecordBatch> = Vec::new();
    // Bulk-W4 / Inv-bulk-UPDATE #182: per-source-file grouping of the
    // post-SET batches. The write path turns each `(old_path, batches)`
    // entry into its OWN replacement file (preserving on-disk granularity)
    // rather than concat-ing every file's rows into one mega-file. The flat
    // `replacement_batches` / `replaced_paths` above are still derived from
    // these groups for the constraint / RLS / CDC / RETURNING passes that
    // don't care about file boundaries.
    let mut replacement_groups: Vec<(String, Vec<RecordBatch>)> = Vec::new();
    let mut event_payloads: Vec<RowChange> = Vec::new();
    // Post-update rows that matched (RETURNING input). Each entry is one
    // filtered batch with only the matched rows.
    let mut returning_input: Vec<RecordBatch> = Vec::new();

    // Inv-bulk-UPDATE #182: the serial read-modify-write loop over data
    // files is the dominant cost (80-90%) of bulk UPDATE. Files are
    // independent in the NO-SINK / no-capture common case, so we fan the
    // read+mask+apply out with `buffer_unordered` and reassemble in a
    // deterministic order afterwards.
    //
    // The capture_events path (CDC sinks, audit, generated columns,
    // RETURNING) keeps the serial loop: event ordering and the per-file
    // before/after pairing are correctness-load-bearing there.
    if capture_events {
        for f in &data_files {
            let outcome = file_outcome(pred.as_ref(), f, schema.as_ref());
            match (outcome, &pred) {
                (PruneOutcome::NoMatch, Some(_)) => {
                    // Pass-through. No read, no write.
                }
                // AllMatch with predicate, or no predicate at all: every row
                // is matched. We still need the file's contents to apply SET.
                (PruneOutcome::AllMatch, _) | (PruneOutcome::Mixed, None) => {
                    let catalog = &sess.engine.config().catalog;
                    let befores =
                        read_file_to_batches(&storage, &sess.project, &f.path, schema.as_ref())
                            .await?;
                    let mut new_batches = apply_assignments_all(
                        &sess.engine.config().catalog,
                        &sess.project,
                        &befores,
                        &assignments,
                    )
                    .await?;
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
                    replacement_groups
                        .push((f.path.as_ref().to_string(), new_batches.clone()));
                    capture_update_events(&befores, &new_batches, None, &mut event_payloads)?;
                    if want_returning_rows {
                        // AllMatch: every row matches; the unfiltered
                        // post-update batches feed RETURNING directly.
                        for b in &new_batches {
                            returning_input.push(b.clone());
                        }
                    }
                    replacement_batches.extend(new_batches);
                }
                (PruneOutcome::Mixed, Some(p)) => {
                    let catalog = &sess.engine.config().catalog;
                    let befores =
                        read_file_to_batches(&storage, &sess.project, &f.path, schema.as_ref())
                            .await?;
                    let mut mask_per_batch = Vec::with_capacity(befores.len());
                    let mut new_batches = Vec::with_capacity(befores.len());
                    let mut rows_matched = 0usize;
                    for b in &befores {
                        let mask = evaluate_compound(b, p).map_err(|e| {
                            BasinError::internal(format!("update predicate eval: {e}"))
                        })?;
                        rows_matched += mask.iter().filter(|x| matches!(x, Some(true))).count();
                        new_batches.push(
                            apply_assignments(catalog, &sess.project, b, &mask, &assignments)
                                .await?,
                        );
                        mask_per_batch.push(mask);
                    }
                    if rows_matched == 0 {
                        // Stats said maybe-match but no rows actually matched
                        // — pass through instead of pointlessly rewriting.
                        continue;
                    }
                    if has_generated_cols {
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
                    replacement_groups
                        .push((f.path.as_ref().to_string(), new_batches.clone()));
                    capture_update_events(
                        &befores,
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
                                    BasinError::internal(format!("filter returning batch: {e}"))
                                })?;
                            if filtered.num_rows() > 0 {
                                returning_input.push(filtered);
                            }
                        }
                    }
                    replacement_batches.extend(new_batches);
                }
                // AllMatch + None handled above; this branch is unreachable
                // in practice but kept for the exhaustive match.
                (PruneOutcome::NoMatch, None) => unreachable!(),
            }
        }
    } else {
        // Parallel read-modify-apply. Each file is independent: the
        // predicate and SET assignments are read-only shared state, every
        // file produces its own replacement batches with no cross-file
        // mutation. We collect `Option<PerFileUpdate>` (None = pruned /
        // no-op pass-through) and sort by original file path so the
        // downstream commit, PK/UNIQUE enforcement, and GIN/GIST index
        // maintenance see a deterministic file order.
        // Shared state captured by each per-file future is wrapped in `Arc`
        // (or is already cheaply clonable) so the spawned futures own their
        // captures — borrowing `&str`/`&ProjectSession` across the
        // `buffer_unordered` await points trips a higher-ranked `Send`
        // inference failure when the whole UPDATE future is later spawned
        // (reactor sink path), so we hand each file an owned clone instead.
        let catalog = sess.engine.config().catalog.clone();
        let storage_arc = storage.clone();
        let assignments_arc = Arc::new(assignments.clone());
        let pred_arc = Arc::new(pred.clone());
        let project_id = sess.project.clone();
        let concurrency = update_scan_concurrency(data_files.len());
        let results: Vec<Option<PerFileUpdate>> = futures::stream::iter(data_files.iter().cloned())
            .map(|f| {
                let catalog = catalog.clone();
                let storage = storage_arc.clone();
                let assignments = assignments_arc.clone();
                let pred = pred_arc.clone();
                let project = project_id.clone();
                let schema = schema.clone();
                apply_update_to_file(catalog, storage, project, schema, pred, assignments, f)
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<Result<Option<PerFileUpdate>>>>()
            .await
            .into_iter()
            // Propagate the first error — never silently drop a failed file.
            .collect::<Result<Vec<Option<PerFileUpdate>>>>()?;

        // Deterministic reassembly: sort by original file path so
        // replaced_paths and replacement_batches stay in a stable order
        // independent of buffer_unordered completion order.
        let mut updates: Vec<PerFileUpdate> = results.into_iter().flatten().collect();
        updates.sort_by(|a, b| a.path.cmp(&b.path));
        for u in updates {
            updated_total += u.rows_matched;
            replaced_paths.push(u.path.clone());
            replacement_batches.extend(u.batches.clone());
            replacement_groups.push((u.path, u.batches));
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
                &storage,
                &sess.project,
                &table,
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
                batch,
            )
            .await?;
        }
    }
    // BUG #133: RLS WITH CHECK on UPDATE. The post-SET (new-image)
    // batches must satisfy an applicable policy's WITH CHECK (or USING
    // when no WITH CHECK), or PG raises 42501 — an UPDATE may not move a
    // row out of the policy. No-op when rls_enabled = false (one bool).
    if meta.rls_enabled {
        for batch in &replacement_batches {
            crate::rls::enforce_with_check(
                &sess.auth_context,
                table.as_str(),
                meta.rls_enabled,
                &meta.policies,
                &sess.current_user,
                basin_catalog::PolicyCommand::Update,
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
            sess,
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
    // Phase 5.19.E / Bulk-W4: clone replacement batches per source file for
    // GIN/GIST posting-list maintenance BEFORE the write consumes the groups.
    // Keyed by old_path so each old file's purge is paired with the SAME new
    // file that now physically holds its post-SET rows.
    let gin_batches_by_old_path: std::collections::HashMap<String, Vec<RecordBatch>> =
        replacement_groups
            .iter()
            .map(|(p, b)| (p.clone(), b.clone()))
            .collect();
    drop(replacement_batches);
    // Pre-commit before writing the replacement file so a rejecting
    // sink leaves no orphan parquet on disk.
    dispatch_pre_commit(&sess.engine, &events).await?;
    // Bulk-W4: write N replacement files (one per source file) in parallel
    // instead of concat-ing every file's rows into a single serial encode.
    // `per_file` is `(old_path, new DataFileRef)` sorted by old_path; we
    // derive the flat `added_files` for the commit and keep the mapping for
    // precise per-file index maintenance below.
    let per_file =
        write_replacement_per_file(sess, &table, schema.clone(), replacement_groups).await?;
    let added_files: Vec<DataFileRef> = per_file.iter().map(|(_, df)| df.clone()).collect();
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        replaced_paths.clone(),
        added_files.clone(),
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);

    // Phase 5.19.E / Bulk-W4 — GIN posting-list maintenance for UPDATE.
    // Each replaced file's old entries are purged and re-emitted against the
    // SPECIFIC new file that now holds its post-SET rows (per-file granularity
    // — was a single lumped new file before Bulk-W4's multi-file write).
    {
        use basin_storage::index::index_maint::GinPostingListMaintainer;
        if let Some(maint) = GinPostingListMaintainer::new(
            sess.engine.gin_index_registry().as_ref(),
            &sess.project,
            &table,
            &meta,
        ) {
            let rewritten: std::collections::HashSet<&str> =
                per_file.iter().map(|(p, _)| p.as_str()).collect();
            for (old_path, new_file) in &per_file {
                if let Some(batches) = gin_batches_by_old_path.get(old_path) {
                    maint.on_file_replaced(old_path, &new_file.path, batches);
                }
            }
            // Defensive: a replaced file that produced no new file (empty
            // post-SET result) still must have its old posting-list entries
            // purged so we never serve stale postings.
            for old_path in &replaced_paths {
                if !rewritten.contains(old_path.as_str()) {
                    maint.on_file_removed(old_path);
                }
            }
        }
    }

    // Phase 5.24.D / Bulk-W4 — GIST interval-tree maintenance for UPDATE.
    // Each replaced file's entries are purged; the matching new file's entries
    // are rebuilt from that file's own post-SET batches.
    {
        let gist_cols: Vec<String> = meta
            .indexes
            .iter()
            .filter(|idx| idx.access_method == "gist" && idx.columns.len() == 1)
            .map(|idx| idx.columns[0].clone())
            .collect();
        if !gist_cols.is_empty() {
            let ireg = sess.engine.interval_registry();
            let rewritten: std::collections::HashSet<&str> =
                per_file.iter().map(|(p, _)| p.as_str()).collect();
            for col in &gist_cols {
                // Purge any replaced file that produced no new file.
                for old_path in &replaced_paths {
                    if !rewritten.contains(old_path.as_str()) {
                        ireg.remove_file(&sess.project, &table, col, old_path);
                    }
                }
                for (old_path, new_file) in &per_file {
                    ireg.remove_file(&sess.project, &table, col, old_path);
                    let Some(batches) = gin_batches_by_old_path.get(old_path) else {
                        continue;
                    };
                    for batch in batches {
                        use arrow_array::Array;
                        if let Ok(col_idx) = batch.schema().index_of(col) {
                            let col_arr = batch.column(col_idx);
                            if let Some(arr) = col_arr.as_any().downcast_ref::<arrow_array::StringArray>() {
                                for row in 0..arr.len() {
                                    if arr.is_null(row) { continue; }
                                    ireg.index_row(&sess.project, &table, col, arr.value(row), &new_file.path, 0);
                                }
                            }
                        }
                    }
                    ireg.mark_file_indexed(&sess.project, &table, col, &new_file.path);
                }
            }
        }
    }

    // B-tree secondary-index maintenance for UPDATE: purge each replaced
    // file's stale locations and re-register the SPECIFIC new file that now
    // holds its post-SET rows (same per-file pairing as the GIN/GIST blocks
    // above). See `maintain_btree_secondary_on_replace` for why re-register
    // (not just purge) is required for read soundness.
    {
        let rewrites: Vec<(String, Vec<RecordBatch>)> = per_file
            .iter()
            .filter_map(|(old_path, new_file)| {
                gin_batches_by_old_path
                    .get(old_path)
                    .map(|b| (new_file.path.clone(), b.clone()))
            })
            .collect();
        // Box::pin: keeps the maintenance future's locals off the caller's
        // poll frame (exec_update sits inside recursive DML chains too).
        Box::pin(maintain_btree_secondary_on_replace(
            sess,
            &table,
            &meta,
            &replaced_paths,
            &rewrites,
        ))
        .await;
    }

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
    let pk_proj: Vec<String> = pk.iter().map(|c| format!("{target_str}.{c}")).collect();

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
        ExecResult::Empty { .. } => {
            return Ok(ExecResult::Empty {
                tag: "DELETE 0".into(),
            })
        }
    };

    // ── Hot-tier fast path (single-PK) ──────────────────────────────────────
    //
    // When the target satisfies the same table-level gates as the point DELETE
    // fast path (single-column PK, no RLS / soft-delete / audit / DELETE reactor
    // / secondary index / inbound FK), no event sink is attached, and the in-tx
    // eligibility holds, tombstone the join's matched PKs DIRECTLY. This skips
    // the `build_pk_in_predicate` string assembly (which materialises a
    // `pk IN (v1,…,vN)` clause megabytes wide for a high-cardinality join) and
    // the subsequent full DELETE re-parse / re-plan / fast-path re-resolution —
    // the source of the quadratic blow-up that made join-DELETE 100s of x slower
    // than Postgres on a million-row match. The join SELECT above already found
    // the rows; we encode their PKs straight to RowKeys and write tombstones,
    // mirroring `exec_update_from`'s "never round-trip through SQL" strategy.
    let tx_active = crate::session::tx_is_active(&sess.state);
    let fast_eligible = hottier_fastpath_enabled("BASIN_HOTTIER_DELETE_FASTPATH")
        && !post_commit_sink_attached(sess)
        && (!tx_active || tx_fastpath_eligible_for_table(sess, target))
        && delete_fastpath_table_eligible(sess, target, &meta).await?;
    if fast_eligible {
        // The join projection's column 0 IS `target.<pk>` (single-PK guaranteed
        // by the eligibility gate). Encode each value to a RowKey, deduplicating
        // so the affected-row count is DISTINCT target rows (the join can match a
        // target row more than once; PG reports rows-deleted, not join
        // cardinality). A value whose type has no RowKey encoder (exotic PK)
        // yields `None` → fall through to the SQL predicate path unchanged.
        let pk_col = &pk[0];
        let pk_idx = meta.schema.index_of(pk_col).map_err(|_| {
            BasinError::internal(format!("PK column {pk_col:?} missing from schema"))
        })?;
        let pk_dt = meta.schema.field(pk_idx).data_type().clone();
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut keys: Vec<basin_hottier::RowKey> = Vec::new();
        let mut unsupported_pk = false;
        'batches: for batch in &batches {
            let arr = batch.column(0);
            for row in 0..batch.num_rows() {
                match crate::hot_tombstone::array_value_to_row_key(arr.as_ref(), row, &pk_dt) {
                    Some(rk) => {
                        if seen.insert(rk.as_bytes().to_vec()) {
                            keys.push(rk);
                        }
                    }
                    None => {
                        unsupported_pk = true;
                        break 'batches;
                    }
                }
            }
        }
        if !unsupported_pk {
            if keys.is_empty() {
                return Ok(ExecResult::Empty {
                    tag: "DELETE 0".into(),
                });
            }
            let n = if tx_active {
                hot_tier_delete_by_pk_tx(sess, target, keys)
            } else {
                hot_tier_delete_by_pk(sess, target, keys)
            };
            return Ok(ExecResult::Empty {
                tag: format!("DELETE {n}"),
            });
        }
        // exotic PK type → fall through to the SQL predicate path below.
    }

    // Collect pk tuples.
    let pk_tuples = collect_pk_batches(&batches, pk)?;
    if pk_tuples.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "DELETE 0".into(),
        });
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
/// Set-oriented strategy:
/// 1. Require that the target table has exactly one PK column.
/// 2. Run ONE join SELECT that projects, in catalog-schema column order, the
///    full POST-image of every matched target row: each SET column rendered
///    as its RHS expression (`u.col`, a literal, or any join-evaluable
///    expression — evaluated by DataFusion against the joined pre-image, the
///    PG semantic), every other column as `target.col` (carried unchanged).
///    The join also projects `target.pk` so each result row is keyed.
/// 3. Collect the matched `(key, post_image)` pairs, deduplicating by PK with
///    LAST-occurrence-wins (see "multi-match semantics" below).
/// 4. Write them in ONE batched `write_overlay_post_images` call (the same
///    staging / promoted-shadow / memory-budget / overlay-write tail the
///    single-table fast path uses). On budget decline (`Ok(None)`) fall to
///    ONE batched cold rewrite — never a per-row loop.
///
/// ── Multi-match semantics ────────────────────────────────────────────────
/// PostgreSQL: a target row that joins MORE than one FROM row is updated at
/// most ONCE, using an arbitrary (unspecified) one of the matching source
/// rows (the "join produces a Cartesian product" caveat in the PG docs). The
/// historical Basin loop issued one `UPDATE … WHERE pk = X` per join result
/// row, so for a multi-matched PK the LAST result row's values won (each
/// rewrite clobbered the prior). "Last row in join-result order wins" is a
/// valid concrete choice under PG's "unspecified" contract, so we preserve it
/// exactly: the dedup keeps the last `(key, post_image)` seen in result order.
/// `UPDATE n` therefore counts DISTINCT updated PKs, matching the loop (whose
/// repeated `UPDATE … WHERE pk = X` each reported 1 but mutated the same row)
/// — actually a strictly more PG-faithful count (PG reports rows-affected =
/// distinct target rows, not join cardinality).
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

    // The cold-rewrite fallback below reads the RAW base, which is NOT
    // overlay-aware. Materialise any pending overlay into cold FIRST so a
    // prior hot UPDATE/DELETE on this table is visible to both the join probe
    // (it reads overlay-aware, so this is belt-and-braces there) and the cold
    // fallback (#94/#95). This mirrors the single-table cold prologue.
    pre_mutation_flush_if_tail(sess, &target).await?;

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
    if meta.pk_columns.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "UPDATE … FROM requires the target table {:?} to have a single-column PRIMARY KEY in v0.1",
            target_name
        )));
    }
    let pk_col = meta.pk_columns[0].clone();
    let schema = meta.schema.clone();

    // Collect the SET targets, validating each is a real, writable column and
    // capturing its RHS expression text. We keep the same per-assignment
    // guards `parse_assignments` enforces for the single-table path
    // (generated / ALWAYS-IDENTITY columns are immutable) so UPDATE…FROM can
    // never write a column the single-table path would reject.
    let mut set_idx_to_rhs: std::collections::HashMap<usize, String> =
        std::collections::HashMap::new();
    for a in &assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => single_part_name(name)?.to_string(),
            AssignmentTarget::Tuple(_) => {
                return Err(BasinError::InvalidSchema(
                    "UPDATE FROM: tuple assignment targets not supported in v0.1".into(),
                ))
            }
        };
        let idx = schema.index_of(&col).map_err(|_| {
            BasinError::InvalidSchema(format!("unknown column {col} in UPDATE … FROM target"))
        })?;
        if crate::types::field_is_generated(schema.field(idx)).is_some() {
            return Err(BasinError::InvalidSchema(format!(
                "cannot insert into generated column {:?}",
                schema.field(idx).name()
            )));
        }
        if matches!(
            crate::types::field_identity_mode(schema.field(idx)),
            Some(crate::types::IdentityMode::Always)
        ) {
            return Err(BasinError::InvalidSchema(format!(
                "column {:?} can only be updated to DEFAULT (GENERATED ALWAYS AS IDENTITY)",
                schema.field(idx).name()
            )));
        }
        // Last assignment to a column wins (PG forbids duplicate SET targets,
        // but if the parser admitted one we mirror normal map-overwrite).
        set_idx_to_rhs.insert(idx, a.value.to_string());
    }

    let where_sql = match &selection {
        Some(e) => format!(" WHERE {e}"),
        None => String::new(),
    };

    // ── Project the FULL POST-image in catalog-schema column order ───────────
    //
    // Column 0 is `target.pk` (the dedup / write key). Columns 1..=N are the
    // target's columns in schema order, each either its SET RHS expression
    // (evaluated by DataFusion against the joined pre-image — PG's "RHS sees
    // the OLD target row" semantic) or `target.col` carried unchanged. Because
    // the result row IS the post-image, no per-row assignment re-evaluation is
    // needed downstream: one join evaluates every RHS for every matched row.
    //
    // We qualify carried columns with the target name to disambiguate from
    // identically-named FROM columns; SET RHS expressions are emitted verbatim
    // (the user already qualified `u.col` references). Aliasing each output to
    // the catalog column name keeps the result schema field-name-aligned with
    // the catalog schema for `reattach_catalog_metadata` below.
    let mut proj_parts: Vec<String> = Vec::with_capacity(schema.fields().len() + 1);
    proj_parts.push(format!("{target_name}.{pk_col} AS __pk"));
    for (idx, field) in schema.fields().iter().enumerate() {
        let col = field.name();
        match set_idx_to_rhs.get(&idx) {
            Some(rhs) => proj_parts.push(format!("({rhs}) AS {col}")),
            None => proj_parts.push(format!("{target_name}.{col} AS {col}")),
        }
    }
    // Change-event capture: when a sink is attached, ALSO carry the OLD target
    // row (the join's "pre-image", which it already materialises to evaluate the
    // RHS) as trailing columns so we can emit before/after UPDATE events without
    // a second read. Lazy — appended ONLY under `capture_events`, so the no-sink
    // path projects exactly the post-image columns it did before (byte-identical
    // join shape, no extra evaluation). The N pre-image columns occupy result
    // indices `N+1 ..= 2N`, each carrying the unchanged `target.col`.
    let capture_events = post_commit_sink_attached(sess);
    if capture_events {
        for field in schema.fields().iter() {
            let col = field.name();
            // Quote the alias: the column-position decode below locates these by
            // INDEX, so the alias only needs to be a syntactically valid, unique
            // label — quoting keeps reserved-word / punctuation column names
            // (and the `$` separator) from tripping the parser.
            proj_parts.push(format!("{target_name}.{col} AS \"__before${col}\""));
        }
    }
    let proj = proj_parts.join(", ");

    let from_str = from_table.relation.to_string();
    let join_select = format!(
        "SELECT {proj} FROM {target_name} , {from_str}{wh}",
        wh = where_sql,
    );

    let join_result = Box::pin(sess.execute(&join_select)).await?;
    let batches = match join_result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => {
            return Ok(ExecResult::Empty {
                tag: "UPDATE 0".into(),
            })
        }
    };

    // Decode each result row into a `(RowKey, post_image)` pair, deduplicating
    // by PK with last-occurrence-wins (see the multi-match note above). The
    // post-image is the result row with column 0 (the projected `__pk`)
    // dropped, then padded / type-normalized to the catalog schema exactly as
    // the single-table write path expects.
    let pk_field = schema.field_with_name(&pk_col).map_err(|_| {
        BasinError::InvalidSchema(format!("PK column {pk_col:?} not in table schema"))
    })?;
    let pk_dt = pk_field.data_type().clone();
    // Insertion-ordered dedup: track key → slot so a later row overwrites the
    // earlier post-image while preserving first-seen order (deterministic for
    // the write / RETURNING-count and stable for tests).
    let mut order: Vec<basin_hottier::RowKey> = Vec::new();
    let mut by_key: std::collections::HashMap<Vec<u8>, RecordBatch> =
        std::collections::HashMap::new();
    // Captured pre-images (full prior target row) keyed by PK bytes, populated
    // only under `capture_events`. Empty otherwise → no change-event work.
    let mut pre_by_key: std::collections::HashMap<Vec<u8>, RecordBatch> =
        std::collections::HashMap::new();
    let ncols = schema.fields().len();
    for batch in &batches {
        let pk_arr = batch.column(0);
        // Columns 1..=N are the post-image in catalog-schema order. Coerce each
        // to its catalog field's physical type: a literal / carried-column RHS
        // can come back with a different-but-compatible Arrow type (e.g. an
        // integer literal as a narrower int, a Vortex-narrowed Binary for a
        // catalog LargeBinary), and `RecordBatch::try_new` demands an EXACT
        // type match. `cast` is the same coercion the single-table write path
        // relies on (via `apply_assignments`) to land catalog-typed rows.
        // Under `capture_events` columns N+1..=2N carry the OLD target row.
        let expected_cols = if capture_events { 2 * ncols + 1 } else { ncols + 1 };
        if batch.num_columns() != expected_cols {
            return Err(BasinError::internal(format!(
                "UPDATE … FROM: join projected {} columns, expected {} (pk + {} target cols{})",
                batch.num_columns(),
                expected_cols,
                ncols,
                if capture_events { " + N pre-image cols" } else { "" }
            )));
        }
        let mut post_cols: Vec<arrow_array::ArrayRef> =
            Vec::with_capacity(schema.fields().len());
        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx + 1);
            let coerced = if col.data_type() == field.data_type() {
                col.clone()
            } else {
                arrow::compute::cast(col.as_ref(), field.data_type()).map_err(|e| {
                    BasinError::internal(format!(
                        "UPDATE … FROM: cannot coerce post-image column {:?} from {:?} to catalog type {:?}: {e}",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    ))
                })?
            };
            post_cols.push(coerced);
        }
        let post_batch = RecordBatch::try_new(schema.clone(), post_cols).map_err(|e| {
            BasinError::internal(format!(
                "UPDATE … FROM: post-image projection mismatched the target schema: {e}"
            ))
        })?;
        let post_batch = reattach_catalog_metadata(schema.as_ref(), post_batch)?;
        // Build the catalog-schema pre-image batch from the trailing N columns
        // (`__before$col`) when capturing events. Same per-column coercion as the
        // post-image so `build_row_json` sees catalog-typed arrays.
        let pre_batch: Option<RecordBatch> = if capture_events {
            let mut pre_cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(ncols);
            for (idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(ncols + 1 + idx);
                let coerced = if col.data_type() == field.data_type() {
                    col.clone()
                } else {
                    arrow::compute::cast(col.as_ref(), field.data_type()).map_err(|e| {
                        BasinError::internal(format!(
                            "UPDATE … FROM: cannot coerce pre-image column {:?} from {:?} to catalog type {:?}: {e}",
                            field.name(),
                            col.data_type(),
                            field.data_type()
                        ))
                    })?
                };
                pre_cols.push(coerced);
            }
            let pb = RecordBatch::try_new(schema.clone(), pre_cols).map_err(|e| {
                BasinError::internal(format!(
                    "UPDATE … FROM: pre-image projection mismatched the target schema: {e}"
                ))
            })?;
            Some(reattach_catalog_metadata(schema.as_ref(), pb)?)
        } else {
            None
        };
        for row in 0..batch.num_rows() {
            let Some(rk) =
                crate::hot_tombstone::array_value_to_row_key(pk_arr.as_ref(), row, &pk_dt)
            else {
                // NULL / unsupported-type PK can't be keyed — a PK is NOT NULL,
                // so this is unreachable for a real target row; skip defensively.
                continue;
            };
            let kb = rk.as_bytes().to_vec();
            let one = post_batch.slice(row, 1);
            if by_key.insert(kb.clone(), one).is_none() {
                order.push(rk);
            }
            // Pre-image: keep the FIRST-seen (the OLD row before this statement);
            // a later duplicate PK row carries the same pre-image, so last-wins
            // would be identical — `or_insert` is just cheaper.
            if let Some(ref pb) = pre_batch {
                pre_by_key
                    .entry(kb)
                    .or_insert_with(|| pb.slice(row, 1));
            }
        }
    }

    if order.is_empty() {
        return Ok(ExecResult::Empty {
            tag: "UPDATE 0".into(),
        });
    }

    // Single-table fast path threads tx-mode; UPDATE…FROM does too: an
    // in-tx override is rollback-able, an auto-commit one lands in the shared
    // registry. The join probe ran through the overlay-aware SELECT, so the
    // post-images already reflect any in-tx prior writes on the target.
    let tx_mode = crate::session::tx_is_active(&sess.state);
    let key_post_images: Vec<(basin_hottier::RowKey, RecordBatch)> = order
        .into_iter()
        .map(|rk| {
            let post = by_key
                .get(rk.as_bytes())
                .expect("every ordered key has a post-image")
                .clone();
            (rk, post)
        })
        .collect();

    // ── ONE batched overlay write (with budget guard) ───────────────────────
    // `pre_by_key` is empty unless a sink is attached, so the no-sink path emits
    // no events here. On success the write tail builds before/after UPDATE
    // events from the join pre-images and the post-images (auto-commit:
    // dispatch; in-tx: buffer for the COMMIT drain).
    if write_overlay_post_images(sess, &target, &meta, &key_post_images, tx_mode, &pre_by_key)?
        .is_some()
    {
        return Ok(ExecResult::Empty {
            tag: format!("UPDATE {}", key_post_images.len()),
        });
    }

    // ── Budget decline → ONE batched cold rewrite (never a per-row loop) ─────
    //
    // The overlay budget hard cap could not admit the whole post-image set.
    // The correct remedy is the SAME one the single-table fast path uses on
    // decline — drain the overlay into cold (which frees memtable memory) and
    // route the post-images through the overlay-materialize machinery so they
    // land in cold storage via ONE narrowed merge per drain, with full GIN /
    // FTS / B-tree / GIST index maintenance and the physical-delete + overlay-
    // ack bookkeeping — none of which a bespoke `write_replacement` could
    // reproduce without duplicating ~200 lines of index maintenance.
    //
    // We drain first (frees the budget), then write the post-images and drain
    // again. In the (pathological) case where the post-image set alone still
    // exceeds the freed budget, we chunk: write as many as the budget admits,
    // drain, repeat — guaranteeing forward progress and a per-statement
    // resident footprint bounded by the budget, never by the match count.
    //
    // This branch is auto-commit-only: in `tx_mode` the tx-overlay write is
    // NOT budget-reserved (`write_overlay_post_images` skips the reservation),
    // so it never declines and we returned above. The cold drain therefore
    // always operates on the shared memtable (`tx_mode = false`), which is
    // what `materialize_hot_overlay_into_cold` drains.
    debug_assert!(!tx_mode, "UPDATE … FROM cold drain is auto-commit-only");
    cold_drain_post_images(sess, &target, &meta, &key_post_images, &pre_by_key).await?;
    Ok(ExecResult::Empty {
        tag: format!("UPDATE {}", key_post_images.len()),
    })
}

/// Land a `(key, post_image)` set in COLD storage by writing it to the hot-tier
/// overlay and draining, reusing the overlay-materialize merge + full index
/// maintenance. The batched fallback for [`exec_update_from`] when the overlay
/// budget declines the in-one-shot write. No per-row SQL: each drain performs
/// ONE narrowed merge over the candidate cold files.
///
/// Forward-progress under a tight budget: we drain once up front to free the
/// declining footprint, then write the post-images in budget-admitted chunks,
/// draining between chunks. Each chunk's resident overlay footprint is bounded
/// by the project memtable budget — never by the total match count — so a
/// large UPDATE…FROM whose post-images exceed the budget still completes in
/// bounded memory instead of failing or looping per row.
async fn cold_drain_post_images(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    key_post_images: &[(basin_hottier::RowKey, RecordBatch)],
    // Per-key full pre-images for change-event capture (empty when no sink is
    // attached). Sliced to the current chunk per write so events fire even on
    // the budget-decline cold fallback. Always auto-commit here, so events are
    // dispatched (not buffered).
    pre_by_key: &std::collections::HashMap<Vec<u8>, RecordBatch>,
) -> Result<()> {
    // Free the budget that just declined before attempting any overlay write.
    materialize_hot_overlay_into_cold(sess, table).await?;

    let mut remaining: &[(basin_hottier::RowKey, RecordBatch)] = key_post_images;
    while !remaining.is_empty() {
        // Try the whole remaining set first; on decline, halve the chunk until
        // the budget admits it (a single override always fits — its bytes were
        // already reservable when first read). Each admitted chunk is drained
        // before the next, so resident memory stays bounded by the budget.
        let mut chunk_len = remaining.len();
        loop {
            let chunk = &remaining[..chunk_len];
            // Restrict the pre-image map to this chunk's keys so events match the
            // rows actually written this iteration. Empty when no sink attached.
            let chunk_pre: std::collections::HashMap<Vec<u8>, RecordBatch> =
                if pre_by_key.is_empty() {
                    std::collections::HashMap::new()
                } else {
                    chunk
                        .iter()
                        .filter_map(|(k, _)| {
                            pre_by_key
                                .get(k.as_bytes())
                                .map(|pb| (k.as_bytes().to_vec(), pb.clone()))
                        })
                        .collect()
                };
            // Auto-commit overlay write (shared memtable) so the subsequent
            // `materialize_hot_overlay_into_cold` drains it.
            if write_overlay_post_images(sess, table, meta, chunk, false, &chunk_pre)?.is_some() {
                // Drain this chunk to cold (narrowed merge + index maintenance)
                // and advance.
                materialize_hot_overlay_into_cold(sess, table).await?;
                remaining = &remaining[chunk_len..];
                break;
            }
            if chunk_len == 1 {
                // A single override could not be admitted even after a fresh
                // drain — the budget is smaller than one row's bytes, which is
                // a misconfiguration rather than a recoverable condition.
                return Err(BasinError::internal(
                    "UPDATE … FROM: memtable budget cannot admit a single row's overlay write",
                ));
            }
            chunk_len = chunk_len.div_ceil(2);
        }
    }
    Ok(())
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
        let pk_proj: Vec<String> = pk.iter().map(|c| format!("{qualifier}.{c}")).collect();

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
    materialize_hot_overlay_into_cold(sess, table).await?;

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
    // Defense-in-depth (#94/#95): filter out files the catalog
    // already removed before counting.
    let listed_files = storage
        .list_data_files_with_stats(&sess.project, table)
        .await?;
    let live_files = filter_to_live_data_files(sess, table, listed_files).await?;
    let total_rows_before: usize = live_files
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
    // Defense-in-depth (#94/#95): exclude any files the catalog has
    // already removed even if the async cleanup hasn't unlinked them.
    let data_files = filter_to_live_data_files(sess, table, data_files).await?;
    let all_paths: Vec<String> = data_files
        .iter()
        .map(|f| f.path.as_ref().to_string())
        .collect();

    // Clone the kept batches for B-tree index maintenance BEFORE the write
    // consumes them (Arrow clones are cheap Arc-buffer bumps).
    let kept_for_index: Vec<RecordBatch> = keep_batches.iter().cloned().collect();
    // Write the kept rows as a new file (empty → no file written).
    let added = write_replacement(sess, table, schema.clone(), keep_batches).await?;

    // Atomically swap old files → new file in the catalog snapshot.
    commit_replace(
        sess,
        table,
        meta.current_snapshot,
        all_paths.clone(),
        added.clone(),
    )
    .await?;

    // B-tree secondary-index maintenance: this path replaces EVERY live file
    // with one rewritten file of the kept rows, so purge all old locations
    // and re-register the new file (a full per-column rebuild). See
    // `maintain_btree_secondary_on_replace` for the soundness argument.
    {
        let rewrites: Vec<(String, Vec<RecordBatch>)> = added
            .first()
            .map(|f| vec![(f.path.clone(), kept_for_index)])
            .unwrap_or_default();
        Box::pin(maintain_btree_secondary_on_replace(
            sess, table, &meta, &all_paths, &rewrites,
        ))
        .await;
    }

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
    let pk_proj: Vec<String> = pk.iter().map(|c| format!("{table_str}.{c}")).collect();

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
    let set_parts: Vec<String> = assignments.iter().map(|a| format!("{}", a)).collect();
    let set_clause = set_parts.join(", ");

    let schema = meta.schema.clone();
    let where_pred = build_pk_in_predicate(&pk_tuples, pk, &schema)?;
    let update_sql = format!("UPDATE {table_str} SET {set_clause} WHERE {where_pred}");
    Box::pin(sess.execute(&update_sql)).await
}

/// Collect `(pk_col1, pk_col2, …)` tuples from a result set as string
/// literals. Each column value is rendered as its SQL literal form
/// (quoted for text, unquoted for numeric types).
fn collect_pk_batches(batches: &[RecordBatch], pk_cols: &[String]) -> Result<Vec<Vec<String>>> {
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
fn scalar_from_array(arr: &dyn arrow_array::Array, row: usize, dt: &DataType) -> Result<String> {
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
        DataType::Int8 => Ok(arr
            .as_primitive::<arrow_array::types::Int8Type>()
            .value(row)
            .to_string()),
        DataType::Int16 => Ok(arr
            .as_primitive::<arrow_array::types::Int16Type>()
            .value(row)
            .to_string()),
        DataType::Int32 => Ok(arr
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(row)
            .to_string()),
        DataType::Int64 => Ok(arr
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float32 => Ok(arr
            .as_primitive::<arrow_array::types::Float32Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(arr
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row)
            .to_string()),
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
        DataType::Date32 => {
            // Render as ISO-8601 date literal: '2024-01-15'.
            // Date32 is days since 1970-01-01 (Arrow spec).
            use arrow_array::types::Date32Type;
            let days = arr.as_primitive::<Date32Type>().value(row);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days as i64);
            Ok(format!("'{}'", d.format("%Y-%m-%d")))
        }
        DataType::Binary => {
            // Render as PostgreSQL hex-escape bytea literal: '\xDEADBEEF'.
            let bytes = arr.as_binary::<i32>().value(row);
            Ok(format!("'\\x{}'", hex::encode(bytes)))
        }
        DataType::LargeBinary => {
            // Render as PostgreSQL hex-escape bytea literal: '\xDEADBEEF'.
            let bytes = arr.as_binary::<i64>().value(row);
            Ok(format!("'\\x{}'", hex::encode(bytes)))
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
    DFExpr {
        sql_text: String,
        col_type: DataType,
    },
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

/// Hot point-mutation probe: is a POST-COMMIT change-event sink (CDC ring /
/// realtime websocket) attached? This is the gate the hot-tier UPDATE/DELETE
/// fast paths (and UPDATE…FROM) use to decide whether to capture before/after
/// images — NOT `sinks_attached`.
///
/// `sinks_attached` is permanently TRUE because the engine always registers a
/// pre-commit `ReactorSink` (see `attach_reactor_sink`), so it cannot gate the
/// OLTP hot path: a no-CDC benchmark run would otherwise build payloads and
/// consume seqs on every point mutation. The hot fast paths are only reached
/// for tables with NO declared reactor (the fast-path admission gates route
/// reactor-bearing tables to the slow CoW path), so the ONLY legitimate
/// consumer of a hot-path event is a post-commit CDC / realtime sink. Gating on
/// `post_commit` non-empty keeps the zero-CDC-sink hot path allocation-free and
/// seq-free, exactly as the published OLTP benchmarks measured it.
fn post_commit_sink_attached(sess: &ProjectSession) -> bool {
    let guard = sess
        .engine
        .event_sinks()
        .read()
        .expect("event_sinks lock poisoned");
    !guard.post_commit_is_empty()
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

/// Route a set of captured row-level changes to the change-event pipeline from
/// a hot-tier point-mutation fast path, post-commit.
///
/// This is the single seam the hot-tier UPDATE/DELETE fast paths (and the
/// UPDATE…FROM overlay-write tail) use so the realtime websocket and the CDC
/// ring see hot-tier mutations exactly as they see cold copy-on-write ones.
///
/// Two delivery modes, matching the cold paths' contract:
///   * **auto-commit** (`tx_mode == false`): the overlay write IS the commit,
///     so we build the events (allocating each a `(project, table)` `seq`) and
///     `dispatch_post_commit` immediately — fire-and-forget, after the write.
///   * **in-transaction** (`tx_mode == true`): events must NOT be emitted at
///     statement time. We buffer them per-tx (`tx_change_events_push`) WITHOUT
///     a seq; the executor's COMMIT drain assigns commit-ordered seqs and
///     dispatches them, and ROLLBACK drops the buffer so nothing leaks.
///
/// `rows` is empty whenever no row matched OR — by the caller's lazy gate — no
/// sink is attached, so on the zero-sink OLTP hot path this is a single
/// `is_empty` branch and returns before touching the registry or the tx lock.
fn emit_or_buffer_change_events(
    sess: &ProjectSession,
    table: &TableName,
    op: ChangeOp,
    rows: Vec<RowChange>,
    tx_mode: bool,
) {
    if rows.is_empty() {
        return;
    }
    let causation_user = if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.clone())
    };
    if tx_mode {
        for RowChange { before, after } in rows {
            crate::session::tx_change_events_push(
                &sess.state,
                crate::session::TxChangeEvent {
                    table: table.clone(),
                    op,
                    before,
                    after,
                    causation_user: causation_user.clone(),
                },
            );
        }
    } else {
        let events = build_events(sess, table, op, rows);
        dispatch_post_commit(&sess.engine, events);
    }
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
    catalog_schema: &Schema,
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
        // Reattach catalog schema (and coerce Vortex-narrowed types) so
        // the kept batches concat cleanly against the catalog schema in
        // `write_replacement`. See `reattach_catalog_metadata`.
        let batch = reattach_catalog_metadata(catalog_schema, batch?)?;
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

/// Re-attach the catalog schema's per-field metadata onto a batch read
/// back from storage AND coerce the per-column physical type back to the
/// catalog's declared type when storage round-tripped it through a
/// narrower representation.
///
/// Parquet preserves Arrow field metadata in its key-value footer, so a
/// batch read from a `.parquet` file already carries the Basin logical-
/// type markers (`BASIN_CHARLEN`, `BASIN_GENERATED_AS`, enum/domain/UUID
/// tags, …). Vortex's on-disk `DType` is structural only — when
/// `Storage::read_file` decodes a `.vortex` file it has no catalog schema
/// to graft onto and recovers the Arrow schema from the file's own DType,
/// which drops all field metadata. Downstream UPDATE-path validators
/// (`enforce_charlen_array` via `parse_charlen`, generated-column
/// recompute via `field_is_generated`) key entirely off that metadata, so
/// a Vortex-backed table would silently skip the CHAR/VARCHAR length
/// limit and never recompute GENERATED columns on UPDATE.
///
/// Type-narrowing also happens on the Vortex read path: a catalog
/// `LargeBinary` (JSONB's physical type) round-trips through Vortex's
/// `BinaryView`, which `normalize_view_types_schema` then maps to plain
/// `Binary` (32-bit offsets). The subsequent UPDATE rewrite tries to
/// `concat_batches(catalog_schema, …)` — catalog says `LargeBinary`,
/// batch carries `Binary` — and arrow rejects with "expected LargeBinary
/// but found Binary at column index N". Before this coercion the bulk
/// UPDATE bench (`compare_postgres_*`) panicked on the events.payload
/// JSONB column at scale ≥10k. We cast `Binary → LargeBinary` and
/// `Utf8 → LargeUtf8` (the symmetric case) here so the rewrite path
/// always sees catalog-aligned batches. The cast is cheap for the small
/// row counts UPDATEs touch and is a no-op on Parquet (round-trip type
/// is already authoritative).
///
/// This restores parity: for every batch field that exists in the catalog
/// schema by name, adopt the catalog field's metadata (and cast the
/// column to the catalog type if it's narrower than the catalog declared).
/// Fields without a catalog match are left exactly as read so any column
/// the catalog doesn't describe passes through unchanged.
fn reattach_catalog_metadata(catalog_schema: &Schema, batch: RecordBatch) -> Result<RecordBatch> {
    use arrow_array::ArrayRef;
    use arrow_schema::DataType;
    let read_schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(read_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(read_schema.fields().len());
    let mut changed = false;
    for (col_idx, f) in read_schema.fields().iter().enumerate() {
        let col = batch.column(col_idx).clone();
        match catalog_schema.field_with_name(f.name()) {
            Ok(cat_f) => {
                let cat_dt = cat_f.data_type();
                let read_dt = f.data_type();
                // Decide whether the read column needs a physical cast to
                // match the catalog. Limited to the Vortex round-trip
                // narrowings we know about (Binary↔LargeBinary,
                // Utf8↔LargeUtf8) — keeps the surface area small and
                // avoids surprising implicit conversions on unrelated
                // type mismatches (those would still flow through to the
                // downstream validator's error path).
                let needs_widen = matches!(
                    (read_dt, cat_dt),
                    (DataType::Binary, DataType::LargeBinary)
                        | (DataType::LargeBinary, DataType::Binary)
                        | (DataType::Utf8, DataType::LargeUtf8)
                        | (DataType::LargeUtf8, DataType::Utf8)
                );
                let coerced = if needs_widen {
                    changed = true;
                    arrow::compute::cast(&col, cat_dt).map_err(|e| {
                        BasinError::internal(format!(
                            "reattach catalog metadata: cast column {:?} from {:?} to {:?}: {e}",
                            f.name(),
                            read_dt,
                            cat_dt
                        ))
                    })?
                } else {
                    col
                };
                let final_dt = coerced.data_type().clone();
                let same_type = final_dt == *read_dt;
                if cat_f.metadata() != f.metadata() || !same_type {
                    changed = true;
                }
                fields.push(Arc::new(
                    Field::new(f.name(), final_dt, f.is_nullable())
                        .with_metadata(cat_f.metadata().clone()),
                ));
                columns.push(coerced);
            }
            Err(_) => {
                fields.push(f.clone());
                columns.push(col);
            }
        }
    }
    if !changed {
        return Ok(batch);
    }
    let merged = Arc::new(Schema::new_with_metadata(
        fields,
        read_schema.metadata().clone(),
    ));
    RecordBatch::try_new(merged, columns)
        .map_err(|e| BasinError::internal(format!("reattach catalog metadata: {e}")))
}

/// Read a file into in-memory batches. Used by the event-capturing
/// UPDATE path so we can pair before/after row-by-row without re-reading.
async fn read_file_to_batches(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
    catalog_schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(project, path).await?;
    let mut out = Vec::new();
    let target: arrow_schema::SchemaRef = Arc::new(catalog_schema.clone());
    while let Some(batch) = stream.next().await {
        let b = reattach_catalog_metadata(catalog_schema, batch?)?;
        // Pad to the current catalog schema so a file written before an
        // `ALTER TABLE ADD COLUMN` gains the new columns (as NULL) here —
        // otherwise a `SET <new_col> = …` in the rewrite has no column to
        // target and the assignment is silently lost.
        let b = crate::hot_tombstone::pad_batch_to_schema(b, &target)?;
        out.push(b);
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
/// Concurrency for the parallel bulk-UPDATE/DELETE read-modify-apply fan-out
/// (Inv-bulk-UPDATE #182). Defaults to `available_parallelism()`, capped at
/// 16, overridable via `BASIN_UPDATE_SCAN_CONCURRENCY`. Never fans out wider
/// than the file count, and is at least 1.
fn update_scan_concurrency(file_count: usize) -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    let cap = *CAP.get_or_init(|| {
        let from_env: Option<usize> = std::env::var("BASIN_UPDATE_SCAN_CONCURRENCY")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0);
        from_env.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
    });
    cap.min(16).min(file_count.max(1)).max(1)
}

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
///
/// The `catalog_schema` argument is the table's catalog-declared schema; it
/// is used to coerce any Vortex-narrowed columns (Binary→LargeBinary,
/// Utf8→LargeUtf8) back to the catalog's authoritative physical type so
/// the kept batches survive the downstream `concat_batches(catalog_schema,
/// kept)` in `write_replacement`. See `reattach_catalog_metadata` for the
/// rationale — the JSONB column on the bench's `events` table is the
/// canonical trigger. The cast is a no-op when reading Parquet (the
/// round-trip type is already catalog-aligned).
async fn evaluate_and_partition_delete(
    storage: &Storage,
    project: &basin_common::ProjectId,
    path: &object_store::path::Path,
    pred: &CompoundPredicate,
    catalog_schema: &Schema,
) -> Result<(Vec<RecordBatch>, u64)> {
    let mut stream = storage.read_file(project, path).await?;
    let mut kept = Vec::new();
    let mut decoded_total: u64 = 0;
    while let Some(batch) = stream.next().await {
        let batch = reattach_catalog_metadata(catalog_schema, batch?)?;
        decoded_total += batch.num_rows() as u64;
        let mask = evaluate_compound(&batch, pred)
            .map_err(|e| BasinError::internal(format!("delete predicate eval: {e}")))?;
        let inverse = invert_mask(&mask);
        let kb = arrow_select::filter::filter_record_batch(&batch, &inverse)
            .map_err(|e| BasinError::internal(format!("delete filter batch: {e}")))?;
        if kb.num_rows() > 0 {
            kept.push(kb);
        }
    }
    Ok((kept, decoded_total))
}

/// One file's contribution to a bulk UPDATE: the original file path, the
/// number of rows that matched (= rewritten), and the post-SET replacement
/// batches. Produced by the parallel no-capture fan-out and reassembled in
/// deterministic path order by the caller.
struct PerFileUpdate {
    path: String,
    rows_matched: usize,
    batches: Vec<RecordBatch>,
}

/// Read+mask+apply for a single data file in the parallel (no-capture)
/// bulk-UPDATE path. All shared state is owned (`Arc`/clone) so the future
/// is fully `Send` and can be driven by `buffer_unordered` even when the
/// enclosing UPDATE future is later spawned. Returns `None` for a pruned or
/// no-op (stats-said-maybe-but-matched-nothing) file.
async fn apply_update_to_file(
    catalog: Arc<dyn basin_catalog::Catalog>,
    storage: Storage,
    project: ProjectId,
    schema: Arc<Schema>,
    pred: Arc<Option<CompoundPredicate>>,
    assignments: Arc<Vec<(usize, AssignmentRhs)>>,
    f: DataFile,
) -> Result<Option<PerFileUpdate>> {
    let outcome = file_outcome(pred.as_ref().as_ref(), &f, schema.as_ref());
    match (outcome, pred.as_ref()) {
        (PruneOutcome::NoMatch, Some(_)) => Ok(None),
        (PruneOutcome::AllMatch, _) | (PruneOutcome::Mixed, None) => {
            let news = read_and_apply_assignments(
                &catalog,
                &storage,
                &project,
                &f.path,
                None,
                assignments.as_ref(),
                schema.as_ref(),
            )
            .await?;
            // ROW-CONSERVATION TRIPWIRE (the DELETE CoW sibling of this check
            // landed in 8b7465ad; UPDATE never got one). Every row of the file
            // is rewritten here, so the output MUST carry every catalog row. If
            // the read decoded the file as empty or partial — a poisoned/short
            // GET, a truncated body accepted as Ok — we would replace a
            // populated file with a fragment while reporting `f.row_count` rows
            // updated, destroying every row we failed to see.
            let carried = decoded_rows(&news);
            if carried != f.row_count {
                return Err(BasinError::storage(format!(
                    "UPDATE rewrite of {}: read decoded {carried} rows but the catalog \
                     attributes {} — refusing the rewrite (it would destroy the rows it \
                     could not see)",
                    f.path.as_ref(),
                    f.row_count
                )));
            }
            Ok(Some(PerFileUpdate {
                path: f.path.as_ref().to_string(),
                rows_matched: f.row_count as usize,
                batches: news,
            }))
        }
        (PruneOutcome::Mixed, Some(p)) => {
            let (matched, news) = read_and_apply_assignments_mixed(
                &catalog,
                &storage,
                &project,
                &f.path,
                p,
                assignments.as_ref(),
                schema.as_ref(),
            )
            .await?;
            if matched == 0 {
                // Stats said maybe-match but no rows actually matched —
                // pass through, no rewrite.
                return Ok(None);
            }
            // Same tripwire as the AllMatch arm: `news` carries EVERY decoded
            // row (updated and untouched alike), so a short decode here rewrites
            // the file down to the fragment it managed to read. The `matched ==
            // 0` pass-through above only saves the fully-empty case — a partial
            // decode with one match still destroys the rows it never saw.
            let carried = decoded_rows(&news);
            if carried != f.row_count {
                return Err(BasinError::storage(format!(
                    "UPDATE rewrite of {}: read decoded {carried} rows but the catalog \
                     attributes {} — refusing the rewrite (it would destroy the rows it \
                     could not see)",
                    f.path.as_ref(),
                    f.row_count
                )));
            }
            Ok(Some(PerFileUpdate {
                path: f.path.as_ref().to_string(),
                rows_matched: matched,
                batches: news,
            }))
        }
        (PruneOutcome::NoMatch, None) => unreachable!(),
    }
}

/// Total rows carried by a decoded/rewritten batch set. Used by the UPDATE
/// row-conservation tripwires to compare what a read actually produced
/// against what the catalog attributes to the file.
fn decoded_rows(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
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
    catalog_schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    let mut stream = storage.read_file(project, path).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = reattach_catalog_metadata(catalog_schema, batch?)?;
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
    catalog_schema: &Schema,
) -> Result<(usize, Vec<RecordBatch>)> {
    let mut stream = storage.read_file(project, path).await?;
    let mut matched = 0usize;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = reattach_catalog_metadata(catalog_schema, batch?)?;
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
    sess: &ProjectSession,
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
    let storage = &sess.engine.config().storage;
    let project = &sess.project;
    let replaced: HashSet<&str> = replaced_paths.iter().map(|s| s.as_str()).collect();
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    // Defense-in-depth (#94/#95): the cold-path lister can include
    // files that the catalog has already removed; treat the catalog as
    // truth so a stale on-disk Parquet doesn't reintroduce phantom PKs.
    let data_files = filter_to_live_data_files(sess, table, data_files).await?;
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

/// Flush the shard INSERT tail to Parquet only when `(project, table)` has a
/// resident, un-flushed tail. INSERT populates the shard TAIL, not the
/// memtable; a fast-path UPDATE/DELETE's read-before-write reads
/// memtable+cold (not the tail), so a just-INSERTed-same-row would be invisible
/// without this flush (read-your-writes break). `has_pending_tail` is an O(1)
/// resident-map probe that neither lists object storage nor drains the tail, so
/// the common no-tail case (an update-heavy loop) pays nothing while the
/// read-your-writes guarantee is preserved whenever a tail exists.
async fn pre_mutation_flush_if_tail(sess: &ProjectSession, table: &TableName) -> Result<()> {
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        if shard.has_pending_tail(&sess.project, table).await {
            shard.flush_to_parquet().await?;
            // #70 edit-3: `flush_to_parquet` (compact_all) SWALLOWS per-
            // partition drain failures (warn + Ok) so one wedged partition
            // can't stall the background tick — correct there, WRONG as a
            // mutation barrier: a copy-on-write DELETE/UPDATE proceeding on a
            // partially-flushed base misses the still-resident tail rows;
            // they commit later and "reappear" after the mutation (the
            // purge-confirm livelock chain proven on the 1B capstone).
            // Re-probe and FAIL LOUD instead — retryable (40001 class), and
            // an honest error beats a silently incomplete mutation.
            if shard.has_pending_tail(&sess.project, table).await {
                return Err(BasinError::CommitConflict(format!(
                    "{table}: mutation under drain backlog — pre-mutation flush left \
                     un-drained tail rows (a partition's drain failed); retry",
                )));
            }
        }
    }
    Ok(())
}

/// Materialize the hot-tier UPDATE/DELETE overlay for `table` into the cold
/// tier, then clear exactly the materialized overlay keys.
///
/// The hot-tier fast path writes `MemRowValue::Update` / `Tombstone` entries
/// into the process-wide `MemTableRegistry`; those are applied to reads via
/// `TombstoneFilterExec` / `UpdateOverlayExec`, but a cold-path copy-on-write
/// UPDATE/DELETE reads the RAW cold files (it does not see the overlay). So a
/// hot-tier UPDATE/DELETE followed by a cold-path UPDATE/DELETE on the same row
/// would rewrite stale cold data while the un-cleared overlay shadows the
/// result on read — silently losing the cold-path mutation (bug #94).
///
/// `pre_mutation_flush` already drains the shard INSERT tail; this drains the
/// UPDATE/DELETE overlay too, so the cold path reads a consistent base with an
/// empty overlay. No-op (a cheap registry probe) when the overlay is empty —
/// the overwhelmingly common case. Only fires when a row has a pending hot
/// overlay AND a cold-path mutation is about to touch its table; cold-path
/// mutations are already full-file rewrites, so the extra rewrite is in budget.
pub(crate) async fn materialize_hot_overlay_into_cold(
    sess: &ProjectSession,
    table: &TableName,
) -> Result<()> {
    materialize_overlay_for_table(&sess.engine, sess.project, table).await
}

/// Engine-callable core of [`materialize_hot_overlay_into_cold`]. Everything
/// the materialize consults lives on the engine (catalog, storage, memtable
/// registry) plus the project id — no per-session state is read — so the
/// background overlay reconciler ([`crate::overlay_reconcile`]) can drive it
/// without opening a `ProjectSession`. Behavior is byte-identical to the
/// sess-based wrapper: same dirty-snapshot ack semantics, same
/// `retain_secs == 0` kill switch, same narrowed file selection, and the
/// same optimistic `commit_replace` conflict propagation.
pub(crate) async fn materialize_overlay_for_table(
    engine: &crate::Engine,
    project: ProjectId,
    table: &TableName,
) -> Result<()> {
    let registry = engine.memtable_registry();
    let Some(entry) = registry.get(&project, table) else {
        return Ok(());
    };
    // O(1) overlay-presence gate (S4): tombstone/update entries are by
    // definition DIRTY (a flush ack REMOVES clean tombstones and re-tags
    // acked `Update`s as `Row`), so when both newest-version counters are
    // zero there is nothing to materialize — skip without walking the map.
    if entry.memtable.tombstone_count() == 0 && entry.memtable.update_count() == 0 {
        return Ok(());
    }
    // Capture the overlay through `dirty_snapshot` so each entry carries the
    // MVCC seq the post-materialize ack must cover (S4 age-based residency).
    // The dirty snapshot is exactly the overlay an auto-commit read sees:
    // every Tombstone/Update entry is dirty (see the counter gate above), and
    // counter-/PK-keyed `Row` entries are not part of the UPDATE/DELETE
    // overlay (they are cold-committed HTAP INSERT residency and are skipped
    // here exactly as the previous `snapshot_*(…, None)` calls skipped them).
    let mut tombstones: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut updates: std::collections::HashMap<Vec<u8>, RecordBatch> =
        std::collections::HashMap::new();
    // `(key, seq)` pairs handed to `mark_flushed` after the cold commit. The
    // seq is captured HERE, at snapshot time, so an overlay write that lands
    // while we materialize carries a HIGHER seq than the ack and survives
    // DIRTY (the version-loss fix `mark_flushed` exists for).
    let mut acks: Vec<(basin_hottier::RowKey, u64)> = Vec::new();
    for (key, value, seq) in entry.memtable.dirty_snapshot() {
        match value {
            basin_hottier::MemRowValue::Tombstone => {
                tombstones.insert(key.as_bytes().to_vec());
                acks.push((key, seq));
            }
            basin_hottier::MemRowValue::Update { bytes, .. } => {
                if let Some(rb) = crate::hot_tombstone::decode_ipc_row(&bytes) {
                    if rb.num_rows() > 0 {
                        updates.insert(key.as_bytes().to_vec(), rb);
                        acks.push((key, seq));
                    }
                }
            }
            // HTAP INSERT residency rows — not part of the overlay.
            basin_hottier::MemRowValue::Row { .. } => {}
        }
    }
    if tombstones.is_empty() && updates.is_empty() {
        return Ok(());
    }

    let meta = engine
        .config()
        .catalog
        .load_table(&project, table)
        .await?;
    // The overlay is only ever written for single-PK tables; defensively skip
    // (and clear) otherwise so we never wedge.
    if meta.pk_columns.len() != 1 {
        registry.remove(&project, table);
        return Ok(());
    }
    let pk_col = meta.pk_columns[0].clone();
    let pk_dt = meta
        .schema
        .field_with_name(&pk_col)
        .map_err(|_| BasinError::internal(format!("PK column {pk_col:?} missing from schema")))?
        .data_type()
        .clone();

    // Narrow the rewrite to the cold files that actually contain the overlay
    // keys we're materializing. Pre-narrowing every single-row UPDATE folded
    // the entire cold tier; for a 10k-row table this was the dominant cost.
    //
    // Safety contract: any RowKey we cannot localize (unsupported PK type, or
    // a key the probe couldn't decode) forces a fall-back to the full live
    // set. A key whose probe is *definitively* Absent contributes zero files
    // (apply_update_overlay_to_batches still appends the override row, so the
    // overlay rewrite is durable even when no cold file holds the row).
    let live_all = meta.live_data_files();
    let overlay_keys: Vec<&[u8]> = tombstones
        .iter()
        .map(|v| v.as_slice())
        .chain(updates.keys().map(|v| v.as_slice()))
        .collect();
    let narrowed_paths: Option<std::collections::HashSet<String>> =
        narrow_materialize_files(&overlay_keys, &pk_col, &pk_dt, &live_all, meta.schema.as_ref());
    let live: Vec<DataFileRef> = match &narrowed_paths {
        Some(allow) => live_all
            .iter()
            .filter(|f| allow.contains(f.path.as_str()))
            .cloned()
            .collect(),
        None => live_all.clone(),
    };

    // Read the narrowed cold rows (RAW — without the overlay) for this table.
    let removed: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
    let paths: Vec<object_store::path::Path> = live
        .iter()
        .map(|f| object_store::path::Path::from(f.path.as_str()))
        .collect();
    let mut batches: Vec<RecordBatch> = Vec::new();
    if !paths.is_empty() {
        let mut stream = engine
            .config()
            .storage
            .read_paths(&project, paths, basin_storage::ReadOptions::default())
            .await?;
        while let Some(b) = stream.next().await {
            batches.push(b?);
        }
    }

    // Apply the overlay exactly as the read path does: drop tombstoned rows,
    // suppress overridden cold rows, append the post-SET override rows.
    let batches = crate::hot_tombstone::apply_tombstone_filter_to_batches(
        batches,
        &tombstones,
        &pk_col,
        &pk_dt,
    )
    .map_err(|e| BasinError::internal(format!("materialize overlay (tombstones): {e}")))?;
    // Full materialization: append EVERY override row (no query predicate to
    // restrict to — this rewrites the entire overlay into cold storage). Pass
    // an empty predicate slice so `override_row_matches` keeps all rows.
    let batches = crate::hot_tombstone::apply_update_overlay_to_batches(
        batches,
        &updates,
        &pk_col,
        &pk_dt,
        &[],
    )
    .map_err(|e| BasinError::internal(format!("materialize overlay (updates): {e}")))?;

    // Normalize every merged batch to the CATALOG physical types before the
    // `write_replacement` concat. The cold batches were read RAW (no
    // `reattach_catalog_metadata`), so on a Vortex table a JSONB column comes
    // back as `Binary` while the override rows decoded from the memtable IPC
    // blob carry the writer's catalog `LargeBinary`. Mixed Binary/LargeBinary
    // batches make `concat_batches(meta.schema, …)` fail with
    // "concatenate arrays of different data types (Binary, LargeBinary)" —
    // the exact bug this guards. Casting to `meta.schema` here makes the cold
    // batches and the appended override rows uniformly catalog-typed; the
    // writer then re-encodes to disk in its own format (we do NOT change what
    // lands on disk, only what feeds the in-memory concat).
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| crate::hot_tombstone::normalize_batch_to_schema(b, meta.schema.as_ref()))
        .collect();
    // ADR 0027 Phase 4: `write_replacement` materialises promoted shadow
    // column(s) into the replacement file (see its body); passing the catalog
    // schema is correct — the shadow extension happens there so EVERY cold-path
    // rewrite (this overlay-materialize, exec_update, exec_delete, …) emits the
    // column and keeps the promoted-column read fast path enabled.
    // Clone the merged batches for the GIN-family registry maintenance below
    // BEFORE `write_replacement_engine` consumes them (Arrow RecordBatch
    // clones are cheap Arc-buffer bumps) — and only when the table declares a
    // GIN index, so the overwhelmingly common index-free materialize pays
    // nothing. The batches are already normalized to the CATALOG physical
    // types above, which is exactly what the index maintainers expect (JSONB
    // → `LargeBinary`, tsvector → `Utf8`).
    let table_has_gin = meta.indexes.iter().any(|idx| idx.access_method == "gin");
    // Single-column B-tree secondary indexes (the default access method; not
    // GIN/GIST/vector) are now admitted to the overlay fast path, so a drain
    // must re-register their replacement file's locations the same way the
    // cold CoW commit paths do (`maintain_btree_secondary_on_replace`). Build
    // the index batches when EITHER family is present; index-free materializes
    // still pay nothing.
    let table_has_btree_secondary = meta.indexes.iter().any(|idx| {
        idx.access_method != "gin"
            && idx.access_method != "gist"
            && idx.columns.len() == 1
            && !idx.columns[0].starts_with("expr:")
    });
    let index_batches: Vec<RecordBatch> = if table_has_gin || table_has_btree_secondary {
        batches.iter().cloned().collect()
    } else {
        Vec::new()
    };
    let added =
        write_replacement_engine(engine, project, table, meta.schema.clone(), batches).await?;
    commit_replace_engine(
        engine,
        project,
        table,
        meta.current_snapshot,
        removed.clone(),
        added.clone(),
    )
    .await?;

    // GIN-family registry maintenance — the materialize half of the UPDATE
    // fast-path gate's old blocker #3. Now that GIN-only tables are admitted
    // to the overlay fast path (see `try_resolve_fast_path_update`), this
    // drain is a routine event for GIN-indexed tables; without maintenance
    // the replaced files would keep stale posting entries while the
    // replacement file is never indexed or completeness-sealed, so every
    // post-drain probe would fail the completeness guards and degrade to a
    // full scan FOREVER (correct, but unpruned until a re-CREATE INDEX).
    //
    // Wiring mirrors the cold CoW commit paths (`exec_update`/`exec_delete`):
    // run AFTER `commit_replace` and BEFORE the physical delete — the
    // ordering contract documented on `GinPostingListMaintainer`. Maintained
    // here:
    //   * jsonb GIN file-level posting list (`GinIndexRegistry`) via
    //     `GinPostingListMaintainer` — purge replaced paths, rebuild and
    //     completeness-seal the replacement file; restores the `Empty`
    //     short-circuit and the file-level posting prune path.
    //   * tsvector FTS posting list (`GinTsvectorRegistry`) — same
    //     purge/rebuild/seal, mirroring `maintain_gin_fts_index_on_insert`
    //     (same writer-priority rg_size so row-group ordinals line up).
    // NOT maintained (matches `exec_update`, which also leaves them to the
    // per-file completeness guards): the `GinRowGroupRegistry` bloom
    // summaries and the JSONB posting SIDECARS — the un-sealed replacement
    // file is force-scanned by those paths (correct, file-level pruning above
    // still engages) until compaction / backfill re-seals it. B-tree / GIST /
    // vector indexes need no handling here: the fast-path gate never admits
    // an overlay onto tables that declare them.
    if table_has_gin {
        {
            use basin_storage::index::index_maint::GinPostingListMaintainer;
            if let Some(maint) = GinPostingListMaintainer::new(
                engine.gin_index_registry().as_ref(),
                &project,
                table,
                &meta,
            ) {
                match added.first() {
                    Some(new_file) if removed.is_empty() => {
                        // The narrowed rewrite touched no cold file (every
                        // overlay key probed definitively Absent): the
                        // override rows were appended into a fresh file with
                        // nothing to purge. old == new makes the purge half a
                        // no-op while the rebuild indexes + seals the file.
                        maint.on_file_replaced(&new_file.path, &new_file.path, &index_batches);
                    }
                    Some(new_file) => {
                        for old_path in &removed {
                            maint.on_file_replaced(old_path, &new_file.path, &index_batches);
                        }
                    }
                    None => {
                        // Every merged row was tombstoned — no replacement
                        // file; just purge the replaced files' stale postings.
                        for old_path in &removed {
                            maint.on_file_removed(old_path);
                        }
                    }
                }
            }
        }
        // FTS (tsvector_ops GIN) twin: purge replaced paths, re-index the
        // replacement file's lexemes under their writer-aligned row-group
        // ordinals, and seal it so `fts_empty_probe_is_trustworthy` /
        // `apply_gin_fts_pruning_for_query` regain completeness post-drain.
        let fts_cols: Vec<String> = meta
            .indexes
            .iter()
            .filter(|idx| {
                idx.access_method == "gin"
                    && idx.opclass.as_deref() == Some("tsvector_ops")
                    && idx.columns.len() == 1
            })
            .map(|idx| idx.columns[0].clone())
            .collect();
        if !fts_cols.is_empty() {
            use arrow_array::Array;
            let fts = engine.gin_fts_registry();
            // Same row-group-size priority as the writer / CREATE INDEX
            // backfill (`row_block_size` > `row_group_rows` > default) so the
            // recorded ordinals line up with the on-disk layout.
            let rg_size = meta
                .row_block_size
                .map(|v| v as usize)
                .or(meta.row_group_rows)
                .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
                .max(1);
            for col in &fts_cols {
                for old_path in &removed {
                    fts.remove_file(&project, table, col, old_path);
                }
                let Some(new_file) = added.first() else {
                    continue;
                };
                let mut file_row_off = 0usize;
                let mut indexed_any = false;
                for batch in &index_batches {
                    if let Ok(col_idx) = batch.schema().index_of(col) {
                        if let Some(arr) = batch
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                        {
                            indexed_any = true;
                            for row in 0..arr.len() {
                                if arr.is_null(row) {
                                    continue;
                                }
                                let file_row = file_row_off + row;
                                fts.index_row(
                                    &project,
                                    table,
                                    col,
                                    arr.value(row),
                                    &new_file.path,
                                    (file_row / rg_size) as u32,
                                    file_row as u64,
                                );
                            }
                        }
                    }
                    file_row_off += batch.num_rows();
                }
                // Seal only when the column was actually found + processed —
                // claiming completeness over rows we never read would let the
                // probe paths prune real matches.
                if indexed_any {
                    fts.mark_file_indexed(&project, table, col, &new_file.path);
                }
            }
        }
    }
    // B-tree secondary-index maintenance — the materialize twin of
    // `maintain_btree_secondary_on_replace` (the cold CoW commit paths). Now
    // that single-column B-tree-indexed tables are admitted to the overlay
    // fast path (see the index gate in `delete_fastpath_table_eligible` /
    // `try_resolve_fast_path_update`), a drain MUST keep the
    // `ProjectIndexRegistry` location sets complete over the live file set:
    // purge every replaced file's locations and re-register the replacement
    // file's rows. Without this the `fast_select` HIT allowlist would prune to
    // a stale file set and silently drop the replacement file's rows (the
    // reverted-attempt failure mode). The `fast_select` probe additionally
    // skips the allowlist entirely while an overlay is live
    // (`table_has_live_overlay` guard), so pruning resumes only AFTER this
    // drain restores completeness. No `mark_file_indexed`: the B-tree HIT
    // contract is "every live file's rows are registered" (maintained on every
    // file-creating path), not per-file sealing — mirroring the maintainer.
    if table_has_btree_secondary {
        let registry = engine.secondary_index_registry();
        let storage = &engine.config().storage;
        // Same row-group-size priority as the writer / CREATE INDEX backfill so
        // the re-registered row-group ordinals line up with the on-disk layout
        // (Parquet read hint only; Vortex uses the file-level allowlist).
        let rg_size = meta
            .row_block_size
            .map(|v| v as usize)
            .or(meta.row_group_rows)
            .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
            .max(1);
        let btree_cols: Vec<String> = meta
            .indexes
            .iter()
            .filter(|idx| {
                idx.access_method != "gin"
                    && idx.access_method != "gist"
                    && idx.columns.len() == 1
                    && !idx.columns[0].starts_with("expr:")
            })
            .map(|idx| idx.columns[0].clone())
            .collect();
        for col in &btree_cols {
            if !registry.is_loaded(&project, table, col) {
                // Pull the persisted sidecar into RAM — maintenance must apply
                // to the FULL index, never seed a partial one.
                crate::secondary_index::load_index(registry, storage, &project, table, col).await;
                if !registry.is_loaded(&project, table, col) {
                    continue;
                }
            }
            for old_path in &removed {
                registry.remove_file_from_index(&project, table, col, old_path);
            }
            if let Some(new_file) = added.first() {
                let mut file_row_off = 0usize;
                for batch in &index_batches {
                    crate::executor::backfill_btree_batch(
                        registry.as_ref(),
                        &project,
                        table,
                        col,
                        batch,
                        &new_file.path,
                        rg_size,
                        file_row_off,
                    );
                    file_row_off += batch.num_rows();
                }
            }
            // Re-persist so a restart's lazy sidecar load can't resurrect the
            // stale pre-rewrite locations (the B-tree sidecar survives restarts).
            crate::secondary_index::flush_index(registry, storage, &project, table, col).await;
        }
    }
    // Physically delete the just-replaced files so a subsequent
    // `list_data_files_with_stats` (which lists the object store directly, not
    // the catalog) doesn't return them alongside the new merged file. Without
    // this the cold-path UPDATE/DELETE that triggered the materialize would
    // re-read the stale base, rewrite it with the SET applied, and emit a file
    // containing rows duplicated against the materialized file — yielding the
    // "+1 lost on the overlaid row" failure mode in #95. Mirrors every other
    // commit_replace site (exec_update / exec_delete / soft_delete) which also
    // pairs the catalog swap with the physical delete.
    delete_objects_engine(engine, project, table, meta.schema.as_ref(), &removed).await?;
    // Ack the materialized overlay at the seqs captured by the dirty snapshot
    // (S4 age-based residency): acked tombstones are REMOVED (the cold delete
    // is committed — nothing left to suppress), acked `Update`s are re-tagged
    // as retained CLEAN `Row`s (free residency for the rows we just rewrote —
    // their bytes are byte-identical to the replacement file), and any
    // overlay write that landed AFTER the snapshot carries a higher seq and
    // survives DIRTY for the next materialize/flush. The legacy
    // `remove_flushed` dropped whole chains by key, destroying post-snapshot
    // writes AND forfeiting residency.
    //
    // NOTE on ordering: `commit_replace` above already evicted this table's
    // PREVIOUSLY-retained clean rows (a CoW rewrite may change values under
    // them); the entries re-acked clean here are the overlay values that
    // rewrite just materialized, so retaining them is exact.
    let freed = entry.memtable.mark_flushed(&acks);
    if freed > 0 {
        registry.release_bytes(&project, freed);
    }
    if registry.config().retain_secs == 0 {
        // Retain-nothing kill switch: immediately evict what the ack left
        // clean — the pre-S4 drain-at-materialize end state (mirrors the
        // hottier flush worker's step 6).
        let evicted = entry.memtable.evict_clean(u64::MAX, None);
        if evicted > 0 {
            registry.release_bytes(&project, evicted);
        }
    }
    Ok(())
}

/// Compute the subset of `live_files` whose PK zone-map/bloom indicates the
/// file MAY contain at least one of the overlay's `keys`. This is the
/// row-targeted narrowing for `materialize_hot_overlay_into_cold`.
///
/// Returns:
/// * `Some(set)` — narrow the rewrite to these file paths. An empty set means
///   no cold file needs to be rewritten (override rows can still be appended
///   into a fresh file).
/// * `None` — fall back to the full live set. Triggered when (a) the table
///   doesn't have a single-column PK, (b) any overlay key's PK type isn't
///   supported by [`pk_row_key_to_scalar`] (e.g. UUID, Decimal), or (c) the
///   PK column is missing from the schema. The safety contract: any key we
///   cannot localize MUST be materialized against the full live-file set so
///   the merge can't lose its cold counterpart.
fn narrow_materialize_files(
    keys: &[&[u8]],
    pk_col: &str,
    pk_dt: &arrow_schema::DataType,
    live_files: &[DataFileRef],
    schema: &arrow_schema::Schema,
) -> Option<std::collections::HashSet<String>> {
    if keys.is_empty() {
        return Some(std::collections::HashSet::new());
    }
    // Decode every key to a ScalarValue up front. If ANY decode fails the
    // probe is unsafe (we'd silently drop a possible cold match), so we bail
    // to the full-scan path.
    let mut scalars: Vec<basin_storage::ScalarValue> = Vec::with_capacity(keys.len());
    for raw in keys {
        let rk = basin_hottier::RowKey::from_bytes(raw.to_vec());
        let s = pk_row_key_to_scalar(&rk, pk_dt)?;
        scalars.push(s);
    }
    match crate::index_probe::pk_point_probe_multi(pk_col, &scalars, live_files, schema) {
        crate::index_probe::PkProbeOutcome::Absent { .. } => {
            Some(std::collections::HashSet::new())
        }
        crate::index_probe::PkProbeOutcome::Candidates { paths, .. } => {
            Some(paths.into_iter().map(|p| p.to_string()).collect())
        }
    }
}

/// Defense-in-depth filter for the cold-path lister output.
///
/// The cold-path lister (`Storage::list_data_files_with_stats`) scans
/// the object store directly. Post-#94/#95 the physical delete that
/// follows `commit_replace` is fire-and-forget, so a freshly-listed
/// directory can briefly include files that the catalog already
/// considers removed. Intersecting with `meta.live_data_files()`
/// (which is computed by walking snapshots and applying every
/// `removed_paths`) filters those ghosts out before any read decision
/// is made on them. The catalog is the source of truth — this filter
/// just enforces it on the cold path.
async fn filter_to_live_data_files(
    sess: &ProjectSession,
    table: &TableName,
    listed: Vec<DataFile>,
) -> Result<Vec<DataFile>> {
    if listed.is_empty() {
        return Ok(listed);
    }
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let live: std::collections::HashSet<String> = meta
        .live_data_files()
        .into_iter()
        .map(|f| f.path)
        .collect();
    Ok(listed
        .into_iter()
        .filter(|f| live.contains(f.path.as_ref()))
        .collect())
}

/// Write the replacement batches as one new Parquet file (none if `batches`
/// is empty). We concat first because `Storage::write_batch` is one-batch-
/// per-call; a multi-batch table would otherwise produce N replacement files
/// per UPDATE which fragments the Parquet base needlessly.
/// ADR 0027 Phase 4 — extend a copy-on-write replacement schema + its batches
/// with the promoted `__promoted$col$key` shadow column(s).
///
/// Every cold-path rewrite (`exec_update` / `exec_delete` / overlay
/// materialize) MUST emit the shadow column for each promoted JSONB path,
/// otherwise the replacement file becomes a live file missing the column and
/// the promoted-column read guard ("every live file carries the shadow column")
/// fails for the WHOLE table — silently demoting every subsequent
/// `payload->>'key'` query to a full per-row JSONB UDF scan for as long as the
/// file stays live (confirmed: a single post-promotion `jsonb_set … WHERE
/// id < 10` UPDATE at 1M demoted a later `COUNT(*) … WHERE payload->>'category'
/// = …` from ~ms to ~3.4s).
///
/// `materialize_promoted_columns` is idempotent (skips a column already present
/// on a batch — cold rows read from a backfilled file already carry it; only
/// freshly-SET / pre-backfill rows are filled), and always appends new shadow
/// columns at the END, matching the extended schema's field order. Returns the
/// catalog schema unchanged (and batches untouched) when no paths are promoted.
fn extend_replacement_with_shadow_cols(
    base_schema: &Arc<Schema>,
    promoted_paths: &[basin_catalog::PromotedJsonbPath],
    batches: Vec<RecordBatch>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    if promoted_paths.is_empty() {
        return Ok((base_schema.clone(), batches));
    }
    // Build the target schema: base fields + one Utf8 shadow field per promoted
    // path that the base schema doesn't already carry, appended at the END.
    let mut fields: Vec<Arc<arrow_schema::Field>> = base_schema.fields().iter().cloned().collect();
    for path in promoted_paths {
        let name = path.shadow_col_name();
        if base_schema.field_with_name(&name).is_err() {
            fields.push(Arc::new(arrow_schema::Field::new(
                &name,
                arrow_schema::DataType::Utf8,
                true,
            )));
        }
    }
    let extended = Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        base_schema.metadata().clone(),
    ));
    // Materialise the (fresh) shadow values, then reorder every batch's columns
    // to EXACTLY the extended-schema field order by name. Cold-read batches may
    // carry the shadow column at a different position than the extended schema
    // appends it; concat requires identical schemas, so we project by name here
    // (filling an all-NULL column for any field a batch somehow lacks) rather
    // than relying on positional alignment.
    let materialized: Result<Vec<RecordBatch>> = batches
        .into_iter()
        .map(|b| {
            let m = crate::promoted_columns::materialize_promoted_columns(&b, promoted_paths)?;
            // Reorder columns to the extended-schema field order by NAME,
            // preserving each column's actual physical type (downstream
            // `normalize_batch_to_schema` coerces Binary↔LargeBinary etc. before
            // the concat). Build the per-batch schema from the real column
            // types so `RecordBatch::try_new` doesn't reject a type mismatch.
            let mut out_fields: Vec<Arc<arrow_schema::Field>> =
                Vec::with_capacity(extended.fields().len());
            let mut out_cols: Vec<arrow_array::ArrayRef> =
                Vec::with_capacity(extended.fields().len());
            for f in extended.fields() {
                match m.schema().index_of(f.name()) {
                    Ok(i) => {
                        out_fields.push(m.schema().field(i).clone().into());
                        out_cols.push(m.column(i).clone());
                    }
                    Err(_) => {
                        // Field absent from this batch — supply an all-NULL
                        // column of the declared type.
                        out_fields.push(f.clone());
                        out_cols.push(arrow_array::new_null_array(f.data_type(), m.num_rows()));
                    }
                }
            }
            let per_batch_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
                out_fields,
                m.schema().metadata().clone(),
            ));
            RecordBatch::try_new(per_batch_schema, out_cols)
                .map_err(|e| BasinError::internal(format!("shadow-col reproject: {e}")))
        })
        .collect();
    Ok((extended, materialized?))
}

/// B-tree secondary-index maintenance for copy-on-write replacement commits
/// (the UPDATE / DELETE slow paths). Mirrors the GIN posting-list and GIST
/// interval-tree maintenance blocks at the same call sites: purge every
/// removed file's locations from the `ProjectIndexRegistry`, then re-register
/// each replacement file from the batches that were written into it, and
/// re-flush the persisted sidecar.
///
/// Why purge alone is NOT enough: `fast_select`'s secondary-index probe
/// treats a HIT as a definitive file allowlist ("skip any live file NOT in
/// the location set"). With no maintenance at all, a key whose locations all
/// point at the replaced (now dead) file prunes EVERY live file and the read
/// silently returns zero rows. With purge-only
/// maintenance, a key that has rows in BOTH an untouched file and the
/// (unregistered) replacement file would HIT on the untouched file alone and
/// the pruned read would drop the replacement file's rows. Only
/// purge + re-register keeps the per-key location sets complete over the
/// live file set, which is what the HIT-is-authoritative contract requires.
///
/// Indexes that are neither in RAM nor lazily loadable from their sidecar are
/// left untouched: seeding a PARTIAL index containing only the replacement
/// entries would itself violate the contract above, while an absent index
/// keeps probing as MISS → full pruned scan, which is always correct.
///
/// `rewrites` pairs each NEW file path with the (pre-write) batches that the
/// caller handed `write_replacement{,_per_file}` for it. Batches are
/// normalised to the catalog schema before extraction so the indexed column
/// downcasts in `backfill_btree_batch` see the same physical types the
/// INSERT-path extractor indexed (`Utf8View`/`Binary` decode variants would
/// otherwise extract nothing and silently leave the key sets incomplete).
async fn maintain_btree_secondary_on_replace(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    removed_paths: &[String],
    rewrites: &[(String, Vec<RecordBatch>)],
) {
    // Mirror the INSERT-path dispatch (`maintain_secondary_indexes_on_insert`
    // in executor.rs): every single-column non-expression index that is not
    // GIN/GIST feeds the B-tree location registry (columns whose Arrow type
    // the extractor doesn't support no-op there, exactly as on INSERT).
    let mut cols: Vec<String> = meta
        .indexes
        .iter()
        .filter(|idx| {
            idx.access_method != "gin"
                && idx.access_method != "gist"
                && idx.columns.len() == 1
                && !idx.columns[0].starts_with("expr:")
        })
        .map(|idx| idx.columns[0].clone())
        .collect();
    cols.sort_unstable();
    cols.dedup();
    if cols.is_empty() {
        return;
    }
    let registry = sess.engine.secondary_index_registry();
    let storage = &sess.engine.config().storage;
    // Same row-group-size priority as the writer / CREATE INDEX backfill so
    // the re-registered row-group ordinals line up with the on-disk layout
    // (`row_group_selection` is a Parquet-only read hint; Vortex ignores it
    // and uses only the file-level allowlist).
    let rg_size = meta
        .row_block_size
        .map(|v| v as usize)
        .or(meta.row_group_rows)
        .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
        .max(1);
    let normalized: Vec<(String, Vec<RecordBatch>)> = rewrites
        .iter()
        .map(|(path, batches)| {
            let n = batches
                .iter()
                .map(|b| {
                    crate::hot_tombstone::normalize_batch_to_schema(
                        b.clone(),
                        meta.schema.as_ref(),
                    )
                })
                .collect();
            (path.clone(), n)
        })
        .collect();
    for col in &cols {
        if !registry.is_loaded(&sess.project, table, col) {
            // Pull the persisted sidecar into RAM first — maintenance must
            // apply to the FULL index, never seed a partial one.
            crate::secondary_index::load_index(registry, storage, &sess.project, table, col)
                .await;
            if !registry.is_loaded(&sess.project, table, col) {
                continue;
            }
        }
        for p in removed_paths {
            registry.remove_file_from_index(&sess.project, table, col, p);
        }
        for (new_path, batches) in &normalized {
            let mut file_row_off = 0usize;
            for batch in batches {
                crate::executor::backfill_btree_batch(
                    registry.as_ref(),
                    &sess.project,
                    table,
                    col,
                    batch,
                    new_path,
                    rg_size,
                    file_row_off,
                );
                file_row_off += batch.num_rows();
            }
        }
        // Re-persist so a restart's lazy sidecar load can't resurrect the
        // stale pre-rewrite locations (unlike the RAM-only GIN posting list,
        // the B-tree sidecar survives restarts).
        crate::secondary_index::flush_index(registry, storage, &sess.project, table, col).await;
    }
}

async fn write_replacement(
    sess: &ProjectSession,
    table: &TableName,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFileRef>> {
    write_replacement_engine(&sess.engine, sess.project, table, schema, batches).await
}

/// Sess-less core of [`write_replacement`] (the body only ever consulted
/// `sess.engine` + `sess.project`). Split out so the engine-callable
/// [`materialize_overlay_for_table`] can reuse it.
async fn write_replacement_engine(
    engine: &crate::Engine,
    project: ProjectId,
    table: &TableName,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFileRef>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    // Honour the per-table writer overrides (bloom-filter columns,
    // row-group size) on the rewrite path too; otherwise the
    // copy-on-write replacement file would silently lose the table's
    // configured pruning aids.
    let meta = engine
        .config()
        .catalog
        .load_table(&project, table)
        .await?;
    // ADR 0027 Phase 4: extend the replacement schema + batches with promoted
    // shadow column(s) so the rewritten file carries them (see
    // `extend_replacement_with_shadow_cols`). No-op when nothing is promoted.
    let (schema, batches) =
        extend_replacement_with_shadow_cols(&schema, &meta.promoted_jsonb_paths, batches)?;
    // Normalize each batch to the target (catalog) physical types so a mix of
    // Vortex-cold-decoded (`Binary`/`Utf8View`) and memtable-IPC-decoded
    // (`LargeBinary`/`Utf8`) batches concat cleanly. Without this a JSONB
    // column round-tripped through Vortex (catalog `LargeBinary`, decoded
    // `Binary`) makes `concat_batches` fail with
    // "concatenate arrays of different data types (Binary, LargeBinary)".
    // No-op for Parquet tables and any caller whose batches already match.
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| crate::hot_tombstone::normalize_batch_to_schema(b, schema.as_ref()))
        .collect();
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
    let opts = basin_storage::WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        // #204: default to the single-column PK when no explicit clustering
        // is declared, so the rewritten (whole-file-merged) replacement is
        // PK-sorted and its row-group / zone ranges are disjoint.
        cluster_columns: if meta.cluster_columns.is_empty() {
            meta.default_cluster_cols()
        } else {
            meta.cluster_columns.clone()
        },
        // Phase 3: honour the table's persisted on-disk format so the
        // copy-on-write replacement file matches the rest of the table.
        // Defaults to Parquet (catalog default) — byte-identical to the
        // legacy rewrite path for every Parquet table.
        file_format: crate::executor::map_file_format(meta.file_format),
        row_block_size: meta.row_block_size,
        bloom_columns: meta.global_sort_order.clone().unwrap_or_default(),
        // Vortex encoder cascade: Fast — a copy-on-write rewrite is a
        // latency-critical foreground op (the client is waiting on the
        // UPDATE/DELETE), so it takes the cheap cascade; background
        // compaction re-encodes with Best later via its own write path.
        // Ignored for Parquet tables (see `WriteOptions::encoding_mode`).
        encoding_mode: basin_storage::EncodingMode::Fast,
        ..Default::default()
    };
    let df = engine
        .config()
        .storage
        .write_batch_with_options(&project, table, &part, &merged, &opts)
        .await?;
    Ok(vec![DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    }])
}

/// Inv-bulk-UPDATE #182 / Bulk-W4: write the replacement batches as N files,
/// one per source file, preserving the original on-disk granularity instead
/// of concat-ing 1M rows into a single mega-file and serial-encoding it.
///
/// Each `(old_path, batches)` group becomes its own output file. Groups whose
/// batches sum to zero rows (an UPDATE that deleted everything in that file —
/// not currently produced by the SET path, but defensive) are skipped so we
/// don't emit empty Parquet. The PUTs are independent object-store writes, so
/// we fan them out with `buffer_unordered`; on S3 the N parallel uploads
/// compound the read-loop parallelism Bulk-W3 added.
///
/// Returns `(old_path, DataFileRef)` pairs sorted by `old_path`, so the
/// caller's GIN/GIST/JSONB index maintenance can map each replaced file to
/// the precise new file that now holds its rows (vs. the old single-file
/// path that lumped every old file onto one new file).
async fn write_replacement_per_file(
    sess: &ProjectSession,
    table: &TableName,
    schema: Arc<Schema>,
    groups: Vec<(String, Vec<RecordBatch>)>,
) -> Result<Vec<(String, DataFileRef)>> {
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    // Load writer overrides ONCE (not per file) so N parallel writes don't
    // hammer the catalog. Mirrors `write_replacement`'s option derivation.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let opts = Arc::new(basin_storage::WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        // #204: default to the single-column PK when no explicit clustering
        // is declared (per-file rewrite still sorts each merged output file).
        cluster_columns: if meta.cluster_columns.is_empty() {
            meta.default_cluster_cols()
        } else {
            meta.cluster_columns.clone()
        },
        file_format: crate::executor::map_file_format(meta.file_format),
        row_block_size: meta.row_block_size,
        bloom_columns: meta.global_sort_order.clone().unwrap_or_default(),
        // Vortex encoder cascade: Fast — same policy as `write_replacement`:
        // the per-file UPDATE rewrite is a foreground op the client waits on,
        // and background compaction re-encodes with Best later. Ignored for
        // Parquet tables.
        encoding_mode: basin_storage::EncodingMode::Fast,
        ..Default::default()
    });

    let storage = sess.engine.config().storage.clone();
    let project = sess.project.clone();
    let table_owned = table.clone();
    let concurrency = update_scan_concurrency(groups.len());

    // ADR 0027 Phase 4: extend the per-file rewrite schema with promoted shadow
    // column(s) once, and materialise them into each group's batches inside the
    // task. Same rationale as `write_replacement`: a per-file UPDATE rewrite
    // that drops the shadow column makes the replacement file fail the
    // promoted-column read guard, demoting every later `payload->>'key'` query
    // to a full per-row JSONB UDF scan. (This is the path the bench's
    // `jsonb_set … WHERE id < 10` UPDATE actually takes.)
    let promoted_paths = Arc::new(meta.promoted_jsonb_paths.clone());
    let schema = {
        let (extended, _) =
            extend_replacement_with_shadow_cols(&schema, &promoted_paths, Vec::new())?;
        extended
    };

    let mut results: Vec<(String, DataFileRef)> = futures::stream::iter(groups.into_iter())
        .map(|(old_path, batches)| {
            let storage = storage.clone();
            let project = project.clone();
            let table = table_owned.clone();
            let schema = schema.clone();
            let opts = opts.clone();
            let promoted_paths = promoted_paths.clone();
            async move {
                // ADR 0027 Phase 4: materialise promoted shadow column(s) into
                // each batch (idempotent; no-op when nothing is promoted) so the
                // concat target `schema` (extended above) is satisfied and the
                // rewritten file carries the column.
                let batches: Vec<RecordBatch> = batches
                    .into_iter()
                    .map(|b| {
                        crate::promoted_columns::materialize_promoted_columns(&b, &promoted_paths)
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Normalize to catalog physical types before concat (same
                // Binary/LargeBinary + Utf8 view-family rationale as
                // `write_replacement`). The bulk-UPDATE-by-range path that
                // feeds this fn is the canonical trigger for the JSONB
                // `events.payload` concat failure at scale ≥10k.
                let batches: Vec<RecordBatch> = batches
                    .into_iter()
                    .map(|b| {
                        crate::hot_tombstone::normalize_batch_to_schema(b, schema.as_ref())
                    })
                    .collect();
                let merged = if batches.len() == 1 {
                    batches.into_iter().next().unwrap()
                } else {
                    let refs: Vec<&RecordBatch> = batches.iter().collect();
                    arrow_select::concat::concat_batches(&schema, refs).map_err(|e| {
                        BasinError::internal(format!("concat batches for per-file rewrite: {e}"))
                    })?
                };
                if merged.num_rows() == 0 {
                    // Defensive: a file whose every row vanished — nothing to
                    // write, the old file is still removed via replaced_paths.
                    return Ok::<Option<(String, DataFileRef)>, BasinError>(None);
                }
                let part = PartitionKey::default_key();
                let df = storage
                    .write_batch_with_options(&project, &table, &part, &merged, &opts)
                    .await?;
                Ok(Some((
                    old_path,
                    DataFileRef {
                        path: df.path.as_ref().to_string(),
                        size_bytes: df.size_bytes,
                        row_count: df.row_count,
                        column_stats: df.column_stats.clone(),
                        bloom_filters: df.bloom_filters.clone(),
                        hll_sketches: std::collections::BTreeMap::new(),
                        tdigest_sketches: std::collections::BTreeMap::new(),
                    },
                )))
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<Result<Option<(String, DataFileRef)>>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Option<(String, DataFileRef)>>>>()?
        .into_iter()
        .flatten()
        .collect();

    // Deterministic commit order: independent of buffer_unordered completion.
    results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(results)
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
    commit_replace_engine(&sess.engine, sess.project, table, expected, removed, added).await
}

/// Sess-less core of [`commit_replace`] (the body only ever consulted
/// `sess.engine` + `sess.project`). Split out so the engine-callable
/// [`materialize_overlay_for_table`] can reuse it.
async fn commit_replace_engine(
    engine: &crate::Engine,
    project: ProjectId,
    table: &TableName,
    expected: basin_catalog::SnapshotId,
    removed: Vec<String>,
    added: Vec<DataFileRef>,
) -> Result<()> {
    // Optimistic-locking semantics: the catalog rejects the commit when the
    // table snapshot has advanced past `expected`. We MUST propagate that
    // `CommitConflict` to the router so the entire statement re-runs against
    // the fresh snapshot — re-evaluating the WHERE clause from scratch.
    //
    // Earlier versions of this helper silently swallowed `CommitConflict`
    // and re-applied the *same* `removed`/`added` plan against the newer
    // snapshot. That broke OCC: a TypeORM-style
    //   UPDATE t SET v = $1, version = version + 1
    //   WHERE id = $2 AND version = $3
    // could observe both concurrent writers "win" because the loser blindly
    // replayed its stale removed/added list — clobbering the winner's
    // change instead of observing that no row now matches version = $3.
    //
    // The router (`execute_with_conflict_retry`) already handles the
    // transparent retry with backoff; bouncing the error up gives the
    // statement a chance to re-read the new snapshot, re-evaluate the
    // predicate, and either commit on a still-matching row or correctly
    // return zero affected rows.
    engine
        .config()
        .catalog
        .replace_data_files(&project, table, expected, removed, added)
        .await?;
    // S4 age-based residency: a successful copy-on-write replace may have
    // changed row values UNDER this table's retained CLEAN memtable entries
    // (clean = "byte-identical to cold", which the rewrite just falsified).
    // Drop exactly the clean set for THIS table: the registry's memtable is
    // already per-(project, table), so `evict_clean(u64::MAX, None)` on its
    // memtable is the table-scoped clear (no registry-level table-scoped API
    // is needed). DIRTY entries are intentionally untouched — they are
    // committed fast-path overlay writes that legitimately override whatever
    // the rewrite produced, and dropping them would lose those writes.
    let registry = engine.memtable_registry();
    if let Some(entry) = registry.get(&project, table) {
        let freed = entry.memtable.evict_clean(u64::MAX, None);
        if freed > 0 {
            registry.release_bytes(&project, freed);
        }
    }
    Ok(())
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
    delete_objects_engine(&sess.engine, sess.project, table, schema, paths).await
}

/// Sess-less core of [`delete_objects`] (the body only ever consulted
/// `sess.engine` + `sess.project`). Split out so the engine-callable
/// [`materialize_overlay_for_table`] can reuse it.
async fn delete_objects_engine(
    engine: &crate::Engine,
    project: ProjectId,
    table: &TableName,
    schema: &Schema,
    paths: &[String],
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let storage = engine.config().storage.clone();
    let store = storage.project_object_store(&project);
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

    // Build the full list of objects to delete (data files + any HNSW
    // sidecars). Then hand off to a fire-and-forget tokio task so the
    // caller's UPDATE/DELETE returns as soon as the catalog commit has
    // landed. Defense-in-depth against #94/#95 is provided at every
    // `list_data_files_with_stats` call site via the
    // `live_data_files` HashSet intersect — even if a stale physical
    // file lingers on disk briefly, the cold-path lister cannot see it.
    type ObjectPath = object_store::path::Path;
    let mut all_paths: Vec<ObjectPath> = Vec::with_capacity(paths.len());
    let mut sidecars: Vec<ObjectPath> = Vec::new();
    for p in paths {
        all_paths.push(object_store::path::Path::from(p.as_str()));
        for column in &vector_columns {
            if let Some(sidecar) = vector_index_segment_key_for_data_file(
                root.as_ref(),
                &project,
                table,
                column,
                p,
            ) {
                sidecars.push(sidecar);
            }
        }
    }

    let store_for_task = store.clone();
    let storage_for_task = storage.clone();
    let catalog_for_task = engine.config().catalog.clone();
    let table_for_task = table.clone();
    tokio::spawn(async move {
        // LIVE-SET GUARD. These paths were handed to `replace_data_files`, and
        // we only get here when it returned Ok — but "the commit succeeded" and
        // "the catalog no longer references this path" are not the same claim.
        // The commit fans out across the META chain and N partition segments,
        // and it decides which chain owns each removed path by probing those
        // segments. Deleting a file the catalog still lists as live is the one
        // unrecoverable outcome in this system: its rows keep being counted
        // (count(*) sums the catalog) while no scan can ever produce them again
        // (scans enumerate the object store), and it is invisible to every
        // row-conservation guard we have, because the catalog's own arithmetic
        // still balances.
        //
        // So re-read the live set and delete only what the catalog agrees is
        // dead. An orphaned object wastes space and is reclaimable; a deleted
        // live object is gone. On any catalog error, skip the delete entirely —
        // leaking an object always beats destroying one.
        let live: std::collections::HashSet<String> =
            match catalog_for_task.load_table(&project, &table_for_task).await {
                Ok(meta) => meta.live_data_files().into_iter().map(|f| f.path).collect(),
                Err(e) => {
                    tracing::warn!(
                        target: "basin_engine",
                        error = %e,
                        "post-replace delete: cannot read the live set; skipping the delete \
                         (leaves orphans, never loses rows)"
                    );
                    return;
                }
            };
        let (still_live, deletable): (Vec<ObjectPath>, Vec<ObjectPath>) =
            all_paths.into_iter().partition(|p| live.contains(p.as_ref()));
        if !still_live.is_empty() {
            tracing::error!(
                target: "basin_engine",
                count = still_live.len(),
                paths = ?still_live.iter().map(|p| p.as_ref()).collect::<Vec<_>>(),
                "post-replace delete: REFUSING to delete files the catalog still lists as \
                 live — the replace did not remove them from their owning chain; leaving \
                 them in place"
            );
        }
        // Native batch path for the data files (S3 DeleteObjects on AWS,
        // 64-way buffer_unordered everywhere else) via the storage helper
        // so page-cache entries are invalidated atomically with the
        // physical deletes.
        if let Err(e) = storage_for_task
            .bulk_delete_files(&project, deletable)
            .await
        {
            tracing::warn!(
                target: "basin_engine",
                error = %e,
                "post-replace bulk delete failed; relying on catalog as source of truth"
            );
        }
        // Sidecars are best-effort — most files never have one. Issue
        // them through the per-file `delete` so NotFound on the missing
        // case is cheap.
        for sidecar in sidecars {
            if let Err(e) = store_for_task.delete(&sidecar).await {
                let msg = format!("{e}");
                if msg.contains("NotFound") || msg.contains("not found") {
                    tracing::debug!(path = %sidecar, "no hnsw sidecar to delete");
                } else {
                    tracing::warn!(path = %sidecar, error = %e, "hnsw sidecar delete failed");
                }
            }
        }
    });
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
                let built = build_assigned_column(field.data_type(), &original, mask, scalar)?;
                // VARCHAR(n)/CHAR(n): the SET may grow the value past the
                // declared limit (22001) or, for CHAR(n), need re-padding.
                // Unassigned columns keep `original` (already enforced at
                // INSERT) so only the assigned path needs the check.
                crate::dml::enforce_charlen_array(field, built)?
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
                let merged = crate::generated_cols::merge_by_mask(&original, &computed, mask)?;
                crate::dml::enforce_charlen_array(field, merged)?
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

/// Strip `"public"."t"."col"` / `"t"."col"` table-and-schema qualifiers
/// down to bare `"col"` in a RETURNING `SelectItem`.
///
/// The captured DML output batch is registered under `__basin_returning_src`,
/// not the user's table name. Without this rewrite a Prisma RETURNING shape
/// like `RETURNING "public"."User"."id"` would hit DataFusion as a column
/// reference qualified by `public.User`, which doesn't exist in the
/// `__basin_returning_src` table.
///
/// Scope: only the column-reference shape produced by ORMs is rewritten.
/// More complex expressions (function calls, arithmetic) pass through
/// unchanged so any DataFusion-valid expression keeps working. If those
/// inner expressions ever embed qualified column refs, we extend this
/// helper rather than auto-walking — keeping it AST-shallow keeps the
/// surface narrow and avoids accidentally rewriting an inner JOIN
/// reference some future RETURNING shape might need.
fn strip_table_qualifier_in_returning_item(item: SelectItem) -> SelectItem {
    use sqlparser::ast::SelectItem as SI;
    match item {
        SI::UnnamedExpr(e) => SI::UnnamedExpr(strip_table_qualifier_in_expr(e)),
        SI::ExprWithAlias { expr, alias } => SI::ExprWithAlias {
            expr: strip_table_qualifier_in_expr(expr),
            alias,
        },
        // `"public"."t".*` qualified wildcard collapses to plain `*` —
        // the captured batch already contains exactly the table's columns.
        SI::QualifiedWildcard(_, opts) => SI::Wildcard(opts),
        other => other,
    }
}

/// Walk a `SelectItem` expression's outer column references and strip
/// 2-part / 3-part qualifiers. See `strip_table_qualifier_in_returning_item`
/// for scope.
fn strip_table_qualifier_in_expr(expr: Expr) -> Expr {
    match expr {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            // `"t"."col"` → `"col"`
            [_tbl, col] => Expr::Identifier(col.clone()),
            // `"public"."t"."col"` → `"col"`
            [_schema, _tbl, col] => Expr::Identifier(col.clone()),
            // 4+ parts: not a shape any supported ORM emits — leave as-is
            // so DataFusion produces its own error message.
            _ => Expr::CompoundIdentifier(parts),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(strip_table_qualifier_in_expr(*inner))),
        other => other,
    }
}

/// Deep qualifier strip for an UPDATE SET right-hand-side expression. The RHS
/// is evaluated over a single temp table holding exactly the target table's
/// columns, so every qualified column reference can be safely de-qualified.
/// Django `F()` expressions arrive qualified — `SET pages = "dj_books"."pages"
/// + 1` — and otherwise fail to resolve ("No field named dj_books.pages").
/// Unlike [`strip_table_qualifier_in_expr`] (deliberately AST-shallow for the
/// RETURNING column-list shape), this recurses through the arithmetic nodes an
/// `F()` expression produces; other nodes pass through unchanged.
fn strip_table_qualifiers_deep(expr: Expr) -> Expr {
    match expr {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [_tbl, col] => Expr::Identifier(col.clone()),
            [_schema, _tbl, col] => Expr::Identifier(col.clone()),
            _ => Expr::CompoundIdentifier(parts),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(strip_table_qualifiers_deep(*inner))),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(strip_table_qualifiers_deep(*left)),
            op,
            right: Box::new(strip_table_qualifiers_deep(*right)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(strip_table_qualifiers_deep(*expr)),
        },
        // `(CASE … END)::integer` — Django's bulk_update wraps the CASE in a
        // cast; recurse into the cast operand so the inner qualified refs reach
        // the CASE/identifier arms below.
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => Expr::Cast {
            kind,
            expr: Box::new(strip_table_qualifiers_deep(*expr)),
            data_type,
            array,
            format,
        },
        // `CASE WHEN "t"."id" = 1 THEN … ELSE "t"."pages" END` — Django's
        // `bulk_update` emits a qualified CASE on the SET RHS. Recurse into the
        // operand, every WHEN condition/result, and the ELSE branch.
        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token,
            end_token,
            operand: operand.map(|o| Box::new(strip_table_qualifiers_deep(*o))),
            conditions: conditions
                .into_iter()
                .map(|w| sqlparser::ast::CaseWhen {
                    condition: strip_table_qualifiers_deep(w.condition),
                    result: strip_table_qualifiers_deep(w.result),
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(strip_table_qualifiers_deep(*e))),
        },
        other => other,
    }
}

/// Run the RETURNING projection over the captured input batches. Items
/// are rendered back to SQL and pushed into one DataFusion SELECT, so
/// any expression DataFusion understands (column refs, arithmetic,
/// function calls) works without a bespoke evaluator here.
pub(crate) async fn project_returning(
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
    //
    // RETURNING items may carry table / schema qualifiers from the
    // original user SQL (e.g. Prisma emits
    // `RETURNING "public"."User"."id", "public"."User"."name"`). The
    // captured input batch is registered under `__basin_returning_src`,
    // not the user's table name, so a verbatim render would fail with
    // `No field named public.User.id` from DataFusion. Strip
    // `"public"."t"."col"` / `"t"."col"` qualifiers down to `"col"`
    // before rendering — the column names line up with the projected
    // batch's schema, which carries bare column names.
    let rewritten_items: Vec<SelectItem> = items
        .iter()
        .cloned()
        .map(strip_table_qualifier_in_returning_item)
        .collect();
    let projection = rewritten_items
        .iter()
        .map(|it| it.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let projection_sql = format!("SELECT {projection} FROM __basin_returning_src");
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
        // DATE assignment — the scalar is i64 days-since-epoch (widened by
        // the literal parser), narrowed back to the Date32 i32 range.
        (DataType::Date32, ScalarValue::Int64(v)) => {
            let set_val = i32::try_from(*v).map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "UPDATE SET: value {v} out of range for DATE column"
                ))
            })?;
            let arr = original
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| BasinError::internal("expected Date32Array for SET"))?;
            let mut b = Date32Builder::with_capacity(n);
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
                // De-qualify column refs: the RHS is evaluated over a temp
                // table, so an ORM-qualified `F()` expression like
                // `"dj_books"."pages" + 1` must become `pages + 1`.
                AssignmentRhs::Expr(strip_table_qualifiers_deep(a.value.clone()).to_string())
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
pub(crate) fn try_literal_to_scalar(
    expr: &Expr,
    dt: &DataType,
    col: &str,
) -> Result<Option<ScalarValue>> {
    let (negated, inner) = peel_unary(expr);
    match (dt, inner) {
        // BIGINT / INT8 column — 64-bit signed.
        (
            DataType::Int64,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
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
        (
            DataType::Int32,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(ScalarValue::Int64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        // SMALLINT / INT2 column — 16-bit. Store as Int64 for the same reason.
        (
            DataType::Int16,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(ScalarValue::Int64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        // DOUBLE PRECISION / FLOAT8 column.
        (
            DataType::Float64,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
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
        (
            DataType::Float32,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(Some(ScalarValue::Float64(if negated {
                -parsed
            } else {
                parsed
            })))
        }
        (
            DataType::Utf8,
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(s),
                ..
            }),
        )
        | (
            DataType::Utf8,
            Expr::Value(ValueWithSpan {
                value: Value::DoubleQuotedString(s),
                ..
            }),
        )
        | (
            DataType::Utf8,
            Expr::Value(ValueWithSpan {
                value: Value::EscapedStringLiteral(s),
                ..
            }),
        )
        | (
            DataType::Utf8,
            Expr::Value(ValueWithSpan {
                value: Value::NationalStringLiteral(s),
                ..
            }),
        ) => {
            if negated {
                Err(BasinError::InvalidSchema(format!(
                    "cannot negate string literal in SET {col} = {expr}"
                )))
            } else {
                Ok(Some(ScalarValue::Utf8(s.clone())))
            }
        }
        (
            DataType::Boolean,
            Expr::Value(ValueWithSpan {
                value: Value::Boolean(b),
                ..
            }),
        ) => {
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
                Expr::Value(ValueWithSpan {
                    value: Value::Number(s, _),
                    ..
                }) => s.parse::<i64>().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad timestamp literal {s:?}: {e}"))
                })?,
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::DoubleQuotedString(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::EscapedStringLiteral(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::NationalStringLiteral(s),
                    ..
                }) => chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
                    .map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "bad RFC3339 timestamp {s:?} in SET {col}: {e}"
                        ))
                    })?,
                _ => return Ok(None),
            };
            Ok(Some(ScalarValue::Int64(if negated {
                -micros
            } else {
                micros
            })))
        }
        // DATE column — a `'YYYY-MM-DD'` string literal, coerced to
        // days-since-epoch (Arrow Date32). Widened to Int64 in the scalar
        // so the SET path narrows back to i32, mirroring the INT4/INT2
        // convention above. Non-string forms (`DATE '…'`, `'…'::DATE`,
        // `CURRENT_DATE`, …) flow through the expression fallback.
        (DataType::Date32, _) => {
            let s = match inner {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::DoubleQuotedString(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::EscapedStringLiteral(s),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::NationalStringLiteral(s),
                    ..
                }) => s,
                _ => return Ok(None),
            };
            if negated {
                return Err(BasinError::InvalidSchema(format!(
                    "cannot negate date literal in SET {col} = {expr}"
                )));
            }
            let days = crate::dml::parse_date32_string(s, col)?;
            Ok(Some(ScalarValue::Int64(i64::from(days))))
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
fn parse_compound_predicate(
    expr: &Expr,
    schema: &Schema,
    table_name: &str,
) -> Result<CompoundPredicate> {
    match expr {
        Expr::Nested(inner) => parse_compound_predicate(inner, schema, table_name),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(CompoundPredicate::And(vec![
                parse_compound_predicate(left, schema, table_name)?,
                parse_compound_predicate(right, schema, table_name)?,
            ])),
            BinaryOperator::Or => Ok(CompoundPredicate::Or(vec![
                parse_compound_predicate(left, schema, table_name)?,
                parse_compound_predicate(right, schema, table_name)?,
            ])),
            BinaryOperator::Eq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::LtEq
            | BinaryOperator::GtEq => parse_atom(left, op, right, schema, table_name),
            BinaryOperator::NotEq => {
                let atom = parse_atom(left, &BinaryOperator::Eq, right, schema, table_name)?;
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
            inner, schema, table_name,
        )?))),
        Expr::IsNull(col) => {
            let name = identifier_or_err(col, table_name)?;
            schema
                .index_of(&name)
                .map_err(|_| BasinError::InvalidSchema(format!("unknown column {name}")))?;
            Ok(CompoundPredicate::IsNull(name))
        }
        Expr::IsNotNull(col) => {
            let name = identifier_or_err(col, table_name)?;
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
            let name = identifier_or_err(col_expr, table_name)?;
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
        // `col BETWEEN low AND high` desugars to `col >= low AND col <= high`.
        // We route each bound through `parse_atom`, which already resolves the
        // column (bare/qualified), validates it against the schema, and
        // synthesises `>=`/`<=` as the `Lt OR Eq` / `Gt OR Eq` disjunctions the
        // storage atom enum understands. Both `evaluate_compound` (per-row cold
        // rewrite) and `evaluate_compound_for_pruning` (file-stat pruning)
        // already handle the resulting And/Or/Atom tree, so range deletes flow
        // through the existing copy-on-write path with no new storage atoms.
        // `NOT BETWEEN` wraps the whole conjunction in `Not` (Kleene-correct:
        // NULL bounds stay UNKNOWN → row is kept, matching Postgres).
        Expr::Between {
            expr: col_expr,
            negated,
            low,
            high,
        } => {
            let lower = parse_atom(col_expr, &BinaryOperator::GtEq, low, schema, table_name)?;
            let upper = parse_atom(col_expr, &BinaryOperator::LtEq, high, schema, table_name)?;
            let range = CompoundPredicate::And(vec![lower, upper]);
            if *negated {
                Ok(CompoundPredicate::Not(Box::new(range)))
            } else {
                Ok(range)
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
    table_name: &str,
) -> Result<CompoundPredicate> {
    // `<col> OP <literal>` or the swapped `<literal> OP <col>`.
    let (col, op, lit) = if let (Some(c), Ok(Some(l))) =
        (as_identifier(left, table_name), as_literal(right))
    {
        (c, op.clone(), l)
    } else if let (Some(c), Ok(Some(l))) = (as_identifier(right, table_name), as_literal(left)) {
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

/// Try to extract a column name from `e`, accepting both bare identifiers
/// (`col`) and qualified references against the table being mutated
/// (`table.col`, `schema.table.col`). Returns `None` when the expression
/// isn't a recognisable column reference — callers may then try the swapped
/// form (literal OP col). A qualified reference whose table prefix doesn't
/// match `table_name` is treated as "not a column ref" (returns `None`) so
/// the caller can produce the unsupported-shape error instead of silently
/// mis-routing a cross-table column. Comparisons are case-insensitive to
/// match how identifiers come back from the parser (and PG itself).
fn as_identifier(e: &Expr, table_name: &str) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::Nested(inner) => as_identifier(inner, table_name),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            // `table.col`
            [tbl, col] if tbl.value.eq_ignore_ascii_case(table_name) => Some(col.value.clone()),
            // `schema.table.col` — accept any schema qualifier (we don't yet
            // model multi-schema search paths in DML; the surrounding catalog
            // lookup has already resolved the table). The middle segment must
            // still match the mutated table.
            [_schema, tbl, col] if tbl.value.eq_ignore_ascii_case(table_name) => {
                Some(col.value.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

fn identifier_or_err(e: &Expr, table_name: &str) -> Result<String> {
    match e {
        Expr::Identifier(i) => Ok(i.value.clone()),
        Expr::Nested(inner) => identifier_or_err(inner, table_name),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [tbl, col] if tbl.value.eq_ignore_ascii_case(table_name) => Ok(col.value.clone()),
            [_schema, tbl, col] if tbl.value.eq_ignore_ascii_case(table_name) => {
                Ok(col.value.clone())
            }
            // Qualified to a different table: cross-table WHERE in DELETE /
            // UPDATE is legal in PG but not supported here. Emit a clearer
            // error so the user isn't left wondering why a qualified ref
            // "didn't parse".
            [tbl, _col] | [_, tbl, _col] => Err(BasinError::InvalidSchema(format!(
                "WHERE references column qualified by {:?}, but mutation targets {:?}; \
                 cross-table column references are not supported",
                tbl.value, table_name
            ))),
            _ => Err(BasinError::InvalidSchema(format!(
                "WHERE expects column identifier, got {e}"
            ))),
        },
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
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => {
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
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => {
            if negated {
                return Err(BasinError::InvalidSchema(
                    "cannot negate string literal in WHERE".into(),
                ));
            }
            Some(ScalarValue::Utf8(s.clone()))
        }
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => {
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
        TableFactor::Table { name, args, .. } => {
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

/// Resolve an `ObjectName` (bare or schema-qualified) to the bare table /
/// column identifier the rest of the DML path expects.
///
/// Accepted shapes:
/// - `"t"` — bare; returned as-is.
/// - `"public"."t"` — Prisma / TypeORM / Sequelize emit this verbatim on
///   every `findUnique` / `update` / `delete`. PostgreSQL's default
///   `search_path` resolves an unqualified table to `public`, so stripping
///   the qualifier is an identity transform under Basin's flat-namespace
///   catalog. Case-insensitive ASCII match to `"public"`.
///
/// - `"myapp"."t"` — any user-created / unknown schema aliases to the bare
///   table under the flat-namespace model, the same `resolve_schema` rule the
///   CREATE / INSERT / SELECT paths apply (so drizzle's `myapp.events` UPDATE
///   resolves the same way it was created).
///
/// Rejected shapes:
/// - A reserved schema (`auth`, `storage`, …) — those schemas are owned by
///   the engine and reach the catalog via `load_table_qualified`. A user DML
///   statement that names a reserved schema is rejected here with the "not in
///   search_path" message rather than being silently aliased to `public`,
///   which would mis-route the write.
/// - Three-or-more-part names (`"db"."public"."t"`) — rejected; PG
///   `database.schema.table` syntax has never been supported here.
fn single_part_name(name: &ObjectName) -> Result<&str> {
    match name.0.len() {
        1 => Ok(name.0[0].id_val()),
        2 => {
            let schema = name.0[0].id_val().as_str();
            // Flat-namespace model: `public` and any user-created / unknown
            // schema alias to the bare table (matching `resolve_schema` on the
            // CREATE / INSERT / SELECT paths, so `myapp.t` written by drizzle
            // resolves the same way for UPDATE / DELETE). A reserved schema
            // (`auth`, `storage`, …) is owned by the engine and must not be a
            // user DML target — reject it with the search_path message rather
            // than silently aliasing to `public`, which would mis-route the
            // write.
            if schema.eq_ignore_ascii_case("public")
                || basin_catalog::reserved_schema::ReservedSchema::from_str(schema).is_none()
            {
                Ok(name.0[1].id_val())
            } else {
                Err(BasinError::InvalidIdent(format!(
                    "schema {schema:?} not in search_path; basin only supports \
                     the 'public' schema today (got: {name})"
                )))
            }
        }
        _ => Err(BasinError::InvalidIdent(format!(
            "table name must have at most one schema qualifier: {name}"
        ))),
    }
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
fn inject_auto_update_assignments(schema: &Schema, assignments: &mut Vec<(usize, AssignmentRhs)>) {
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
    let assignments = vec![(
        sd_idx,
        AssignmentRhs::Scalar(ScalarValue::Int64(now_micros)),
    )];

    // Compose the effective predicate: `<user-pred> AND <sd_col> IS NULL`.
    // Without the IS NULL guard a re-DELETE on already-soft-deleted rows
    // would re-stamp the column and re-emit an audit row.
    let mut pred = match predicate_expr {
        None => CompoundPredicate::IsNull(sd_col.clone()),
        Some(e) => {
            let user_pred = parse_compound_predicate(e, schema.as_ref(), table.as_str())?;
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
    // Defense-in-depth (#94/#95): exclude any files the catalog
    // already removed even if the async cleanup hasn't unlinked them.
    let data_files = filter_to_live_data_files(sess, &table, data_files).await?;
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
                let befores =
                    read_file_to_batches(&storage, &sess.project, &f.path, schema.as_ref()).await?;
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
                let befores =
                    read_file_to_batches(&storage, &sess.project, &f.path, schema.as_ref()).await?;
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
    // Clone the rewritten batches for B-tree index maintenance BEFORE the
    // write consumes them (cheap Arc-buffer bumps).
    let rewritten_for_index: Vec<RecordBatch> = replacement_batches.iter().cloned().collect();
    let added_files = write_replacement(sess, &table, schema.clone(), replacement_batches).await?;
    commit_replace(
        sess,
        &table,
        meta.current_snapshot,
        replaced_paths.clone(),
        added_files.clone(),
    )
    .await?;
    dispatch_post_commit(&sess.engine, events);
    // B-tree secondary-index maintenance: the soft-delete rewrite replaces
    // files just like a hard DELETE (rows move to a new file), so stale
    // locations must be purged and the replacement re-registered. See
    // `maintain_btree_secondary_on_replace` for the soundness argument.
    {
        let rewrites: Vec<(String, Vec<RecordBatch>)> = added_files
            .first()
            .map(|f| vec![(f.path.clone(), rewritten_for_index)])
            .unwrap_or_default();
        Box::pin(maintain_btree_secondary_on_replace(
            sess,
            &table,
            &meta,
            &replaced_paths,
            &rewrites,
        ))
        .await;
    }
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
                let ValueWithSpan {
                    value: Value::Number(ref s, _),
                    ..
                } = *v
                else {
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
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                ref expr,
            } => {
                let Expr::Value(ref v) = **expr else {
                    panic!("expected Number inside UnaryOp, got {expr:?}");
                };
                let ValueWithSpan {
                    value: Value::Number(ref s, _),
                    ..
                } = *v
                else {
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
                let ValueWithSpan {
                    value: Value::Number(ref s, _),
                    ..
                } = *v
                else {
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
        let arr = Float32Array::from(vec![2.5f32]);
        let expr = arrow_col_value_to_expr(&arr, 0).expect("Float32 must not error");
        // The value is widened to f64 — just verify it's a Number and parses
        // back to something close to 2.5.
        match expr {
            Expr::Value(ref v) => {
                let ValueWithSpan {
                    value: Value::Number(ref s, _),
                    ..
                } = *v
                else {
                    panic!("expected Number literal, got {expr:?}");
                };
                let parsed: f64 = s.parse().expect("must be numeric string");
                assert!((parsed - 2.5f64).abs() < 0.01, "got {parsed}");
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
                let ValueWithSpan {
                    value: Value::Number(ref s, _),
                    ..
                } = *v
                else {
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
            sqlparser::ast::Statement::Query(q) => match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => match &s.projection[0] {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => e.clone(),
                    other => panic!("not an expr: {other:?}"),
                },
                other => panic!("not a SELECT: {other:?}"),
            },
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

    // ── scalar_from_array PK quoting (P1 security fix) ───────────────────────
    //
    // Verifies that `scalar_from_array` correctly quotes / escapes all types
    // that can appear as primary-key columns.  Un-escaped string injection
    // (e.g. a PK value of `'; DROP TABLE users; --`) would allow SQL injection
    // via the UPDATE/DELETE WHERE clause.

    /// A Utf8 PK value containing a single-quote must be escaped to `''`.
    #[test]
    fn scalar_from_array_utf8_single_quote_escaped() {
        use arrow_array::StringArray;
        let arr = StringArray::from(vec!["it's a test"]);
        let lit = scalar_from_array(&arr, 0, &DataType::Utf8).expect("Utf8 must succeed");
        assert_eq!(lit, "'it''s a test'", "single-quote must be doubled: {lit}");
    }

    /// A LargeUtf8 PK value containing a single-quote must also be escaped.
    #[test]
    fn scalar_from_array_large_utf8_quote_escaped() {
        use arrow_array::LargeStringArray;
        let arr = LargeStringArray::from(vec!["O'Brien"]);
        let lit =
            scalar_from_array(&arr, 0, &DataType::LargeUtf8).expect("LargeUtf8 must succeed");
        assert_eq!(lit, "'O''Brien'", "single-quote must be doubled: {lit}");
    }

    /// A Date32 PK renders as a quoted ISO-8601 date literal (not a bare integer).
    #[test]
    fn scalar_from_array_date32_is_quoted_iso() {
        use arrow_array::Date32Array;
        // Day 0 = 1970-01-01; day 1 = 1970-01-02.
        let arr = Date32Array::from(vec![1i32]);
        let lit = scalar_from_array(&arr, 0, &DataType::Date32).expect("Date32 must succeed");
        assert_eq!(lit, "'1970-01-02'", "Date32 must render as quoted ISO date: {lit}");
    }

    /// A Binary PK renders as a PostgreSQL hex-escape bytea literal.
    #[test]
    fn scalar_from_array_binary_is_hex_escaped() {
        use arrow_array::BinaryArray;
        let arr = BinaryArray::from(vec![b"\xde\xad\xbe\xef".as_slice()]);
        let lit = scalar_from_array(&arr, 0, &DataType::Binary).expect("Binary must succeed");
        assert_eq!(lit, "'\\xdeadbeef'", "Binary must render as hex-escaped literal: {lit}");
    }

    /// A NULL value renders as `NULL` regardless of type.
    #[test]
    fn scalar_from_array_null_renders_as_null() {
        use arrow_array::StringArray;
        let arr = StringArray::from(vec![None::<&str>]);
        let lit = scalar_from_array(&arr, 0, &DataType::Utf8).expect("null Utf8 must succeed");
        assert_eq!(lit, "NULL");
    }

    /// An unhandled Arrow type (e.g. Float16) must return an error rather than
    /// bare-interpolating an unescaped value.
    #[test]
    fn scalar_from_array_unhandled_type_returns_error() {
        // UInt8 is not in the handled set — it should error, not interpolate bare.
        use arrow_array::UInt8Array;
        let arr = UInt8Array::from(vec![42u8]);
        let result = scalar_from_array(&arr, 0, &DataType::UInt8);
        assert!(
            result.is_err(),
            "unhandled type UInt8 must return an error, not interpolate bare"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("unsupported PK column type"),
            "error message should mention unsupported type, got: {msg}"
        );
    }

    // ── qualified column refs in WHERE (TypeORM / Drizzle compat) ────────────
    //
    // TypeORM (and other ORMs) emit `WHERE "users"."id" IN ($1)` against the
    // extended protocol. sqlparser hands that back as `CompoundIdentifier(
    // ["users", "id"])`, which the WHERE parser used to reject with
    // "WHERE expects column identifier". These tests pin the fix.

    fn ident(s: &str) -> sqlparser::ast::Ident {
        sqlparser::ast::Ident::new(s)
    }

    /// `WHERE "users"."id" = 42` — qualifier matches the mutated table; the
    /// column name is extracted from the compound identifier.
    #[test]
    fn as_identifier_accepts_qualified_matching_table() {
        let e = Expr::CompoundIdentifier(vec![ident("users"), ident("id")]);
        assert_eq!(as_identifier(&e, "users"), Some("id".to_string()));
    }

    /// Qualifier comparison is ASCII-case-insensitive: `"Users"."id"` against
    /// table `users` resolves the same as the lowercase form.
    #[test]
    fn as_identifier_qualifier_case_insensitive() {
        let e = Expr::CompoundIdentifier(vec![ident("Users"), ident("id")]);
        assert_eq!(as_identifier(&e, "users"), Some("id".to_string()));
    }

    /// `"public"."users"."id"` — three-part qualifier; middle segment must
    /// match the mutated table.
    #[test]
    fn as_identifier_accepts_schema_qualified() {
        let e = Expr::CompoundIdentifier(vec![ident("public"), ident("users"), ident("id")]);
        assert_eq!(as_identifier(&e, "users"), Some("id".to_string()));
    }

    /// Qualifier referring to a different table: as_identifier returns None
    /// so parse_atom falls through to the swapped form (and ultimately the
    /// generic "WHERE atom must be…" error). identifier_or_err produces the
    /// explicit cross-table message.
    #[test]
    fn as_identifier_rejects_cross_table_qualifier() {
        let e = Expr::CompoundIdentifier(vec![ident("posts"), ident("id")]);
        assert_eq!(as_identifier(&e, "users"), None);
    }

    #[test]
    fn identifier_or_err_cross_table_message_is_explicit() {
        let e = Expr::CompoundIdentifier(vec![ident("posts"), ident("id")]);
        let err = identifier_or_err(&e, "users").expect_err("cross-table must error");
        let msg = err.to_string();
        assert!(
            msg.contains("cross-table"),
            "error should mention cross-table, got: {msg}"
        );
    }

    /// `DELETE FROM "users" WHERE "users"."id" IN (42)` — the literal form
    /// the bound case rewrites to. End-to-end via parse_compound_predicate.
    #[test]
    fn parse_compound_predicate_accepts_qualified_in_list() {
        use arrow_schema::{DataType, Field};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let expr = Expr::InList {
            expr: Box::new(Expr::CompoundIdentifier(vec![ident("users"), ident("id")])),
            list: vec![Expr::Value(ValueWithSpan {
                value: Value::Number("42".to_string(), false),
                span: sqlparser::tokenizer::Span::empty(),
            })],
            negated: false,
        };
        let pred = parse_compound_predicate(&expr, &schema, "users")
            .expect("qualified IN list must parse");
        match pred {
            CompoundPredicate::In(col, vals) => {
                assert_eq!(col, "id");
                assert_eq!(vals.len(), 1);
            }
            other => panic!("expected In(_, _), got {other:?}"),
        }
    }

    /// `UPDATE "users" SET … WHERE "users"."id" = 42` — the eq atom shape.
    #[test]
    fn parse_compound_predicate_accepts_qualified_eq_atom() {
        use arrow_schema::{DataType, Field};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![ident("users"), ident("id")])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(ValueWithSpan {
                value: Value::Number("42".to_string(), false),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        };
        let pred = parse_compound_predicate(&expr, &schema, "users")
            .expect("qualified eq must parse");
        match pred {
            CompoundPredicate::Atom(Predicate::Eq(col, _)) => assert_eq!(col, "id"),
            other => panic!("expected Atom(Eq(…)), got {other:?}"),
        }
    }

    // ── Hot-tier DELETE fast-path helpers ────────────────────────────────────
    //
    // Pin the parser shape recognised by `predicate_resolves_to_pk_list` and
    // the encoding of `pk_scalar_to_row_key`. End-to-end fast-path firing is
    // exercised by the dml_extras / orm_compat integration tests.

    fn num_expr(s: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    fn neg_num_expr(s: &str) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(num_expr(s)),
        }
    }

    fn str_expr(s: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s.to_string()),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    /// `id = 7` resolves to `[7]`.
    #[test]
    fn fast_path_eq_lit_resolves() {
        let e = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(num_expr("7")),
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("eq form must resolve");
        assert_eq!(got.len(), 1);
    }

    /// `7 = id` — swapped form also resolves.
    #[test]
    fn fast_path_eq_lit_swapped_resolves() {
        let e = Expr::BinaryOp {
            left: Box::new(num_expr("7")),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(ident("id"))),
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("swapped eq must resolve");
        assert_eq!(got.len(), 1);
    }

    /// `id IN (1, 2, 3)` — the keystone bench shape — resolves to 3 lits.
    #[test]
    fn fast_path_in_list_resolves() {
        let e = Expr::InList {
            expr: Box::new(Expr::Identifier(ident("id"))),
            list: vec![num_expr("1"), num_expr("2"), num_expr("3")],
            negated: false,
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("IN list must resolve");
        assert_eq!(got.len(), 3);
    }

    /// `id NOT IN (1, 2, 3)` — negated IN never fast-paths.
    #[test]
    fn fast_path_negated_in_list_returns_none() {
        let e = Expr::InList {
            expr: Box::new(Expr::Identifier(ident("id"))),
            list: vec![num_expr("1"), num_expr("2")],
            negated: true,
        };
        assert!(predicate_resolves_to_pk_list(&e, "id", "t").is_none());
    }

    /// `id IN ()` — empty list is still a fast-path (matches zero rows).
    #[test]
    fn fast_path_empty_in_list_resolves_to_empty_vec() {
        let e = Expr::InList {
            expr: Box::new(Expr::Identifier(ident("id"))),
            list: vec![],
            negated: false,
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("empty IN must resolve");
        assert!(got.is_empty());
    }

    /// `name = 'alice'` against PK column `id` — wrong column, no fast path.
    #[test]
    fn fast_path_eq_on_non_pk_column_returns_none() {
        let e = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("name"))),
            op: BinaryOperator::Eq,
            right: Box::new(str_expr("alice")),
        };
        assert!(predicate_resolves_to_pk_list(&e, "id", "t").is_none());
    }

    /// `id > 5` — range predicate, not pk-list shape.
    #[test]
    fn fast_path_range_predicate_returns_none() {
        let e = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("id"))),
            op: BinaryOperator::Gt,
            right: Box::new(num_expr("5")),
        };
        assert!(predicate_resolves_to_pk_list(&e, "id", "t").is_none());
    }

    /// `id = 1 AND name = 'a'` — composite predicate, no fast path.
    #[test]
    fn fast_path_composite_and_returns_none() {
        let e = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(ident("id"))),
                op: BinaryOperator::Eq,
                right: Box::new(num_expr("1")),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(ident("name"))),
                op: BinaryOperator::Eq,
                right: Box::new(str_expr("a")),
            }),
        };
        assert!(predicate_resolves_to_pk_list(&e, "id", "t").is_none());
    }

    /// `(id IN (1, 2))` — Nested wrapper must transparently unwrap.
    #[test]
    fn fast_path_nested_unwraps() {
        let inner = Expr::InList {
            expr: Box::new(Expr::Identifier(ident("id"))),
            list: vec![num_expr("1"), num_expr("2")],
            negated: false,
        };
        let e = Expr::Nested(Box::new(inner));
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("Nested must unwrap");
        assert_eq!(got.len(), 2);
    }

    /// `id = -5` — negative literal must still resolve.
    #[test]
    fn fast_path_negative_literal_resolves() {
        let e = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(neg_num_expr("5")),
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "t").expect("negative lit must resolve");
        assert_eq!(got.len(), 1);
    }

    /// `"users"."id" IN (1, 2)` — qualified-column form (ORM-shape) resolves.
    #[test]
    fn fast_path_qualified_in_list_resolves() {
        let e = Expr::InList {
            expr: Box::new(Expr::CompoundIdentifier(vec![ident("users"), ident("id")])),
            list: vec![num_expr("1"), num_expr("2")],
            negated: false,
        };
        let got = predicate_resolves_to_pk_list(&e, "id", "users")
            .expect("qualified IN must resolve");
        assert_eq!(got.len(), 2);
    }

    /// `id IN (1, name)` — non-literal list element falls through to slow path.
    #[test]
    fn fast_path_non_literal_in_list_returns_none() {
        let e = Expr::InList {
            expr: Box::new(Expr::Identifier(ident("id"))),
            list: vec![num_expr("1"), Expr::Identifier(ident("name"))],
            negated: false,
        };
        assert!(predicate_resolves_to_pk_list(&e, "id", "t").is_none());
    }

    // ── rmw function allowlist (`rmw_rhs_is_fast_path_eligible` fn arm) ──────

    /// Schema for the function-allowlist tests: a JSONB-ish `payload` (modelled
    /// as Utf8 here — the allowlist only checks names + arg eligibility, not
    /// types) plus a numeric `v`.
    fn fn_allowlist_schema() -> Schema {
        Schema::new(vec![
            Field::new("payload", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ])
    }

    /// `jsonb_set(payload, '{a,b}', '"x"')` — allowlisted, all args eligible.
    #[test]
    fn rmw_fn_jsonb_set_eligible() {
        let schema = fn_allowlist_schema();
        let e = parse_expr(r#"jsonb_set(payload, '{a,b}', '"x"')"#);
        assert!(rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// `coalesce(v, 0) + 1` — allowlisted fn nested under arithmetic.
    #[test]
    fn rmw_fn_coalesce_in_arithmetic_eligible() {
        let schema = fn_allowlist_schema();
        let e = parse_expr("coalesce(v, 0) + 1");
        assert!(rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// `jsonb_strip_nulls(payload)` and `jsonb_insert(...)` — allowlisted.
    #[test]
    fn rmw_fn_jsonb_strip_and_insert_eligible() {
        let schema = fn_allowlist_schema();
        assert!(rmw_rhs_is_fast_path_eligible(
            &parse_expr("jsonb_strip_nulls(payload)"),
            &schema
        ));
        assert!(rmw_rhs_is_fast_path_eligible(
            &parse_expr(r#"jsonb_insert(payload, '{a}', '1')"#),
            &schema
        ));
    }

    /// `upper(payload)` — deterministic but NOT on the allowlist → cold.
    #[test]
    fn rmw_fn_non_allowlisted_returns_false() {
        let schema = fn_allowlist_schema();
        let e = parse_expr("upper(payload)");
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// `now()` — volatile, must NOT be eligible.
    #[test]
    fn rmw_fn_now_returns_false() {
        let schema = fn_allowlist_schema();
        let e = parse_expr("now()");
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// `jsonb_set(payload, '{a}', now())` — an allowlisted fn with a
    /// NON-allowlisted (volatile) argument must be rejected: arg recursion
    /// closes the volatile-leak hole.
    #[test]
    fn rmw_fn_allowlisted_with_volatile_arg_returns_false() {
        let schema = fn_allowlist_schema();
        let e = parse_expr(r#"jsonb_set(payload, '{a}', now())"#);
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    // ── rmw CASE eligibility (`rmw_rhs_is_fast_path_eligible` Case arm) ─────

    /// Schema for the CASE tests: an Int64 PK-shaped `id` plus a value `v`.
    fn case_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])
    }

    /// Simple (operand-form) CASE keyed on a column with literal WHEN values
    /// and a bare-column ELSE — the exact shape `try_batched_do_update` emits
    /// (`v = CASE id WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE v END`). Row-local and
    /// deterministic, must be eligible (it is the structural equivalent of the
    /// already-admitted searched form `CASE WHEN id = 1 THEN 10 …`).
    #[test]
    fn rmw_case_simple_operand_form_eligible() {
        let schema = case_schema();
        let e = parse_expr("CASE id WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE v END");
        assert!(rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// Searched CASE remains eligible (regression guard for the searched arm).
    #[test]
    fn rmw_case_searched_form_eligible() {
        let schema = case_schema();
        let e = parse_expr("CASE WHEN id = 1 THEN 10 WHEN id > 2 THEN v + 1 ELSE v END");
        assert!(rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// Simple CASE with a volatile WHEN *value* (`CASE id WHEN now() …`) must
    /// stay cold — the operand-form admission recurses into each arm value.
    #[test]
    fn rmw_case_simple_with_volatile_when_value_returns_false() {
        let schema = case_schema();
        let e = parse_expr("CASE id WHEN now() THEN 10 ELSE v END");
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// Simple CASE with a volatile THEN result must stay cold.
    #[test]
    fn rmw_case_simple_with_volatile_then_result_returns_false() {
        let schema = case_schema();
        let e = parse_expr("CASE id WHEN 1 THEN now() ELSE v END");
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    /// Searched CASE still requires comparison-shaped conditions: a bare
    /// column as a WHEN condition (no operand to compare against) is not a
    /// comparison tree and must remain ineligible.
    #[test]
    fn rmw_case_searched_with_bare_column_condition_returns_false() {
        let schema = case_schema();
        let e = parse_expr("CASE WHEN id THEN 10 ELSE v END");
        assert!(!rmw_rhs_is_fast_path_eligible(&e, &schema));
    }

    // ── pk_scalar_to_row_key ─────────────────────────────────────────────────

    /// i64 PK → 8-byte big-endian bias-flipped encoding (matches cold sort).
    #[test]
    fn pk_scalar_to_row_key_int64() {
        use basin_storage::ScalarValue;
        let k = pk_scalar_to_row_key(&ScalarValue::Int64(7), &DataType::Int64)
            .expect("Int64 PK must encode");
        // The expected encoding is the same one `RowKey::builder().append_i64(7)`
        // produces.
        let want = basin_hottier::RowKey::builder().append_i64(7).finish();
        assert_eq!(k, want);
    }

    /// Int32 PK column receives an Int64 scalar (the literal coercion is
    /// widening) — must narrow back to Int32 encoding.
    #[test]
    fn pk_scalar_to_row_key_int32_narrows() {
        use basin_storage::ScalarValue;
        let k = pk_scalar_to_row_key(&ScalarValue::Int64(7), &DataType::Int32)
            .expect("Int32 PK must accept widened literal");
        let want = basin_hottier::RowKey::builder().append_i32(7).finish();
        assert_eq!(k, want);
    }

    /// Int32 PK with out-of-range literal must fail to encode (caller falls
    /// back to slow path, which surfaces a clean error).
    #[test]
    fn pk_scalar_to_row_key_int32_overflow_returns_none() {
        use basin_storage::ScalarValue;
        let overflowed: i64 = i64::from(i32::MAX) + 1;
        let k = pk_scalar_to_row_key(&ScalarValue::Int64(overflowed), &DataType::Int32);
        assert!(k.is_none(), "Int32 narrowing must reject overflowed literal");
    }

    /// Utf8 PK → null-terminated bytes (matches cold sort).
    #[test]
    fn pk_scalar_to_row_key_utf8() {
        use basin_storage::ScalarValue;
        let k = pk_scalar_to_row_key(&ScalarValue::Utf8("abc".into()), &DataType::Utf8)
            .expect("Utf8 PK must encode");
        let want = basin_hottier::RowKey::builder().append_str("abc").finish();
        assert_eq!(k, want);
    }

    /// Unsupported PK type (Float64) → `None` so caller routes to slow path.
    #[test]
    fn pk_scalar_to_row_key_unsupported_type_returns_none() {
        use basin_storage::ScalarValue;
        let k = pk_scalar_to_row_key(&ScalarValue::Float64(1.5), &DataType::Float64);
        assert!(k.is_none(), "Float64 PK must fall through to slow path");
    }

    // ── parse_assignments + parse_compound_predicate: ORM-compat shapes ──────
    //
    // These exercise the two table-stakes UPDATE forms that every ORM emits
    // (audit 2026-05-23): column-referencing expressions on the SET RHS and
    // `WHERE col IN (SELECT …)` on the WHERE side. Both are already wired
    // end-to-end (AssignmentRhs::Expr → generated_cols::eval_expression for
    // SET, resolve_subqueries_in_expr → Expr::InList for IN-subquery). These
    // unit tests pin the parser-side routing so a refactor can't silently
    // drop an UPDATE shape that real applications depend on.

    /// Parse the SET assignments from a full UPDATE statement, so the
    /// `Assignment` values we hand to `parse_assignments` are exactly the
    /// ones sqlparser produces in production.
    fn parse_set_assignments(sql: &str) -> Vec<Assignment> {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            sqlparser::ast::Statement::Update(sqlparser::ast::Update {
                assignments, ..
            }) => assignments,
            other => panic!("expected UPDATE, got {other:?}"),
        }
    }

    /// `SET views = views + 1` routes through the expression fallback
    /// (AssignmentRhs::Expr) rather than the literal-scalar fast path.
    /// Asserts the parser correctly identifies the RHS as "not a literal"
    /// and stashes its textual form for DataFusion to evaluate.
    #[test]
    fn parse_assignments_col_plus_literal_uses_expr_path() {
        let asgs = parse_set_assignments("UPDATE counters SET views = views + 1 WHERE id = 1");
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("views", DataType::Int64, false),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        assert_eq!(parsed.len(), 1, "one SET clause");
        let (idx, rhs) = &parsed[0];
        assert_eq!(*idx, 1, "views is column 1");
        match rhs {
            AssignmentRhs::Expr(text) => {
                assert!(
                    text.contains("views") && text.contains("+") && text.contains('1'),
                    "expected expression text to contain `views`, `+`, `1`; got {text:?}"
                );
            }
            other => panic!("expected AssignmentRhs::Expr, got {other:?}"),
        }
    }

    /// `SET balance = balance - 5` (the substituted-bind form of
    /// `balance - $1`) also routes through Expr — proves the fast-path
    /// gate doesn't accidentally swallow the `col - lit` shape that
    /// every "decrement an account" UPDATE emits.
    #[test]
    fn parse_assignments_col_minus_substituted_param_uses_expr_path() {
        // The prepared-statement layer substitutes $1 -> 5 before SQL reaches
        // dml_mutate, so the parser sees `balance - 5` (a BinaryOp with a
        // column on the left). This pins that shape to the Expr path.
        let asgs =
            parse_set_assignments("UPDATE accounts SET balance = balance - 5 WHERE id = 1");
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("balance", DataType::Int64, false),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        assert_eq!(parsed.len(), 1);
        let (idx, rhs) = &parsed[0];
        assert_eq!(*idx, 1, "balance is column 1");
        assert!(
            matches!(rhs, AssignmentRhs::Expr(t) if t.contains("balance") && t.contains("-")),
            "balance - lit must route to AssignmentRhs::Expr, got {rhs:?}"
        );
    }

    /// `SET updated_at = NOW()` is a function call (no column ref) and still
    /// belongs on the Expr path because timestamp literals aren't function
    /// calls. The Timestamp-column branch in `try_literal_to_scalar` only
    /// matches Number / RFC3339 string literals; everything else (including
    /// `NOW()`) falls through to Expr where DataFusion evaluates it.
    #[test]
    fn parse_assignments_now_function_uses_expr_path() {
        let asgs = parse_set_assignments(
            "UPDATE rows SET updated_at = NOW() WHERE id = 1",
        );
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        let (_, rhs) = &parsed[0];
        assert!(
            matches!(rhs, AssignmentRhs::Expr(t) if t.to_uppercase().contains("NOW")),
            "NOW() must route to AssignmentRhs::Expr, got {rhs:?}"
        );
    }

    /// `SET status = CASE WHEN amount > 100 THEN 'high' ELSE 'low' END` —
    /// the CASE expression is structurally not a literal so it routes to
    /// the Expr path where DataFusion evaluates it per-row.
    #[test]
    fn parse_assignments_case_when_uses_expr_path() {
        let asgs = parse_set_assignments(
            "UPDATE t SET status = CASE WHEN amount > 100 THEN 'high' ELSE 'low' END \
             WHERE id IN (1, 2)",
        );
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        let (idx, rhs) = &parsed[0];
        assert_eq!(*idx, 2, "status is column 2");
        assert!(
            matches!(rhs, AssignmentRhs::Expr(t) if t.to_uppercase().contains("CASE")),
            "CASE WHEN must route to AssignmentRhs::Expr, got {rhs:?}"
        );
    }

    /// `SET name = COALESCE(nickname, name)` — function call with two column
    /// references. Same routing target as the rest.
    #[test]
    fn parse_assignments_coalesce_uses_expr_path() {
        let asgs = parse_set_assignments(
            "UPDATE t SET name = COALESCE(nickname, name) WHERE id = 1",
        );
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("nickname", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        let (idx, rhs) = &parsed[0];
        assert_eq!(*idx, 2, "name is column 2");
        assert!(
            matches!(rhs, AssignmentRhs::Expr(t) if t.to_uppercase().contains("COALESCE")),
            "COALESCE(...) must route to AssignmentRhs::Expr, got {rhs:?}"
        );
    }

    /// Bare-literal RHS still hits the fast Scalar path — regression guard so
    /// the new Expr-routing tests above can't mask a fast-path regression.
    #[test]
    fn parse_assignments_bare_literal_uses_scalar_path() {
        let asgs = parse_set_assignments("UPDATE t SET status = 'low' WHERE id = 1");
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ]);
        let parsed = parse_assignments(&asgs, &schema).expect("must parse");
        let (_, rhs) = &parsed[0];
        assert!(
            matches!(rhs, AssignmentRhs::Scalar(ScalarValue::Utf8(s)) if s == "low"),
            "bare literal must route to AssignmentRhs::Scalar, got {rhs:?}"
        );
    }

    /// MAX_IN_SUBQUERY_ROWS is the documented user-facing limit. Pin its
    /// value so a refactor can't silently raise/lower it (the audit settled
    /// on 100k as the threshold past which `IN (lit, lit, …)` predicate
    /// rewriting becomes pathologically slow vs. the JOIN form). If the
    /// limit needs to change, update this test AND the docs in the same
    /// commit so users aren't surprised.
    #[test]
    fn max_in_subquery_rows_is_100k() {
        assert_eq!(
            MAX_IN_SUBQUERY_ROWS, 100_000,
            "subquery materialisation cap must stay at 100k; \
             update docs + JOIN guidance if you change this"
        );
    }

    // ── single_part_name: "public"."t" qualifier acceptance ─────────────────
    //
    // Prisma / TypeORM / Sequelize emit `UPDATE "public"."t" …` and
    // `DELETE FROM "public"."t" …` verbatim. PG's default search_path is
    // `"$user", public`, so an unqualified table name resolves to `public`
    // — stripping the `"public"."` prefix is an identity transform.
    // Non-public qualifiers are rejected with a clearer message naming
    // `search_path` and `public`, so the ORM author can diagnose the gap.

    fn obj_name_2(schema: &str, table: &str) -> ObjectName {
        ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(schema)),
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(table)),
        ])
    }

    fn obj_name_1(table: &str) -> ObjectName {
        ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            sqlparser::ast::Ident::new(table),
        )])
    }

    /// `single_part_name(t)` → `"t"` — bare name path is unchanged.
    #[test]
    fn single_part_name_bare_table() {
        let n = obj_name_1("User");
        assert_eq!(single_part_name(&n).unwrap(), "User");
    }

    /// `single_part_name("public"."t")` → `"t"` — the Prisma case.
    #[test]
    fn single_part_name_public_schema_prefix_stripped() {
        let n = obj_name_2("public", "User");
        assert_eq!(single_part_name(&n).unwrap(), "User");
    }

    /// ASCII-case-insensitive: `"PUBLIC"."User"` matches `"public"`.
    #[test]
    fn single_part_name_public_schema_case_insensitive() {
        let n = obj_name_2("PUBLIC", "User");
        assert_eq!(single_part_name(&n).unwrap(), "User");
    }

    /// A user-created (non-reserved) schema aliases to the bare table under
    /// the flat-namespace model — the same `myapp.t → t` resolution the
    /// CREATE / INSERT / SELECT paths apply, so drizzle's `myapp.events`
    /// UPDATE / DELETE resolve consistently. (Previously this rejected; the
    /// flat model now makes it an identity transform, matching how the rest
    /// of the engine resolves user-schema-qualified names.)
    #[test]
    fn single_part_name_user_schema_aliases_to_bare_table() {
        let n = obj_name_2("myapp", "events");
        assert_eq!(single_part_name(&n).unwrap(), "events");
    }

    /// A reserved schema (owned by the engine — `auth`, `storage`, …) is NOT
    /// a valid user-DML target and is rejected with a message pointing at
    /// `search_path` and naming `public`. The wording is load-bearing — ORM
    /// authors grep for "public" and "search_path" to diagnose the gap.
    #[test]
    fn single_part_name_reserved_schema_rejected() {
        let n = obj_name_2("auth", "users");
        let err = single_part_name(&n).expect_err("reserved schema must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("auth"),
            "error should name the offending schema, got: {msg}"
        );
        assert!(
            msg.contains("public"),
            "error should mention 'public' as the supported schema, got: {msg}"
        );
        assert!(
            msg.contains("search_path"),
            "error should reference search_path, got: {msg}"
        );
    }

    /// Three-part `db.schema.t` (PG fully-qualified form) is rejected —
    /// Basin doesn't model cross-database refs in DML.
    #[test]
    fn single_part_name_three_part_rejected() {
        let n = ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("db")),
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("public")),
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("t")),
        ]);
        let err = single_part_name(&n).expect_err("three-part must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("at most one schema qualifier"),
            "three-part error must say 'at most one schema qualifier', got: {msg}"
        );
    }

    // ── Parser-driven: UPDATE / DELETE with "public"."t" qualifier ──────────
    //
    // These exercise the full resolver path that the executor takes when
    // sqlparser hands back a real `Statement::Update` / `Statement::Delete`
    // built from the user's SQL. They guard against regressions where a
    // refactor wires the table-name extraction through a different helper
    // that bypasses `single_part_name`.

    fn parse_update_target(sql: &str) -> ObjectName {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse UPDATE");
        let Statement::Update(upd) = stmts.into_iter().next().unwrap() else {
            panic!("expected UPDATE");
        };
        match upd.table.relation {
            TableFactor::Table { name, .. } => name,
            other => panic!("expected TableFactor::Table, got {other:?}"),
        }
    }

    fn parse_delete_target(sql: &str) -> ObjectName {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse DELETE");
        let Statement::Delete(d) = stmts.into_iter().next().unwrap() else {
            panic!("expected DELETE");
        };
        let tables = match d.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
        };
        match tables.into_iter().next().unwrap().relation {
            TableFactor::Table { name, .. } => name,
            other => panic!("expected TableFactor::Table, got {other:?}"),
        }
    }

    /// `UPDATE "public"."t" SET col = 1 WHERE id = 1` — Prisma shape;
    /// the resolver strips `"public"."` and the executor sees bare `"t"`.
    #[test]
    fn update_with_public_schema_prefix() {
        let name = parse_update_target("UPDATE \"public\".\"t\" SET col = 1 WHERE id = 1");
        assert_eq!(single_part_name(&name).unwrap(), "t");
    }

    /// `DELETE FROM "public"."t" WHERE id = 1` — Prisma shape; resolver
    /// strips `"public"."` and the executor sees bare `"t"`.
    #[test]
    fn delete_with_public_schema_prefix() {
        let name = parse_delete_target("DELETE FROM \"public\".\"t\" WHERE id = 1");
        assert_eq!(single_part_name(&name).unwrap(), "t");
    }

    /// `UPDATE "public"."t" SET col = 1 WHERE "public"."t"."id" = 1` —
    /// three-part WHERE column ref combined with two-part table qualifier.
    /// Resolver strips the table prefix and the existing
    /// `as_identifier` path handles the 3-part column ref (per #71).
    #[test]
    fn update_with_qualified_col_under_schema_prefix() {
        let name = parse_update_target(
            "UPDATE \"public\".\"t\" SET col = 1 WHERE \"public\".\"t\".\"id\" = 1",
        );
        assert_eq!(single_part_name(&name).unwrap(), "t");
        // Confirm the WHERE col ref still resolves under "t" after table
        // qualifier stripping (this is the path the executor takes after
        // `let table_name = single_part_name(name)?` succeeds).
        let three_part = Expr::CompoundIdentifier(vec![
            sqlparser::ast::Ident::new("public"),
            sqlparser::ast::Ident::new("t"),
            sqlparser::ast::Ident::new("id"),
        ]);
        assert_eq!(as_identifier(&three_part, "t"), Some("id".to_string()));
    }

    /// `UPDATE "auth"."t" SET col = 1 WHERE id = 1` — a reserved schema is
    /// rejected with a clear message naming `public` and `search_path`. (A
    /// user schema like `myapp.t` aliases to the bare table under the flat
    /// model and is covered by `single_part_name_user_schema_aliases_to_bare_table`.)
    #[test]
    fn update_with_reserved_schema_refused() {
        let name = parse_update_target("UPDATE \"auth\".\"t\" SET col = 1 WHERE id = 1");
        let err = single_part_name(&name).expect_err("reserved schema must reject");
        let msg = err.to_string();
        assert!(msg.contains("auth"), "must name offending schema: {msg}");
        assert!(msg.contains("public"), "must mention 'public': {msg}");
        assert!(msg.contains("search_path"), "must mention search_path: {msg}");
    }

    /// `DELETE FROM "auth"."t" WHERE id = 1` — same as above.
    #[test]
    fn delete_with_reserved_schema_refused() {
        let name = parse_delete_target("DELETE FROM \"auth\".\"t\" WHERE id = 1");
        let err = single_part_name(&name).expect_err("reserved schema must reject");
        let msg = err.to_string();
        assert!(msg.contains("auth"), "must name offending schema: {msg}");
        assert!(msg.contains("public"), "must mention 'public': {msg}");
        assert!(msg.contains("search_path"), "must mention search_path: {msg}");
    }

    // ── narrow_materialize_files (row-targeted UPDATE rewrite) ────────────────
    //
    // These tests cover the cold-tier narrowing the `materialize_hot_overlay_into_cold`
    // path runs to avoid folding the whole live data-file set for a PK-targeted
    // UPDATE. The helper is a pure decision over (overlay keys, PK schema, live
    // files, table schema) — the call site in `materialize_hot_overlay_into_cold`
    // is mechanical.

    fn i64_pk_file_ref(path: &str, min: i64, max: i64) -> DataFileRef {
        let mut column_stats = std::collections::BTreeMap::new();
        column_stats.insert(
            "id".to_string(),
            basin_catalog::ColumnStats {
                null_count: Some(0),
                min_bytes: Some(min.to_le_bytes().to_vec()),
                max_bytes: Some(max.to_le_bytes().to_vec()),
                sum_bytes: None,
            },
        );
        DataFileRef {
            path: path.to_string(),
            size_bytes: 0,
            row_count: (max - min + 1).max(1) as u64,
            column_stats,
            bloom_filters: std::collections::BTreeMap::new(),
            hll_sketches: std::collections::BTreeMap::new(),
            tdigest_sketches: std::collections::BTreeMap::new(),
        }
    }

    fn int64_pk_schema() -> arrow_schema::Schema {
        arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ])
    }

    fn row_key_i64(v: i64) -> Vec<u8> {
        basin_hottier::RowKey::builder()
            .append_i64(v)
            .finish()
            .as_bytes()
            .to_vec()
    }

    /// One overlay row whose PK lives in exactly one of N live files → only
    /// that file is in the narrow rewrite set. This is the headline win: a
    /// 1000-file cold tier collapses to 1 file rewrite per single-row UPDATE.
    #[test]
    fn materialize_narrowing_single_overlay_row_rewrites_one_file() {
        let files = vec![
            i64_pk_file_ref("a.parquet", 0, 999),
            i64_pk_file_ref("b.parquet", 1000, 1999),
            i64_pk_file_ref("c.parquet", 2000, 2999),
        ];
        let key = row_key_i64(1500);
        let keys: Vec<&[u8]> = vec![key.as_slice()];
        let schema = int64_pk_schema();
        let out = narrow_materialize_files(&keys, "id", &DataType::Int64, &files, &schema)
            .expect("Int64 PK must produce a narrowed set");
        assert_eq!(out.len(), 1, "exactly one file should match: {out:?}");
        assert!(out.contains("b.parquet"), "expected b.parquet, got {out:?}");
    }

    /// Two overlay rows that land in different files → both files in the
    /// narrow set, others pruned.
    #[test]
    fn materialize_narrowing_overlay_rows_span_two_files() {
        let files = vec![
            i64_pk_file_ref("a.parquet", 0, 999),
            i64_pk_file_ref("b.parquet", 1000, 1999),
            i64_pk_file_ref("c.parquet", 2000, 2999),
        ];
        let k1 = row_key_i64(50);
        let k2 = row_key_i64(2500);
        let keys: Vec<&[u8]> = vec![k1.as_slice(), k2.as_slice()];
        let schema = int64_pk_schema();
        let out = narrow_materialize_files(&keys, "id", &DataType::Int64, &files, &schema)
            .expect("Int64 PK must produce a narrowed set");
        assert_eq!(out.len(), 2, "two files should match: {out:?}");
        assert!(out.contains("a.parquet"), "expected a.parquet in {out:?}");
        assert!(out.contains("c.parquet"), "expected c.parquet in {out:?}");
        assert!(!out.contains("b.parquet"), "b.parquet should be pruned in {out:?}");
    }

    /// An overlay row whose PK is outside the zone-map range of every live
    /// file (e.g. the row only exists in the hot overlay, never landed in
    /// cold) → narrow set is empty. The full materialize path still appends
    /// the override row via `apply_update_overlay_to_batches`, so correctness
    /// is preserved even without a cold file to rewrite. The narrow set being
    /// Some({}) (NOT None) is the load-bearing signal that we trust the probe.
    #[test]
    fn materialize_narrowing_overlay_row_not_in_any_file_falls_back_to_full() {
        let files = vec![
            i64_pk_file_ref("a.parquet", 0, 999),
            i64_pk_file_ref("b.parquet", 1000, 1999),
        ];
        let key = row_key_i64(9_999_999);
        let keys: Vec<&[u8]> = vec![key.as_slice()];
        let schema = int64_pk_schema();
        let out = narrow_materialize_files(&keys, "id", &DataType::Int64, &files, &schema)
            .expect("Int64 PK must produce a narrowed set, even when Absent");
        assert!(
            out.is_empty(),
            "Absent probe must yield zero-file rewrite (override is appended on the fresh file): {out:?}"
        );
    }

    /// Live files with NO column stats (no zone-map, no bloom) → the probe
    /// can't prune anything → narrow set is the full live set. This protects
    /// older-format tables that were written before the catalog started
    /// recording PK stats.
    #[test]
    fn materialize_narrowing_pk_index_missing_falls_back_to_full() {
        // Build files with empty column_stats so the zone-map prune path
        // can't fire — the probe is conservative and returns Candidates
        // containing every file.
        let mk = |path: &str| DataFileRef {
            path: path.to_string(),
            size_bytes: 0,
            row_count: 1000,
            column_stats: std::collections::BTreeMap::new(),
            bloom_filters: std::collections::BTreeMap::new(),
            hll_sketches: std::collections::BTreeMap::new(),
            tdigest_sketches: std::collections::BTreeMap::new(),
        };
        let files = vec![mk("a.parquet"), mk("b.parquet"), mk("c.parquet")];
        let key = row_key_i64(42);
        let keys: Vec<&[u8]> = vec![key.as_slice()];
        let schema = int64_pk_schema();
        let out = narrow_materialize_files(&keys, "id", &DataType::Int64, &files, &schema)
            .expect("missing stats fall back to keep-all, not None");
        assert_eq!(
            out.len(),
            3,
            "with no zone-map every file must remain a candidate: {out:?}"
        );
    }

    /// Composite-PK and other unsupported encodings: `materialize_hot_overlay_into_cold`
    /// itself early-returns for composite PKs (the overlay isn't written for
    /// them), but the narrowing helper still has to make the right call on
    /// non-supported PK types — fall back to None so the caller scans every
    /// file. Modelled here as a non-Int64 PK type that
    /// `pk_row_key_to_scalar` doesn't decode (Int16, Decimal, etc.).
    #[test]
    fn materialize_narrowing_composite_pk_works() {
        // Use a Decimal PK type — pk_row_key_to_scalar returns None for it,
        // so the narrowing helper must return None (fall back to full set).
        let schema = arrow_schema::Schema::new(vec![Field::new(
            "id",
            DataType::Decimal128(10, 0),
            false,
        )]);
        let files = vec![i64_pk_file_ref("a.parquet", 0, 999)];
        // The key bytes don't matter — decoding will reject the type before
        // the probe even runs.
        let key = vec![0u8; 8];
        let keys: Vec<&[u8]> = vec![key.as_slice()];
        let out = narrow_materialize_files(
            &keys,
            "id",
            &DataType::Decimal128(10, 0),
            &files,
            &schema,
        );
        assert!(
            out.is_none(),
            "unsupported PK type must fall back to full set (None): got {out:?}"
        );
    }

    /// UUID PK: `pk_row_key_to_scalar` returns None for UUID (FixedSizeBinary
    /// in current Arrow), so the helper must conservatively return None →
    /// full-set rewrite. This is the safety contract that protects UUID-keyed
    /// tables from silently dropping their cold counterpart during a
    /// hot-overlay materialize.
    #[test]
    fn materialize_narrowing_uuid_pk_works() {
        // UUID is stored as FixedSizeBinary(16) in Basin's catalog.
        let uuid_dt = DataType::FixedSizeBinary(16);
        let schema = arrow_schema::Schema::new(vec![Field::new("id", uuid_dt.clone(), false)]);
        let files = vec![i64_pk_file_ref("a.parquet", 0, 999)];
        let key = vec![0u8; 16];
        let keys: Vec<&[u8]> = vec![key.as_slice()];
        let out = narrow_materialize_files(&keys, "id", &uuid_dt, &files, &schema);
        assert!(
            out.is_none(),
            "UUID PK must fall back to full set (None): got {out:?}"
        );
    }

    // ── #204: PK-sort drives point-query row-group pruning ──────────────────
    //
    // ROOT CAUSE the fix addresses: the Parquet reader already prunes
    // row-groups whose `[min,max]` stats can't contain the `WHERE pk = $1`
    // literal. But un-sorted cold files have every row-group spanning the
    // whole key domain, so nothing prunes. Defaulting `cluster_columns` to the
    // single-column PK makes the whole-file-sorted write produce DISJOINT
    // per-row-group id ranges, so the existing prune isolates ~1 row-group.
    //
    // This test inserts rows in REVERSE PK order through the real INSERT write
    // path (`write_options_for`) into ONE cold Parquet file with a small
    // row-group cap, then runs a point query and asserts the reader pruned
    // row-groups by stats — which is only possible if the file came out
    // PK-sorted. Scale validation across 10k/100k/1M and the Vortex zone-map
    // equivalent is deferred to #206 (Vortex exposes no per-zone prune counter
    // today, so this Parquet counter is the unambiguous proof of the fix).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pk_sort_enables_point_query_row_group_prune() {
        use std::sync::Arc;

        use crate::{Engine, EngineConfig, ExecResult};
        use arrow_array::Int64Array;
        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::{ProjectId, TableName};
        use futures::StreamExt;
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            // Caches off so each read is a deterministic decode and the
            // row-group prune counter reflects the physical layout.
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        });

        let project = ProjectId::new();
        let table = TableName::new("pk_sort_t").unwrap();
        let sess = engine.open_session(project).await.unwrap();

        // Single-column integer PK — the prunable shape the policy targets.
        // Force Parquet: the catalog default is Vortex, whose per-zone prune
        // has no exposed counter today, so the row-group stats counter
        // (`row_groups_pruned_by_stats`) is only observable on Parquet. The
        // same PK-sort policy applies identically to Vortex zone-maps; its
        // scale validation is deferred to #206.
        sess.execute(
            "CREATE TABLE pk_sort_t (id BIGINT PRIMARY KEY, payload TEXT) \
             WITH (basin.file_format='parquet')",
        )
        .await
        .unwrap();

        // Force a small Parquet row-group cap so 4096 rows span multiple
        // groups in a single file. This is the only knob `WITH (...)` doesn't
        // expose at the SQL layer, so we set it on the catalog directly; it is
        // read fresh by `write_options_for` on the next INSERT (the set bumps
        // the catalog epoch, invalidating any cached metadata).
        const ROWS: i64 = 4096;
        const RG: usize = 512;
        engine
            .config()
            .catalog
            .set_row_group_rows(&project, &table, Some(RG))
            .await
            .unwrap();

        // Confirm the policy decision before exercising it: with no explicit
        // clustering and a single prunable PK, the default is exactly [id].
        let meta = engine
            .config()
            .catalog
            .load_table(&project, &table)
            .await
            .unwrap();
        assert_eq!(
            meta.default_cluster_cols(),
            vec!["id".to_string()],
            "single-column integer PK must auto-drive cluster_columns",
        );

        // Insert in REVERSE PK order in one statement → one cold file. If the
        // write path did NOT sort by the PK, the row-groups would each span
        // the full id domain and nothing could prune.
        let tuples: Vec<String> = (0..ROWS)
            .rev()
            .map(|i| format!("({i}, 'p-{i:06}')"))
            .collect();
        sess.execute(&format!(
            "INSERT INTO pk_sort_t VALUES {}",
            tuples.join(",")
        ))
        .await
        .unwrap();

        // (1) End-to-end correctness through the engine's point-query path.
        // NB: the engine takes a PK fastpath (OLTP / pk-row-cache) for
        // `WHERE pk = $1` that bypasses the Parquet row-group reader, so this
        // arm proves the answer is right but cannot observe the row-group
        // prune counter — that is asserted in (2) below.
        let mid = ROWS / 2;
        let res = sess
            .execute(&format!("SELECT id FROM pk_sort_t WHERE id = {mid}"))
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 1, "point query must return exactly one row");
                let col = batches[0]
                    .column_by_name("id")
                    .expect("id column")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id is Int64Array");
                assert_eq!(col.value(0), mid, "returned the wrong row");
            }
            other => panic!("expected Rows, got {other:?}"),
        }

        // (2) The win: read the cold file the INSERT wrote back through the
        // Parquet reader with the same `pk = mid` Eq predicate the planner
        // pushes down, and assert the reader's min/max row-group pruning
        // fired. This is the load-bearing assertion — it can ONLY pass if the
        // file came out PK-sorted (so per-row-group [min,max] ranges are
        // disjoint). The read path itself is unchanged by #204; sorting at
        // write is what makes the existing prune effective.
        let files = engine
            .config()
            .storage
            .list_data_files(&project, &table)
            .await
            .unwrap();
        assert_eq!(files.len(), 1, "expected exactly one cold file");
        let paths = vec![files[0].path.clone()];

        engine.config().storage.read_counters().reset();
        let opts = basin_storage::ReadOptions {
            filters: vec![basin_storage::Predicate::Eq(
                "id".to_string(),
                basin_storage::ScalarValue::Int64(mid),
            )],
            ..Default::default()
        };
        let mut stream = engine
            .config()
            .storage
            .read_paths_with_schema(&project, paths, opts, Some(meta.schema.clone()))
            .await
            .unwrap();
        // Drain (and re-check correctness on the physical read path too).
        let mut matched = 0usize;
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let col = b
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64Array");
            for v in col.iter().flatten() {
                if v == mid {
                    matched += 1;
                }
            }
        }
        assert_eq!(matched, 1, "physical read must surface the one matching row");

        // 4096 rows / 512-row groups = 8 row-groups; the point key lives in
        // one, so a PK-sorted file lets the reader prune the other 7 by stats.
        let c = engine.config().storage.read_counters().snapshot();
        assert!(
            c.row_groups_considered > 1,
            "expected a multi-row-group file; counters were {c:?}",
        );
        assert!(
            c.row_groups_pruned_by_stats > 0,
            "PK-sorted cold file must let the reader prune row-groups by stats; \
             counters were {c:?} (no prune ⇒ the file was not PK-sorted)",
        );
    }
}
