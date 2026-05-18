//! Point-query fast path.
//!
//! When the SQL is a "simple SELECT" — a flat projection from a single table
//! with at most one equality predicate against a column literal and an
//! optional LIMIT — we bypass DataFusion entirely and read directly through
//! `basin_storage::Storage` (or the shard's [`ProjectHandle`] when one is
//! configured). Skipping the DataFusion plan saves the per-query planning +
//! arrow-version conversion cost that dominates point-query latency on this
//! PoC.
//!
//! Anything outside the pattern (JOINs, GROUP BY, ORDER BY, expressions in
//! the projection, OR'd predicates, etc.) is left to the existing
//! DataFusion-based [`exec_select`](crate::executor::exec_select) path.
//!
//! The recogniser is conservative: when in doubt, return `None` and let the
//! caller fall through. The cost of a missed fast path is the planner work
//! we already pay today; the cost of an over-eager match is a wrong answer.
//!
//! [`ProjectHandle`]: basin_shard::ProjectHandle

use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_catalog::TableMetadata;
use basin_common::{BasinError, PartitionKey, Result, TableName};
use basin_storage::{
    evaluate_compound_for_pruning, CompoundPredicate, Predicate, PruneOutcome, ReadOptions,
    ScalarValue,
};
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectName, Query, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value,
};

use crate::{ExecResult, ProjectSession};

/// Recognised "simple SELECT" plan. When `predicates` is empty the read is
/// an unfiltered scan; when `limit` is `Some(n)` we truncate the merged
/// batches to `n` rows total.
#[derive(Debug)]
pub(crate) struct SimpleSelectPlan {
    pub table: TableName,
    /// `None` means project every column (`SELECT *`).
    pub projection: Option<Vec<String>>,
    /// Zero or more conjunctive predicates. Up to two atoms are accepted
    /// (single `col op lit` or BETWEEN / `col op lit AND col op lit`).
    pub predicates: Vec<Predicate>,
    pub limit: Option<usize>,
    /// `Some((column, ascending))` for a single-column ORDER BY recognised by
    /// the fast path. `ascending=true` means ASC (or no direction specified);
    /// `ascending=false` means DESC. When `Some`, `execute_simple_select` sorts
    /// all decoded rows by this column and applies the limit post-sort.
    pub order_by: Option<(String, bool)>,
}

/// Recognise the supported "simple SELECT" shape. Returns `None` if any
/// clause we don't handle is present, in which case the caller should fall
/// back to the DataFusion path.
pub(crate) fn match_simple_select(stmt: &Statement) -> Option<SimpleSelectPlan> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };
    match_query(query.as_ref())
}

fn match_query(q: &Query) -> Option<SimpleSelectPlan> {
    if q.with.is_some()
        || !q.ext_limit_by().is_empty()
        || q.ext_offset().is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return None;
    }

    // Parse an optional single-column ORDER BY. Any ORDER BY that is more
    // complex (multiple columns, expressions, NULLS FIRST/LAST, WITH FILL)
    // falls through to DataFusion. `None` here means "no ORDER BY at all".
    let order_by: Option<(String, bool)> = match q.order_by.as_ref() {
        None => None,
        Some(ob) => {
            let exprs = ob.ext_exprs();
            if exprs.len() != 1 {
                return None;
            }
            let e = &exprs[0];
            // Reject ClickHouse WITH FILL and NULLS FIRST/LAST — leave those
            // to DataFusion so NULL ordering matches its semantics exactly.
            if e.with_fill.is_some() || e.options.nulls_first.is_some() {
                return None;
            }
            let col = match &e.expr {
                Expr::Identifier(id) => id.value.clone(),
                _ => return None,
            };
            let ascending = e.options.asc.unwrap_or(true); // default ASC
            Some((col, ascending))
        }
    };

    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !select.connect_by.is_empty()
    {
        return None;
    }

    // GROUP BY: accept only the empty-expression form. `GROUP BY ALL` and
    // any explicit grouping expressions take us off the fast path.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return None,
    }

    // FROM clause: exactly one bare table, no joins.
    if select.from.len() != 1 {
        return None;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return None;
    }
    let table = match &from.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            ..
        } => {
            if alias.is_some()
                || args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
            {
                return None;
            }
            single_part_table(name)?
        }
        _ => return None,
    };

    // Projection: either `*` (unqualified, no ILIKE/EXCLUDE/etc.) or a list
    // of bare column identifiers. Compound names (`t.col`), expressions, and
    // aliases are left to DataFusion so we don't have to teach the fast path
    // about them.
    let projection = parse_projection(&select.projection)?;

    // WHERE clause: zero to two conjunctive predicates. Each atom is one of
    // `<col> <op> <literal>` where `<op>` is `=`, `>`, `<`, `>=`, `<=`;
    // `<col> BETWEEN <lo> AND <hi>` is also accepted (expands to two atoms).
    // A bare `AND` of exactly two such atoms is accepted too (covers the
    // compound-filter benchmark shape `BETWEEN … AND b = true`). Anything
    // richer — OR, IS NULL, expressions, nested ANDs — falls through to
    // DataFusion.
    let predicates: Vec<Predicate> = match &select.selection {
        None => vec![],
        Some(expr) => parse_predicates(expr)?,
    };

    // LIMIT: literal non-negative integer. `LIMIT ALL`, expressions, and
    // placeholders fall through to DataFusion.
    let limit = match q.ext_limit() {
        None => None,
        Some(Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        })) => match s.parse::<i64>() {
            Ok(n) if n >= 0 => Some(n as usize),
            _ => return None,
        },
        Some(_) => return None,
    };

    // ORDER BY without LIMIT means "sort ALL rows" — let DataFusion handle
    // that; it can parallelise and pipeline the sort better. Only handle
    // ORDER BY when paired with a LIMIT.
    if order_by.is_some() && limit.is_none() {
        return None;
    }

    Some(SimpleSelectPlan {
        table,
        projection,
        predicates,
        limit,
        order_by,
    })
}

fn single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].id_val().clone()).ok()
}

fn parse_projection(items: &[SelectItem]) -> Option<Option<Vec<String>>> {
    if items.len() == 1 {
        if let SelectItem::Wildcard(opts) = &items[0] {
            // The wildcard helper carries options like ILIKE / EXCLUDE; we
            // only handle the bare `*` case here.
            if opts.opt_ilike.is_none()
                && opts.opt_exclude.is_none()
                && opts.opt_except.is_none()
                && opts.opt_replace.is_none()
                && opts.opt_rename.is_none()
            {
                return Some(None);
            }
            return None;
        }
    }
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => cols.push(ident.value.clone()),
            _ => return None,
        }
    }
    Some(Some(cols))
}

/// Parse the WHERE expression into zero, one, or two conjunctive
/// `Predicate`s. Returns `None` to fall back to DataFusion on anything
/// we cannot represent cleanly:
///
/// * A single `<col> <op> <literal>` → one `Predicate`
/// * `<col> BETWEEN <lo> AND <hi>` → two `Predicate`s (`Gt(col, lo-1)` +
///   `Lt(col, hi+1)`) — only for `Int64` literals
/// * `<left> AND <right>` where both sides are simple atoms → two
///   `Predicate`s (enables compound shapes like `BETWEEN … AND b = true`)
///
/// More than two atoms, OR, IS NULL, sub-queries, etc. all return `None`.
fn parse_predicates(expr: &Expr) -> Option<Vec<Predicate>> {
    // `col BETWEEN lo AND hi` — sqlparser represents this as `Between { expr, low, high }`.
    // It expands to `col > lo-1 AND col < hi+1` for Int64, which our
    // Predicate type can represent without a new variant.
    if let Expr::Between {
        expr: col_expr,
        negated: false,
        low,
        high,
    } = expr
    {
        let col = as_identifier(col_expr)?;
        // Only integer BETWEEN for now — float/string BETWEEN stays in DataFusion.
        let lo = match literal_value(low)? {
            ScalarValue::Int64(v) => v.checked_sub(1)?,
            _ => return None,
        };
        let hi = match literal_value(high)? {
            ScalarValue::Int64(v) => v.checked_add(1)?,
            _ => return None,
        };
        return Some(vec![
            Predicate::Gt(col.clone(), ScalarValue::Int64(lo)),
            Predicate::Lt(col, ScalarValue::Int64(hi)),
        ]);
    }

    // `<left> AND <right>` — accept when both sides are parseable as a single
    // atom OR as a BETWEEN (which expands to two atoms). Cap at two atoms total
    // so we don't fan out too much complexity.
    if let Expr::BinaryOp {
        op: BinaryOperator::And,
        left,
        right,
    } = expr
    {
        let left_preds = parse_predicates(left)?;
        let right_preds = parse_predicates(right)?;
        // Reject if the combined total exceeds three atoms — keeps the logic
        // conservative and avoids building huge conjunctive filters.
        if left_preds.len() + right_preds.len() > 3 {
            return None;
        }
        let mut out = left_preds;
        out.extend(right_preds);
        return Some(out);
    }

    // Single atom.
    parse_predicate(expr).map(|p| vec![p])
}

/// Parse a single `<col> <op> <literal>` predicate where `<op>` is one of
/// `=`, `>`, `<`, `>=`, `<=` (and their mirror forms). Anything else returns
/// `None` and falls back to the DataFusion path.
///
/// `>=` and `<=` are encoded as the open-ended variants `Gt`/`Lt` of the
/// adjacent integer: `col >= v` → `Predicate::Gt(col, v-1)` and
/// `col <= v` → `Predicate::Lt(col, v+1)`. This transformation is only
/// applied to `Int64` literals where the predecessor/successor is
/// representable; all other types return `None` so the fast path does not
/// silently widen the result set.
fn parse_predicate(expr: &Expr) -> Option<Predicate> {
    let (op, left, right) = match expr {
        Expr::BinaryOp { op, left, right } => (op, left.as_ref(), right.as_ref()),
        _ => return None,
    };

    // (col, literal, flipped): flipped=true means the user wrote `lit op col`
    // so we have to invert the comparison direction.
    let (col, lit, flipped) = if let (Some(c), Some(v)) = (as_identifier(left), literal_value(right)) {
        (c, v, false)
    } else if let (Some(c), Some(v)) = (as_identifier(right), literal_value(left)) {
        (c, v, true)
    } else {
        return None;
    };

    // Map the SQL operator (accounting for col/literal order flip) to a
    // `Predicate` variant. `>=` / `<=` use the predecessor/successor trick for
    // Int64 only — other scalar types remain unsupported to avoid silent
    // widening of float or string comparisons.
    let pred = match (op, flipped) {
        (BinaryOperator::Eq, _) => Predicate::Eq(col, lit),

        // col > lit  /  lit < col
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => Predicate::Gt(col, lit),

        // col < lit  /  lit > col
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => Predicate::Lt(col, lit),

        // col >= lit → Gt(col, lit-1), only for Int64
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => match lit {
            ScalarValue::Int64(v) => {
                let prev = v.checked_sub(1)?; // don't fast-path i64::MIN
                Predicate::Gt(col, ScalarValue::Int64(prev))
            }
            _ => return None,
        },

        // col <= lit → Lt(col, lit+1), only for Int64
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => match lit {
            ScalarValue::Int64(v) => {
                let next = v.checked_add(1)?; // don't fast-path i64::MAX
                Predicate::Lt(col, ScalarValue::Int64(next))
            }
            _ => return None,
        },

        _ => return None,
    };
    Some(pred)
}

fn as_identifier(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        _ => None,
    }
}

/// Recognise the literal forms we can push down: signed integers, strings,
/// booleans. Anything richer (NULL, casts, vectors, floats) drops us out of
/// the fast path.
fn literal_value(e: &Expr) -> Option<ScalarValue> {
    let (negate, inner) = match e {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => (true, expr.as_ref()),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => (false, expr.as_ref()),
        other => (false, other),
    };
    match inner {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => {
            let parsed: i64 = s.parse().ok()?;
            Some(ScalarValue::Int64(if negate { -parsed } else { parsed }))
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
            if negate {
                None
            } else {
                Some(ScalarValue::Utf8(s.clone()))
            }
        }
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => {
            if negate {
                None
            } else {
                Some(ScalarValue::Boolean(*b))
            }
        }
        _ => None,
    }
}

/// Threshold above which we move the actual scan onto the blocking thread
/// pool (Change C). The numbers are deliberately conservative: a SELECT that
/// has to read more than 100 K rows OR 50 MiB of Parquet is heavy enough
/// that pinning a cooperative tokio worker for the duration of the decode
/// loop materially hurts other projects on the same runtime. Point queries
/// (which carry a predicate) always stay on the cooperative pool — they
/// only touch one row group thanks to predicate pushdown, and the
/// `spawn_blocking` round-trip would dwarf the actual work.
const HEAVY_ROW_THRESHOLD: u64 = 100_000;
const HEAVY_BYTE_THRESHOLD: u64 = 50 * 1024 * 1024;

/// Run a recognised plan against the engine's storage layer (or, when wired
/// up, the shard's [`ProjectHandle`]). Returns the merged result set ready to
/// hand back to the caller.
///
/// `prefetched_meta` may carry `TableMetadata` already loaded by the caller's
/// fast-path gate check (which loaded it to inspect `rls_enabled` and the
/// soft-delete column). When supplied we skip the redundant `load_table` call;
/// when `None` we fall back to loading it ourselves (e.g. callers that don't
/// pre-check).
///
/// [`ProjectHandle`]: basin_shard::ProjectHandle
pub(crate) async fn execute_simple_select(
    sess: &ProjectSession,
    plan: SimpleSelectPlan,
    prefetched_meta: Option<TableMetadata>,
) -> Result<ExecResult> {
    // Flush the in-RAM tail before we look up the table's metadata so the
    // post-flush snapshot drives the heavy-read gating below. Without this
    // the catalog reports zero data files (everything still in WAL) and we'd
    // misclassify a 5M-row scan as a small one.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    // Use the pre-fetched metadata when available (saves one catalog round-trip
    // that the fast-path gate already paid). Fall back to loading when not.
    let meta = match prefetched_meta {
        Some(m) => m,
        None => {
            sess.engine
                .config()
                .catalog
                .load_table(&sess.project, &plan.table)
                .await?
        }
    };

    // Validate the projection against the cached schema. Doing it here means
    // unknown columns surface as a clean `NotFound` instead of an opaque
    // storage error, and lets us build a projected schema without hitting
    // Parquet at all.
    let proj_indices: Option<Vec<usize>> = match &plan.projection {
        Some(cols) => {
            let mut idxs = Vec::with_capacity(cols.len());
            for c in cols {
                let i = meta
                    .schema
                    .index_of(c)
                    .map_err(|_| BasinError::InvalidSchema(format!("unknown column {c}")))?;
                idxs.push(i);
            }
            Some(idxs)
        }
        None => None,
    };

    let opts = ReadOptions {
        projection: plan.projection.clone(),
        filters: plan.predicates.clone(),
        partition: None,
    };

    // Gate Change C off the catalog's reported snapshot size. We treat a read
    // as "heavy" only when there are no predicates (i.e. we're scanning rather
    // than point-looking up) and the table is large enough that the decode
    // loop will dominate. With predicates, row-group pruning keeps the
    // actual decode small no matter how big the table is, so the
    // `spawn_blocking` round-trip would be pure overhead.
    let heavy = plan.predicates.is_empty()
        && meta
            .current()
            .map(|s| {
                let rows: u64 = s.data_files.iter().map(|f| f.row_count).sum();
                let bytes: u64 = s.data_files.iter().map(|f| f.size_bytes).sum();
                rows >= HEAVY_ROW_THRESHOLD || bytes >= HEAVY_BYTE_THRESHOLD
            })
            .unwrap_or(false);

    // Build the catalog-driven live file list. `live_data_files()` replays the
    // snapshot chain up to `current_snapshot`, so after a rollback it returns
    // only the pre-rollback files — physical files from post-rollback snapshots
    // are never included (bug #41 fix). Convert each path string to an
    // `ObjectPath` for `storage.read_paths`.
    // Catalog-stats file pruning: skip any data file whose per-file
    // column_stats (min/max/null-count, populated at write time for BOTH
    // Parquet and Vortex) prove the predicate cannot match a single row
    // (`PruneOutcome::NoMatch`). Done BEFORE `read_paths` so a pruned file
    // is never opened — decisive for Vortex, whose per-file open is far
    // heavier than a Parquet footer read, and a win for Parquet too
    // (point/range/compound queries touch one file instead of all).
    let live_files = meta.live_data_files();
    let live_paths: Vec<object_store::path::Path> = if plan.predicates.is_empty() {
        live_files
            .into_iter()
            .map(|f| object_store::path::Path::from(f.path.as_str()))
            .collect()
    } else {
        // Build a compound AND predicate for catalog-level file pruning.
        let cp = if plan.predicates.len() == 1 {
            CompoundPredicate::Atom(plan.predicates[0].clone())
        } else {
            CompoundPredicate::And(
                plan.predicates
                    .iter()
                    .map(|p| CompoundPredicate::Atom(p.clone()))
                    .collect(),
            )
        };
        let schema = meta.schema.as_ref();
        live_files
            .into_iter()
            .filter(|f| {
                !matches!(
                    evaluate_compound_for_pruning(&cp, &f.column_stats, schema, f.row_count),
                    PruneOutcome::NoMatch
                )
            })
            .map(|f| object_store::path::Path::from(f.path.as_str()))
            .collect()
    };

    let batches = if let Some(shard) = sess.engine.config().shard.as_ref() {
        // Shard path: this read merges the in-RAM tail with the Parquet base.
        // The pre-flush above already drained the tail into Parquet so this
        // read is bounded, then `handle.read` only has to scan whatever new
        // tail rows have arrived since.
        let handle = shard
            .get(&sess.project, &PartitionKey::default_key())
            .await?;
        if heavy {
            let table = plan.table.clone();
            run_blocking(move || async move { handle.read(&table, opts).await }).await?
        } else {
            handle.read(&plan.table, opts).await?
        }
    } else if live_paths.is_empty() {
        // No live files at this snapshot — return an empty batch set rather
        // than listing the directory (which would see rolled-back files).
        vec![]
    } else if heavy {
        let storage = sess.engine.config().storage.clone();
        let project = sess.project;
        let schema = Some(meta.schema.clone());
        run_blocking(move || async move {
            use futures::StreamExt;
            let stream = storage
                .read_paths_with_schema(&project, live_paths, opts, schema)
                .await?;
            let collected: Vec<Result<RecordBatch>> = stream.collect().await;
            collected.into_iter().collect::<Result<Vec<_>>>()
        })
        .await?
    } else {
        use futures::StreamExt;
        let schema = Some(meta.schema.clone());
        let stream = sess
            .engine
            .config()
            .storage
            .read_paths_with_schema(&sess.project, live_paths, opts, schema)
            .await?;
        let collected: Vec<Result<RecordBatch>> = stream.collect().await;
        collected.into_iter().collect::<Result<Vec<_>>>()?
    };

    let projected_schema: Arc<Schema> = match &proj_indices {
        Some(idxs) => Arc::new(
            meta.schema
                .project(idxs)
                .map_err(|e| BasinError::internal(format!("project schema: {e}")))?,
        ),
        None => meta.schema.clone(),
    };

    // ORDER BY + LIMIT: merge batches into one, sort by the column, take
    // the first `limit` rows. We only reach here when `order_by.is_some()`
    // implies `limit.is_some()` (enforced in match_query).
    let trimmed = if let Some((ref col, ascending)) = plan.order_by {
        let limit = plan.limit.expect("order_by implies limit");
        apply_order_by_limit(batches, col, ascending, limit, &projected_schema)?
    } else {
        match plan.limit {
            Some(limit) => apply_limit(batches, limit),
            None => batches,
        }
    };

    Ok(ExecResult::Rows {
        schema: projected_schema,
        batches: trimmed,
    })
}

/// Run an async closure on the blocking thread pool, driving it through a
/// tiny `current_thread` runtime. The point of this dance is to keep the
/// heavy parquet-decode loop off the cooperative tokio worker pool: a
/// `spawn_blocking` task gets its own OS thread, so it can monopolise CPU
/// without blocking other tasks on the runtime that scheduled it.
///
/// We accept `FnOnce` returning a future rather than just an async block so
/// callers can build the future inside the spawned thread (i.e. with values
/// that are `Send + 'static` once moved in).
async fn run_blocking<F, Fut, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T>>,
    T: Send + 'static,
{
    let join = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| BasinError::internal(format!("blocking runtime: {e}")))?;
        rt.block_on(f())
    })
    .await
    .map_err(|e| BasinError::internal(format!("spawn_blocking join: {e}")))?;
    join
}

/// Sort batches by a single column and return the first `limit` rows as one
/// or more `RecordBatch`es. Uses Arrow's `sort_to_indices` + `take` so the
/// sort key is only materialised once. NULLs sort last in ASC order (matching
/// DataFusion's default NULL handling for `ORDER BY col ASC`).
///
/// Returns an error if the sort column is absent from the projected schema.
fn apply_order_by_limit(
    batches: Vec<RecordBatch>,
    col: &str,
    ascending: bool,
    limit: usize,
    schema: &Arc<Schema>,
) -> Result<Vec<RecordBatch>> {
    use arrow::compute::{SortOptions, sort_to_indices, take};
    use arrow_select::concat;

    if batches.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    // Concatenate all batches into one so the sort sees global row order.
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let merged = concat::concat_batches(schema, refs)
        .map_err(|e| BasinError::internal(format!("order_by concat: {e}")))?;

    let col_idx = merged.schema().index_of(col).map_err(|_| {
        BasinError::InvalidSchema(format!("ORDER BY column '{col}' not in result schema"))
    })?;
    let sort_col = merged.column(col_idx);

    // nulls_first=false: NULLs sort LAST (DataFusion default for ASC).
    // For DESC, NULLs sort FIRST (DataFusion default), so nulls_first=true.
    let opts = SortOptions {
        descending: !ascending,
        nulls_first: !ascending,
    };
    let indices = sort_to_indices(sort_col, Some(opts), Some(limit))
        .map_err(|e| BasinError::internal(format!("order_by sort_to_indices: {e}")))?;

    // Reorder every column by the sort indices.
    let columns: Vec<arrow_array::ArrayRef> = (0..merged.num_columns())
        .map(|i| {
            take(merged.column(i).as_ref(), &indices, None)
                .map_err(|e| BasinError::internal(format!("order_by take col {i}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;

    let result = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("order_by result batch: {e}")))?;

    Ok(vec![result])
}

/// Truncate the merged batches to at most `limit` rows total. Empty batches
/// are dropped so the caller doesn't see zero-row trailers.
fn apply_limit(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    if limit == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(batches.len());
    let mut remaining = limit;
    for b in batches {
        if remaining == 0 {
            break;
        }
        if b.num_rows() <= remaining {
            remaining -= b.num_rows();
            out.push(b);
        } else {
            // Slice keeps the underlying buffers; no copy.
            out.push(b.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut s = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        s.pop().unwrap()
    }

    #[test]
    fn matches_select_with_eq_int_literal() {
        let stmt = parse_one("SELECT id, name FROM users WHERE id = 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.table.as_str(), "users");
        assert_eq!(
            plan.projection.as_ref().map(|v| v.as_slice()),
            Some(["id".to_string(), "name".to_string()].as_slice()),
        );
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Eq(col, ScalarValue::Int64(v)) => {
                assert_eq!(col, "id");
                assert_eq!(*v, 5);
            }
            other => panic!("unexpected predicate: {other:?}"),
        }
        assert!(plan.limit.is_none());
    }

    #[test]
    fn matches_select_star_with_string_literal() {
        let stmt = parse_one("SELECT * FROM events WHERE name = 'alice' LIMIT 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert!(
            plan.projection.is_none(),
            "wildcard should mean no projection"
        );
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Eq(col, ScalarValue::Utf8(v)) => {
                assert_eq!(col, "name");
                assert_eq!(v, "alice");
            }
            other => panic!("unexpected predicate: {other:?}"),
        }
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn matches_select_without_where() {
        let stmt = parse_one("SELECT id FROM t");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert!(plan.predicates.is_empty());
    }

    #[test]
    fn matches_gt_predicate() {
        let stmt = parse_one("SELECT id FROM t WHERE k > 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(10)) => assert_eq!(col, "k"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_lt_predicate() {
        let stmt = parse_one("SELECT id FROM t WHERE k < 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Lt(col, ScalarValue::Int64(5)) => assert_eq!(col, "k"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_gteq_predicate_as_gt_predecessor() {
        let stmt = parse_one("SELECT * FROM t WHERE id >= 6000");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        // >=6000 is encoded as >5999 so existing Predicate::Gt handles it.
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(5999)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_lteq_predicate_as_lt_successor() {
        let stmt = parse_one("SELECT * FROM t WHERE id <= 100");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        // <=100 is encoded as <101.
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Lt(col, ScalarValue::Int64(101)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_order_by_limit_with_inequality() {
        let stmt =
            parse_one("SELECT * FROM t WHERE id >= 6000 ORDER BY id DESC LIMIT 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.order_by, Some(("id".to_string(), false)));
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(5999)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_between_as_two_predicates() {
        let stmt = parse_one("SELECT * FROM t WHERE id BETWEEN 100 AND 200");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 2);
        // BETWEEN 100 AND 200 → Gt(id, 99) AND Lt(id, 201)
        match (&plan.predicates[0], &plan.predicates[1]) {
            (
                Predicate::Gt(c1, ScalarValue::Int64(99)),
                Predicate::Lt(c2, ScalarValue::Int64(201)),
            ) => {
                assert_eq!(c1, "id");
                assert_eq!(c2, "id");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_and_of_two_atoms() {
        let stmt = parse_one("SELECT * FROM t WHERE id = 1 AND k > 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 2);
    }

    #[test]
    fn rejects_four_atom_conjunction() {
        // Four atoms (BETWEEN = 2 + two more) exceeds the cap → DataFusion.
        let stmt =
            parse_one("SELECT * FROM t WHERE id BETWEEN 1 AND 10 AND k > 2 AND s = 'x'");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_join() {
        let stmt = parse_one("SELECT a.id FROM a JOIN b ON a.id = b.id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_order_by() {
        let stmt = parse_one("SELECT id FROM t WHERE id = 1 ORDER BY id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_group_by() {
        let stmt = parse_one("SELECT id FROM t GROUP BY id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_or_predicate() {
        // OR is not supported — always falls through to DataFusion.
        let stmt = parse_one("SELECT id FROM t WHERE id = 1 OR id = 2");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_aliased_table() {
        let stmt = parse_one("SELECT t.id FROM t AS t WHERE t.id = 1");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_expression_in_projection() {
        let stmt = parse_one("SELECT id + 1 FROM t");
        assert!(match_simple_select(&stmt).is_none());
    }

    /// Timing micro-bench: measures median latency of the fast-path SELECT
    /// (catalog lookup + Parquet read) over 200 iterations on a 10k-row table.
    /// Run with:
    ///   `CARGO_BUILD_JOBS=1 cargo test -p basin-engine --lib \
    ///    fast_select::tests::bench_fast_select_latency -- --nocapture --ignored`
    #[tokio::test]
    #[ignore]
    async fn bench_fast_select_latency() {
        use std::sync::Arc;
        use std::time::Instant;

        use crate::{Engine, EngineConfig};
        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::ProjectId;
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
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
        let sess = engine.open_session(project).await.unwrap();

        // Create a table and insert 10k rows.
        sess.execute("CREATE TABLE audit_log (id BIGINT, user_id BIGINT, action TEXT, ts BIGINT)")
            .await
            .unwrap();
        let mut tuples = Vec::with_capacity(10_000);
        for i in 0i64..10_000 {
            tuples.push(format!("({i}, {}, 'login', {})", i % 100, i * 1000));
        }
        sess.execute(&format!(
            "INSERT INTO audit_log VALUES {}",
            tuples.join(",")
        ))
        .await
        .unwrap();

        const ITERS: usize = 200;
        let mut samples: Vec<f64> = Vec::with_capacity(ITERS);

        // Warm up.
        for _ in 0..10 {
            let _ = sess
                .execute("SELECT id, user_id FROM audit_log WHERE id = 42")
                .await
                .unwrap();
        }

        for i in 0..ITERS {
            let t0 = Instant::now();
            let _ = sess
                .execute(&format!(
                    "SELECT id, user_id FROM audit_log WHERE id = {}",
                    i as i64 % 10_000
                ))
                .await
                .unwrap();
            samples.push(t0.elapsed().as_secs_f64() * 1000.0);
        }

        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = samples[ITERS / 2];
        let p95 = samples[(ITERS * 95) / 100];
        let p99 = samples[(ITERS * 99) / 100];
        let mean = samples.iter().sum::<f64>() / ITERS as f64;
        println!(
            "\nbench_fast_select_latency: mean={mean:.3}ms  p50={p50:.3}ms  p95={p95:.3}ms  p99={p99:.3}ms"
        );
    }
}
