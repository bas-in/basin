//! Point-query fast path.
//!
//! When the SQL is a "simple SELECT" — a flat projection from a single table
//! with at most one equality predicate against a column literal and an
//! optional LIMIT — we bypass DataFusion entirely and read directly through
//! `basin_storage::Storage` (or the shard's [`TenantHandle`] when one is
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
//! [`TenantHandle`]: basin_shard::TenantHandle

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_common::{BasinError, PartitionKey, Result, TableName};
use basin_storage::{Predicate, ReadOptions, ScalarValue};
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectName, Query, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value,
};

use crate::{ExecResult, TenantSession};

/// Recognised "simple SELECT" plan. When `predicate` is `None` the read is
/// an unfiltered scan; when `limit` is `Some(n)` we truncate the merged
/// batches to `n` rows total.
#[derive(Debug)]
pub(crate) struct SimpleSelectPlan {
    pub table: TableName,
    /// `None` means project every column (`SELECT *`).
    pub projection: Option<Vec<String>>,
    pub predicate: Option<Predicate>,
    pub limit: Option<usize>,
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
        || q.order_by.is_some()
        || !q.limit_by.is_empty()
        || q.offset.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return None;
    }

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
        || select.connect_by.is_some()
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

    // WHERE clause: an optional `<col> = <literal>` predicate.
    let predicate = match &select.selection {
        Some(expr) => Some(parse_eq_literal(expr)?),
        None => None,
    };

    // LIMIT: literal non-negative integer. `LIMIT ALL`, expressions, and
    // placeholders fall through to DataFusion.
    let limit = match &q.limit {
        None => None,
        Some(Expr::Value(Value::Number(s, _))) => match s.parse::<i64>() {
            Ok(n) if n >= 0 => Some(n as usize),
            _ => return None,
        },
        Some(_) => return None,
    };

    Some(SimpleSelectPlan {
        table,
        projection,
        predicate,
        limit,
    })
}

fn single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].value.clone()).ok()
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

/// Parse `<col> = <literal>` (or its mirror, `<literal> = <col>`).
fn parse_eq_literal(expr: &Expr) -> Option<Predicate> {
    let (left, right) = match expr {
        Expr::BinaryOp { op: BinaryOperator::Eq, left, right } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };

    if let (Some(col), Some(lit)) = (as_identifier(left), literal_value(right)) {
        return Some(Predicate::Eq(col, lit));
    }
    if let (Some(col), Some(lit)) = (as_identifier(right), literal_value(left)) {
        return Some(Predicate::Eq(col, lit));
    }
    None
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
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => (true, expr.as_ref()),
        Expr::UnaryOp { op: UnaryOperator::Plus, expr } => (false, expr.as_ref()),
        other => (false, other),
    };
    match inner {
        Expr::Value(Value::Number(s, _)) => {
            let parsed: i64 = s.parse().ok()?;
            Some(ScalarValue::Int64(if negate { -parsed } else { parsed }))
        }
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => {
            if negate {
                None
            } else {
                Some(ScalarValue::Utf8(s.clone()))
            }
        }
        Expr::Value(Value::Boolean(b)) => {
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
/// loop materially hurts other tenants on the same runtime. Point queries
/// (which carry a predicate) always stay on the cooperative pool — they
/// only touch one row group thanks to predicate pushdown, and the
/// `spawn_blocking` round-trip would dwarf the actual work.
const HEAVY_ROW_THRESHOLD: u64 = 100_000;
const HEAVY_BYTE_THRESHOLD: u64 = 50 * 1024 * 1024;

/// Run a recognised plan against the engine's storage layer (or, when wired
/// up, the shard's [`TenantHandle`]). Returns the merged result set ready to
/// hand back to the caller.
///
/// [`TenantHandle`]: basin_shard::TenantHandle
pub(crate) async fn execute_simple_select(
    sess: &TenantSession,
    plan: SimpleSelectPlan,
) -> Result<ExecResult> {
    // Flush the in-RAM tail before we look up the table's metadata so the
    // post-flush snapshot drives the heavy-read gating below. Without this
    // the catalog reports zero data files (everything still in WAL) and we'd
    // misclassify a 5M-row scan as a small one.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &plan.table)
        .await?;

    // Validate the projection against the cached schema. Doing it here means
    // unknown columns surface as a clean `NotFound` instead of an opaque
    // storage error, and lets us build a projected schema without hitting
    // Parquet at all.
    let proj_indices: Option<Vec<usize>> = match &plan.projection {
        Some(cols) => {
            let mut idxs = Vec::with_capacity(cols.len());
            for c in cols {
                let i = meta.schema.index_of(c).map_err(|_| {
                    BasinError::InvalidSchema(format!("unknown column {c}"))
                })?;
                idxs.push(i);
            }
            Some(idxs)
        }
        None => None,
    };

    let opts = ReadOptions {
        projection: plan.projection.clone(),
        filters: plan.predicate.clone().into_iter().collect(),
        partition: None,
    };

    // Gate Change C off the catalog's reported snapshot size. We treat a read
    // as "heavy" only when there's no predicate (i.e. we're scanning rather
    // than point-looking up) and the table is large enough that the decode
    // loop will dominate. With a predicate, row-group pruning keeps the
    // actual decode small no matter how big the table is, so the
    // `spawn_blocking` round-trip would be pure overhead.
    let heavy = plan.predicate.is_none()
        && meta
            .current()
            .map(|s| {
                let rows: u64 = s.data_files.iter().map(|f| f.row_count).sum();
                let bytes: u64 = s.data_files.iter().map(|f| f.size_bytes).sum();
                rows >= HEAVY_ROW_THRESHOLD || bytes >= HEAVY_BYTE_THRESHOLD
            })
            .unwrap_or(false);

    let batches = if let Some(shard) = sess.engine.config().shard.as_ref() {
        // Shard path: this read merges the in-RAM tail with the Parquet base.
        // The pre-flush above already drained the tail into Parquet so this
        // read is bounded, then `handle.read` only has to scan whatever new
        // tail rows have arrived since.
        let handle = shard.get(&sess.tenant, &PartitionKey::default_key()).await?;
        if heavy {
            let table = plan.table.clone();
            run_blocking(move || async move { handle.read(&table, opts).await }).await?
        } else {
            handle.read(&plan.table, opts).await?
        }
    } else {
        if heavy {
            let storage = sess.engine.config().storage.clone();
            let tenant = sess.tenant;
            let table = plan.table.clone();
            run_blocking(move || async move {
                use futures::StreamExt;
                let stream = storage.read(&tenant, &table, opts).await?;
                let collected: Vec<Result<RecordBatch>> = stream.collect().await;
                collected.into_iter().collect::<Result<Vec<_>>>()
            })
            .await?
        } else {
            use futures::StreamExt;
            let stream = sess
                .engine
                .config()
                .storage
                .read(&sess.tenant, &plan.table, opts)
                .await?;
            let collected: Vec<Result<RecordBatch>> = stream.collect().await;
            collected.into_iter().collect::<Result<Vec<_>>>()?
        }
    };

    let projected_schema: Arc<Schema> = match &proj_indices {
        Some(idxs) => Arc::new(meta.schema.project(idxs).map_err(|e| {
            BasinError::internal(format!("project schema: {e}"))
        })?),
        None => meta.schema.clone(),
    };

    let trimmed = match plan.limit {
        Some(limit) => apply_limit(batches, limit),
        None => batches,
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
        match plan.predicate {
            Some(Predicate::Eq(col, ScalarValue::Int64(v))) => {
                assert_eq!(col, "id");
                assert_eq!(v, 5);
            }
            other => panic!("unexpected predicate: {other:?}"),
        }
        assert!(plan.limit.is_none());
    }

    #[test]
    fn matches_select_star_with_string_literal() {
        let stmt = parse_one("SELECT * FROM events WHERE name = 'alice' LIMIT 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert!(plan.projection.is_none(), "wildcard should mean no projection");
        match plan.predicate {
            Some(Predicate::Eq(col, ScalarValue::Utf8(v))) => {
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
        assert!(plan.predicate.is_none());
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
    fn rejects_complex_predicate() {
        let stmt = parse_one("SELECT id FROM t WHERE id = 1 AND name = 'x'");
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
}
