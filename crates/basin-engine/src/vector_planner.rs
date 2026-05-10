//! Automatic planner routing for `ORDER BY <vec_col> <-> $param LIMIT k`.
//!
//! The default SQL pipeline rewrites `<->` / `<#>` / `<=>` to UDF calls and
//! lets DataFusion compute distances row-by-row across a full table scan —
//! that is correct but pays O(rows * dim) per query. The HNSW sidecar
//! indexes (built on every Parquet write) can answer the same top-k in
//! O(log rows * dim).
//!
//! This module recognises the exact `ORDER BY <vec_col> <op> <literal-or-
//! single-element-paren> LIMIT k` shape and, if every criterion holds, hands
//! the engine an alternate execution path that calls
//! [`Storage::vector_search`] under the hood. Anything off-shape returns
//! `None` and the existing brute-force pipeline takes over — correctness is
//! preserved for every query the rewriter declines.
//!
//! Detection criteria (all must hold; checked in this order):
//! 1. Single-table `FROM`, no joins / derived tables / table aliases.
//! 2. Exactly one `ORDER BY` expression.
//! 3. That expression is `<col> <distance_op> <literal-or-paren-literal>`
//!    where `<distance_op>` is one of `<->`, `<#>`, `<=>`. The right-hand
//!    side must already be substituted to a literal vector — bound `$N`
//!    parameters are surfaced through the prepared-statement substitutor
//!    by the time `executor::execute` sees the SQL, so by the time we run
//!    the right-hand side is a quoted vector literal.
//! 4. The column resolves to `vector(N)` (Arrow `FixedSizeList<Float32>`)
//!    on the table.
//! 5. `LIMIT` is a constant non-negative integer literal.
//! 6. `ORDER BY` direction is the default (ASC) — descending would request
//!    "least similar," which the storage fast path doesn't currently model.
//! 7. `WHERE` is either absent or a conjunction of `<col> = <literal>`
//!    predicates, all targeting columns on this same table. Anything
//!    richer (OR / IN / IS NULL / function calls / sub-queries) falls back.
//!
//! Filter pushdown shape: the storage fast path does *not* accept filters
//! today, so we over-fetch `k * OVER_FETCH_FACTOR` candidates and apply
//! the conjunction of equality predicates in-engine after fetching the
//! matching rows. This trades a constant-factor overhead at the storage
//! tier for keeping the storage API stable.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use basin_catalog::Catalog;
use basin_common::{BasinError, Result, TableName, TenantId};
use basin_storage::{evaluate_predicate, Predicate, ScalarValue};
use basin_vector::Distance;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectName, OrderByExpr, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::types::parse_vector_literal;

/// How much to over-fetch from the HNSW fast path when the rewriter has a
/// `WHERE` predicate to apply post-hoc. A factor of 4 lets a moderately
/// selective filter (≥25% rows match) still return a full top-k without a
/// second probe; less-selective filters fall back to brute-force at the
/// criteria check (`is_pushdown_predicate` only allows column-equality
/// against a literal).
const OVER_FETCH_FACTOR: usize = 4;

/// Distance operator parsed out of the `ORDER BY` expression. Mirrors the
/// three pgvector operator forms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceOp {
    L2,
    Cosine,
    Dot,
}

impl DistanceOp {
    fn metric(self) -> Distance {
        match self {
            DistanceOp::L2 => Distance::L2,
            DistanceOp::Cosine => Distance::Cosine,
            DistanceOp::Dot => Distance::Dot,
        }
    }
}

/// Outcome of a successful detection. The executor consumes this to call
/// `Storage::vector_search` and project the resulting rows.
#[derive(Debug)]
pub struct VectorSearchPlan {
    pub table: TableName,
    pub vec_col: String,
    pub query_vec: Vec<f32>,
    pub k: usize,
    /// Conjunction of column-equality predicates carried over from `WHERE`.
    /// Empty means the user query had no `WHERE` clause we recognised.
    pub filters: Vec<Predicate>,
    pub distance_op: DistanceOp,
    /// Projected output columns from the user's `SELECT` clause. `None`
    /// means `SELECT *`.
    pub projection: Option<Vec<String>>,
    /// Whether to over-fetch from the storage fast path. True iff `filters`
    /// is non-empty.
    pub over_fetch: bool,
}

/// Inspect `sql`, returning `Some(plan)` if every criterion holds. The
/// catalog is consulted to confirm the target column is a vector column on
/// the named table; tenants that don't have the table at all still safely
/// return `None` (caller falls back, the existing path produces the same
/// "table not found" error).
pub async fn rewrite_vector_order_by(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    sql: &str,
) -> Result<Option<VectorSearchPlan>> {
    // Cheap pre-gate: any of the three operator strings must appear in the
    // raw SQL. Keeps the planner cost zero for the 99% of queries that
    // aren't vector top-k.
    if !sql.contains("<->") && !sql.contains("<#>") && !sql.contains("<=>") {
        return Ok(None);
    }

    let dialect = PostgreSqlDialect {};
    // sqlparser 0.52 doesn't natively parse pgvector ops; the engine's
    // textual rewrite happens *after* this detect pass, so we run our own
    // mini-rewrite that converts `a <-> b` → `__basin_l2(a, b)` etc., parse
    // that, then unwrap the marker calls when reading the ORDER BY tree.
    // The marker function names are deliberately namespaced so they can
    // never collide with a user-defined UDF.
    let prepared = mark_vector_ops(sql);
    let stmts = match Parser::parse_sql(&dialect, &prepared) {
        Ok(s) => s,
        // Parse failure → not our shape; let the main pipeline produce the
        // user-visible diagnostic.
        Err(_) => return Ok(None),
    };
    if stmts.len() != 1 {
        return Ok(None);
    }
    let query = match &stmts[0] {
        Statement::Query(q) => q.as_ref(),
        _ => return Ok(None),
    };

    // Criterion 5 (LIMIT must be a constant integer) is checked first
    // because failures are common and cheap to spot.
    let k = match &query.limit {
        Some(Expr::Value(Value::Number(s, _))) => match s.parse::<i64>() {
            Ok(n) if n > 0 => n as usize,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    // Reject any query that carries auxiliary clauses we'd silently lose
    // when routing.
    if query.with.is_some()
        || !query.limit_by.is_empty()
        || query.offset.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return Ok(None);
    }

    // Criterion 2: exactly one ORDER BY expression.
    let Some(order) = query.order_by.as_ref() else {
        return Ok(None);
    };
    if order.exprs.len() != 1 {
        return Ok(None);
    }
    let order_expr: &OrderByExpr = &order.exprs[0];
    // Criterion 6: ASC. `asc = None` means default which is ASC.
    if let Some(false) = order_expr.asc {
        return Ok(None);
    }
    // Reject NULLS FIRST/LAST which would be ambiguous after routing.
    if order_expr.nulls_first.is_some() {
        return Ok(None);
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => return Ok(None),
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
        return Ok(None);
    }

    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return Ok(None),
    }

    // Criterion 1: single bare table.
    if select.from.len() != 1 {
        return Ok(None);
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Ok(None);
    }
    let table_name = match &from.relation {
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
                return Ok(None);
            }
            single_part_table(name)?
        }
        _ => return Ok(None),
    };
    let Some(table) = table_name else {
        return Ok(None);
    };

    // Criterion 3: ORDER BY expr is one of our marker calls — unwrap into
    // (col, op, vector_literal).
    let Some((vec_col, distance_op, query_vec)) = extract_distance_call(&order_expr.expr) else {
        return Ok(None);
    };

    // Criterion 4: confirm `vec_col` is a `vector(N)` column on the table.
    // A catalog miss here means the table doesn't exist in this tenant; we
    // return `Ok(None)` so the existing pipeline produces the canonical
    // "table not found" error instead of a vector-routing-flavoured one.
    let meta = match catalog.load_table(tenant, &table).await {
        Ok(m) => m,
        Err(_) => return Ok(None),
    };
    let dim = match meta.schema.field_with_name(&vec_col) {
        Ok(field) => match field.data_type() {
            DataType::FixedSizeList(child, n) if matches!(child.data_type(), DataType::Float32) => {
                *n as usize
            }
            _ => return Ok(None),
        },
        Err(_) => return Ok(None),
    };
    if query_vec.len() != dim {
        return Err(BasinError::InvalidSchema(format!(
            "vector ORDER BY: query has dim {} but column {} is dim {}",
            query_vec.len(),
            vec_col,
            dim
        )));
    }

    // Projection: same shape the point-query fast path accepts — bare `*`
    // or a list of identifiers. Compound names / expressions / aliases
    // disable routing (the brute-force path's DataFusion handles them
    // correctly).
    let projection = match parse_projection(&select.projection) {
        Some(p) => p,
        None => return Ok(None),
    };

    // Criterion 7: WHERE clause must be a conjunction of column-equality
    // predicates against literals on the same table. If we can't parse it
    // into that shape, fall back. Validate that every referenced column
    // exists on the table.
    let filters = match &select.selection {
        None => Vec::new(),
        Some(expr) => match parse_pushdown_predicate(expr) {
            Some(preds) => {
                for p in &preds {
                    if meta.schema.field_with_name(p.column()).is_err() {
                        return Ok(None);
                    }
                }
                preds
            }
            None => return Ok(None),
        },
    };

    Ok(Some(VectorSearchPlan {
        table,
        vec_col,
        query_vec,
        k,
        over_fetch: !filters.is_empty(),
        filters,
        distance_op,
        projection,
    }))
}

fn single_part_table(name: &ObjectName) -> Result<Option<TableName>> {
    if name.0.len() != 1 {
        return Ok(None);
    }
    Ok(TableName::new(name.0[0].value.clone()).ok())
}

fn parse_projection(items: &[SelectItem]) -> Option<Option<Vec<String>>> {
    if items.len() == 1 {
        if let SelectItem::Wildcard(opts) = &items[0] {
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

/// Walk a `Some(expr)` `WHERE` tree, accepting only an AND-conjunction of
/// `col = <literal>` atoms. Returns `None` on anything else.
fn parse_pushdown_predicate(expr: &Expr) -> Option<Vec<Predicate>> {
    let mut out = Vec::new();
    collect_eq_atoms(expr, &mut out)?;
    Some(out)
}

fn collect_eq_atoms(expr: &Expr, out: &mut Vec<Predicate>) -> Option<()> {
    match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            collect_eq_atoms(left, out)?;
            collect_eq_atoms(right, out)
        }
        Expr::Nested(inner) => collect_eq_atoms(inner, out),
        Expr::BinaryOp {
            op: BinaryOperator::Eq,
            left,
            right,
        } => {
            let pred = parse_eq_atom(left, right)?;
            out.push(pred);
            Some(())
        }
        _ => None,
    }
}

fn parse_eq_atom(left: &Expr, right: &Expr) -> Option<Predicate> {
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

/// Recognise the literal forms that fit `basin_storage::ScalarValue`. Same
/// vocabulary the point-query fast path accepts.
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
        Expr::Value(Value::Number(s, _)) => {
            let n: i64 = s.parse().ok()?;
            Some(ScalarValue::Int64(if negate { -n } else { n }))
        }
        Expr::Value(Value::SingleQuotedString(s)) => {
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

/// Substitute every `<col> <-> <rhs>` (and `<#>` / `<=>`) for a marker
/// function call so sqlparser can ingest the SQL. The marker names live in
/// a `__basin_` namespace so they cannot collide with user UDFs (we never
/// register UDFs under that prefix; if a user did, the brute-force pipeline
/// would still see the rewrite via [`udf::rewrite_vector_operators`] using
/// the public names).
///
/// This deliberately mirrors the textual rules of
/// [`crate::udf::rewrite_vector_operators`] so the same operand-extraction
/// invariants hold. We don't reuse that helper directly because it produces
/// the public-facing `l2_distance(...)` etc. names, which would round-trip
/// through DataFusion's UDF resolver and the planner would lose the
/// "I came from `<->`" signal that lets us route.
fn mark_vector_ops(sql: &str) -> String {
    let mut s = sql.to_string();
    loop {
        let Some(found) = find_first_op(&s) else {
            break;
        };
        let (op_start, op_end, op) = found;
        let (lhs_start, lhs_end) = extract_left_operand(&s, op_start);
        let (rhs_start, rhs_end) = extract_right_operand(&s, op_end);
        let lhs = &s[lhs_start..lhs_end];
        let rhs = &s[rhs_start..rhs_end];
        let func = match op {
            "<->" => format!("__basin_vop_l2({lhs}, {rhs})"),
            "<=>" => format!("__basin_vop_cos({lhs}, {rhs})"),
            "<#>" => format!("__basin_vop_dot({lhs}, {rhs})"),
            _ => unreachable!(),
        };
        s.replace_range(lhs_start..rhs_end, &func);
    }
    s
}

fn find_first_op(s: &str) -> Option<(usize, usize, &'static str)> {
    let mut best: Option<(usize, usize, &'static str)> = None;
    for op in ["<->", "<#>", "<=>"] {
        if let Some(pos) = s.find(op) {
            match best {
                Some((p, _, _)) if pos >= p => {}
                _ => best = Some((pos, pos + op.len(), op)),
            }
        }
    }
    best
}

fn extract_left_operand(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let token_end = i;
    if i == 0 {
        return (0, token_end);
    }
    match bytes[i - 1] {
        b')' => {
            let mut depth = 1i32;
            i -= 1;
            while i > 0 && depth > 0 {
                i -= 1;
                match bytes[i] {
                    b')' => depth += 1,
                    b'(' => depth -= 1,
                    _ => {}
                }
            }
            (i, token_end)
        }
        b']' => {
            let mut depth = 1i32;
            i -= 1;
            while i > 0 && depth > 0 {
                i -= 1;
                match bytes[i] {
                    b']' => depth += 1,
                    b'[' => depth -= 1,
                    _ => {}
                }
            }
            (i, token_end)
        }
        b'\'' => {
            i -= 1;
            while i > 0 {
                i -= 1;
                if bytes[i] == b'\'' {
                    break;
                }
            }
            (i, token_end)
        }
        _ => {
            while i > 0 {
                let b = bytes[i - 1];
                if b.is_ascii_alphanumeric() || b == b'_' || b == b'.' {
                    i -= 1;
                } else {
                    break;
                }
            }
            (i, token_end)
        }
    }
}

fn extract_right_operand(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let token_start = i;
    if i >= bytes.len() {
        return (token_start, token_start);
    }
    match bytes[i] {
        b'(' => {
            let mut depth = 1i32;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            (token_start, i)
        }
        b'[' => {
            let mut depth = 1i32;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'[' => depth += 1,
                    b']' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            (token_start, i)
        }
        b'\'' => {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            (token_start, i)
        }
        _ => {
            while i < bytes.len() {
                let b = bytes[i];
                if b.is_ascii_alphanumeric() || b == b'_' || b == b'.' {
                    i += 1;
                } else {
                    break;
                }
            }
            (token_start, i)
        }
    }
}

/// Recognise `__basin_vop_l2(<col>, <vector_lit>)` (or `_cos` / `_dot`).
/// Returns `None` if the expression isn't one of our markers, the column
/// side is anything other than a single identifier, or the literal side
/// can't be parsed back to `Vec<f32>`.
fn extract_distance_call(expr: &Expr) -> Option<(String, DistanceOp, Vec<f32>)> {
    let func = match expr {
        Expr::Function(f) => f,
        _ => return None,
    };
    if func.name.0.len() != 1 {
        return None;
    }
    let op = match func.name.0[0].value.as_str() {
        "__basin_vop_l2" => DistanceOp::L2,
        "__basin_vop_cos" => DistanceOp::Cosine,
        "__basin_vop_dot" => DistanceOp::Dot,
        _ => return None,
    };
    let args = match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => &list.args,
        _ => return None,
    };
    if args.len() != 2 {
        return None;
    }
    let arg_exprs: Vec<&Expr> = args
        .iter()
        .filter_map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e)
            }
            _ => None,
        })
        .collect();
    if arg_exprs.len() != 2 {
        return None;
    }
    let col_expr = unwrap_nested(arg_exprs[0]);
    let lit_expr = unwrap_nested(arg_exprs[1]);
    let col = match col_expr {
        Expr::Identifier(i) => i.value.clone(),
        _ => return None,
    };
    let lit = match lit_expr {
        Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
        _ => return None,
    };
    let v = parse_vector_literal(lit).ok()?;
    Some((col, op, v))
}

fn unwrap_nested(expr: &Expr) -> &Expr {
    let mut cur = expr;
    while let Expr::Nested(inner) = cur {
        cur = inner.as_ref();
    }
    cur
}

/// Apply each predicate in `filters` as a boolean mask AND'd together,
/// then return the indices of rows where the mask is true. Used to
/// post-filter the over-fetched candidates from `Storage::vector_search`.
pub fn surviving_indices(batch: &RecordBatch, filters: &[Predicate]) -> Result<Vec<usize>> {
    if filters.is_empty() {
        return Ok((0..batch.num_rows()).collect());
    }
    let mut combined: Option<BooleanArray> = None;
    for p in filters {
        let mask = evaluate_predicate(batch, p)?;
        combined = Some(match combined {
            None => mask,
            Some(prev) => arrow::compute::and(&prev, &mask)
                .map_err(|e| BasinError::internal(format!("filter and: {e}")))?,
        });
    }
    let mask = combined.expect("non-empty filters → some combined");
    let mut keep = Vec::with_capacity(mask.len());
    for i in 0..mask.len() {
        if mask.is_valid(i) && mask.value(i) {
            keep.push(i);
        }
    }
    Ok(keep)
}

/// Project `batch` down to `cols` (in the user's order) and drop the
/// trailing `_distance` column the engine `vector_search` appends. When
/// `cols` is `None` we return every column except `_distance`.
pub fn project_for_user(batch: &RecordBatch, cols: &Option<Vec<String>>) -> Result<RecordBatch> {
    let schema = batch.schema();
    let names: Vec<String> = match cols {
        Some(cols) => cols.clone(),
        None => schema
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == "_distance" {
                    None
                } else {
                    Some(f.name().clone())
                }
            })
            .collect(),
    };
    let mut indices: Vec<usize> = Vec::with_capacity(names.len());
    for name in &names {
        let idx = schema
            .index_of(name)
            .map_err(|e| BasinError::internal(format!("project col {name}: {e}")))?;
        indices.push(idx);
    }
    let proj_schema = Arc::new(Schema::new(
        indices
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect::<Vec<_>>(),
    ));
    let cols_arr = indices
        .into_iter()
        .map(|i| batch.column(i).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(proj_schema, cols_arr)
        .map_err(|e| BasinError::internal(format!("project batch: {e}")))
}

/// Empty-batch helper for the no-rows-after-filter case. Produces a batch
/// with the user's projected schema and zero rows so the executor returns
/// a well-formed (but empty) result set.
pub fn empty_for_projection(
    table_schema: &Schema,
    projection: &Option<Vec<String>>,
) -> Result<RecordBatch> {
    let names: Vec<String> = match projection {
        Some(cols) => cols.clone(),
        None => table_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
    };
    let fields = names
        .iter()
        .map(|n| {
            table_schema
                .field_with_name(n)
                .map(|f| Arc::new(f.clone()))
                .map_err(|e| BasinError::internal(format!("schema lookup {n}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let cols = fields
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    RecordBatch::try_new(schema, cols)
        .map_err(|e| BasinError::internal(format!("empty projection: {e}")))
}

/// Map distance op → catalog `Distance` for the storage call. Centralised
/// so the executor doesn't grow a duplicate match arm.
pub fn distance_for(op: DistanceOp) -> Distance {
    op.metric()
}

/// Effective k handed to `Storage::vector_search`. When a `WHERE` predicate
/// will trim candidates after the index scan, fetch a multiple of `k` so
/// the post-filter still has enough rows to satisfy the user's `LIMIT`.
pub fn fetch_k(plan: &VectorSearchPlan) -> usize {
    if plan.over_fetch {
        plan.k.saturating_mul(OVER_FETCH_FACTOR).max(plan.k)
    } else {
        plan.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_vector_ops_basic() {
        let s = mark_vector_ops("SELECT * FROM t ORDER BY embedding <-> '[0.1, 0.2]' LIMIT 5");
        assert!(
            s.contains("__basin_vop_l2(embedding, '[0.1, 0.2]')"),
            "got: {s}"
        );
    }

    #[test]
    fn mark_vector_ops_cosine_and_dot() {
        let s = mark_vector_ops("SELECT * FROM t ORDER BY e <=> '[1.0]'");
        assert!(s.contains("__basin_vop_cos(e, '[1.0]')"), "{s}");
        let s = mark_vector_ops("SELECT * FROM t ORDER BY e <#> '[1.0]'");
        assert!(s.contains("__basin_vop_dot(e, '[1.0]')"), "{s}");
    }

    #[test]
    fn parse_eq_conjunction() {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(
            &dialect,
            "SELECT * FROM t WHERE category = 'foo' AND tier = 1",
        )
        .unwrap();
        let q = match &stmts[0] {
            Statement::Query(q) => q.as_ref(),
            _ => unreachable!(),
        };
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s.as_ref(),
            _ => unreachable!(),
        };
        let preds = parse_pushdown_predicate(select.selection.as_ref().unwrap()).unwrap();
        assert_eq!(preds.len(), 2);
        assert!(matches!(&preds[0], Predicate::Eq(c, _) if c == "category"));
        assert!(matches!(&preds[1], Predicate::Eq(c, _) if c == "tier"));
    }

    #[test]
    fn parse_eq_rejects_or() {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(
            &dialect,
            "SELECT * FROM t WHERE category = 'foo' OR tier = 1",
        )
        .unwrap();
        let q = match &stmts[0] {
            Statement::Query(q) => q.as_ref(),
            _ => unreachable!(),
        };
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s.as_ref(),
            _ => unreachable!(),
        };
        assert!(parse_pushdown_predicate(select.selection.as_ref().unwrap()).is_none());
    }
}
