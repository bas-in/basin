//! Phase 2 of ADR 0014. PgNode → DataFusion LogicalPlan translator.
//!
//! v0.1 supports only single-table SELECT with: column projection (or *),
//! optional WHERE (column op literal), optional ORDER BY (column ASC/DESC),
//! optional LIMIT. Anything else returns [`TranslateError::Unsupported`]
//! so the executor can fall back to the sqlparser-based path.
//!
//! Phase 2.x will extend to JOIN, GROUP BY, HAVING, window functions, set
//! ops, subqueries, CTE.

use std::sync::Arc;

use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{col, lit};
use pg_query::protobuf::node::Node as NodeEnum;
use pg_query::protobuf::{AExprKind, SetOperation, SortByDir};

/// Trait for resolving a table's [`datafusion::catalog::TableProvider`] by
/// name. The executor adapter pre-fetches the registered DataFusion provider
/// so that the translated `LogicalPlan::TableScan` embeds the real listing
/// table source and `ctx.execute_logical_plan(plan)` actually scans Parquet.
///
/// Unit tests supply a mock that builds a `MemTable`-backed source from a
/// fixed Arrow schema.
pub trait SchemaLookup {
    fn table_schema(
        &self,
        name: &str,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, TranslateError>;
}

/// Error returned when translation cannot proceed.
#[derive(Debug)]
pub enum TranslateError {
    /// The input contains a construct this v0.1 translator does not yet
    /// handle. The executor should fall back to the sqlparser path.
    Unsupported(&'static str),
    /// An internal error (e.g. DataFusion API failure) that is unlikely to be
    /// a user error. Logged and then treated like `Unsupported` by the
    /// executor (fall through; surface only if both paths fail).
    Internal(String),
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslateError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            TranslateError::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

/// Translate a single top-level pg_query `Node` into a DataFusion
/// `LogicalPlan`.
///
/// `node` must wrap a `SelectStmt`; anything else returns
/// `Err(Unsupported("not a SELECT"))`.
pub fn translate(
    node: &pg_query::protobuf::Node,
    schema_lookup: &dyn SchemaLookup,
) -> Result<LogicalPlan, TranslateError> {
    let inner = node
        .node
        .as_ref()
        .ok_or(TranslateError::Unsupported("not a SELECT"))?;

    let stmt = match inner {
        NodeEnum::SelectStmt(s) => s.as_ref(),
        _ => return Err(TranslateError::Unsupported("not a SELECT")),
    };

    // --- Gate: only SetopNone (no set ops) ------------------------------------
    let op = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op != SetOperation::SetopNone && op != SetOperation::Undefined {
        return Err(TranslateError::Unsupported("set ops"));
    }

    // --- Gate: unsupported clauses --------------------------------------------
    if stmt.with_clause.is_some() {
        return Err(TranslateError::Unsupported("CTE"));
    }
    if !stmt.group_clause.is_empty() {
        return Err(TranslateError::Unsupported("GROUP BY"));
    }
    if stmt.having_clause.is_some() {
        return Err(TranslateError::Unsupported("HAVING"));
    }
    if !stmt.window_clause.is_empty() {
        return Err(TranslateError::Unsupported("window functions"));
    }
    if !stmt.distinct_clause.is_empty() {
        return Err(TranslateError::Unsupported("DISTINCT"));
    }
    if !stmt.locking_clause.is_empty() {
        return Err(TranslateError::Unsupported("locking clause"));
    }
    if stmt.limit_offset.is_some() {
        return Err(TranslateError::Unsupported("OFFSET"));
    }

    // --- FROM clause: exactly one bare table ---------------------------------
    if stmt.from_clause.len() != 1 {
        return Err(TranslateError::Unsupported("only single-table FROM"));
    }
    let from_node = &stmt.from_clause[0];
    let rv = match from_node.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => rv,
        _ => return Err(TranslateError::Unsupported("only single-table FROM")),
    };
    let table_name = &rv.relname;

    // Resolve the table provider. The executor's adapter wraps the real
    // registered DataFusion provider; unit tests supply a MemTable.
    let provider = schema_lookup.table_schema(table_name)?;
    let source = provider_as_source(provider);

    // --- Build the base TableScan --------------------------------------------
    let mut builder = LogicalPlanBuilder::scan(table_name.as_str(), source, None)
        .map_err(|e| TranslateError::Internal(format!("scan: {e}")))?;

    // --- WHERE clause --------------------------------------------------------
    if let Some(where_node) = stmt.where_clause.as_deref() {
        let filter_expr = translate_where(where_node)?;
        builder = builder
            .filter(filter_expr)
            .map_err(|e| TranslateError::Internal(format!("filter: {e}")))?;
    }

    // --- target_list (SELECT columns / *) ------------------------------------
    // Determine if this is a `SELECT *`. If any target is non-star, collect
    // projection expressions.
    let mut exprs: Vec<datafusion::logical_expr::Expr> = Vec::new();
    let mut has_star = false;
    for target_node in &stmt.target_list {
        let res_target = match target_node.node.as_ref() {
            Some(NodeEnum::ResTarget(rt)) => rt.as_ref(),
            _ => return Err(TranslateError::Unsupported("complex target expression")),
        };
        let val_node = res_target
            .val
            .as_deref()
            .ok_or(TranslateError::Unsupported("complex target expression"))?;
        let expr = translate_target(val_node)?;
        match expr {
            TargetExpr::Star => {
                has_star = true;
            }
            TargetExpr::Expr(e) => {
                exprs.push(e);
            }
        }
    }
    // If all targets are `*` (or target_list is empty), omit explicit
    // projection — the TableScan already returns all columns.
    if !has_star && !exprs.is_empty() {
        builder = builder
            .project(exprs)
            .map_err(|e| TranslateError::Internal(format!("project: {e}")))?;
    }

    // --- ORDER BY ------------------------------------------------------------
    if !stmt.sort_clause.is_empty() {
        let mut sorts: Vec<datafusion::logical_expr::SortExpr> = Vec::new();
        for sort_node in &stmt.sort_clause {
            let sb = match sort_node.node.as_ref() {
                Some(NodeEnum::SortBy(s)) => s.as_ref(),
                _ => return Err(TranslateError::Unsupported("complex ORDER BY")),
            };
            let col_node = sb
                .node
                .as_deref()
                .ok_or(TranslateError::Unsupported("complex ORDER BY"))?;
            let col_name = column_name_from_node(col_node)?;
            let dir = SortByDir::try_from(sb.sortby_dir).unwrap_or(SortByDir::Undefined);
            let asc = match dir {
                SortByDir::SortbyDefault | SortByDir::SortbyAsc | SortByDir::Undefined => true,
                SortByDir::SortbyDesc => false,
                SortByDir::SortbyUsing => {
                    return Err(TranslateError::Unsupported("complex ORDER BY"))
                }
            };
            sorts.push(col(col_name.as_str()).sort(asc, false));
        }
        builder = builder
            .sort(sorts)
            .map_err(|e| TranslateError::Internal(format!("sort: {e}")))?;
    }

    // --- LIMIT ---------------------------------------------------------------
    if let Some(limit_node) = stmt.limit_count.as_deref() {
        let n = const_integer_value(limit_node)?;
        builder = builder
            .limit(0, Some(n as usize))
            .map_err(|e| TranslateError::Internal(format!("limit: {e}")))?;
    }

    builder
        .build()
        .map_err(|e| TranslateError::Internal(format!("build: {e}")))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

enum TargetExpr {
    Star,
    Expr(datafusion::logical_expr::Expr),
}

fn translate_target(
    node: &pg_query::protobuf::Node,
) -> Result<TargetExpr, TranslateError> {
    match node.node.as_ref() {
        Some(NodeEnum::ColumnRef(cr)) => {
            // Check for `*` (A_Star field)
            if cr.fields.len() == 1 {
                if let Some(NodeEnum::AStar(_)) = cr.fields[0].node.as_ref() {
                    return Ok(TargetExpr::Star);
                }
            }
            // Regular column reference: one String field
            let col_name = column_name_from_column_ref(cr)?;
            Ok(TargetExpr::Expr(col(col_name.as_str())))
        }
        Some(NodeEnum::AConst(ac)) => {
            let e = translate_a_const(ac)?;
            Ok(TargetExpr::Expr(e))
        }
        _ => Err(TranslateError::Unsupported("complex target expression")),
    }
}

fn translate_where(
    node: &pg_query::protobuf::Node,
) -> Result<datafusion::logical_expr::Expr, TranslateError> {
    match node.node.as_ref() {
        Some(NodeEnum::AExpr(ae)) => {
            let kind = AExprKind::try_from(ae.kind).unwrap_or(AExprKind::Undefined);
            if kind != AExprKind::AexprOp {
                return Err(TranslateError::Unsupported("complex WHERE"));
            }
            // Operator name: must be a single String node
            if ae.name.len() != 1 {
                return Err(TranslateError::Unsupported("complex WHERE"));
            }
            let op_name = match ae.name[0].node.as_ref() {
                Some(NodeEnum::String(s)) => s.sval.as_str(),
                _ => return Err(TranslateError::Unsupported("complex WHERE")),
            };
            // LHS: ColumnRef
            let lhs_node = ae
                .lexpr
                .as_deref()
                .ok_or(TranslateError::Unsupported("complex WHERE"))?;
            let col_name = column_name_from_node(lhs_node)?;
            // RHS: A_Const literal
            let rhs_node = ae
                .rexpr
                .as_deref()
                .ok_or(TranslateError::Unsupported("complex WHERE"))?;
            let rhs_lit = match rhs_node.node.as_ref() {
                Some(NodeEnum::AConst(ac)) => translate_a_const(ac)?,
                _ => return Err(TranslateError::Unsupported("complex WHERE")),
            };
            let lhs = col(col_name.as_str());
            let expr = match op_name {
                "=" => lhs.eq(rhs_lit),
                "<>" | "!=" => lhs.not_eq(rhs_lit),
                "<" => lhs.lt(rhs_lit),
                "<=" => lhs.lt_eq(rhs_lit),
                ">" => lhs.gt(rhs_lit),
                ">=" => lhs.gt_eq(rhs_lit),
                _ => return Err(TranslateError::Unsupported("complex WHERE")),
            };
            Ok(expr)
        }
        _ => Err(TranslateError::Unsupported("complex WHERE")),
    }
}

fn translate_a_const(
    ac: &pg_query::protobuf::AConst,
) -> Result<datafusion::logical_expr::Expr, TranslateError> {
    use pg_query::protobuf::a_const::Val;
    match ac.val.as_ref() {
        Some(Val::Ival(i)) => Ok(lit(i.ival as i64)),
        Some(Val::Fval(f)) => {
            let v: f64 = f
                .fval
                .parse()
                .map_err(|_| TranslateError::Unsupported("unparseable float literal"))?;
            Ok(lit(v))
        }
        Some(Val::Sval(s)) => Ok(lit(s.sval.as_str())),
        Some(Val::Boolval(b)) => Ok(lit(b.boolval)),
        _ => Err(TranslateError::Unsupported("unsupported literal type")),
    }
}

/// Extract `i64` from a Node that must be an `A_Const { val: Ival(...) }`.
fn const_integer_value(node: &pg_query::protobuf::Node) -> Result<i64, TranslateError> {
    use pg_query::protobuf::a_const::Val;
    match node.node.as_ref() {
        Some(NodeEnum::AConst(ac)) => match ac.val.as_ref() {
            Some(Val::Ival(i)) => Ok(i.ival as i64),
            _ => Err(TranslateError::Unsupported("dynamic LIMIT")),
        },
        // `$N` params or anything else
        Some(NodeEnum::ParamRef(_)) => Err(TranslateError::Unsupported("dynamic LIMIT")),
        _ => Err(TranslateError::Unsupported("dynamic LIMIT")),
    }
}

/// Extract a bare column name from a `ColumnRef { fields: [String(col)] }`.
fn column_name_from_node(node: &pg_query::protobuf::Node) -> Result<String, TranslateError> {
    match node.node.as_ref() {
        Some(NodeEnum::ColumnRef(cr)) => column_name_from_column_ref(cr),
        _ => Err(TranslateError::Unsupported("complex column reference")),
    }
}

fn column_name_from_column_ref(
    cr: &pg_query::protobuf::ColumnRef,
) -> Result<String, TranslateError> {
    if cr.fields.len() != 1 {
        return Err(TranslateError::Unsupported("complex column reference"));
    }
    match cr.fields[0].node.as_ref() {
        Some(NodeEnum::String(s)) => Ok(s.sval.clone()),
        _ => Err(TranslateError::Unsupported("complex column reference")),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    // Use DataFusion's internal arrow types (v53) because `MemTable::try_new`
    // expects DataFusion's arrow schema, not the workspace arrow v54 types.
    use datafusion::arrow::datatypes::{DataType, Field, Schema as DfSchema};
    use datafusion::datasource::memory::MemTable;

    /// A mock `SchemaLookup` backed by a `MemTable` so the translator can
    /// build a real `LogicalPlan::TableScan` without a live DataFusion session.
    struct MockLookup {
        name: &'static str,
        schema: Arc<DfSchema>,
    }

    impl SchemaLookup for MockLookup {
        fn table_schema(
            &self,
            name: &str,
        ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, TranslateError> {
            if name == self.name {
                let mem = MemTable::try_new(self.schema.clone(), vec![])
                    .map_err(|e| TranslateError::Internal(e.to_string()))?;
                Ok(Arc::new(mem))
            } else {
                Err(TranslateError::Unsupported("table not found"))
            }
        }
    }

    fn two_col_schema() -> Arc<DfSchema> {
        Arc::new(DfSchema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn three_col_schema() -> Arc<DfSchema> {
        Arc::new(DfSchema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    fn parse_first_node(sql: &str) -> pg_query::protobuf::Node {
        let tree = crate::pg_ast::parse(sql).expect("parse failed");
        let node = tree.stmts().next().expect("no statements");
        node.clone()
    }

    fn lookup(schema: Arc<DfSchema>) -> MockLookup {
        MockLookup { name: "t", schema }
    }

    // --- Positive tests ---------------------------------------------------

    #[test]
    fn select_star() {
        let node = parse_first_node("SELECT * FROM t");
        let l = lookup(two_col_schema());
        let plan = translate(&node, &l).expect("translate failed");
        let disp = format!("{}", plan.display_indent());
        // The plan should reference our table
        assert!(
            disp.contains("t"),
            "plan display should mention table 't': {disp}"
        );
        // No explicit projection wrapping the scan
        assert!(
            !disp.contains("Projection"),
            "SELECT * should not emit Projection: {disp}"
        );
    }

    #[test]
    fn select_columns_where_order_limit() {
        let node =
            parse_first_node("SELECT a, b FROM t WHERE c = 1 ORDER BY a DESC LIMIT 10");
        let l = MockLookup {
            name: "t",
            schema: three_col_schema(),
        };
        let plan = translate(&node, &l).expect("translate failed");
        let disp = format!("{}", plan.display_indent());
        assert!(disp.contains("t"), "plan should reference 't': {disp}");
        // Projection, Sort, Limit should all appear
        assert!(
            disp.contains("Projection") || disp.contains("project"),
            "expected projection: {disp}"
        );
    }

    // --- Negative / fallback tests ----------------------------------------

    #[test]
    fn join_is_unsupported() {
        let node =
            parse_first_node("SELECT * FROM t JOIN u ON t.id = u.id");
        let l = lookup(two_col_schema());
        let err = translate(&node, &l).expect_err("should be unsupported");
        assert!(
            matches!(err, TranslateError::Unsupported(_)),
            "expected Unsupported, got {err}"
        );
    }

    #[test]
    fn group_by_is_unsupported() {
        let node = parse_first_node("SELECT * FROM t GROUP BY a");
        let l = lookup(two_col_schema());
        let err = translate(&node, &l).expect_err("should be unsupported");
        assert!(
            matches!(err, TranslateError::Unsupported(_)),
            "expected Unsupported, got {err}"
        );
    }

    #[test]
    fn subquery_is_unsupported() {
        // Subquery in FROM — pg_query parses this as RangeSubselect
        let node = parse_first_node("SELECT * FROM (SELECT 1) sub");
        let l = lookup(two_col_schema());
        let err = translate(&node, &l).expect_err("should be unsupported");
        assert!(
            matches!(err, TranslateError::Unsupported(_)),
            "expected Unsupported, got {err}"
        );
    }

    #[test]
    fn param_ref_limit_is_unsupported() {
        // `$1` is a ParamRef — dynamic LIMIT, not yet supported
        let node = parse_first_node("SELECT * FROM t LIMIT $1");
        let l = lookup(two_col_schema());
        let err = translate(&node, &l).expect_err("should be unsupported");
        assert!(
            matches!(err, TranslateError::Unsupported(_)),
            "expected Unsupported, got {err}"
        );
    }

    #[test]
    fn cte_is_unsupported() {
        let node = parse_first_node("WITH cte AS (SELECT 1) SELECT * FROM t");
        let l = lookup(two_col_schema());
        let err = translate(&node, &l).expect_err("should be unsupported");
        assert!(
            matches!(err, TranslateError::Unsupported(_)),
            "expected Unsupported, got {err}"
        );
    }
}
