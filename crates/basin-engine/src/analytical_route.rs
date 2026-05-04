//! Planner heuristic that decides whether a parsed statement should be
//! routed to the analytical (DuckDB) engine instead of the OLTP DataFusion
//! path.
//!
//! The detector is intentionally cheap: it walks the sqlparser AST and looks
//! at three signals, in order:
//!
//! 1. `/*+ analytical */` — an explicit hint anywhere in the raw SQL. This is
//!    the first thing we check because it lets a user override the heuristic
//!    without us having to reason about the query shape at all. A substring
//!    scan is fine: sqlparser strips comments before producing the AST, so
//!    the hint is invisible there, and the analytical engine itself
//!    revalidates the SQL before executing it (`basin_analytical::validate_sql`).
//!
//! 2. An aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`,
//!    `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`,
//!    `VAR_SAMP`, `COUNT_DISTINCT`) anywhere in the SELECT projection. This
//!    catches the common "`SELECT count(*) FROM big_table`" shape, which on
//!    even a moderate dataset is several times faster on DuckDB.
//!
//! 3. A non-empty `GROUP BY`. Hash-aggregation is the workload DuckDB was
//!    built for; routing every grouped query to it is a clear win on the
//!    workloads `viability_analytical` exercises.
//!
//! v0.2 deliberately does NOT inspect catalog statistics. Item 4 from the
//! design brief — "scanned-row estimate exceeds 10M" — needs per-table row
//! counts that the catalog currently does not expose to the planner. That
//! becomes a cheap addition once the catalog grows a row-count column, and
//! is tracked as a v0.3 follow-up.

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Query, SelectItem, SetExpr, Statement,
};

/// The marker we look for in the raw SQL string. Keep this in one place so
/// the docs and the code can't drift.
const ANALYTICAL_HINT: &str = "/*+ analytical */";

/// Aggregate function names we treat as "this query wants a column engine".
/// We compare case-insensitively so `count(*)` and `COUNT(*)` both match.
/// The list is a curated subset of the SQL standard; vendor-specific
/// extensions (e.g. DuckDB's `arg_max`) are not included because we don't
/// want to misroute a query that DataFusion would handle perfectly well.
const AGGREGATE_FUNCTION_NAMES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "count_distinct",
];

/// Returns `true` if the planner heuristic says route `stmt` to the
/// analytical engine.
///
/// `raw_sql` is the SQL as the user wrote it, used only for the
/// `/*+ analytical */` hint scan; sqlparser strips comments before producing
/// `stmt`, so the hint is otherwise invisible.
pub(crate) fn is_analytical(stmt: &Statement, raw_sql: &str) -> bool {
    // Explicit hint wins outright. We deliberately don't try to validate
    // hint placement (inside vs outside the SELECT, etc.); any presence of
    // the marker forces analytical routing.
    if raw_sql.contains(ANALYTICAL_HINT) {
        return matches!(stmt, Statement::Query(_));
    }

    let query = match stmt {
        Statement::Query(q) => q,
        // INSERT / UPDATE / DELETE / DDL never route analytical: the
        // analytical engine is read-only.
        _ => return false,
    };

    query_is_analytical(query.as_ref())
}

fn query_is_analytical(q: &Query) -> bool {
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        // Set-ops, VALUES, table refs etc. fall through to OLTP for now.
        _ => return false,
    };

    // Non-empty GROUP BY → analytical. Both `Expressions` (the standard
    // form) and `All` (`GROUP BY ALL`) qualify.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => return true,
        GroupByExpr::All(_) => return true,
        _ => {}
    }

    // Any aggregate function in the projection → analytical.
    for item in &select.projection {
        if projection_contains_aggregate(item) {
            return true;
        }
    }

    false
}

fn projection_contains_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_contains_aggregate(e)
        }
        // Wildcards (`*`, `t.*`) carry no expression to inspect.
        _ => false,
    }
}

fn expr_contains_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Function(f) => is_aggregate_function(f) || function_args_contain_aggregate(f),
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::Nested(inner) => expr_contains_aggregate(inner),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_deref()
                .map(expr_contains_aggregate)
                .unwrap_or(false)
                || conditions.iter().any(expr_contains_aggregate)
                || results.iter().any(expr_contains_aggregate)
                || else_result
                    .as_deref()
                    .map(expr_contains_aggregate)
                    .unwrap_or(false)
        }
        _ => false,
    }
}

fn is_aggregate_function(f: &Function) -> bool {
    // sqlparser stores qualified names as parts; we treat any function whose
    // *final* identifier matches our list as an aggregate. That handles both
    // `count(*)` and `pg_catalog.count(*)`.
    let last = match f.name.0.last() {
        Some(part) => part.value.to_ascii_lowercase(),
        None => return false,
    };
    AGGREGATE_FUNCTION_NAMES.contains(&last.as_str())
}

fn function_args_contain_aggregate(f: &Function) -> bool {
    match &f.args {
        FunctionArguments::List(list) => {
            list.args.iter().any(function_arg_contains_aggregate)
        }
        FunctionArguments::Subquery(_) | FunctionArguments::None => false,
    }
}

fn function_arg_contains_aggregate(a: &FunctionArg) -> bool {
    match a {
        FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
            function_arg_expr_contains_aggregate(arg)
        }
    }
}

fn function_arg_expr_contains_aggregate(e: &FunctionArgExpr) -> bool {
    match e {
        FunctionArgExpr::Expr(expr) => expr_contains_aggregate(expr),
        FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        let mut s = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        s.pop().unwrap()
    }

    #[test]
    fn is_analytical_detects_aggregate_projection() {
        let sql = "SELECT count(*) FROM t";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_detects_sum_avg_min_max() {
        for sql in [
            "SELECT sum(x) FROM t",
            "SELECT avg(x) FROM t",
            "SELECT min(x), max(x) FROM t",
            "SELECT stddev(x) FROM t",
        ] {
            assert!(is_analytical(&parse(sql), sql), "expected analytical for {sql}");
        }
    }

    #[test]
    fn is_analytical_detects_aggregate_under_cast() {
        // The integration tests cast aggregates to BIGINT to nail down
        // DuckDB's hugeint promotion; that wrapping must not hide the
        // aggregate from the detector.
        let sql = "SELECT CAST(sum(x) AS BIGINT) AS s FROM t";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_detects_group_by() {
        let sql = "SELECT x, count(*) FROM t GROUP BY x";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_detects_group_by_without_aggregate() {
        // GROUP BY alone (e.g. for distinct-style projection) is enough;
        // we don't require an aggregate in the projection.
        let sql = "SELECT x FROM t GROUP BY x";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_detects_hint() {
        let sql = "SELECT /*+ analytical */ * FROM t";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_detects_hint_outside_select() {
        // The hint can sit anywhere — inside a leading comment, between
        // identifiers, etc. We deliberately don't restrict its placement.
        let sql = "/*+ analytical */ SELECT id FROM t WHERE id = 1";
        assert!(is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_skips_simple_select() {
        let sql = "SELECT * FROM t WHERE id = 1";
        assert!(!is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_skips_select_with_predicate_only() {
        let sql = "SELECT id, name FROM t WHERE id = 1";
        assert!(!is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_skips_insert() {
        // INSERT statements never route analytical — the analytical engine
        // is read-only and the OLTP path owns durability.
        let sql = "INSERT INTO t VALUES (1, 'a')";
        assert!(!is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_skips_create_table() {
        let sql = "CREATE TABLE t (id BIGINT)";
        assert!(!is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_skips_update_with_aggregate_lookalike() {
        // UPDATE doesn't hit the analytical engine even if its body would
        // otherwise look aggregate-shaped.
        let sql = "UPDATE t SET x = x + 1";
        assert!(!is_analytical(&parse(sql), sql));
    }

    #[test]
    fn is_analytical_case_insensitive_aggregate() {
        let sql = "SELECT COUNT(*) FROM t";
        assert!(is_analytical(&parse(sql), sql));
    }
}
