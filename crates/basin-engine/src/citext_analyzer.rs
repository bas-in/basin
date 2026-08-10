//! Schema-aware logical-plan rewrite for `CITEXT` columns (Phase 5.30.C/E).
//!
//! ## Problem
//!
//! Basin stores `CITEXT` columns as Arrow `Utf8` with field metadata
//! `BASIN_TYPE=CITEXT`. DataFusion has no knowledge of this marker, so a plain
//! SQL predicate like `WHERE email = 'alice@example.com'` compiles to a
//! case-*sensitive* `BinaryExpr { Eq, col("email"), lit("alice@example.com") }`.
//! That breaks the core citext contract: equality and ordering must be
//! case-insensitive.
//!
//! ## Solution: DataFusion `AnalyzerRule`
//!
//! An `AnalyzerRule` runs *after* the logical planner has resolved all column
//! references and attached field schemas to every node. At that point we can
//! inspect the input schema to determine which columns carry `BASIN_TYPE=CITEXT`
//! and rewrite:
//!
//! - **Binary comparisons** (`=`, `<>`, `<`, `<=`, `>`, `>=`) where at least
//!   one operand resolves to a citext column: wrap both sides in `lower()`.
//!
//! - **LIKE** on a citext column: rewrite to `lower(col) LIKE lower(pattern)`
//!   with `case_insensitive=true`.
//!
//! - **Sort expressions** on citext columns: wrap the sort key in `lower()`.
//!
//! ## Scope discipline (no false-positives)
//!
//! The rule fires *only* when it can prove at least one operand is citext:
//! a `Column` expression whose Arrow `Field` has `BASIN_TYPE=CITEXT`, or a
//! `Cast`/`TryCast` that wraps such a column.  Normal `TEXT` columns are
//! never touched.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result as DFResult;
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::{
    expr::Alias,
    expr::Sort as SortExpr,
    logical_plan::{Filter, Sort},
    BinaryExpr, Cast, Expr, Like, LogicalPlan, Operator, TryCast,
};
use datafusion::optimizer::AnalyzerRule;

use crate::types::{BASIN_TYPE_CITEXT, BASIN_TYPE_KEY};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// DataFusion `AnalyzerRule` that rewrites comparisons and sort expressions on
/// `CITEXT` columns to use case-insensitive semantics.
///
/// Register with `SessionStateBuilder::with_analyzer_rule(...)`.
#[derive(Debug, Default)]
pub(crate) struct CitextAnalyzerRule;

impl AnalyzerRule for CitextAnalyzerRule {
    fn name(&self) -> &str {
        "citext_analyzer"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        // Fast path: the rewrite can only matter when some column in the
        // plan is CITEXT-typed. The overwhelmingly common no-CITEXT case
        // previously paid a full transform_up tree walk on EVERY query;
        // a flat scan of the plan's schemas is far cheaper and exact
        // (CITEXT-ness is carried in field metadata end to end).
        if !plan_has_citext_column(&plan) {
            return Ok(plan);
        }
        let transformed = plan.transform_up(rewrite_plan_node)?;
        Ok(transformed.data)
    }
}

/// `true` when any field anywhere in the plan tree carries the CITEXT
/// type marker. Walks node schemas only (no expression rewriting), so the
/// no-CITEXT fast path costs one metadata scan per node.
fn plan_has_citext_column(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if node.schema().fields().iter().any(|f| {
            f.metadata()
                .get(crate::types::BASIN_TYPE_KEY)
                .map(|v| v == crate::types::BASIN_TYPE_CITEXT)
                .unwrap_or(false)
        }) {
            found = true;
            return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-plan-node rewrite
// ─────────────────────────────────────────────────────────────────────────────

fn rewrite_plan_node(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    match plan {
        // ── Filter: predicate expressions reference the *input* schema ─────────
        LogicalPlan::Filter(filter) => {
            let input_schema = filter.input.schema();
            let new_pred = rewrite_expr(filter.predicate, input_schema)?;
            let did_change = new_pred.transformed;
            let new_filter = LogicalPlan::Filter(Filter::try_new(new_pred.data, filter.input)?);
            if did_change {
                Ok(Transformed::yes(new_filter))
            } else {
                Ok(Transformed::no(new_filter))
            }
        }

        // ── Sort: sort-key expressions reference the *input* schema ───────────
        LogicalPlan::Sort(sort) => {
            let input_schema = sort.input.schema();
            let mut changed = false;
            let new_exprs: Vec<SortExpr> = sort
                .expr
                .into_iter()
                .map(|sort_key| {
                    if is_citext_expr(&sort_key.expr, input_schema) {
                        changed = true;
                        SortExpr {
                            expr: lower(sort_key.expr),
                            asc: sort_key.asc,
                            nulls_first: sort_key.nulls_first,
                        }
                    } else {
                        sort_key
                    }
                })
                .collect();
            if changed {
                Ok(Transformed::yes(LogicalPlan::Sort(Sort {
                    expr: new_exprs,
                    input: sort.input,
                    fetch: sort.fetch,
                })))
            } else {
                Ok(Transformed::no(LogicalPlan::Sort(Sort {
                    expr: new_exprs,
                    input: sort.input,
                    fetch: sort.fetch,
                })))
            }
        }

        // All other node types: unchanged.
        other => Ok(Transformed::no(other)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression-level rewrite
// ─────────────────────────────────────────────────────────────────────────────

/// Recursively rewrite `expr`, folding citext comparisons/LIKE to use `lower()`.
fn rewrite_expr(expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    expr.transform_up(|e| rewrite_single_expr(e, schema))
}

fn rewrite_single_expr(expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    match &expr {
        // ── Binary comparisons ─────────────────────────────────────────────────
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let op = *op;
            match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let l_is_citext = is_citext_expr(left, schema);
                    let r_is_citext = is_citext_expr(right, schema);
                    if l_is_citext || r_is_citext {
                        let new_expr = Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(lower(*left.clone())),
                            op,
                            right: Box::new(lower(*right.clone())),
                        });
                        return Ok(Transformed::yes(new_expr));
                    }
                    Ok(Transformed::no(expr))
                }
                _ => Ok(Transformed::no(expr)),
            }
        }

        // ── LIKE on citext column ──────────────────────────────────────────────
        // Rewrite to `lower(col) LIKE lower(pattern)` with case_insensitive=true.
        Expr::Like(Like {
            negated,
            expr: like_expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            let negated = *negated;
            let escape_char = *escape_char;
            // Only rewrite LIKE (not ILIKE which is already case-insensitive).
            if !case_insensitive && is_citext_expr(like_expr, schema) {
                let new_expr = Expr::Like(Like {
                    negated,
                    expr: Box::new(lower(*like_expr.clone())),
                    pattern: Box::new(lower(*pattern.clone())),
                    escape_char,
                    case_insensitive: true,
                });
                return Ok(Transformed::yes(new_expr));
            }
            Ok(Transformed::no(expr))
        }

        _ => Ok(Transformed::no(expr)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Wrap `expr` in `lower(...)` using DataFusion's built-in `lower` scalar UDF.
fn lower(expr: Expr) -> Expr {
    datafusion::functions::string::lower().call(vec![expr])
}

/// Returns `true` if `expr` resolves to a citext-typed value according to the
/// column schema. Only `Column` references (and casts over them) are checked;
/// literals are never considered citext.
fn is_citext_expr(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::Column(col) => column_is_citext(col, schema),

        // CAST(col AS TEXT) on a citext column — the `::citext` PG cast appears
        // as a Utf8→Utf8 no-op Cast. Propagate citext through it.
        Expr::Cast(Cast { expr, data_type }) => {
            *data_type == DataType::Utf8 && is_citext_expr(expr, schema)
        }
        Expr::TryCast(TryCast { expr, data_type }) => {
            *data_type == DataType::Utf8 && is_citext_expr(expr, schema)
        }

        // Alias wrapping a citext expr (e.g. `col AS alias` in a derived query).
        Expr::Alias(Alias { expr, .. }) => is_citext_expr(expr, schema),

        _ => false,
    }
}

/// Check whether `col` refers to a citext field in `schema`.
fn column_is_citext(col: &Column, schema: &DFSchema) -> bool {
    let field = if let Some(rel) = col.relation.as_ref() {
        schema.field_with_qualified_name(rel, &col.name).ok()
    } else {
        schema.field_with_unqualified_name(&col.name).ok()
    };

    field
        .map(|f| {
            f.metadata()
                .get(BASIN_TYPE_KEY)
                .map(|v| v.as_str() == BASIN_TYPE_CITEXT)
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;
    use datafusion::logical_expr::{
        col, lit, BinaryExpr, Expr, Filter, LogicalPlan, LogicalPlanBuilder, Operator,
    };
    use datafusion::optimizer::AnalyzerRule;

    use super::CitextAnalyzerRule;
    use crate::types::{BASIN_TYPE_CITEXT, BASIN_TYPE_KEY};

    fn mixed_schema() -> Arc<Schema> {
        let citext_field = Field::new("email", DataType::Utf8, true).with_metadata(HashMap::from(
            [(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_CITEXT.to_string())],
        ));
        Arc::new(Schema::new(vec![
            citext_field,
            Field::new("name", DataType::Utf8, true), // plain text, no marker
        ]))
    }

    fn make_scan(schema: Arc<Schema>) -> Arc<LogicalTableSource> {
        Arc::new(LogicalTableSource::new(schema))
    }

    #[test]
    fn eq_on_citext_col_wraps_both_in_lower() {
        let schema = mixed_schema();
        let scan = make_scan(schema);
        let pred = col("email").eq(lit("Alice@Example.com"));
        let plan = LogicalPlanBuilder::scan("t", scan, None)
            .unwrap()
            .filter(pred)
            .unwrap()
            .build()
            .unwrap();

        let result = CitextAnalyzerRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        let pred_str = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("expected Filter, got: {other:?}"),
        };
        // The Debug form uses "LowerFunc" rather than "lower"; either works as proof.
        assert!(
            pred_str.contains("LowerFunc") || pred_str.contains("lower("),
            "expected lower() wrapping; predicate: {pred_str}"
        );
    }

    #[test]
    fn eq_on_plain_text_col_unchanged() {
        let schema = mixed_schema();
        let scan = make_scan(schema);
        let pred = col("name").eq(lit("Alice"));
        let plan = LogicalPlanBuilder::scan("t", scan, None)
            .unwrap()
            .filter(pred)
            .unwrap()
            .build()
            .unwrap();

        let result = CitextAnalyzerRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();

        let pred_str = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("expected Filter, got: {other:?}"),
        };
        // Must NOT contain lower() wrapping — plain text is untouched.
        assert!(
            !pred_str.contains("LowerFunc") && !pred_str.contains("lower("),
            "plain text col must not be lower()-wrapped; predicate: {pred_str}"
        );
    }

    #[test]
    fn lt_on_citext_col_wraps_in_lower() {
        let schema = mixed_schema();
        let scan = make_scan(schema);
        let pred = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("email")),
            op: Operator::Lt,
            right: Box::new(lit("zoo@example.com")),
        });
        let plan = LogicalPlanBuilder::scan("t", scan, None)
            .unwrap()
            .filter(pred)
            .unwrap()
            .build()
            .unwrap();

        let result = CitextAnalyzerRule
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let pred_str = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("expected Filter, got: {other:?}"),
        };
        assert!(
            pred_str.contains("LowerFunc") || pred_str.contains("lower("),
            "< on citext col must produce lower(); got: {pred_str}"
        );
    }
}
