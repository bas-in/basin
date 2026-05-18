//! Optimizer rule that rewrites uncorrelated `lhs op ANY/ALL (SELECT col FROM
//! subq)` into a scalar-subquery form **before** DataFusion's
//! `RewriteSetComparison` decomposes it into two correlated EXISTS subqueries.
//!
//! ## Why this matters
//!
//! `RewriteSetComparison` converts `k > ANY (SELECT v FROM t)` into two
//! EXISTS subqueries: `EXISTS(SELECT … WHERE k > v IS TRUE)` and
//! `EXISTS(SELECT … WHERE k > v IS NULL)`.  DataFusion then plans each EXISTS
//! as a `LeftMark NestedLoopJoinExec` — an O(n²) algorithm.
//!
//! The scalar-subquery equivalent `k > (SELECT MIN(v) FROM t)` is an
//! uncorrelated scalar subquery that `ScalarSubqueryToJoin` decorrelates into
//! a `HashJoin` in O(n) time.
//!
//! ## Eligibility
//!
//! We only rewrite when:
//! * `subquery.outer_ref_columns.is_empty()` — truly uncorrelated
//! * `quantifier` is `Any` or `All`
//! * `op` is one of `Gt`, `GtEq`, `Lt`, `LtEq` (equality handled by InSubquery)
//! * The subquery projection has exactly ONE output column
//! * The subquery's top-level logical plan is NOT already an `Aggregate`
//!
//! Anything else is left unchanged so `RewriteSetComparison` handles it.
//!
//! ## NULL / empty-input semantics
//!
//! SQL spec:
//! * `ANY` over an empty set returns `false`
//! * `ALL`  over an empty set returns `true`
//!
//! `MIN`/`MAX` return NULL on an empty input, so `lhs > NULL` is NULL.  To
//! preserve the spec semantics we wrap:
//! * Any:  `(lhs op scalar_subq) IS TRUE`      — NULL/false both become false ✓
//! * All:  `CASE WHEN (cmp IS NULL) THEN true ELSE cmp END` — NULL becomes true  ✓

use std::sync::Arc;

use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator,
    expr::{BinaryExpr, Case, SetComparison, SetQuantifier},
    expr_fn::scalar_subquery,
    lit,
};
use datafusion::common::{Column, Result, tree_node::{Transformed, TreeNode}};

/// Whether we use MIN or MAX for a given `(quantifier, op)` pair.
#[derive(Debug, Clone, Copy)]
enum AggKind {
    Min,
    Max,
}

/// Map `(quantifier, op)` → the aggregate function needed for the scalar
/// rewrite, or `None` if this combination is not handled (e.g. `Eq`).
fn agg_for(quantifier: SetQuantifier, op: Operator) -> Option<AggKind> {
    match (quantifier, op) {
        // k > ANY(subq)  ≡  k > min(subq)   — true iff at least one element < k
        (SetQuantifier::Any, Operator::Gt) => Some(AggKind::Min),
        // k >= ANY(subq) ≡  k >= min(subq)
        (SetQuantifier::Any, Operator::GtEq) => Some(AggKind::Min),
        // k < ANY(subq)  ≡  k < max(subq)   — true iff at least one element > k
        (SetQuantifier::Any, Operator::Lt) => Some(AggKind::Max),
        // k <= ANY(subq) ≡  k <= max(subq)
        (SetQuantifier::Any, Operator::LtEq) => Some(AggKind::Max),
        // k > ALL(subq)  ≡  k > max(subq)   — true iff every element < k
        (SetQuantifier::All, Operator::Gt) => Some(AggKind::Max),
        (SetQuantifier::All, Operator::GtEq) => Some(AggKind::Max),
        // k < ALL(subq)  ≡  k < min(subq)   — true iff every element > k
        (SetQuantifier::All, Operator::Lt) => Some(AggKind::Min),
        (SetQuantifier::All, Operator::LtEq) => Some(AggKind::Min),
        _ => None,
    }
}

/// Returns `true` if the outermost node in `plan` is an `Aggregate`.
/// We bail on already-aggregated subqueries to avoid double-aggregation.
fn plan_is_aggregate(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate(_))
}

/// Attempt to rewrite a single `SetComparison` expression.
///
/// Returns `Transformed::yes(new_expr)` on success, `Transformed::no(orig)`
/// on any bail path.
fn try_rewrite(set_cmp: SetComparison) -> Result<Transformed<Expr>> {
    let SetComparison {
        expr: lhs,
        subquery,
        op,
        quantifier,
    } = set_cmp;

    // ── Eligibility checks ────────────────────────────────────────────────

    // 1. Only uncorrelated subqueries.
    if !subquery.outer_ref_columns.is_empty() {
        return Ok(Transformed::no(Expr::SetComparison(SetComparison {
            expr: lhs,
            subquery,
            op,
            quantifier,
        })));
    }

    // 2. Single-column projection required (the agg is always a scalar).
    let subq_schema = subquery.subquery.schema();
    if subq_schema.fields().len() != 1 {
        return Ok(Transformed::no(Expr::SetComparison(SetComparison {
            expr: lhs,
            subquery,
            op,
            quantifier,
        })));
    }

    // 3. Choose the aggregate kind — bails for Eq / other ops.
    let agg_kind = match agg_for(quantifier, op) {
        Some(k) => k,
        None => {
            return Ok(Transformed::no(Expr::SetComparison(SetComparison {
                expr: lhs,
                subquery,
                op,
                quantifier,
            })));
        }
    };

    // 4. Bail if the subquery is already aggregating (avoid double-agg).
    if plan_is_aggregate(&subquery.subquery) {
        return Ok(Transformed::no(Expr::SetComparison(SetComparison {
            expr: lhs,
            subquery,
            op,
            quantifier,
        })));
    }

    // ── Rewrite ───────────────────────────────────────────────────────────

    // 5. Column reference for the single output field of the subquery.
    let (qualifier, field) = subq_schema.qualified_field(0);
    let col_expr = Expr::Column(Column::new(
        qualifier.map(|q| q.clone()),
        field.name(),
    ));

    // 6. Wrap the subquery plan in Aggregate(agg(col)).
    let agg_expr = match agg_kind {
        AggKind::Min => datafusion::functions_aggregate::min_max::min(col_expr),
        AggKind::Max => datafusion::functions_aggregate::min_max::max(col_expr),
    };
    let agg_plan = LogicalPlanBuilder::from(subquery.subquery.as_ref().clone())
        .aggregate(Vec::<Expr>::new(), vec![agg_expr])?
        .build()?;

    // 7. Build `lhs op (SELECT agg FROM subq)`.
    let scalar_subq = scalar_subquery(Arc::new(agg_plan));
    let cmp = Expr::BinaryExpr(BinaryExpr {
        left: lhs,
        op,
        right: Box::new(scalar_subq),
    });

    // 8. Apply NULL/empty-set semantics wrapper.
    let result = match quantifier {
        // ANY over empty set → false; (lhs op NULL) → NULL → false via IS TRUE.
        SetQuantifier::Any => Expr::IsTrue(Box::new(cmp)),

        // ALL over empty set → true; (lhs op NULL) → NULL → true.
        // Build as CASE WHEN (cmp IS NULL) THEN true ELSE cmp END.
        SetQuantifier::All => Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(Expr::IsNull(Box::new(cmp.clone()))),
                Box::new(lit(true)),
            )],
            else_expr: Some(Box::new(cmp)),
        }),
    };

    Ok(Transformed::yes(result))
}

/// Optimizer rule that rewrites uncorrelated `ANY`/`ALL` set-comparison
/// subqueries to scalar subqueries **before** `RewriteSetComparison` runs.
///
/// Insert this rule at **position 0** of the rule list so it fires first.
/// `ScalarSubqueryToJoin` (run later by default) then decorrelates the
/// resulting scalar subquery into a `HashJoin`.
#[derive(Debug, Default)]
pub(crate) struct AnyAllToScalarSubquery;

impl OptimizerRule for AnyAllToScalarSubquery {
    fn name(&self) -> &str {
        "any_all_to_scalar_subquery"
    }

    /// Traverse the whole plan tree (including subquery plans) and rewrite
    /// `SetComparison` expressions.  Mirrors `RewriteSetComparison`:
    /// `transform_up_with_subqueries` + `map_expressions` + `transform_up`.
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|p: LogicalPlan| {
            p.map_expressions(|expr: Expr| {
                expr.transform_up(|e: Expr| match e {
                    Expr::SetComparison(sc) => try_rewrite(sc),
                    other => Ok(Transformed::no(other)),
                })
            })
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{
        lit,
        logical_plan::builder::LogicalTableSource,
        LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion::logical_expr::expr::{BinaryExpr, SetComparison, SetQuantifier};
    use datafusion::logical_expr::Operator;
    use datafusion::common::tree_node::Transformed;
    use datafusion::common::Column;

    /// Build a simple in-memory table scan for testing.
    fn make_scan(table: &str, fields: Vec<(&str, DataType)>) -> Arc<LogicalPlan> {
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let source = Arc::new(LogicalTableSource::new(Arc::clone(&schema)));
        Arc::new(
            LogicalPlanBuilder::scan(table, source, None)
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    /// Build a `Subquery` wrapper (uncorrelated) around a plan.
    fn make_subquery(
        plan: Arc<LogicalPlan>,
    ) -> datafusion::logical_expr::logical_plan::Subquery {
        datafusion::logical_expr::logical_plan::Subquery {
            subquery: plan,
            outer_ref_columns: vec![],
            spans: datafusion::common::Spans::new(),
        }
    }

    // ── test: any_gt_rewrites_to_scalar_subquery_with_min ─────────────────

    /// `lit(5) > ANY (SELECT v FROM t)` must rewrite to `IS TRUE(5 > (SELECT
    /// MIN(v) FROM t))` — a scalar subquery containing a `MIN` aggregate.
    ///
    /// This test name is explicitly referenced in the task spec.
    #[test]
    fn any_gt_rewrites_to_scalar_subquery_with_min() -> datafusion::common::Result<()> {
        let subq_plan = make_scan("t", vec![("v", DataType::Int64)]);
        let subquery = make_subquery(subq_plan);

        let set_cmp = SetComparison {
            expr: Box::new(lit(5i64)),
            subquery,
            op: Operator::Gt,
            quantifier: SetQuantifier::Any,
        };

        let rewritten = Expr::SetComparison(set_cmp)
            .transform_up(|e: Expr| match e {
                Expr::SetComparison(sc) => try_rewrite(sc),
                other => Ok(Transformed::no(other)),
            })?
            .data;

        // Must NOT remain a SetComparison.
        assert!(
            !matches!(rewritten, Expr::SetComparison(_)),
            "Expected SetComparison to be rewritten, got: {:?}",
            rewritten
        );

        // Top-level must be IS TRUE(...).
        let Expr::IsTrue(inner) = &rewritten else {
            panic!("Expected IsTrue wrapper, got: {:?}", rewritten);
        };

        // Inner must be a BinaryExpr with Gt.
        let Expr::BinaryExpr(BinaryExpr { right, op, .. }) = inner.as_ref() else {
            panic!("Expected BinaryExpr inside IsTrue, got: {:?}", inner);
        };
        assert_eq!(*op, Operator::Gt, "operator must be Gt");

        // Right-hand side must be a ScalarSubquery.
        let Expr::ScalarSubquery(sq) = right.as_ref() else {
            panic!("Expected ScalarSubquery on rhs, got: {:?}", right);
        };

        // The subquery plan must be an Aggregate.
        let LogicalPlan::Aggregate(agg) = sq.subquery.as_ref() else {
            panic!("Expected Aggregate in ScalarSubquery, got: {:?}", sq.subquery);
        };

        // The aggregate expression must contain "min".
        let agg_display = format!("{:?}", agg.aggr_expr);
        assert!(
            agg_display.to_lowercase().contains("min"),
            "Expected MIN aggregate, got: {}",
            agg_display
        );

        Ok(())
    }

    // ── test: all_lt_rewrites_to_scalar_subquery_with_min ─────────────────

    /// `lit(5) < ALL (SELECT v FROM t)` must rewrite to a CASE expression
    /// wrapping `5 < (SELECT MIN(v) FROM t)`.
    #[test]
    fn all_lt_rewrites_to_scalar_subquery_with_min() -> datafusion::common::Result<()> {
        let subq_plan = make_scan("t", vec![("v", DataType::Int64)]);
        let subquery = make_subquery(subq_plan);

        let set_cmp = SetComparison {
            expr: Box::new(lit(5i64)),
            subquery,
            op: Operator::Lt,
            quantifier: SetQuantifier::All,
        };

        let rewritten = Expr::SetComparison(set_cmp)
            .transform_up(|e: Expr| match e {
                Expr::SetComparison(sc) => try_rewrite(sc),
                other => Ok(Transformed::no(other)),
            })?
            .data;

        assert!(
            !matches!(rewritten, Expr::SetComparison(_)),
            "Expected SetComparison to be rewritten"
        );

        // ALL uses a CASE expression for NULL-to-true semantics.
        assert!(
            matches!(rewritten, Expr::Case(_)),
            "Expected CASE wrapper for ALL, got: {:?}",
            rewritten
        );

        // Dig into the CASE to confirm MIN in the scalar subquery.
        let Expr::Case(case_expr) = &rewritten else {
            unreachable!()
        };
        let else_branch = case_expr.else_expr.as_ref().unwrap();
        let Expr::BinaryExpr(BinaryExpr { right, op, .. }) = else_branch.as_ref() else {
            panic!("Expected BinaryExpr in else branch, got: {:?}", else_branch);
        };
        assert_eq!(*op, Operator::Lt);
        let Expr::ScalarSubquery(sq) = right.as_ref() else {
            panic!("Expected ScalarSubquery, got: {:?}", right);
        };
        let LogicalPlan::Aggregate(agg) = sq.subquery.as_ref() else {
            panic!("Expected Aggregate, got: {:?}", sq.subquery);
        };
        let agg_display = format!("{:?}", agg.aggr_expr);
        assert!(
            agg_display.to_lowercase().contains("min"),
            "Expected MIN aggregate, got: {}",
            agg_display
        );

        Ok(())
    }

    // ── test: correlated_subquery_is_not_rewritten ────────────────────────

    /// A correlated subquery must NOT be rewritten — left for
    /// `RewriteSetComparison` to handle.
    #[test]
    fn correlated_subquery_is_not_rewritten() -> datafusion::common::Result<()> {
        let subq_plan = make_scan("t", vec![("v", DataType::Int64)]);

        // Simulate a correlated subquery by adding an outer-reference column.
        let outer_col = Expr::OuterReferenceColumn(
            Arc::new(Field::new("x", DataType::Int64, true)),
            Column::new_unqualified("x"),
        );
        let subquery = datafusion::logical_expr::logical_plan::Subquery {
            subquery: subq_plan,
            outer_ref_columns: vec![outer_col],
            spans: datafusion::common::Spans::new(),
        };

        let set_cmp = SetComparison {
            expr: Box::new(lit(5i64)),
            subquery,
            op: Operator::Gt,
            quantifier: SetQuantifier::Any,
        };

        let rewritten = Expr::SetComparison(set_cmp)
            .transform_up(|e: Expr| match e {
                Expr::SetComparison(sc) => try_rewrite(sc),
                other => Ok(Transformed::no(other)),
            })?
            .data;

        // Must remain a SetComparison — unchanged.
        assert!(
            matches!(rewritten, Expr::SetComparison(_)),
            "Correlated subquery should NOT be rewritten, got: {:?}",
            rewritten
        );

        Ok(())
    }
}
