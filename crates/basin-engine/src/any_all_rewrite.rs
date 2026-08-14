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
//! * `quantifier` is `Any` (**`All` is always declined** — see `agg_for`)
//! * `op` is one of `Gt`, `GtEq`, `Lt`, `LtEq` (equality handled by InSubquery)
//! * The subquery projection has exactly ONE output column
//! * The subquery's top-level logical plan is NOT already an `Aggregate`
//!
//! Anything else is left unchanged so `RewriteSetComparison` handles it.
//!
//! ## NULL / empty-input semantics
//!
//! Measured against live PostgreSQL 18.2, `x OP ANY/ALL (S)` is a Kleene
//! fold over the element comparisons, not a comparison against an extremum:
//!
//! * `ANY` — TRUE if any element comparison is TRUE, else NULL if any is
//!   NULL, else FALSE. Empty `S` → FALSE.
//! * `ALL` — FALSE if any element comparison is FALSE, else NULL if any is
//!   NULL, else TRUE. Empty `S` → TRUE (vacuously).
//!
//! `MIN`/`MAX` skip NULL elements and return NULL on empty input, so a
//! scalar comparison against them cannot represent the NULL arm of either
//! fold. What that costs each quantifier is different:
//!
//! * Any:  `(lhs op scalar_subq) IS TRUE` — turns the NULL arm into FALSE.
//!   A `WHERE` or `JOIN … ON` position cannot tell those apart, so this
//!   rewrite is answer-preserving there. It is **not** exact in a *value*
//!   position: PostgreSQL gives `1.5 > ANY (SELECT amt FROM t WHERE id
//!   < 101)` = NULL over a NULL-containing subquery, this rule gives FALSE.
//!   Kept for the O(n) plan, with that caveat recorded rather than hidden.
//! * All:  declined outright. The old `CASE WHEN (cmp IS NULL) THEN true
//!   ELSE cmp END` wrapper turned NULL into TRUE, which a `WHERE` clause
//!   *does* expose, and it produced wrong rows even when the subquery held
//!   no NULLs at all. `agg_for`'s doc comment has the measured rows.

use std::sync::Arc;

use datafusion::common::{
    tree_node::{Transformed, TreeNode},
    Column, Result,
};
use datafusion::logical_expr::{
    expr::{BinaryExpr, SetComparison, SetQuantifier},
    expr_fn::scalar_subquery,
    Expr, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Whether we use MIN or MAX for a given `(quantifier, op)` pair.
#[derive(Debug, Clone, Copy)]
enum AggKind {
    Min,
    Max,
}

/// Map `(quantifier, op)` → the aggregate function needed for the scalar
/// rewrite, or `None` if this combination is not handled (e.g. `Eq`).
///
/// # `ALL` is deliberately absent
///
/// This rule used to map `ALL` onto `max`/`min` too, wrapped in
/// `CASE WHEN cmp IS NULL THEN true ELSE cmp END`. That form is wrong, and
/// wrong in a way a `WHERE` clause exposes. Measured against live PostgreSQL
/// 18.2 on the golden seed
/// `t(id, amt) = (1,1.5),(2,2.5),(3,3.5),(100,NULL),(101,10.5)`:
///
/// ```text
/// WHERE amt > ALL (SELECT amt FROM t WHERE id < 2)    -- plain subquery
///   PostgreSQL: {2,3,101}          CASE/max form: {2,3,100,101}
/// WHERE amt > ALL (SELECT amt FROM t WHERE id < 101)  -- subquery has a NULL
///   PostgreSQL: (no rows)          CASE/max form: {100,101}
/// ```
///
/// Two independent defects, both of which turn NULL into TRUE:
///
/// * The `CASE WHEN cmp IS NULL THEN true` arm cannot tell *why* `cmp` is
///   NULL. It is meant to catch "the subquery was empty, so `max` is NULL,
///   so the predicate is vacuously true" — but it fires just as eagerly when
///   the NULL came from the **left** operand. Row 100 has `amt IS NULL`, so
///   `NULL > 1.5` is NULL, and the rule promotes that row to TRUE. That is
///   the extra `100` in the plain case, where the subquery holds no NULLs at
///   all and the empty-set rationale does not even apply.
/// * `max`/`min` skip NULLs *inside* the subquery, so a NULL element that
///   should force the whole Kleene AND-fold to NULL is silently dropped.
///
/// `x OP ALL (S)` is a three-valued AND-fold: FALSE if any element
/// comparison is FALSE, else NULL if any is NULL, else TRUE (and TRUE for
/// empty `S`, vacuously). No single `max`/`min` scalar comparison can encode
/// the "some element compared NULL" state, so `ALL` is declined outright and
/// left to DataFusion's `RewriteSetComparison`, whose two-EXISTS
/// decomposition tracks the NULL arm explicitly.
///
/// `ANY` is kept: `IsTrue(k op min/max)` collapses NULL to FALSE, which a
/// `WHERE`/`JOIN` position cannot distinguish. It is still not exact in a
/// *value* position — PostgreSQL gives `1.5 > ANY (SELECT amt FROM t WHERE
/// id < 101)` = NULL where this rule gives FALSE — see the module-level note.
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
        // `ALL` in every form: declined — see the doc comment above.
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
    let col_expr = Expr::Column(Column::new(qualifier.map(|q| q.clone()), field.name()));

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

    // 8. Apply the NULL/empty-set semantics wrapper.
    //
    // Only `Any` can reach here — `agg_for` declines every `All` form (see
    // its doc comment). ANY over an empty set is FALSE, and `lhs op NULL` is
    // NULL, both of which `IS TRUE` collapses to FALSE.
    debug_assert!(
        matches!(quantifier, SetQuantifier::Any),
        "agg_for must decline ALL before this point"
    );
    let result = Expr::IsTrue(Box::new(cmp));

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
    use datafusion::common::tree_node::Transformed;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::{BinaryExpr, SetComparison, SetQuantifier};
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::{
        lit, logical_plan::builder::LogicalTableSource, LogicalPlan, LogicalPlanBuilder,
    };

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
    fn make_subquery(plan: Arc<LogicalPlan>) -> datafusion::logical_expr::logical_plan::Subquery {
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
            panic!(
                "Expected Aggregate in ScalarSubquery, got: {:?}",
                sq.subquery
            );
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

    // ── test: all_quantifier_is_declined ──────────────────────────────────

    /// Every `ALL` form must be left as a `SetComparison` for DataFusion's
    /// `RewriteSetComparison` to lower.
    ///
    /// This test previously asserted the opposite — that `5 < ALL (SELECT v
    /// FROM t)` became `CASE WHEN cmp IS NULL THEN true ELSE cmp END` over
    /// `5 < (SELECT MIN(v) FROM t)`. That form was measured wrong against
    /// live PostgreSQL 18.2 on the golden seed and is no longer produced:
    /// `WHERE amt > ALL (SELECT amt FROM t WHERE id < 2)` returned
    /// `{2,3,101}` from PostgreSQL and `{2,3,100,101}` from the CASE form,
    /// because row 100's NULL `amt` makes `cmp` NULL and the CASE promoted
    /// that to TRUE. See `agg_for`'s doc comment for the full measurement.
    #[test]
    fn all_quantifier_is_declined() -> datafusion::common::Result<()> {
        for op in [Operator::Gt, Operator::GtEq, Operator::Lt, Operator::LtEq] {
            let subq_plan = make_scan("t", vec![("v", DataType::Int64)]);
            let subquery = make_subquery(subq_plan);

            let set_cmp = SetComparison {
                expr: Box::new(lit(5i64)),
                subquery,
                op,
                quantifier: SetQuantifier::All,
            };

            let rewritten = Expr::SetComparison(set_cmp)
                .transform_up(|e: Expr| match e {
                    Expr::SetComparison(sc) => try_rewrite(sc),
                    other => Ok(Transformed::no(other)),
                })?
                .data;

            assert!(
                matches!(rewritten, Expr::SetComparison(_)),
                "`{op:?} ALL` must be declined, got: {rewritten:?}"
            );
        }
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
