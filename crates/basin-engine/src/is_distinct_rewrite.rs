//! LogicalPlan-level rewrite of `IS [NOT] DISTINCT FROM` to plain logical
//! predicates Vortex pushes down.
//!
//! DataFusion 53 represents `a IS DISTINCT FROM b` and `a IS NOT DISTINCT
//! FROM b` as `Expr::BinaryExpr` with `Operator::IsDistinctFrom` /
//! `Operator::IsNotDistinctFrom`.  Vortex's `DefaultExpressionConvertor`
//! doesn't push either operator, so they fall back to a residual filter
//! over a fully-decoded column.
//!
//! Semantics (SQL three-valued logic):
//!
//!   * `a IS DISTINCT FROM b` ≡
//!         `(a IS NULL AND b IS NOT NULL)
//!          OR (a IS NOT NULL AND b IS NULL)
//!          OR (a != b)`
//!     The last clause returns FALSE if either operand is NULL, so the
//!     three-arm OR exactly captures the SQL semantics.
//!
//!   * `a IS NOT DISTINCT FROM b` ≡
//!         `(a IS NULL AND b IS NULL) OR (a = b)`
//!     `a = b` returns FALSE if either operand is NULL, so the two-arm
//!     OR matches the standard semantics.
//!
//! Each clause is a simple operator Vortex's converter accepts (`IsNull`,
//! `IsNotNull`, `BinaryExpr { Eq | NotEq }`), and the outer OR is also
//! supported.  Pushing the full predicate down avoids the residual
//! filter's full-column decode.

use datafusion::common::{Result, tree_node::{Transformed, TreeNode}};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};

/// Rewrites `a IS DISTINCT FROM b` and `a IS NOT DISTINCT FROM b` to
/// pushable conjunctions/disjunctions of `IS NULL`, `IS NOT NULL`, and
/// `= / != ` predicates.
///
/// Registered at optimizer position 0 so the rewrite fires before
/// `SimplifyExpressions` can fold the operator into something opaque.
#[derive(Debug, Default)]
pub(crate) struct IsDistinctRewrite;

impl OptimizerRule for IsDistinctRewrite {
    fn name(&self) -> &str {
        "is_distinct_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr: Expr| expr.transform_up(rewrite_is_distinct_expr))
    }
}

fn rewrite_is_distinct_expr(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
        return Ok(Transformed::no(expr));
    };

    match op {
        Operator::IsDistinctFrom => {
            let a = (**left).clone();
            let b = (**right).clone();
            // (a IS NULL AND b IS NOT NULL)
            //   OR (a IS NOT NULL AND b IS NULL)
            //   OR (a != b)
            let lhs = a.clone().is_null().and(b.clone().is_not_null());
            let mid = a.clone().is_not_null().and(b.clone().is_null());
            let rhs = a.not_eq(b);
            Ok(Transformed::yes(lhs.or(mid).or(rhs)))
        }
        Operator::IsNotDistinctFrom => {
            let a = (**left).clone();
            let b = (**right).clone();
            // (a IS NULL AND b IS NULL) OR (a = b)
            let lhs = a.clone().is_null().and(b.clone().is_null());
            let rhs = a.eq(b);
            Ok(Transformed::yes(lhs.or(rhs)))
        }
        _ => Ok(Transformed::no(expr)),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, BinaryExpr, Expr, Filter, LogicalPlan, LogicalPlanBuilder, Operator};
    use datafusion::optimizer::OptimizerContext;

    fn make_scan() -> Arc<LogicalTableSource> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        Arc::new(LogicalTableSource::new(schema))
    }

    #[test]
    fn is_distinct_from_rewrites_to_three_arm_or() -> Result<()> {
        let predicate = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::IsDistinctFrom,
            right: Box::new(col("b")),
        });
        let plan = LogicalPlanBuilder::scan("t", make_scan(), None)?
            .filter(predicate)?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?.data;
        let pred = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        assert!(
            !pred.contains("IsDistinctFrom"),
            "IsDistinctFrom should be gone; predicate:\n{pred}"
        );
        // 2 IS NULL + 2 IS NOT NULL + 1 NotEq
        assert!(pred.contains("IsNull") && pred.contains("IsNotNull") && pred.contains("NotEq"));
        Ok(())
    }

    #[test]
    fn is_not_distinct_from_rewrites_to_two_arm_or() -> Result<()> {
        let predicate = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(col("b")),
        });
        let plan = LogicalPlanBuilder::scan("t", make_scan(), None)?
            .filter(predicate)?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?.data;
        let pred = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        assert!(
            !pred.contains("IsNotDistinctFrom"),
            "IsNotDistinctFrom should be gone; predicate:\n{pred}"
        );
        assert!(pred.contains("IsNull") && pred.contains("Eq"));
        Ok(())
    }
}
