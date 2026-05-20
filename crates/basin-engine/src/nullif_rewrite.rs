//! LogicalPlan-level rewrite of `NULLIF(a, b) IS [NOT] NULL` to a plain
//! conjunction of simple predicates that Vortex pushes down trivially.
//!
//! The earlier generic NULLIF → CASE rewrite was correct but slower than
//! leaving NULLIF alone: Vortex's `CaseWhen` evaluator decoded the column
//! to evaluate the case, which cost more than DataFusion's residual filter
//! over NULLIF.  The narrower rewrite below only fires for the specific
//! null-test patterns the smoke benchmark cares about — `NULLIF(a, b) IS
//! NOT NULL` and `NULLIF(a, b) IS NULL` — which decompose to two simple
//! predicates each, all of which Vortex pushes natively.
//!
//! Semantics (PostgreSQL):
//!   * `NULLIF(a, b)` returns NULL when `a = b`, else `a`.
//!   * `NULLIF(a, b) IS NOT NULL` ≡ `a IS NOT NULL AND a != b`
//!   * `NULLIF(a, b) IS NULL`     ≡ `a IS NULL OR a = b`
//!
//! Both equivalences preserve three-valued logic for NULL handling: when
//! `a` is NULL, the LHS evaluates to UNKNOWN (NULL) and the RHS clauses
//! produce the same UNKNOWN under SQL's standard 3VL truth tables.
//!
//! Bare NULLIF calls outside an `IS [NOT] NULL` wrapper are left
//! untouched and handled by DataFusion as a residual filter.

use datafusion::common::{Result, tree_node::{Transformed, TreeNode}};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};

/// Rewrites `NULLIF(a, b) IS NOT NULL` to `a IS NOT NULL AND a != b`,
/// and `NULLIF(a, b) IS NULL` to `a IS NULL OR a = b`.
///
/// Registered at optimizer position 0 so it sees the original NULLIF
/// before `SimplifyExpressions` folds anything.  Bare NULLIF calls
/// (no `IS NULL`/`IS NOT NULL` wrapper) are left untouched.
#[derive(Debug, Default)]
pub(crate) struct NullifRewrite;

impl OptimizerRule for NullifRewrite {
    fn name(&self) -> &str {
        "nullif_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr: Expr| expr.transform_up(rewrite_nullif_is_null_expr))
    }
}

/// Pull the (a, b) args out of an expression iff it's a `NULLIF(a, b)`
/// scalar function call.  Returns `None` otherwise.
fn match_nullif(expr: &Expr) -> Option<(Expr, Expr)> {
    let sf = match expr {
        Expr::ScalarFunction(sf) => sf,
        _ => return None,
    };
    if !sf.name().eq_ignore_ascii_case("nullif") {
        return None;
    }
    if sf.args.len() != 2 {
        return None;
    }
    Some((sf.args[0].clone(), sf.args[1].clone()))
}

fn rewrite_nullif_is_null_expr(expr: Expr) -> Result<Transformed<Expr>> {
    match &expr {
        // NULLIF(a, b) IS NOT NULL  ≡  a IS NOT NULL AND a != b
        Expr::IsNotNull(inner) => {
            if let Some((a, b)) = match_nullif(inner) {
                let lhs = a.clone().is_not_null();
                let rhs = a.not_eq(b);
                return Ok(Transformed::yes(lhs.and(rhs)));
            }
            Ok(Transformed::no(expr))
        }
        // NULLIF(a, b) IS NULL  ≡  a IS NULL OR a = b
        Expr::IsNull(inner) => {
            if let Some((a, b)) = match_nullif(inner) {
                let lhs = a.clone().is_null();
                let rhs = a.eq(b);
                return Ok(Transformed::yes(lhs.or(rhs)));
            }
            Ok(Transformed::no(expr))
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
    use datafusion::functions::core::nullif as make_nullif_udf;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, lit, Expr, Filter, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::OptimizerContext;

    fn schema_k() -> Arc<LogicalTableSource> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
        ]));
        Arc::new(LogicalTableSource::new(schema))
    }

    fn nullif_k_0() -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: make_nullif_udf(),
            args: vec![col("k"), lit(0_i64)],
        })
    }

    /// `NULLIF(k, 0) IS NOT NULL` must rewrite to `k IS NOT NULL AND k != 0`.
    #[test]
    fn is_not_null_rewrites_to_conjunction() -> Result<()> {
        let predicate = Expr::IsNotNull(Box::new(nullif_k_0()));
        let plan = LogicalPlanBuilder::scan("t", schema_k(), None)?
            .filter(predicate)?
            .build()?;

        let result = NullifRewrite.rewrite(plan, &OptimizerContext::new())?.data;
        let pred = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        assert!(
            !pred.to_lowercase().contains("nullif("),
            "NULLIF should be gone; predicate:\n{pred}"
        );
        assert!(
            pred.contains("IsNotNull") && pred.contains("NotEq"),
            "Expected `IS NOT NULL AND != ` form; predicate:\n{pred}"
        );
        Ok(())
    }

    /// `NULLIF(k, 0) IS NULL` must rewrite to `k IS NULL OR k = 0`.
    #[test]
    fn is_null_rewrites_to_disjunction() -> Result<()> {
        let predicate = Expr::IsNull(Box::new(nullif_k_0()));
        let plan = LogicalPlanBuilder::scan("t", schema_k(), None)?
            .filter(predicate)?
            .build()?;

        let result = NullifRewrite.rewrite(plan, &OptimizerContext::new())?.data;
        let pred = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        assert!(
            !pred.to_lowercase().contains("nullif("),
            "NULLIF should be gone; predicate:\n{pred}"
        );
        assert!(
            pred.contains("IsNull") && pred.contains("Eq"),
            "Expected `IS NULL OR = ` form; predicate:\n{pred}"
        );
        Ok(())
    }

    /// Bare `NULLIF(k, 0)` (no IS NULL/IS NOT NULL wrapper) must NOT be
    /// rewritten — DataFusion handles it as a residual.
    #[test]
    fn bare_nullif_is_left_alone() -> Result<()> {
        let predicate = nullif_k_0().gt(lit(5_i64));
        let plan = LogicalPlanBuilder::scan("t", schema_k(), None)?
            .filter(predicate)?
            .build()?;

        let result = NullifRewrite.rewrite(plan, &OptimizerContext::new())?.data;
        let pred = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => format!("{predicate:?}"),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        // The NULLIF scalar function must survive untouched. Newer
        // DataFusion renders the Debug form as the UDF struct name
        // (`NullIfFunc`) rather than call-style `nullif(...)`, so match
        // the function name case-insensitively without assuming a paren.
        assert!(
            pred.to_lowercase().contains("nullif"),
            "Bare NULLIF should be preserved; predicate:\n{pred}"
        );
        // And it must NOT have been decomposed into the IS NULL / Eq
        // conjunction the wrapped-NULLIF rewrite produces.
        assert!(
            !(pred.contains("IsNull") && pred.contains("Eq")),
            "Bare NULLIF must not be rewritten to a conjunction; predicate:\n{pred}"
        );
        Ok(())
    }
}
