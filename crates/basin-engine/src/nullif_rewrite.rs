//! LogicalPlan-level rewrite of `NULLIF(a, b)` to an equivalent `CASE` expression.
//!
//! `NULLIF(a, b)` is semantically identical to
//! `CASE WHEN a = b THEN NULL ELSE a END`, but DataFusion's Vortex
//! integration does not push `NULLIF` calls down into the Vortex read path
//! (the expression is opaque to the DefaultExpressionConvertor).  Rewriting
//! to a `CASE` expression happens at the `LogicalPlan` layer, before
//! physical planning, so the Vortex convertor sees the expanded form and can
//! push the predicate down to the file-chunk level.
//!
//! The rule is registered at position 0 in the optimizer pipeline so it runs
//! before `SimplifyExpressions`, which would otherwise fold constant
//! `NULLIF`s before we get a chance to see them in their original form.

use datafusion::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};
use datafusion::common::{DFSchema, Result, ScalarValue, tree_node::{Transformed, TreeNode}};
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};

/// Rewrites every `NULLIF(arg0, arg1)` in a `LogicalPlan` to
/// `CASE WHEN arg0 = arg1 THEN NULL ELSE arg0 END`.
///
/// This is a semantics-preserving identity: SQL defines `NULLIF(V, X)` as
/// returning NULL when `V = X` and returning `V` otherwise.  The rewrite
/// makes the expression visible to DataFusion's Vortex expression convertor
/// so it can be pushed down through the Vortex file-read path.
#[derive(Debug, Default)]
pub(crate) struct NullifRewrite;

impl OptimizerRule for NullifRewrite {
    fn name(&self) -> &str {
        "nullif_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Walk bottom-up: rewrite inner NULLIFs before outer ones so nested
        // calls (unusual but legal) are handled correctly in a single pass.
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Build the merged schema from all inputs so column references resolve.
        let input_schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
        plan.map_expressions(|expr: Expr| {
            expr.transform_up(|e| rewrite_nullif_expr(e, &input_schema))
        })
    }
}

/// Rewrite a single `NULLIF(arg0, arg1)` call to an equivalent `CASE` expr.
///
/// `NULLIF(arg0, arg1)` rewrites to `CASE WHEN arg0 = arg1 THEN <null> ELSE arg0 END`
/// where `<null>` is a typed null whose type matches `arg0`.  The typed null is
/// needed because the CASE expression may reach the Vortex expression convertor
/// before DataFusion's type-coercion pass can resolve untyped `ScalarValue::Null`.
///
/// Any expression that is not a two-argument `nullif` scalar function is
/// returned unchanged (`Transformed::no`).
fn rewrite_nullif_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match &expr {
        Expr::ScalarFunction(sf) if sf.name().eq_ignore_ascii_case("nullif") => {
            // nullif is always called with exactly 2 arguments; guard anyway.
            if sf.args.len() != 2 {
                return Ok(Transformed::no(expr));
            }
            // Clone the args out of the borrowed expr before moving.
            let arg0 = sf.args[0].clone();
            let arg1 = sf.args[1].clone();

            // Derive the return type of NULLIF — same as arg0's type — so the
            // THEN-branch null is typed.  This prevents the Vortex expression
            // convertor from seeing a bare `null` alongside a typed ELSE branch
            // and panicking on a type mismatch.
            let null_scalar = match arg0.get_type(schema) {
                Ok(dt) => ScalarValue::try_from(&dt).unwrap_or(ScalarValue::Null),
                Err(_) => ScalarValue::Null,
            };

            // CASE WHEN arg0 = arg1 THEN NULL::<type> ELSE arg0 END
            let when_expr = Box::new(arg0.clone().eq(arg1));
            let then_expr = Box::new(Expr::Literal(null_scalar, None));
            let else_expr = Some(Box::new(arg0));

            let case_expr = Expr::Case(Case {
                expr: None,
                when_then_expr: vec![(when_expr, then_expr)],
                else_expr,
            });

            Ok(Transformed::yes(case_expr))
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
    use datafusion::functions::core::nullif as make_nullif_udf;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::common::Result;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, lit, Filter, LogicalPlan, LogicalPlanBuilder};

    /// Build a simple `Filter(NULLIF(col_k, lit(0)) IS NOT NULL, Scan t)` plan,
    /// apply the rule directly, and assert the NULLIF was expanded to a typed CASE.
    #[test]
    fn nullif_in_filter_becomes_case() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
        ]));
        let source = Arc::new(LogicalTableSource::new(Arc::clone(&schema)));

        // NULLIF(k, 0) IS NOT NULL — make_nullif_udf() returns Arc<ScalarUDF>
        let nullif_udf = make_nullif_udf();
        let nullif_sf = ScalarFunction {
            func: nullif_udf,
            args: vec![col("k"), lit(0_i64)],
        };
        let predicate = Expr::IsNotNull(Box::new(Expr::ScalarFunction(nullif_sf)));

        let plan = LogicalPlanBuilder::scan("t", source, None)?
            .filter(predicate)?
            .build()?;

        // Apply the rule directly without going through the full optimizer,
        // to avoid check_invariants on a non-real table source.
        let config = OptimizerContext::new();
        let rule = NullifRewrite;
        let result = rule.rewrite(plan, &config)?.data;

        // The rewritten Filter's predicate must contain a Case, not a NULLIF call.
        let filter_predicate = match &result {
            LogicalPlan::Filter(Filter { predicate, .. }) => predicate.clone(),
            other => panic!("Expected Filter, got: {other:?}"),
        };

        let pred_str = format!("{filter_predicate:?}");
        assert!(
            !pred_str.to_lowercase().contains("nullif("),
            "NULLIF should have been rewritten; predicate:\n{pred_str}"
        );
        assert!(
            pred_str.contains("CASE") || pred_str.contains("Case"),
            "Expected CASE expression after rewrite; predicate:\n{pred_str}"
        );

        Ok(())
    }
}
