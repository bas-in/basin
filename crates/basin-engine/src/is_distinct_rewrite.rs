//! LogicalPlan-level rewrites that run at optimizer position 0 (before any
//! of DataFusion's stock rules). Two rewrites live here:
//!
//! 1. `IS [NOT] DISTINCT FROM` → plain logical predicates Vortex pushes down.
//! 2. `DISTINCT ON` → a leaner grouped `first_value` aggregate than the one
//!    DataFusion's stock `ReplaceDistinctWithAggregate` rule produces.
//!
//! # 1. `IS [NOT] DISTINCT FROM`
//!
//! DataFusion 53 represents `a IS DISTINCT FROM b` and `a IS NOT DISTINCT
//! FROM b` as `Expr::BinaryExpr` with `Operator::IsDistinctFrom` /
//! `Operator::IsNotDistinctFrom`.  Vortex's `DefaultExpressionConvertor`
//! doesn't push either operator, so they fall back to a residual filter
//! over a fully-decoded column.
//!
//! Semantics (SQL three-valued logic).  Both operators are **total**: they
//! return TRUE or FALSE and never NULL, for every combination of operands.
//! Measured against PostgreSQL 18.2 in a *value* position (`SELECT a IS
//! DISTINCT FROM b`, where NULL and FALSE are distinguishable):
//!
//! | a    | b    | `IS DISTINCT FROM` | `IS NOT DISTINCT FROM` |
//! |------|------|--------------------|------------------------|
//! | NULL | NULL | false              | true                   |
//! | NULL | 1    | true               | false                  |
//! | 1    | NULL | true               | false                  |
//! | 1    | 1    | false              | true                   |
//! | 1    | 2    | true               | false                  |
//!
//! The rewrite must reproduce that table exactly:
//!
//!   * `a IS DISTINCT FROM b` ≡
//!         `(a IS NULL AND b IS NOT NULL)
//!          OR (a IS NOT NULL AND b IS NULL)
//!          OR (a IS NOT NULL AND b IS NOT NULL AND a != b)`
//!
//!   * `a IS NOT DISTINCT FROM b` ≡
//!         `(a IS NULL AND b IS NULL)
//!          OR (a IS NOT NULL AND b IS NOT NULL AND a = b)`
//!
//! ## Why the last arm carries an explicit NOT-NULL guard
//!
//! A bare `a != b` / `a = b` arm evaluates to **NULL**, not FALSE, when
//! either operand is NULL, and `FALSE OR NULL` is NULL.  Without the guard
//! the rewrites returned NULL where PostgreSQL returns FALSE:
//!
//! * `NULL IS DISTINCT FROM NULL` — every arm of the un-guarded three-arm
//!   OR is `FALSE, FALSE, NULL` → NULL; PostgreSQL says **false**.
//! * `NULL IS NOT DISTINCT FROM 1` — `FALSE OR NULL` → NULL; PostgreSQL
//!   says **false**.
//!
//! Both errors are invisible in a `WHERE` or `JOIN … ON` position, which
//! treats NULL and FALSE identically, and visible in a value position.
//! The guarded arms above were verified cell-by-cell against the live
//! PostgreSQL 18.2 server and match all ten cells of the table.
//!
//! Each clause is a simple operator Vortex's converter accepts (`IsNull`,
//! `IsNotNull`, `BinaryExpr { Eq | NotEq }`), and the outer AND/OR are also
//! supported.  Pushing the full predicate down avoids the residual
//! filter's full-column decode.
//!
//! # 2. `DISTINCT ON` lowering
//!
//! DataFusion's stock `ReplaceDistinctWithAggregate` rule lowers
//!
//! ```sql
//! SELECT DISTINCT ON (user_id) user_id, id, amount, created_at
//! FROM events ORDER BY user_id, created_at DESC
//! ```
//!
//! into one `first_value(<col> ORDER BY user_id, created_at DESC)` aggregate
//! per select column — including the ON column itself — grouped by the ON
//! columns. Each ordered `first_value` runs a `FirstPrimitiveGroupsAccumulator`
//! that keeps one `Vec<ScalarValue>` ordering row per group and runs a
//! `LexicographicalComparator` over **all** ORDER BY columns per candidate
//! row. Two of those costs are pure waste:
//!
//! * the ORDER BY prefix (`user_id`) is one of the GROUP BY expressions, so
//!   it is constant within every group — comparing it per row and storing a
//!   `ScalarValue` of it per group per aggregate buys nothing;
//! * `first_value(user_id ORDER BY …)` over a group keyed by `user_id` is
//!   just the group key itself — a whole aggregate column of overhead.
//!
//! This rule produces instead:
//!
//! ```text
//! Projection: user_id, first_value(id ORDER BY created_at DESC) AS id, …
//!   Sort: user_id ASC
//!     Aggregate: groupBy=[user_id],
//!                aggr=[first_value(id) ORDER BY [created_at DESC], …]
//! ```
//!
//! i.e. the ON columns are projected straight from the group keys and every
//! remaining `first_value` orders by only the non-key ORDER BY tail. On the
//! 1M-row Postgres-compare card this roughly halves the per-row comparator
//! work and removes one of four aggregates.
//!
//! ## Equivalence proof (bit-identical output, ties included)
//!
//! * Dropping ORDER BY entries that syntactically equal a GROUP BY
//!   expression: within any group all rows share the same value of every
//!   group expression (GROUP BY treats NULLs as equal, so this includes NULL
//!   keys), hence those sort columns are constant inside the group and the
//!   lexicographic winner is decided entirely by the remaining tail — same
//!   winner row as before.
//! * Replacing `first_value(on_col ORDER BY …)` with the group key: the
//!   expression is constant within the group, so its first value is the
//!   group key no matter which row wins.
//! * The outer `Sort` and the aliasing `Projection` are copied verbatim from
//!   the stock rule (sort on the first `on_expr.len()` ORDER BY entries,
//!   project back to the original `DistinctOn` schema names).
//!
//! Row coherence is therefore exactly the same as the stock lowering: every
//! `first_value` in one query shares the same ordering tail, sees batches in
//! the same order, and applies the same strict-improvement update rule, so
//! all of them pick the same winning row per group (PostgreSQL itself leaves
//! the winner unspecified when the ORDER BY tail has duplicates).
//!
//! ## When the rule declines (falls through to the stock lowering)
//!
//! * No ORDER BY: nothing to trim and `first_value` without ORDER BY has no
//!   `GroupsAccumulator` fast path — rewriting would regress.
//! * ORDER BY covers only the ON columns (empty tail): same reason.
//! * The leading ORDER BY entries do not match the ON expressions (cannot
//!   happen via `DistinctOn::try_new`, which validates the prefix, but the
//!   fields are `pub` so we re-verify defensively).

use datafusion::common::{
    tree_node::{Transformed, TreeNode},
    Column, Result,
};
use datafusion::functions_aggregate::first_last::first_value;
use datafusion::logical_expr::expr_rewriter::{normalize_cols, normalize_sorts};
use datafusion::logical_expr::{
    col, Aggregate, BinaryExpr, Distinct, DistinctOn, Expr, LogicalPlan, LogicalPlanBuilder,
    Operator,
};
use datafusion::optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule};

/// Rewrites `a IS DISTINCT FROM b` / `a IS NOT DISTINCT FROM b` to pushable
/// conjunctions/disjunctions of `IS NULL`, `IS NOT NULL`, and `= / !=`
/// predicates, and lowers `DISTINCT ON` to a trimmed grouped `first_value`
/// aggregate (see module docs).
///
/// Registered at optimizer position 0 so the `IS DISTINCT` rewrite fires
/// before `SimplifyExpressions` can fold the operator into something opaque,
/// and so the `DISTINCT ON` lowering wins the race against DataFusion's
/// stock `ReplaceDistinctWithAggregate` rule.
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
        match plan {
            LogicalPlan::Distinct(Distinct::On(distinct_on)) => lower_distinct_on(distinct_on),
            plan => plan.map_expressions(|expr: Expr| expr.transform_up(rewrite_is_distinct_expr)),
        }
    }
}

/// Lower a `DistinctOn` node to `Projection ← Sort ← Aggregate` with the ON
/// columns projected from the group keys and per-aggregate ORDER BY trimmed
/// to the non-key tail. Returns the node untransformed when the optimized
/// form does not apply (DataFusion's stock rule then lowers it instead).
fn lower_distinct_on(node: DistinctOn) -> Result<Transformed<LogicalPlan>> {
    let expr_cnt = node.on_expr.len();

    // Fire only when there is an ORDER BY whose leading entries are exactly
    // the ON expressions (the `DistinctOn::try_new` invariant) AND a non-empty
    // ordering tail remains after them. Without a tail, the trimmed
    // `first_value` would have no ORDER BY and would lose its
    // GroupsAccumulator fast path — worse than the stock lowering.
    let applies = expr_cnt > 0
        // One select expression per output field (no un-expanded wildcards) —
        // the projection below maps them 1:1.
        && node.select_expr.len() == node.schema.fields().len()
        && match &node.sort_expr {
            Some(sort) => {
                sort.len() > expr_cnt
                    && sort[..expr_cnt]
                        .iter()
                        .zip(node.on_expr.iter())
                        .all(|(s, o)| &s.expr == o)
            }
            None => false,
        };
    if !applies {
        return Ok(Transformed::no(LogicalPlan::Distinct(Distinct::On(node))));
    }

    let DistinctOn {
        on_expr,
        select_expr,
        sort_expr,
        input,
        schema,
    } = node;
    let sort = sort_expr.expect("checked Some above");

    // Normalize all column references against the input (idempotent — the
    // sort expressions are already normalized by `DistinctOn::with_sort_expr`).
    let group_expr = normalize_cols(on_expr, input.as_ref())?;
    let select_expr = normalize_cols(select_expr, input.as_ref())?;
    let sort = normalize_sorts(sort, input.as_ref())?;

    // The non-key ordering tail shared by every first_value aggregate.
    let tail = sort[expr_cnt..].to_vec();

    // Map each select expression to either a group-key slot (projected
    // directly) or a first_value aggregate slot.
    enum Slot {
        /// Index into the aggregate's group columns.
        Key(usize),
        /// Index into the aggregate's aggregate columns.
        Agg(usize),
    }
    let mut slots = Vec::with_capacity(select_expr.len());
    let mut aggr_expr = Vec::new();
    for e in select_expr {
        if let Some(p) = group_expr.iter().position(|g| g == &e) {
            slots.push(Slot::Key(p));
        } else {
            slots.push(Slot::Agg(aggr_expr.len()));
            aggr_expr.push(first_value(e, tail.clone()));
        }
    }

    let group_cnt = group_expr.len();
    let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(input, group_expr, aggr_expr)?);

    // Aggregate output layout: group columns first, then aggregate columns.
    let agg_columns: Vec<Column> = aggregate
        .schema()
        .iter()
        .map(|(qualifier, field)| Column::from((qualifier, field)))
        .collect();

    // Mirror the stock rule: sorting by the ON-column prefix alone is enough
    // because the group keys are unique in the aggregate output (any further
    // ORDER BY entries can never break a tie that cannot exist).
    let outer_sort = sort[..expr_cnt].to_vec();

    // Project back to the original DistinctOn output schema (qualifiers and
    // field names), exactly like the stock rule does.
    let project_expr: Vec<Expr> = slots
        .iter()
        .zip(schema.iter())
        .map(|(slot, (old_qualifier, old_field))| {
            let idx = match slot {
                Slot::Key(p) => *p,
                Slot::Agg(i) => group_cnt + *i,
            };
            col(agg_columns[idx].clone()).alias_qualified(old_qualifier.cloned(), old_field.name())
        })
        .collect();

    let plan = LogicalPlanBuilder::from(aggregate)
        .sort(outer_sort)?
        .project(project_expr)?
        .build()?;
    Ok(Transformed::yes(plan))
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
            //   OR (a IS NOT NULL AND b IS NOT NULL AND a != b)
            //
            // The third arm's NOT-NULL guard is load-bearing: a bare
            // `a != b` is NULL (not FALSE) when either side is NULL, and
            // `FALSE OR NULL` is NULL — so `NULL IS DISTINCT FROM NULL`
            // would yield NULL where PostgreSQL yields FALSE.
            let lhs = a.clone().is_null().and(b.clone().is_not_null());
            let mid = a.clone().is_not_null().and(b.clone().is_null());
            let rhs = a
                .clone()
                .is_not_null()
                .and(b.clone().is_not_null())
                .and(a.not_eq(b));
            Ok(Transformed::yes(lhs.or(mid).or(rhs)))
        }
        Operator::IsNotDistinctFrom => {
            let a = (**left).clone();
            let b = (**right).clone();
            // (a IS NULL AND b IS NULL)
            //   OR (a IS NOT NULL AND b IS NOT NULL AND a = b)
            //
            // Same guard, same reason: a bare `a = b` is NULL when either
            // side is NULL, so `NULL IS NOT DISTINCT FROM 1` would yield
            // NULL where PostgreSQL yields FALSE.
            let lhs = a.clone().is_null().and(b.clone().is_null());
            let rhs = a
                .clone()
                .is_not_null()
                .and(b.clone().is_not_null())
                .and(a.eq(b));
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

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{
        col, BinaryExpr, Expr, Filter, LogicalPlan, LogicalPlanBuilder, Operator,
    };
    use datafusion::optimizer::OptimizerContext;
    use std::sync::Arc;

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

        let result = IsDistinctRewrite
            .rewrite(plan, &OptimizerContext::new())?
            .data;
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

        let result = IsDistinctRewrite
            .rewrite(plan, &OptimizerContext::new())?
            .data;
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

    // ── value-position truth table ──────────────────────────────────────────

    /// Evaluate `rewrite_is_distinct_expr(a OP b)` in a *value* position over
    /// the five-row operand table and return one `Option<bool>` per row.
    ///
    /// A `WHERE`/`JOIN` position cannot see the difference between NULL and
    /// FALSE, so a projection is the only position that pins these semantics.
    async fn eval_rewritten(op: Operator) -> Result<Vec<Option<bool>>> {
        use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        // a    | b
        // NULL | NULL
        // NULL | 1
        // 1    | NULL
        // 1    | 1
        // 1    | 2
        let a = Int64Array::from(vec![None, None, Some(1), Some(1), Some(1)]);
        let b = Int64Array::from(vec![None, Some(1), None, Some(1), Some(2)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)])?;

        let rewritten = rewrite_is_distinct_expr(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op,
            right: Box::new(col("b")),
        }))?;
        assert!(rewritten.transformed, "{op:?} must be rewritten");

        let ctx = SessionContext::new();
        ctx.register_batch("p", batch)?;
        // Sort to keep row order deterministic across partitioning changes —
        // but ASC NULLS FIRST explicitly, not `sort_by`'s default. The default
        // is NULLS LAST, which reorders these five rows to
        // (1,1),(1,2),(1,NULL),(NULL,1),(NULL,NULL) while the expectations
        // below are labelled in the *input* order. That mismatch made both of
        // these tests fail against a correct rewrite.
        let batches = ctx
            .table("p")
            .await?
            .select(vec![
                col("a"),
                col("b"),
                rewritten.data.alias("r"),
            ])?
            .sort(vec![
                col("a").sort(true, true),
                col("b").sort(true, true),
            ])?
            .collect()
            .await?;

        let mut out = Vec::new();
        for batch in &batches {
            let arr = batch
                .column(2)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean result column");
            for i in 0..arr.len() {
                out.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                });
            }
        }
        Ok(out)
    }

    /// `a IS DISTINCT FROM b` is total — never NULL. Recorded from live
    /// PostgreSQL 18.2; rows ordered by (a, b) NULLS FIRST:
    /// (NULL,NULL)=false, (NULL,1)=true, (1,NULL)=true, (1,1)=false, (1,2)=true.
    ///
    /// Before the NOT-NULL guard on the third arm, row (NULL, NULL) evaluated
    /// to NULL here — invisible in `WHERE`, wrong in a projection.
    #[tokio::test]
    async fn is_distinct_from_value_position_matches_postgres() -> Result<()> {
        assert_eq!(
            eval_rewritten(Operator::IsDistinctFrom).await?,
            vec![
                Some(false), // NULL, NULL
                Some(true),  // NULL, 1
                Some(true),  // 1, NULL
                Some(false), // 1, 1
                Some(true),  // 1, 2
            ]
        );
        Ok(())
    }

    /// `a IS NOT DISTINCT FROM b` is the exact negation, also never NULL.
    /// Before the guard, (NULL,1) and (1,NULL) evaluated to NULL where
    /// PostgreSQL returns false.
    #[tokio::test]
    async fn is_not_distinct_from_value_position_matches_postgres() -> Result<()> {
        assert_eq!(
            eval_rewritten(Operator::IsNotDistinctFrom).await?,
            vec![
                Some(true),  // NULL, NULL
                Some(false), // NULL, 1
                Some(false), // 1, NULL
                Some(true),  // 1, 1
                Some(false), // 1, 2
            ]
        );
        Ok(())
    }

    // ── DISTINCT ON lowering ────────────────────────────────────────────────

    /// Extract the `Aggregate:` line from an indent-formatted plan. The
    /// Projection line echoes aggregate output column NAMES (which contain
    /// `first_value(...)` text), so aggregate-shape assertions must scope to
    /// the Aggregate node line.
    fn aggregate_line(plan: &str) -> String {
        plan.lines()
            .find(|l| l.trim_start().starts_with("Aggregate:"))
            .unwrap_or_else(|| panic!("no Aggregate node in plan:\n{plan}"))
            .to_string()
    }

    /// events(user_id, id, amount, created_at) scan for DISTINCT ON tests.
    fn make_events_scan() -> Arc<LogicalTableSource> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("created_at", DataType::Int64, true),
        ]));
        Arc::new(LogicalTableSource::new(schema))
    }

    /// The benchmark shape: DISTINCT ON (user_id) with ORDER BY
    /// user_id, created_at DESC. Must lower to a grouped first_value plan
    /// where the ON column is projected from the group key (NO
    /// first_value(user_id)) and each aggregate orders by ONLY the non-key
    /// tail (created_at).
    #[test]
    fn distinct_on_lowers_to_trimmed_grouped_first_value() -> Result<()> {
        let plan = LogicalPlanBuilder::scan("t", make_events_scan(), None)?
            .distinct_on(
                vec![col("user_id")],
                vec![col("user_id"), col("id"), col("amount"), col("created_at")],
                Some(vec![
                    col("user_id").sort(true, false),
                    col("created_at").sort(false, true),
                ]),
            )?
            .build()?;
        let original_fields: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?;
        assert!(result.transformed, "DISTINCT ON must be lowered");
        let s = format!("{}", result.data.display_indent());

        // No DistinctOn left; aggregate present.
        assert!(!s.contains("DistinctOn"), "plan:\n{s}");
        let agg = aggregate_line(&s);
        // The ON column must NOT be wrapped in an aggregate.
        assert!(!agg.contains("first_value(t.user_id"), "plan:\n{s}");
        // Exactly the three non-key columns are aggregated.
        assert_eq!(agg.matches("first_value(").count(), 3, "plan:\n{s}");
        // The per-aggregate ORDER BY is trimmed to the non-key tail —
        // `ORDER BY [t.user_id …]` must not appear inside any aggregate.
        assert!(!agg.contains("ORDER BY [t.user_id"), "plan:\n{s}");
        assert!(
            agg.contains("ORDER BY [t.created_at DESC NULLS FIRST]"),
            "plan:\n{s}"
        );
        // The stock rule's outer sort on the ON prefix is preserved.
        assert!(s.contains("Sort: t.user_id ASC NULLS LAST"), "plan:\n{s}");

        // Output schema names are preserved.
        let new_fields: Vec<String> = result
            .data
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(new_fields, original_fields);
        Ok(())
    }

    /// ORDER BY only covers the ON columns (no tail) → no win available and
    /// un-ordered first_value would lose its GroupsAccumulator fast path, so
    /// the node must be left for DataFusion's stock rule.
    #[test]
    fn distinct_on_without_order_tail_left_untouched() -> Result<()> {
        let plan = LogicalPlanBuilder::scan("t", make_events_scan(), None)?
            .distinct_on(
                vec![col("user_id")],
                vec![col("user_id"), col("id")],
                Some(vec![col("user_id").sort(true, false)]),
            )?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?;
        assert!(!result.transformed);
        assert!(
            matches!(result.data, LogicalPlan::Distinct(Distinct::On(_))),
            "node must survive untouched"
        );
        Ok(())
    }

    /// No ORDER BY at all → arbitrary row per group; leave it to the stock
    /// lowering (nothing to trim).
    #[test]
    fn distinct_on_without_order_by_left_untouched() -> Result<()> {
        let plan = LogicalPlanBuilder::scan("t", make_events_scan(), None)?
            .distinct_on(vec![col("user_id")], vec![col("user_id"), col("id")], None)?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?;
        assert!(!result.transformed);
        assert!(matches!(
            result.data,
            LogicalPlan::Distinct(Distinct::On(_))
        ));
        Ok(())
    }

    /// PG allows the ON column to be absent from the select list. The group
    /// key then exists only in the Aggregate; the projection must contain
    /// only the selected (aggregated) columns.
    #[test]
    fn distinct_on_key_not_in_select_list() -> Result<()> {
        let plan = LogicalPlanBuilder::scan("t", make_events_scan(), None)?
            .distinct_on(
                vec![col("user_id")],
                vec![col("id")],
                Some(vec![
                    col("user_id").sort(true, false),
                    col("created_at").sort(false, true),
                ]),
            )?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?;
        assert!(result.transformed);
        let s = format!("{}", result.data.display_indent());
        let agg = aggregate_line(&s);
        assert_eq!(agg.matches("first_value(").count(), 1, "plan:\n{s}");
        assert!(!agg.contains("first_value(t.user_id"), "plan:\n{s}");

        let new_fields: Vec<String> = result
            .data
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(new_fields, vec!["id".to_string()]);
        Ok(())
    }

    /// Multi-column ON list: both keys projected from the group, single
    /// aggregate for the remaining column, tail trimmed past BOTH keys.
    #[test]
    fn distinct_on_multi_key() -> Result<()> {
        let plan = LogicalPlanBuilder::scan("t", make_events_scan(), None)?
            .distinct_on(
                vec![col("user_id"), col("amount")],
                vec![col("user_id"), col("amount"), col("id")],
                Some(vec![
                    col("user_id").sort(true, false),
                    col("amount").sort(true, false),
                    col("created_at").sort(false, true),
                ]),
            )?
            .build()?;

        let result = IsDistinctRewrite.rewrite(plan, &OptimizerContext::new())?;
        assert!(result.transformed);
        let s = format!("{}", result.data.display_indent());
        let agg = aggregate_line(&s);
        assert_eq!(agg.matches("first_value(").count(), 1, "plan:\n{s}");
        assert!(!agg.contains("ORDER BY [t.user_id"), "plan:\n{s}");
        assert!(!agg.contains("ORDER BY [t.amount"), "plan:\n{s}");
        assert!(
            agg.contains("ORDER BY [t.created_at DESC NULLS FIRST]"),
            "plan:\n{s}"
        );

        let new_fields: Vec<String> = result
            .data
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            new_fields,
            vec![
                "user_id".to_string(),
                "amount".to_string(),
                "id".to_string()
            ]
        );
        Ok(())
    }
}
