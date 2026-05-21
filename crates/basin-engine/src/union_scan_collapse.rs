//! Optimizer rule: `Union(Filter(Scan t, A), Filter(Scan t, B))` →
//! `Filter(Scan t, A OR B)`.
//!
//! When multiple `UNION ALL` branches scan the **same** table and each branch
//! is at most a single `Filter` over a `TableScan` (with optional transparent
//! column-only `Projection` wrappers), we can collapse them into one scan
//! whose predicate is the disjunction of all branch predicates.  This
//! eliminates duplicate file reads that would otherwise be issued for the same
//! table.
//!
//! A branch with *no* filter subsumes all other filtered branches for the same
//! table (the unfiltered scan already returns every row), so the merged result
//! is also unfiltered.
//!
//! The rule bails conservatively when any branch contains `Aggregate`, `Limit`,
//! `Sort`, `Distinct`, `Window`, or `Join` nodes, or when branches over the
//! same table have differing output schemas (mismatched projection shapes).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Result, tree_node::{Transformed, TreeNode}};
use datafusion::logical_expr::{
    Expr, Filter, LogicalPlan, Projection,
    logical_plan::Union,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` when the plan subtree contains any node that makes it unsafe
/// to collapse the branch (aggregates, limits, sorts, distinct, windows, joins).
fn contains_blocking_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Join(_) => true,

        LogicalPlan::Filter(f) => contains_blocking_node(&f.input),
        LogicalPlan::Projection(p) => contains_blocking_node(&p.input),
        LogicalPlan::TableScan(_) => false,

        // Anything else is unknown — bail.
        _ => true,
    }
}

/// Describes what we found by peeling a branch's transparent wrapper layers.
#[derive(Debug)]
struct BranchInfo {
    /// Table name used as grouping key.
    table_name: String,
    /// The original `TableScan` plan (shared reference).
    scan: Arc<LogicalPlan>,
    /// Predicate from the `Filter` wrapper, if any.
    predicate: Option<Expr>,
}

/// Try to parse a `Union` input branch as `[Projection*] [Filter] TableScan`.
///
/// Returns `None` if:
/// * the branch does not end in a `TableScan` through only Filter/Projection
/// * the branch contains a blocking node
fn parse_branch(plan: &Arc<LogicalPlan>) -> Option<BranchInfo> {
    if contains_blocking_node(plan) {
        return None;
    }

    // Walk through outer column-only Projections (transparent wrappers).
    let mut cursor: &Arc<LogicalPlan> = plan;

    // Peel optional outermost Projection.
    while let LogicalPlan::Projection(p) = cursor.as_ref() {
        // Only allow pass-through column-only projections.
        let all_columns = p.expr.iter().all(|e| matches!(e, Expr::Column(_)));
        if !all_columns {
            return None;
        }
        cursor = &p.input;
    }

    // Now expect either Filter or TableScan.
    match cursor.as_ref() {
        LogicalPlan::TableScan(ts) => Some(BranchInfo {
            table_name: ts.table_name.to_string(),
            scan: Arc::clone(cursor),
            predicate: None,
        }),

        LogicalPlan::Filter(f) => {
            // Peel any intermediate Projections between Filter and Scan.
            let mut inner = &f.input;
            while let LogicalPlan::Projection(p) = inner.as_ref() {
                let all_columns = p.expr.iter().all(|e| matches!(e, Expr::Column(_)));
                if !all_columns {
                    return None;
                }
                inner = &p.input;
            }

            match inner.as_ref() {
                LogicalPlan::TableScan(ts) => Some(BranchInfo {
                    table_name: ts.table_name.to_string(),
                    scan: Arc::clone(inner),
                    predicate: Some(f.predicate.clone()),
                }),
                _ => None,
            }
        }

        _ => None,
    }
}

/// Collapse groups of branches that share the same `TableScan`.
///
/// Returns `None` if no collapsing is possible (no group has ≥ 2 members).
fn try_collapse(union: Union) -> Result<Transformed<LogicalPlan>> {
    let Union { inputs, schema } = union;

    // Parse every branch; bail if any branch is un-parseable.
    let mut infos: Vec<BranchInfo> = Vec::with_capacity(inputs.len());
    for arc in &inputs {
        match parse_branch(arc) {
            Some(info) => infos.push(info),
            None => {
                // Cannot collapse — return original unchanged.
                return Ok(Transformed::no(LogicalPlan::Union(Union { inputs, schema })));
            }
        }
    }

    // Group by table_name to find collapsible groups.
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, info) in infos.iter().enumerate() {
        groups.entry(info.table_name.clone()).or_default().push(idx);
    }

    // Check that at least one group has ≥ 2 branches.
    let any_collapsible = groups.values().any(|v| v.len() >= 2);
    if !any_collapsible {
        return Ok(Transformed::no(LogicalPlan::Union(Union { inputs, schema })));
    }

    // Verify that branches sharing a table have identical output schemas.
    for indices in groups.values() {
        if indices.len() < 2 {
            continue;
        }
        let first_schema = inputs[indices[0]].schema();
        for &idx in &indices[1..] {
            if inputs[idx].schema() != first_schema {
                return Ok(Transformed::no(LogicalPlan::Union(Union { inputs, schema })));
            }
        }
    }

    // Build the merged plan list.
    // For each group with ≥ 2 members, replace them with a single merged node.
    // Track which original indices have been consumed.
    let mut consumed = vec![false; inputs.len()];
    let mut new_inputs: Vec<Arc<LogicalPlan>> = Vec::new();

    // Process groups in a deterministic order (sort keys).
    let mut sorted_keys: Vec<String> = groups.keys().cloned().collect();
    sorted_keys.sort();

    // We need the original index order preserved for non-collapsed inputs.
    // Build a set of (first_index_in_group → merged_plan) entries.
    let mut merged_at: HashMap<usize, Arc<LogicalPlan>> = HashMap::new();

    for key in &sorted_keys {
        let indices = &groups[key];
        if indices.len() < 2 {
            continue;
        }
        // Compute the first index where this group appears in original order.
        let first_idx = *indices.iter().min().unwrap();

        // Collect predicates in original order.
        let mut predicates: Vec<Option<Expr>> = Vec::new();
        for &idx in indices {
            predicates.push(infos[idx].predicate.clone());
        }

        // Safety: do NOT collapse when all predicates are identical.
        // `P OR P = P`, which is semantically correct as a predicate but
        // wrong for UNION ALL row-count semantics: two branches with the same
        // filter both produce the same rows, and UNION ALL must return them
        // twice. Collapsing would return them once. This is the RLS bug:
        // `SELECT t FROM tbl UNION ALL SELECT t FROM tbl` with identical RLS
        // filters on both arms must return 2 rows, not 1.
        let all_predicates_identical = predicates.windows(2).all(|w| {
            match (&w[0], &w[1]) {
                (Some(a), Some(b)) => format!("{a:?}") == format!("{b:?}"),
                (None, None) => true,
                _ => false,
            }
        });
        if all_predicates_identical {
            // Skip collapsing this group — leave the branches as-is.
            continue;
        }

        let scan = Arc::clone(&infos[first_idx].scan);

        // If any branch is unfiltered, the merged result is also unfiltered.
        let merged_plan: Arc<LogicalPlan> = if predicates.iter().any(|p| p.is_none()) {
            scan
        } else {
            // Fold with OR.
            let combined = predicates
                .into_iter()
                .flatten()
                .reduce(|acc, p| acc.or(p))
                .expect("non-empty group always produces a predicate");

            let filter = Filter::try_new(combined, scan)?;
            Arc::new(LogicalPlan::Filter(filter))
        };

        // Re-wrap with the original projection if the first branch had one.
        //
        // DataFusion's optimizer asserts that a rule preserves the output
        // schema of the node it rewrites.  When each Union branch is
        // `Projection([id], Filter(Scan t, pred))`, collapsing to
        // `Filter(Scan t, pred_a OR pred_b)` widens the schema from
        // `{id}` to all columns — triggering the invariant check and
        // failing with a schema-mismatch error.
        //
        // If `inputs[first_idx]` has a narrower schema than `merged_plan`
        // (the full-table scan), that narrowing came from an outer
        // Projection in the original branch.  We restore it here so the
        // final plan still outputs only the columns the query requested.
        //
        // `Projection::new_from_schema` is used (rather than `try_new`)
        // because it preserves the exact output qualifier from the branch
        // schema (e.g. `qualifier: None` for an unqualified `id`).  A plain
        // `try_new` with unqualified column refs would inherit the qualifier
        // from the input scan (`Some(Bare{"tp"})`), causing a second schema
        // mismatch.
        let merged_plan: Arc<LogicalPlan> = {
            let branch_schema = Arc::clone(inputs[first_idx].schema());
            if *branch_schema != **merged_plan.schema() {
                let proj = Projection::new_from_schema(merged_plan, branch_schema);
                Arc::new(LogicalPlan::Projection(proj))
            } else {
                merged_plan
            }
        };

        // Mark all group members as consumed.
        for &idx in indices {
            consumed[idx] = true;
        }

        merged_at.insert(first_idx, merged_plan);
    }

    // Reconstruct output list in original index order.
    for (idx, arc) in inputs.iter().enumerate() {
        if consumed[idx] {
            // Either this index is the "first" of a merged group or a later
            // member that was folded into it.
            if let Some(merged) = merged_at.remove(&idx) {
                new_inputs.push(merged);
            }
            // else: later member of a group — already represented by the first.
        } else {
            new_inputs.push(Arc::clone(arc));
        }
    }

    // If we collapsed everything to a single input, unwrap the Union entirely.
    if new_inputs.len() == 1 {
        let plan = Arc::try_unwrap(new_inputs.remove(0))
            .unwrap_or_else(|arc| (*arc).clone());
        return Ok(Transformed::yes(plan));
    }

    // Otherwise rebuild the Union with the reduced input list.
    let new_union = Union::try_new_with_loose_types(new_inputs)?;
    Ok(Transformed::yes(LogicalPlan::Union(new_union)))
}

// ── rule ─────────────────────────────────────────────────────────────────────

/// Collapses `Union(Filter(Scan t, A), Filter(Scan t, B))` into
/// `Filter(Scan t, A OR B)` when all branches read the same `TableScan`.
#[derive(Debug, Default)]
pub(crate) struct UnionScanCollapse;

impl OptimizerRule for UnionScanCollapse {
    fn name(&self) -> &str {
        "union_scan_collapse"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|node: LogicalPlan| match node {
            LogicalPlan::Union(union) => try_collapse(union),
            other => Ok(Transformed::no(other)),
        })
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::{
        lit,
        logical_plan::builder::LogicalTableSource,
        Expr, LogicalPlanBuilder, Operator,
        expr::BinaryExpr,
    };
    use datafusion::optimizer::{OptimizerContext, OptimizerRule};

    /// Build a simple in-memory table scan for testing.
    fn make_scan(table: &str, fields: Vec<(&str, DataType)>) -> LogicalPlan {
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let source = Arc::new(LogicalTableSource::new(Arc::clone(&schema)));
        LogicalPlanBuilder::scan(table, source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    // ── positive test ─────────────────────────────────────────────────────────

    /// `UNION ALL` of two filtered scans over the same table should collapse to
    /// a single `Filter(Scan t, pred_a OR pred_b)`.
    #[test]
    fn union_two_filters_same_table_collapses() -> Result<()> {
        let scan_a = make_scan("t", vec![("id", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = make_scan("t", vec![("id", DataType::Int64), ("v", DataType::Int64)]);

        let pred_a = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("id"))),
            op: Operator::Lt,
            right: Box::new(lit(10i64)),
        });
        let pred_b = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("id"))),
            op: Operator::GtEq,
            right: Box::new(lit(10i64)),
        });

        let branch_a = LogicalPlanBuilder::from(scan_a)
            .filter(pred_a.clone())?
            .build()?;
        let branch_b = LogicalPlanBuilder::from(scan_b)
            .filter(pred_b.clone())?
            .build()?;

        let union_plan = LogicalPlanBuilder::from(branch_a)
            .union(branch_b)?
            .build()?;

        let rule = UnionScanCollapse;
        let config = OptimizerContext::new();
        let result = rule.rewrite(union_plan, &config)?.data;

        // Result must NOT be a Union.
        assert!(
            !matches!(result, LogicalPlan::Union(_)),
            "Expected Union to be collapsed, got: {:?}",
            result
        );

        // Result must be a Filter over a TableScan.
        let LogicalPlan::Filter(filter) = &result else {
            panic!("Expected Filter plan, got: {:?}", result);
        };

        assert!(
            matches!(filter.input.as_ref(), LogicalPlan::TableScan(_)),
            "Expected TableScan under Filter"
        );

        // Predicate must be a BinaryExpr with OR.
        let pred_str = format!("{:?}", filter.predicate);
        assert!(
            pred_str.contains("Or"),
            "Expected OR predicate, got: {}",
            pred_str
        );

        Ok(())
    }

    // ── negative test ─────────────────────────────────────────────────────────

    /// `UNION ALL` of scans from different tables must be left unchanged.
    #[test]
    fn union_different_tables_is_unchanged() -> Result<()> {
        let scan_t1 = make_scan("t1", vec![("id", DataType::Int64)]);
        let scan_t2 = make_scan("t2", vec![("id", DataType::Int64)]);

        let union_plan = LogicalPlanBuilder::from(scan_t1)
            .union(scan_t2)?
            .build()?;

        let rule = UnionScanCollapse;
        let config = OptimizerContext::new();
        let result = rule.rewrite(union_plan.clone(), &config)?.data;

        // Must remain a Union since the tables differ.
        assert!(
            matches!(result, LogicalPlan::Union(_)),
            "Expected Union to be unchanged, got: {:?}",
            result
        );

        Ok(())
    }
}
