//! The rule driver.
//!
//! Applies rules in order, repeatedly, until the plan stops changing or a pass
//! budget is exhausted. DataFusion 53 runs `max_passes = 3` and the ablation in
//! `docs/migration/df-removal/05-optimizer-rules.md` confirmed its plans are
//! genuine fixpoints — a rule frequently only becomes applicable after another
//! has fired. That makes the driver part of increment one rather than a later
//! refinement.

use crate::LogicalPlan;

/// A logical rewrite rule.
pub trait OptimizerRule {
    /// Stable name, for `EXPLAIN` and for the ablation harness that checks
    /// which rules a benchmark shape actually depends on.
    fn name(&self) -> &'static str;

    /// Rewrite `plan`, returning `Some` only if something changed.
    ///
    /// Returning `Some` for an unchanged plan is not a correctness bug but it
    /// defeats fixpoint detection and costs a full extra pass, so rules are
    /// expected to be honest about it.
    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan>;
}

/// How many times the whole rule list may be re-applied.
///
/// Matches DataFusion 53's `max_passes`. A plan that has not converged after
/// this many passes is returned as-is rather than looped on: a non-converging
/// rule set is a bug, and hanging is a worse way to report it than producing a
/// valid, if unpolished, plan.
pub const MAX_PASSES: usize = 3;

/// Apply `rules` to a fixpoint, at most [`MAX_PASSES`] times.
///
/// Returns the optimized plan and the number of passes that made progress —
/// the count is what the ablation harness uses to prove a rule mattered.
pub fn optimize(plan: LogicalPlan, rules: &[&dyn OptimizerRule]) -> (LogicalPlan, usize) {
    let mut current = plan;
    let mut productive = 0;

    for _ in 0..MAX_PASSES {
        let mut changed = false;
        for rule in rules {
            if let Some(next) = rule.rewrite(&current) {
                current = next;
                changed = true;
            }
        }
        if !changed {
            break;
        }
        productive += 1;
    }

    (current, productive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, SnapshotId, TableId};

    fn scan(projection: Vec<ColId>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(1),
            projection,
            filters: vec![],
            snapshot: SnapshotId(0),
        }
    }

    /// A rule that drops one column each time it fires, so convergence is
    /// observable.
    struct DropOneColumn;
    impl OptimizerRule for DropOneColumn {
        fn name(&self) -> &'static str {
            "drop_one_column"
        }
        fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
            let LogicalPlan::Scan {
                table,
                projection,
                filters,
                snapshot,
            } = plan
            else {
                return None;
            };
            if projection.is_empty() {
                return None; // honest: nothing changed
            }
            let mut p = projection.clone();
            p.pop();
            Some(LogicalPlan::Scan {
                table: *table,
                projection: p,
                filters: filters.clone(),
                snapshot: *snapshot,
            })
        }
    }

    /// A rule that always reports a change without making one. Exists to prove
    /// the pass budget bounds it rather than the driver spinning.
    struct NeverConverges;
    impl OptimizerRule for NeverConverges {
        fn name(&self) -> &'static str {
            "never_converges"
        }
        fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
            Some(plan.clone())
        }
    }

    #[test]
    fn stops_as_soon_as_no_rule_fires() {
        let (out, passes) = optimize(scan(vec![]), &[&DropOneColumn]);
        assert_eq!(passes, 0, "nothing to do means no productive pass");
        assert_eq!(out, scan(vec![]));
    }

    #[test]
    fn reaches_a_fixpoint_across_passes() {
        // Two columns needs two firings, which the single-pass loop cannot do.
        let (out, passes) = optimize(scan(vec![ColId(0), ColId(1)]), &[&DropOneColumn]);
        assert_eq!(out, scan(vec![]));
        assert_eq!(passes, 2);
    }

    #[test]
    fn a_non_converging_rule_is_bounded_not_hung() {
        let (_, passes) = optimize(scan(vec![ColId(0)]), &[&NeverConverges]);
        assert_eq!(passes, MAX_PASSES);
    }
}
