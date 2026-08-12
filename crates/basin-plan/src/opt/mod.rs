//! The logical optimizer.
//!
//! Rules are applied to a fixpoint. DataFusion 53 runs its rule list up to
//! `max_passes = 3`, and the ablation in
//! `docs/migration/df-removal/05-optimizer-rules.md` found that plans are
//! genuine fixpoints rather than single-pass results — so the driver is
//! required in the first increment, not a later refinement.

pub mod decorrelate;
pub mod driver;
pub mod limit;
pub mod projection;
pub mod pushdown;
pub mod simplify;

pub use driver::{optimize, OptimizerRule};

use crate::LogicalPlan;

/// The default rule pipeline, in the order it is applied.
///
/// Order is not cosmetic. The ablation in
/// `docs/migration/df-removal/05-optimizer-rules.md` measured which rules the
/// published benchmark numbers actually depend on, and two findings determine
/// this sequence:
///
/// - **Decorrelation runs early.** It rewrites subqueries into joins, and every
///   rule after it then sees a plan with more structure to work on. Running it
///   late wastes every pass before it.
/// - **Projection pruning runs last.** It computes the columns each node
///   actually needs, so it must see the final shape. Pruning before a rule that
///   introduces a column reference — decorrelation does exactly that when it
///   builds a join condition — would drop a column that is about to be used.
///
/// Filter pushdown sits between: after decorrelation, so it can push through
/// the joins that produced, and before projection pruning, so the predicates it
/// lands in a scan count toward what that scan must read.
///
/// `SimplifyExpressions` is absent because it is not written yet. That is a gap
/// rather than a choice — the ablation found it foundational, since later rules
/// assume simplified predicates and an unfolded `BETWEEN` or `CAST` cannot
/// become a storage `Predicate`. Its slot is first when it lands.
pub fn default_rules() -> Vec<Box<dyn OptimizerRule>> {
    vec![
        Box::new(decorrelate::Decorrelate),
        Box::new(pushdown::FilterPushdown),
        Box::new(limit::LimitPushdown),
        Box::new(projection::ProjectionPruning),
    ]
}

/// Optimize `plan` with [`default_rules`], to a fixpoint.
pub fn optimize_default(plan: LogicalPlan) -> (LogicalPlan, usize) {
    let rules = default_rules();
    let refs: Vec<&dyn OptimizerRule> = rules.iter().map(|r| r.as_ref()).collect();
    optimize(plan, &refs)
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use crate::{ColId, Datum, Expr, LogicalPlan, SnapshotId, TableId};

    fn scan(projection: Vec<ColId>, filters: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(1),
            projection,
            filters,
            snapshot: SnapshotId(0),
        }
    }

    /// Every rule in the default set must be reachable and must not panic on an
    /// ordinary plan. Before this pipeline existed the rules were written,
    /// tested in isolation, and assembled nowhere — so nothing ever ran them
    /// together and an interaction between two of them could not be noticed.
    #[test]
    fn the_default_pipeline_runs_every_rule_without_panicking() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Literal(Datum::Bool(true), basin_pgtype::PgType::BOOL),
            }),
            exprs: vec![],
        };
        let (_out, passes) = optimize_default(plan);
        assert!(passes <= driver::MAX_PASSES);
    }

    /// A plan with nothing to do must converge immediately. A rule reporting a
    /// change it did not make shows up here as a non-zero pass count, and would
    /// cost a full extra pass on every real query.
    #[test]
    fn a_plan_with_nothing_to_optimize_converges_in_zero_passes() {
        let (_out, passes) = optimize_default(scan(vec![], vec![]));
        assert_eq!(
            passes, 0,
            "no rule should claim a change on an already-minimal scan"
        );
    }

    /// Projection pruning must be LAST: it computes required columns from the
    /// final plan shape, so a rule running after it could introduce a reference
    /// to a column it has already pruned away.
    #[test]
    fn projection_pruning_runs_last() {
        let names: Vec<&str> = default_rules().iter().map(|r| r.name()).collect();
        assert!(names.contains(&"push_down_filter"));
        // Basin's own name — deliberately not DataFusion's "optimize_projections",
        // which is what I assumed writing this test and had to correct.
        assert_eq!(*names.last().unwrap(), "projection_pruning");
    }
}
