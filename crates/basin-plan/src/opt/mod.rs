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

use crate::{ColumnRef, LogicalPlan};

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
/// `SimplifyExpressions` runs first, for the reason the ablation gave: later
/// rules assume simplified predicates, and predicate quality decides whether
/// filter pushdown can produce a storage `Predicate` at all.
pub fn default_rules() -> Vec<Box<dyn OptimizerRule>> {
    vec![
        // Foundational: every rule after this assumes simplified predicates,
        // and an unfolded CAST or unexpanded BETWEEN cannot become a storage
        // Predicate, so a filter that would have pruned files silently does not.
        Box::new(simplify::SimplifyExpressions),
        Box::new(decorrelate::Decorrelate),
        Box::new(pushdown::FilterPushdown),
        Box::new(limit::LimitPushdown),
        Box::new(projection::ProjectionPruning),
    ]
}

/// Which input(s) of a join a predicate sitting directly above it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Reads no column at all — a constant predicate.
    Neither,
    Left,
    Right,
    /// Reads columns from both inputs.
    Both,
    /// Reads a column that addresses neither input: an index past the end of
    /// the concatenated output, or a `relation` tag naming an input this join
    /// does not have. Always a bug somewhere upstream, never something to
    /// guess at — see [`column_side`].
    Unattributable,
}

/// Which input of a join a single [`crate::ColumnRef`] belongs to.
///
/// # Two conventions, one rule
///
/// A column reference sitting directly above a join can arrive in either of
/// two shapes, and both are live in this codebase:
///
/// - **Flat, from `crate::lower`.** `Scope::resolve` in `lower/select.rs`
///   hardcodes `relation: 0` for *every* name it resolves, qualified or not,
///   and gives it a FLAT index across the concatenated left++right scope. So
///   `u.tid` in `t LEFT JOIN u` — a right-side column — arrives as
///   `relation: 0, index: 4` when `t` is three columns wide.
/// - **Tagged, from `crate::opt::decorrelate`.** The joins that rule
///   synthesizes carry genuine `relation: 1` references whose index is
///   already *right-relative* (see `decorrelate`'s `value_col` / `agg_col` /
///   `on` construction).
///
/// The two reconcile because they never disagree: a right-relative index
/// under `relation: 1` is unambiguous, and a `relation: 0` index is a left
/// column exactly when it falls inside the left input's width. So the rule is
/// **position first, tag second**.
///
/// # Why it lives here rather than in either rule
///
/// Two rules have to apply it — [`pushdown`], deciding which input a `WHERE`
/// conjunct may be pushed into, and [`projection`], deciding which input's
/// required-column set a join `filter`'s reads belong to. They were two
/// implementations of one convention and they disagreed, which is how a
/// right-side predicate ended up in a left-side `Scan` long after
/// [`projection`] had been fixed. One function, called from both, is the
/// version of that invariant a future edit cannot half-apply.
///
/// Reading the TAG alone is what this function exists to stop. Doing that
/// attributed every flat right-side reference to the left input, pushed it
/// into the left `Scan`, and made projection pruning panic on an index past
/// the end of that scan's column list — see the integration test
/// `crates/basin-plan/tests/anti_join_pushdown.rs`, whose `LEFT JOIN … WHERE
/// u.tid IS NULL` anti-join idiom is exactly that shape.
///
/// # Why out-of-range is refused rather than clamped
///
/// An index that addresses neither input cannot be pushed anywhere safely,
/// and leaving it above the join is the one placement that is never wrong.
/// This is what makes the invariant *enforceable* rather than merely
/// documented: a future convention this function does not know about gets a
/// stranded (slow) predicate, not a predicate silently evaluated against the
/// wrong table's column.
fn column_side(c: &ColumnRef, left_width: usize, right_width: usize) -> Side {
    match c.relation {
        0 => {
            let i = c.index as usize;
            if i < left_width {
                Side::Left
            } else if i < left_width + right_width {
                Side::Right
            } else {
                Side::Unattributable
            }
        }
        1 if (c.index as usize) < right_width => Side::Right,
        _ => Side::Unattributable,
    }
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
