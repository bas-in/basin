//! Limit pushdown.
//!
//! # Why this rule exists
//!
//! `docs/migration/df-removal/05-optimizer-rules.md` ran a rule-by-rule
//! ablation against real DataFusion 53 and found `push_down_limit` changes
//! four headline shapes — star join, `LATERAL`, correlated subquery, and the
//! 9.3x window-frame `SUM` — enough to promote it from its original Tier 2
//! guess to `MUST`: "`LIMIT 1000` above a window has to reach the scan or the
//! query materializes the whole table." The same argument applies directly to
//! Basin's `ORDER BY … LIMIT` numbers: unless the `Limit` sitting above a
//! `Sort` or a `Scan` is the one actually left there by this rule (rather than
//! stranded above several inert layers of `Project`/`Union ALL`), the physical
//! planner never gets the `Limit`-directly-over-`Sort` / `Limit`-directly-
//! over-`Scan` shape it needs to recognize `TopK` and per-file early
//! termination. This rule does not invent either physical operator; it only
//! guarantees the shape they pattern-match on actually reaches them.
//!
//! # Shape
//!
//! A [`LogicalPlan::Limit`] is pushed one layer at a time into its input:
//!
//! - Through [`LogicalPlan::Project`]: a projection is row-count- and
//!   row-order-preserving, so `Limit` and `Project` simply swap places, and
//!   the push continues recursively into what was under the `Project`.
//! - Into an inner [`LogicalPlan::Limit`]: the two combine into one (see
//!   [`combine`]), and the push continues with the combined bound.
//! - Into both arms of a `UNION ALL` ([`LogicalPlan::SetOp`] with
//!   `op: SetOpKind::Union, all: true`): unlike `Project`, this is additive,
//!   not a swap — the original `Limit` stays on top (concatenation still
//!   needs the final skip/take), and each arm additionally gets its own
//!   `Limit { skip: None, fetch: Some(skip + fetch) }`, since either arm
//!   could in the worst case supply every row the top `Limit` keeps.
//!
//! Everything else is a barrier: the `Limit` stops exactly where it is.
//!
//! # Correctness rules, and the wrong answers they prevent
//!
//! - **`Filter`.** Never pushed below. `LIMIT 10` moved under a `Filter`
//!   takes 10 rows *before* filtering, which can leave fewer than 10 (or the
//!   wrong 10) after — see [`never_pushes_below_a_filter`]. This is the
//!   single most tempting wrong move in this file, because "fewer rows to
//!   filter" sounds like a pure win right up until the filter is what
//!   decides which rows those should have been.
//! - **`Aggregate`.** Never pushed below. `Limit` above an `Aggregate`
//!   bounds the number of *groups*, not input rows — pushing it below would
//!   bound the input instead, which can silently produce fewer groups than
//!   the real aggregation would have — see
//!   [`never_pushes_below_an_aggregate`].
//! - **`Sort`.** Never pushed below, and the sort itself is never dropped or
//!   reordered. `Limit` directly above `Sort` *is* top-K and is deliberately
//!   left in that exact shape for the physical planner to recognize — taking
//!   some 10 rows and sorting them is not the same as sorting and taking the
//!   first 10 — see [`never_pushes_below_a_sort_or_drops_it`].
//! - **`Join`, in general.** Never pushed into either side. A join can
//!   multiply (or drop) rows relative to either input, so an input capped at
//!   `n` rows can produce fewer than `n` outputs even when `n` outputs exist
//!   in the untruncated join — see [`never_pushes_into_a_join`]. The
//!   preserved side of a `LEFT`/`RIGHT` join is tempting to special-case
//!   ("every row survives, so how could truncating it lose an output row?"),
//!   but that is only true when the join is provably one-to-one on that side,
//!   and nothing in [`LogicalPlan::Join`] carries key/uniqueness metadata to
//!   prove it — nullable, non-unique join keys are the default assumption a
//!   planner has to make, and there under a plain `LEFT JOIN` one preserved
//!   left row can still fan out into several output rows, so truncating the
//!   left input to `n` can produce far fewer than `n` outputs. Refused
//!   unconditionally rather than guessed at — see
//!   [`left_join_preserved_side_is_never_assumed_one_to_one`].
//! - **`UNION` (without `ALL`).** Never pushed into either arm, unlike
//!   `UNION ALL`. Dedup can drop rows from either arm entirely; an arm
//!   truncated to `n` rows before dedup can starve the final result of rows
//!   that would have survived deduplication — see
//!   [`never_pushes_into_a_plain_union_arm`].
//! - **`OFFSET` without `LIMIT`.** A `Limit` with `fetch: None` carries no
//!   bound to push anywhere that needs one synthesized (a `UNION ALL` arm's
//!   `skip + fetch` cap in particular) — `skip` alone does not imply any
//!   upper bound on row count, so it is never pushed as if it did — see
//!   [`offset_without_limit_is_never_pushed_as_a_fetch_bound`]. Note this is
//!   distinct from combining two literal nested `Limit`s where the *inner*
//!   `fetch` supplies the missing bound (see [`combine`]'s doc) — that case
//!   is a real arithmetic fact, not a synthesized one.
//! - **Non-literal bounds.** `skip`/`fetch` are arbitrary [`Expr`]s (a bind
//!   parameter, say), not necessarily [`Expr::Literal`]. Combining two nested
//!   `Limit`s, or sizing a `UNION ALL` arm, both require actual integer
//!   arithmetic on these values; when either bound cannot be read as a
//!   literal integer (see [`as_i64`]), the combination/push is refused rather
//!   than guessed — the plan is left with the (still entirely correct)
//!   un-combined `Limit`s.
//! - **`WITH TIES`.** An inner `Limit { with_ties: true, .. }` can return
//!   *more* rows than its own `fetch` — every row tied with the last one on
//!   the `ORDER BY` key. That means the row count the outer `Limit` sees is
//!   not actually `min(fetch, inner_fetch)`; it is unknown until execution.
//!   [`combine`] refuses to combine across an inner `with_ties: true` for
//!   exactly this reason.
//! - **Returning `None` on a no-op.** Matches
//!   [`super::driver::OptimizerRule::rewrite`]'s contract: reporting `Some`
//!   for an unchanged plan costs a full extra fixpoint pass in
//!   [`super::driver::optimize`] — see [`reports_no_change_when_nothing_moves`].
//!
//! # What this file does not do
//!
//! `Distinct`, `Window`, `Values`, `Empty`, `Scan`, `ProjectSet`, `Cte`,
//! `CteRef`, `LateralJoin`, `Intersect`/`Except`, and DML nodes are all
//! treated as opaque barriers, the same as `Filter`/`Aggregate`/`Sort`/`Join`.
//! `Distinct` in particular can drop rows exactly like a dedup `UNION`, for
//! the same reason a plain `UNION` arm is refused above. None of this moves a
//! benchmark number the ablation identified, so — same policy as
//! `push_down_filter` — it is left for a later increment rather than guessed
//! at here.

use crate::{Datum, Expr, LogicalPlan, SetOpKind};
use basin_pgtype::PgType;

use super::OptimizerRule;

/// Limit pushdown — see the module docs for the full contract.
pub struct LimitPushdown;

impl OptimizerRule for LimitPushdown {
    fn name(&self) -> &'static str {
        // Named to match DataFusion's own rule, which is what the ablation
        // harness in `05-optimizer-rules.md` is keyed on.
        "push_down_limit"
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let rewritten = rewrite_node(plan);
        (rewritten != *plan).then_some(rewritten)
    }
}

/// Recursively rewrite `plan`: every child is rewritten first (so a `Limit`
/// nested anywhere is reached), and if `plan` itself is a `Limit`, it is
/// pushed as deep into its now-settled input as correctness allows.
fn rewrite_node(plan: &LogicalPlan) -> LogicalPlan {
    let rebuilt = rebuild_children(plan);
    if let LogicalPlan::Limit {
        input,
        skip,
        fetch,
        with_ties,
    } = rebuilt
    {
        push_limit(*input, skip, fetch, with_ties)
    } else {
        rebuilt
    }
}

/// Reconstruct `plan` with every immediate child passed through
/// [`rewrite_node`]. Pure tree-shape plumbing — no pushdown decision is made
/// here, that is entirely [`push_limit`]'s job.
fn rebuild_children(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::CteRef { .. } => plan.clone(),

        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(rewrite_node(input)),
            exprs: exprs.clone(),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite_node(input)),
            predicate: predicate.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } => LogicalPlan::Aggregate {
            input: Box::new(rewrite_node(input)),
            group: group.clone(),
            aggs: aggs.clone(),
            grouping_sets: grouping_sets.clone(),
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(rewrite_node(input)),
            keys: keys.clone(),
        },
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_node(input)),
            skip: skip.clone(),
            fetch: fetch.clone(),
            with_ties: *with_ties,
        },
        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } => LogicalPlan::Join {
            left: Box::new(rewrite_node(left)),
            right: Box::new(rewrite_node(right)),
            kind: *kind,
            on: on.clone(),
            filter: filter.clone(),
        },
        LogicalPlan::LateralJoin { outer, inner, kind } => LogicalPlan::LateralJoin {
            outer: Box::new(rewrite_node(outer)),
            inner: Box::new(rewrite_node(inner)),
            kind: *kind,
        },
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => LogicalPlan::SetOp {
            left: Box::new(rewrite_node(left)),
            right: Box::new(rewrite_node(right)),
            op: *op,
            all: *all,
        },
        LogicalPlan::Distinct { input, on } => LogicalPlan::Distinct {
            input: Box::new(rewrite_node(input)),
            on: on.clone(),
        },
        LogicalPlan::Window { input, windows } => LogicalPlan::Window {
            input: Box::new(rewrite_node(input)),
            windows: windows.clone(),
        },
        LogicalPlan::ProjectSet { input, srfs } => LogicalPlan::ProjectSet {
            input: Box::new(rewrite_node(input)),
            srfs: srfs.clone(),
        },
        LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } => LogicalPlan::Cte {
            name: *name,
            recursive: *recursive,
            body: Box::new(rewrite_node(body)),
            input: Box::new(rewrite_node(input)),
        },
        LogicalPlan::Insert {
            table,
            input,
            columns,
            on_conflict,
            returning,
        } => LogicalPlan::Insert {
            table: *table,
            input: Box::new(rewrite_node(input)),
            columns: columns.clone(),
            on_conflict: on_conflict.clone(),
            returning: returning.clone(),
        },
        LogicalPlan::Update {
            table,
            set,
            from,
            predicate,
            returning,
            snapshot,
        } => LogicalPlan::Update {
            table: *table,
            set: set.clone(),
            from: from.as_ref().map(|p| Box::new(rewrite_node(p))),
            predicate: predicate.clone(),
            returning: returning.clone(),
            snapshot: *snapshot,
        },
        LogicalPlan::Delete {
            table,
            using,
            predicate,
            returning,
            snapshot,
        } => LogicalPlan::Delete {
            table: *table,
            using: using.as_ref().map(|p| Box::new(rewrite_node(p))),
            predicate: predicate.clone(),
            returning: returning.clone(),
            snapshot: *snapshot,
        },
    }
}

/// Push a `Limit { skip, fetch, with_ties }` as deep into `plan` as
/// correctness allows, wrapping it directly above whatever it gets stuck on.
/// Always returns a complete, valid plan.
fn push_limit(
    plan: LogicalPlan,
    skip: Option<Expr>,
    fetch: Option<Expr>,
    with_ties: bool,
) -> LogicalPlan {
    match plan {
        // A projection changes columns, never row count or row order: the
        // `Limit` and the `Project` simply swap places, and the push
        // continues into what was under the `Project`.
        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(push_limit(*input, skip, fetch, with_ties)),
            exprs,
        },

        // Two directly-nested `Limit`s combine into one — see `combine`'s
        // doc for the arithmetic (and for exactly when it refuses to guess).
        LogicalPlan::Limit {
            input: inner_input,
            skip: inner_skip,
            fetch: inner_fetch,
            with_ties: inner_with_ties,
        } => match combine(
            skip.as_ref(),
            fetch.as_ref(),
            inner_skip.as_ref(),
            inner_fetch.as_ref(),
            inner_with_ties,
        ) {
            Some((new_skip, new_fetch)) => push_limit(*inner_input, new_skip, new_fetch, with_ties),
            // Cannot prove the combination correct (non-literal bound, or an
            // inner WITH TIES whose true row count is unknown until
            // execution) — leave both `Limit`s exactly as they were.
            None => LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Limit {
                    input: inner_input,
                    skip: inner_skip,
                    fetch: inner_fetch,
                    with_ties: inner_with_ties,
                }),
                skip,
                fetch,
                with_ties,
            },
        },

        // `UNION ALL`: additive, not a swap. Concatenation still needs the
        // original `skip`/`fetch` applied on top, but each arm can supply at
        // most `skip + fetch` rows to that concatenation, so each arm gets
        // its own `Limit { skip: None, fetch: Some(skip + fetch) }`. Only
        // possible when `fetch` is actually present (see the module docs'
        // "OFFSET without LIMIT" rule) and both bounds are literal integers.
        LogicalPlan::SetOp {
            left,
            right,
            op: SetOpKind::Union,
            all: true,
        } => match arm_bound(skip.as_ref(), fetch.as_ref()) {
            Some(bound) => {
                let arm = |side: LogicalPlan| push_limit(side, None, Some(int_lit(bound)), false);
                LogicalPlan::Limit {
                    input: Box::new(LogicalPlan::SetOp {
                        left: Box::new(arm(*left)),
                        right: Box::new(arm(*right)),
                        op: SetOpKind::Union,
                        all: true,
                    }),
                    skip,
                    fetch,
                    with_ties,
                }
            }
            // No provable bound (see `arm_bound`) — refuse the push and
            // rebuild the identical shape unchanged.
            None => LogicalPlan::Limit {
                input: Box::new(LogicalPlan::SetOp {
                    left,
                    right,
                    op: SetOpKind::Union,
                    all: true,
                }),
                skip,
                fetch,
                with_ties,
            },
        },

        // Every other node is a barrier: `Filter` and `Aggregate` change
        // *which*/*how many* rows exist below a `Limit` in ways a truncated
        // input cannot reproduce; `Sort` must keep the `Limit` directly above
        // it so the physical planner can recognize top-K; `Join` (including
        // the preserved side of a `LEFT`/`RIGHT` join — no key/uniqueness
        // metadata exists to prove it one-to-one) can multiply rows; a plain
        // `UNION` can drop rows via dedup that a truncated arm would have
        // starved; and `Distinct`, `Window`, `Values`, `Empty`, `Scan`,
        // `ProjectSet`, `Cte`, `CteRef`, `LateralJoin`, `Intersect`/`Except`,
        // and the DML nodes are all out of scope (see the module docs). Also
        // reached whenever a `UNION ALL` arm bound could not be proven (see
        // `arm_bound`).
        other => LogicalPlan::Limit {
            input: Box::new(other),
            skip,
            fetch,
            with_ties,
        },
    }
}

/// Combine an outer `Limit { skip: s1, fetch: f1 }` sitting directly over an
/// inner `Limit { skip: s2, fetch: f2 }` into one equivalent `Limit`.
///
/// The inner limit selects rows `[s2, s2+f2)` of its input (or `[s2, end)` if
/// `f2` is `None`). The outer limit then selects rows `[s1, s1+f1)` of *that*
/// (or `[s1, end)` if `f1` is `None`) — which are rows `[s1+s2, ..)` of the
/// original input. So the combined skip is always `s1 + s2`.
///
/// The combined fetch depends on which bounds are present:
/// - Both present: at most `f1` rows, but never more than the `f2 - s1` rows
///   the inner limit actually has left after the outer's own skip — i.e.
///   `min(f1, f2 - s1)`, clamped at 0. E.g. `LIMIT 5 OFFSET 3` over
///   `LIMIT 10` yields `min(5, 10 - 3) = 5`, matching the doc example this
///   rule was specified against.
/// - Only `f1`: the inner limit places no cap, so `f1` stands unchanged.
/// - Only `f2`: the outer limit places no cap of its own, so the bound is
///   however many of the inner's `f2` rows remain after skipping `s1` of
///   them — `f2 - s1`, clamped at 0. (This is *not* the "OFFSET without
///   LIMIT" case the module docs warn about: the bound here comes from the
///   inner `fetch`, not from treating the outer `skip` as a bound in its own
///   right.)
/// - Neither: no fetch bound exists to combine; the result is a pure
///   `skip`-only `Limit`.
///
/// Returns `None` — refusing rather than guessing — when any present bound
/// is not a provable literal integer ([`as_i64`]), or when the inner `Limit`
/// is `WITH TIES`: a `WITH TIES` limit can return more rows than its `fetch`,
/// so the row count the outer limit would see is not actually bounded by
/// `f2` at all.
fn combine(
    skip: Option<&Expr>,
    fetch: Option<&Expr>,
    inner_skip: Option<&Expr>,
    inner_fetch: Option<&Expr>,
    inner_with_ties: bool,
) -> Option<(Option<Expr>, Option<Expr>)> {
    if inner_with_ties {
        return None;
    }

    let s1 = opt_as_i64(skip)?.unwrap_or(0);
    let s2 = opt_as_i64(inner_skip)?.unwrap_or(0);
    let f1 = opt_as_i64(fetch)?;
    let f2 = opt_as_i64(inner_fetch)?;

    let combined_fetch = match (f1, f2) {
        (Some(f1), Some(f2)) => Some((f2 - s1).max(0).min(f1)),
        (Some(f1), None) => Some(f1),
        (None, Some(f2)) => Some((f2 - s1).max(0)),
        (None, None) => None,
    };
    let combined_skip = s1 + s2;

    Some((
        (combined_skip != 0).then_some(int_lit(combined_skip)),
        combined_fetch.map(int_lit),
    ))
}

/// The literal row bound a `UNION ALL` arm needs: `skip + fetch`, the most
/// rows either arm could possibly contribute to the top `skip + fetch` of the
/// concatenated result. `None` — refuse, never guess — when `fetch` is
/// absent (an `OFFSET`-only `Limit` carries no upper bound at all; see the
/// module docs) or when either bound is not a provable literal integer.
fn arm_bound(skip: Option<&Expr>, fetch: Option<&Expr>) -> Option<i64> {
    let f = as_i64(fetch?)?;
    let s = opt_as_i64(skip)?.unwrap_or(0);
    Some(s + f)
}

/// Read `expr` as a literal non-negative integer, or `None` if it is not a
/// provable literal (a bind parameter, a computed expression, …). Pushdown
/// arithmetic never guesses at a value it cannot see.
fn as_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Datum::Int16(v), _) => Some(i64::from(*v)),
        Expr::Literal(Datum::Int32(v), _) => Some(i64::from(*v)),
        Expr::Literal(Datum::Int64(v), _) => Some(*v),
        _ => None,
    }
}

/// Resolve an optional `skip`/`fetch` slot to an optional literal value,
/// distinguishing "absent" from "present but not provably literal": `Some(None)`
/// means absent (the bound simply doesn't apply), while a plain `None` means
/// *refuse* — the slot is present but its value could not be read as a
/// literal integer, so nothing downstream may guess at it.
fn opt_as_i64(expr: Option<&Expr>) -> Option<Option<i64>> {
    match expr {
        None => Some(None),
        Some(e) => as_i64(e).map(Some),
    }
}

fn int_lit(v: i64) -> Expr {
    Expr::Literal(Datum::Int64(v), PgType::INT8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, ColumnRef, JoinKind, SnapshotId, SortKey, TableId};
    use basin_pgtype::Oid;

    fn scan(table: u32) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(table),
            projection: vec![ColId(0), ColId(1)],
            filters: vec![],
            snapshot: SnapshotId(0),
        }
    }

    fn col(index: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: format!("c{index}"),
        })
    }

    fn lit(v: i64) -> Expr {
        int_lit(v)
    }

    /// `int4 = int4`, oid 96 — a real `pg_operator` row, used only to build a
    /// throwaway predicate for the `Filter` test.
    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            op: crate::OpId(Oid(96)),
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn limit(input: LogicalPlan, skip: Option<i64>, fetch: Option<i64>) -> LogicalPlan {
        LogicalPlan::Limit {
            input: Box::new(input),
            skip: skip.map(lit),
            fetch: fetch.map(lit),
            with_ties: false,
        }
    }

    #[test]
    fn rule_name_matches_the_ablation_harness() {
        assert_eq!(LimitPushdown.name(), "push_down_limit");
    }

    #[test]
    fn pushes_a_limit_through_a_project() {
        let project = LogicalPlan::Project {
            input: Box::new(scan(1)),
            exprs: vec![(col(0), "a".to_string()), (col(1), "b".to_string())],
        };
        let plan = limit(project, None, Some(10));

        let rewritten = LimitPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Project { input, .. } = rewritten else {
            panic!("expected Project to survive on top, got {rewritten:?}");
        };
        assert_eq!(*input, limit(scan(1), None, Some(10)));
    }

    /// The doc's own worked example: `LIMIT 5 OFFSET 3` over `LIMIT 10`
    /// yields at most 5 rows starting from row 3 of the inner 10, i.e. a
    /// combined fetch of `min(5, 10 - 3) = 5`.
    #[test]
    fn combines_nested_limits_with_correct_offset_arithmetic() {
        let inner = limit(scan(1), None, Some(10));
        let outer = limit(inner, Some(3), Some(5));

        let rewritten = LimitPushdown.rewrite(&outer).expect("must combine");
        assert_eq!(rewritten, limit(scan(1), Some(3), Some(5)));
    }

    /// A skip-only outer limit over a fetch-bounded inner limit still has a
    /// computable combined fetch: whatever of the inner's `fetch` remains
    /// after the outer's `skip`. `OFFSET 3` over `LIMIT 10` leaves 7.
    #[test]
    fn combines_nested_limits_when_the_bound_comes_from_the_inner_fetch() {
        let inner = limit(scan(1), None, Some(10));
        let outer = limit(inner, Some(3), None);

        let rewritten = LimitPushdown.rewrite(&outer).expect("must combine");
        assert_eq!(rewritten, limit(scan(1), Some(3), Some(7)));
    }

    /// An outer skip that consumes the entire inner fetch (or more) combines
    /// to an empty (fetch = 0) result rather than a negative one.
    #[test]
    fn combined_fetch_never_goes_negative() {
        let inner = limit(scan(1), None, Some(3));
        let outer = limit(inner, Some(10), None);

        let rewritten = LimitPushdown.rewrite(&outer).expect("must combine");
        assert_eq!(rewritten, limit(scan(1), Some(10), Some(0)));
    }

    /// Wrong answer this prevents: an inner `WITH TIES` limit can return more
    /// rows than its own `fetch` (every tie on the ORDER BY key), so treating
    /// its row count as exactly `fetch` when combining would silently drop
    /// rows the un-combined plan would have kept.
    #[test]
    fn never_combines_across_an_inner_with_ties_limit() {
        let inner = LogicalPlan::Limit {
            input: Box::new(scan(1)),
            skip: None,
            fetch: Some(lit(10)),
            with_ties: true,
        };
        let outer = limit(inner, Some(3), Some(5));

        assert!(
            LimitPushdown.rewrite(&outer).is_none(),
            "combining across a WITH TIES inner limit assumes a row count it cannot prove"
        );
    }

    /// A non-literal bound (here, a bind parameter) cannot be combined
    /// arithmetically, so the two `Limit`s must be left exactly as they were
    /// rather than guessed at.
    #[test]
    fn never_combines_non_literal_bounds() {
        let inner = limit(scan(1), None, Some(10));
        let outer = LogicalPlan::Limit {
            input: Box::new(inner),
            skip: Some(Expr::Parameter {
                index: 1,
                ty: PgType::INT8,
            }),
            fetch: Some(lit(5)),
            with_ties: false,
        };

        assert!(
            LimitPushdown.rewrite(&outer).is_none(),
            "a non-literal skip must block combination rather than being guessed at"
        );
    }

    #[test]
    fn pushes_into_both_union_all_arms() {
        let union = LogicalPlan::SetOp {
            left: Box::new(scan(1)),
            right: Box::new(scan(2)),
            op: SetOpKind::Union,
            all: true,
        };
        let plan = limit(union, Some(2), Some(3));

        let rewritten = LimitPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Limit {
            input, skip, fetch, ..
        } = &rewritten
        else {
            panic!("expected the outer Limit to remain on top, got {rewritten:?}");
        };
        assert_eq!(*skip, Some(lit(2)));
        assert_eq!(*fetch, Some(lit(3)));
        let LogicalPlan::SetOp { left, right, .. } = input.as_ref() else {
            panic!("expected SetOp under the outer Limit, got {input:?}");
        };
        // Each arm may supply at most skip + fetch = 5 rows to the top.
        assert_eq!(**left, limit(scan(1), None, Some(5)));
        assert_eq!(**right, limit(scan(2), None, Some(5)));
    }

    /// Wrong answer this prevents: `UNION` (implicit `DISTINCT`) can drop
    /// rows from either arm during deduplication. Truncating an arm to `n`
    /// rows *before* dedup can throw away exactly the rows that would have
    /// survived deduplication, starving the final result below what the
    /// untruncated plan would have returned.
    #[test]
    fn never_pushes_into_a_plain_union_arm() {
        let union = LogicalPlan::SetOp {
            left: Box::new(scan(1)),
            right: Box::new(scan(2)),
            op: SetOpKind::Union,
            all: false,
        };
        let plan = limit(union, None, Some(5));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "a dedup UNION arm must not be truncated before the dedup runs"
        );
    }

    /// Wrong answer this prevents: `LIMIT 10` moved below a `Filter` takes
    /// the first 10 rows *before* filtering, which can leave fewer than 10 —
    /// or the wrong 10 — once the filter actually runs.
    #[test]
    fn never_pushes_below_a_filter() {
        let filter = LogicalPlan::Filter {
            input: Box::new(scan(1)),
            predicate: eq(col(0), lit(1)),
        };
        let plan = limit(filter, None, Some(10));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "Filter must be an unconditional pushdown barrier for Limit"
        );
    }

    /// Wrong answer this prevents: a `Limit` above an `Aggregate` bounds the
    /// number of *groups* in the output, not the number of input rows.
    /// Pushing it below would cap the input instead, which can produce fewer
    /// groups than the real (untruncated) aggregation would have.
    #[test]
    fn never_pushes_below_an_aggregate() {
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan(1)),
            group: vec![col(0)],
            aggs: vec![],
            grouping_sets: None,
        };
        let plan = limit(agg, None, Some(10));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "Aggregate must be an unconditional pushdown barrier for Limit"
        );
    }

    /// Wrong answer this prevents: a `Limit` directly above a `Sort` is
    /// top-K — "the smallest/largest N by this order" — which is not the
    /// same operation as "any N rows, then sort them." Pushing the limit
    /// below (or dropping the sort while doing so) picks an arbitrary N
    /// before the ordering that was supposed to select them has run.
    #[test]
    fn never_pushes_below_a_sort_or_drops_it() {
        let sort = LogicalPlan::Sort {
            input: Box::new(scan(1)),
            keys: vec![SortKey {
                expr: col(0),
                descending: false,
                nulls_first: false,
            }],
        };
        let plan = limit(sort, None, Some(10));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "Limit directly over Sort is the top-K shape and must be left exactly as-is"
        );
    }

    /// Wrong answer this prevents: a join can multiply rows relative to
    /// either input (or relative to what a naive "N in, N out" assumption
    /// would predict), so truncating an input to N rows can produce fewer
    /// than N *output* rows even though N outputs exist in the untruncated
    /// join.
    #[test]
    fn never_pushes_into_a_join() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1)),
            right: Box::new(scan(2)),
            kind: JoinKind::Inner,
            on: vec![],
            filter: None,
        };
        let plan = limit(join, None, Some(10));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "Join must be an unconditional pushdown barrier for Limit"
        );
    }

    /// Wrong answer this prevents: it is tempting to push a `Limit` into the
    /// preserved side of a `LEFT JOIN` on the theory that "every left row
    /// survives, so truncating it can't lose output rows." That reasoning
    /// silently assumes the join is one-to-one on that side. Nothing on
    /// `LogicalPlan::Join` carries key/uniqueness metadata to prove that, and
    /// in general one preserved left row can join to several right rows, so
    /// truncating the left input to N can produce far fewer than N outputs.
    #[test]
    fn left_join_preserved_side_is_never_assumed_one_to_one() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1)),
            right: Box::new(scan(2)),
            kind: JoinKind::Left,
            on: vec![],
            filter: None,
        };
        let plan = limit(join, None, Some(10));

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "the preserved side of an outer join must not be assumed one-to-one without key metadata"
        );
    }

    /// Wrong answer this prevents: an `OFFSET`-only `Limit` (`fetch: None`)
    /// carries no upper bound on row count at all. Pushing it into a
    /// `UNION ALL` arm would require synthesizing a fetch bound (`skip +
    /// fetch`) that does not exist, which is not a real arithmetic fact —
    /// unlike combining two literal nested `Limit`s, where a genuine bound
    /// can come from the *inner* fetch (see
    /// `combines_nested_limits_when_the_bound_comes_from_the_inner_fetch`).
    #[test]
    fn offset_without_limit_is_never_pushed_as_a_fetch_bound() {
        let union = LogicalPlan::SetOp {
            left: Box::new(scan(1)),
            right: Box::new(scan(2)),
            op: SetOpKind::Union,
            all: true,
        };
        let plan = limit(union, Some(5), None);

        assert!(
            LimitPushdown.rewrite(&plan).is_none(),
            "an OFFSET-only Limit has no fetch bound to push into a UNION ALL arm"
        );
    }

    #[test]
    fn reports_no_change_when_nothing_moves() {
        // Nothing above a bare Scan should ever report a change: there is
        // nothing left to push into.
        let plan = limit(scan(1), None, Some(10));
        assert!(LimitPushdown.rewrite(&plan).is_none());
    }

    /// The driver's fixpoint detection depends on repeated application
    /// eventually reporting no change. Push once, then push the *result*
    /// again and confirm it is now a true no-op.
    #[test]
    fn a_second_pass_over_an_already_pushed_plan_is_a_true_no_op() {
        let project = LogicalPlan::Project {
            input: Box::new(scan(1)),
            exprs: vec![(col(0), "a".to_string())],
        };
        let plan = limit(project, None, Some(10));

        let once = LimitPushdown.rewrite(&plan).expect("first pass must push");
        assert!(
            LimitPushdown.rewrite(&once).is_none(),
            "a plan with nothing left to push must report no change"
        );
    }

    /// A plan with no `Limit` at all is trivially a no-op — pins that the
    /// rule does not spuriously rewrite unrelated tree shapes.
    #[test]
    fn a_plan_with_no_limit_is_a_no_op() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1)),
            predicate: eq(col(0), lit(1)),
        };
        assert!(LimitPushdown.rewrite(&plan).is_none());
    }
}
