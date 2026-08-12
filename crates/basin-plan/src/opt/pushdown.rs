//! Filter pushdown.
//!
//! # Why this rule exists
//!
//! `docs/migration/df-removal/05-optimizer-rules.md` ran a rule-by-rule
//! ablation against real DataFusion 53 and found `push_down_filter` is one of
//! only two rules the *storage-side* benchmark wins depend on (the other is
//! projection pruning). Its job is narrow but load-bearing: it is what
//! populates [`LogicalPlan::Scan::filters`], and that field is the entire
//! interface between the planner and row-group / bloom / zone-map pruning in
//! `basin-storage`. Without it, `WHERE id BETWEEN 100 AND 200` evaluates
//! after every row has already been decoded — "no predicate reaches the
//! scan, so no row-group pruning, no bloom probe, no zone-map skip," per that
//! doc. Filters left stranded above a join are worse still: they run after
//! the join has already done the (larger) work of matching rows.
//!
//! # Shape
//!
//! A [`LogicalPlan::Filter`] is processed in two steps:
//!
//! 1. **Split.** `WHERE a AND b AND c` is one [`Expr`] tree; splitting it
//!    into three independent conjuncts (see [`split_conjunction`]) is what
//!    lets `b` reach a different destination than `a` and `c` — e.g. `a`
//!    stays above a `LEFT JOIN` while `b` and `c` each reach a different base
//!    table. Skipping this step turns the whole predicate into one immovable
//!    blob wherever any single conjunct cannot move, which is most of the
//!    lost benchmark headroom the doc above describes.
//! 2. **Push**, independently, each conjunct through whatever sits between
//!    the `Filter` and a base table — [`LogicalPlan::Project`] (rewriting
//!    column references through the projection),
//!    [`LogicalPlan::Aggregate`] (only when every referenced column is a
//!    grouping key — see below), and [`LogicalPlan::Join`] (into whichever
//!    side supplies every column it needs, subject to the outer-join rule
//!    below) — finally landing in [`LogicalPlan::Scan::filters`].
//!
//! Whatever a conjunct cannot move through is re-wrapped in a `Filter`
//! exactly where it got stuck; nothing is ever dropped.
//!
//! # Correctness rules, and the wrong answers they prevent
//!
//! - **Outer joins.** A `WHERE` predicate sitting above a join is evaluated
//!   *after* the join, including after null-extension. Its preserved side
//!   (`LEFT`'s left, `RIGHT`'s right, both sides for `INNER`/`CROSS`) can
//!   always absorb a predicate that only touches that side, because a
//!   preserved-side row is unconditionally present in the output and
//!   filtering it before or after the join makes no difference. Its
//!   null-supplying side cannot: filtering that side *before* the join runs
//!   the predicate before the row is known to be unmatched, which silently
//!   degrades the outer join into an inner one for that predicate (see
//!   [`allowed_sides`] and the `left_outer_join_never_pushes_into_null_side`
//!   / `right_outer_join_never_pushes_into_null_side` /
//!   `full_outer_join_pushes_into_neither_side` tests). For the same reason,
//!   a predicate that touches *both* sides is never folded into
//!   [`LogicalPlan::Join::filter`] either — that field is evaluated at
//!   join-build time too, before null-extension, so merging a `WHERE`
//!   predicate into it has exactly the same failure mode as pushing into the
//!   null-supplying side. It is left above the join, untouched.
//! - **Aggregation.** A predicate only pushes below
//!   [`LogicalPlan::Aggregate`] if *every* column it references is a
//!   grouping key (`group[i]` for `i < group.len()`), and only when there
//!   are no `grouping_sets` (no `ROLLUP`/`CUBE`/`GROUPING SETS`). A
//!   predicate on an aggregate result (`i >= group.len()`, e.g. `SUM(x)`) is
//!   a `HAVING` clause by definition and evaluating it before the
//!   aggregation runs is meaningless — the value does not exist yet. Even a
//!   pure grouping-key predicate is refused under `ROLLUP`/`CUBE`: those
//!   introduce synthetic super-aggregate rows with `NULL` in the rolled-up
//!   column, and pushing `dept = 'x'` below the aggregate would let the
//!   `(NULL, sum-over-x)` super-aggregate row survive when the un-pushed
//!   `HAVING dept = 'x'` would have dropped it (`NULL <> 'x'`) — an extra row
//!   that should not be there.
//! - **`LIMIT`.** Never pushed through. Filtering below a `LIMIT` changes
//!   which rows are among the first *n*, which changes the result set, not
//!   just its speed.
//! - **`WINDOW`.** Never pushed through. A window frame is computed over the
//!   row set as it stood *before* filtering; moving the filter underneath
//!   changes what every window function sees.
//! - **Subqueries and set-returning functions.** A conjunct containing
//!   either ([`Expr::Subquery`], [`Expr::SetReturning`]) is never pushed at
//!   all, through anything — see [`is_unpushable`]. A correlated subquery's
//!   truth value can depend on outer columns Basin has not attempted to
//!   track through a rewrite here, and a set-returning function changes row
//!   *cardinality*, which a predicate can only safely relate to at the
//!   position it started in.
//!
//! # What this file does not do
//!
//! Scope is deliberately bounded to what the ablation above actually
//! measured as load-bearing. All correct, all safe, none of it wrong:
//!
//! - [`LogicalPlan::Sort`], [`LogicalPlan::Distinct`], [`LogicalPlan::SetOp`],
//!   [`LogicalPlan::LateralJoin`], [`LogicalPlan::Cte`], and
//!   [`LogicalPlan::ProjectSet`] are treated as opaque barriers, same as
//!   `Limit`/`Window`. Pushing through plain `Sort` or plain (non-`ON`)
//!   `Distinct` is a valid transformation Postgres itself performs, but
//!   `DISTINCT ON` and every other listed node have real hazards (a `DISTINCT
//!   ON` "first row per group" pick can change under a pre-filter,
//!   `LateralJoin`'s right side may be correlated on the left, `SetOp`'s two
//!   inputs need matching treatment) and none of it moves a benchmark number
//!   the ablation identified, so it is left for a later increment rather than
//!   guessed at here.
//! - Two adjacent `Filter` nodes are not fused into one before splitting;
//!   each is split and pushed independently. This can leave a `Filter`
//!   directly on top of another `Filter` where a single combined node would
//!   be tidier, but it is never wrong, and the driver's multi-pass fixpoint
//!   (`opt::driver::optimize`) converges on it regardless.
//! - `Expr` has no dedicated variant for the boolean connectives yet — see
//!   [`AND_OP`] below for how conjunction splitting works around that.
//!
//! # A note on how conjunction is recognized
//!
//! Postgres's `AND` (and `OR`, `NOT`) is a `BoolExpr` parser node, not a
//! `pg_operator` row — confirmed against `basin_pgtype::operator::OPERATORS`,
//! which is `pg_catalog.pg_operator` verbatim and has no entry for it. This
//! crate's [`Expr`] does not yet have a dedicated logical-connective variant
//! to carry that distinction, so until it does, this file recognizes (and
//! reconstructs) a conjunction structurally through [`AND_OP`], a private
//! sentinel [`OpId`] that cannot collide with any real `pg_operator` oid.
//! This convention is local to this file — it changes nothing about what
//! `crate::lower` actually produces. If `Expr` gains a real variant for `AND`
//! later, [`split_conjunction`] and [`and`] are the only two functions that
//! need to change.

use crate::{ColumnRef, Expr, JoinKind, LogicalPlan, OpId, Subscript};
use basin_pgtype::Oid;

use super::OptimizerRule;

/// See the "note on how conjunction is recognized" in the module docs. Chosen
/// as `u32::MAX` specifically because every real `pg_operator` oid is a small
/// number (the largest in `basin_pgtype::operator::OPERATORS` is in the low
/// thousands) — this can never alias a real operator by accident.
const AND_OP: OpId = OpId(Oid(u32::MAX));

/// Filter pushdown — see the module docs for the full contract.
pub struct FilterPushdown;

impl OptimizerRule for FilterPushdown {
    fn name(&self) -> &'static str {
        // Named to match DataFusion's own rule, which is what the ablation
        // harness in `05-optimizer-rules.md` is keyed on.
        "push_down_filter"
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let rewritten = rewrite_node(plan);
        (rewritten != *plan).then_some(rewritten)
    }
}

/// Recursively rewrite `plan`: every child is rewritten first (so a `Filter`
/// nested anywhere is reached), and if `plan` itself is a `Filter`, its
/// predicate is split and pushed into its now-settled input.
fn rewrite_node(plan: &LogicalPlan) -> LogicalPlan {
    let rebuilt = rebuild_children(plan);
    if let LogicalPlan::Filter { input, predicate } = rebuilt {
        push_into(*input, split_conjunction(predicate))
    } else {
        rebuilt
    }
}

/// Reconstruct `plan` with every immediate child passed through
/// [`rewrite_node`]. Pure tree-shape plumbing — no pushdown decision is made
/// here, that is entirely [`push_into`]'s job.
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

/// Push `preds` as deep into `plan` as correctness allows, wrapping whatever
/// cannot go any further in a `Filter` directly above the point it got
/// stuck. Always returns a complete, valid plan — never a partial result —
/// so every call site can treat it as the final answer for its subtree.
fn push_into(plan: LogicalPlan, preds: Vec<Expr>) -> LogicalPlan {
    if preds.is_empty() {
        return plan;
    }

    // Subqueries and set-returning functions never move, full stop — see the
    // module docs. Checked before anything else so a conjunct that fails
    // this test is never even considered for Project/Aggregate/Join
    // rewriting below.
    let mut blocked = Vec::new();
    let mut candidates = Vec::new();
    for p in preds {
        if is_unpushable(&p) {
            blocked.push(p);
        } else {
            candidates.push(p);
        }
    }

    let pushed = match plan {
        LogicalPlan::Project { input, exprs } if !candidates.is_empty() => {
            let mut deeper = Vec::new();
            for p in candidates {
                match substitute_via_project(&p, &exprs) {
                    // Substitution can *introduce* a subquery or SRF that
                    // was not directly visible in the predicate before
                    // (e.g. the predicate referenced a projected column
                    // whose defining expression is itself a subquery) —
                    // re-check after rewriting, not just before.
                    Some(rewritten) if !is_unpushable(&rewritten) => deeper.push(rewritten),
                    _ => blocked.push(p),
                }
            }
            LogicalPlan::Project {
                input: Box::new(push_into(*input, deeper)),
                exprs,
            }
        }

        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } if !candidates.is_empty() && grouping_sets.is_none() => {
            let mut deeper = Vec::new();
            for p in candidates {
                match substitute_via_group(&p, &group) {
                    Some(rewritten) if !is_unpushable(&rewritten) => deeper.push(rewritten),
                    _ => blocked.push(p),
                }
            }
            LogicalPlan::Aggregate {
                input: Box::new(push_into(*input, deeper)),
                group,
                aggs,
                grouping_sets,
            }
        }

        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } if !candidates.is_empty() => {
            let (left_ok, right_ok) = allowed_sides(kind);
            let mut to_left = Vec::new();
            let mut to_right = Vec::new();
            for p in candidates {
                let uses_left = touches_relation(&p, 0);
                let uses_right = touches_relation(&p, 1);
                match (uses_left, uses_right) {
                    // Straddles both sides: cannot become a single-input
                    // predicate on either child, and must not be folded
                    // into `Join::filter` either (see the outer-join note
                    // in the module docs) — stays above the join.
                    (true, true) => blocked.push(p),
                    (true, false) if left_ok => to_left.push(p),
                    (false, true) if right_ok => to_right.push(rebase_right(p)),
                    // No column reference at all (e.g. a constant
                    // predicate) — either side is semantically fine;
                    // prefer whichever the join kind allows.
                    (false, false) if left_ok => to_left.push(p),
                    (false, false) if right_ok => to_right.push(rebase_right(p)),
                    _ => blocked.push(p),
                }
            }
            LogicalPlan::Join {
                left: Box::new(push_into(*left, to_left)),
                right: Box::new(push_into(*right, to_right)),
                kind,
                on,
                filter,
            }
        }

        LogicalPlan::Scan {
            table,
            projection,
            mut filters,
            snapshot,
        } if !candidates.is_empty() => {
            filters.extend(candidates);
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                snapshot,
            }
        }

        // Every other node — including `Limit` and `Window`, which must
        // never be pushed through (see the module docs) — is a barrier.
        other => {
            blocked.extend(candidates);
            other
        }
    };

    wrap_filter(pushed, blocked)
}

/// Wrap `plan` in a `Filter` over the conjunction of `preds`, or return
/// `plan` unchanged if there is nothing left to wrap.
fn wrap_filter(plan: LogicalPlan, preds: Vec<Expr>) -> LogicalPlan {
    match conjunction(preds) {
        Some(predicate) => LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    }
}

/// Which side(s) of a join a pre-join filter may be pushed into. See the
/// module docs' "Outer joins" section for the reasoning.
fn allowed_sides(kind: JoinKind) -> (bool, bool) {
    match kind {
        JoinKind::Inner | JoinKind::Cross => (true, true),
        // LEFT JOIN: the left input is preserved (every left row appears,
        // matched or null-extended); the right input is the null-supplying
        // side. Only the preserved side may absorb a pre-join filter.
        JoinKind::Left => (true, false),
        // Mirror image of LEFT.
        JoinKind::Right => (false, true),
        // FULL JOIN: either side can be the null-supplying one depending on
        // the row, so neither is safe to filter before the join.
        JoinKind::Full => (false, false),
        // Semi/anti joins output only the left side's columns (the right
        // side exists purely as an existence test), so a WHERE-position
        // predicate above one can only ever reference relation 0 to begin
        // with. Pushing into the right side would change which rows the
        // existence test matches, so it is refused even defensively.
        JoinKind::LeftSemi | JoinKind::LeftAnti => (true, false),
    }
}

/// Whether `expr` contains a subquery or a set-returning function anywhere
/// beneath it — either disqualifies a predicate from being pushed at all.
/// See the module docs.
fn is_unpushable(expr: &Expr) -> bool {
    expr.contains_srf() || expr.any(&mut |e| matches!(e, Expr::Subquery { .. }))
}

/// Whether `expr` references any column of the given relation index (0 or 1
/// — see `ColumnRef::relation`'s doc comment).
fn touches_relation(expr: &Expr, relation: u16) -> bool {
    expr.any(&mut |e| matches!(e, Expr::Column(c) if c.relation == relation))
}

/// Rewrite a predicate that [`touches_relation`] confirmed only uses
/// relation 1 (a join's right side) so it can sit directly over that side as
/// a single-input predicate, where relation 0 means "the input" again.
fn rebase_right(expr: Expr) -> Expr {
    map_columns(&expr, &mut |c| {
        if c.relation == 1 {
            Expr::Column(ColumnRef {
                relation: 0,
                index: c.index,
                name: c.name.clone(),
            })
        } else {
            Expr::Column(c.clone())
        }
    })
}

/// Rewrite a predicate sitting above a `Project` in terms of the `Project`'s
/// input, by replacing each output-column reference with the expression
/// that produces it. Returns `None` if any referenced column is out of
/// range or (defensively) references a relation other than 0 — a `Project`
/// has one input, so nothing above it should ever address a second one.
fn substitute_via_project(expr: &Expr, exprs: &[(Expr, String)]) -> Option<Expr> {
    let mut ok = true;
    let rewritten = map_columns(expr, &mut |c| {
        if c.relation == 0 {
            if let Some((e, _)) = exprs.get(c.index as usize) {
                return e.clone();
            }
        }
        ok = false;
        Expr::Column(c.clone())
    });
    ok.then_some(rewritten)
}

/// Rewrite a predicate sitting above an `Aggregate` in terms of the
/// `Aggregate`'s input, by replacing each referenced output column with its
/// grouping expression. Returns `None` (refusing the whole predicate, not
/// just the offending part) as soon as any referenced column index falls
/// outside `group` — that is exactly "references an aggregate result",
/// which is the `HAVING`-vs-`WHERE` line described in the module docs.
fn substitute_via_group(expr: &Expr, group: &[Expr]) -> Option<Expr> {
    let mut ok = true;
    let rewritten = map_columns(expr, &mut |c| {
        if c.relation == 0 {
            if let Some(e) = group.get(c.index as usize) {
                return e.clone();
            }
        }
        ok = false;
        Expr::Column(c.clone())
    });
    ok.then_some(rewritten)
}

/// Rewrite every [`Expr::Column`] in `expr` via `f`, otherwise reconstructing
/// the tree unchanged. Deliberately does not descend into [`Expr::Subquery`]
/// — same boundary [`Expr::for_each_child`] observes and for the same
/// reason: a subquery's column indices belong to its own plan, not this one.
/// A predicate reaching this function has already been confirmed pushable by
/// [`is_unpushable`], so `Subquery` and `SetReturning` are left untouched
/// rather than handled.
fn map_columns(expr: &Expr, f: &mut impl FnMut(&ColumnRef) -> Expr) -> Expr {
    match expr {
        Expr::Column(c) => f(c),
        Expr::Literal(..) | Expr::Parameter { .. } => expr.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: Box::new(map_columns(arg, f)),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: Box::new(map_columns(lhs, f)),
            rhs: Box::new(map_columns(rhs, f)),
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: Box::new(map_columns(arg, f)),
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: operand.as_ref().map(|o| Box::new(map_columns(o, f))),
            whens: whens
                .iter()
                .map(|(w, t)| (map_columns(w, f), map_columns(t, f)))
                .collect(),
            else_: else_.as_ref().map(|e| Box::new(map_columns(e, f))),
        },
        Expr::Coalesce(xs) => Expr::Coalesce(xs.iter().map(|x| map_columns(x, f)).collect()),
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: Box::new(map_columns(arg, f)),
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: Box::new(map_columns(arg, f)),
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: Box::new(map_columns(lhs, f)),
            rhs: Box::new(map_columns(rhs, f)),
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: Box::new(map_columns(arg, f)),
            list: list.iter().map(|x| map_columns(x, f)).collect(),
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: Box::new(map_columns(arg, f)),
            low: Box::new(map_columns(low, f)),
            high: Box::new(map_columns(high, f)),
            symmetric: *symmetric,
            negated: *negated,
        },
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            arg: Box::new(map_columns(arg, f)),
            pattern: Box::new(map_columns(pattern, f)),
            escape: escape.as_ref().map(|e| Box::new(map_columns(e, f))),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: args.iter().map(|a| map_columns(a, f)).collect(),
        },
        // Neither can legally appear inside a WHERE-position predicate: an
        // `Expr::Aggregate` there is rejected during lowering
        // (`Expr::contains_aggregate`'s doc), and a window function's value
        // is only ever exposed through a `LogicalPlan::Window` output
        // column, never embedded directly in a filter above it. Left
        // structurally alone rather than panicking — worst case is a
        // predicate that fails to push, which is still correct.
        Expr::Aggregate { .. } | Expr::Window { .. } => expr.clone(),
        // Already excluded by `is_unpushable` before this function is ever
        // called on a top-level predicate.
        Expr::SetReturning { .. } | Expr::Subquery { .. } => expr.clone(),
        Expr::ArrayLit(xs) => Expr::ArrayLit(xs.iter().map(|x| map_columns(x, f)).collect()),
        Expr::RowLit(xs) => Expr::RowLit(xs.iter().map(|x| map_columns(x, f)).collect()),
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: Box::new(map_columns(arg, f)),
            indices: indices
                .iter()
                .map(|i| match i {
                    Subscript::Index(e) => Subscript::Index(map_columns(e, f)),
                    Subscript::Slice { lower, upper } => Subscript::Slice {
                        lower: lower.as_ref().map(|l| map_columns(l, f)),
                        upper: upper.as_ref().map(|u| map_columns(u, f)),
                    },
                })
                .collect(),
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: Box::new(map_columns(arg, f)),
            field: *field,
        },
    }
}

/// Flatten `WHERE a AND b AND c` into `[a, b, c]`. See [`AND_OP`] for how
/// `AND` is recognized.
fn split_conjunction(expr: Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    split_into(expr, &mut out);
    out
}

fn split_into(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary { op, lhs, rhs } if op == AND_OP => {
            split_into(*lhs, out);
            split_into(*rhs, out);
        }
        other => out.push(other),
    }
}

/// The inverse of [`split_conjunction`]: fold a list of predicates back into
/// one `AND`-conjoined expression. `None` for an empty list — there is
/// nothing to wrap in a `Filter`.
fn conjunction(preds: Vec<Expr>) -> Option<Expr> {
    let mut iter = preds.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, and))
}

fn and(lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary {
        op: AND_OP,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, Datum, GroupingSets, SnapshotId, SortKey, TableId};
    use basin_pgtype::PgType;

    fn scan(table: u32, filters: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(table),
            projection: vec![ColId(0), ColId(1)],
            filters,
            snapshot: SnapshotId(0),
        }
    }

    fn col(relation: u16, index: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation,
            index,
            name: format!("c{index}"),
        })
    }

    fn lit(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    /// `int4 = int4`, oid 96 — a real `pg_operator` row (see
    /// `basin_pgtype::operator::OPERATORS`), unlike `AND_OP`.
    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            op: OpId(Oid(96)),
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    #[test]
    fn rule_name_matches_the_ablation_harness() {
        assert_eq!(FilterPushdown.name(), "push_down_filter");
    }

    #[test]
    fn splits_a_conjunction_into_independently_pushable_predicates() {
        // WHERE c0 = 1 AND c1 = 2 AND c0 = 3, all on one base table: a
        // single un-split predicate would still land in `filters` as one
        // blob, so the real assertion is that it becomes three *separate*
        // entries.
        let predicate = and(
            and(eq(col(0, 0), lit(1)), eq(col(0, 1), lit(2))),
            eq(col(0, 0), lit(3)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![])),
            predicate,
        };

        let rewritten = FilterPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Scan { filters, .. } = rewritten else {
            panic!("expected a bare Scan, got {rewritten:?}");
        };
        assert_eq!(filters.len(), 3, "each conjunct should push independently");
    }

    #[test]
    fn pushes_through_project_rewriting_column_references() {
        // Project swaps the two scan columns: output col 0 = input col 1.
        let project = LogicalPlan::Project {
            input: Box::new(scan(1, vec![])),
            exprs: vec![(col(0, 1), "a".to_string()), (col(0, 0), "b".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: eq(col(0, 0), lit(7)),
        };

        let rewritten = FilterPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Project { input, .. } = rewritten else {
            panic!("expected Project to survive, got {rewritten:?}");
        };
        let LogicalPlan::Scan { filters, .. } = *input else {
            panic!("expected the predicate to reach the scan");
        };
        assert_eq!(filters.len(), 1);
        // Output column 0 is input column 1 under this projection, so the
        // pushed predicate must reference relation 0 / index 1, not 0.
        assert_eq!(
            filters[0],
            eq(col(0, 1), lit(7)),
            "column reference must be rewritten through the projection, not copied verbatim"
        );
    }

    #[test]
    fn pushes_through_inner_join_by_side() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1, vec![])),
            right: Box::new(scan(2, vec![])),
            kind: JoinKind::Inner,
            on: vec![],
            filter: None,
        };
        let predicate = and(eq(col(0, 0), lit(1)), eq(col(1, 0), lit(2)));
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let rewritten = FilterPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Join { left, right, .. } = rewritten else {
            panic!("expected Join to survive, got {rewritten:?}");
        };
        let LogicalPlan::Scan { filters: lf, .. } = *left else {
            panic!("left side must be a scan");
        };
        let LogicalPlan::Scan { filters: rf, .. } = *right else {
            panic!("right side must be a scan");
        };
        assert_eq!(lf, vec![eq(col(0, 0), lit(1))]);
        // The right-side predicate must be rebased from relation 1 to
        // relation 0 once it sits directly over the right scan alone.
        assert_eq!(rf, vec![eq(col(0, 0), lit(2))]);
    }

    /// Wrong answer this prevents: pushing `r.y = 5` into the right side of
    /// a LEFT JOIN turns `SELECT * FROM l LEFT JOIN r ON l.id = r.id WHERE
    /// r.y = 5` into `SELECT * FROM l LEFT JOIN (SELECT * FROM r WHERE y =
    /// 5) r2 ON l.id = r2.id`. Any left row with no matching (or
    /// y=5-failing) right row would then null-extend and *survive*, when
    /// Postgres's real LEFT JOIN + WHERE semantics (filter evaluated after
    /// null-extension, where `NULL = 5` is not true) drops it. That silently
    /// degrades the LEFT JOIN into an INNER JOIN for every row this
    /// predicate touches.
    #[test]
    fn left_outer_join_never_pushes_into_null_side() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1, vec![])),
            right: Box::new(scan(2, vec![])),
            kind: JoinKind::Left,
            on: vec![],
            filter: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col(1, 0), lit(5)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a right-only predicate over a LEFT JOIN must not move at all"
        );
    }

    /// Mirror image of the LEFT JOIN case: RIGHT JOIN's null-supplying side
    /// is the left input, so a left-only predicate must not push there.
    #[test]
    fn right_outer_join_never_pushes_into_null_side() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1, vec![])),
            right: Box::new(scan(2, vec![])),
            kind: JoinKind::Right,
            on: vec![],
            filter: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col(0, 0), lit(5)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a left-only predicate over a RIGHT JOIN must not move at all"
        );
    }

    /// Wrong answer this prevents: with FULL JOIN, either side can be the
    /// null-extended one depending on the row (an unmatched right row
    /// appears with `NULL` left columns, and vice versa). Pushing a
    /// left-only predicate into the left input before the join would drop
    /// exactly the unmatched-right rows that a real FULL JOIN + WHERE keeps
    /// dropped only when the WHERE clause actually fails against their
    /// (NULL) left columns — i.e. it changes which "NULL side" rows survive.
    #[test]
    fn full_outer_join_pushes_into_neither_side() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1, vec![])),
            right: Box::new(scan(2, vec![])),
            kind: JoinKind::Full,
            on: vec![],
            filter: None,
        };
        // Single-sided predicate: safe for INNER, unsafe for FULL.
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col(0, 0), lit(5)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "FULL JOIN must push into neither side, even a single-sided predicate"
        );
    }

    /// A predicate that references columns from both sides can be neither a
    /// single-input predicate on either child nor folded into `Join::filter`
    /// (see the module docs) — it must stay exactly where it started.
    #[test]
    fn join_predicate_referencing_both_sides_is_never_pushed_to_either_side() {
        let join = LogicalPlan::Join {
            left: Box::new(scan(1, vec![])),
            right: Box::new(scan(2, vec![])),
            kind: JoinKind::Inner,
            on: vec![],
            filter: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col(0, 0), col(1, 0)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a two-sided predicate must not be pushed to either child or into Join::filter"
        );
    }

    #[test]
    fn aggregate_pushes_predicates_on_grouping_columns() {
        // GROUP BY c1 (scan's second column) -- deliberately index 1, not 0,
        // so a passing test proves real substitution happened rather than
        // an identity coincidence.
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan(1, vec![])),
            group: vec![col(0, 1)],
            aggs: vec![],
            grouping_sets: None,
        };
        // The Aggregate's own output column 0 is the grouping key.
        let plan = LogicalPlan::Filter {
            input: Box::new(agg),
            predicate: eq(col(0, 0), lit(9)),
        };

        let rewritten = FilterPushdown.rewrite(&plan).expect("must push");
        let LogicalPlan::Aggregate { input, .. } = rewritten else {
            panic!("expected Aggregate to survive, got {rewritten:?}");
        };
        let LogicalPlan::Scan { filters, .. } = *input else {
            panic!("expected the predicate to reach the scan");
        };
        assert_eq!(filters, vec![eq(col(0, 1), lit(9))]);
    }

    /// Wrong answer this prevents: `SELECT dept, SUM(sal) FROM emp GROUP BY
    /// dept HAVING SUM(sal) > 100` is not equivalent to filtering
    /// `SUM(sal) > 100` on rows *before* grouping -- the aggregate value
    /// does not exist until grouping has happened, so "pushing" it below
    /// the Aggregate cannot even be given a meaning that computes the same
    /// sums. This is the textbook WHERE/HAVING distinction.
    #[test]
    fn aggregate_never_pushes_a_predicate_on_an_aggregate_result() {
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan(1, vec![])),
            group: vec![col(0, 0)], // one grouping key -> index 0
            aggs: vec![Expr::Aggregate {
                func: crate::FuncId(Oid(2108)), // sum(int8), same oid used in expr.rs's own tests
                args: vec![col(0, 1)],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        // Output column 1 is the aggregate result (index >= group.len()).
        let plan = LogicalPlan::Filter {
            input: Box::new(agg),
            predicate: eq(col(0, 1), lit(100)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a predicate on an aggregate result is HAVING, not WHERE, and must not move"
        );
    }

    /// Wrong answer this prevents: `SELECT dept, SUM(sal) FROM emp GROUP BY
    /// ROLLUP(dept) HAVING dept = 'x'` keeps only the `('x', sum-over-all-x)`
    /// row -- the synthetic `(NULL, ...)` super-aggregate row fails `NULL =
    /// 'x'`. Pushing `dept = 'x'` below the Aggregate would compute ROLLUP
    /// over the already-filtered (x-only) rows, which *still produces* a
    /// `(NULL, sum-over-x)` super-aggregate row -- an extra row that should
    /// not survive. Grouping-key-only is necessary but not sufficient once
    /// `ROLLUP`/`CUBE`/`GROUPING SETS` are involved.
    #[test]
    fn aggregate_with_rollup_never_pushes_even_a_grouping_key_predicate() {
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan(1, vec![])),
            group: vec![col(0, 0)],
            aggs: vec![],
            grouping_sets: Some(GroupingSets(vec![vec![0], vec![]])), // ROLLUP(c0)
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(agg),
            predicate: eq(col(0, 0), lit(1)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "ROLLUP/CUBE/GROUPING SETS must block pushdown even for a pure grouping-key predicate"
        );
    }

    /// Wrong answer this prevents: `SELECT * FROM t LIMIT 10` picks the
    /// first 10 rows the input produces. Pushing a filter below the `LIMIT`
    /// would apply it *before* the cap, changing which 10 rows (or how
    /// many) survive -- the whole point of "first N" is that it operates on
    /// the row set as the input presents it.
    #[test]
    fn never_pushes_through_limit() {
        let limit = LogicalPlan::Limit {
            input: Box::new(scan(1, vec![])),
            skip: None,
            fetch: Some(lit(10)),
            with_ties: false,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(limit),
            predicate: eq(col(0, 0), lit(1)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "LIMIT must be an unconditional pushdown barrier"
        );
    }

    /// Wrong answer this prevents: `ROW_NUMBER() OVER (ORDER BY x)` numbers
    /// the row set as it exists when the window runs. Pushing a filter below
    /// `Window` changes that row set, so every window function's result
    /// changes for every surviving row, not just for the ones the filter
    /// would have removed.
    #[test]
    fn never_pushes_through_window() {
        let window = LogicalPlan::Window {
            input: Box::new(scan(1, vec![])),
            windows: vec![],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(window),
            predicate: eq(col(0, 0), lit(1)),
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "WINDOW must be an unconditional pushdown barrier"
        );
    }

    #[test]
    fn never_pushes_a_predicate_containing_a_subquery() {
        let subplan = scan(2, vec![]);
        let project = LogicalPlan::Project {
            input: Box::new(scan(1, vec![])),
            exprs: vec![(col(0, 0), "a".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::Subquery {
                kind: crate::SubqueryKind::Exists,
                subplan: Box::new(subplan),
                operand: None,
            },
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a predicate containing a subquery must never move, even through a Project"
        );
    }

    #[test]
    fn never_pushes_a_predicate_containing_a_set_returning_function() {
        let project = LogicalPlan::Project {
            input: Box::new(scan(1, vec![])),
            exprs: vec![(col(0, 0), "a".to_string())],
        };
        let srf = Expr::SetReturning {
            func: crate::FuncId(Oid(1066)), // generate_series, per expr.rs's own tests
            args: vec![lit(1), lit(10)],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::IsNull {
                arg: Box::new(srf),
                negated: true,
            },
        };

        assert!(
            FilterPushdown.rewrite(&plan).is_none(),
            "a predicate containing a set-returning function must never move"
        );
    }

    #[test]
    fn a_filter_with_no_predicate_left_to_move_reports_no_change() {
        // Nothing above a bare Scan should ever report a change.
        let plan = scan(1, vec![eq(col(0, 0), lit(1))]);
        assert!(FilterPushdown.rewrite(&plan).is_none());
    }

    /// The driver's fixpoint detection depends on repeated application
    /// eventually reporting no change. Push once, then push the *result*
    /// again and confirm it is now a true no-op.
    #[test]
    fn a_second_pass_over_an_already_pushed_plan_is_a_true_no_op() {
        let project = LogicalPlan::Project {
            input: Box::new(scan(1, vec![])),
            exprs: vec![(col(0, 0), "a".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: eq(col(0, 0), lit(1)),
        };

        let once = FilterPushdown.rewrite(&plan).expect("first pass must push");
        assert!(
            FilterPushdown.rewrite(&once).is_none(),
            "a plan with nothing left to push must report no change"
        );
    }

    #[test]
    fn sort_is_left_as_a_barrier_for_now() {
        // Documented scope gap: plain Sort is safe to push through in
        // principle, but is not implemented here (see the module docs).
        // Pin that it is at least *safe* -- the predicate stays put rather
        // than being dropped or miscomputed.
        let sort = LogicalPlan::Sort {
            input: Box::new(scan(1, vec![])),
            keys: vec![SortKey {
                expr: col(0, 0),
                descending: false,
                nulls_first: false,
            }],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(sort),
            predicate: eq(col(0, 0), lit(1)),
        };

        assert!(FilterPushdown.rewrite(&plan).is_none());
    }
}
