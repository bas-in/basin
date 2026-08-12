//! Subquery decorrelation.
//!
//! # Why this rule exists
//!
//! `docs/migration/df-removal/05-optimizer-rules.md` ran a rule-by-rule
//! ablation against real DataFusion 53 and found that the published 113×
//! correlated-subquery benchmark win depends on `scalar_subquery_to_join`
//! (980 LOC) together with the shared correlated-pull-up machinery
//! (`decorrelate.rs`, 513 LOC in DataFusion) — "the real complexity sink:
//! correlated-column set tracking, empty-batch evaluation, pull-through-
//! aggregate legality." `decorrelate_predicate_subquery` (1,780 LOC, `EXISTS`/
//! `IN`/`NOT IN`) has no *headline* number but is "pervasive in ORM SQL." This
//! file is the increment that keeps that win: without it, every correlated
//! `EXISTS`/`IN`/scalar subquery re-executes once per outer row (a nested-loop
//! re-scan), which is exactly the shape the 113× measures the cost of.
//!
//! # The four transforms
//!
//! All four apply only to a subquery expression sitting as one conjunct of a
//! [`LogicalPlan::Filter`]'s predicate (after splitting `AND`, the same way
//! [`super::pushdown`] does) — matching the task's own scope, "in a filter."
//! A subquery anywhere else (a `Project` target list, nested inside `OR`,
//! inside `CASE`, ...) is left untouched.
//!
//! 1. `EXISTS (correlated subquery)` → [`JoinKind::LeftSemi`] on the
//!    correlation predicate.
//! 2. `NOT EXISTS (...)` → [`JoinKind::LeftAnti`].
//! 3. `x IN (correlated subquery)` → `LeftSemi`, with `x = <the subquery's
//!    column>` folded into the join condition alongside the correlation
//!    predicate.
//! 4. A correlated **scalar** subquery used in a comparison (`col > (SELECT
//!    ...)`) → [`JoinKind::Left`] against a version of the subquery's
//!    aggregate re-grouped by the correlation key, with the original
//!    comparison re-applied above the join against the aggregate's result
//!    column. See [`decorrelate_scalar`] for why this is gated so tightly.
//!
//! # Correctness traps, and how each is prevented
//!
//! 1. **`NOT IN` must never become an anti-join.** Under three-valued logic,
//!    if the subquery yields *any* `NULL`, `x NOT IN (...)` is `NULL` for
//!    *every* outer row, not `true` for the ones with no match — so a naive
//!    anti-join (which returns exactly the outer rows with no match) returns
//!    rows Postgres would filter out. Basin's [`Schema`](crate::Schema) has no
//!    nullability information yet (confirmed: it is `Vec<(String, PgType)>`,
//!    no `NOT NULL` flag), so there is no way to *prove* the subquery's column
//!    is non-nullable, and "probably fine" is not a standard this file uses
//!    for a wrong-answer bug. [`attempt_decorrelate`] therefore refuses every
//!    [`SubqueryKind::NotIn`] unconditionally — correlated or not — full stop,
//!    until real nullability tracking exists. See
//!    `not_in_is_never_decorrelated_even_when_correlated` and
//!    `not_in_is_never_decorrelated_even_when_uncorrelated`.
//!
//!    Contrast with plain (positive) `IN`, which this file *does*
//!    decorrelate: in a `WHERE`-position predicate, `NULL` and `false` have
//!    the same observable effect (the row is excluded either way), and a
//!    semi-join's "at least one match" test agrees with `IN`'s truth value
//!    under exactly that collapse. `NOT IN`'s `NULL` case has no such
//!    equivalent collapse — see trap 1 above — which is the one place the two
//!    directions genuinely differ.
//!
//! 2. **An uncorrelated subquery must never become a join.** It has no
//!    correlation predicate to join on, and is better evaluated once rather
//!    than once per outer row (a join over a constant relation is strictly
//!    more expensive than a value computed once and reused). [`strip_correlation`]
//!    returns `None` — refusing outright, not guessing — whenever it cannot
//!    find at least one predicate that mentions [`OUTER_REF`] (see below), and
//!    every case in [`attempt_decorrelate`] treats that `None` as "leave this
//!    conjunct exactly where it is." See `uncorrelated_exists_is_left_untouched`,
//!    `uncorrelated_in_is_left_untouched`, `uncorrelated_scalar_subquery_is_left_untouched`.
//!
//! 3. **A scalar subquery producing more than one row is a Postgres runtime
//!    error** (`21000`, `cardinality_violation`), not a silently-picked row.
//!    Decorrelating a *bare* correlated scalar subquery (no aggregate — e.g.
//!    `(SELECT price FROM catalog WHERE catalog.sku = outer.sku)` where `sku`
//!    is not unique) into a plain join would hide that error: the join would
//!    just emit one row per match, silently fanning the outer row out instead
//!    of erroring. This file only decorrelates the shape it can *prove*
//!    caps at one row per correlation-key value: the subquery's residual (with
//!    the correlation predicate removed) must be exactly an ungrouped
//!    [`LogicalPlan::Aggregate`] (`group` empty, no `grouping_sets`) — the
//!    standard "scalar aggregate" shape, which by definition folds arbitrarily
//!    many input rows down to one. [`decorrelate_scalar`] refuses (returns
//!    `None`) for every other subplan shape, including a bare un-aggregated
//!    correlated column, *precisely so a query whose subquery can violate the
//!    one-row rule keeps evaluating it as a real per-row subquery* rather than
//!    silently becoming a join that cannot report the violation. See
//!    `scalar_subquery_without_an_aggregate_is_refused` and
//!    `scalar_subquery_with_its_own_group_by_is_refused`.
//!
//! 4. **A rule must report `None` when nothing changed.** [`Decorrelate::rewrite`]
//!    always rebuilds the whole tree and compares it to the input with
//!    `PartialEq`, the same pattern [`super::pushdown::FilterPushdown`] uses —
//!    see `no_op_plan_returns_none` and
//!    `a_second_pass_over_an_already_decorrelated_plan_is_a_true_no_op`. This
//!    is what lets [`super::driver::optimize`]'s fixpoint terminate instead of
//!    looping for the full [`super::driver::MAX_PASSES`] budget on every
//!    query.
//!
//! # A convention this file invents, and why
//!
//! Marking a correlated reference apart from an ordinary one needs *some* bit
//! of information `Expr` does not carry yet. [`opt::projection`](super::projection)
//! already says so explicitly: "The ablation doc confirms real correlated
//! plans keep an `outer_ref` that survives optimization untouched — Basin's
//! `Expr` has no equivalent marker yet, so there is no safe way for this rule
//! to tell a correlated reference apart from an ordinary one" — which is
//! exactly why that rule refuses to touch [`LogicalPlan::LateralJoin`] at all.
//! Decorrelation cannot take the same way out (its entire job *is* telling
//! correlated references apart), so this file defines a minimal, local,
//! documented convention instead of guessing: within a subquery's own
//! `subplan`, [`ColumnRef::relation`] value [`OUTER_REF`] (`1`) marks a column
//! reaching into the enclosing query's current row, exactly the way
//! `ColumnRef::relation == 1` already marks "the right side" inside a
//! [`LogicalPlan::Join`]'s `on`/`filter` (see [`super::pushdown`]'s
//! `touches_relation`/`rebase_right`, and [`super::projection`]'s
//! `collect_columns(expr, 0/1, ...)` for `Join::on`/`filter`). This is not a
//! new idea, just the *same* idea applied to a subquery boundary instead of a
//! join boundary — relation `0` is always "my own scope," `1` is always "the
//! other side," and a subquery's "other side" is the enclosing query.
//!
//! No real lowering path constructs this today — `SubqueryLowerer::lower`
//! (`lower/expr.rs`) recurses back into `lower_select` with a *fresh* `Scope`
//! that has no access to the outer scope at all, so a genuinely correlated
//! reference cannot resolve yet and correlated-subquery lowering is a
//! separate, later increment. That is precisely why this file's tests build
//! subplans by hand with `outer_col`/`local_col` (below) rather than by
//! parsing SQL — this rule is exercised exactly the same way `driver.rs`'s own
//! tests exercise the fixpoint driver, structurally, ahead of the lowering
//! path that will eventually produce these shapes for real.
//!
//! This file also reuses, independently, [`super::pushdown`]'s private
//! `AND_OP` sentinel-`OpId` convention for recognizing/reconstructing `AND`
//! (documented there: `Expr` has no dedicated boolean-connective variant yet).
//! It is redefined here rather than imported because it is `pushdown.rs`'s own
//! private convention, but it is bit-for-bit the same value (`u32::MAX`) on
//! purpose: a predicate `FilterPushdown` re-wrapped in a `Filter` using that
//! sentinel must still split apart correctly if `Decorrelate` sees it in a
//! later fixpoint pass, and vice versa.
//!
//! [`SubqueryKind::In`]/[`NotIn`](SubqueryKind::NotIn) carry no operator —
//! confirmed against `lower_sub_link`: Postgres represents plain `IN` with an
//! *empty* `oper_name`, unlike `x = ANY (...)`, which resolves and stores a
//! real `OpId`. Building the join condition `x = <subquery column>` this file
//! needs for transform 3 therefore needs *some* `OpId`, and there is no
//! `OperatorResolver` seam available inside the optimizer (that trait exists
//! only in `lower::expr`, driven by a real catalog this crate does not have).
//! Rather than silently guessing a real `pg_operator` oid from a `Schema` this
//! file mostly cannot even compute (`crate::schema::output_schema` fails on
//! anything rooted in a catalog-less `Scan` — i.e. on every subquery over a
//! real table), this file defines [`IN_EQ_OP`], a second private sentinel
//! `OpId`, on exactly the same footing as `AND_OP`: a documented placeholder
//! for "the equality `IN` implies," to be recognized the same way `AND_OP`
//! already must be by anything that consumes a Basin-optimized plan, until an
//! `OperatorResolver` is wired into the optimizer too.
//!
//! # What this file does not do
//!
//! - **`x op ANY/ALL (subquery)`** is not decorrelated. `SubqueryKind::Any`/
//!   `All` carry a real, type-resolved `OpId` already (unlike plain `IN`), so
//!   decorrelating them would be strictly *more* principled than the `IN`
//!   path above, not less — but DataFusion's own `RewriteSetComparison`
//!   (129 LOC) is rated **NICE** in the ablation doc precisely because Basin's
//!   `AnyAllToScalarSubquery` pre-empts it, so no published number depends on
//!   this file also covering it. Left for a later increment.
//! - **Correlation nested under `Sort`/`Limit`/`Join`/`Window` inside the
//!   subquery's own plan** is not pulled up. [`strip_correlation`] only
//!   descends through `Project`/`Aggregate` — both of which preserve
//!   per-row filtering semantics regardless of what sits below them — looking
//!   for the first `Filter`. A correlation predicate stuck below a `Sort`
//!   inside the subquery (rare — a subquery's own `ORDER BY` is usually
//!   irrelevant to `EXISTS`/`IN`/a scalar aggregate and lowering may drop it)
//!   is left as an un-decorrelated subquery, exactly as
//!   `push_down_filter`'s own module docs describe `Sort`/`Distinct` as "left
//!   for a later increment" barriers rather than guessed through.
//! - **Equijoin extraction into `Join::on`** does not happen here. Every join
//!   this file builds puts its condition in `Join::filter` and leaves `on`
//!   empty (Nested Loop rather than Hash — see `display.rs`'s `method`
//!   selection) except for transform 4, which needs concrete grouping
//!   expressions anyway and builds `on` directly from them. Splitting a
//!   general `Join::filter` equality into `on` is `extract_equijoin_predicate`'s
//!   job — a separate **MUST** rule in the ablation doc's build order, not
//!   part of this one.
//! - **A `NOT (EXISTS (...))`/`NOT (x IN (...))` spelling** (as opposed to the
//!   `NotExists`/`NotIn` variants) is not specially unwrapped. `lower_bool_expr`
//!   (`lower/expr.rs`) already collapses *either* spelling into
//!   `SubqueryKind::NotExists`/`NotIn` during lowering — "this is what lets
//!   the optimizer [rely on it]," per that file's own comment — so a bare
//!   `Unary { op: NOT, arg: Subquery { kind: Exists, .. } }` should not occur
//!   in a plan this rule ever sees. If one is hand-built anyway (bypassing
//!   that normalization), it is simply left alone: not the shape this rule
//!   matches, and leaving it alone is never wrong, only a missed optimization.

use basin_pgtype::Oid;

use crate::{ColumnRef, Expr, JoinKind, LogicalPlan, OpId, SubqueryKind, Subscript};

use super::OptimizerRule;

/// See "A convention this file invents" in the module docs. Bit-for-bit the
/// same sentinel `super::pushdown::FilterPushdown` uses privately, and for the
/// same reason (`Expr` has no dedicated `AND` variant yet) — redefined here
/// rather than imported because it is that file's own private convention.
const AND_OP: OpId = OpId(Oid(u32::MAX));

/// Placeholder for "the equality Postgres's `IN` implies" — see "A convention
/// this file invents" in the module docs for why a real, type-resolved
/// `pg_operator` `OpId` is not available here. Chosen one below `AND_OP`'s
/// `u32::MAX` sentinel so the two can never collide with each other or with
/// any real `pg_operator` oid (the largest of which is in the low thousands,
/// per `basin_pgtype::operator::OPERATORS`).
const IN_EQ_OP: OpId = OpId(Oid(u32::MAX - 1));

/// Within a subquery's own `subplan`, the [`ColumnRef::relation`] value that
/// marks a reference reaching into the enclosing query's current row. See "A
/// convention this file invents" in the module docs.
const OUTER_REF: u16 = 1;
/// The ordinary, non-correlated relation index a subquery's own columns use —
/// same as everywhere else in this crate (`ColumnRef::relation`'s own doc
/// comment: "0 for a single input").
const LOCAL_REF: u16 = 0;

/// Subquery decorrelation — see the module docs for the full contract.
pub struct Decorrelate;

impl OptimizerRule for Decorrelate {
    fn name(&self) -> &'static str {
        // Basin's single rule covers what DataFusion splits into
        // `decorrelate_predicate_subquery` (EXISTS/IN/NOT IN) and
        // `scalar_subquery_to_join` (scalar); named for Basin's own shape
        // rather than either DF name specifically.
        "decorrelate_subquery"
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let rewritten = rewrite_node(plan);
        (rewritten != *plan).then_some(rewritten)
    }
}

/// Recursively rewrite `plan`: every child is rewritten first (so a `Filter`
/// nested anywhere is reached — mirrors `pushdown.rs`'s own `rewrite_node`),
/// and if `plan` itself is a `Filter`, its predicate's conjuncts are each
/// offered to [`attempt_decorrelate`].
fn rewrite_node(plan: &LogicalPlan) -> LogicalPlan {
    let rebuilt = rebuild_children(plan);
    if let LogicalPlan::Filter { input, predicate } = rebuilt {
        decorrelate_filter(*input, predicate)
    } else {
        rebuilt
    }
}

/// Reconstruct `plan` with every immediate child passed through
/// [`rewrite_node`]. Pure tree-shape plumbing, structurally identical to
/// `pushdown.rs`'s function of the same name (reproduced locally since that
/// one is private to its own file).
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

// ─── The four transforms ────────────────────────────────────────────────

/// Split `predicate` into conjuncts and offer each to [`attempt_decorrelate`]
/// in turn, folding a successful one into `base` (which subsequent conjuncts
/// then build on) and leaving the rest wrapped in a `Filter` directly above
/// whatever `base` ends up being.
fn decorrelate_filter(input: LogicalPlan, predicate: Expr) -> LogicalPlan {
    let conjuncts = split_conjunction(predicate);
    let mut base = input;
    let mut remaining = Vec::new();
    for conjunct in conjuncts {
        match attempt_decorrelate(&base, &conjunct) {
            Some(new_base) => base = new_base,
            None => remaining.push(conjunct),
        }
    }
    wrap_filter(base, remaining)
}

/// Try to decorrelate one conjunct against `base`. Returns the full
/// replacement for `base` (a new `Join`, or a `Filter` over a new `Join` for
/// transform 4) on success; `None` — never a partial or best-effort result —
/// whenever `conjunct` is not one of the four recognized shapes, or is one of
/// them but a correctness trap (see the module docs) applies. `None` always
/// means "leave this conjunct exactly where it was."
fn attempt_decorrelate(base: &LogicalPlan, conjunct: &Expr) -> Option<LogicalPlan> {
    match conjunct {
        Expr::Subquery {
            kind: SubqueryKind::Exists,
            subplan,
            operand: None,
        } => decorrelate_exists_like(base, subplan, JoinKind::LeftSemi),

        Expr::Subquery {
            kind: SubqueryKind::NotExists,
            subplan,
            operand: None,
        } => decorrelate_exists_like(base, subplan, JoinKind::LeftAnti),

        Expr::Subquery {
            kind: SubqueryKind::In,
            subplan,
            operand: Some(operand),
        } => decorrelate_in(base, operand, subplan),

        // Trap 1 (see module docs): NOT IN must never become an anti-join,
        // correlated or not.
        Expr::Subquery {
            kind: SubqueryKind::NotIn,
            ..
        } => None,

        // Transform 4: a scalar subquery embedded in a top-level comparison.
        Expr::Binary { op, lhs, rhs } => decorrelate_scalar_comparison(base, *op, lhs, rhs),

        // Any/All, a bare Scalar subquery outside a comparison, and anything
        // else: not a shape this file decorrelates (see "What this file does
        // not do").
        _ => None,
    }
}

/// Transforms 1/2: `EXISTS`/`NOT EXISTS`. `kind` is `LeftSemi` or `LeftAnti`.
fn decorrelate_exists_like(
    base: &LogicalPlan,
    subplan: &LogicalPlan,
    kind: JoinKind,
) -> Option<LogicalPlan> {
    let normalized = rewrite_node(subplan);
    let (residual, corr) = strip_correlation(&normalized)?;
    Some(LogicalPlan::Join {
        left: Box::new(base.clone()),
        right: Box::new(residual),
        kind,
        on: vec![],
        filter: Some(swap_relations(&corr)),
    })
}

/// Transform 3: `x IN (correlated subquery)`.
fn decorrelate_in(
    base: &LogicalPlan,
    operand: &Expr,
    subplan: &LogicalPlan,
) -> Option<LogicalPlan> {
    let normalized = rewrite_node(subplan);
    let (residual, corr) = strip_correlation(&normalized)?;
    // A subquery on the right of IN produces exactly one column by SQL
    // grammar; this file only decorrelates when it can prove that
    // structurally (see `is_single_column`) rather than assume it.
    if !is_single_column(&residual) {
        return None;
    }

    let corr_filter = swap_relations(&corr);
    // The residual's sole output column, from the new join's right side.
    let value_col = Expr::Column(ColumnRef {
        relation: 1,
        index: 0,
        name: "in_value".to_string(),
    });
    let membership = Expr::Binary {
        op: IN_EQ_OP,
        lhs: Box::new(operand.clone()),
        rhs: Box::new(value_col),
    };
    Some(LogicalPlan::Join {
        left: Box::new(base.clone()),
        right: Box::new(residual),
        kind: JoinKind::LeftSemi,
        on: vec![],
        filter: Some(and(corr_filter, membership)),
    })
}

/// Transform 4 entry point: recognize `<scalar subquery> op <expr>` or
/// `<expr> op <scalar subquery>` as one top-level conjunct. Refuses if the
/// non-subquery side itself contains a subquery anywhere (out of scope: this
/// file handles one decorrelation per conjunct, not simultaneous ones).
fn decorrelate_scalar_comparison(
    base: &LogicalPlan,
    op: OpId,
    lhs: &Expr,
    rhs: &Expr,
) -> Option<LogicalPlan> {
    let has_subquery = |e: &Expr| e.any(&mut |x| matches!(x, Expr::Subquery { .. }));
    match (lhs, rhs) {
        (
            Expr::Subquery {
                kind: SubqueryKind::Scalar,
                operand: None,
                subplan,
            },
            other,
        ) if !has_subquery(other) => decorrelate_scalar(base, op, other, subplan, true),

        (
            other,
            Expr::Subquery {
                kind: SubqueryKind::Scalar,
                operand: None,
                subplan,
            },
        ) if !has_subquery(other) => decorrelate_scalar(base, op, other, subplan, false),

        _ => None,
    }
}

/// Transform 4: build the `LEFT JOIN` against a re-grouped aggregate, and
/// re-apply the original comparison above it. See trap 3 in the module docs
/// for why the residual is required to be exactly an ungrouped `Aggregate`.
fn decorrelate_scalar(
    base: &LogicalPlan,
    op: OpId,
    other_side: &Expr,
    subplan: &LogicalPlan,
    subquery_is_lhs: bool,
) -> Option<LogicalPlan> {
    let normalized = rewrite_node(subplan);
    let (residual, corr) = strip_correlation(&normalized)?;

    // Trap 3: only a *provably single-row* subquery may become a join. The
    // one shape this file can prove that for is an ungrouped scalar
    // aggregate — see the module docs' trap 3 for the full reasoning and
    // `scalar_subquery_without_an_aggregate_is_refused` /
    // `scalar_subquery_with_its_own_group_by_is_refused` for what gets
    // refused and why.
    let LogicalPlan::Aggregate {
        input,
        group,
        aggs,
        grouping_sets,
    } = &residual
    else {
        return None;
    };
    if !group.is_empty() || grouping_sets.is_some() || aggs.len() != 1 {
        return None;
    }

    // The correlation predicate must decompose into clean single-column
    // equalities to serve as grouping expressions — see `split_equalities`.
    let pairs = split_equalities(&corr)?;
    if pairs.is_empty() {
        return None;
    }

    let new_group: Vec<Expr> = pairs.iter().map(|(local, _)| local.clone()).collect();
    let grouped = LogicalPlan::Aggregate {
        input: input.clone(),
        group: new_group,
        aggs: aggs.clone(),
        grouping_sets: None,
    };

    // `on`: for each correlation pair, (outer key rebased onto the new
    // join's left, the grouped aggregate's own output column for that key).
    // Grouping keys occupy the aggregate's output positions
    // `0..pairs.len()`, per `crate::schema::output_schema`'s Aggregate case
    // ("the grouping columns first ... then the aggregates").
    let on: Vec<(Expr, Expr)> = pairs
        .iter()
        .enumerate()
        .map(|(i, (_, outer))| {
            (
                force_relation(outer, 0),
                Expr::Column(ColumnRef {
                    relation: 1,
                    index: i as u16,
                    name: "corr_key".to_string(),
                }),
            )
        })
        .collect();

    let join = LogicalPlan::Join {
        left: Box::new(base.clone()),
        right: Box::new(grouped),
        kind: JoinKind::Left,
        on,
        filter: None,
    };

    // The aggregate's own result sits immediately after its grouping keys.
    let agg_col = Expr::Column(ColumnRef {
        relation: 1,
        index: pairs.len() as u16,
        name: "agg_result".to_string(),
    });
    let (new_lhs, new_rhs) = if subquery_is_lhs {
        (agg_col, other_side.clone())
    } else {
        (other_side.clone(), agg_col)
    };
    let comparison = Expr::Binary {
        op,
        lhs: Box::new(new_lhs),
        rhs: Box::new(new_rhs),
    };
    Some(LogicalPlan::Filter {
        input: Box::new(join),
        predicate: comparison,
    })
}

// ─── Correlation extraction ─────────────────────────────────────────────

/// Look for a correlation predicate reachable by descending through any
/// number of `Project`/`Aggregate` wrappers (both preserve per-row filtering
/// semantics regardless of what sits below them) from `plan`'s root, and
/// split it away from whatever the subquery still needs to evaluate on its
/// own. Returns `None` — refusing, not guessing — whenever no [`OUTER_REF`]
/// column reference is found this way. See "What this file does not do" in
/// the module docs for the shapes this conservative walk does not attempt.
fn strip_correlation(plan: &LogicalPlan) -> Option<(LogicalPlan, Expr)> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let conjuncts = split_conjunction(predicate.clone());
            let mut corr = Vec::new();
            let mut local = Vec::new();
            for c in conjuncts {
                if references_outer(&c) {
                    corr.push(c);
                } else {
                    local.push(c);
                }
            }
            if corr.is_empty() {
                return None; // trap 2: nothing correlated here — refuse.
            }
            let corr_pred = conjunction(corr).expect("corr is non-empty");
            let residual = wrap_filter((**input).clone(), local);
            Some((residual, corr_pred))
        }

        LogicalPlan::Project { input, exprs } => {
            let (new_input, corr) = strip_correlation(input)?;
            Some((
                LogicalPlan::Project {
                    input: Box::new(new_input),
                    exprs: exprs.clone(),
                },
                corr,
            ))
        }

        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } => {
            let (new_input, corr) = strip_correlation(input)?;
            Some((
                LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group: group.clone(),
                    aggs: aggs.clone(),
                    grouping_sets: grouping_sets.clone(),
                },
                corr,
            ))
        }

        _ => None,
    }
}

/// Whether `e` references [`OUTER_REF`] anywhere. Built on [`Expr::any`],
/// which already stops at an `Expr::Subquery`'s own `subplan` boundary, so a
/// correlated reference two subquery levels down is correctly not attributed
/// to this level.
fn references_outer(e: &Expr) -> bool {
    e.any(&mut |x| matches!(x, Expr::Column(c) if c.relation == OUTER_REF))
}

/// Whether `e` references [`LOCAL_REF`] anywhere — the subquery's own,
/// non-correlated columns.
fn references_local(e: &Expr) -> bool {
    e.any(&mut |x| matches!(x, Expr::Column(c) if c.relation == LOCAL_REF))
}

/// Decompose a correlation predicate into clean `(local, outer)` column-pair
/// equalities, refusing (`None`) if any conjunct is not exactly "one side is
/// pure `LOCAL_REF`, the other is pure `OUTER_REF`" — e.g. `t.a + 1 =
/// outer.b` or a predicate mixing both markers on one side is refused, not
/// guessed at. Required only by transform 4, which needs concrete grouping
/// expressions rather than a single opaque boolean predicate; transforms 1–3
/// use the correlation predicate as-is via [`swap_relations`] and never call
/// this.
fn split_equalities(pred: &Expr) -> Option<Vec<(Expr, Expr)>> {
    let mut out = Vec::new();
    for conjunct in split_conjunction(pred.clone()) {
        let Expr::Binary { lhs, rhs, .. } = &conjunct else {
            return None;
        };
        let lhs_local = references_local(lhs);
        let lhs_outer = references_outer(lhs);
        let rhs_local = references_local(rhs);
        let rhs_outer = references_outer(rhs);
        match (lhs_local, lhs_outer, rhs_local, rhs_outer) {
            (true, false, false, true) => out.push(((**lhs).clone(), (**rhs).clone())),
            (false, true, true, false) => out.push(((**rhs).clone(), (**lhs).clone())),
            _ => return None,
        }
    }
    Some(out)
}

/// Whether `plan` structurally, provably produces exactly one output column
/// — without needing `crate::schema::output_schema` (which fails on anything
/// rooted in a catalog-less `Scan`, i.e. on essentially every real subquery).
/// Used only to justify treating a residual subquery's sole column as "the"
/// value an `IN` predicate compares against.
fn is_single_column(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project { exprs, .. } => exprs.len() == 1,
        LogicalPlan::Scan { projection, .. } => projection.len() == 1,
        LogicalPlan::Aggregate {
            group,
            aggs,
            grouping_sets,
            ..
        } => grouping_sets.is_none() && group.len() + aggs.len() == 1,
        // A predicate narrows rows, never columns.
        LogicalPlan::Filter { input, .. } => is_single_column(input),
        _ => false,
    }
}

// ─── Column-reference remapping ─────────────────────────────────────────

/// Swap [`LOCAL_REF`]/[`OUTER_REF`] (`0`/`1`) throughout `expr` — turning a
/// subquery-internal correlation predicate into a predicate addressed the way
/// `Join::filter` is everywhere else in this crate: `0` = the new join's left
/// child (the former enclosing query, `OUTER_REF` before the swap), `1` = its
/// right child (the former subquery's own scope, `LOCAL_REF` before the
/// swap). A plain involution — any other relation index (there should not be
/// one, but this is not the place to assert that) passes through unchanged.
fn swap_relations(expr: &Expr) -> Expr {
    map_columns(expr, &mut |c| {
        let relation = match c.relation {
            LOCAL_REF => OUTER_REF,
            OUTER_REF => LOCAL_REF,
            other => other,
        };
        Expr::Column(ColumnRef {
            relation,
            index: c.index,
            name: c.name.clone(),
        })
    })
}

/// Force every column reference in `expr` to `relation`, regardless of its
/// original tag. Only safe to call once a caller has already confirmed (via
/// [`references_local`]/[`references_outer`]) that `expr` uses exactly one
/// relation throughout — [`split_equalities`] is the only such caller.
fn force_relation(expr: &Expr, relation: u16) -> Expr {
    map_columns(expr, &mut |c| {
        Expr::Column(ColumnRef {
            relation,
            index: c.index,
            name: c.name.clone(),
        })
    })
}

/// Rewrite every [`Expr::Column`] in `expr` via `f`, otherwise reconstructing
/// the tree unchanged. Structurally identical to `pushdown.rs`'s function of
/// the same name (reproduced locally since that one is private to its own
/// file) including the same deliberate boundary: does not descend into
/// [`Expr::Subquery`]'s `subplan`, since a nested subquery's column indices
/// belong to its own plan, not this one.
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
        // Neither can legally appear inside a WHERE-position predicate — see
        // `pushdown.rs`'s `map_columns` for the same reasoning. Left
        // structurally alone rather than panicking.
        Expr::Aggregate { .. } | Expr::Window { .. } => expr.clone(),
        // A nested subquery's own column indices belong to its own plan.
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

// ─── Conjunction splitting/joining ──────────────────────────────────────
//
// Reproduced locally from `pushdown.rs` — see [`AND_OP`]'s doc comment for
// why this must stay bit-for-bit compatible with that file's own copy.

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

/// Wrap `plan` in a `Filter` over the conjunction of `preds`, or return `plan`
/// unchanged if there is nothing left to wrap.
fn wrap_filter(plan: LogicalPlan, preds: Vec<Expr>) -> LogicalPlan {
    match conjunction(preds) {
        Some(predicate) => LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, Datum, SnapshotId, TableId};
    use basin_pgtype::PgType;

    fn scan(table: u32, projection: Vec<ColId>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(table),
            projection,
            filters: vec![],
            snapshot: SnapshotId(0),
        }
    }

    /// A column of the current query level (an outer plan, or a subquery's
    /// own local scope) — `relation: 0`, per `LOCAL_REF`.
    fn col(index: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation: LOCAL_REF,
            index,
            name: format!("c{index}"),
        })
    }

    /// A reference, from *inside a subquery's subplan*, to the enclosing
    /// query's current row — `relation: 1`, per `OUTER_REF`. See the module
    /// docs' "A convention this file invents" section.
    fn outer_col(index: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation: OUTER_REF,
            index,
            name: format!("outer{index}"),
        })
    }

    fn lit(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    /// `int4 = int4`, oid 96 — a real `pg_operator` row, same helper
    /// `pushdown.rs`'s own tests use.
    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            op: OpId(Oid(96)),
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    /// `int4 > int4`, oid 147 in a live `pg_operator` — used only to prove
    /// transform 4 preserves whatever comparison operator the original SQL
    /// used, not to have that operator interpreted by anything in this file.
    fn gt(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            op: OpId(Oid(147)),
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn exists(subplan: LogicalPlan) -> Expr {
        Expr::Subquery {
            kind: SubqueryKind::Exists,
            subplan: Box::new(subplan),
            operand: None,
        }
    }

    fn not_exists(subplan: LogicalPlan) -> Expr {
        Expr::Subquery {
            kind: SubqueryKind::NotExists,
            subplan: Box::new(subplan),
            operand: None,
        }
    }

    fn in_subquery(operand: Expr, subplan: LogicalPlan) -> Expr {
        Expr::Subquery {
            kind: SubqueryKind::In,
            subplan: Box::new(subplan),
            operand: Some(Box::new(operand)),
        }
    }

    fn not_in_subquery(operand: Expr, subplan: LogicalPlan) -> Expr {
        Expr::Subquery {
            kind: SubqueryKind::NotIn,
            subplan: Box::new(subplan),
            operand: Some(Box::new(operand)),
        }
    }

    fn scalar_subquery(subplan: LogicalPlan) -> Expr {
        Expr::Subquery {
            kind: SubqueryKind::Scalar,
            subplan: Box::new(subplan),
            operand: None,
        }
    }

    #[test]
    fn rule_name_is_stable() {
        assert_eq!(Decorrelate.name(), "decorrelate_subquery");
    }

    // ── Transform 1: EXISTS ────────────────────────────────────────────

    #[test]
    fn correlated_exists_becomes_left_semi_join() {
        // WHERE EXISTS (SELECT ... FROM t WHERE t.a = outer.b)
        let subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0)])),
            predicate: eq(col(0), outer_col(0)),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: exists(subplan),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } = rewritten
        else {
            panic!("expected a Join, got {rewritten:?}");
        };
        assert_eq!(*left, scan(1, vec![ColId(0)]));
        assert_eq!(
            *right,
            scan(2, vec![ColId(0)]),
            "the correlation Filter must be stripped, leaving the bare scan"
        );
        assert_eq!(kind, JoinKind::LeftSemi);
        assert!(on.is_empty());
        // relation 0 = outer (was OUTER_REF inside the subplan), relation 1 =
        // the new right side (was LOCAL_REF inside the subplan) — a clean
        // swap. Names are carried through unchanged by the swap.
        let expected = eq(
            Expr::Column(ColumnRef {
                relation: 1,
                index: 0,
                name: "c0".to_string(),
            }),
            Expr::Column(ColumnRef {
                relation: 0,
                index: 0,
                name: "outer0".to_string(),
            }),
        );
        assert_eq!(filter, Some(expected));
    }

    // ── Transform 2: NOT EXISTS ────────────────────────────────────────

    #[test]
    fn correlated_not_exists_becomes_left_anti_join() {
        let subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0)])),
            predicate: eq(col(0), outer_col(0)),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: not_exists(subplan),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Join { kind, .. } = rewritten else {
            panic!("expected a Join, got {rewritten:?}");
        };
        assert_eq!(kind, JoinKind::LeftAnti);
    }

    // ── Transform 3: IN ─────────────────────────────────────────────────

    #[test]
    fn correlated_in_becomes_left_semi_with_extra_join_condition() {
        // WHERE x IN (SELECT dept_id FROM depts WHERE depts.company = outer.company)
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1)])),
                predicate: eq(col(1), outer_col(0)),
            }),
            exprs: vec![(col(0), "dept_id".to_string())],
        };
        let x = col(0);
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: in_subquery(x.clone(), subplan),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = rewritten
        else {
            panic!("expected a Join, got {rewritten:?}");
        };
        assert_eq!(kind, JoinKind::LeftSemi);
        assert!(on.is_empty());

        let value_col = Expr::Column(ColumnRef {
            relation: 1,
            index: 0,
            name: "in_value".to_string(),
        });
        // The correlation predicate, swapped: relation 1 = the residual's own
        // scope (was LOCAL_REF inside the subplan), relation 0 = the new
        // join's left/outer side (was OUTER_REF). Names are carried through
        // unchanged.
        let corr_swapped = eq(
            Expr::Column(ColumnRef {
                relation: 1,
                index: 1,
                name: "c1".to_string(),
            }),
            Expr::Column(ColumnRef {
                relation: 0,
                index: 0,
                name: "outer0".to_string(),
            }),
        );
        let membership = Expr::Binary {
            op: IN_EQ_OP,
            lhs: Box::new(x),
            rhs: Box::new(value_col),
        };
        assert_eq!(filter, Some(and(corr_swapped, membership)));
    }

    // ── Trap 1: NOT IN ──────────────────────────────────────────────────

    /// Wrong answer this prevents: under three-valued logic, if the subquery
    /// yields any NULL, `x NOT IN (...)` is NULL (excluded) for every outer
    /// row, not `true` for the ones with no exact match. A naive anti-join
    /// only tests "no match," which disagrees with Postgres whenever the
    /// subquery's column can be NULL — and `Schema` has no nullability
    /// tracking to rule that out. Being correlated does not change this: the
    /// three-valued-logic hazard lives entirely in the *values* the subquery
    /// can produce, not in how it is scoped.
    #[test]
    fn not_in_is_never_decorrelated_even_when_correlated() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1)])),
                predicate: eq(col(1), outer_col(0)),
            }),
            exprs: vec![(col(0), "dept_id".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: not_in_subquery(col(0), subplan),
        };

        assert!(
            Decorrelate.rewrite(&plan).is_none(),
            "NOT IN must never become a join, even when correlated"
        );
    }

    #[test]
    fn not_in_is_never_decorrelated_even_when_uncorrelated() {
        let subplan = scan(2, vec![ColId(0)]);
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: not_in_subquery(col(0), subplan),
        };

        assert!(Decorrelate.rewrite(&plan).is_none());
    }

    // ── Trap 2: uncorrelated subqueries ────────────────────────────────

    /// Wrong answer/regression this prevents: an uncorrelated subquery has no
    /// join key at all. Turning it into a join anyway would be strictly
    /// worse than evaluating it once (and, absent a real correlation
    /// predicate, this file has nothing principled to join on).
    #[test]
    fn uncorrelated_exists_is_left_untouched() {
        let subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0)])),
            predicate: eq(col(0), lit(5)), // no OUTER_REF anywhere
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: exists(subplan),
        };

        assert!(
            Decorrelate.rewrite(&plan).is_none(),
            "an uncorrelated EXISTS must not become a join"
        );
    }

    #[test]
    fn uncorrelated_in_is_left_untouched() {
        let subplan = LogicalPlan::Project {
            input: Box::new(scan(2, vec![ColId(0)])),
            exprs: vec![(col(0), "v".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: in_subquery(col(0), subplan),
        };

        assert!(Decorrelate.rewrite(&plan).is_none());
    }

    #[test]
    fn uncorrelated_scalar_subquery_is_left_untouched() {
        let subplan = LogicalPlan::Aggregate {
            input: Box::new(scan(2, vec![ColId(0)])),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: crate::FuncId(Oid(2108)),
                args: vec![col(0)],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: gt(col(0), scalar_subquery(subplan)),
        };

        assert!(
            Decorrelate.rewrite(&plan).is_none(),
            "an uncorrelated scalar subquery must not become a join"
        );
    }

    // ── Transform 4: correlated scalar subquery ────────────────────────

    fn avg_agg() -> Expr {
        Expr::Aggregate {
            func: crate::FuncId(Oid(2101)), // avg(int4), any placeholder oid
            args: vec![col(1)],
            distinct: false,
            filter: None,
            order_by: vec![],
        }
    }

    #[test]
    fn correlated_scalar_subquery_becomes_left_outer_join_over_grouped_aggregate() {
        // WHERE salary > (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = outer.dept)
        let subplan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1)])), // dept, salary
                predicate: eq(col(0), outer_col(0)),
            }),
            group: vec![],
            aggs: vec![avg_agg()],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0), ColId(1)])), // dept, salary
            predicate: gt(col(1), scalar_subquery(subplan)),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Filter {
            input: join,
            predicate,
        } = rewritten
        else {
            panic!("expected a Filter over the new Join, got {rewritten:?}");
        };
        let LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } = *join
        else {
            panic!("expected a Join");
        };
        assert_eq!(*left, scan(1, vec![ColId(0), ColId(1)]));
        assert_eq!(kind, JoinKind::Left);
        assert!(filter.is_none());

        let LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } = *right
        else {
            panic!("expected the right side to be the re-grouped Aggregate");
        };
        assert_eq!(
            *input,
            scan(2, vec![ColId(0), ColId(1)]),
            "the correlation Filter must be stripped from beneath the aggregate"
        );
        // Re-grouped by the local side of the correlation predicate (dept,
        // column 0), not left ungrouped.
        assert_eq!(group, vec![col(0)]);
        assert_eq!(aggs, vec![avg_agg()]);
        assert!(grouping_sets.is_none());

        // on: outer.dept (rebased to the new join's left, relation 0) =
        // the aggregate's own group-key output column (relation 1, index 0).
        // `force_relation` only changes `relation`, so the original name
        // ("outer0", from `outer_col(0)`) is carried through unchanged.
        assert_eq!(
            on,
            vec![(
                Expr::Column(ColumnRef {
                    relation: 0,
                    index: 0,
                    name: "outer0".to_string()
                }),
                Expr::Column(ColumnRef {
                    relation: 1,
                    index: 0,
                    name: "corr_key".to_string()
                })
            )]
        );

        // The comparison is re-applied above the join, against the
        // aggregate's result column (index 1: right after the one grouping
        // key).
        let agg_col = Expr::Column(ColumnRef {
            relation: 1,
            index: 1,
            name: "agg_result".to_string(),
        });
        assert_eq!(predicate, gt(col(1), agg_col));
    }

    /// Confirms the transform is symmetric: `(SELECT ...) < salary` puts the
    /// aggregate column on the left of the re-applied comparison.
    #[test]
    fn correlated_scalar_subquery_on_the_left_of_the_comparison_is_also_decorrelated() {
        let subplan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1)])),
                predicate: eq(col(0), outer_col(0)),
            }),
            group: vec![],
            aggs: vec![avg_agg()],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0), ColId(1)])),
            predicate: gt(scalar_subquery(subplan), col(1)),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Filter { predicate, .. } = rewritten else {
            panic!("expected a Filter over the new Join");
        };
        let agg_col = Expr::Column(ColumnRef {
            relation: 1,
            index: 1,
            name: "agg_result".to_string(),
        });
        assert_eq!(predicate, gt(agg_col, col(1)));
    }

    // ── Trap 3: cardinality ─────────────────────────────────────────────

    /// Wrong answer this prevents: `(SELECT price FROM catalog WHERE
    /// catalog.sku = outer.sku)` can return more than one row if `sku` is not
    /// unique — real Postgres raises `21000 cardinality_violation` at
    /// runtime. A plain join over the un-aggregated residual would instead
    /// silently fan the outer row out across every match, hiding the error
    /// rather than reporting it. Refusing keeps this a real per-row subquery
    /// evaluation, which is where that check can still happen.
    #[test]
    fn scalar_subquery_without_an_aggregate_is_refused() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1)])),
                predicate: eq(col(0), outer_col(0)),
            }),
            exprs: vec![(col(1), "price".to_string())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0), ColId(1)])),
            predicate: gt(col(1), scalar_subquery(subplan)),
        };

        assert!(
            Decorrelate.rewrite(&plan).is_none(),
            "a correlated scalar subquery with no aggregate cannot be proven single-row; must not become a join"
        );
    }

    /// A subquery with its own explicit `GROUP BY` (unrelated to the outer
    /// correlation) is a second way the "ungrouped scalar aggregate" proof
    /// fails — also refused, for the same reason.
    #[test]
    fn scalar_subquery_with_its_own_group_by_is_refused() {
        let subplan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(2, vec![ColId(0), ColId(1), ColId(2)])),
                predicate: eq(col(0), outer_col(0)),
            }),
            group: vec![col(1)], // subquery's own GROUP BY
            aggs: vec![avg_agg()],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0), ColId(1)])),
            predicate: gt(col(1), scalar_subquery(subplan)),
        };

        assert!(Decorrelate.rewrite(&plan).is_none());
    }

    // ── Trap 4 / fixpoint honesty ───────────────────────────────────────

    #[test]
    fn no_op_plan_returns_none() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: eq(col(0), lit(1)),
        };
        assert!(Decorrelate.rewrite(&plan).is_none());
    }

    #[test]
    fn a_plan_with_no_filter_at_all_returns_none() {
        assert!(Decorrelate.rewrite(&scan(1, vec![ColId(0)])).is_none());
    }

    #[test]
    fn a_second_pass_over_an_already_decorrelated_plan_is_a_true_no_op() {
        let subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0)])),
            predicate: eq(col(0), outer_col(0)),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: exists(subplan),
        };

        let once = Decorrelate
            .rewrite(&plan)
            .expect("first pass must decorrelate");
        assert!(
            Decorrelate.rewrite(&once).is_none(),
            "a plan with nothing left to decorrelate must report no change"
        );
    }

    // ── Mixed predicates and nesting ────────────────────────────────────

    #[test]
    fn a_non_decorrelated_conjunct_stays_in_a_filter_above_the_new_join() {
        // WHERE a = 1 AND EXISTS (...)
        let subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0)])),
            predicate: eq(col(0), outer_col(0)),
        };
        let predicate = and(eq(col(0), lit(1)), exists(subplan));
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate,
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Filter { input, predicate } = rewritten else {
            panic!("expected a Filter wrapping the new Join, got {rewritten:?}");
        };
        assert!(matches!(*input, LogicalPlan::Join { .. }));
        assert_eq!(predicate, eq(col(0), lit(1)));
    }

    /// A correlated subquery nested inside another correlated subquery is
    /// also decorrelated: `attempt_decorrelate` recursively normalizes a
    /// subquery's own subplan via `rewrite_node` before extracting its
    /// correlation predicate.
    #[test]
    fn a_correlated_subquery_nested_inside_another_is_also_decorrelated() {
        // EXISTS (SELECT ... FROM t2 WHERE t2.a = outer.a
        //           AND EXISTS (SELECT ... FROM t3 WHERE t3.b = t2.b))
        let inner_subplan = LogicalPlan::Filter {
            input: Box::new(scan(3, vec![ColId(0)])),
            predicate: eq(col(0), outer_col(0)), // correlated to t2, one level up
        };
        let outer_subplan = LogicalPlan::Filter {
            input: Box::new(scan(2, vec![ColId(0), ColId(1)])),
            predicate: and(eq(col(0), outer_col(0)), exists(inner_subplan)),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(1, vec![ColId(0)])),
            predicate: exists(outer_subplan),
        };

        let rewritten = Decorrelate.rewrite(&plan).expect("must decorrelate");
        let LogicalPlan::Join { right, .. } = rewritten else {
            panic!("expected the outer EXISTS to become a Join");
        };
        // The right side (the former outer subquery's residual) must itself
        // now be a Join, not a Filter still containing a raw Expr::Subquery —
        // proof the inner EXISTS was decorrelated too, in the same pass.
        assert!(
            matches!(*right, LogicalPlan::Join { .. }),
            "the nested EXISTS inside the outer subquery must also be decorrelated: {right:?}"
        );
    }
}
