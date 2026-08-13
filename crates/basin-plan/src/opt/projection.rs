//! Projection pruning — the rule that sets `LogicalPlan::Scan { projection }`.
//!
//! The ablation in `docs/migration/df-removal/05-optimizer-rules.md` found
//! that DataFusion's `optimize_projections` changes **12 of 12** benchmarked
//! plan shapes, more than any other rule: "It is what sets `TableScan …
//! projection=[id, user_id, amount]`; without it the scan is `TableScan:
//! events` with no projection and the reader decodes every column. The 'we
//! only touch the projected columns' property that produces the columnar win
//! *is* this rule." This is that rule for Basin.
//!
//! # Algorithm
//!
//! A single top-down recursion, [`prune`], carrying the set of column
//! positions ("required") that whatever sits above a node actually reads from
//! it. At a [`LogicalPlan::Scan`] this terminates: any entry of `projection`
//! whose position was never demanded is dropped. Above the scan, `required`
//! is built up node by node from two sources — see [`prune`]'s per-variant
//! comments:
//!
//! - what the node's *own* parent asked for (translated through this node's
//!   shape), and
//! - what the node's *own* expressions read (a predicate, a join key, a sort
//!   key, a `GROUP BY` list, an aggregate's arguments — anything the node
//!   evaluates itself, whether or not it appears in that node's output). The
//!   task description for this rule is blunt about why this half is not
//!   optional: "Missing this produces wrong answers, not slow ones."
//!
//! Only `Scan.projection` ever shrinks. Every other node keeps exactly the
//! columns/expressions it already had — this rule does not, for instance,
//! drop an unused entry from a `Project`'s own expression list. That keeps
//! the rewrite mechanically simple: the only thing "pruning" ever does is
//! remove entries from one `Vec<ColId>`. But shrinking that one `Vec` changes
//! the *positions* of every column after the removed ones, and every
//! [`ColumnRef`] anywhere in the plan that pointed at the scan's old
//! numbering — directly, or through any number of column-count-preserving
//! nodes stacked on top of it — has to be renumbered to match. That
//! renumbering is what most of this file's code is: [`prune`] returns a
//! [`Remap`] alongside the rewritten node, and every caller applies it to its
//! own expressions before deciding what its own `Remap` is.
//!
//! # What this rule does not do (yet)
//!
//! - **Inside a [`LogicalPlan::SetOp`] arm**, both sides are pruned with the
//!   *same* required-position set (their schemas must match), but if one
//!   arm's own predicate/keys need a column the other doesn't, the two arms'
//!   `Remap`s can diverge — position 2 on the left and position 2 on the
//!   right would stop meaning "the same slot" post-pruning. `prune` detects
//!   this (the two `Remap`s stop being equal) and leaves that `SetOp`
//!   untouched rather than risk it. This essentially never fires in practice
//!   — a `UNION` arm is normally a `Project` (an explicit target list), and
//!   `Project` ignores incoming `required` entirely (see below) — so both
//!   arms report the same trivial `Remap` and pruning proceeds.
//! - **[`LogicalPlan::LateralJoin`] is left alone entirely.** Its own doc
//!   comment explains why: "the correlation is the defining property and a
//!   planner that loses track of it produces wrong answers rather than slow
//!   ones." The ablation doc confirms real correlated plans keep an
//!   `outer_ref` that survives optimization untouched — Basin's `Expr` has no
//!   equivalent marker yet, so there is no safe way for this rule to tell a
//!   correlated reference apart from an ordinary one. Renumbering a scan
//!   under a `LateralJoin` risks silently invalidating a correlated
//!   reference this rule cannot see.
//! - **A subquery's subplan is a separate query level and is left
//!   untouched**, per this rule's own requirements: collecting `required`
//!   never descends into `Expr::Subquery::subplan` (mirroring
//!   `Expr::for_each_child`, which already stops there), and neither does
//!   renumbering. `Expr::Subquery::operand` — the part of the expression that
//!   belongs to *this* query level, e.g. the `x` in `x IN (SELECT …)` — is
//!   traversed and renumbered like any other child expression.
//! - **DML (`Insert`/`Update`/`Delete`) and CTE bodies are left alone.** A
//!   `Cte`'s `input` (the query that consumes it) is pruned normally, but its
//!   `body` is not: a CTE can be referenced multiple times, and pruning the
//!   body correctly would mean finding every `CteRef` first and pruning to
//!   the union of their needs — a second traversal this increment does not
//!   do. DML's `RETURNING` list and `columns`/target alignment add their own
//!   bookkeeping this rule does not attempt yet.
//!
//! None of these are correctness gaps — a node this rule declines to touch
//! is returned exactly as given. They are missed optimizations, not wrong
//! answers, which is the trade this file is explicit about making wherever a
//! trade was necessary.

use std::collections::BTreeSet;

use super::OptimizerRule;
use crate::{ColumnRef, Expr, FrameBound, JoinKind, LogicalPlan, SortKey, WindowFrame};

/// Removes entries from every `LogicalPlan::Scan { projection }` that nothing
/// above the scan reads, and renumbers every [`ColumnRef`] that pointed at a
/// shrunk scan's old positions.
///
/// See the module docs for the algorithm and its known gaps.
pub struct ProjectionPruning;

impl OptimizerRule for ProjectionPruning {
    fn name(&self) -> &'static str {
        "projection_pruning"
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        // At the root there is no parent to ask what's needed, so — absent a
        // `Project` trimming the result — every column the plan currently
        // produces is required. `prune` on any non-root node with this same
        // full-width `required` set is exactly "nothing to drop here."
        let required: BTreeSet<u16> = (0..output_width(plan) as u16).collect();
        let (new_plan, _own_remap, changed) = prune(plan, &required);
        changed.then_some(new_plan)
    }
}

/// Old output position -> new output position, for a node whose column
/// numbering changed underneath it.
///
/// `None` (the outer `Option`) is the identity case: this node's output
/// positions are exactly what they were. That is true for the overwhelming
/// majority of a plan — pruning is normally a small, local edit — so giving
/// "nothing changed" a representation that costs nothing (no `Vec`, no
/// per-index lookup) rather than an explicit `0..n -> 0..n` map is what keeps
/// [`prune`] cheap on the common case.
///
/// `Some(v)`: `v[old]` is `Some(new)` if that column survived (at its new
/// position), or `None` if nothing downstream required it and it was
/// dropped. `v.len()` always equals this node's output width *before* this
/// call to `prune`.
type Remap = Option<Vec<Option<u16>>>;

/// Look up how position `idx` of the current relation `relation` was
/// renumbered, and rewrite it into `e`.
fn apply_remap(remap: &Remap, e: &Expr, relation: u16) -> Expr {
    match remap {
        None => e.clone(),
        Some(v) => remap_expr(e, relation, v),
    }
}

/// The number of columns `plan` currently produces.
///
/// Not `basin_plan::schema::output_schema` — that is still a stub (see
/// `schema.rs`) and, being general schema inference, would need to resolve
/// types this rule has no use for. This only ever needs a count, computed
/// straight from each variant's own shape.
fn output_width(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Scan { projection, .. } => projection.len(),
        LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema, .. }
        | LogicalPlan::CteRef { schema, .. } => schema.len(),
        LogicalPlan::Project { exprs, .. } => exprs.len(),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Cte { input, .. } => output_width(input),
        LogicalPlan::Aggregate { group, aggs, .. } => group.len() + aggs.len(),
        LogicalPlan::Window { input, windows } => output_width(input) + windows.len(),
        LogicalPlan::ProjectSet { input, srfs } => output_width(input) + srfs.len(),
        LogicalPlan::Join {
            left, right, kind, ..
        } => {
            if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                // A semi/anti join only ever emits the probe (left) side —
                // the build side exists to test for a match, not to
                // contribute columns.
                output_width(left)
            } else {
                output_width(left) + output_width(right)
            }
        }
        LogicalPlan::LateralJoin { outer, inner, kind } => {
            if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                output_width(outer)
            } else {
                output_width(outer) + output_width(inner)
            }
        }
        // Both arms of a set operation are required to have matching
        // schemas; `left` is as good a source of truth as `right`.
        LogicalPlan::SetOp { left, .. } => output_width(left),
        LogicalPlan::Insert { returning, .. }
        | LogicalPlan::Update { returning, .. }
        | LogicalPlan::Delete { returning, .. } => returning.as_ref().map_or(0, |r| r.len()),
    }
}

/// Collect the positions, within relation `relation`, of every
/// [`Expr::Column`] reachable from `e` — i.e. every column `e` itself reads.
///
/// Deliberately built on [`Expr::for_each_child`] rather than a bespoke walk:
/// that method already stops at an `Expr::Subquery`'s `subplan` (a subquery
/// is a separate query level — `expr.rs` enforces this for
/// `contains_aggregate` for exactly the same reason), so this inherits that
/// boundary for free and this rule never has to special-case it.
fn collect_columns(e: &Expr, relation: u16, out: &mut BTreeSet<u16>) {
    if let Expr::Column(cr) = e {
        if cr.relation == relation {
            out.insert(cr.index);
        }
    }
    e.for_each_child(&mut |c| collect_columns(c, relation, out));
}

/// Rewrite every [`Expr::Column`] in relation `relation` through `remap`.
///
/// Panics if `remap` has no entry for a column this expression reads — that
/// would mean some caller asked for a column to be dropped that its own
/// expressions still reference, which is exactly the "required must include
/// what a node reads itself" invariant [`prune`] is responsible for
/// upholding. A stale index here is not a case to degrade gracefully from: it
/// is silent data corruption (a query returning the wrong column) dressed up
/// as a query returning *a* column, so this fails loudly instead.
fn remap_expr(e: &Expr, relation: u16, remap: &[Option<u16>]) -> Expr {
    match e {
        Expr::Column(cr) => {
            if cr.relation == relation {
                let new_index = remap[cr.index as usize].unwrap_or_else(|| {
                    panic!(
                        "projection pruning: column {relation}.{} was read by an \
                         expression but not marked required — this is a bug in \
                         required-column collection, not a prunable plan",
                        cr.index
                    )
                });
                Expr::Column(ColumnRef {
                    relation: cr.relation,
                    index: new_index,
                    name: cr.name.clone(),
                })
            } else {
                e.clone()
            }
        }
        Expr::Literal(..) | Expr::Parameter { .. } => e.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: Box::new(remap_expr(arg, relation, remap)),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: Box::new(remap_expr(lhs, relation, remap)),
            rhs: Box::new(remap_expr(rhs, relation, remap)),
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: Box::new(remap_expr(arg, relation, remap)),
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(remap_expr(o, relation, remap))),
            whens: whens
                .iter()
                .map(|(w, t)| {
                    (
                        remap_expr(w, relation, remap),
                        remap_expr(t, relation, remap),
                    )
                })
                .collect(),
            else_: else_
                .as_ref()
                .map(|e2| Box::new(remap_expr(e2, relation, remap))),
        },
        Expr::Coalesce(xs) => {
            Expr::Coalesce(xs.iter().map(|x| remap_expr(x, relation, remap)).collect())
        }
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: Box::new(remap_expr(arg, relation, remap)),
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: Box::new(remap_expr(arg, relation, remap)),
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: Box::new(remap_expr(lhs, relation, remap)),
            rhs: Box::new(remap_expr(rhs, relation, remap)),
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: Box::new(remap_expr(arg, relation, remap)),
            list: list
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: Box::new(remap_expr(arg, relation, remap)),
            low: Box::new(remap_expr(low, relation, remap)),
            high: Box::new(remap_expr(high, relation, remap)),
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
            arg: Box::new(remap_expr(arg, relation, remap)),
            pattern: Box::new(remap_expr(pattern, relation, remap)),
            escape: escape
                .as_ref()
                .map(|e2| Box::new(remap_expr(e2, relation, remap))),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: args
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => Expr::Aggregate {
            func: *func,
            args: args
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(remap_expr(f, relation, remap))),
            order_by: remap_sort_keys(order_by, relation, remap),
        },
        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: args
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
            partition_by: partition_by
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
            order_by: remap_sort_keys(order_by, relation, remap),
            frame: WindowFrame {
                units: frame.units,
                start: remap_frame_bound(&frame.start, relation, remap),
                end: remap_frame_bound(&frame.end, relation, remap),
            },
        },
        Expr::SetReturning { func, args } => Expr::SetReturning {
            func: *func,
            args: args
                .iter()
                .map(|x| remap_expr(x, relation, remap))
                .collect(),
        },
        Expr::Subquery {
            kind,
            subplan,
            operand,
        } => Expr::Subquery {
            kind: *kind,
            // A separate query level: left untouched, see module docs.
            subplan: subplan.clone(),
            operand: operand
                .as_ref()
                .map(|o| Box::new(remap_expr(o, relation, remap))),
        },
        Expr::ArrayLit(xs) => {
            Expr::ArrayLit(xs.iter().map(|x| remap_expr(x, relation, remap)).collect())
        }
        Expr::RowLit(xs) => {
            Expr::RowLit(xs.iter().map(|x| remap_expr(x, relation, remap)).collect())
        }
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: Box::new(remap_expr(arg, relation, remap)),
            indices: indices
                .iter()
                .map(|i| match i {
                    crate::Subscript::Index(e2) => {
                        crate::Subscript::Index(remap_expr(e2, relation, remap))
                    }
                    crate::Subscript::Slice { lower, upper } => crate::Subscript::Slice {
                        lower: lower.as_ref().map(|l| remap_expr(l, relation, remap)),
                        upper: upper.as_ref().map(|u| remap_expr(u, relation, remap)),
                    },
                })
                .collect(),
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: Box::new(remap_expr(arg, relation, remap)),
            field: *field,
        },
    }
}

fn remap_sort_keys(keys: &[SortKey], relation: u16, remap: &[Option<u16>]) -> Vec<SortKey> {
    keys.iter()
        .map(|k| SortKey {
            expr: remap_expr(&k.expr, relation, remap),
            descending: k.descending,
            nulls_first: k.nulls_first,
        })
        .collect()
}

fn remap_frame_bound(b: &FrameBound, relation: u16, remap: &[Option<u16>]) -> FrameBound {
    match b {
        FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
        FrameBound::Preceding(e) => FrameBound::Preceding(Box::new(remap_expr(e, relation, remap))),
        FrameBound::CurrentRow => FrameBound::CurrentRow,
        FrameBound::Following(e) => FrameBound::Following(Box::new(remap_expr(e, relation, remap))),
        FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
    }
}

/// The recursive rewrite. `required` is the set of `plan`'s *own current*
/// output positions that whatever sits above `plan` actually reads.
///
/// Returns the (possibly rewritten) plan, `plan`'s own [`Remap`] — for the
/// caller to apply to whatever expressions of its own reference `plan` — and
/// whether anything changed anywhere in this subtree. A `false` here is what
/// lets [`ProjectionPruning::rewrite`] honor the driver's fixpoint contract:
/// see `driver.rs`'s "rules are expected to be honest about it."
fn prune(plan: &LogicalPlan, required: &BTreeSet<u16>) -> (LogicalPlan, Remap, bool) {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            snapshot,
        } => {
            // `required` alone is not the full story: a scan's own `filters`
            // (predicates pushed into the scan by the sibling
            // `push_down_filter` rule) read columns too, regardless of
            // whether anything above the scan also wants them.
            let mut req = required.clone();
            for f in filters {
                collect_columns(f, 0, &mut req);
            }
            if req.len() == projection.len() {
                // Every current column is required — nothing to drop. `req`
                // is a subset of `0..projection.len()` by construction (every
                // index collected came from a column reference resolved
                // against this scan), so equal lengths means equal sets.
                return (plan.clone(), None, false);
            }
            let mut new_projection = Vec::with_capacity(req.len());
            let mut remap = Vec::with_capacity(projection.len());
            for (old_idx, col) in projection.iter().enumerate() {
                if req.contains(&(old_idx as u16)) {
                    remap.push(Some(new_projection.len() as u16));
                    new_projection.push(*col);
                } else {
                    remap.push(None);
                }
            }
            let new_filters = filters.iter().map(|f| remap_expr(f, 0, &remap)).collect();
            let new_plan = LogicalPlan::Scan {
                table: *table,
                projection: new_projection,
                filters: new_filters,
                snapshot: *snapshot,
            };
            (new_plan, Some(remap), true)
        }

        // Pure passthrough: a `Filter` neither adds nor removes columns, so
        // its own output numbering is exactly whatever its (possibly pruned)
        // input's numbering turns out to be, and `required` translates
        // straight through with no offset. Its predicate is evaluated
        // regardless of what's required upward, so the columns it reads are
        // unconditionally required from the input.
        LogicalPlan::Filter { input, predicate } => {
            let mut child_required = required.clone();
            collect_columns(predicate, 0, &mut child_required);
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_predicate = apply_remap(&remap, predicate, 0);
            let new_plan = LogicalPlan::Filter {
                input: Box::new(new_input),
                predicate: new_predicate,
            };
            (new_plan, remap, true)
        }

        LogicalPlan::Sort { input, keys } => {
            let mut child_required = required.clone();
            for k in keys {
                collect_columns(&k.expr, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_keys = keys
                .iter()
                .map(|k| SortKey {
                    expr: apply_remap(&remap, &k.expr, 0),
                    descending: k.descending,
                    nulls_first: k.nulls_first,
                })
                .collect();
            let new_plan = LogicalPlan::Sort {
                input: Box::new(new_input),
                keys: new_keys,
            };
            (new_plan, remap, true)
        }

        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
        } => {
            let mut child_required = required.clone();
            if let Some(e) = skip {
                collect_columns(e, 0, &mut child_required);
            }
            if let Some(e) = fetch {
                collect_columns(e, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_skip = skip.as_ref().map(|e| apply_remap(&remap, e, 0));
            let new_fetch = fetch.as_ref().map(|e| apply_remap(&remap, e, 0));
            let new_plan = LogicalPlan::Limit {
                input: Box::new(new_input),
                skip: new_skip,
                fetch: new_fetch,
                with_ties: *with_ties,
            };
            (new_plan, remap, true)
        }

        LogicalPlan::Distinct { input, on } => {
            let mut child_required = required.clone();
            if let Some(exprs) = on {
                for e in exprs {
                    collect_columns(e, 0, &mut child_required);
                }
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_on = on
                .as_ref()
                .map(|exprs| exprs.iter().map(|e| apply_remap(&remap, e, 0)).collect());
            let new_plan = LogicalPlan::Distinct {
                input: Box::new(new_input),
                on: new_on,
            };
            (new_plan, remap, true)
        }

        // `Cte`'s own output is exactly `input`'s (passthrough, like
        // `Filter`). `body` is deliberately left untouched — see module
        // docs on why pruning it needs a second traversal this increment
        // does not do.
        LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } => {
            let (new_input, remap, changed) = prune(input, required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_plan = LogicalPlan::Cte {
                name: *name,
                recursive: *recursive,
                body: body.clone(),
                input: Box::new(new_input),
            };
            (new_plan, remap, true)
        }

        // `Project` and `Aggregate` define a brand-new, fixed-width schema —
        // their own output is exactly their expression list, independent of
        // what their input looks like. That makes `required` from the parent
        // irrelevant to *this* node (every one of its own expressions is
        // always kept) and makes it a remap barrier: whatever pruning
        // happens to `input`, this node's own numbering — `0..exprs.len()`,
        // untouched — never changes, so it reports `Remap::None` (identity)
        // to its own caller regardless of whether `input` changed.
        LogicalPlan::Project { input, exprs } => {
            let mut child_required = BTreeSet::new();
            for (e, _) in exprs {
                collect_columns(e, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_exprs = exprs
                .iter()
                .map(|(e, name)| (apply_remap(&remap, e, 0), name.clone()))
                .collect();
            let new_plan = LogicalPlan::Project {
                input: Box::new(new_input),
                exprs: new_exprs,
            };
            (new_plan, None, true)
        }

        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } => {
            let mut child_required = BTreeSet::new();
            for e in group {
                collect_columns(e, 0, &mut child_required);
            }
            for e in aggs {
                collect_columns(e, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_group = group.iter().map(|e| apply_remap(&remap, e, 0)).collect();
            let new_aggs = aggs.iter().map(|e| apply_remap(&remap, e, 0)).collect();
            let new_plan = LogicalPlan::Aggregate {
                input: Box::new(new_input),
                group: new_group,
                aggs: new_aggs,
                // Indices into `group` itself, not schema positions — safe
                // to carry over unchanged.
                grouping_sets: grouping_sets.clone(),
            };
            (new_plan, None, true)
        }

        // `windows` appends new columns after all of `input`'s columns
        // (unlike `Project`/`Aggregate`, the input columns really do flow
        // through), so this node's own output width tracks `input`'s width
        // and it is a passthrough-plus-append, not a barrier.
        LogicalPlan::Window { input, windows } => {
            let old_input_width = output_width(input);
            let mut child_required: BTreeSet<u16> = required
                .iter()
                .copied()
                .filter(|&p| (p as usize) < old_input_width)
                .collect();
            for w in windows {
                collect_columns(w, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_windows = windows.iter().map(|w| apply_remap(&remap, w, 0)).collect();
            let new_input_width = output_width(&new_input);
            let passthrough = (0..old_input_width).map(|p| match &remap {
                None => Some(p as u16),
                Some(v) => v[p],
            });
            let appended = (0..windows.len()).map(|k| Some((new_input_width + k) as u16));
            let own_remap: Vec<Option<u16>> = passthrough.chain(appended).collect();
            let new_plan = LogicalPlan::Window {
                input: Box::new(new_input),
                windows: new_windows,
            };
            (new_plan, Some(own_remap), true)
        }

        // Same shape as `Window`: `srfs` is appended after `input`'s columns.
        LogicalPlan::ProjectSet { input, srfs } => {
            let old_input_width = output_width(input);
            let mut child_required: BTreeSet<u16> = required
                .iter()
                .copied()
                .filter(|&p| (p as usize) < old_input_width)
                .collect();
            for s in srfs {
                collect_columns(s, 0, &mut child_required);
            }
            let (new_input, remap, changed) = prune(input, &child_required);
            if !changed {
                return (plan.clone(), None, false);
            }
            let new_srfs = srfs.iter().map(|s| apply_remap(&remap, s, 0)).collect();
            let new_input_width = output_width(&new_input);
            let passthrough = (0..old_input_width).map(|p| match &remap {
                None => Some(p as u16),
                Some(v) => v[p],
            });
            let appended = (0..srfs.len()).map(|k| Some((new_input_width + k) as u16));
            let own_remap: Vec<Option<u16>> = passthrough.chain(appended).collect();
            let new_plan = LogicalPlan::ProjectSet {
                input: Box::new(new_input),
                srfs: new_srfs,
            };
            (new_plan, Some(own_remap), true)
        }

        // A join's output is the concatenation of its sides (or, for
        // semi/anti, just the left side — see `output_width`), so `required`
        // splits by side, and each side additionally requires whatever the
        // join's own `on`/`filter` read from it regardless of what's
        // required upward.
        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } => {
            let is_semi_anti = matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti);
            let left_width = output_width(left);
            let right_width = output_width(right);

            let mut left_required: BTreeSet<u16> = required
                .iter()
                .copied()
                .filter(|&p| (p as usize) < left_width)
                .collect();
            let mut right_required: BTreeSet<u16> = if is_semi_anti {
                // The build side never contributes to a semi/anti join's
                // output, so `required` (positions in *this join's* output)
                // never reaches past `left_width` to begin with.
                BTreeSet::new()
            } else {
                required
                    .iter()
                    .copied()
                    .filter(|&p| (p as usize) >= left_width)
                    .map(|p| p - left_width as u16)
                    .collect()
            };

            // Attribute a join condition's columns to their side by POSITION,
            // not by `ColumnRef::relation`.
            //
            // Lowering emits every column with `relation: 0` and a FLAT index
            // across the concatenated scope — see `Scope::resolve` in
            // lower/select.rs, which hardcodes `relation: 0` for both qualified
            // and unqualified lookups. So `collect_columns(expr, 1, ..)` found
            // nothing, the build side's required set came back empty, and this
            // rule pruned away the very column the join keys on.
            //
            // That is not a cosmetic mismatch: it produced a plan whose join
            // key indexed a zero-column schema, which arrow answered by
            // panicking and taking the session down (fixed defensively in
            // basin-exec's HashJoin, but the cause is here). The `required` set
            // a few lines above already uses this same flat convention; the
            // join keys were the one place that did not.
            // BOTH conventions are live in this codebase and both must work.
            // A `relation: 1` reference is already attributed to the build
            // side. A `relation: 0` reference is a flat position, so it belongs
            // to whichever side its index falls in.
            // An `on` pair is ALREADY per-side. `lower/select.rs`'s
            // `split_equijoin_conjuncts` rebases the right operand to be
            // right-schema-relative before the pair reaches a plan, so its
            // index is not a flat position and must not be treated as one.
            //
            // My earlier fix here did treat it as one. It was written to close
            // a panic where the build side got pruned to zero columns, and it
            // replaced that wrong plan with a different wrong plan: applying
            // the flat rule to an already-rebased right index attributes the
            // join key to the LEFT side, so the right side loses it again. The
            // symptom was identical, which is why it survived.
            for (l, r) in on {
                collect_columns(l, 0, &mut left_required);
                collect_columns(l, 1, &mut right_required);
                collect_columns(r, 1, &mut right_required);
                // The right operand carries `relation: 0` from lowering even
                // though its index is right-relative, so read it as right.
                collect_columns(r, 0, &mut right_required);
            }

            // A join `filter` is NOT rebased — it is an arbitrary residual
            // predicate over the concatenated output, so a flat position is
            // exactly what it holds.
            if let Some(f) = filter {
                let mut flat = BTreeSet::new();
                collect_columns(f, 0, &mut flat);
                for p in flat {
                    if (p as usize) >= left_width {
                        right_required.insert(p - left_width as u16);
                    } else {
                        left_required.insert(p);
                    }
                }
                collect_columns(f, 1, &mut right_required);
            }

            let (new_left, left_remap, left_changed) = prune(left, &left_required);
            let (new_right, right_remap, right_changed) = prune(right, &right_required);
            if !left_changed && !right_changed {
                return (plan.clone(), None, false);
            }

            let new_on = on
                .iter()
                .map(|(l, r)| {
                    // The two halves of an `on` pair are remapped through
                    // DIFFERENT tables, because `rebase_columns` leaves the
                    // right operand tagged `relation: 0` while making its index
                    // right-relative. Sending both through `left_remap` looks
                    // symmetric and reads fine, and it is what this did — it
                    // resolved the right operand's index against the left
                    // side's table. When the two sides have different widths or
                    // different pruning, that is either a panic (index not
                    // marked required) or, worse, an in-range hit on an
                    // unrelated column.
                    //
                    // Collection above was already fixed to attribute `r`'s
                    // relation-0 index to the right side. This is the other
                    // half of that same fix, which I left behind: collection
                    // said "the right side needs column k" and the rewrite then
                    // looked k up in the left table.
                    let l = apply_remap(&right_remap, &apply_remap(&left_remap, l, 0), 1);
                    let r = apply_remap(&right_remap, &apply_remap(&right_remap, r, 0), 1);
                    (l, r)
                })
                .collect();
            // A flat table over the CONCATENATED output: positions below
            // `left_width` resolve through the left side's remap, the rest
            // through the right's and then shifted by the pruned left width.
            //
            // This is what a join `filter` needs, and it is needed whatever the
            // join kind — a semi join's filter still reads right-side columns
            // during the match test even though they never reach the output.
            // Remapping the filter with `left_remap` alone (which is what this
            // did) resolves every right-side position against the left side's
            // table: a panic when the index is not marked required, and a wrong
            // column when it happens to be.
            let new_left_width = output_width(&new_left);
            let flat_remap: Remap = Some(
                (0..left_width)
                    .map(|p| match &left_remap {
                        None => Some(p as u16),
                        Some(v) => v[p],
                    })
                    .chain((0..right_width).map(|p| {
                        let mapped = match &right_remap {
                            None => Some(p as u16),
                            Some(v) => v[p],
                        };
                        mapped.map(|y| (new_left_width + y as usize) as u16)
                    }))
                    .collect(),
            );

            let new_filter = filter
                .as_ref()
                .map(|f| apply_remap(&right_remap, &apply_remap(&flat_remap, f, 0), 1));

            // A semi/anti join outputs only its left side, so what it exposes
            // upward is the left remap — not the concatenated one above.
            let own_remap = if is_semi_anti {
                left_remap.clone()
            } else {
                flat_remap
            };

            let new_plan = LogicalPlan::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                kind: *kind,
                on: new_on,
                filter: new_filter,
            };
            (new_plan, own_remap, true)
        }

        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            // Both arms must keep matching schemas, so the same
            // `required` applies to both — see the module docs for why the
            // two sides' `Remap`s can still (rarely) diverge, and why
            // falling back to leaving the node alone is the safe response
            // when they do.
            let (new_left, left_remap, left_changed) = prune(left, required);
            let (new_right, right_remap, right_changed) = prune(right, required);
            if !left_changed && !right_changed {
                return (plan.clone(), None, false);
            }
            if left_remap != right_remap {
                return (plan.clone(), None, false);
            }
            let own_remap = left_remap;
            let new_plan = LogicalPlan::SetOp {
                left: Box::new(new_left),
                right: Box::new(new_right),
                op: *op,
                all: *all,
            };
            (new_plan, own_remap, true)
        }

        // Left untouched — see the module docs for why each is out of scope
        // for this increment: `LateralJoin` (correlation this rule cannot
        // see), `Values`/`Empty`/`CteRef` (leaves; no scan beneath them to
        // prune), `Insert`/`Update`/`Delete` (DML bookkeeping not attempted
        // yet).
        LogicalPlan::LateralJoin { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::CteRef { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. } => (plan.clone(), None, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, Datum, FuncId, OpId, SnapshotId, TableId};
    use basin_pgtype::{oid::Oid, PgType};

    fn col(index: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: format!("c{index}"),
        })
    }

    fn scan(projection: Vec<ColId>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(1),
            projection,
            filters: vec![],
            snapshot: SnapshotId(0),
        }
    }

    fn optimize(plan: &LogicalPlan) -> Option<LogicalPlan> {
        ProjectionPruning.rewrite(plan)
    }

    /// The headline behavior: a `Project` that only reads column 0 must
    /// prune column 1 out of the scan beneath it — this is literally what
    /// makes "read only the projected columns" true, per the ablation doc's
    /// framing of why this rule is the whole columnar win.
    #[test]
    fn scan_under_a_projection_loses_unreferenced_columns() {
        let plan = LogicalPlan::Project {
            input: Box::new(scan(vec![ColId(10), ColId(20)])),
            exprs: vec![(col(0), "a".into())],
        };
        let optimized = optimize(&plan).expect("column 20 should have been dropped");
        let LogicalPlan::Project { input, exprs } = &optimized else {
            panic!("expected Project");
        };
        assert_eq!(exprs, &vec![(col(0), "a".into())]);
        let LogicalPlan::Scan { projection, .. } = input.as_ref() else {
            panic!("expected Scan");
        };
        assert_eq!(projection, &vec![ColId(10)]);
    }

    /// A column that never appears in the outer projection but is read by a
    /// `WHERE` predicate must still reach the scan. Getting this wrong is
    /// the "wrong answers, not slow ones" failure mode the task calls out.
    #[test]
    fn a_column_used_only_in_a_where_predicate_is_retained() {
        // 0:a (outer projection), 1:b (predicate only), 2:unused — column 2
        // is the one that should actually get pruned; column 1 must survive
        // even though it never appears in the outer projection.
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(vec![ColId(10), ColId(20), ColId(30)])),
                predicate: Expr::Binary {
                    op: OpId(Oid(147)), // >
                    lhs: Box::new(col(1)),
                    rhs: Box::new(Expr::Literal(Datum::Int64(0), PgType::INT8)),
                },
            }),
            exprs: vec![(col(0), "a".into())],
        };
        let optimized = optimize(&plan).expect("column 30 should have been dropped");
        let LogicalPlan::Project { input, .. } = &optimized else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected Filter");
        };
        let LogicalPlan::Scan { projection, .. } = input.as_ref() else {
            panic!("expected Scan");
        };
        assert_eq!(
            projection,
            &vec![ColId(10), ColId(20)],
            "column 20 is read by the predicate and must not be pruned, even \
             though it never reaches the outer projection"
        );
        // Columns 0 and 1 kept their positions (only the trailing, unused
        // column 2 was dropped), so the predicate's reference is unchanged.
        let Expr::Binary { lhs, .. } = predicate else {
            panic!("expected Binary");
        };
        assert_eq!(lhs.as_ref(), &col(1));
    }

    /// `COUNT(*)` has no arguments, so an `Aggregate` with no `GROUP BY`
    /// requires nothing from its input — the scan's projection must go all
    /// the way to empty, and empty must not be confused with "all columns."
    /// `lib.rs`'s `empty_projection_is_not_a_wildcard` test guards the same
    /// invariant one layer down; this is the optimizer producing that state.
    #[test]
    fn count_star_yields_an_empty_projection() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan(vec![ColId(10), ColId(20)])),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: FuncId(Oid(2803)), // count(*)
                args: vec![],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        let optimized = optimize(&plan).expect("both columns should have been dropped");
        let LogicalPlan::Aggregate { input, .. } = &optimized else {
            panic!("expected Aggregate");
        };
        let LogicalPlan::Scan { projection, .. } = input.as_ref() else {
            panic!("expected Scan");
        };
        assert!(
            projection.is_empty(),
            "COUNT(*) needs no columns, so the scan should read none"
        );

        // And it stays empty: re-running on the now-pruned plan is a no-op.
        assert_eq!(
            optimize(&optimized),
            None,
            "an already-pruned COUNT(*) plan must not report further change"
        );
    }

    /// The fixpoint-honesty contract from `driver.rs`: a plan that is
    /// already minimal must come back `None`, not an equal-but-freshly-built
    /// clone, or the driver spends a full extra pass discovering nothing
    /// changed.
    #[test]
    fn returns_none_on_an_already_pruned_plan() {
        let plan = LogicalPlan::Project {
            input: Box::new(scan(vec![ColId(10)])),
            exprs: vec![(col(0), "a".into())],
        };
        assert_eq!(optimize(&plan), None);
    }

    /// A bare scan with no enclosing `Project` must keep every column: there
    /// is nothing downstream to say some of them are unwanted.
    #[test]
    fn a_bare_scan_with_no_projection_above_it_is_untouched() {
        let plan = scan(vec![ColId(10), ColId(20), ColId(30)]);
        assert_eq!(optimize(&plan), None);
    }

    /// Two-table join: a `Project` that only reads the left side's column
    /// must prune the entire right-side scan, and the join's own `on` key
    /// (needed to evaluate the join itself, even though it's not in the
    /// final output) must still survive on both sides.
    #[test]
    fn projection_prunes_through_a_join() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(scan(vec![ColId(1), ColId(2)])), // 0:id 1:extra
                right: Box::new(scan(vec![ColId(9), ColId(8)])), // 0:id 1:extra
                kind: JoinKind::Inner,
                on: vec![(
                    col(0), // left.id
                    Expr::Column(ColumnRef {
                        relation: 1,
                        index: 0,
                        name: "id".into(),
                    }), // right.id
                )],
                filter: None,
            }),
            // Only the left side's column 1 ("extra") makes it out.
            exprs: vec![(col(1), "extra".into())],
        };
        let optimized =
            optimize(&plan).expect("the right side should have been pruned to just its join key");
        let LogicalPlan::Project { input, .. } = &optimized else {
            panic!("expected Project");
        };
        let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
            panic!("expected Join");
        };
        let LogicalPlan::Scan {
            projection: left_proj,
            ..
        } = left.as_ref()
        else {
            panic!("expected Scan");
        };
        let LogicalPlan::Scan {
            projection: right_proj,
            ..
        } = right.as_ref()
        else {
            panic!("expected Scan");
        };
        assert_eq!(
            left_proj,
            &vec![ColId(1), ColId(2)],
            "left needs both its join key and the projected column"
        );
        assert_eq!(
            right_proj,
            &vec![ColId(9)],
            "right only needs its join key — ColId(8) is unused"
        );
    }
}
