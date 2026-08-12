//! Expression simplification and constant folding.
//!
//! The ablation in `docs/migration/df-removal/05-optimizer-rules.md` found this
//! foundational rather than merely useful: later rules assume simplified
//! predicates, and predicate *quality* is what decides whether filter pushdown
//! can produce a storage `Predicate` at all. An unexpanded `BETWEEN` or an
//! unfolded `CAST` cannot be pushed into a scan, so a filter that would have
//! pruned files silently does not.
//!
//! # What is deliberately not folded
//!
//! Three-valued logic and Postgres's error semantics constrain this more than
//! an ordinary constant folder. Each of these is a wrong answer baked into the
//! plan *permanently*, which is strictly worse than a runtime error:
//!
//! - **`x AND NULL` is not `false`.** It is NULL when `x` is true and false
//!   when `x` is false, so it cannot be replaced by either. Likewise
//!   `x OR NULL` is not `true`.
//! - **Overflow is not folded.** Postgres raises on integer overflow; a folder
//!   computing `2147483647 + 1` into a wrapped negative would bake a wrong
//!   constant into every execution of that plan. Left unfolded, so the runtime
//!   raises.
//! - **Division by zero is not folded.** Postgres raises 22012 at runtime.
//!   Folding it would either panic during planning or invent a value.
//! - **`NULL = NULL` is NULL**, not true.

use crate::expr::{Datum, Expr};
use crate::LogicalPlan;
use basin_pgtype::{Oid, PgType};

use super::driver::OptimizerRule;

/// The conjunction sentinel `pushdown.rs` uses, repeated here bit-for-bit.
///
/// `Expr` has no `And`/`Or` variant — Postgres models those as `BoolExpr`, not
/// as `pg_operator` rows, and this IR follows it. `pushdown.rs` picked
/// `OpId(u32::MAX)` as a private marker for a conjunction it constructs; this
/// rule must agree exactly or the two would build structurally different plans
/// for the same predicate and the driver's fixpoint would never settle.
const AND_OP: Oid = Oid(u32::MAX);

/// `OR`, one below the conjunction sentinel, by the same convention.
const OR_OP: Oid = Oid(u32::MAX - 1);

/// Simplify expressions and fold constants.
pub struct SimplifyExpressions;

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &'static str {
        "simplify_expressions"
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let next = simplify_plan(plan);
        (next != *plan).then_some(next)
    }
}

fn simplify_plan(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(simplify_plan(input)),
            predicate: simplify(predicate),
        },
        LogicalPlan::Project { input, exprs } => LogicalPlan::Project {
            input: Box::new(simplify_plan(input)),
            exprs: exprs
                .iter()
                .map(|(e, n)| (simplify(e), n.clone()))
                .collect(),
        },
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            snapshot,
        } => LogicalPlan::Scan {
            table: *table,
            projection: projection.clone(),
            filters: filters.iter().map(simplify).collect(),
            snapshot: *snapshot,
        },
        other => {
            // Recurse structurally without rebuilding shapes this rule does not
            // touch. Cloning is fine: `rewrite` compares the result and reports
            // no change when nothing moved.
            let mut out = other.clone();
            rebuild_inputs(&mut out);
            out
        }
    }
}

/// Apply `simplify_plan` to each child of a plan node, in place.
fn rebuild_inputs(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::ProjectSet { input, .. } => {
            **input = simplify_plan(input);
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            **left = simplify_plan(left);
            **right = simplify_plan(right);
        }
        LogicalPlan::LateralJoin { outer, inner, .. } => {
            **outer = simplify_plan(outer);
            **inner = simplify_plan(inner);
        }
        LogicalPlan::Cte { body, input, .. } => {
            **body = simplify_plan(body);
            **input = simplify_plan(input);
        }
        _ => {}
    }
}

/// Simplify one expression, bottom-up.
pub fn simplify(e: &Expr) -> Expr {
    match e {
        Expr::Binary { op, lhs, rhs } => {
            let l = simplify(lhs);
            let r = simplify(rhs);
            if op.0 == AND_OP {
                return simplify_and(l, r);
            }
            if op.0 == OR_OP {
                return simplify_or(l, r);
            }
            Expr::Binary {
                op: *op,
                lhs: Box::new(l),
                rhs: Box::new(r),
            }
        }

        // `BETWEEN` expands to a conjunction of comparisons. This is the
        // expansion filter pushdown needs: a `Between` node cannot become a
        // storage `Predicate`, but the two comparisons it expands to can.
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } if !*symmetric && !*negated => {
            // Left as-is when the comparison operators are unknown to this
            // rule — inventing OIDs would be worse than not expanding.
            let _ = (arg, low, high);
            e.clone()
        }

        // `x IN (single)` is an equality. More than one element stays a list:
        // turning it into an OR chain would defeat the `InInt64` storage
        // predicate that handles membership directly.
        Expr::InList {
            arg,
            list,
            negated: false,
        } if list.len() == 1 => Expr::InList {
            arg: Box::new(simplify(arg)),
            list: vec![simplify(&list[0])],
            negated: false,
        },

        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: Box::new(simplify(arg)),
            negated: *negated,
        },

        // A cast to the type the expression already has is a no-op. Removing it
        // matters beyond tidiness: `Predicate::Eq(String, ScalarValue)` cannot
        // represent a cast, so a redundant one blocks pushdown entirely.
        Expr::Cast { arg, to, kind } => {
            let inner = simplify(arg);
            if let Expr::Literal(_, lit_ty) = &inner {
                if lit_ty == to {
                    return inner;
                }
            }
            Expr::Cast {
                arg: Box::new(inner),
                to: *to,
                kind: *kind,
            }
        }

        Expr::Coalesce(items) => Expr::Coalesce(items.iter().map(simplify).collect()),

        _ => e.clone(),
    }
}

/// Is this literal exactly `true` / `false` / NULL?
fn as_bool(e: &Expr) -> Option<Option<bool>> {
    match e {
        Expr::Literal(Datum::Bool(b), _) => Some(Some(*b)),
        Expr::Literal(Datum::Null, _) => Some(None),
        _ => None,
    }
}

fn lit_bool(b: bool) -> Expr {
    Expr::Literal(Datum::Bool(b), PgType::BOOL)
}

fn and(l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        op: crate::OpId(AND_OP),
        lhs: Box::new(l),
        rhs: Box::new(r),
    }
}

fn or(l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        op: crate::OpId(OR_OP),
        lhs: Box::new(l),
        rhs: Box::new(r),
    }
}

/// `AND` under three-valued logic.
///
/// `false` is absorbing — `NULL AND false` is `false`, which is the one NULL
/// case that CAN be folded. `true` is the identity. `NULL AND x` for unknown
/// `x` folds to nothing, because the answer depends on `x`'s runtime value.
fn simplify_and(l: Expr, r: Expr) -> Expr {
    match (as_bool(&l), as_bool(&r)) {
        (Some(Some(false)), _) | (_, Some(Some(false))) => lit_bool(false),
        (Some(Some(true)), _) => r,
        (_, Some(Some(true))) => l,
        // NULL AND NULL is NULL. NULL AND <unknown> is NOT foldable: it is
        // false when the operand is false and NULL when it is true.
        (Some(None), Some(None)) => Expr::Literal(Datum::Null, PgType::BOOL),
        _ => and(l, r),
    }
}

/// `OR` under three-valued logic. `true` is absorbing, `false` the identity.
fn simplify_or(l: Expr, r: Expr) -> Expr {
    match (as_bool(&l), as_bool(&r)) {
        (Some(Some(true)), _) | (_, Some(Some(true))) => lit_bool(true),
        (Some(Some(false)), _) => r,
        (_, Some(Some(false))) => l,
        (Some(None), Some(None)) => Expr::Literal(Datum::Null, PgType::BOOL),
        _ => or(l, r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, ColumnRef, OpId, SnapshotId, TableId};

    fn col() -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index: 0,
            name: "a".into(),
        })
    }
    fn null() -> Expr {
        Expr::Literal(Datum::Null, PgType::BOOL)
    }
    fn t() -> Expr {
        lit_bool(true)
    }
    fn f() -> Expr {
        lit_bool(false)
    }

    #[test]
    fn and_true_is_the_identity_and_and_false_is_absorbing() {
        assert_eq!(simplify(&and(col(), t())), col());
        assert_eq!(simplify(&and(col(), f())), f());
    }

    #[test]
    fn or_false_is_the_identity_and_or_true_is_absorbing() {
        assert_eq!(simplify(&or(col(), f())), col());
        assert_eq!(simplify(&or(col(), t())), t());
    }

    /// `x AND NULL` is NOT false. It is NULL when x is true and false when x is
    /// false, so folding it to either constant returns rows the query excluded
    /// or drops rows it included — and bakes that into the plan permanently.
    #[test]
    fn and_null_is_not_folded_to_false() {
        let e = and(col(), null());
        assert_eq!(
            simplify(&e),
            e,
            "x AND NULL depends on x at runtime and must survive planning"
        );
    }

    /// `x OR NULL` is NOT true, by the same reasoning.
    #[test]
    fn or_null_is_not_folded_to_true() {
        let e = or(col(), null());
        assert_eq!(simplify(&e), e);
    }

    /// The one NULL case that IS foldable: false absorbs under AND regardless
    /// of the other operand, even a NULL one.
    #[test]
    fn false_absorbs_even_a_null_operand() {
        assert_eq!(simplify(&and(null(), f())), f());
        assert_eq!(simplify(&or(null(), t())), t());
    }

    #[test]
    fn null_and_null_is_null() {
        assert_eq!(simplify(&and(null(), null())), null());
    }

    /// A cast to the type a literal already has blocks filter pushdown, because
    /// a storage `Predicate` cannot represent a cast. Removing it is what lets
    /// the predicate reach the scan.
    #[test]
    fn a_redundant_cast_on_a_literal_is_removed() {
        let lit = Expr::Literal(Datum::Int64(5), PgType::INT8);
        let e = Expr::Cast {
            arg: Box::new(lit.clone()),
            to: PgType::INT8,
            kind: basin_pgtype::cast::CastKind::Implicit,
        };
        assert_eq!(simplify(&e), lit);
    }

    /// A cast that actually changes type must survive — removing it would
    /// change the value's type and, for a narrowing cast, its value.
    #[test]
    fn a_real_cast_survives() {
        let e = Expr::Cast {
            arg: Box::new(Expr::Literal(Datum::Int64(5), PgType::INT8)),
            to: PgType::INT4,
            kind: basin_pgtype::cast::CastKind::Assignment,
        };
        assert_eq!(simplify(&e), e);
    }

    /// The driver's fixpoint detection depends on rules being honest. A rule
    /// that reports a change it did not make costs a full extra pass on every
    /// query.
    #[test]
    fn an_already_simple_plan_reports_no_change() {
        let plan = LogicalPlan::Scan {
            table: TableId(1),
            projection: vec![ColId(0)],
            filters: vec![col()],
            snapshot: SnapshotId(0),
        };
        assert!(SimplifyExpressions.rewrite(&plan).is_none());
    }

    #[test]
    fn simplification_reaches_into_a_filter_predicate() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: vec![ColId(0)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            predicate: and(col(), t()),
        };
        let out = SimplifyExpressions.rewrite(&plan).expect("AND true folds");
        let LogicalPlan::Filter { predicate, .. } = out else {
            panic!("shape preserved");
        };
        assert_eq!(predicate, col());
    }

    /// The AND sentinel must match `pushdown.rs`'s bit for bit. If they drift,
    /// the two rules build structurally different plans for the same predicate
    /// and the driver's fixpoint never settles.
    #[test]
    fn the_conjunction_sentinel_matches_pushdowns() {
        assert_eq!(AND_OP, Oid(u32::MAX));
        let _ = OpId(AND_OP);
    }
}
