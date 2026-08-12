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
//!   raises. The same goes for float arithmetic that overflows to infinity —
//!   confirmed live against PostgreSQL 18, which raises `value out of range:
//!   overflow` rather than producing `Infinity`.
//! - **Division by zero is not folded.** Postgres raises 22012 at runtime,
//!   for both integer and float division — confirmed live. Folding it would
//!   either panic during planning or invent a value.
//! - **`NULL = NULL` is NULL**, not true — confirmed live. More generally,
//!   every operator folded here is STRICT (NULL in, NULL out), which is what
//!   [`fold_binary`] relies on rather than special-casing `=` alone.
//!
//! # BETWEEN
//!
//! `x BETWEEN a AND b` cannot become a storage `Predicate` — only the
//! comparisons it expands to can — so [`expand_between`] expands all four
//! `SYMMETRIC`/`NOT` combinations when an operand's type can be determined,
//! and leaves the node alone otherwise. See its doc comment for why a type is
//! not always available to a rule that runs with no schema, and why guessing
//! an OID is refused the same way the constant-folding cases above are.

use std::cmp::Ordering;

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

/// `NOT`, one more below `OR_OP`, by the same "no dedicated boolean-connective
/// variant" convention: `Expr::Unary`'s `op` is a real `pg_operator` oid for a
/// true prefix operator like unary `-`, but Postgres models `NOT` as a
/// `BoolExpr`, not a `pg_operator` row, the same as `AND`/`OR` — so there is
/// no real oid to recognize it by. Unlike `AND_OP`, no other file in this
/// crate has needed to build or recognize a `NOT` node yet
/// (`decorrelate.rs`'s module docs note that a bare `Unary { op: NOT, .. }`
/// should not even reach that rule), so this is the first such convention
/// rather than one this file must agree with bit-for-bit against another
/// file. Chosen below `OR_OP` for the same reason both of those are chosen
/// from the top of `u32`: every real `pg_operator` oid in
/// `basin_pgtype::operator::OPERATORS` is in the low thousands.
const NOT_OP: Oid = Oid(u32::MAX - 2);

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
            if let Some(folded) = fold_binary(op.0, &l, &r) {
                return folded;
            }
            Expr::Binary {
                op: *op,
                lhs: Box::new(l),
                rhs: Box::new(r),
            }
        }

        Expr::Unary { op, arg } => {
            let inner = simplify(arg);
            // `NOT NOT x` -> `x`. Only collapses a genuine double negation —
            // `NOT (-x)` (a real prefix operator nested under `NOT`) must not
            // be touched, so this checks the inner node's own op too.
            if op.0 == NOT_OP {
                if let Expr::Unary {
                    op: inner_op,
                    arg: inner_arg,
                } = &inner
                {
                    if inner_op.0 == NOT_OP {
                        return (**inner_arg).clone();
                    }
                }
            }
            Expr::Unary {
                op: *op,
                arg: Box::new(inner),
            }
        }

        // `BETWEEN` expands to comparisons filter pushdown can turn into a
        // storage `Predicate` — a `Between` node itself cannot become one.
        // See `expand_between` for the four `SYMMETRIC`/`NOT` combinations
        // and why a missing operand type leaves this unexpanded rather than
        // guessed at.
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => {
            let arg = simplify(arg);
            let low = simplify(low);
            let high = simplify(high);
            if let Some(expanded) = expand_between(&arg, &low, &high, *symmetric, *negated) {
                // Route the expansion back through `simplify` so a literal
                // `arg` (or literal bounds) that make the result a compile-
                // time constant get folded in the same pass — see
                // `between_symmetric_handles_reversed_bounds_correctly`.
                return simplify(&expanded);
            }
            Expr::Between {
                arg: Box::new(arg),
                low: Box::new(low),
                high: Box::new(high),
                symmetric: *symmetric,
                negated: *negated,
            }
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

        Expr::IsNull { arg, negated } => {
            let inner = simplify(arg);
            // Foldable exactly when the operand's nullness is already known,
            // i.e. it is a literal. A column's nullness is a runtime fact
            // this schema-less rule cannot see, so it stays a runtime check.
            if let Expr::Literal(d, _) = &inner {
                let is_null = matches!(d, Datum::Null);
                return lit_bool(if *negated { !is_null } else { is_null });
            }
            Expr::IsNull {
                arg: Box::new(inner),
                negated: *negated,
            }
        }

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

// ─── BETWEEN expansion ────────────────────────────────────────────────────

/// Expand `BETWEEN` (all four `SYMMETRIC`/`NOT` combinations) into the
/// comparisons filter pushdown can turn into a storage `Predicate`. Returns
/// `None`, leaving the `Between` node in place, when no operand's type can be
/// determined: this rule runs with no schema, so `x BETWEEN a AND b` over
/// three plain columns has nothing to resolve `>=`/`<=` against, and an
/// unexpanded `BETWEEN` is merely slower — a guessed OID would be a wrong
/// answer.
///
/// Where a type *is* available, it is read off whichever operand is a
/// literal (preferring `arg`, then `low`, then `high`) and used for every
/// comparison built here. This assumes `arg`, `low`, and `high` share one
/// comparable type — which is what made the original `BETWEEN` valid SQL in
/// the first place — so it is not a new assumption, just one this
/// schema-less rule cannot independently confirm.
fn expand_between(
    arg: &Expr,
    low: &Expr,
    high: &Expr,
    symmetric: bool,
    negated: bool,
) -> Option<Expr> {
    let ty = literal_type(arg)
        .or_else(|| literal_type(low))
        .or_else(|| literal_type(high))?;
    let cmp = |name: &str, l: &Expr, r: &Expr| compare_expr(name, ty, l, r);

    if !symmetric {
        let ge = cmp(">=", arg, low)?;
        let le = cmp("<=", arg, high)?;
        if !negated {
            // x BETWEEN a AND b  ->  x >= a AND x <= b
            Some(and(ge, le))
        } else {
            // x NOT BETWEEN a AND b  ->  x < a OR x > b
            let lt = cmp("<", arg, low)?;
            let gt = cmp(">", arg, high)?;
            Some(or(lt, gt))
        }
    } else if !negated {
        // x BETWEEN SYMMETRIC a AND b
        //   -> (x >= a AND x <= b) OR (x >= b AND x <= a)
        //
        // SYMMETRIC means the bounds may be given in either order; expanding
        // it as if ordered (the plain `!symmetric` case above) returns the
        // wrong rows whenever `a > b` — see
        // `between_symmetric_handles_reversed_bounds_correctly` below for
        // the concrete wrong answer this prevents.
        let ge_low = cmp(">=", arg, low)?;
        let le_high = cmp("<=", arg, high)?;
        let ge_high = cmp(">=", arg, high)?;
        let le_low = cmp("<=", arg, low)?;
        Some(or(and(ge_low, le_high), and(ge_high, le_low)))
    } else {
        // x NOT BETWEEN SYMMETRIC a AND b, by De Morgan over the SYMMETRIC
        // expansion above: (x < a OR x > b) AND (x < b OR x > a)
        let lt_low = cmp("<", arg, low)?;
        let gt_high = cmp(">", arg, high)?;
        let lt_high = cmp("<", arg, high)?;
        let gt_low = cmp(">", arg, low)?;
        Some(and(or(lt_low, gt_high), or(lt_high, gt_low)))
    }
}

/// The literal's own type, when `e` is a literal whose type is already
/// resolved — not the `unknown` pseudo-type a bare `NULL` or an unresolved
/// string constant carries before context settles it.
fn literal_type(e: &Expr) -> Option<PgType> {
    match e {
        Expr::Literal(_, ty) if !ty.is_unknown() => Some(*ty),
        _ => None,
    }
}

/// Build `lhs <name> rhs` by resolving `name` against the real `pg_operator`
/// table at `ty` on both sides. `None` when no such operator exists (e.g. a
/// type this table does not cover), in which case the caller leaves the
/// surrounding `BETWEEN` unexpanded rather than half-expand it.
fn compare_expr(name: &str, ty: PgType, lhs: &Expr, rhs: &Expr) -> Option<Expr> {
    let sig = basin_pgtype::operator::resolve(name, Some(ty.oid), ty.oid)?;
    Some(Expr::Binary {
        op: crate::OpId(sig.oid),
        lhs: Box::new(lhs.clone()),
        rhs: Box::new(rhs.clone()),
    })
}

// ─── Constant folding ──────────────────────────────────────────────────────

/// Look up the real `pg_operator` row an `OpId` names. The `AND_OP`/`OR_OP`/
/// `NOT_OP` sentinels above never match: every real oid in
/// `basin_pgtype::operator::OPERATORS` is in the low thousands, and those
/// sentinels are deliberately chosen from the top of `u32` to stay clear of
/// that range.
fn operator_row(op: Oid) -> Option<&'static basin_pgtype::operator::OperatorSig> {
    basin_pgtype::operator::OPERATORS
        .iter()
        .find(|sig| sig.oid == op)
}

/// Fold a binary operator over two literals, honoring every refusal in the
/// module docs. Returns `None` — leaving the operator in place — whenever
/// folding would require guessing, or would silently replace a Postgres
/// runtime error with a value.
fn fold_binary(op: Oid, l: &Expr, r: &Expr) -> Option<Expr> {
    let (Expr::Literal(ld, _), Expr::Literal(rd, _)) = (l, r) else {
        return None;
    };
    let sig = operator_row(op)?;

    // Every operator in this table is STRICT, same as (almost) every builtin
    // Postgres operator: NULL in, NULL out. This is also what makes
    // `NULL = NULL` fold to NULL rather than true.
    if matches!(ld, Datum::Null) || matches!(rd, Datum::Null) {
        return Some(Expr::Literal(Datum::Null, PgType::new(sig.result)));
    }

    match sig.name {
        "=" | "<>" | "<" | "<=" | ">" | ">=" => fold_compare(sig.name, ld, rd),
        "+" | "-" | "*" | "/" | "%" => fold_arith(sig.name, ld, rd, sig.result),
        _ => None,
    }
}

/// Fold a comparison over two non-NULL literal datums. `None` for any pair
/// this rule cannot order with confidence — `Bytes` (opaque here) or
/// mismatched variants — rather than a wrong answer.
fn fold_compare(name: &str, l: &Datum, r: &Datum) -> Option<Expr> {
    let ord = datum_cmp(l, r)?;
    let b = match name {
        "=" => ord.is_eq(),
        "<>" => !ord.is_eq(),
        "<" => ord.is_lt(),
        "<=" => ord.is_le(),
        ">" => ord.is_gt(),
        ">=" => ord.is_ge(),
        _ => return None,
    };
    Some(lit_bool(b))
}

/// Compare two literal datums the way Postgres would, for the types this
/// rule folds.
fn datum_cmp(l: &Datum, r: &Datum) -> Option<Ordering> {
    match (l, r) {
        (Datum::Bool(a), Datum::Bool(b)) => Some(a.cmp(b)),
        (Datum::Int16(a), Datum::Int16(b)) => Some(a.cmp(b)),
        (Datum::Int32(a), Datum::Int32(b)) => Some(a.cmp(b)),
        (Datum::Int64(a), Datum::Int64(b)) => Some(a.cmp(b)),
        (Datum::Utf8(a), Datum::Utf8(b)) => Some(a.cmp(b)),
        (Datum::Float32(a), Datum::Float32(b)) => Some(pg_float_cmp(*a as f64, *b as f64)),
        (Datum::Float64(a), Datum::Float64(b)) => Some(pg_float_cmp(*a, *b)),
        // Bytes (numeric-beyond-i64, uuid, jsonb, arrays) and any mismatched
        // pairing: no ordering this rule can compute without guessing.
        _ => None,
    }
}

/// Postgres's own float total order, not IEEE 754's `partial_cmp` — confirmed
/// live against PostgreSQL 18: `'NaN'::float8 = 'NaN'::float8` is `true` and
/// `'NaN'::float8 > 1e308` is `true`. NaN sorts as the greatest value and
/// compares equal to itself, the opposite of what a bare `f64::partial_cmp`
/// (which returns `None` for any comparison involving NaN) would suggest.
fn pg_float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).expect("neither operand is NaN"),
    }
}

/// Fold arithmetic over two non-NULL literal datums, refusing exactly the
/// cases the module docs call out: integer overflow and division (or modulo)
/// by zero. Both are runtime errors in Postgres — confirmed live against
/// PostgreSQL 18 (`2147483647::int4 + 1` raises `integer out of range`;
/// `1::int4 / 0` and `1.0::float8 / 0.0` both raise `division by zero`;
/// `1e308::float8 * 10` raises `value out of range: overflow`) — so folding
/// any of them would silently replace an error with a wrong answer.
fn fold_arith(name: &str, l: &Datum, r: &Datum, result: Oid) -> Option<Expr> {
    match (l, r) {
        (Datum::Int16(a), Datum::Int16(b)) => {
            checked_int(name, *a as i64, *b as i64, i16::MIN as i64, i16::MAX as i64)
                .map(|v| Expr::Literal(Datum::Int16(v as i16), PgType::new(result)))
        }
        (Datum::Int32(a), Datum::Int32(b)) => {
            checked_int(name, *a as i64, *b as i64, i32::MIN as i64, i32::MAX as i64)
                .map(|v| Expr::Literal(Datum::Int32(v as i32), PgType::new(result)))
        }
        (Datum::Int64(a), Datum::Int64(b)) => checked_int_i64(name, *a, *b)
            .map(|v| Expr::Literal(Datum::Int64(v), PgType::new(result))),
        (Datum::Float32(a), Datum::Float32(b)) => checked_float(name, *a as f64, *b as f64)
            .map(|v| Expr::Literal(Datum::Float32(v as f32), PgType::new(result))),
        (Datum::Float64(a), Datum::Float64(b)) => checked_float(name, *a, *b)
            .map(|v| Expr::Literal(Datum::Float64(v), PgType::new(result))),
        _ => None,
    }
}

/// `i16`/`i32` arithmetic, done widened to `i64` (so a shape like
/// `i16::MIN / -1` cannot itself panic Basin's own process) and then
/// range-checked against the narrower type's bounds — that range check is
/// the overflow refusal.
fn checked_int(name: &str, a: i64, b: i64, min: i64, max: i64) -> Option<i64> {
    let v = checked_int_i64(name, a, b)?;
    (min..=max).contains(&v).then_some(v)
}

/// `i64` arithmetic. `checked_add`/`checked_sub`/`checked_mul` already refuse
/// on overflow (including the `i64::MIN / -1` shape for division); division
/// and modulo additionally refuse a zero divisor up front rather than rely on
/// `checked_div`/`checked_rem` alone, since Postgres raises `division by
/// zero` there, not an overflow error.
fn checked_int_i64(name: &str, a: i64, b: i64) -> Option<i64> {
    match name {
        "+" => a.checked_add(b),
        "-" => a.checked_sub(b),
        "*" => a.checked_mul(b),
        "/" if b != 0 => a.checked_div(b),
        "%" if b != 0 => a.checked_rem(b),
        _ => None,
    }
}

/// `f32`/`f64` arithmetic, promoted to `f64`. Postgres has no float modulo
/// operator at all (see `basin_pgtype::operator`'s module docs), so `%` never
/// reaches here for a float pair — the `_ => return None` covers it.
fn checked_float(name: &str, a: f64, b: f64) -> Option<f64> {
    let v = match name {
        "+" => a + b,
        "-" => a - b,
        "*" => a * b,
        "/" if b != 0.0 => a / b,
        _ => return None,
    };
    // Neither operand was infinite, but the result is: Postgres raises
    // "value out of range: overflow" for this rather than producing an
    // IEEE-754 infinity, so this rule must not fold it either.
    if v.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        None
    } else {
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, ColumnRef, OpId, SnapshotId, TableId};
    use basin_pgtype::oid;

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
    fn int4(n: i32) -> Expr {
        Expr::Literal(Datum::Int32(n), PgType::INT4)
    }
    fn null_int4() -> Expr {
        Expr::Literal(Datum::Null, PgType::INT4)
    }
    fn op(name: &str) -> OpId {
        OpId(
            basin_pgtype::operator::resolve(name, Some(oid::INT4), oid::INT4)
                .unwrap_or_else(|| panic!("{name} on int4 must resolve"))
                .oid,
        )
    }
    fn bin(name: &str, l: Expr, r: Expr) -> Expr {
        Expr::Binary {
            op: op(name),
            lhs: Box::new(l),
            rhs: Box::new(r),
        }
    }
    fn not(e: Expr) -> Expr {
        Expr::Unary {
            op: OpId(NOT_OP),
            arg: Box::new(e),
        }
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

    /// The three boolean-connective sentinels this file invents must be
    /// pairwise distinct, or `simplify_and`/`simplify_or`/the `NOT NOT`
    /// collapse would misfire on each other's nodes.
    #[test]
    fn the_and_or_not_sentinels_are_pairwise_distinct() {
        assert_ne!(AND_OP, OR_OP);
        assert_ne!(AND_OP, NOT_OP);
        assert_ne!(OR_OP, NOT_OP);
    }

    // ─── BETWEEN expansion ──────────────────────────────────────────────

    #[test]
    fn between_expands_into_a_conjunction_of_comparisons_so_it_can_reach_a_scan() {
        let e = Expr::Between {
            arg: Box::new(col()),
            low: Box::new(int4(1)),
            high: Box::new(int4(10)),
            symmetric: false,
            negated: false,
        };
        let expect = and(bin(">=", col(), int4(1)), bin("<=", col(), int4(10)));
        assert_eq!(simplify(&e), expect);
    }

    #[test]
    fn not_between_expands_into_a_disjunction_of_strict_comparisons() {
        let e = Expr::Between {
            arg: Box::new(col()),
            low: Box::new(int4(1)),
            high: Box::new(int4(10)),
            symmetric: false,
            negated: true,
        };
        let expect = or(bin("<", col(), int4(1)), bin(">", col(), int4(10)));
        assert_eq!(simplify(&e), expect);
    }

    /// `5 BETWEEN SYMMETRIC 10 AND 1` must be true: SYMMETRIC treats the
    /// bounds as an unordered pair, so this really means `5 BETWEEN 1 AND
    /// 10`. Expanding it as if the bounds were already ordered — the plain
    /// (non-symmetric) rule's `5 >= 10 AND 5 <= 1` — folds to `false`, the
    /// wrong answer this rule must prevent.
    #[test]
    fn between_symmetric_handles_reversed_bounds_correctly() {
        let e = Expr::Between {
            arg: Box::new(int4(5)),
            low: Box::new(int4(10)),
            high: Box::new(int4(1)),
            symmetric: true,
            negated: false,
        };
        assert_eq!(simplify(&e), lit_bool(true));
    }

    /// The `NOT ... SYMMETRIC` combination, by De Morgan over the same
    /// reversed-bounds case: `5` IS between the unordered pair `{10, 1}`, so
    /// `NOT BETWEEN SYMMETRIC` must be false — not the `true` a naive
    /// non-symmetric `NOT` expansion (`5 < 10 OR 5 > 1`) would give.
    #[test]
    fn not_between_symmetric_handles_reversed_bounds_correctly() {
        let e = Expr::Between {
            arg: Box::new(int4(5)),
            low: Box::new(int4(10)),
            high: Box::new(int4(1)),
            symmetric: true,
            negated: true,
        };
        assert_eq!(simplify(&e), lit_bool(false));
    }

    /// No schema is available to this rule. With no literal anywhere to
    /// anchor a type on, resolving `>=`/`<=` would mean guessing an OID —
    /// worse than leaving `BETWEEN` unexpanded, which is merely slower.
    #[test]
    fn between_without_any_literal_operand_is_left_unexpanded() {
        let b = Expr::Column(ColumnRef {
            relation: 0,
            index: 1,
            name: "b".into(),
        });
        let c = Expr::Column(ColumnRef {
            relation: 0,
            index: 2,
            name: "c".into(),
        });
        let e = Expr::Between {
            arg: Box::new(col()),
            low: Box::new(b),
            high: Box::new(c),
            symmetric: false,
            negated: false,
        };
        assert_eq!(simplify(&e), e);
    }

    // ─── Constant folding ───────────────────────────────────────────────

    /// `2147483647 + 1` overflows `int4`. Postgres raises `integer out of
    /// range` for this (confirmed live) — folding it would silently bake the
    /// wrapped, negative `i32` result into the plan forever.
    #[test]
    fn integer_overflow_is_not_folded() {
        let e = bin("+", int4(i32::MAX), int4(1));
        assert_eq!(simplify(&e), e);
    }

    /// `1 / 0` raises `division by zero` (22012) in Postgres at runtime
    /// (confirmed live). Folding it would have to either panic during
    /// planning or invent a value — this rule does neither.
    #[test]
    fn integer_division_by_zero_is_not_folded() {
        let e = bin("/", int4(1), int4(0));
        assert_eq!(simplify(&e), e);
    }

    /// Same refusal for `%`, which shares the zero-divisor hazard with `/`.
    #[test]
    fn integer_modulo_by_zero_is_not_folded() {
        let e = bin("%", int4(1), int4(0));
        assert_eq!(simplify(&e), e);
    }

    /// `NULL = NULL` is NULL in Postgres, not `true` (confirmed live) —
    /// every operator this rule folds is STRICT, so any NULL operand yields
    /// NULL rather than a comparison result.
    #[test]
    fn null_equals_null_folds_to_null_not_true() {
        let e = bin("=", null_int4(), null_int4());
        assert_eq!(simplify(&e), Expr::Literal(Datum::Null, PgType::BOOL));
    }

    /// The ordinary case constant folding exists for: arithmetic that does
    /// not overflow folds to its literal result, letting downstream rules
    /// see a constant instead of an expression.
    #[test]
    fn arithmetic_over_two_literals_folds() {
        let e = bin("+", int4(2), int4(3));
        assert_eq!(simplify(&e), int4(5));
    }

    /// Likewise for comparisons: a decidable comparison over two literals
    /// folds to the boolean it actually evaluates to.
    #[test]
    fn comparison_over_two_literals_folds() {
        assert_eq!(simplify(&bin("<", int4(2), int4(3))), lit_bool(true));
        assert_eq!(simplify(&bin("<", int4(3), int4(2))), lit_bool(false));
    }

    // ─── NOT NOT ────────────────────────────────────────────────────────

    #[test]
    fn double_negation_cancels() {
        assert_eq!(simplify(&not(not(col()))), col());
    }

    /// A single `NOT` must not be dropped — only a genuine double negation
    /// cancels.
    #[test]
    fn single_negation_is_not_removed() {
        let e = not(col());
        assert_eq!(simplify(&e), e);
    }

    // ─── IS [NOT] NULL ──────────────────────────────────────────────────

    /// `x IS NOT NULL` on a literal is decidable at plan time and folds to a
    /// constant.
    #[test]
    fn is_not_null_on_a_literal_folds_to_a_constant() {
        let present = Expr::IsNull {
            arg: Box::new(int4(5)),
            negated: true,
        };
        assert_eq!(simplify(&present), lit_bool(true));

        let absent = Expr::IsNull {
            arg: Box::new(null_int4()),
            negated: true,
        };
        assert_eq!(simplify(&absent), lit_bool(false));
    }

    /// `x IS NOT NULL` on a column is a runtime fact this schema-less rule
    /// cannot see, so it must not be folded — folding it either way would be
    /// a guess presented as a certainty.
    #[test]
    fn is_not_null_on_a_column_is_not_folded() {
        let e = Expr::IsNull {
            arg: Box::new(col()),
            negated: true,
        };
        assert_eq!(simplify(&e), e);
    }
}
