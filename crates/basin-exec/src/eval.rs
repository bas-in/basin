//! Scalar expression evaluation over Arrow `RecordBatch`es.
//!
//! # Shape
//!
//! [`eval`] walks a `basin_plan::Expr` and returns one Arrow array per node,
//! always the same length as the input batch. Every leaf and combinator is
//! implemented by calling into an arrow compute kernel — `arrow` supplies
//! comparison, arithmetic, boolean and cast kernels that are already
//! vectorised and SIMD-friendly, so hand-rolling a `for row in 0..n` loop
//! here would both duplicate that work and be slower. The two exceptions
//! ([`eval_bool_test`] and the float-zero-divisor check in [`eval_div`]) are
//! called out below, because arrow has no kernel for either at all.
//!
//! # Where Arrow's defaults are wrong for Postgres
//!
//! Arrow is a columnar format, not a SQL engine, and several of its kernels
//! make choices that are reasonable defaults for Arrow but wrong answers for
//! Postgres. Getting each of these right (and pinning it with a test) is the
//! actual point of this file:
//!
//! 1. **Integer overflow.** [`arrow::compute::kernels::numeric::add`] (and
//!    `sub`/`mul`) are the *checked* variants — they already error on
//!    overflow rather than wrapping (the wrapping ones are `add_wrapping`
//!    etc., deliberately not used here). What this file adds is translating
//!    `ArrowError::ArithmeticOverflow` into [`ExecError::Overflow`], which is
//!    what makes it visible as SQLSTATE 22003 rather than an opaque internal
//!    error.
//! 2. **Division by zero.** Integer division already errors via
//!    `ArrowError::DivideByZero` (mapped straight through). Floats do not:
//!    `numeric::div`'s own doc says floats "follow the IEEE 754 rules", i.e.
//!    `1.0 / 0.0` silently becomes `Infinity`. Postgres's `float8div` checks
//!    the divisor itself and raises `division_by_zero` regardless — see
//!    [`eval_div`].
//! 3. **Three-valued logic.** `AND`/`OR` use the `_kleene` kernel variants,
//!    not the plain ones — the plain `and`/`or` treat NULL as if it were an
//!    ordinary missing value and produce NULL where Kleene logic says the
//!    known operand already decides the answer (`NULL AND FALSE = FALSE`).
//! 4. **`IS DISTINCT FROM`.** `arrow_ord::cmp` ships `distinct`/`not_distinct`
//!    kernels that already implement Postgres's null-safe equality exactly
//!    (never NULL, two NULLs are NOT DISTINCT) — no extra work needed beyond
//!    picking them over `eq`/`neq`.
//! 5. **Boolean tests.** No arrow kernel answers "is this exactly TRUE"
//!    versus "is this not exactly TRUE" — that distinction (`NULL IS NOT
//!    TRUE` is `true`, `NULL = TRUE` is NULL) is specific to Postgres's
//!    `BoolTest` node. [`eval_bool_test`] is the one place in this file that
//!    is a hand-written pass rather than a kernel call, precisely because no
//!    kernel exists.
//! 6. **`IN` with a NULL in the list.** Built from `eq`/`neq` folded with
//!    `or_kleene`/`and_kleene` rather than a dedicated kernel, which is what
//!    makes `2 IN (1, NULL)` come out NULL instead of FALSE "for free" — it
//!    falls out of Kleene `OR` the same way `NULL OR FALSE = NULL` does.
//!
//! # What is deliberately absent
//!
//! - `Aggregate`, `Window`, `SetReturning` and `Subquery` are operator-level
//!   concerns — an `Aggregate` node, for instance, is consumed by a hash- or
//!   sort-based aggregate operator that groups rows *before* any scalar
//!   evaluation happens, so there is no single-row value for `eval` to
//!   produce. Reaching one here is a planner/lowering bug, not user error.
//! - `LIKE ... ESCAPE` is not implemented: arrow's `like`/`ilike` kernels
//!   take no escape-character parameter, and faking one by pre-rewriting the
//!   pattern string is exactly the kind of textual hackery
//!   `basin_pgtype::operator`'s module docs describe replacing. Left as a
//!   named gap rather than a silent wrong answer.
//! - `Expr::Parameter`, `ScalarFn`, `ArrayLit`, `RowLit`, `Subscript` and
//!   `FieldSelect` are simply not built yet; they fall through to a single
//!   catch-all `Internal` arm at the bottom of [`eval`].
//! - `AND` / `OR` have no `pg_operator` row — Postgres parses them as a
//!   `BoolExpr`, not an `OpExpr` — and `Expr` has no dedicated variant for
//!   them yet either (see the same gap noted in
//!   `basin_plan::opt::pushdown`'s module docs). This file recognizes them
//!   through the same kind of private sentinel `OpId` that
//!   `opt::pushdown::AND_OP` already uses for exactly this reason, reusing
//!   its exact value for `AND` so the two files agree by construction rather
//!   than by coincidence. If `Expr` grows a real `And`/`Or` variant, only
//!   [`AND_OP`] and [`OR_OP`] need to change.

use std::sync::Arc;

use arrow::compute::kernels::{boolean, cast, cmp, comparison, numeric, zip};
use arrow_array::{
    new_null_array, Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType};

use basin_pgtype::{physical, Oid, PgType};
use basin_plan::{BoolTest, ColumnRef, Datum as PlanDatum, Expr, OpId};

use crate::ExecError;

/// See the module docs' note on `AND`/`OR`. Chosen as `u32::MAX` to match
/// `basin_plan::opt::pushdown::AND_OP` exactly — both files independently
/// need a sentinel that cannot alias a real `pg_operator` oid (the largest
/// real one in `basin_pgtype::operator::OPERATORS` is in the low thousands),
/// and picking the same value means a plan built one way and evaluated the
/// other stays consistent by construction.
const AND_OP: OpId = OpId(Oid(u32::MAX));
/// `OR`'s counterpart to [`AND_OP`]. No other file needs an `OR` sentinel
/// yet, so this one is local to `eval.rs`.
const OR_OP: OpId = OpId(Oid(u32::MAX - 1));

/// Evaluate a scalar expression against every row of `batch`, producing one
/// Arrow array of length `batch.num_rows()`.
pub fn eval(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    match expr {
        Expr::Column(col) => eval_column(col, batch),
        Expr::Literal(datum, ty) => eval_literal(datum, *ty, batch.num_rows()),
        Expr::Unary { op, arg } => eval_unary(*op, arg, batch),
        Expr::Binary { op, lhs, rhs } => eval_binary(*op, lhs, rhs, batch),
        Expr::Cast { arg, to, .. } => eval_cast(arg, *to, batch),
        Expr::Case {
            operand,
            whens,
            else_,
        } => eval_case(operand, whens, else_, batch),
        Expr::Coalesce(exprs) => eval_coalesce(exprs, batch),
        Expr::IsNull { arg, negated } => eval_is_null(arg, *negated, batch),
        Expr::BoolTest { arg, test } => {
            let a = eval(arg, batch)?;
            let a = require_bool(&a)?;
            Ok(Arc::new(eval_bool_test(a, *test)))
        }
        Expr::DistinctFrom { lhs, rhs, negated } => eval_distinct_from(lhs, rhs, *negated, batch),
        Expr::InList { arg, list, negated } => eval_in_list(arg, list, *negated, batch),
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => eval_between(arg, low, high, *symmetric, *negated, batch),
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => eval_like(arg, pattern, escape, *case_insensitive, *negated, batch),

        // Operator-level, not scalar — see the module docs.
        Expr::Aggregate { .. } => Err(ExecError::Internal(
            "aggregate expressions are evaluated by the Aggregate operator, over groups of rows, \
             not by scalar eval over a single row"
                .to_string(),
        )),
        Expr::Window { .. } => Err(ExecError::Internal(
            "window expressions are evaluated by the Window operator, which needs the whole \
             partition, not by scalar eval"
                .to_string(),
        )),
        Expr::SetReturning { .. } => Err(ExecError::Internal(
            "set-returning functions are expanded by ProjectSet, which can change the row count, \
             not by scalar eval"
                .to_string(),
        )),
        Expr::Subquery { .. } => Err(ExecError::Internal(
            "subqueries must be decorrelated into a join (or a scalar materialized elsewhere) \
             before scalar eval sees them"
                .to_string(),
        )),

        // Not yet built — see the module docs' "what is deliberately absent".
        other => Err(ExecError::Internal(format!(
            "{other:?} is not implemented in eval yet"
        ))),
    }
}

fn eval_column(col: &ColumnRef, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let idx = col.index as usize;
    batch.columns().get(idx).cloned().ok_or_else(|| {
        ExecError::Internal(format!(
            "column index {idx} ('{}') out of range for a {}-column batch — a planner bug, \
             not user error",
            col.name,
            batch.num_columns()
        ))
    })
}

/// Materialize a literal as an array of length `len`.
///
/// Only the scalar physical types that appear in practice today (bool, the
/// integer and float widths, text, bytea) are handled. `numeric` beyond
/// `Decimal128`, `uuid`, `jsonb` and array literals all need their own
/// builders and are not implemented yet — that is a real gap, called out
/// with `Internal` rather than silently producing the wrong type.
fn eval_literal(datum: &PlanDatum, ty: PgType, len: usize) -> Result<ArrayRef, ExecError> {
    let arrow_ty = physical(ty).map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

    if matches!(datum, PlanDatum::Null) {
        return Ok(new_null_array(&arrow_ty, len));
    }

    let array: ArrayRef = match (datum, &arrow_ty) {
        (PlanDatum::Bool(v), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*v; len])),
        (PlanDatum::Int16(v), DataType::Int16) => Arc::new(Int16Array::from_value(*v, len)),
        (PlanDatum::Int32(v), DataType::Int32) => Arc::new(Int32Array::from_value(*v, len)),
        (PlanDatum::Int64(v), DataType::Int64) => Arc::new(Int64Array::from_value(*v, len)),
        (PlanDatum::Float32(v), DataType::Float32) => Arc::new(Float32Array::from_value(*v, len)),
        (PlanDatum::Float64(v), DataType::Float64) => Arc::new(Float64Array::from_value(*v, len)),
        (PlanDatum::Utf8(s), DataType::Utf8) => Arc::new(StringArray::from(vec![s.as_str(); len])),
        (PlanDatum::Bytes(b), DataType::Binary) => {
            Arc::new(BinaryArray::from(vec![b.as_slice(); len]))
        }
        _ => {
            return Err(ExecError::Internal(format!(
                "literal {datum:?} of physical type {arrow_ty:?} is not implemented in eval yet"
            )));
        }
    };
    Ok(array)
}

fn eval_unary(op: OpId, arg: &Expr, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let v = eval(arg, batch)?;
    match catalog_op_name(op) {
        // Unary minus is the only builtin prefix operator over these types —
        // see `basin_pgtype::operator`'s module docs. `numeric::neg` is the
        // checked variant, so overflow (`-(-2147483648)` on int4) already
        // errors rather than wrapping; only the translation to
        // `ExecError::Overflow` happens here.
        Some("-") => numeric::neg(v.as_ref()).map_err(|e| map_arrow(e, "negation")),
        Some(other) => Err(ExecError::Internal(format!(
            "unary operator '{other}' is not implemented in eval yet"
        ))),
        None => Err(ExecError::Internal(format!(
            "unknown unary operator oid {} — a planner bug, not user error",
            op.0.get()
        ))),
    }
}

fn eval_binary(
    op: OpId,
    lhs: &Expr,
    rhs: &Expr,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    if op == AND_OP {
        let l = eval(lhs, batch)?;
        let r = eval(rhs, batch)?;
        let l = require_bool(&l)?;
        let r = require_bool(&r)?;
        // Kleene, not the plain kernel: `NULL AND FALSE` must be FALSE, not
        // NULL — see the module docs' point 3.
        return Ok(Arc::new(
            boolean::and_kleene(l, r).map_err(|e| map_arrow(e, "AND"))?,
        ));
    }
    if op == OR_OP {
        let l = eval(lhs, batch)?;
        let r = eval(rhs, batch)?;
        let l = require_bool(&l)?;
        let r = require_bool(&r)?;
        return Ok(Arc::new(
            boolean::or_kleene(l, r).map_err(|e| map_arrow(e, "OR"))?,
        ));
    }

    let name = catalog_op_name(op).ok_or_else(|| {
        ExecError::Internal(format!(
            "unknown operator oid {} — a planner bug, not user error",
            op.0.get()
        ))
    })?;

    let l = eval(lhs, batch)?;
    let r = eval(rhs, batch)?;

    match name {
        "=" => Ok(Arc::new(cmp::eq(&l, &r).map_err(|e| map_arrow(e, "="))?)),
        "<>" => Ok(Arc::new(cmp::neq(&l, &r).map_err(|e| map_arrow(e, "<>"))?)),
        "<" => Ok(Arc::new(cmp::lt(&l, &r).map_err(|e| map_arrow(e, "<"))?)),
        "<=" => Ok(Arc::new(
            cmp::lt_eq(&l, &r).map_err(|e| map_arrow(e, "<="))?,
        )),
        ">" => Ok(Arc::new(cmp::gt(&l, &r).map_err(|e| map_arrow(e, ">"))?)),
        ">=" => Ok(Arc::new(
            cmp::gt_eq(&l, &r).map_err(|e| map_arrow(e, ">="))?,
        )),
        // `add`/`sub`/`mul` are arrow's *checked* kernels — they already
        // error on overflow instead of wrapping. See the module docs' point 1.
        "+" => numeric::add(&l, &r).map_err(|e| map_arrow(e, "integer addition")),
        "-" => numeric::sub(&l, &r).map_err(|e| map_arrow(e, "integer subtraction")),
        "*" => numeric::mul(&l, &r).map_err(|e| map_arrow(e, "integer multiplication")),
        "/" => eval_div(&l, &r),
        "%" => numeric::rem(&l, &r).map_err(|e| map_arrow(e, "modulo")),
        other => Err(ExecError::Internal(format!(
            "operator '{other}' (oid {}) is not implemented in eval yet",
            op.0.get()
        ))),
    }
}

/// `lhs / rhs`. See the module docs' point 2: arrow's integer division
/// already errors on a zero divisor (`ArrowError::DivideByZero`, mapped
/// through by [`map_arrow`]), but float division follows IEEE 754 and
/// silently produces `Infinity`/`NaN` instead. Postgres's `float8div` /
/// `float4div` check the divisor themselves and raise `division_by_zero`
/// regardless of type, so this function checks first for float divisors
/// rather than trusting the kernel.
fn eval_div(l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef, ExecError> {
    reject_float_zero_divisor(r)?;
    numeric::div(l, r).map_err(|e| map_arrow(e, "division"))
}

fn reject_float_zero_divisor(r: &ArrayRef) -> Result<(), ExecError> {
    // A single pass over already-materialized values, not a reimplementation
    // of a compute kernel — arrow has no "does this array contain a zero"
    // kernel, and this only runs over the (small) divisor side.
    match r.data_type() {
        DataType::Float32 => {
            let a = r
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("data_type() said Float32");
            if a.iter().flatten().any(|v| v == 0.0) {
                return Err(ExecError::DivisionByZero);
            }
        }
        DataType::Float64 => {
            let a = r
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("data_type() said Float64");
            if a.iter().flatten().any(|v| v == 0.0) {
                return Err(ExecError::DivisionByZero);
            }
        }
        _ => {}
    }
    Ok(())
}

fn eval_cast(arg: &Expr, to: PgType, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let v = eval(arg, batch)?;
    let target = physical(to).map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
    // `kind` (implicit/assignment/explicit) governs whether a cast is
    // *legal* at a given syntactic position — a planning-time question,
    // already settled by the time this Expr exists. It has no bearing on how
    // the cast runs, so it is not consulted here.
    cast::cast(&v, &target).map_err(|e| map_arrow(e, "CAST"))
}

/// `CASE`. Built from `zip`, applied right-to-left: start from `ELSE` (or an
/// untyped NULL array if there is none), then fold each `WHEN` over the
/// accumulator. `zip`'s own semantics — truthy where the mask is `true`,
/// falsy where it is `false` *or NULL* — are exactly Postgres's CASE
/// semantics (an unproven or NULL condition falls through to the next
/// branch), so no extra NULL-handling is needed here.
fn eval_case(
    operand: &Option<Box<Expr>>,
    whens: &[(Expr, Expr)],
    else_: &Option<Box<Expr>>,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    if whens.is_empty() {
        return match else_ {
            Some(e) => eval(e, batch),
            None => Err(ExecError::Internal(
                "CASE with no WHEN and no ELSE — a planner bug, not user error".to_string(),
            )),
        };
    }

    // `CASE operand WHEN v THEN …` is Postgres sugar for
    // `CASE WHEN operand = v THEN …`; evaluate `operand` once up front.
    let operand_arr = match operand {
        Some(o) => Some(eval(o, batch)?),
        None => None,
    };

    let mut acc: Option<ArrayRef> = match else_ {
        Some(e) => Some(eval(e, batch)?),
        None => None,
    };

    for (cond_expr, then_expr) in whens.iter().rev() {
        let then_arr = eval(then_expr, batch)?;
        let cond_arr: BooleanArray = match &operand_arr {
            Some(o) => {
                let v = eval(cond_expr, batch)?;
                cmp::eq(o, &v).map_err(|e| map_arrow(e, "CASE"))?
            }
            None => {
                let c = eval(cond_expr, batch)?;
                require_bool(&c)?.clone()
            }
        };
        let base: ArrayRef = match acc.take() {
            Some(a) => a,
            None => new_null_array(then_arr.data_type(), batch.num_rows()),
        };
        acc = Some(zip::zip(&cond_arr, &then_arr, &base).map_err(|e| map_arrow(e, "CASE"))?);
    }

    Ok(acc.expect("loop ran at least once since whens is non-empty"))
}

/// `COALESCE(a, b, c)`: the first non-null value, left to right.
///
/// Note this is *not* `arrow::compute::kernels::coalesce` — that module is
/// `BatchCoalescer`, which concatenates small `RecordBatch`es into bigger
/// ones after `filter`/`take`, an entirely unrelated concept that happens to
/// share the SQL function's name. This builds `COALESCE` instead from
/// `is_not_null` + `zip`, folded right to left: start from the last
/// expression, then for each earlier one, take it where it is not null and
/// fall back to the accumulator otherwise.
fn eval_coalesce(exprs: &[Expr], batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let Some((last, rest)) = exprs.split_last() else {
        return Err(ExecError::Internal(
            "COALESCE with no arguments — a planner bug, not user error".to_string(),
        ));
    };
    let mut acc = eval(last, batch)?;
    for e in rest.iter().rev() {
        let v = eval(e, batch)?;
        let mask = boolean::is_not_null(&v).map_err(|e| map_arrow(e, "COALESCE"))?;
        acc = zip::zip(&mask, &v, &acc).map_err(|e| map_arrow(e, "COALESCE"))?;
    }
    Ok(acc)
}

fn eval_is_null(arg: &Expr, negated: bool, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let a = eval(arg, batch)?;
    let result = if negated {
        boolean::is_not_null(&a)
    } else {
        boolean::is_null(&a)
    }
    .map_err(|e| map_arrow(e, "IS NULL"))?;
    Ok(Arc::new(result))
}

/// Postgres's six boolean tests. No arrow kernel implements any of these —
/// they are specific to three-valued SQL boolean logic, e.g. `NULL IS NOT
/// TRUE` is `true` where `NULL = TRUE` is `NULL`. This is therefore a
/// hand-written pass, but a bounded one: a single pass over an
/// already-materialized `BooleanArray`, not a reimplementation of a numeric
/// kernel.
fn eval_bool_test(a: &BooleanArray, test: BoolTest) -> BooleanArray {
    let values: Vec<bool> = a
        .iter()
        .map(|v| match test {
            BoolTest::IsTrue => v == Some(true),
            BoolTest::IsNotTrue => v != Some(true),
            BoolTest::IsFalse => v == Some(false),
            BoolTest::IsNotFalse => v != Some(false),
            BoolTest::IsUnknown => v.is_none(),
            BoolTest::IsNotUnknown => v.is_some(),
        })
        .collect();
    BooleanArray::from(values)
}

/// `IS [NOT] DISTINCT FROM`. `cmp::distinct`/`cmp::not_distinct` already are
/// Postgres's null-safe equality exactly — never NULL, two NULLs are NOT
/// DISTINCT — so this is a direct pass-through, not a semantic gap to close.
fn eval_distinct_from(
    lhs: &Expr,
    rhs: &Expr,
    negated: bool,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    let l = eval(lhs, batch)?;
    let r = eval(rhs, batch)?;
    let result = if negated {
        cmp::not_distinct(&l, &r)
    } else {
        cmp::distinct(&l, &r)
    }
    .map_err(|e| map_arrow(e, "IS DISTINCT FROM"))?;
    Ok(Arc::new(result))
}

/// `x [NOT] IN (v1, .., vn)`, built as a fold of `eq`/`neq` over
/// `or_kleene`/`and_kleene` rather than a dedicated kernel. That choice is
/// what makes three-valued logic fall out for free: `2 IN (1, NULL)` folds
/// to `or_kleene(eq(2,1), eq(2,NULL))` = `or_kleene(false, NULL)` = `NULL`,
/// exactly matching Postgres — see the module docs' point 6. `NOT IN` folds
/// the De Morgan dual (`neq` over `and_kleene`) rather than negating the `IN`
/// result afterwards, because that is what Postgres's own rewrite does and
/// it keeps the two spellings symmetric.
fn eval_in_list(
    arg: &Expr,
    list: &[Expr],
    negated: bool,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    let x = eval(arg, batch)?;
    let Some((first, rest)) = list.split_first() else {
        return Err(ExecError::Internal(
            "IN with an empty list — a planner bug, not user error (the SQL grammar requires \
             at least one element)"
                .to_string(),
        ));
    };

    let mut acc = eval_in_list_test(&x, first, negated, batch)?;
    for item in rest {
        let test = eval_in_list_test(&x, item, negated, batch)?;
        acc = if negated {
            boolean::and_kleene(&acc, &test)
        } else {
            boolean::or_kleene(&acc, &test)
        }
        .map_err(|e| map_arrow(e, "IN"))?;
    }
    Ok(Arc::new(acc))
}

fn eval_in_list_test(
    x: &ArrayRef,
    item: &Expr,
    negated: bool,
    batch: &RecordBatch,
) -> Result<BooleanArray, ExecError> {
    let v = eval(item, batch)?;
    if negated {
        cmp::neq(x, &v)
    } else {
        cmp::eq(x, &v)
    }
    .map_err(|e| map_arrow(e, "IN"))
}

/// `x [NOT] BETWEEN [SYMMETRIC] low AND high`.
///
/// `BETWEEN` is `x >= low AND x <= high`; `SYMMETRIC` additionally tries the
/// swapped bounds and takes either match, i.e.
/// `(x BETWEEN low,high) OR (x BETWEEN high,low)`. `NOT` is applied last, as
/// a single `boolean::not` over the whole (possibly-SYMMETRIC) result rather
/// than restructured into the equivalent `x < low OR x > high` — `not` is
/// Kleene-correct (NULL stays NULL) and De Morgan holds under Kleene logic
/// too, so negating at the end is both simpler and exactly equivalent.
#[allow(clippy::too_many_arguments)]
fn eval_between(
    arg: &Expr,
    low: &Expr,
    high: &Expr,
    symmetric: bool,
    negated: bool,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    let x = eval(arg, batch)?;
    let low_v = eval(low, batch)?;
    let high_v = eval(high, batch)?;

    let ge_low = cmp::gt_eq(&x, &low_v).map_err(|e| map_arrow(e, "BETWEEN"))?;
    let le_high = cmp::lt_eq(&x, &high_v).map_err(|e| map_arrow(e, "BETWEEN"))?;
    let base = if symmetric {
        let ascending =
            boolean::and_kleene(&ge_low, &le_high).map_err(|e| map_arrow(e, "BETWEEN"))?;
        let ge_high = cmp::gt_eq(&x, &high_v).map_err(|e| map_arrow(e, "BETWEEN"))?;
        let le_low = cmp::lt_eq(&x, &low_v).map_err(|e| map_arrow(e, "BETWEEN"))?;
        let descending =
            boolean::and_kleene(&ge_high, &le_low).map_err(|e| map_arrow(e, "BETWEEN"))?;
        boolean::or_kleene(&ascending, &descending).map_err(|e| map_arrow(e, "BETWEEN"))?
    } else {
        boolean::and_kleene(&ge_low, &le_high).map_err(|e| map_arrow(e, "BETWEEN"))?
    };

    let result = if negated {
        boolean::not(&base).map_err(|e| map_arrow(e, "BETWEEN"))?
    } else {
        base
    };
    Ok(Arc::new(result))
}

/// `x [NOT] LIKE|ILIKE pattern [ESCAPE e]`. `ESCAPE` is rejected explicitly
/// (see the module docs) rather than silently ignored, since silently
/// ignoring it would change which rows match.
fn eval_like(
    arg: &Expr,
    pattern: &Expr,
    escape: &Option<Box<Expr>>,
    case_insensitive: bool,
    negated: bool,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    if escape.is_some() {
        return Err(ExecError::Internal(
            "LIKE ... ESCAPE is not implemented — arrow's LIKE/ILIKE kernels take no \
             escape-character parameter"
                .to_string(),
        ));
    }
    let a = eval(arg, batch)?;
    let p = eval(pattern, batch)?;
    let base = if case_insensitive {
        comparison::ilike(&a, &p)
    } else {
        comparison::like(&a, &p)
    }
    .map_err(|e| map_arrow(e, "LIKE"))?;
    let result = if negated {
        boolean::not(&base).map_err(|e| map_arrow(e, "LIKE"))?
    } else {
        base
    };
    Ok(Arc::new(result))
}

/// Look up an operator's `pg_operator.oprname` by oid. `None` for the two
/// `eval.rs`-local sentinels ([`AND_OP`], [`OR_OP`]) as well as for any
/// genuinely unknown oid — callers that care about the difference check the
/// sentinels themselves first.
fn catalog_op_name(op: OpId) -> Option<&'static str> {
    basin_pgtype::operator::OPERATORS
        .iter()
        .find(|sig| sig.oid == op.0)
        .map(|sig| sig.name)
}

fn require_bool(array: &ArrayRef) -> Result<&BooleanArray, ExecError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "expected a boolean array, found {:?}",
                array.data_type()
            ))
        })
}

/// Translate an arrow kernel failure into an [`ExecError`]. `op` names the
/// operation for [`ExecError::Overflow`]'s message; it is unused for the
/// other variants but kept as one signature so every call site reads the
/// same way.
fn map_arrow(err: ArrowError, op: &'static str) -> ExecError {
    match err {
        ArrowError::DivideByZero => ExecError::DivisionByZero,
        ArrowError::ArithmeticOverflow(_) => ExecError::Overflow(op),
        ArrowError::InvalidArgumentError(msg)
        | ArrowError::CastError(msg)
        | ArrowError::ComputeError(msg) => ExecError::TypeMismatch(msg),
        other => ExecError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, Int64Array as I64, RecordBatch};
    use arrow_schema::{Field, Schema};
    use basin_plan::{ColumnRef, Datum, FuncId, SubqueryKind};

    fn batch_i32(name: &str, values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn batch_bool2(a: Vec<Option<bool>>, b: Vec<Option<bool>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(a)),
                Arc::new(BooleanArray::from(b)),
            ],
        )
        .unwrap()
    }

    fn col(index: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.to_string(),
        })
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    fn bool_array(v: &ArrayRef) -> &BooleanArray {
        v.as_any().downcast_ref::<BooleanArray>().unwrap()
    }

    fn i32_array(v: &ArrayRef) -> &Int32Array {
        v.as_any().downcast_ref::<Int32Array>().unwrap()
    }

    fn op(oid_val: u32) -> OpId {
        OpId(Oid(oid_val))
    }

    // ── 1. Integer overflow must error ──────────────────────────────────
    //
    // Arrow's `add`/`sub`/`mul` kernels have both a checked and a wrapping
    // form. Using the wrong one would make `i32::MAX + 1` silently become
    // `i32::MIN` instead of raising Postgres's 22003 — the single worst
    // failure mode this file exists to prevent.
    #[test]
    fn integer_addition_overflow_errors_instead_of_wrapping() {
        let batch = batch_i32("x", vec![Some(i32::MAX)]);
        let expr = Expr::Binary {
            op: op(551), // int4 +
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(1)),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert!(
            matches!(err, ExecError::Overflow(_)),
            "expected Overflow, got {err:?} — a wrapping add would have silently \
             produced i32::MIN"
        );
    }

    #[test]
    fn integer_multiplication_overflow_errors_instead_of_wrapping() {
        let batch = batch_i32("x", vec![Some(i32::MAX)]);
        let expr = Expr::Binary {
            op: op(514), // int4 *
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(2)),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert!(matches!(err, ExecError::Overflow(_)));
    }

    // ── 2. Division by zero must error, not yield NULL ──────────────────
    #[test]
    fn integer_division_by_zero_errors_rather_than_yielding_null() {
        let batch = batch_i32("x", vec![Some(10)]);
        let expr = Expr::Binary {
            op: op(528), // int4 /
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(0)),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert_eq!(
            err,
            ExecError::DivisionByZero,
            "10 / 0 must raise division_by_zero, not silently produce NULL"
        );
    }

    /// Floats are the sharper case: arrow's `div` kernel follows IEEE 754
    /// for floats and would happily return `Infinity` here with no error at
    /// all — Postgres's `float8div` explicitly checks and raises
    /// division_by_zero regardless of type.
    #[test]
    fn float_division_by_zero_errors_rather_than_returning_infinity() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![Some(1.0)]))])
                .unwrap();
        let expr = Expr::Binary {
            op: op(593), // float8 /
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(Expr::Literal(Datum::Float64(0.0), PgType::FLOAT8)),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert_eq!(
            err,
            ExecError::DivisionByZero,
            "1.0 / 0.0 must error like Postgres's float8div, not return Infinity like raw IEEE 754"
        );
    }

    // ── 3. Three-valued logic for AND / OR ───────────────────────────────
    #[test]
    fn null_and_false_is_false_not_null() {
        let batch = batch_bool2(vec![None], vec![Some(false)]);
        let expr = Expr::Binary {
            op: AND_OP,
            lhs: Box::new(col(0, "a")),
            rhs: Box::new(col(1, "b")),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(
            !(arr.value(0)),
            "NULL AND FALSE must be FALSE — the plain (non-Kleene) `and` kernel \
             would have produced NULL here"
        );
        assert!(!arr.is_null(0), "the result must not be NULL either");
    }

    #[test]
    fn null_or_true_is_true_not_null() {
        let batch = batch_bool2(vec![None], vec![Some(true)]);
        let expr = Expr::Binary {
            op: OR_OP,
            lhs: Box::new(col(0, "a")),
            rhs: Box::new(col(1, "b")),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(
            arr.value(0),
            "NULL OR TRUE must be TRUE — the plain `or` kernel would have produced NULL"
        );
        assert!(!arr.is_null(0));
    }

    // ── 4. IS DISTINCT FROM is null-safe ─────────────────────────────────
    #[test]
    fn null_is_distinct_from_null_is_false() {
        let batch = batch_i32("x", vec![None]);
        let expr = Expr::DistinctFrom {
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(Expr::Literal(Datum::Null, PgType::INT4)),
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(
            !arr.is_null(0),
            "IS DISTINCT FROM must never itself return NULL"
        );
        assert!(
            !(arr.value(0)),
            "NULL IS DISTINCT FROM NULL must be FALSE — plain `<>` would have \
             produced NULL instead"
        );
    }

    #[test]
    fn null_is_distinct_from_a_value_is_true() {
        let batch = batch_i32("x", vec![None]);
        let expr = Expr::DistinctFrom {
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(1)),
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.is_null(0));
        assert!(arr.value(0), "NULL IS DISTINCT FROM 1 must be TRUE");
    }

    // ── 5. BoolTest on NULL ──────────────────────────────────────────────
    #[test]
    fn null_is_not_true_is_true() {
        let batch = batch_bool2(vec![None], vec![Some(true)]);
        let expr = Expr::BoolTest {
            arg: Box::new(col(0, "a")),
            test: BoolTest::IsNotTrue,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.is_null(0), "IS NOT TRUE never itself returns NULL");
        assert!(
            arr.value(0),
            "NULL IS NOT TRUE must be TRUE — confusing this with `<> TRUE` (which \
             is NULL) is the classic mistake here"
        );
    }

    #[test]
    fn null_is_true_is_false_not_null() {
        let batch = batch_bool2(vec![None], vec![Some(true)]);
        let expr = Expr::BoolTest {
            arg: Box::new(col(0, "a")),
            test: BoolTest::IsTrue,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.is_null(0));
        assert!(!(arr.value(0)), "NULL IS TRUE must be FALSE, not NULL");
    }

    // ── 6. IN with a NULL in the list ────────────────────────────────────
    #[test]
    fn in_list_containing_null_yields_null_when_no_other_match() {
        // x = 2, list = (1, NULL): no definite match, but the NULL means the
        // answer is "unknown", not "definitely not in the list".
        let batch = batch_i32("x", vec![Some(2)]);
        let expr = Expr::InList {
            arg: Box::new(col(0, "x")),
            list: vec![lit_i32(1), Expr::Literal(Datum::Null, PgType::INT4)],
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(
            arr.is_null(0),
            "2 IN (1, NULL) must be NULL, not FALSE — a naive equals-any-element \
             scan ignoring the NULL would wrongly say FALSE"
        );
    }

    #[test]
    fn in_list_containing_null_is_still_true_on_a_definite_match() {
        // x = 1: a definite match makes the answer TRUE even though the list
        // also contains a NULL — Kleene OR's other rule (`true OR NULL = true`).
        let batch = batch_i32("x", vec![Some(1)]);
        let expr = Expr::InList {
            arg: Box::new(col(0, "x")),
            list: vec![lit_i32(1), Expr::Literal(Datum::Null, PgType::INT4)],
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.is_null(0));
        assert!(arr.value(0), "1 IN (1, NULL) must be TRUE");
    }

    // ── Supporting coverage for the rest of the required surface ────────

    #[test]
    fn column_reads_the_matching_arrow_array() {
        let batch = batch_i32("x", vec![Some(7), None]);
        let result = eval(&col(0, "x"), &batch).unwrap();
        let arr = i32_array(&result);
        assert_eq!(arr.value(0), 7);
        assert!(arr.is_null(1));
    }

    #[test]
    fn literal_broadcasts_to_every_row() {
        let batch = batch_i32("x", vec![Some(1), Some(2), Some(3)]);
        let result = eval(&lit_i32(42), &batch).unwrap();
        let arr = i32_array(&result);
        assert_eq!(arr.len(), 3);
        assert!(arr.iter().all(|v| v == Some(42)));
    }

    #[test]
    fn unary_minus_negates() {
        let batch = batch_i32("x", vec![Some(5)]);
        let expr = Expr::Unary {
            op: op(558), // int4 unary -
            arg: Box::new(col(0, "x")),
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(i32_array(&result).value(0), -5);
    }

    #[test]
    fn case_searched_form_falls_through_null_conditions_to_else() {
        // WHEN NULL THEN 1 ELSE 2 END — a NULL condition must NOT match,
        // same as FALSE, and fall through to ELSE.
        let batch = batch_i32("x", vec![Some(0)]);
        let expr = Expr::Case {
            operand: None,
            whens: vec![(Expr::Literal(Datum::Null, PgType::BOOL), lit_i32(1))],
            else_: Some(Box::new(lit_i32(2))),
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(i32_array(&result).value(0), 2);
    }

    #[test]
    fn case_simple_form_compares_operand_to_each_when() {
        let batch = batch_i32("x", vec![Some(2)]);
        let expr = Expr::Case {
            operand: Some(Box::new(col(0, "x"))),
            whens: vec![(lit_i32(1), lit_i32(100)), (lit_i32(2), lit_i32(200))],
            else_: Some(Box::new(lit_i32(0))),
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(i32_array(&result).value(0), 200);
    }

    #[test]
    fn coalesce_returns_the_first_non_null() {
        let batch = batch_i32("x", vec![None]);
        let expr = Expr::Coalesce(vec![col(0, "x"), lit_i32(9), lit_i32(1)]);
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(i32_array(&result).value(0), 9);
    }

    #[test]
    fn between_matches_inclusive_bounds() {
        let batch = batch_i32("x", vec![Some(5), Some(10), Some(11)]);
        let expr = Expr::Between {
            arg: Box::new(col(0, "x")),
            low: Box::new(lit_i32(5)),
            high: Box::new(lit_i32(10)),
            symmetric: false,
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(arr.value(0), "lower bound is inclusive");
        assert!(arr.value(1), "upper bound is inclusive");
        assert!(!(arr.value(2)));
    }

    #[test]
    fn between_symmetric_accepts_swapped_bounds() {
        let batch = batch_i32("x", vec![Some(7)]);
        let expr = Expr::Between {
            arg: Box::new(col(0, "x")),
            low: Box::new(lit_i32(10)), // low > high
            high: Box::new(lit_i32(5)),
            symmetric: true,
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        assert!(
            bool_array(&result).value(0),
            "BETWEEN SYMMETRIC must try both orderings of the bounds"
        );
    }

    #[test]
    fn like_matches_percent_and_underscore_wildcards() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .unwrap();
        let expr = Expr::Like {
            arg: Box::new(Expr::Column(ColumnRef {
                relation: 0,
                index: 0,
                name: "s".to_string(),
            })),
            pattern: Box::new(Expr::Literal(Datum::Utf8("h_%".to_string()), PgType::TEXT)),
            escape: None,
            case_insensitive: false,
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(arr.value(0));
        assert!(!(arr.value(1)));
    }

    #[test]
    fn like_escape_is_a_named_gap_not_a_silent_wrong_answer() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["100%"]))]).unwrap();
        let expr = Expr::Like {
            arg: Box::new(Expr::Column(ColumnRef {
                relation: 0,
                index: 0,
                name: "s".to_string(),
            })),
            pattern: Box::new(Expr::Literal(
                Datum::Utf8("100$%".to_string()),
                PgType::TEXT,
            )),
            escape: Some(Box::new(Expr::Literal(
                Datum::Utf8("$".to_string()),
                PgType::TEXT,
            ))),
            case_insensitive: false,
            negated: false,
        };
        assert!(matches!(eval(&expr, &batch), Err(ExecError::Internal(_))));
    }

    #[test]
    fn is_null_and_is_not_null_never_themselves_return_null() {
        let batch = batch_i32("x", vec![None, Some(1)]);
        let is_null = eval(
            &Expr::IsNull {
                arg: Box::new(col(0, "x")),
                negated: false,
            },
            &batch,
        )
        .unwrap();
        let arr = bool_array(&is_null);
        assert!(arr.value(0));
        assert!(!(arr.value(1)));
        assert_eq!(arr.null_count(), 0);
    }

    #[test]
    fn cast_converts_the_physical_arrow_type() {
        let batch = batch_i32("x", vec![Some(5)]);
        let expr = Expr::Cast {
            arg: Box::new(col(0, "x")),
            to: PgType::INT8,
            kind: basin_pgtype::cast::CastKind::Implicit,
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(result.data_type(), &DataType::Int64);
        let arr = result.as_any().downcast_ref::<I64>().unwrap();
        assert_eq!(arr.value(0), 5);
    }

    #[test]
    fn aggregate_window_set_returning_and_subquery_are_rejected_as_operator_level() {
        let batch = batch_i32("x", vec![Some(1)]);
        for expr in [
            Expr::Aggregate {
                func: FuncId(Oid(2108)),
                args: vec![col(0, "x")],
                distinct: false,
                filter: None,
                order_by: vec![],
            },
            Expr::Window {
                func: FuncId(Oid(3100)),
                args: vec![col(0, "x")],
                partition_by: vec![],
                order_by: vec![],
                frame: basin_plan::WindowFrame {
                    units: basin_plan::FrameUnits::Rows,
                    start: basin_plan::FrameBound::UnboundedPreceding,
                    end: basin_plan::FrameBound::CurrentRow,
                },
            },
            Expr::SetReturning {
                func: FuncId(Oid(1066)),
                args: vec![lit_i32(1), lit_i32(10)],
            },
            Expr::Subquery {
                kind: SubqueryKind::Exists,
                subplan: Box::new(basin_plan::LogicalPlan::Empty {
                    produce_one_row: true,
                    schema: vec![],
                }),
                operand: None,
            },
        ] {
            let err = eval(&expr, &batch).unwrap_err();
            assert!(
                matches!(err, ExecError::Internal(_)),
                "{expr:?} must be rejected as operator-level, got {err:?}"
            );
        }
    }

    #[test]
    fn catalog_op_name_does_not_resolve_the_local_and_or_sentinels() {
        // AND_OP / OR_OP must never collide with a real pg_operator oid —
        // pinning this catches a future OPERATORS table edit that happened
        // to add a row at u32::MAX or u32::MAX - 1.
        assert_eq!(catalog_op_name(AND_OP), None);
        assert_eq!(catalog_op_name(OR_OP), None);
        assert_ne!(AND_OP, OR_OP);
    }

    #[test]
    fn oid_out_of_range_gives_an_internal_error_not_a_panic() {
        let batch = batch_i32("x", vec![Some(1)]);
        let expr = Expr::Binary {
            op: op(999_999), // not a real operator oid
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(1)),
        };
        assert!(matches!(eval(&expr, &batch), Err(ExecError::Internal(_))));
    }
}
