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
//!    (never NULL, two NULLs are NOT DISTINCT) — the semantics need no extra
//!    work beyond picking them over `eq`/`neq`. The *operands* still do:
//!    `distinct`/`not_distinct` reject a mismatched-type pair exactly like
//!    `eq` does, so [`eval_distinct_from`] runs its operands through the same
//!    [`eval_operand_pair`] widening/untyped-literal resolution `eval_binary`
//!    uses for `=`/`<`/etc., or `bigint_col IS DISTINCT FROM 4` would fall
//!    back the same way an unwidened `>` would.
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
//! - `Expr::Parameter`, `ArrayLit`, `RowLit`, `Subscript` and `FieldSelect`
//!   are simply not built yet; they fall through to a single catch-all
//!   `Internal` arm at the bottom of [`eval`].
//! - `AND` / `OR` / `NOT` have no `pg_operator` row — Postgres parses them as
//!   a `BoolExpr`, not an `OpExpr` — and `Expr` has no dedicated variant for
//!   them yet either (see the same gap noted in
//!   `basin_plan::opt::pushdown`'s module docs). This file recognizes them
//!   through the same kind of private sentinel `OpId` that
//!   `opt::pushdown::AND_OP` already uses for exactly this reason, reusing
//!   its exact value for `AND` so the two files agree by construction rather
//!   than by coincidence. `NOT` gets its own local sentinel, [`NOT_OP`], the
//!   same way [`OR_OP`] does — nothing outside this file needs it yet. If
//!   `Expr` grows real `And`/`Or`/`Not` variants, only [`AND_OP`], [`OR_OP`]
//!   and [`NOT_OP`] need to change.
//! 7. **Scalar functions.** [`Expr::ScalarFn`] dispatches on a `FuncId` — a
//!    `pg_proc` OID — to one of the common Postgres scalar functions, in
//!    [`eval_scalar_fn`]. Every OID in that dispatch table was read from a
//!    live PostgreSQL 18 `pg_proc`, the same discipline `basin_pgtype::cast`
//!    and `basin_pgtype::operator` already use, not recalled from memory.
//!    Several of Postgres's function semantics diverge from what arrow's
//!    (sparse) string/math kernels provide by default, or from a naive
//!    reading of the function name, and are called out at each function's
//!    definition below rather than repeated here:
//!    - `substr`'s start is 1-based and clamps rather than errors when it is
//!      less than 1; a negative *length*, by contrast, is a hard error.
//!    - `round` on `numeric` rounds half away from zero; on `float8` it
//!      matches the platform's `rint` (round half to even).
//!    - Every scalar function here returns NULL for a NULL input — none of
//!      them special-case NULL into a value.
//!    - `length` on text counts characters, not bytes — unlike arrow's own
//!      `length` kernel (see [`text_char_length`]), which is byte length and
//!      is deliberately not used here for that reason.
//!    - `concat` skips NULL arguments rather than propagating them (unlike
//!      `||`, which is an ordinary strict operator and yields NULL if either
//!      side is NULL).
//!    - `btrim`/`ltrim`/`rtrim` with no explicit character set trim only the
//!      ASCII space character, not Rust's notion of whitespace (tabs and
//!      newlines are left alone) — see [`trim_with`].
//!
//!    Arrow ships no kernel at all for lower/upper-casing, character-based
//!    length or substring, trimming, `replace`, or `strpos`, so those go
//!    through a single hand-written pass over the materialized array, the
//!    same category of exception as [`eval_bool_test`] above. Where arrow
//!    *does* have the right numeric primitive (`arrow_arith::arity`'s
//!    `unary`/`try_unary`/`binary`/`try_binary`), this file uses it — it is
//!    still the kernel layer, just the generic elementwise one rather than a
//!    named function, and it is what supplies the null-handling (including,
//!    importantly, never evaluating the closure against the garbage value
//!    behind a null slot, which matters for the decimal arithmetic below:
//!    `arity::unary` runs unconditionally and would risk an `i128` overflow
//!    panic on unmasked garbage, so decimal paths use the `try_` variants
//!    even though the closure itself cannot fail).
//!    An OID this table does not recognize is reported as `ExecError::Internal`
//!    naming the OID, precisely so the bridge above this crate can fall back
//!    to DataFusion for it instead of guessing.

use std::sync::Arc;

use arrow::compute::kernels::{
    arity, boolean, cast, cmp, comparison, concat_elements, numeric, zip,
};
use arrow_array::{
    new_null_array,
    types::{Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type},
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType};

use basin_pgtype::{physical, Oid, PgType};
use basin_plan::{BoolTest, ColumnRef, Datum as PlanDatum, Expr, FuncId, OpId};

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
/// `NOT`'s counterpart to [`AND_OP`]/[`OR_OP`]. `NOT` is a unary prefix
/// operator, which lives in a different `Expr` variant ([`Expr::Unary`], not
/// [`Expr::Binary`]) from `AND`/`OR`, so it cannot alias either of those even
/// though they share the same sentinel numbering scheme.
const NOT_OP: OpId = OpId(Oid(u32::MAX - 2));

// ─── Scalar function OIDs ───────────────────────────────────────────────────
//
// Every value below is a real `pg_proc.oid`, read from a live PostgreSQL 18
// with:
//
// ```sql
// SELECT oid, proname, pg_get_function_identity_arguments(oid)
//   FROM pg_proc
//  WHERE proname IN ('lower','upper','length','substr','abs','round','ceil',
//                     'floor','coalesce','concat','trim','ltrim','rtrim',
//                     'replace','strpos','left','right')
//  ORDER BY proname, oid;
// ```
//
// Postgres gives every distinct-argument-type overload of a function its own
// `pg_proc` row and OID (`substr(text,int)` and `substr(text,int,int)` are
// different OIDs, not one function with a default argument), so each row here
// is one specific overload, not a function name. `coalesce` has no `pg_proc`
// row at all — it is SQL grammar, not a function call — which is exactly why
// `Expr::Coalesce` is its own IR node ([`eval_coalesce`]) rather than routing
// through here.
const OID_LOWER: u32 = 870; // lower(text)
const OID_UPPER: u32 = 871; // upper(text)
const OID_LENGTH_TEXT: u32 = 1317; // length(text)
const OID_SUBSTR_2: u32 = 883; // substr(text, int)
const OID_SUBSTR_3: u32 = 877; // substr(text, int, int)
const OID_LEFT: u32 = 3060; // left(text, int)
const OID_RIGHT: u32 = 3061; // right(text, int)
const OID_ABS_INT2: u32 = 1398; // abs(smallint)
const OID_ABS_INT4: u32 = 1397; // abs(integer)
const OID_ABS_INT8: u32 = 1396; // abs(bigint)
const OID_ABS_FLOAT4: u32 = 1394; // abs(real)
const OID_ABS_FLOAT8: u32 = 1395; // abs(double precision)
const OID_ABS_NUMERIC: u32 = 1705; // abs(numeric)
const OID_ROUND_FLOAT8: u32 = 1342; // round(double precision)
const OID_ROUND_NUMERIC: u32 = 1708; // round(numeric)
const OID_ROUND_NUMERIC_N: u32 = 1707; // round(numeric, int)
const OID_CEIL_NUMERIC: u32 = 1711; // ceil(numeric)
const OID_CEIL_FLOAT8: u32 = 2308; // ceil(double precision)
const OID_FLOOR_NUMERIC: u32 = 1712; // floor(numeric)
const OID_FLOOR_FLOAT8: u32 = 2309; // floor(double precision)
const OID_CONCAT: u32 = 3058; // concat(VARIADIC "any")
const OID_BTRIM_1: u32 = 885; // btrim(text) — trim(x)/trim(both from x)
const OID_BTRIM_2: u32 = 884; // btrim(text, text)
const OID_LTRIM_1: u32 = 881; // ltrim(text)
const OID_LTRIM_2: u32 = 875; // ltrim(text, text)
const OID_RTRIM_1: u32 = 882; // rtrim(text)
const OID_RTRIM_2: u32 = 876; // rtrim(text, text)
const OID_REPLACE: u32 = 2087; // replace(text, text, text)
const OID_STRPOS: u32 = 868; // strpos(text, text)

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
        Expr::ScalarFn { func, args } => eval_scalar_fn(*func, args, batch),
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
/// Is this an untyped literal — one lowering left as `unknown` because
/// Postgres resolves it from context rather than from the token?
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::Literal(_, ty) if ty.is_unknown())
}

/// Materialise an untyped literal at a type taken from its context.
///
/// This is Postgres's rule, applied where the information exists. A
/// non-literal, or a literal that already has a type, falls through to the
/// ordinary path — this never overrides a type the planner did establish.
fn eval_untyped_literal(
    e: &Expr,
    target: &arrow_schema::DataType,
    len: usize,
) -> Result<ArrayRef, ExecError> {
    let Expr::Literal(datum, _) = e else {
        return Err(ExecError::Internal(
            "eval_untyped_literal called on a non-literal — a caller bug".into(),
        ));
    };
    if matches!(datum, PlanDatum::Null) {
        return Ok(new_null_array(target, len));
    }
    // Build the literal as text — which is what an unquoted SQL literal is
    // before resolution — then cast into the target. That routes every
    // conversion through arrow's cast kernel rather than duplicating a parser
    // per type, and a value the target cannot represent errors rather than
    // silently becoming NULL.
    let text = match datum {
        PlanDatum::Utf8(v) => v.clone(),
        PlanDatum::Int16(v) => v.to_string(),
        PlanDatum::Int32(v) => v.to_string(),
        PlanDatum::Int64(v) => v.to_string(),
        PlanDatum::Float32(v) => v.to_string(),
        PlanDatum::Float64(v) => v.to_string(),
        PlanDatum::Bool(v) => v.to_string(),
        PlanDatum::Bytes(_) | PlanDatum::Null => {
            return Err(ExecError::TypeMismatch(
                "an untyped literal cannot be binary".into(),
            ))
        }
    };
    if *target == arrow_schema::DataType::Utf8 {
        return Ok(Arc::new(arrow_array::StringArray::from(vec![
            text.as_str();
            len
        ])));
    }
    let as_text: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![text.as_str(); len]));
    cast::cast(&as_text, target).map_err(|e| map_arrow(e, "resolving an untyped literal"))
}

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
    // NOT is a sentinel, exactly like AND_OP/OR_OP in eval_binary — it has no
    // pg_operator row, so it must be checked before catalog_op_name, which
    // would otherwise report it as an unknown oid. `boolean::not` already
    // does the right thing for NULL (NOT NULL is NULL, not TRUE): it copies
    // the null buffer across and only negates the underlying bits, never
    // manufacturing a value where there was none.
    if op == NOT_OP {
        let b = require_bool(&v)?;
        return Ok(Arc::new(boolean::not(b).map_err(|e| map_arrow(e, "NOT"))?));
    }
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

    let (l, r) = eval_operand_pair(lhs, rhs, batch)?;

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
        // `text || text` (oid 654). An ordinary strict operator: unlike
        // `concat()` (see [`eval_concat`]), `||` is NOT special about NULL —
        // it yields NULL if EITHER side is NULL, the same as `+` would for
        // numbers. `concat_elements_utf8`'s own doc example
        // (`["a","b"] + [None,"c"] = [None,"bc"]`) already unions the two
        // null buffers, which is exactly that strictness — no extra
        // NULL-handling needed here beyond picking this kernel over a
        // hand-rolled loop. Verified against a live PostgreSQL 18:
        // `SELECT 'a' || NULL || 'b'` is NULL, while
        // `SELECT concat('a', NULL, 'b')` is `'ab'`.
        "||" => {
            let l = downcast_array::<StringArray>(&l, "text")?;
            let r = downcast_array::<StringArray>(&r, "text")?;
            Ok(Arc::new(
                concat_elements::concat_elements_utf8(l, r).map_err(|e| map_arrow(e, "||"))?,
            ))
        }
        other => Err(ExecError::Internal(format!(
            "operator '{other}' (oid {}) is not implemented in eval yet",
            op.0.get()
        ))),
    }
}

/// Resolve `lhs`/`rhs` into a pair of arrays ready for a binary arrow kernel:
/// untyped literals materialised from whichever side does have a type, then
/// numeric widening applied. Shared by every binary node that hands its two
/// operands straight to an arrow comparison/arithmetic kernel — [`eval_binary`]
/// and [`eval_distinct_from`] today — rather than duplicated at each call
/// site, since both need exactly the same fix for exactly the same reason
/// (arrow's kernels demand identical types on both sides; Postgres does not).
fn eval_operand_pair(
    lhs: &Expr,
    rhs: &Expr,
    batch: &RecordBatch,
) -> Result<(ArrayRef, ArrayRef), ExecError> {
    // Postgres resolves an UNTYPED literal from the other operand: in
    // `SELECT 'x' = col`, the literal is `unknown` until the column types it.
    // Lowering marks such literals `PgType::UNKNOWN` (oid 705) faithfully, and
    // nothing resolved them, so `physical()` correctly refused and the query
    // fell back. Both types are known here, so this is where it costs nothing:
    // evaluate the typed side first, then materialise the literal at its type.
    let (l, r) = match (is_unknown_literal(lhs), is_unknown_literal(rhs)) {
        (true, false) => {
            let r = eval(rhs, batch)?;
            let l = eval_untyped_literal(lhs, r.data_type(), batch.num_rows())?;
            (l, r)
        }
        (false, true) => {
            let l = eval(lhs, batch)?;
            let r = eval_untyped_literal(rhs, l.data_type(), batch.num_rows())?;
            (l, r)
        }
        // Both untyped is `'a' = 'b'`, which Postgres resolves to text.
        (true, true) => (
            eval_untyped_literal(lhs, &arrow_schema::DataType::Utf8, batch.num_rows())?,
            eval_untyped_literal(rhs, &arrow_schema::DataType::Utf8, batch.num_rows())?,
        ),
        (false, false) => (eval(lhs, batch)?, eval(rhs, batch)?),
    };
    // Arrow's comparison and arithmetic kernels require both sides to have the
    // SAME type; Postgres does not. `bigint_col > 2` is ordinary SQL — the
    // literal is int4, the column int8, and Postgres widens implicitly. Without
    // this the kernel rejects the pair and the whole query falls back, which is
    // an enormous share of real statements.
    unify_numeric(l, r)
}

/// Widen a mismatched numeric pair to a common type, the way Postgres's
/// implicit coercions do before an operator is applied.
///
/// Only widening is performed, and only within the numeric family: int16 to
/// int32 to int64 to float32 to float64. That direction is always
/// value-preserving. NARROWING IS NOT DONE — Postgres treats those casts as
/// assignment-only rather than implicit precisely because they can lose value,
/// and silently narrowing here would turn a comparison into a wrong answer.
/// A pair this cannot unify is left alone so the kernel reports the mismatch.
fn unify_numeric(l: ArrayRef, r: ArrayRef) -> Result<(ArrayRef, ArrayRef), ExecError> {
    use arrow_schema::DataType as DT;
    /// Rank within the widening chain. `None` means "not part of it", which
    /// includes decimals — those carry precision and scale that a rank cannot
    /// express, so they are deliberately excluded rather than approximated.
    fn rank(dt: &DT) -> Option<u8> {
        Some(match dt {
            DT::Int8 | DT::Int16 => 1,
            DT::Int32 => 2,
            DT::Int64 => 3,
            DT::Float32 => 4,
            DT::Float64 => 5,
            _ => return None,
        })
    }
    let (lt, rt) = (l.data_type().clone(), r.data_type().clone());
    if lt == rt {
        return Ok((l, r));
    }
    let (Some(lr), Some(rr)) = (rank(&lt), rank(&rt)) else {
        return Ok((l, r));
    };
    let target = if lr >= rr { lt } else { rt };
    let l = cast::cast(&l, &target).map_err(|e| map_arrow(e, "implicit widening"))?;
    let r = cast::cast(&r, &target).map_err(|e| map_arrow(e, "implicit widening"))?;
    Ok((l, r))
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
/// DISTINCT — so the *semantics* are not a gap to close. The *shape* of the
/// operands is: like every other arrow comparison kernel, `distinct`/
/// `not_distinct` reject a pair of different types outright, so
/// `bigint_col IS DISTINCT FROM 4` (int4 literal against an int8 column) or
/// `col IS DISTINCT FROM 'x'` (an untyped literal) needs exactly the same
/// resolution `=`/`<`/etc. get in [`eval_binary`] — see [`eval_operand_pair`].
/// Skipping that here would silently make every such query fall back, the
/// same failure mode the module docs describe for plain comparisons.
fn eval_distinct_from(
    lhs: &Expr,
    rhs: &Expr,
    negated: bool,
    batch: &RecordBatch,
) -> Result<ArrayRef, ExecError> {
    let (l, r) = eval_operand_pair(lhs, rhs, batch)?;
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

/// Dispatch a scalar function call to its implementation by `pg_proc` oid.
/// See the module docs' point 7 for where these OIDs come from and the
/// Postgres semantics each implementation has to get right.
///
/// `a(i)` evaluates the `i`-th argument on demand rather than eagerly
/// evaluating every argument up front, so a call with the wrong arity fails
/// with a clear "too few arguments" `Internal` error instead of an out-of-
/// bounds panic.
fn eval_scalar_fn(func: FuncId, args: &[Expr], batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let oid = func.0.get();
    let a = |i: usize| -> Result<ArrayRef, ExecError> {
        let e = args.get(i).ok_or_else(|| {
            ExecError::Internal(format!(
                "scalar function oid {oid} called with only {} argument(s) — a planner bug, \
                 not user error",
                args.len()
            ))
        })?;
        eval(e, batch)
    };

    match oid {
        OID_LOWER => text_unary(&a(0)?, str::to_lowercase),
        OID_UPPER => text_unary(&a(0)?, str::to_uppercase),
        OID_LENGTH_TEXT => text_char_length(&a(0)?),

        OID_SUBSTR_2 => eval_substr(&a(0)?, &a(1)?, None),
        OID_SUBSTR_3 => {
            let text = a(0)?;
            let start = a(1)?;
            let len = a(2)?;
            eval_substr(&text, &start, Some(&len))
        }
        OID_LEFT => eval_left_right(&a(0)?, &a(1)?, true),
        OID_RIGHT => eval_left_right(&a(0)?, &a(1)?, false),

        OID_ABS_INT2 => abs_int16(&a(0)?),
        OID_ABS_INT4 => abs_int32(&a(0)?),
        OID_ABS_INT8 => abs_int64(&a(0)?),
        OID_ABS_FLOAT4 => abs_float32(&a(0)?),
        OID_ABS_FLOAT8 => abs_float64(&a(0)?),
        OID_ABS_NUMERIC => abs_decimal(&a(0)?),

        OID_ROUND_FLOAT8 => round_float8(&a(0)?),
        OID_ROUND_NUMERIC => decimal_round_fixed(&a(0)?, 0),
        OID_ROUND_NUMERIC_N => {
            let val = a(0)?;
            let ndigits = a(1)?;
            decimal_round_per_row(&val, &ndigits)
        }

        OID_CEIL_NUMERIC => decimal_ceil(&a(0)?),
        OID_CEIL_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::ceil,
        ))),
        OID_FLOOR_NUMERIC => decimal_floor(&a(0)?),
        OID_FLOOR_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::floor,
        ))),

        OID_CONCAT => eval_concat(args, batch),

        OID_BTRIM_1 => eval_trim_1(&a(0)?, TrimSide::Both),
        OID_BTRIM_2 => {
            let s = a(0)?;
            let set = a(1)?;
            eval_trim_2(&s, &set, TrimSide::Both)
        }
        OID_LTRIM_1 => eval_trim_1(&a(0)?, TrimSide::Left),
        OID_LTRIM_2 => {
            let s = a(0)?;
            let set = a(1)?;
            eval_trim_2(&s, &set, TrimSide::Left)
        }
        OID_RTRIM_1 => eval_trim_1(&a(0)?, TrimSide::Right),
        OID_RTRIM_2 => {
            let s = a(0)?;
            let set = a(1)?;
            eval_trim_2(&s, &set, TrimSide::Right)
        }

        OID_REPLACE => {
            let s = a(0)?;
            let from = a(1)?;
            let to = a(2)?;
            eval_replace(&s, &from, &to)
        }
        OID_STRPOS => {
            let s = a(0)?;
            let needle = a(1)?;
            eval_strpos(&s, &needle)
        }

        other => Err(ExecError::Internal(format!(
            "scalar function oid {other} is not implemented in eval yet — the bridge should \
             fall back to DataFusion for it rather than guess"
        ))),
    }
}

/// Downcast an [`ArrayRef`] to a concrete arrow array type, or a
/// [`ExecError::TypeMismatch`] naming what was expected. `what` is a
/// human-readable label (e.g. `"text"`), not a Rust type name, since this is
/// surfaced to whatever reports the error.
fn downcast_array<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    what: &'static str,
) -> Result<&'a T, ExecError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ExecError::TypeMismatch(format!("expected {what}, found {:?}", array.data_type()))
    })
}

/// `lower(text)` / `upper(text)`, and the shared shape every other
/// single-argument text-to-text function below reuses. Arrow ships no
/// case-conversion kernel at all, so this is a hand-written pass over the
/// materialized array — the same category of exception [`eval_bool_test`]
/// already is, for the same reason (no kernel exists to call instead).
///
/// NULL in, NULL out: `v.map(&f)` only calls `f` for a `Some`, so a NULL
/// input never reaches `f` and is never turned into a value.
fn text_unary(arr: &ArrayRef, f: impl Fn(&str) -> String) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<StringArray>(arr, "text")?;
    let out: StringArray = a.iter().map(|v| v.map(&f)).collect();
    Ok(Arc::new(out))
}

/// `length(text)`. Deliberately not `arrow::compute::kernels::length::length`
/// — that kernel's own doc says "length is the number of *bytes*", which is
/// simply the wrong answer for Postgres's `length(text)` (character count).
/// `'héllo'` is 6 bytes (`é` is 2 bytes in UTF-8) but 5 characters; a caller
/// that used the byte-length kernel here would report 6.
fn text_char_length(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<StringArray>(arr, "text")?;
    let out: Int32Array = a
        .iter()
        .map(|v| v.map(|s| s.chars().count() as i32))
        .collect();
    Ok(Arc::new(out))
}

/// `substr(text, start)` / `substr(text, start, length)`. `length` is `None`
/// for the two-argument form, meaning "to the end of the string".
///
/// Both `start` and `length` are ordinary expressions in Postgres (they can
/// be columns, not just literals), so this cannot be a single arrow kernel
/// call the way `eval_cast` is: arrow's own `substring`/`substring_by_char`
/// kernels (see the module docs) take one scalar `start`/`length` applied to
/// every row, not a per-row array of them. This is therefore a hand-written
/// per-row pass, like [`text_unary`] above, built on [`pg_substr`] which
/// implements Postgres's exact 1-based, clamping-not-erroring-on-a-too-low
/// start semantics.
///
/// A negative `length` is the one input this function rejects outright
/// (`ERROR: negative substring length not allowed` on a live Postgres 18) —
/// checked for up front, over the whole array, the same way
/// [`reject_float_zero_divisor`] checks the divisor before dividing.
fn eval_substr(
    text: &ArrayRef,
    start: &ArrayRef,
    len: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let t = downcast_array::<StringArray>(text, "text")?;
    let s = downcast_array::<Int32Array>(start, "integer")?;
    let l = len
        .map(|l| downcast_array::<Int32Array>(l, "integer"))
        .transpose()?;

    if let Some(l) = l {
        if l.iter().flatten().any(|v| v < 0) {
            return Err(ExecError::TypeMismatch(
                "negative substring length not allowed".to_string(),
            ));
        }
    }

    let n = t.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        let row_len = l.map(|l| l.is_null(i)).unwrap_or(false);
        if t.is_null(i) || s.is_null(i) || row_len {
            out.push(None);
            continue;
        }
        let length = l.map(|l| l.value(i) as i64);
        out.push(Some(pg_substr(t.value(i), s.value(i) as i64, length)));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// Postgres's `substr(string, start [, length])`, on already-unwrapped
/// values. `start` is 1-based; a `start` below 1 is *clamped* to 1 rather
/// than erroring (`substr('hello', -3, 5)` is `'h'`, not an error and not
/// `'hello'`) — the characters "before" position 1 still count against
/// `length`, they are just not part of the output. Verified against a live
/// PostgreSQL 18:
///
/// ```text
/// substr('hello', -3, 5) = 'h'     -- end = start + length - 1 = 1
/// substr('hello',  0, 3) = 'he'    -- end = 0 + 3 - 1 = 2
/// substr('hello', 10, 3) = ''      -- clamped start (10) is past end (12)
/// substr('hello',  2)    = 'ello'  -- no length: clamped start to the end
/// ```
fn pg_substr(s: &str, start: i64, length: Option<i64>) -> String {
    let chars: Vec<char> = s.chars().collect();
    let char_count = chars.len() as i64;
    let clamped_start = start.max(1);

    let end = match length {
        None => {
            return if clamped_start > char_count {
                String::new()
            } else {
                chars[(clamped_start - 1) as usize..].iter().collect()
            };
        }
        Some(length) => start + length - 1, // 1-based, inclusive
    };

    let end = end.min(char_count);
    if end < clamped_start {
        return String::new();
    }
    chars[(clamped_start - 1) as usize..end as usize]
        .iter()
        .collect()
}

/// `left(text, n)` / `right(text, n)`. Both accept a negative `n`: `left`
/// with `n < 0` returns everything *except* the last `|n|` characters, and
/// `right` with `n < 0` returns everything except the first `|n|` — verified
/// against a live PostgreSQL 18 (`left('hello', -2) = 'hel'`,
/// `right('hello', -2) = 'llo'`).
fn eval_left_right(text: &ArrayRef, n: &ArrayRef, is_left: bool) -> Result<ArrayRef, ExecError> {
    let t = downcast_array::<StringArray>(text, "text")?;
    let n = downcast_array::<Int32Array>(n, "integer")?;
    let out: StringArray = t
        .iter()
        .zip(n.iter())
        .map(|(s, n)| match (s, n) {
            (Some(s), Some(n)) => Some(if is_left {
                pg_left(s, n)
            } else {
                pg_right(s, n)
            }),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

fn pg_left(s: &str, n: i32) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i32;
    let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
    chars[..take as usize].iter().collect()
}

fn pg_right(s: &str, n: i32) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i32;
    let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
    chars[(len - take) as usize..].iter().collect()
}

/// Which end(s) [`trim_with`] strips from.
#[derive(Clone, Copy)]
enum TrimSide {
    Both,
    Left,
    Right,
}

/// `btrim(text)` / `ltrim(text)` / `rtrim(text)` — the one-argument forms,
/// which trim only the ASCII space character. This is easy to get wrong:
/// Rust's `str::trim()` strips every Unicode whitespace character (tabs,
/// newlines, …), but Postgres's default trim set is *just* `' '` — verified
/// against a live PostgreSQL 18, where `btrim(E'\t hi \t')` leaves the tabs
/// untouched (`'\t hi \t'` comes back unchanged, because the outermost
/// characters are tabs, not spaces, so there is nothing to trim from either
/// end). Using `str::trim()` here would have silently eaten them.
fn eval_trim_1(arr: &ArrayRef, side: TrimSide) -> Result<ArrayRef, ExecError> {
    text_unary(arr, |s| trim_with(s, " ", side))
}

/// `btrim(text, text)` / `ltrim(text, text)` / `rtrim(text, text)` — the
/// two-argument forms, which trim any character *present in* the second
/// argument (not the second argument as a literal substring) from the given
/// side(s).
fn eval_trim_2(arr: &ArrayRef, set: &ArrayRef, side: TrimSide) -> Result<ArrayRef, ExecError> {
    let t = downcast_array::<StringArray>(arr, "text")?;
    let c = downcast_array::<StringArray>(set, "text")?;
    let out: StringArray = t
        .iter()
        .zip(c.iter())
        .map(|(s, set)| match (s, set) {
            (Some(s), Some(set)) => Some(trim_with(s, set, side)),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

fn trim_with(s: &str, set: &str, side: TrimSide) -> String {
    let is_trim_char = |c: char| set.contains(c);
    match side {
        TrimSide::Both => s.trim_matches(is_trim_char).to_string(),
        TrimSide::Left => s.trim_start_matches(is_trim_char).to_string(),
        TrimSide::Right => s.trim_end_matches(is_trim_char).to_string(),
    }
}

/// `replace(string, from, to)`: every occurrence of `from` in `string`
/// replaced with `to`. An ordinary strict function — no NULL-skipping
/// special case like `concat`'s.
fn eval_replace(s: &ArrayRef, from: &ArrayRef, to: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let from = downcast_array::<StringArray>(from, "text")?;
    let to = downcast_array::<StringArray>(to, "text")?;
    let out: StringArray = s
        .iter()
        .zip(from.iter())
        .zip(to.iter())
        .map(|((s, from), to)| match (s, from, to) {
            (Some(s), Some(from), Some(to)) => Some(s.replace(from, to)),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

/// `strpos(string, substring)`: the 1-based *character* position of the
/// first occurrence of `substring` in `string`, or `0` if it does not occur.
/// Verified character-based (not byte-based) against a live PostgreSQL 18:
/// `strpos('héllo', 'llo') = 3` (the 2-byte `é` is one character), matching
/// `str::find`'s byte offset converted back to a character count via
/// `s[..byte_idx].chars().count()` rather than used directly.
fn eval_strpos(s: &ArrayRef, needle: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let needle = downcast_array::<StringArray>(needle, "text")?;
    let out: Int32Array = s
        .iter()
        .zip(needle.iter())
        .map(|(s, needle)| match (s, needle) {
            (Some(s), Some(needle)) => Some(pg_strpos(s, needle)),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

fn pg_strpos(s: &str, needle: &str) -> i32 {
    match s.find(needle) {
        Some(byte_idx) => s[..byte_idx].chars().count() as i32 + 1,
        None => 0,
    }
}

/// `concat(VARIADIC "any")`. Every argument is cast to text (arrow's `cast`
/// kernel handles the numeric/bool/etc. cases; text arguments pass through
/// unchanged) and NULL arguments are skipped rather than propagated — the
/// opposite of `||`, which is an ordinary strict operator. Verified against a
/// live PostgreSQL 18: `concat('a', NULL, 'b') = 'ab'`, while
/// `'a' || NULL || 'b'` is NULL. `concat(NULL, NULL)` is `''`, not NULL:
/// this function never itself returns NULL.
fn eval_concat(args: &[Expr], batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    let n = batch.num_rows();
    let mut cols: Vec<StringArray> = Vec::with_capacity(args.len());
    for arg in args {
        let v = eval(arg, batch)?;
        let v: ArrayRef = if v.data_type() == &DataType::Utf8 {
            v
        } else {
            Arc::new(cast::cast(&v, &DataType::Utf8).map_err(|e| map_arrow(e, "CONCAT"))?)
        };
        cols.push(downcast_array::<StringArray>(&v, "text (after CONCAT's cast to text)")?.clone());
    }
    let out: StringArray = (0..n)
        .map(|i| {
            let mut buf = String::new();
            for col in &cols {
                if col.is_valid(i) {
                    buf.push_str(col.value(i));
                }
            }
            Some(buf)
        })
        .collect();
    Ok(Arc::new(out))
}

/// `abs(smallint)`. `checked_abs` catches the one input with no
/// representable answer (`abs(i16::MIN)`, whose magnitude does not fit in an
/// `i16`) and turns it into [`ExecError::Overflow`] instead of wrapping to a
/// negative number — the integer-overflow discipline the module docs'
/// point 1 already establishes for `+`/`-`/`*`.
fn abs_int16(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int16Array>(arr, "smallint")?;
    let out = arity::try_unary::<Int16Type, _, Int16Type>(a, |v| {
        v.checked_abs()
            .ok_or_else(|| ArrowError::ArithmeticOverflow("smallint abs".to_string()))
    })
    .map_err(|e| map_arrow(e, "abs"))?;
    Ok(Arc::new(out))
}

/// `abs(integer)`. See [`abs_int16`].
fn abs_int32(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int32Array>(arr, "integer")?;
    let out = arity::try_unary::<Int32Type, _, Int32Type>(a, |v| {
        v.checked_abs()
            .ok_or_else(|| ArrowError::ArithmeticOverflow("integer abs".to_string()))
    })
    .map_err(|e| map_arrow(e, "abs"))?;
    Ok(Arc::new(out))
}

/// `abs(bigint)`. See [`abs_int16`].
fn abs_int64(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int64Array>(arr, "bigint")?;
    let out = arity::try_unary::<Int64Type, _, Int64Type>(a, |v| {
        v.checked_abs()
            .ok_or_else(|| ArrowError::ArithmeticOverflow("bigint abs".to_string()))
    })
    .map_err(|e| map_arrow(e, "abs"))?;
    Ok(Arc::new(out))
}

/// `abs(real)`. Infallible — every finite or non-finite `f32` has a
/// well-defined `.abs()` — so this uses the plain (not `try_`) `unary`
/// kernel, unlike the integer and decimal `abs` variants.
fn abs_float32(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float32Array>(arr, "real")?;
    Ok(Arc::new(arity::unary::<Float32Type, _, Float32Type>(
        a,
        f32::abs,
    )))
}

/// `abs(double precision)`. See [`abs_float32`].
fn abs_float64(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float64Array>(arr, "double precision")?;
    Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
        a,
        f64::abs,
    )))
}

/// `abs(numeric)`. Uses `try_unary` (not the infallible `unary`) even though
/// `checked_abs` on `i128` essentially never fails in practice, purely so the
/// closure is only evaluated for non-null slots — see the module docs' point
/// 7 on why `unary`'s "runs on garbage behind a null too" behavior is
/// something the decimal paths specifically avoid.
fn abs_decimal(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let out = arity::try_unary::<Decimal128Type, _, Decimal128Type>(a, |v| {
        v.checked_abs()
            .ok_or_else(|| ArrowError::ArithmeticOverflow("numeric abs".to_string()))
    })
    .map_err(|e| map_arrow(e, "abs"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "abs"))?;
    Ok(Arc::new(out))
}

/// `round(double precision)`. Postgres's `float8` `round` calls the C
/// library's `rint()`, which under the IEEE 754 default rounding mode is
/// round-half-to-even — *not* the away-from-zero rounding `f64::round()`
/// implements. Verified against a live PostgreSQL 18:
/// `round(2.5::float8) = 2`, `round(-2.5::float8) = -2`,
/// `round(0.5::float8) = 0` — all three are the "to even" answer, and
/// `f64::round()` would have given `3`, `-3` and `1` instead. Rust's
/// `f64::round_ties_even` (stable since 1.77) matches `rint` exactly.
fn round_float8(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float64Array>(arr, "double precision")?;
    Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
        a,
        f64::round_ties_even,
    )))
}

/// `round(numeric)` / `round(numeric, ndigits)` with a single, query-wide
/// `ndigits` (0 for the one-argument form). See [`decimal_round_value`] for
/// the actual rounding rule.
fn decimal_round_fixed(arr: &ArrayRef, ndigits: i32) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let scale_i32 = scale as i32;
    let out = arity::try_unary::<Decimal128Type, _, Decimal128Type>(a, |v| {
        Ok::<_, ArrowError>(decimal_round_value(v, scale_i32, ndigits))
    })
    .map_err(|e| map_arrow(e, "round"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "round"))?;
    Ok(Arc::new(out))
}

/// `round(numeric, ndigits)` where `ndigits` is itself a per-row expression
/// (a column, not necessarily a literal) — the general case
/// [`decimal_round_fixed`] is a convenience wrapper around.
fn decimal_round_per_row(arr: &ArrayRef, ndigits: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let n = downcast_array::<Int32Array>(ndigits, "integer")?;
    let (precision, scale) = (a.precision(), a.scale());
    let scale_i32 = scale as i32;
    let out =
        arity::try_binary::<&Decimal128Array, &Int32Array, _, Decimal128Type>(a, n, |v, nd| {
            Ok::<_, ArrowError>(decimal_round_value(v, scale_i32, nd))
        })
        .map_err(|e| map_arrow(e, "round"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "round"))?;
    Ok(Arc::new(out))
}

/// `round(numeric[, ndigits])`'s rounding rule: half away from zero.
/// Verified against a live PostgreSQL 18: `round(2.5::numeric) = 3` and
/// `round(-2.5::numeric) = -3` — the opposite tie-breaking direction from
/// `round(double precision)` (see [`round_float8`]), which is *why* this is
/// two separate functions rather than one shared implementation.
///
/// `m` is the `Decimal128` physical mantissa (the array's own storage, at
/// `scale` decimal places: the logical value is `m * 10^-scale`). This keeps
/// the *physical* scale of the output identical to the input — narrower than
/// real Postgres, which can widen or shrink the returned numeric's own
/// scale — but matches this crate's existing many-to-one physical/logical
/// split (see `basin_pgtype::physical`'s module docs) rather than requiring
/// `eval` to know a target `PgType` it is not given.
fn decimal_round_value(m: i128, scale: i32, ndigits: i32) -> i128 {
    let digits_to_drop = scale - ndigits;
    if digits_to_drop <= 0 {
        // Rounding to at least as many digits as are physically stored is a
        // no-op — there is nothing to drop.
        return m;
    }
    match pow10(digits_to_drop) {
        Some(divisor) => decimal_round_at(m, divisor),
        // More digits than Decimal128 can represent at all: rounding at that
        // magnitude zeroes the value out entirely.
        None => 0,
    }
}

/// Round `m` to the nearest multiple of `divisor`, ties away from zero.
fn decimal_round_at(m: i128, divisor: i128) -> i128 {
    let q = m / divisor;
    let r = m % divisor;
    if r == 0 {
        return q * divisor;
    }
    // Compare magnitudes via unsigned_abs so this cannot itself overflow
    // (2 * r can exceed i128::MAX for r near the boundary).
    if r.unsigned_abs() * 2 >= divisor.unsigned_abs() {
        if m >= 0 {
            (q + 1) * divisor
        } else {
            (q - 1) * divisor
        }
    } else {
        q * divisor
    }
}

/// `10^d`, or `None` if it does not fit in an `i128` (`d` beyond what
/// `Decimal128`'s 38-digit precision could ever need).
fn pow10(d: i32) -> Option<i128> {
    if d < 0 {
        return None;
    }
    10i128.checked_pow(d as u32)
}

/// `ceil(numeric)`: the smallest integer `>=` the value, at the array's own
/// physical scale — see [`decimal_round_value`]'s doc on why the output
/// keeps the input's scale rather than narrowing to an integer numeric.
/// Verified against a live PostgreSQL 18: `ceil(-4.1::numeric) = -4`.
fn decimal_ceil(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let divisor = pow10(scale as i32).unwrap_or(1);
    let out = arity::try_unary::<Decimal128Type, _, Decimal128Type>(a, |v| {
        let q = v / divisor;
        let r = v % divisor;
        Ok::<_, ArrowError>(if r > 0 {
            (q + 1) * divisor
        } else {
            q * divisor
        })
    })
    .map_err(|e| map_arrow(e, "ceil"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "ceil"))?;
    Ok(Arc::new(out))
}

/// `floor(numeric)`: the largest integer `<=` the value. See [`decimal_ceil`].
/// Verified against a live PostgreSQL 18: `floor(-4.1::numeric) = -5`.
fn decimal_floor(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let divisor = pow10(scale as i32).unwrap_or(1);
    let out = arity::try_unary::<Decimal128Type, _, Decimal128Type>(a, |v| {
        let q = v / divisor;
        let r = v % divisor;
        Ok::<_, ArrowError>(if r < 0 {
            (q - 1) * divisor
        } else {
            q * divisor
        })
    })
    .map_err(|e| map_arrow(e, "floor"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "floor"))?;
    Ok(Arc::new(out))
}

/// Look up an operator's `pg_operator.oprname` by oid. `None` for the three
/// `eval.rs`-local sentinels ([`AND_OP`], [`OR_OP`], [`NOT_OP`]) as well as
/// for any genuinely unknown oid — callers that care about the difference
/// check the sentinels themselves first.
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

    fn func(oid_val: u32) -> FuncId {
        FuncId(Oid(oid_val))
    }

    fn sf(oid_val: u32, args: Vec<Expr>) -> Expr {
        Expr::ScalarFn {
            func: func(oid_val),
            args,
        }
    }

    fn lit_text(s: &str) -> Expr {
        Expr::Literal(Datum::Utf8(s.to_string()), PgType::TEXT)
    }

    fn lit_text_null() -> Expr {
        Expr::Literal(Datum::Null, PgType::TEXT)
    }

    /// A single-row batch for scalar-function tests whose arguments are all
    /// literals — the batch's own shape does not matter, only its row count.
    fn one_row() -> RecordBatch {
        batch_i32("_", vec![Some(0)])
    }

    fn batch_str1(name: &str, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    fn batch_f64(values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    /// A one-column batch of `numeric(precision, scale)`, given as raw
    /// `Decimal128` mantissas (e.g. `-550` at scale 2 is `-5.50`). There is
    /// no `Datum` variant for decimal literals (see `basin_plan::Datum`), so
    /// decimal-function tests build the column directly rather than through
    /// `Expr::Literal`.
    fn decimal_batch(
        name: &str,
        values: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> RecordBatch {
        let arr = Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Decimal128(precision, scale),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn str_array(v: &ArrayRef) -> &StringArray {
        v.as_any().downcast_ref::<StringArray>().unwrap()
    }

    fn decimal_array(v: &ArrayRef) -> &Decimal128Array {
        v.as_any().downcast_ref::<Decimal128Array>().unwrap()
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

    /// The shape a real query produces: a bigint column against an int4
    /// literal, and an untyped literal — the same mismatch
    /// `a_bigint_column_compares_against_an_int4_literal` pins for `>`.
    /// `cmp::distinct` rejects mismatched types exactly like `cmp::eq` does,
    /// so `IS DISTINCT FROM` needs the same untyped-literal/widening
    /// treatment or it silently falls back on every such query.
    #[test]
    fn distinct_from_widens_a_bigint_column_against_an_int4_literal() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![
                Some(4i64),
                Some(5),
            ]))],
        )
        .unwrap();
        let expr = Expr::DistinctFrom {
            lhs: Box::new(col(0, "n")),
            rhs: Box::new(lit_i32(4)),
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.value(0), "4 IS DISTINCT FROM 4 must be FALSE");
        assert!(arr.value(1), "5 IS DISTINCT FROM 4 must be TRUE");
    }

    #[test]
    fn distinct_from_resolves_an_untyped_literal_from_the_column_side() {
        let batch = batch_str1("s", vec![Some("hi"), Some("bye")]);
        let expr = Expr::DistinctFrom {
            lhs: Box::new(col(0, "s")),
            rhs: Box::new(Expr::Literal(Datum::Utf8("hi".into()), PgType::UNKNOWN)),
            negated: false,
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!arr.value(0), "'hi' IS DISTINCT FROM 'hi' must be FALSE");
        assert!(arr.value(1), "'bye' IS DISTINCT FROM 'hi' must be TRUE");
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
        // AND_OP / OR_OP / NOT_OP must never collide with a real pg_operator
        // oid — pinning this catches a future OPERATORS table edit that
        // happened to add a row at u32::MAX, u32::MAX - 1 or u32::MAX - 2.
        assert_eq!(catalog_op_name(AND_OP), None);
        assert_eq!(catalog_op_name(OR_OP), None);
        assert_eq!(catalog_op_name(NOT_OP), None);
        assert_ne!(AND_OP, OR_OP);
        assert_ne!(AND_OP, NOT_OP);
        assert_ne!(OR_OP, NOT_OP);
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

    // ─── NOT ─────────────────────────────────────────────────────────────
    //
    // `NOT` has no `pg_operator` row (it is `BoolExpr`, not `OpExpr` — see the
    // module docs), so it is reached through the local `NOT_OP` sentinel in
    // `eval_unary` rather than `catalog_op_name`.

    #[test]
    fn not_negates_true_and_false() {
        let batch = batch_bool2(
            vec![Some(true), Some(false)],
            vec![Some(false), Some(false)],
        );
        let expr = Expr::Unary {
            op: NOT_OP,
            arg: Box::new(col(0, "a")),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(!(arr.value(0)), "NOT TRUE must be FALSE");
        assert!(arr.value(1), "NOT FALSE must be TRUE");
    }

    /// `boolean::not` copies the null buffer across rather than manufacturing
    /// a value — `NOT NULL` must stay NULL, not become TRUE (the wrong answer
    /// a naive `!value` over the unmasked bit could produce).
    #[test]
    fn not_of_null_is_null() {
        let batch = batch_bool2(vec![None], vec![Some(true)]);
        let expr = Expr::Unary {
            op: NOT_OP,
            arg: Box::new(col(0, "a")),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = bool_array(&result);
        assert!(arr.is_null(0), "NOT NULL must be NULL, not TRUE or FALSE");
    }

    // ─── Scalar functions: dispatch, and the fallback signal ──────────────

    #[test]
    fn scalar_fn_of_an_unknown_oid_is_internal_so_the_bridge_can_fall_back() {
        let expr = sf(999_999, vec![lit_text("x")]);
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(
            matches!(err, ExecError::Internal(_)),
            "an unrecognized function oid must be Internal, not a panic or a wrong \
             value, so the bridge above this crate can fall back to DataFusion instead \
             of guessing"
        );
    }

    /// Requirement 4: a scalar function applied to a NULL argument returns
    /// NULL — pinned on `lower`, representative of every function in
    /// [`eval_scalar_fn`] except `concat`, whose whole point (requirement 3)
    /// is that it is the one exception.
    #[test]
    fn scalar_function_of_null_returns_null() {
        let expr = sf(OID_LOWER, vec![lit_text_null()]);
        let result = eval(&expr, &one_row()).unwrap();
        let arr = str_array(&result);
        assert!(
            arr.is_null(0),
            "lower(NULL) must be NULL — a scalar function that special-cased NULL \
             into a value (e.g. an empty string) would be wrong for every function \
             here except concat"
        );
    }

    // ─── lower / upper ──────────────────────────────────────────────────

    #[test]
    fn lower_and_upper_change_case() {
        let lower = eval(&sf(OID_LOWER, vec![lit_text("HeLLo")]), &one_row()).unwrap();
        assert_eq!(str_array(&lower).value(0), "hello");
        let upper = eval(&sf(OID_UPPER, vec![lit_text("HeLLo")]), &one_row()).unwrap();
        assert_eq!(str_array(&upper).value(0), "HELLO");
    }

    /// `text_unary` (shared by `lower`/`upper`/the one-argument trims) must
    /// operate row-by-row over a real multi-row column, not just a
    /// single-literal broadcast — and a NULL row must stay NULL rather than
    /// becoming, say, an empty string.
    #[test]
    fn lower_operates_row_by_row_over_a_column_and_preserves_nulls() {
        let batch = batch_str1("s", vec![Some("AB"), None, Some("Cd")]);
        let result = eval(&sf(OID_LOWER, vec![col(0, "s")]), &batch).unwrap();
        let arr = str_array(&result);
        assert_eq!(arr.value(0), "ab");
        assert!(arr.is_null(1), "a NULL row must stay NULL, not become \"\"");
        assert_eq!(arr.value(2), "cd");
    }

    // ─── length: requirement 2 ──────────────────────────────────────────

    /// Requirement 2: `length(text)` counts characters, not bytes.
    /// `'héllo'` is 6 bytes (`é` is a 2-byte UTF-8 sequence) but 5 characters
    /// — verified live against PostgreSQL 18 (`length('héllo') = 5`,
    /// `octet_length('héllo') = 6`). Using arrow's own byte-length `length`
    /// kernel here would have wrongly reported 6.
    #[test]
    fn length_counts_characters_not_bytes() {
        let expr = sf(OID_LENGTH_TEXT, vec![lit_text("héllo")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            i32_array(&result).value(0),
            5,
            "length('héllo') must be 5 (characters); 6 would be the byte count \
             ('é' is 2 bytes in UTF-8), the wrong answer arrow's byte-length \
             kernel would give"
        );
    }

    // ─── substr: requirement 1 ──────────────────────────────────────────

    /// Requirement 1: `substr`'s `start` is 1-based, and a `start` below 1
    /// clamps rather than erroring. Verified live against PostgreSQL 18:
    /// `substr('hello', -3, 5) = 'h'` — NOT an error, and NOT `'hello'`
    /// (which is what treating a negative start as "count from the end,
    /// clamped to 0" would wrongly produce).
    #[test]
    fn substr_clamps_a_too_low_start_instead_of_erroring() {
        let expr = sf(
            OID_SUBSTR_3,
            vec![lit_text("hello"), lit_i32(-3), lit_i32(5)],
        );
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            str_array(&result).value(0),
            "h",
            "substr('hello', -3, 5) must clamp to 'h', not error and not return \
             'hello'"
        );
    }

    /// The two-argument form (`substr(text, start)`, no explicit length)
    /// clamps the same way and reads to the end of the string. Verified live:
    /// `substr('hello', 2) = 'ello'`.
    #[test]
    fn substr_two_arg_form_reads_to_the_end() {
        let expr = sf(OID_SUBSTR_2, vec![lit_text("hello"), lit_i32(2)]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "ello");
    }

    /// `start = 0` is also below 1 and clamps the same way. Verified live:
    /// `substr('hello', 0, 3) = 'he'` — the characters "before" position 1
    /// still count against `length`, they are just not part of the output.
    #[test]
    fn substr_zero_start_clamps_and_still_consumes_length() {
        let expr = sf(
            OID_SUBSTR_3,
            vec![lit_text("hello"), lit_i32(0), lit_i32(3)],
        );
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "he");
    }

    #[test]
    fn substr_negative_length_is_a_hard_error() {
        let expr = sf(
            OID_SUBSTR_3,
            vec![lit_text("hello"), lit_i32(1), lit_i32(-1)],
        );
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(
            matches!(err, ExecError::TypeMismatch(_)),
            "a negative length must error (unlike a too-low start, which clamps), \
             got {err:?}"
        );
    }

    // ─── abs ──────────────────────────────────────────────────────────────

    #[test]
    fn abs_negates_negative_integers() {
        let expr = sf(OID_ABS_INT4, vec![lit_i32(-5)]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(i32_array(&result).value(0), 5);
    }

    /// `abs(i32::MIN)` has no representable positive counterpart in `i32` —
    /// this must error (`ExecError::Overflow`), not silently wrap back to
    /// `i32::MIN` the way an unchecked `.wrapping_abs()` would.
    #[test]
    fn abs_int_min_overflows_rather_than_wrapping() {
        let expr = sf(OID_ABS_INT4, vec![lit_i32(i32::MIN)]);
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(matches!(err, ExecError::Overflow(_)));
    }

    #[test]
    fn abs_float_handles_negative_values() {
        let batch = batch_f64(vec![Some(-3.5)]);
        let expr = sf(OID_ABS_FLOAT8, vec![col(0, "x")]);
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.5
        );
    }

    #[test]
    fn abs_numeric_preserves_precision_and_scale() {
        let batch = decimal_batch("x", vec![Some(-550)], 10, 2); // -5.50
        let expr = sf(OID_ABS_NUMERIC, vec![col(0, "x")]);
        let result = eval(&expr, &batch).unwrap();
        let arr = decimal_array(&result);
        assert_eq!(arr.value(0), 550);
        assert_eq!((arr.precision(), arr.scale()), (10, 2));
    }

    // ─── round: requirement 5 ───────────────────────────────────────────

    /// Requirement 5: `round(numeric)` rounds half away from zero. Verified
    /// live against PostgreSQL 18: `round(2.5::numeric) = 3`,
    /// `round(-2.5::numeric) = -3`. This is the OPPOSITE tie-breaking
    /// direction from `round(double precision)`, which rounds half to even
    /// (`round(2.5::float8) = 2`) — conflating the two is the exact mistake
    /// requirement 5 exists to catch.
    #[test]
    fn round_numeric_rounds_half_away_from_zero() {
        let positive = decimal_batch("x", vec![Some(25)], 3, 1); // 2.5
        let result = eval(&sf(OID_ROUND_NUMERIC, vec![col(0, "x")]), &positive).unwrap();
        assert_eq!(
            decimal_array(&result).value(0),
            30, // 3.0 at scale 1
            "round(2.5::numeric) must be 3, not 2 (round-half-to-even would give 2)"
        );

        let negative = decimal_batch("x", vec![Some(-25)], 3, 1); // -2.5
        let result = eval(&sf(OID_ROUND_NUMERIC, vec![col(0, "x")]), &negative).unwrap();
        assert_eq!(
            decimal_array(&result).value(0),
            -30, // -3.0 at scale 1
            "round(-2.5::numeric) must be -3, away from zero in the negative direction too"
        );
    }

    /// The float8 contrast requirement 5 warns against conflating with the
    /// numeric case above: `round(double precision)` rounds half to even.
    /// Verified live: `round(2.5::float8) = 2`, `round(-2.5::float8) = -2`.
    #[test]
    fn round_float8_rounds_half_to_even_unlike_numeric() {
        let batch = batch_f64(vec![Some(2.5), Some(-2.5), Some(0.5)]);
        let result = eval(&sf(OID_ROUND_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            arr.value(0),
            2.0,
            "round(2.5::float8) must be 2 (half to even)"
        );
        assert_eq!(arr.value(1), -2.0, "round(-2.5::float8) must be -2");
        assert_eq!(arr.value(2), 0.0, "round(0.5::float8) must be 0");
    }

    #[test]
    fn round_numeric_with_explicit_ndigits() {
        let batch = decimal_batch("x", vec![Some(12345)], 6, 3); // 12.345
        let ndigits = batch_i32("n", vec![Some(1)]);
        let combined_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Decimal128(6, 3), true),
            Field::new("n", DataType::Int32, true),
        ]));
        let combined = RecordBatch::try_new(
            combined_schema,
            vec![batch.column(0).clone(), ndigits.column(0).clone()],
        )
        .unwrap();
        let expr = sf(OID_ROUND_NUMERIC_N, vec![col(0, "x"), col(1, "n")]);
        let result = eval(&expr, &combined).unwrap();
        assert_eq!(decimal_array(&result).value(0), 12300); // 12.300 at scale 3
    }

    // ─── ceil / floor ───────────────────────────────────────────────────

    /// Verified live against PostgreSQL 18: `ceil(-4.1::numeric) = -4`.
    #[test]
    fn ceil_numeric_rounds_toward_positive_infinity() {
        let batch = decimal_batch("x", vec![Some(-41)], 3, 1); // -4.1
        let result = eval(&sf(OID_CEIL_NUMERIC, vec![col(0, "x")]), &batch).unwrap();
        assert_eq!(decimal_array(&result).value(0), -40); // -4.0
    }

    /// Verified live against PostgreSQL 18: `floor(-4.1::numeric) = -5`.
    #[test]
    fn floor_numeric_rounds_toward_negative_infinity() {
        let batch = decimal_batch("x", vec![Some(-41)], 3, 1); // -4.1
        let result = eval(&sf(OID_FLOOR_NUMERIC, vec![col(0, "x")]), &batch).unwrap();
        assert_eq!(decimal_array(&result).value(0), -50); // -5.0
    }

    #[test]
    fn ceil_and_floor_float8() {
        let batch = batch_f64(vec![Some(4.1)]);
        let c = eval(&sf(OID_CEIL_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let f = eval(&sf(OID_FLOOR_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        assert_eq!(
            c.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
            5.0
        );
        assert_eq!(
            f.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
            4.0
        );
    }

    // ─── concat: requirement 3 ──────────────────────────────────────────

    /// Requirement 3: `concat` IGNORES NULL arguments — unlike `||`, which
    /// is an ordinary strict operator and yields NULL if either side is
    /// NULL. Verified live against PostgreSQL 18: `concat('a', NULL, 'b') =
    /// 'ab'`, while `('a' || NULL || 'b') IS NULL` is true. Easy to
    /// conflate; this test pins `concat`'s side specifically.
    #[test]
    fn concat_skips_null_arguments_instead_of_propagating() {
        let expr = sf(
            OID_CONCAT,
            vec![lit_text("a"), lit_text_null(), lit_text("b")],
        );
        let result = eval(&expr, &one_row()).unwrap();
        let arr = str_array(&result);
        assert!(!arr.is_null(0), "concat must never itself return NULL");
        assert_eq!(
            arr.value(0),
            "ab",
            "concat('a', NULL, 'b') must skip the NULL and produce 'ab' — \
             propagating it (the way '||' does) would be the wrong answer here"
        );
    }

    /// `concat(NULL, NULL)` is `''`, not NULL — pinned separately from the
    /// mixed case above because "skips nulls" and "never returns null even if
    /// EVERY argument is null" are two different claims.
    #[test]
    fn concat_of_all_null_arguments_is_empty_string_not_null() {
        let expr = sf(OID_CONCAT, vec![lit_text_null(), lit_text_null()]);
        let result = eval(&expr, &one_row()).unwrap();
        let arr = str_array(&result);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), "");
    }

    // ─── || (string concatenation, oid 654) ─────────────────────────────

    /// The contrast the module docs (and the `concat` tests above) already
    /// call out, pinned from the `||` side this time: `||` is an ordinary
    /// strict operator, so a NULL ANYWHERE in the chain makes the whole
    /// result NULL — the opposite of `concat`, which skips NULLs. Verified
    /// live against PostgreSQL 18: `SELECT 'a' || NULL || 'b'` is NULL.
    #[test]
    fn double_pipe_yields_null_if_either_operand_is_null_unlike_concat() {
        let expr = Expr::Binary {
            op: op(654), // text ||
            lhs: Box::new(lit_text("a")),
            rhs: Box::new(lit_text_null()),
        };
        let result = eval(&expr, &one_row()).unwrap();
        let arr = str_array(&result);
        assert!(
            arr.is_null(0),
            "'a' || NULL must be NULL — concat() is the one that skips nulls, \
             not ||"
        );
    }

    #[test]
    fn double_pipe_concatenates_non_null_operands() {
        let expr = Expr::Binary {
            op: op(654),
            lhs: Box::new(lit_text("foo")),
            rhs: Box::new(lit_text("bar")),
        };
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "foobar");
    }

    /// A shape a real query produces: an untyped literal on one side, e.g.
    /// `col || 'x'` where the planner left the literal as `unknown` because
    /// `||` resolves it from the column. Mirrors
    /// `an_untyped_literal_takes_its_type_from_the_other_operand` above but
    /// for `||` specifically, since the untyped-literal path in
    /// `eval_binary` runs generically for every operator, not just the
    /// comparison ones exercised elsewhere.
    #[test]
    fn double_pipe_resolves_an_untyped_literal_from_the_column_side() {
        let batch = batch_str1("s", vec![Some("hi"), None]);
        let expr = Expr::Binary {
            op: op(654),
            lhs: Box::new(col(0, "s")),
            rhs: Box::new(Expr::Literal(Datum::Utf8("!".into()), PgType::UNKNOWN)),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = str_array(&result);
        assert_eq!(arr.value(0), "hi!");
        assert!(
            arr.is_null(1),
            "a NULL column operand must still make the whole concatenation NULL"
        );
    }

    /// Column-vs-column, one NULL row among non-NULL ones — the shape an
    /// actual `a || b` over a table produces, as opposed to the
    /// all-literals cases above.
    #[test]
    fn double_pipe_over_columns_is_null_only_on_the_null_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
                Arc::new(StringArray::from(vec![Some("1"), None, Some("2")])),
            ],
        )
        .unwrap();
        let expr = Expr::Binary {
            op: op(654),
            lhs: Box::new(col(0, "a")),
            rhs: Box::new(col(1, "b")),
        };
        let result = eval(&expr, &batch).unwrap();
        let arr = str_array(&result);
        assert_eq!(arr.value(0), "x1");
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    // ─── trim / ltrim / rtrim ───────────────────────────────────────────

    /// The one-argument trim forms strip only the ASCII space character, not
    /// every Unicode whitespace character Rust's `str::trim()` would strip.
    /// Verified live against PostgreSQL 18: `btrim(E'\t hi \t')` comes back
    /// completely UNCHANGED — the outermost characters are tabs, not spaces,
    /// so there is nothing to trim from either end even though there are
    /// spaces just inside them. `str::trim()` would have eaten the tabs (and
    /// then the spaces behind them), which is exactly the wrong answer this
    /// pins against.
    #[test]
    fn btrim_one_arg_strips_only_ascii_space_not_tabs() {
        let expr = sf(OID_BTRIM_1, vec![lit_text("\t hi \t")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            str_array(&result).value(0),
            "\t hi \t",
            "btrim's default set is exactly ' ' — since the outermost characters \
             are tabs, not spaces, nothing at all should be trimmed"
        );
    }

    #[test]
    fn ltrim_and_rtrim_one_arg_strip_only_their_own_side() {
        let l = eval(&sf(OID_LTRIM_1, vec![lit_text("  hi  ")]), &one_row()).unwrap();
        assert_eq!(str_array(&l).value(0), "hi  ");
        let r = eval(&sf(OID_RTRIM_1, vec![lit_text("  hi  ")]), &one_row()).unwrap();
        assert_eq!(str_array(&r).value(0), "  hi");
    }

    /// The two-argument forms trim any character present in the second
    /// argument, treated as a character set, not as a literal substring to
    /// strip once from each end.
    #[test]
    fn btrim_two_arg_strips_any_character_in_the_given_set() {
        let expr = sf(OID_BTRIM_2, vec![lit_text("xxhixx"), lit_text("x")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "hi");
    }

    // ─── replace ────────────────────────────────────────────────────────

    #[test]
    fn replace_substitutes_every_occurrence() {
        let expr = sf(
            OID_REPLACE,
            vec![lit_text("ababab"), lit_text("ab"), lit_text("X")],
        );
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "XXX");
    }

    // ─── strpos ─────────────────────────────────────────────────────────

    /// Verified live against PostgreSQL 18: `strpos('héllo', 'llo') = 3` —
    /// the 2-byte `é` still counts as one character, so a byte-offset answer
    /// (which would be 4) is the wrong answer this test rules out.
    #[test]
    fn strpos_is_a_character_position_not_a_byte_offset() {
        let expr = sf(OID_STRPOS, vec![lit_text("héllo"), lit_text("llo")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            i32_array(&result).value(0),
            3,
            "strpos must count characters ('é' is one character, not the two \
             bytes it takes in UTF-8)"
        );
    }

    #[test]
    fn strpos_of_a_non_match_is_zero() {
        let expr = sf(OID_STRPOS, vec![lit_text("hello"), lit_text("xyz")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(i32_array(&result).value(0), 0);
    }
    /// Postgres widens implicitly before comparing; arrow's kernels demand
    /// identical types. `bigint_col > 2` is ordinary SQL — the literal is int4
    /// and the column int8 — and without widening the kernel rejects the pair,
    /// so the entire query falls back. This was found by a bridge test, not by
    /// a unit test, which is why it survived until the owned engine ran real
    /// queries.
    #[test]
    fn a_bigint_column_compares_against_an_int4_literal() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1i64, 5, 9]))],
        )
        .unwrap();
        let e = Expr::Binary {
            op: OpId(basin_pgtype::Oid(521)), // int4 >
            lhs: Box::new(Expr::Column(basin_plan::ColumnRef {
                relation: 0,
                index: 0,
                name: "n".into(),
            })),
            rhs: Box::new(Expr::Literal(
                basin_plan::Datum::Int32(4),
                basin_pgtype::PgType::INT4,
            )),
        };
        let out = eval(&e, &batch).unwrap();
        let b = out
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert_eq!(
            (b.value(0), b.value(1), b.value(2)),
            (false, true, true),
            "int8 vs int4 must widen, not error"
        );
    }

    /// Widening only. Postgres makes narrowing casts assignment-only rather
    /// than implicit precisely because they can lose value, so a float8 column
    /// compared to an int must widen the INT, never truncate the float — the
    /// latter would silently change which rows match.
    #[test]
    fn widening_goes_toward_the_wider_type_never_the_narrower() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Float64Array::from(vec![2.5f64]))],
        )
        .unwrap();
        let e = Expr::Binary {
            op: OpId(basin_pgtype::Oid(521)),
            lhs: Box::new(Expr::Column(basin_plan::ColumnRef {
                relation: 0,
                index: 0,
                name: "f".into(),
            })),
            rhs: Box::new(Expr::Literal(
                basin_plan::Datum::Int32(2),
                basin_pgtype::PgType::INT4,
            )),
        };
        let out = eval(&e, &batch).unwrap();
        let b = out
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(
            b.value(0),
            "2.5 > 2 is true; truncating 2.5 to 2 would make it false"
        );
    }
    /// Postgres resolves an untyped literal from its context: in
    /// `SELECT 'x' = col`, the literal is `unknown` until the column types it.
    /// Lowering marks these faithfully and nothing resolved them, so
    /// `physical()` refused and every such query fell back.
    #[test]
    fn an_untyped_literal_takes_its_type_from_the_other_operand() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1i64, 42]))],
        )
        .unwrap();
        // `n = '42'` — the literal is unknown, and the column makes it int8.
        let e = Expr::Binary {
            op: OpId(basin_pgtype::Oid(96)), // int4 =
            lhs: Box::new(Expr::Column(basin_plan::ColumnRef {
                relation: 0,
                index: 0,
                name: "n".into(),
            })),
            rhs: Box::new(Expr::Literal(
                basin_plan::Datum::Utf8("42".into()),
                basin_pgtype::PgType::UNKNOWN,
            )),
        };
        let out = eval(&e, &batch).unwrap();
        let b = out
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert_eq!((b.value(0), b.value(1)), (false, true));
    }

    /// An untyped NULL resolves to the other side's type and stays NULL rather
    /// than becoming a value.
    #[test]
    fn an_untyped_null_resolves_without_becoming_a_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![7i64]))],
        )
        .unwrap();
        let e = Expr::Binary {
            op: OpId(basin_pgtype::Oid(96)),
            lhs: Box::new(Expr::Column(basin_plan::ColumnRef {
                relation: 0,
                index: 0,
                name: "n".into(),
            })),
            rhs: Box::new(Expr::Literal(
                basin_plan::Datum::Null,
                basin_pgtype::PgType::UNKNOWN,
            )),
        };
        let out = eval(&e, &batch).unwrap();
        assert!(out.is_null(0), "n = NULL is NULL, never true");
    }
}
