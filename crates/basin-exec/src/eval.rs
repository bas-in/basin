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
//! 7. **CASE/COALESCE branch typing.** Postgres resolves a CASE's or
//!    COALESCE's result type once, across every branch together
//!    (`select_common_type`), not from whichever branch happens to be first
//!    or happens to run first. [`eval_branches_unified`] is the one place
//!    that unification happens: an `unknown`-typed branch (a bare
//!    string/NULL literal) takes whatever type the other branches settle
//!    on — falling back to `text` if every branch is `unknown`, Postgres's
//!    own fallback — and mismatched concrete numeric branches widen the same
//!    way [`unify_numeric`] widens a binary operator's two operands.
//!    `GREATEST`/`LEAST` (`basin_plan::lower::expr::lower_min_max_expr`)
//!    desugar to nested `CASE` at lowering time and ride this for free.
//!    [`eval_case`]'s own doc comment is honest about the one thing this
//!    file does NOT get right for CASE: no short-circuiting, so a branch
//!    that only Postgres's laziness protects from erroring (division by the
//!    guarded-against value, e.g.) can raise here where Postgres would not.
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

// ─── Math — trig/log/exp/power (see docs/migration/df-removal/19-expires-at-removal.md
// entry 1: these OIDs already existed in `basin_pgtype::func::FUNCS` as
// planner-resolution groundwork, unbacked here. Every OID below was read from
// the same live PostgreSQL 18 `pg_proc` that table's own module docs describe
// querying — not recalled from memory. Numeric-argument overloads that would
// need arbitrary-precision transcendental math (`sqrt`/`ln`/`log`/`exp`/
// `power` on `numeric`) are deliberately NOT in this list — see the "Math —
// numeric transcendental overloads" comment further down for why routing them
// through `f64` instead would be the exact silent-wrong-answer class of bug
// this file's own module docs warn against, and why leaving them unresolved
// (falling through to the `other =>` arm) is the honest choice instead.
const OID_SQRT_FLOAT8: u32 = 1344; // sqrt(double precision)
const OID_CBRT_FLOAT8: u32 = 1345; // cbrt(double precision)
const OID_POWER_FLOAT8: u32 = 1368; // power(double precision, double precision)
const OID_LN_FLOAT8: u32 = 1341; // ln(double precision)
const OID_LOG_FLOAT8: u32 = 1340; // log(double precision) — base 10, NOT natural log
const OID_EXP_FLOAT8: u32 = 1347; // exp(double precision)
const OID_TRUNC_FLOAT8: u32 = 1343; // trunc(double precision)
const OID_TRUNC_NUMERIC: u32 = 1710; // trunc(numeric)
const OID_TRUNC_NUMERIC_N: u32 = 1709; // trunc(numeric, int)
const OID_DEGREES_FLOAT8: u32 = 1608; // degrees(double precision)
const OID_RADIANS_FLOAT8: u32 = 1609; // radians(double precision)
const OID_PI: u32 = 1610; // pi() — niladic
const OID_SIGN_FLOAT8: u32 = 2310; // sign(double precision)
const OID_SIGN_NUMERIC: u32 = 1706; // sign(numeric)
const OID_CEILING_FLOAT8: u32 = 2320; // ceiling(double precision) — SQL-standard alias of ceil
const OID_CEILING_NUMERIC: u32 = 2167; // ceiling(numeric)
const OID_ACOS_FLOAT8: u32 = 1601; // acos(double precision)
const OID_ASIN_FLOAT8: u32 = 1600; // asin(double precision)
const OID_ATAN_FLOAT8: u32 = 1602; // atan(double precision)
const OID_ATAN2_FLOAT8: u32 = 1603; // atan2(double precision, double precision)
const OID_COS_FLOAT8: u32 = 1605; // cos(double precision)
const OID_SIN_FLOAT8: u32 = 1604; // sin(double precision)
const OID_TAN_FLOAT8: u32 = 1606; // tan(double precision)

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
    // A literal that reaches here still carrying `unknown` (oid 705) has no
    // sibling to take a type from — it is not one side of a comparison, not a
    // CASE branch, not an IN element. Postgres resolves a standalone unknown
    // literal to TEXT, and so does this.
    //
    // Without it, `physical()` refuses the pseudo-type and the whole statement
    // falls back with "pseudo-type 705 has no physical representation". That
    // was the single cause of BOTH `string_agg(name, ',')` — the delimiter is a
    // bare literal — and `SELECT * FROM (VALUES (1,'a'))`, where the string in
    // a VALUES row has nothing to resolve against either.
    //
    // The typed paths already handle their own cases: `eval_operand_pair` for
    // binary operands, `eval_branches_unified` for CASE and COALESCE, and
    // `eval_operand_against` for IN. This is the remaining floor for a literal
    // that reaches evaluation with no context at all — the same rule those
    // three already fall back to when every candidate is unknown.
    if ty.is_unknown() {
        return eval_untyped_literal(
            &Expr::Literal(datum.clone(), ty),
            &DataType::Utf8,
            len,
        );
    }
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

/// [`eval_operand_pair`] for a caller that already holds its left side as an
/// array — `IN`, whose left operand is evaluated once and then tested against
/// every list element, so re-deriving it per element would be wasteful as well
/// as wrong.
///
/// The resolution is the same one and must stay the same one. `x IN (1, 2)`
/// and `x = 1 OR x = 2` are the same query to Postgres; if only the second
/// spelling widened its literals, the first would fail on the identical data
/// for no reason a user could see.
fn eval_operand_against(
    lhs: ArrayRef,
    rhs: &Expr,
    batch: &RecordBatch,
) -> Result<(ArrayRef, ArrayRef), ExecError> {
    let r = if is_unknown_literal(rhs) {
        eval_untyped_literal(rhs, lhs.data_type(), batch.num_rows())?
    } else {
        eval(rhs, batch)?
    };
    unify_numeric(lhs, r)
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
    let (lt, rt) = (l.data_type().clone(), r.data_type().clone());
    if lt == rt {
        return Ok((l, r));
    }
    let Some(target) = wider_numeric_type(&lt, &rt) else {
        return Ok((l, r));
    };
    let l = cast::cast(&l, &target).map_err(|e| map_arrow(e, "implicit widening"))?;
    let r = cast::cast(&r, &target).map_err(|e| map_arrow(e, "implicit widening"))?;
    Ok((l, r))
}

/// Rank within the int16→int32→int64→float32→float64 widening chain
/// [`unify_numeric`]/[`wider_numeric_type`] widen along. `None` means "not
/// part of it", which includes decimals — those carry precision and scale
/// that a rank cannot express, so they are deliberately excluded rather than
/// approximated.
fn numeric_rank(dt: &DataType) -> Option<u8> {
    Some(match dt {
        DataType::Int8 | DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::Float32 => 4,
        DataType::Float64 => 5,
        _ => return None,
    })
}

/// The wider of two Arrow numeric types on [`numeric_rank`]'s ladder, or
/// `None` if either type isn't on it at all (including when they're already
/// equal — every caller already special-cases that before asking). Shared by
/// [`unify_numeric`] (a binary operator's two operands) and
/// [`eval_branches_unified`] (folded pairwise across however many
/// CASE/COALESCE branches there are), so the two callers' notion of "the
/// common numeric type" cannot drift apart.
fn wider_numeric_type(a: &DataType, b: &DataType) -> Option<DataType> {
    let (ar, br) = (numeric_rank(a)?, numeric_rank(b)?);
    Some(if ar >= br { a.clone() } else { b.clone() })
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
///
/// **Honest limitation: no short-circuiting.** Postgres evaluates a CASE's
/// branches lazily — `CASE WHEN x <> 0 THEN 1/x ELSE 0 END` never raises
/// `division_by_zero` for `x = 0`, because the `1/x` branch simply never
/// runs for that row. This function evaluates every `THEN`/`ELSE` branch
/// eagerly, over the *whole* batch, before `zip` ever looks at the
/// condition — `zip` is what makes the unmatched branches' *values*
/// invisible in the result, not what makes them not run. For a branch that
/// can error on some rows regardless of which arm "wins" (division,
/// `to_date` on a malformed string, ...), this means a CASE that is valid,
/// working Postgres SQL can raise here where Postgres would not. Fixing
/// this needs per-branch row masking through `eval` itself (evaluating a
/// branch only over the subset of rows its condition selects), which is a
/// larger change than this file's kernel-per-node shape supports today —
/// documented rather than silently wrong.
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

    // Every result branch (every THEN, plus ELSE if present) is materialized
    // at one shared Arrow type up front, rather than one at a time as the
    // fold below walks them — see `eval_branches_unified`'s doc for why a
    // per-branch approach gets the type wrong.
    let mut branch_exprs: Vec<&Expr> = whens.iter().map(|(_, then)| then).collect();
    if let Some(e) = else_.as_deref() {
        branch_exprs.push(e);
    }
    let mut branch_arrays = eval_branches_unified(&branch_exprs, batch)?;
    let mut acc: Option<ArrayRef> = if else_.is_some() {
        branch_arrays.pop()
    } else {
        None
    };
    // `branch_arrays` now holds exactly the THEN arrays, aligned index-for-
    // index with `whens` (ELSE, if any, was just popped off the end).
    let then_arrays = branch_arrays;

    for ((cond_expr, _), then_arr) in whens
        .iter()
        .zip(then_arrays)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
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

/// Materialize every CASE-WHEN-THEN/ELSE branch, or every COALESCE argument,
/// at one shared Arrow type before they are combined by `zip`/`is_not_null`.
///
/// Postgres resolves a CASE's or COALESCE's result type once, across every
/// branch together (`select_common_type`) — not from whichever branch a
/// naive left-to-right walk happens to evaluate first. Two things fall out
/// of doing the same here:
///
/// - An `unknown`-typed branch — a bare string/NULL literal Postgres itself
///   leaves untyped until context supplies one, see [`is_unknown_literal`] —
///   takes whatever type the *other* branches settle on, the same way
///   [`eval_operand_pair`] resolves an untyped literal against its sibling
///   operand for an ordinary binary operator. If EVERY branch is `unknown`
///   (`CASE WHEN true THEN 'a' ELSE 'b' END`, `COALESCE('a', 'b')`),
///   Postgres's own fallback is `text` (confirmed against a live PostgreSQL
///   18: `pg_typeof(CASE WHEN true THEN 'a' ELSE 'b' END)` is `text`) — not
///   `unknown` itself, which `basin_pgtype::physical` cannot represent at
///   all (that pseudo-type-705 error is exactly what reaching `eval` with an
///   un-resolved literal branch used to produce).
/// - Two branches with *different concrete* types (`CASE WHEN … THEN
///   int4val ELSE float8val END`) are widened the same way
///   [`unify_numeric`] widens a binary operator's two operands, folded
///   pairwise across every concretely-typed branch (via
///   [`wider_numeric_type`]) so branch *order* cannot silently narrow the
///   result — the widest branch wins regardless of whether it was written
///   first or last.
///
/// A mismatched *non-numeric* pair of concrete types (not a real query shape
/// valid SQL produces for a well-typed CASE/COALESCE) is left as the first
/// one seen; the `cast` kernel reporting that mismatch below is an honest
/// answer, not a guess.
fn eval_branches_unified(exprs: &[&Expr], batch: &RecordBatch) -> Result<Vec<ArrayRef>, ExecError> {
    let len = batch.num_rows();

    let mut typed: Vec<Option<ArrayRef>> = Vec::with_capacity(exprs.len());
    for e in exprs {
        typed.push(if is_unknown_literal(e) {
            None
        } else {
            Some(eval(e, batch)?)
        });
    }

    let mut target: Option<DataType> = None;
    for a in typed.iter().flatten() {
        target = Some(match target {
            None => a.data_type().clone(),
            Some(t) => wider_numeric_type(&t, a.data_type()).unwrap_or(t),
        });
    }
    let target = target.unwrap_or(DataType::Utf8);

    typed
        .into_iter()
        .zip(exprs.iter())
        .map(|(a, e)| match a {
            Some(a) if a.data_type() == &target => Ok(a),
            Some(a) => {
                cast::cast(&a, &target).map_err(|err| map_arrow(err, "CASE/COALESCE branch"))
            }
            None => eval_untyped_literal(e, &target, len),
        })
        .collect()
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
    if exprs.is_empty() {
        return Err(ExecError::Internal(
            "COALESCE with no arguments — a planner bug, not user error".to_string(),
        ));
    }
    // Every argument is materialized at one shared Arrow type up front — see
    // `eval_branches_unified`'s doc — rather than one at a time as the fold
    // below walks them, so e.g. `COALESCE(int4col, int8val)` and
    // `COALESCE(name, 'none')` (a column and an `unknown`-typed literal) both
    // widen/resolve correctly regardless of which argument is written first.
    let arg_refs: Vec<&Expr> = exprs.iter().collect();
    let arrays = eval_branches_unified(&arg_refs, batch)?;
    let (last, rest) = arrays
        .split_last()
        .expect("checked exprs non-empty above, and arrays has the same length");
    let mut acc = last.clone();
    for v in rest.iter().rev() {
        let mask = boolean::is_not_null(v).map_err(|e| map_arrow(e, "COALESCE"))?;
        acc = zip::zip(&mask, v, &acc).map_err(|e| map_arrow(e, "COALESCE"))?;
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
    let Some((first, rest)) = list.split_first() else {
        return Err(ExecError::Internal(
            "IN with an empty list — a planner bug, not user error (the SQL grammar requires \
             at least one element)"
                .to_string(),
        ));
    };

    // The left operand can itself be untyped — `'a' IN (col)` — in which case
    // the list types it, mirroring how a binary comparison takes its type from
    // the other side. The first typed element is the source; a list that is
    // untyped all the way down is text, as Postgres resolves it. This costs one
    // extra evaluation of a single element in a shape that is nearly always
    // literals, and only in the rare case where the left side is untyped.
    let x = if is_unknown_literal(arg) {
        let target = match list.iter().find(|e| !is_unknown_literal(e)) {
            Some(typed) => eval(typed, batch)?.data_type().clone(),
            None => arrow_schema::DataType::Utf8,
        };
        eval_untyped_literal(arg, &target, batch.num_rows())?
    } else {
        eval(arg, batch)?
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
    // Resolved per element rather than once for the list: `x IN (1, 'a')` is a
    // type error in Postgres, but `x IN (1, 2)` where x is bigint is not, and
    // each element widens against x independently.
    let (x, v) = eval_operand_against(Arc::clone(x), item, batch)?;
    if negated {
        cmp::neq(&x, &v)
    } else {
        cmp::eq(&x, &v)
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
    // A LIKE pattern is written as a bare literal essentially always, so it
    // arrives untyped and arrow's kernel — which demands both sides be the same
    // string type — refused it. `col LIKE 'a%'` is about as ordinary as SQL
    // gets, and it fell back on every single query.
    let (a, p) = eval_operand_pair(arg, pattern, batch)?;
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

        OID_SQRT_FLOAT8 => float8_unary_checked(&a(0)?, pg_sqrt_f64),
        OID_CBRT_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::cbrt,
        ))),
        OID_POWER_FLOAT8 => {
            let base = a(0)?;
            let exp = a(1)?;
            float8_binary_checked(&base, &exp, pg_power_f64)
        }
        OID_LN_FLOAT8 => float8_unary_checked(&a(0)?, pg_ln_f64),
        OID_LOG_FLOAT8 => float8_unary_checked(&a(0)?, pg_log10_f64),
        OID_EXP_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::exp,
        ))),
        OID_TRUNC_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::trunc,
        ))),
        OID_TRUNC_NUMERIC => decimal_trunc_fixed(&a(0)?, 0),
        OID_TRUNC_NUMERIC_N => {
            let val = a(0)?;
            let ndigits = a(1)?;
            decimal_trunc_per_row(&val, &ndigits)
        }
        OID_DEGREES_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::to_degrees,
        ))),
        OID_RADIANS_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::to_radians,
        ))),
        OID_PI => Ok(Arc::new(Float64Array::from(vec![
            std::f64::consts::PI;
            batch.num_rows()
        ]))),
        OID_SIGN_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            pg_sign_f64,
        ))),
        OID_SIGN_NUMERIC => decimal_sign(&a(0)?),
        OID_CEILING_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::ceil,
        ))),
        OID_CEILING_NUMERIC => decimal_ceil(&a(0)?),
        OID_ACOS_FLOAT8 => float8_unary_checked(&a(0)?, pg_acos_f64),
        OID_ASIN_FLOAT8 => float8_unary_checked(&a(0)?, pg_asin_f64),
        OID_ATAN_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::atan,
        ))),
        OID_ATAN2_FLOAT8 => {
            let y_arr = a(0)?;
            let x_arr = a(1)?;
            let y = downcast_array::<Float64Array>(&y_arr, "double precision")?;
            let x = downcast_array::<Float64Array>(&x_arr, "double precision")?;
            Ok(Arc::new(
                arity::binary::<_, _, _, Float64Type>(y, x, f64::atan2)
                    .map_err(|e| map_arrow(e, "atan2"))?,
            ))
        }
        OID_COS_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::cos,
        ))),
        OID_SIN_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::sin,
        ))),
        OID_TAN_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::tan,
        ))),

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
            // An empty `from` is the one case where Rust and Postgres part
            // ways. `str::replace` treats "" as matching at every character
            // boundary, so `"hello".replace("", "0")` yields `"0h0e0l0l0o0"`.
            // PostgreSQL 18 returns the subject unchanged:
            // `replace('hello world', '', '0') = 'hello world'` (verified
            // live). Nothing to find means nothing to replace.
            (Some(s), Some(from), Some(to)) if !from.is_empty() => Some(s.replace(from, to)),
            (Some(s), Some(_), Some(_)) => Some(s.to_string()),
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
    // `saturating_sub`, not `-`: `ndigits` is caller-supplied SQL, so
    // `round(n, -2147483648)` would overflow `i32` on the plain subtraction and
    // panic the whole query in a debug build. Saturating to `i32::MAX` lands on
    // the `pow10 -> None` arm below, which returns 0 — which is exactly what
    // PostgreSQL 18 answers for `round(x::numeric, -2147483648)`.
    let digits_to_drop = scale.saturating_sub(ndigits);
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

// ─── Math — trig/log/exp/power (float8) ────────────────────────────────────
//
// Every closure below is `f64 -> Result<f64, ExecError>` rather than the
// infallible `f64 -> f64` the simpler functions above use directly with
// `arity::unary` — `sqrt`/`ln`/`log`/`asin`/`acos` all have real domains
// narrower than "every finite f64", and Postgres ERRORS outside them rather
// than returning `NaN`/`-inf` the way the underlying libm call would. Using
// `try_unary` (via [`float8_unary_checked`]) is what turns that into a
// catchable [`ExecError`] instead of a silently wrong numeric answer reaching
// the client.

/// Apply a fallible `f64 -> f64` closure elementwise, NULL-in/NULL-out (the
/// null slot is never passed to `f`, same guarantee [`arity::try_unary`]
/// gives every other decimal path in this file — see the module docs' point
/// 7). Shared by every float8 math function below that has a real domain
/// restriction (`sqrt`, `ln`, `log`, `asin`, `acos`).
fn float8_unary_checked(
    arr: &ArrayRef,
    f: impl Fn(f64) -> Result<f64, ExecError>,
) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float64Array>(arr, "double precision")?;
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(f(a.value(i))?));
        }
    }
    Ok(Arc::new(Float64Array::from(out)))
}

/// [`float8_unary_checked`]'s two-argument counterpart, for `power(float8,
/// float8)` — the one float8 math function here whose domain restriction
/// depends on both arguments together (negative base, non-integer exponent),
/// not on one argument in isolation.
fn float8_binary_checked(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    f: impl Fn(f64, f64) -> Result<f64, ExecError>,
) -> Result<ArrayRef, ExecError> {
    let l = downcast_array::<Float64Array>(lhs, "double precision")?;
    let r = downcast_array::<Float64Array>(rhs, "double precision")?;
    let n = l.len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if l.is_null(i) || r.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(f(l.value(i), r.value(i))?));
        }
    }
    Ok(Arc::new(Float64Array::from(out)))
}

/// `sqrt(double precision)`. Postgres errors rather than returning `NaN` for
/// a negative input. Verified against a live PostgreSQL 18:
/// `SELECT sqrt(-1::float8)` raises `ERROR: 2201F: cannot take square root of
/// a negative number` (SQLSTATE 2201F, `invalid_argument_for_power_function`
/// — the same SQLSTATE Postgres uses for `power`'s domain error below, not a
/// dedicated "sqrt" code). `f64::sqrt` on a negative number silently produces
/// `NaN`, which is why this cannot be `arity::unary(f64::sqrt)`.
fn pg_sqrt_f64(x: f64) -> Result<f64, ExecError> {
    if x < 0.0 {
        return Err(ExecError::TypeMismatch(
            "cannot take square root of a negative number".to_string(),
        ));
    }
    Ok(x.sqrt())
}

/// `ln(double precision)`: natural log. Distinct from `log(double precision)`
/// ([`pg_log10_f64`]), which is base 10 — confirmed live that Postgres's
/// one-argument `log` is NOT natural log, a common point of confusion.
/// Verified live: `SELECT ln(0::float8)` raises `ERROR: 2201E: cannot take
/// logarithm of zero`; `SELECT ln(-1::float8)` raises `ERROR: 2201E: cannot
/// take logarithm of a negative number` (SQLSTATE 2201E,
/// `invalid_argument_for_logarithm`, both cases — same code, two message
/// shapes, matched exactly here since the shapes differ in real Postgres).
fn pg_ln_f64(x: f64) -> Result<f64, ExecError> {
    reject_nonpositive_log_argument(x)?;
    Ok(x.ln())
}

/// `log(double precision)`: base 10, one-argument form. There is no
/// `log(float8, float8)` two-argument overload in real Postgres — only
/// `numeric` has an explicit-base form (`basin_pgtype::func`'s module docs
/// confirm exactly three `log` `pg_proc` rows exist, not four) — so this is
/// the only `log` float8 entry point. Verified live: `SELECT log(0::float8)`
/// and `SELECT log(-1::float8)` raise the same two SQLSTATE-2201E shapes as
/// `ln` above.
fn pg_log10_f64(x: f64) -> Result<f64, ExecError> {
    reject_nonpositive_log_argument(x)?;
    Ok(x.log10())
}

/// Shared domain check for `ln`/`log`: zero and negative arguments are two
/// different Postgres error messages (both SQLSTATE 2201E), not one generic
/// "invalid argument" — matched exactly rather than collapsed into a single
/// wording.
fn reject_nonpositive_log_argument(x: f64) -> Result<(), ExecError> {
    if x == 0.0 {
        return Err(ExecError::TypeMismatch(
            "cannot take logarithm of zero".to_string(),
        ));
    }
    if x < 0.0 {
        return Err(ExecError::TypeMismatch(
            "cannot take logarithm of a negative number".to_string(),
        ));
    }
    Ok(())
}

/// `power(double precision, double precision)`. `power(0, 0) = 1` needs no
/// special case — confirmed live, and `0f64.powf(0.0) == 1.0` in Rust too
/// (IEEE 754's own rule, not a Postgres-specific one). The one real domain
/// restriction: a negative base raised to a non-integer exponent is a
/// complex number, and Postgres errors rather than returning `NaN` the way
/// `f64::powf` does on its own. Verified live: `SELECT power(-2::float8,
/// 0.5::float8)` raises `ERROR: 2201F: a negative number raised to a
/// non-integer power yields a complex result` (SQLSTATE 2201F, the same code
/// `sqrt`'s domain error uses). `power(-2, 2)` and `power(-2, 3)` (integer
/// exponents) are fine and must NOT hit this check — `exponent.fract() !=
/// 0.0` is exactly Postgres's own "is the exponent integral" test.
fn pg_power_f64(base: f64, exponent: f64) -> Result<f64, ExecError> {
    if base < 0.0 && exponent.fract() != 0.0 {
        return Err(ExecError::TypeMismatch(
            "a negative number raised to a non-integer power yields a complex result".to_string(),
        ));
    }
    Ok(base.powf(exponent))
}

/// `asin(double precision)`. Verified live: `SELECT asin(2::float8)` raises
/// `ERROR: 22003: input is out of range` — SQLSTATE 22003
/// (`numeric_value_out_of_range`), a genuinely different code from the
/// `ln`/`sqrt`/`power` domain errors above, not the same one reused.
/// `f64::asin` outside `[-1, 1]` silently returns `NaN`, which is why this
/// needs the checked path rather than `arity::unary(f64::asin)`.
fn pg_asin_f64(x: f64) -> Result<f64, ExecError> {
    reject_out_of_trig_domain(x)?;
    Ok(x.asin())
}

/// `acos(double precision)`. See [`pg_asin_f64`] — same domain `[-1, 1]`,
/// same SQLSTATE 22003 "input is out of range" on a live PostgreSQL 18.
fn pg_acos_f64(x: f64) -> Result<f64, ExecError> {
    reject_out_of_trig_domain(x)?;
    Ok(x.acos())
}

fn reject_out_of_trig_domain(x: f64) -> Result<(), ExecError> {
    if !(-1.0..=1.0).contains(&x) {
        return Err(ExecError::TypeMismatch("input is out of range".to_string()));
    }
    Ok(())
}

/// `sign(double precision)`. NOT `f64::signum` — Rust's `signum` returns
/// `1.0` for `+0.0` and `-1.0` for `-0.0` (it reports the sign bit, not
/// "is this positive"), where Postgres's `sign(0::float8)` is confirmed live
/// to be `0`, not `1` or `-1`.
fn pg_sign_f64(x: f64) -> f64 {
    if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        // Covers +0.0, -0.0, and NaN (comparisons against NaN are always
        // false, so both branches above fall through here) — NaN is not a
        // documented Postgres `sign` input and this file does not special
        // case it further.
        0.0
    }
}

/// `sign(numeric)`: `-1`, `0` or `1`, at the array's own physical scale —
/// same "output keeps the input's scale" convention [`decimal_round_value`]'s
/// doc explains for `round`/`ceil`/`floor`. Pure integer comparison, no
/// transcendental math needed, unlike `sqrt`/`ln`/`log`/`exp`/`power` on
/// `numeric` (see the "Math — numeric transcendental overloads" comment)
/// — which is exactly why this one IS implemented here. Verified live:
/// `sign(-5::numeric) = -1`.
fn decimal_sign(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let divisor = pow10(scale as i32).unwrap_or(1);
    let out = arity::unary::<Decimal128Type, _, Decimal128Type>(a, |v| match v.cmp(&0) {
        std::cmp::Ordering::Greater => divisor,
        std::cmp::Ordering::Less => -divisor,
        std::cmp::Ordering::Equal => 0,
    });
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "sign"))?;
    Ok(Arc::new(out))
}

/// `trunc(numeric)` / `trunc(numeric, ndigits)` with a single, query-wide
/// `ndigits` (0 for the one-argument form) — the fixed-`ndigits` counterpart
/// to [`decimal_round_fixed`], with truncation instead of rounding.
fn decimal_trunc_fixed(arr: &ArrayRef, ndigits: i32) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let (precision, scale) = (a.precision(), a.scale());
    let scale_i32 = scale as i32;
    let out = arity::try_unary::<Decimal128Type, _, Decimal128Type>(a, |v| {
        Ok::<_, ArrowError>(decimal_trunc_value(v, scale_i32, ndigits))
    })
    .map_err(|e| map_arrow(e, "trunc"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "trunc"))?;
    Ok(Arc::new(out))
}

/// `trunc(numeric, ndigits)` where `ndigits` is a per-row expression — the
/// general case [`decimal_trunc_fixed`] wraps, mirroring
/// [`decimal_round_per_row`].
fn decimal_trunc_per_row(arr: &ArrayRef, ndigits: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Decimal128Array>(arr, "numeric")?;
    let n = downcast_array::<Int32Array>(ndigits, "integer")?;
    let (precision, scale) = (a.precision(), a.scale());
    let scale_i32 = scale as i32;
    let out =
        arity::try_binary::<&Decimal128Array, &Int32Array, _, Decimal128Type>(a, n, |v, nd| {
            Ok::<_, ArrowError>(decimal_trunc_value(v, scale_i32, nd))
        })
        .map_err(|e| map_arrow(e, "trunc"))?;
    let out = out
        .with_precision_and_scale(precision, scale)
        .map_err(|e| map_arrow(e, "trunc"))?;
    Ok(Arc::new(out))
}

/// `trunc(numeric[, ndigits])`'s rule: truncate toward zero (unlike `round`,
/// no tie-breaking question exists here at all). `m` is the `Decimal128`
/// physical mantissa at `scale` decimal places, same representation
/// [`decimal_round_value`] documents. Integer division in Rust already
/// truncates toward zero for negative operands (unlike, e.g., Python's floor
/// division), which is exactly Postgres's `trunc` direction — verified live:
/// `trunc(-3.14159::numeric, 2) = -3.14` (not `-3.15`, which flooring toward
/// negative infinity would give), `trunc(12345::numeric, -2) = 12300`.
fn decimal_trunc_value(m: i128, scale: i32, ndigits: i32) -> i128 {
    // `saturating_sub` for the same reason `decimal_round_value` uses it: a
    // user-supplied `ndigits` of `i32::MIN` overflows the plain subtraction and
    // panics the query. Saturating reaches the `pow10 -> None` arm, i.e. 0,
    // matching PostgreSQL 18's `trunc(x::numeric, -2147483648)`.
    let digits_to_drop = scale.saturating_sub(ndigits);
    if digits_to_drop <= 0 {
        // Truncating to at least as many digits as are physically stored is
        // a no-op, same reasoning as decimal_round_value's early return.
        return m;
    }
    match pow10(digits_to_drop) {
        Some(divisor) => (m / divisor) * divisor,
        // More digits than Decimal128 can represent at all: truncating at
        // that magnitude zeroes the value out entirely.
        None => 0,
    }
}

// ─── Math — numeric transcendental overloads: deliberately NOT implemented ─
//
// `sqrt(numeric)` (1730), `ln(numeric)` (1734), `log(numeric)` (1741),
// `log(numeric, numeric)` (1736), `exp(numeric)` (1732) and
// `power(numeric, numeric)` (2169) are real `pg_proc` rows (see
// `basin_pgtype::func::FUNCS`) with NO arm in `eval_scalar_fn` — a call to
// any of them falls through to the `other =>` catch-all below and, today,
// falls back to DataFusion (see that arm's own comment).
//
// This is a deliberate omission, not an oversight: Postgres's `numeric`
// transcendental functions are computed with arbitrary-precision decimal
// arithmetic (`numeric.c`'s own `sqrt_var`/`ln_var`/`exp_var`), not IEEE 754
// `f64`. The float8 implementations directly above this comment (`pg_sqrt_f64`
// etc.) cannot be reused for the numeric overloads by just converting through
// `f64` and back to `Decimal128` — that would silently produce a numeric
// *shaped* answer with float *precision*, which is exactly the class of bug
// this file's own module docs (point 7's sibling functions) and
// docs/migration/df-removal/19-expires-at-removal.md warn against: "Silently
// computing a numeric result with float semantics is exactly the class of
// error this program keeps finding." `sign`/`trunc`/`ceiling` on `numeric`
// ARE implemented above because they need only integer comparison/division on
// the `Decimal128` mantissa, never a transcendental function — a genuinely
// different, exact computation, not a shortcut of this one. A real
// implementation needs its own arbitrary-precision decimal routines and is
// left as a named follow-up rather than a routed-through-f64 approximation.

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

    /// A bare string literal, `unknown`-typed exactly as lowering leaves it
    /// (`lower_a_const`'s `Val::Sval` arm) until something resolves it —
    /// unlike [`lit_text`], which is already concretely `text`.
    fn lit_text_unknown(s: &str) -> Expr {
        Expr::Literal(Datum::Utf8(s.to_string()), PgType::UNKNOWN)
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

    fn f64_array(v: &ArrayRef) -> &Float64Array {
        v.as_any().downcast_ref::<Float64Array>().unwrap()
    }

    fn lit_f64(v: f64) -> Expr {
        Expr::Literal(Datum::Float64(v), PgType::FLOAT8)
    }

    fn lit_f64_null() -> Expr {
        Expr::Literal(Datum::Null, PgType::FLOAT8)
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
    fn coalesce_of_all_nulls_is_null() {
        let batch = batch_i32("x", vec![None]);
        let expr = Expr::Coalesce(vec![col(0, "x"), Expr::Literal(Datum::Null, PgType::INT4)]);
        let result = eval(&expr, &batch).unwrap();
        assert!(result.is_null(0), "every argument was NULL");
    }

    /// `COALESCE(name, 'none')`: a column and an `unknown`-typed bare string
    /// literal — the exact shape that used to fail with "pseudo-type 705 has
    /// no physical representation" before `eval_branches_unified` existed,
    /// because `eval`ing an `unknown`-typed literal directly asks
    /// `basin_pgtype::physical` to represent a pseudo-type. The literal must
    /// resolve to the column's type (`text`), not error.
    #[test]
    fn coalesce_resolves_an_untyped_literal_against_a_typed_column() {
        let batch = batch_str1("name", vec![None, Some("a")]);
        let expr = Expr::Coalesce(vec![
            col(0, "name"),
            Expr::Literal(Datum::Utf8("none".into()), PgType::UNKNOWN),
        ]);
        let result = eval(&expr, &batch).unwrap();
        let arr = str_array(&result);
        assert_eq!(arr.value(0), "none");
        assert_eq!(arr.value(1), "a");
    }

    // --- CASE: no ELSE, cross-branch typing, and the short-circuit gap --------

    #[test]
    fn case_with_no_else_is_null_for_an_unmatched_row() {
        // Confirmed against a live PostgreSQL 18:
        // `SELECT CASE WHEN false THEN 1 END` is NULL, not an error and not
        // some other default.
        let batch = batch_bool2(vec![Some(false)], vec![Some(false)]);
        let expr = Expr::Case {
            operand: None,
            whens: vec![(col(0, "a"), lit_i32(1))],
            else_: None,
        };
        let result = eval(&expr, &batch).unwrap();
        assert!(result.is_null(0));
    }

    /// The trap: Postgres resolves a CASE's type across every branch, not
    /// from the branch written first. `id` (int8) is written second here —
    /// if the result took the FIRST branch's type (int4), this would either
    /// lose the high bits of `id` or fail outright.
    #[test]
    fn case_result_type_is_the_common_type_of_every_branch_not_just_the_first() {
        let batch = batch_bool2(vec![Some(true), Some(false)], vec![Some(true), Some(true)]);
        let big = Expr::Literal(Datum::Int64(5_000_000_000), PgType::INT8);
        let expr = Expr::Case {
            operand: None,
            whens: vec![(col(0, "a"), lit_i32(2))],
            else_: Some(Box::new(big)),
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(result.data_type(), &DataType::Int64);
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 2, "matched branch widened from int4 to int8");
        assert_eq!(
            arr.value(1),
            5_000_000_000,
            "int8 ELSE branch keeps its own value"
        );
    }

    /// `CASE WHEN … THEN 'a' ELSE 'b' END` — every branch is an untyped
    /// literal, so nothing supplies a concrete type. Confirmed against a
    /// live PostgreSQL 18: `pg_typeof(CASE WHEN true THEN 'a' ELSE 'b' END)`
    /// is `text`, Postgres's own fallback for an all-`unknown` input list.
    #[test]
    fn case_with_every_branch_unknown_defaults_to_text() {
        let batch = batch_bool2(vec![Some(true)], vec![Some(true)]);
        let expr = Expr::Case {
            operand: None,
            whens: vec![(col(0, "a"), lit_text_unknown("big"))],
            else_: Some(Box::new(lit_text_unknown("small"))),
        };
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(result.data_type(), &DataType::Utf8);
        assert_eq!(str_array(&result).value(0), "big");
    }

    /// Honest limitation (see `eval_case`'s doc comment): this file
    /// evaluates every branch eagerly over the whole batch, so a branch that
    /// only Postgres's short-circuiting protects from erroring CAN raise
    /// here, even on a row where that branch never "wins". Live PostgreSQL
    /// 18 runs `SELECT x, CASE WHEN x <> 0 THEN 1/x ELSE 0 END FROM (VALUES
    /// (0),(2)) AS t(x)` with no error at all (both rows come back `0`) —
    /// this crate currently cannot match that, and this test pins the gap
    /// rather than hiding it.
    #[test]
    fn case_does_not_short_circuit_and_can_error_on_the_unmatched_branch() {
        let batch = batch_i32("x", vec![Some(0), Some(2)]);
        let expr = Expr::Case {
            operand: None,
            whens: vec![(
                Expr::Binary {
                    op: op(518), // int4 <>
                    lhs: Box::new(col(0, "x")),
                    rhs: Box::new(lit_i32(0)),
                },
                Expr::Binary {
                    op: op(528), // int4 /
                    lhs: Box::new(lit_i32(1)),
                    rhs: Box::new(col(0, "x")),
                },
            )],
            else_: Some(Box::new(lit_i32(0))),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert_eq!(
            err,
            ExecError::DivisionByZero,
            "documented gap: real Postgres does not error on this query at all"
        );
    }

    // --- NULLIF, as lowering desugars it (see `lower_aexpr_nullif`) -----------
    //
    // `NULLIF(a, b)` has no dedicated `Expr` variant; lowering turns it into
    // `Expr::Case { whens: [(a = b, NULL)], else_: Some(a) }`. These tests
    // build that exact shape by hand to pin `eval_case`'s behaviour for it
    // without going through the parser.

    fn nullif_expr(a: Expr, b: Expr, eq_op: OpId) -> Expr {
        Expr::Case {
            operand: None,
            whens: vec![(
                Expr::Binary {
                    op: eq_op,
                    lhs: Box::new(a.clone()),
                    rhs: Box::new(b),
                },
                Expr::null_unknown(),
            )],
            else_: Some(Box::new(a)),
        }
    }

    #[test]
    fn nullif_returns_null_when_equal_and_a_otherwise() {
        let batch = batch_i32("id", vec![Some(1), Some(2)]);
        let expr = nullif_expr(col(0, "id"), lit_i32(1), op(96)); // int4 =
        let result = eval(&expr, &batch).unwrap();
        assert!(result.is_null(0), "NULLIF(1, 1) is NULL");
        assert_eq!(i32_array(&result).value(1), 2, "NULLIF(2, 1) is 2");
    }

    /// Confirmed against a live PostgreSQL 18: `pg_typeof(NULLIF(1::int8,
    /// 2::int4))` is `bigint` — `a`'s own type, never a type unified between
    /// `a` and `b`. `b` here is int4 while `id` is int8; the result must
    /// stay int8.
    #[test]
    fn nullif_result_type_is_as_own_type_not_a_unified_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(7)]))]).unwrap();
        let expr = nullif_expr(col(0, "id"), lit_i32(1), op(416)); // int8 = int4
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(result.data_type(), &DataType::Int64);
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    // --- GREATEST / LEAST, as lowering desugars them (see `lower_min_max_expr`)

    /// Builds the exact `Expr::Case` `lower_min_max_expr` desugars a
    /// two-argument `GREATEST`/`LEAST` into: `cmp_op` is `>` for GREATEST,
    /// `<` for LEAST.
    fn greatest_or_least_expr(a: Expr, b: Expr, cmp_op: OpId) -> Expr {
        Expr::Case {
            operand: None,
            whens: vec![
                (
                    Expr::IsNull {
                        arg: Box::new(a.clone()),
                        negated: false,
                    },
                    b.clone(),
                ),
                (
                    Expr::IsNull {
                        arg: Box::new(b.clone()),
                        negated: false,
                    },
                    a.clone(),
                ),
                (
                    Expr::Binary {
                        op: cmp_op,
                        lhs: Box::new(a.clone()),
                        rhs: Box::new(b.clone()),
                    },
                    a,
                ),
            ],
            else_: Some(Box::new(b)),
        }
    }

    /// Confirmed against a live PostgreSQL 18: `GREATEST(1, NULL) = 1` and
    /// `LEAST(1, NULL) = 1` — NULL arguments are ignored, not propagated,
    /// unlike almost every other construct in SQL.
    #[test]
    fn greatest_and_least_ignore_a_null_argument() {
        let batch = batch_i32("x", vec![Some(0)]);
        let n = Expr::Literal(Datum::Null, PgType::INT4);

        let greatest = greatest_or_least_expr(lit_i32(1), n.clone(), op(521)); // int4 >
        let result = eval(&greatest, &batch).unwrap();
        assert_eq!(
            i32_array(&result).value(0),
            1,
            "GREATEST(1, NULL) must be 1, not NULL"
        );

        let least = greatest_or_least_expr(lit_i32(1), n, op(97)); // int4 <
        let result = eval(&least, &batch).unwrap();
        assert_eq!(
            i32_array(&result).value(0),
            1,
            "LEAST(1, NULL) must be 1, not NULL"
        );
    }

    #[test]
    fn greatest_and_least_are_null_only_when_every_argument_is_null() {
        let batch = batch_i32("x", vec![Some(0)]);
        let n = || Expr::Literal(Datum::Null, PgType::INT4);
        let expr = greatest_or_least_expr(n(), n(), op(521)); // int4 >
        let result = eval(&expr, &batch).unwrap();
        assert!(result.is_null(0), "GREATEST(NULL, NULL) must be NULL");
    }

    #[test]
    fn greatest_picks_the_larger_and_least_the_smaller_of_two_non_null_values() {
        let batch = batch_i32("x", vec![Some(0)]);
        let greatest = greatest_or_least_expr(lit_i32(3), lit_i32(7), op(521)); // int4 >
        assert_eq!(i32_array(&eval(&greatest, &batch).unwrap()).value(0), 7);
        let least = greatest_or_least_expr(lit_i32(3), lit_i32(7), op(97)); // int4 <
        assert_eq!(i32_array(&eval(&least, &batch).unwrap()).value(0), 3);
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

    /// A LIKE pattern is written as a bare literal in practically every real
    /// query, so it reaches the evaluator as `unknown`. Arrow's kernel wants
    /// both sides to be the same string type and refused it, which sent
    /// `col LIKE 'a%'` — about as ordinary as SQL gets — back to fallback every
    /// time. The `PgType::TEXT` spelling above is the one the older tests use
    /// and is NOT what lowering actually produces.
    #[test]
    fn like_resolves_an_untyped_pattern_literal() {
        let batch = batch_str1("s", vec![Some("hello"), Some("world")]);
        let expr = Expr::Like {
            arg: Box::new(col(0, "s")),
            pattern: Box::new(Expr::Literal(Datum::Utf8("h%".into()), PgType::UNKNOWN)),
            escape: None,
            case_insensitive: false,
            negated: false,
        };
        let arr = eval(&expr, &batch).unwrap();
        let arr = bool_array(&arr);
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn ilike_and_not_like_resolve_an_untyped_pattern_too() {
        let batch = batch_str1("s", vec![Some("HELLO")]);
        let mk = |ci, neg| Expr::Like {
            arg: Box::new(col(0, "s")),
            pattern: Box::new(Expr::Literal(Datum::Utf8("h%".into()), PgType::UNKNOWN)),
            escape: None,
            case_insensitive: ci,
            negated: neg,
        };
        assert!(bool_array(&eval(&mk(true, false), &batch).unwrap()).value(0));
        assert!(!bool_array(&eval(&mk(true, true), &batch).unwrap()).value(0));
        // Case-sensitive LIKE must NOT match, or the resolution above would be
        // quietly folding case as well as type.
        assert!(!bool_array(&eval(&mk(false, false), &batch).unwrap()).value(0));
    }

    /// `x IN (1, 2)` and `x = 1 OR x = 2` are the same query to Postgres. Only
    /// the second spelling widened its literals against a bigint column, so the
    /// first failed on identical data for no reason a user could see.
    #[test]
    fn in_list_widens_a_bigint_column_against_int4_literals() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![
                Some(1i64),
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        let expr = Expr::InList {
            arg: Box::new(col(0, "n")),
            list: vec![lit_i32(1), lit_i32(2)],
            negated: false,
        };
        let arr = eval(&expr, &batch).unwrap();
        let arr = bool_array(&arr);
        assert!(arr.value(0));
        assert!(!arr.value(1));
        // Widening must not disturb three-valued logic: NULL IN (…) is NULL,
        // never false.
        assert!(arr.is_null(2));
    }

    #[test]
    fn not_in_widens_the_same_way_and_keeps_its_null_semantics() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![
                Some(1i64),
                Some(3),
            ]))],
        )
        .unwrap();
        let expr = Expr::InList {
            arg: Box::new(col(0, "n")),
            list: vec![lit_i32(1), lit_i32(2)],
            negated: true,
        };
        let arr = eval(&expr, &batch).unwrap();
        let arr = bool_array(&arr);
        assert!(!arr.value(0));
        assert!(arr.value(1));
    }

    #[test]
    fn in_list_resolves_untyped_literals_against_a_text_column() {
        let batch = batch_str1("s", vec![Some("a"), Some("z")]);
        let expr = Expr::InList {
            arg: Box::new(col(0, "s")),
            list: vec![
                Expr::Literal(Datum::Utf8("a".into()), PgType::UNKNOWN),
                Expr::Literal(Datum::Utf8("b".into()), PgType::UNKNOWN),
            ],
            negated: false,
        };
        let arr = eval(&expr, &batch).unwrap();
        let arr = bool_array(&arr);
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    /// The left operand can itself be untyped — `'a' IN (col)` — and the list
    /// has to type it, mirroring how a binary comparison takes its type from
    /// the other side.
    #[test]
    fn in_list_types_an_untyped_left_operand_from_the_list() {
        let batch = batch_str1("s", vec![Some("a"), Some("b")]);
        let expr = Expr::InList {
            arg: Box::new(Expr::Literal(Datum::Utf8("a".into()), PgType::UNKNOWN)),
            list: vec![col(0, "s")],
            negated: false,
        };
        let arr = eval(&expr, &batch).unwrap();
        let arr = bool_array(&arr);
        assert!(arr.value(0));
        assert!(!arr.value(1));
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

    // ─── Math — trig/log/exp/power (doc 19, entry 1) ─────────────────────

    fn assert_type_mismatch_contains(err: ExecError, needle: &str) {
        match err {
            ExecError::TypeMismatch(msg) => assert!(
                msg.contains(needle),
                "expected error containing {needle:?}, got {msg:?}"
            ),
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    /// `sqrt(-1::float8)` must ERROR, not return `NaN` — verified live
    /// against PostgreSQL 18 (`ERROR: 2201F: cannot take square root of a
    /// negative number`).
    #[test]
    fn sqrt_float8_of_negative_errors_instead_of_returning_nan() {
        let batch = one_row();
        let err = eval(&sf(OID_SQRT_FLOAT8, vec![lit_f64(-1.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(err, "cannot take square root of a negative number");
    }

    #[test]
    fn sqrt_float8_basic_and_null() {
        let batch = batch_f64(vec![Some(4.0), None]);
        let result = eval(&sf(OID_SQRT_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert_eq!(arr.value(0), 2.0);
        assert!(arr.is_null(1), "NULL in, NULL out");
    }

    /// `ln(0)` and `ln(-1)` must ERROR with the two distinct Postgres message
    /// shapes — verified live: `ERROR: 2201E: cannot take logarithm of zero`
    /// and `ERROR: 2201E: cannot take logarithm of a negative number`.
    #[test]
    fn ln_float8_zero_and_negative_error_with_distinct_messages() {
        let batch = one_row();
        let zero_err = eval(&sf(OID_LN_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(zero_err, "cannot take logarithm of zero");

        let neg_err = eval(&sf(OID_LN_FLOAT8, vec![lit_f64(-1.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(neg_err, "cannot take logarithm of a negative number");
    }

    #[test]
    fn ln_float8_basic_and_null() {
        let batch = batch_f64(vec![Some(std::f64::consts::E), None]);
        let result = eval(&sf(OID_LN_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert!((arr.value(0) - 1.0).abs() < 1e-12, "ln(e) must be 1");
        assert!(arr.is_null(1));
    }

    /// `log(double precision)` with one argument is BASE 10, not natural log
    /// — verified live: `log(100::float8) = 2`, while `ln(100::float8)` is
    /// not. Getting these backwards is the exact silent, plausible mistake
    /// the task warns about.
    #[test]
    fn log_float8_one_arg_is_base_10_not_natural_log() {
        let batch = one_row();
        let log_result = eval(&sf(OID_LOG_FLOAT8, vec![lit_f64(100.0)]), &batch).unwrap();
        assert_eq!(f64_array(&log_result).value(0), 2.0, "log(100) must be 2 (base 10)");

        let ln_result = eval(&sf(OID_LN_FLOAT8, vec![lit_f64(100.0)]), &batch).unwrap();
        assert!(
            (f64_array(&ln_result).value(0) - 4.605_170_185_988_091).abs() < 1e-9,
            "ln(100) must NOT equal log(100) — natural log, not base 10"
        );
    }

    #[test]
    fn log_float8_zero_and_negative_error() {
        let batch = one_row();
        let zero_err = eval(&sf(OID_LOG_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(zero_err, "cannot take logarithm of zero");
        let neg_err = eval(&sf(OID_LOG_FLOAT8, vec![lit_f64(-1.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(neg_err, "cannot take logarithm of a negative number");
    }

    #[test]
    fn exp_float8_basic_and_null() {
        let batch = batch_f64(vec![Some(1.0), None]);
        let result = eval(&sf(OID_EXP_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert!((arr.value(0) - std::f64::consts::E).abs() < 1e-12);
        assert!(arr.is_null(1));
    }

    /// Verified live: `cbrt(27) = 3`, `cbrt(-27) = -3` (cube root is defined
    /// for negative numbers, unlike square root).
    #[test]
    fn cbrt_float8_handles_negative_input() {
        let batch = batch_f64(vec![Some(27.0), Some(-27.0), None]);
        let result = eval(&sf(OID_CBRT_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert_eq!(arr.value(0), 3.0);
        assert_eq!(arr.value(1), -3.0);
        assert!(arr.is_null(2));
    }

    /// `power(0, 0) = 1` — verified live, needs no special case in this
    /// implementation (IEEE 754's own `pow` rule).
    #[test]
    fn power_float8_zero_to_the_zero_is_one() {
        let batch = one_row();
        let result = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64(0.0), lit_f64(0.0)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&result).value(0), 1.0);
    }

    /// A negative base with a non-integer exponent is a complex result and
    /// must ERROR — verified live: `ERROR: 2201F: a negative number raised
    /// to a non-integer power yields a complex result`. A negative base with
    /// an INTEGER exponent (even stored as a float) must NOT hit this check.
    #[test]
    fn power_float8_negative_base_fractional_exponent_errors_integer_exponent_does_not() {
        let batch = one_row();
        let err = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64(-2.0), lit_f64(0.5)]),
            &batch,
        )
        .unwrap_err();
        assert_type_mismatch_contains(
            err,
            "a negative number raised to a non-integer power yields a complex result",
        );

        let ok = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64(-2.0), lit_f64(3.0)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&ok).value(0), -8.0, "power(-2, 3) = -8, no error");
    }

    #[test]
    fn power_float8_null_propagates() {
        let batch = one_row();
        let result = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64_null(), lit_f64(2.0)]),
            &batch,
        )
        .unwrap();
        assert!(f64_array(&result).is_null(0));
    }

    /// `degrees`/`radians` are exact conversions of `pi()` — verified live:
    /// `degrees(pi()) = 180` exactly, not merely close.
    #[test]
    fn degrees_of_pi_is_exactly_180() {
        let batch = one_row();
        let pi_val = eval(&sf(OID_PI, vec![]), &batch).unwrap();
        let pi_expr = lit_f64(f64_array(&pi_val).value(0));
        let result = eval(&sf(OID_DEGREES_FLOAT8, vec![pi_expr]), &batch).unwrap();
        assert_eq!(f64_array(&result).value(0), 180.0);
    }

    /// `radians(180) = pi()` exactly — verified live.
    #[test]
    fn radians_of_180_equals_pi_exactly() {
        let batch = one_row();
        let result = eval(&sf(OID_RADIANS_FLOAT8, vec![lit_f64(180.0)]), &batch).unwrap();
        assert_eq!(f64_array(&result).value(0), std::f64::consts::PI);
    }

    #[test]
    fn pi_returns_a_row_per_input_row_not_just_one_value() {
        let batch = batch_f64(vec![Some(1.0), Some(2.0), Some(3.0)]);
        let result = eval(&sf(OID_PI, vec![]), &batch).unwrap();
        let arr = f64_array(&result);
        assert_eq!(arr.len(), 3, "pi() is niladic but must still fill every row");
        for i in 0..3 {
            assert_eq!(arr.value(i), std::f64::consts::PI);
        }
    }

    /// `sign(double precision)`: `-1`/`0`/`1` — verified live
    /// `sign(-5::float8) = -1`, and specifically NOT `f64::signum`'s
    /// "sign bit" answer of `1.0` for `+0.0`/`-1.0` for `-0.0`.
    #[test]
    fn sign_float8_zero_is_zero_not_signum_of_the_sign_bit() {
        let batch = batch_f64(vec![Some(-5.0), Some(0.0), Some(5.0), Some(-0.0), None]);
        let result = eval(&sf(OID_SIGN_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert_eq!(arr.value(0), -1.0);
        assert_eq!(arr.value(1), 0.0);
        assert_eq!(arr.value(2), 1.0);
        assert_eq!(arr.value(3), 0.0, "sign(-0.0) must be 0, not -1 (f64::signum's answer)");
        assert!(arr.is_null(4));
    }

    /// `sign(numeric)` — verified live `sign(-5::numeric) = -1`.
    #[test]
    fn sign_numeric_matches_float8() {
        let batch = decimal_batch("x", vec![Some(-50), Some(0), Some(50)], 5, 1); // -5.0, 0.0, 5.0
        let result = eval(&sf(OID_SIGN_NUMERIC, vec![col(0, "x")]), &batch).unwrap();
        let arr = decimal_array(&result);
        assert_eq!(arr.value(0), -10); // -1.0 at scale 1
        assert_eq!(arr.value(1), 0);
        assert_eq!(arr.value(2), 10); // 1.0 at scale 1
    }

    /// `asin`/`acos` outside `[-1, 1]` must ERROR (SQLSTATE 22003, "input is
    /// out of range") rather than silently return `NaN` — verified live.
    #[test]
    fn asin_and_acos_out_of_domain_error() {
        let batch = one_row();
        let asin_err = eval(&sf(OID_ASIN_FLOAT8, vec![lit_f64(2.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(asin_err, "input is out of range");
        let acos_err = eval(&sf(OID_ACOS_FLOAT8, vec![lit_f64(2.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(acos_err, "input is out of range");
    }

    #[test]
    fn trig_family_basic_values_and_null() {
        let batch = one_row();
        let sin_r = eval(
            &sf(OID_SIN_FLOAT8, vec![lit_f64(std::f64::consts::FRAC_PI_2)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&sin_r).value(0), 1.0);

        let cos_r = eval(&sf(OID_COS_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap();
        assert_eq!(f64_array(&cos_r).value(0), 1.0);

        let tan_r = eval(&sf(OID_TAN_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap();
        assert_eq!(f64_array(&tan_r).value(0), 0.0);

        let atan_r = eval(&sf(OID_ATAN_FLOAT8, vec![lit_f64(1.0)]), &batch).unwrap();
        assert!((f64_array(&atan_r).value(0) - std::f64::consts::FRAC_PI_4).abs() < 1e-12);

        let atan2_r = eval(
            &sf(OID_ATAN2_FLOAT8, vec![lit_f64(1.0), lit_f64(1.0)]),
            &batch,
        )
        .unwrap();
        assert!((f64_array(&atan2_r).value(0) - std::f64::consts::FRAC_PI_4).abs() < 1e-12);

        let null_sin = eval(&sf(OID_SIN_FLOAT8, vec![lit_f64_null()]), &batch).unwrap();
        assert!(f64_array(&null_sin).is_null(0));
    }

    /// `ceiling` is the SQL-standard-named alias of `ceil` — same behaviour,
    /// genuinely different `pg_proc` oid per the module docs — for both
    /// `numeric` and `float8`.
    #[test]
    fn ceiling_matches_ceil_for_both_numeric_and_float8() {
        let float_batch = batch_f64(vec![Some(4.1)]);
        let ceiling_f = eval(&sf(OID_CEILING_FLOAT8, vec![col(0, "x")]), &float_batch).unwrap();
        assert_eq!(f64_array(&ceiling_f).value(0), 5.0);

        let numeric_batch = decimal_batch("x", vec![Some(-41)], 3, 1); // -4.1
        let ceiling_n =
            eval(&sf(OID_CEILING_NUMERIC, vec![col(0, "x")]), &numeric_batch).unwrap();
        assert_eq!(decimal_array(&ceiling_n).value(0), -40); // -4.0
    }

    /// `trunc` truncates toward zero, unlike `floor` — verified live:
    /// `trunc(3.7) = 3` but `trunc(-3.7) = -3`, not `-4`.
    #[test]
    fn trunc_float8_truncates_toward_zero() {
        let batch = batch_f64(vec![Some(3.7), Some(-3.7), None]);
        let result = eval(&sf(OID_TRUNC_FLOAT8, vec![col(0, "x")]), &batch).unwrap();
        let arr = f64_array(&result);
        assert_eq!(arr.value(0), 3.0);
        assert_eq!(arr.value(1), -3.0, "trunc(-3.7) must be -3, not -4 (floor's answer)");
        assert!(arr.is_null(2));
    }

    /// `trunc(numeric)` (no explicit scale) truncates to an integer, toward
    /// zero — verified live `trunc(-3.7::numeric) = -3`.
    #[test]
    fn trunc_numeric_no_scale_truncates_toward_zero() {
        let batch = decimal_batch("x", vec![Some(-37)], 3, 1); // -3.7
        let result = eval(&sf(OID_TRUNC_NUMERIC, vec![col(0, "x")]), &batch).unwrap();
        assert_eq!(decimal_array(&result).value(0), -30); // -3.0
    }

    /// `trunc(numeric, ndigits)` takes a scale, including a NEGATIVE one
    /// (truncating to the left of the decimal point) — verified live:
    /// `trunc(-3.14159::numeric, 2) = -3.14`, `trunc(12345::numeric, -2) =
    /// 12300`.
    #[test]
    fn trunc_numeric_takes_a_scale_including_negative() {
        let batch = decimal_batch("x", vec![Some(-314159)], 6, 5); // -3.14159
        let ndigits = batch_i32("n", vec![Some(2)]);
        let combined_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Decimal128(6, 5), true),
            Field::new("n", DataType::Int32, true),
        ]));
        let combined = RecordBatch::try_new(
            combined_schema,
            vec![batch.column(0).clone(), ndigits.column(0).clone()],
        )
        .unwrap();
        let result = eval(
            &sf(OID_TRUNC_NUMERIC_N, vec![col(0, "x"), col(1, "n")]),
            &combined,
        )
        .unwrap();
        assert_eq!(decimal_array(&result).value(0), -314000); // -3.14000 at scale 5

        // Negative ndigits: truncate to the left of the decimal point.
        let whole = decimal_batch("y", vec![Some(12345)], 5, 0); // 12345
        let neg_ndigits = batch_i32("n", vec![Some(-2)]);
        let neg_schema = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Decimal128(5, 0), true),
            Field::new("n", DataType::Int32, true),
        ]));
        let neg_combined = RecordBatch::try_new(
            neg_schema,
            vec![whole.column(0).clone(), neg_ndigits.column(0).clone()],
        )
        .unwrap();
        let neg_result = eval(
            &sf(OID_TRUNC_NUMERIC_N, vec![col(0, "y"), col(1, "n")]),
            &neg_combined,
        )
        .unwrap();
        assert_eq!(decimal_array(&neg_result).value(0), 12300);
    }

    /// The 30 `pg_proc` rows named in docs/migration/df-removal/
    /// 19-expires-at-removal.md entry 1 include numeric-argument
    /// transcendental overloads (`sqrt`/`ln`/`log`/`exp`/`power` on
    /// `numeric`) that this file deliberately does NOT implement — see the
    /// "Math — numeric transcendental overloads" comment above
    /// [`decimal_sign`]. This test pins that the gap is honest (falls
    /// through to the `other =>` internal-error arm, which the bridge above
    /// this crate turns into a DataFusion fallback) rather than silently
    /// routed through float arithmetic.
    #[test]
    fn numeric_transcendental_overloads_remain_unbacked_not_routed_through_float() {
        let batch = decimal_batch("x", vec![Some(40)], 3, 1); // 4.0
        let err = eval(&sf(1730, vec![col(0, "x")]), &batch).unwrap_err(); // sqrt(numeric)
        match err {
            ExecError::Internal(_) => {}
            other => panic!("expected Internal (unimplemented), got {other:?}"),
        }
    }
}
