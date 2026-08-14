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
//!      side is NULL). `concat_ws` skips NULL *values* the same way but is
//!      strict in its *separator* — see [`eval_concat_ws`].
//!    - `position(a, b)` in functional notation takes `(haystack, needle)` —
//!      the reverse of the `POSITION(needle IN haystack)` grammar that
//!      desugars to it. See [`OID_POSITION`].
//!    - `right(s, i32::MIN)` returns the whole string, not `''`, because
//!      Postgres's own negation of the argument overflows. Reproduced on
//!      purpose; see [`pg_right`] before changing it.
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
//!
//! # The float8 policy: IEEE-754 first, domain second, range last
//!
//! Every `double precision` math function in this file follows one order of
//! operations, and the order is the whole policy. It was read off a live
//! PostgreSQL 18.2 (`PG_DIFF_TEST_DSN`), function by function, not recalled
//! from the C source:
//!
//! 1. **IEEE-754 special cases first.** `NaN` and `±Infinity` inputs are
//!    answered *before* any domain guard runs. `asin('NaN')` is `NaN`, not
//!    "input is out of range" — even though `NaN` is trivially "outside
//!    `[-1, 1]`", because every comparison against `NaN` is false and
//!    Postgres's own guard (`arg < -1.0 || arg > 1.0`) therefore never fires
//!    for it. Likewise `power('NaN', 0) = 1` and `power(1, 'NaN') = 1` (the
//!    POSIX rule) are decided before `power`'s two domain errors, so
//!    `power(0, 'NaN')` is `NaN` rather than "zero raised to a negative
//!    power". Getting this order backwards is the single defect that produced
//!    most of the float8 divergences this section was written to close.
//! 2. **Domain guards second**, on the values that survive step 1 — `sqrt` of
//!    a negative, `ln`/`log` of a non-positive, `asin`/`acos` outside
//!    `[-1, 1]`, `power`'s zero-to-a-negative-power and negative-base
//!    -to-a-non-integer-power. `sin`/`cos`/`tan` are domain guards too, in
//!    the other direction: a *finite* argument is always in domain, and an
//!    infinite one is the error (`sin('Infinity')` raises 22003 "input is out
//!    of range", it does not return `NaN` the way libm does).
//!
//!    "Integer exponent" in `power`'s guard is `exponent.floor() ==
//!    exponent`, Postgres's own `floor(arg2) != arg2` test — deliberately NOT
//!    `exponent.fract() != 0.0`, which is the same thing for finite values
//!    but wrong for `±Infinity`: `f64::fract` of an infinity is `NaN`, so a
//!    `fract`-based guard reports every infinite exponent as non-integral and
//!    rejects `power(-1, 'Infinity')`, which Postgres answers `1`.
//! 3. **Range-check the result last.** libm returns `±Infinity` on overflow
//!    and `0` on underflow; Postgres raises SQLSTATE 22003 for both, "value
//!    out of range: overflow" / "value out of range: underflow". That is
//!    [`check_float8_result`], a transcription of Postgres's own
//!    `check_float8_val(val, inf_is_valid, zero_is_valid)`, and the two
//!    validity flags are the part worth stating precisely:
//!
//!    - An infinite *result* is an error unless an *input* was already
//!      infinite. `exp('Infinity')` is `Infinity`; `exp(1.8e308)` is an
//!      overflow error even though both produce the same bits.
//!    - A *zero* result is an error unless zero was already an achievable
//!      answer for that input — which is function-specific, and is the place
//!      this is easy to get wrong. Verified live: `exp(-745)` is `5e-324`
//!      but `exp(-746)` is an underflow *error*, not `0`; `radians(4.9e-324)`
//!      is an underflow error while `radians(0)` is a perfectly good `0`.
//!      For `power` the rule confirmed live is three-part — a zero result is
//!      legitimate when the base is zero (`power(0, 5) = 0`), when the base
//!      is infinite (`power('Infinity', -2) = 0`), or when the exponent is
//!      infinite (`power(0.5, 'Infinity') = 0`) — and an underflow error
//!      otherwise (`power(0.5, 1.8e308)`, `power(2, -1e300)`).
//!
//! `sqrt`, `cbrt`, `ln` and `log` deliberately carry no step-3 check. Real
//! Postgres runs one on them, but it is provably unreachable for those four:
//! each is monotone and range-contracting, so its result can only be
//! infinite or zero when the input already was (`sqrt(1.8e308)` is
//! `1.3e154`, `cbrt` smaller still, `ln`'s only zero is `ln(1)` which
//! Postgres explicitly permits). A dead branch is worse documentation than
//! this paragraph.
//!
//! One honest gap: [`ExecError`] has no variant for "numeric value out of
//! range" that carries a message, so all of these — the 22003s and the
//! 2201E/2201F domain errors alike — are raised as
//! [`ExecError::TypeMismatch`] carrying Postgres's exact message text. That
//! is the convention the float8 errors in this file already used before this
//! policy existed, kept rather than split. When a SQLSTATE mapping layer
//! lands it has to classify this whole family together; the message strings
//! are exact so that it can.

use std::sync::Arc;

use arrow::compute::kernels::{
    arity, boolean, cast, cmp, comparison, concat_elements, numeric, sort::sort_to_indices, zip,
};
use arrow_array::{
    new_empty_array, new_null_array,
    timezone::Tz,
    types::{
        Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        IntervalMonthDayNano, IntervalMonthDayNanoType,
    },
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array,
    Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch,
    StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, Field, SortOptions};
use arrow_select::{interleave, take};
use chrono::{
    offset::MappedLocalTime, DateTime, Datelike, NaiveDate, NaiveDateTime, Offset, TimeDelta,
    TimeZone as _, Timelike,
};

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

// ─── Date/integer operator OIDs ─────────────────────────────────────────────
//
// Real `pg_operator.oid`s, read from a live PostgreSQL 18.2:
//
// ```sql
// SELECT oid, oprname, oprleft::regtype, oprright::regtype,
//        oprresult::regtype, oprcode
//   FROM pg_operator
//  WHERE 'date'::regtype IN (oprleft, oprright) AND oprname IN ('+','-');
// ```
//
// These four are dispatched by OID rather than by the operator NAME the rest
// of `eval_binary` keys on, because the name cannot tell them apart: `-`
// covers both `date - integer` (which yields another *date*) and
// `date - date` (which yields an *integer* count of days), and the arrow
// arrays alone would not distinguish `date + interval` (a *timestamp*, oid
// 1076, already served by arrow's kernel) from `date + integer`.
const OID_OP_DATE_PLI: u32 = 1100; // date + integer -> date
const OID_OP_DATE_MII: u32 = 1101; // date - integer -> date
const OID_OP_DATE_MI_DATE: u32 = 1099; // date - date -> integer
const OID_OP_INT_PL_DATE: u32 = 2555; // integer + date -> date

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
// `char_length(text)` and `character_length(text)` are the SQL-standard
// spellings of `length(text)`. Postgres gives each its own `pg_proc` row
// rather than aliasing them, so a dispatch keyed on OID must name all three
// even though one implementation answers them: measured live on PG 18.2,
// `length`/`char_length`/`character_length` all return 5 for 'héllo',
// 3 for '日本語' and 7 for '🎉party🎉' — characters, not bytes.
const OID_CHAR_LENGTH_TEXT: u32 = 1381; // char_length(text)
const OID_CHARACTER_LENGTH_TEXT: u32 = 1369; // character_length(text)
// `array_length(anyarray, integer)`. See [`eval_array_length`] for the three
// ways Postgres answers NULL here.
const OID_ARRAY_LENGTH: u32 = 2176; // array_length(anyarray, integer)
const OID_SUBSTR_2: u32 = 883; // substr(text, int)
const OID_SUBSTR_3: u32 = 877; // substr(text, int, int)

// `substring(text, int)`/`substring(text, int, int)` are separate `pg_proc`
// rows from `substr` above, not aliases of them — `SUBSTRING(x FROM y FOR z)`
// desugars to these OIDs. Confirmed identical in behaviour on a live
// PostgreSQL 18 (every value in `orphan_functions.rs`'s battery agrees with
// `substr`), so they share [`eval_substr`]. The `substring(text, text)` and
// `substring(text, text, text)` overloads (oids 2073/2074) are POSIX-regex
// extraction and need a regex engine; they are deliberately absent here and
// from `basin_pgtype::func::FUNCS`, so they do not resolve at all rather than
// resolving to a wrong answer. The `bit` (1680/1699) and `bytea` (2012/2013)
// overloads are likewise absent — Basin has no physical `bit` type.
const OID_SUBSTRING_2: u32 = 937; // substring(text, int)
const OID_SUBSTRING_3: u32 = 936; // substring(text, int, int)
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
const OID_CONCAT_WS: u32 = 3059; // concat_ws(text, VARIADIC "any")
const OID_BTRIM_1: u32 = 885; // btrim(text) — trim(x)/trim(both from x)
const OID_BTRIM_2: u32 = 884; // btrim(text, text)
const OID_LTRIM_1: u32 = 881; // ltrim(text)
const OID_LTRIM_2: u32 = 875; // ltrim(text, text)
const OID_RTRIM_1: u32 = 882; // rtrim(text)
const OID_RTRIM_2: u32 = 876; // rtrim(text, text)
const OID_REPLACE: u32 = 2087; // replace(text, text, text)
const OID_STRPOS: u32 = 868; // strpos(text, text)

// `position(text, text)` is a distinct `pg_proc` row from `strpos`, with the
// *same* argument order: `(haystack, needle)`. The `POSITION(needle IN
// haystack)` grammar reverses them on its way to this OID, so a call written
// in functional notation reads "backwards" relative to the syntax —
// `pg_catalog.position('b', 'abc')` is `0`, not `2` (verified live).
const OID_POSITION: u32 = 849; // position(text, text)

// ─── String — case-folding, padding, repetition, splitting ─────────────────
//
// Every oid below was re-read from the live PostgreSQL 18 `pg_proc` with the
// query at the top of this block, not carried over from a note: `lpad` and
// `rpad` in particular have their two- and three-argument overloads numbered
// in the opposite order to each other's intuition (`lpad(text,int)` is 879
// but `lpad(text,int,text)` is the *lower* 873), which is exactly the kind of
// transposition that would resolve to the wrong function silently.
const OID_INITCAP: u32 = 872; // initcap(text)
const OID_LPAD_2: u32 = 879; // lpad(text, integer)
const OID_LPAD_3: u32 = 873; // lpad(text, integer, text)
const OID_RPAD_2: u32 = 880; // rpad(text, integer)
const OID_RPAD_3: u32 = 874; // rpad(text, integer, text)
const OID_REPEAT: u32 = 1622; // repeat(text, integer)
const OID_SPLIT_PART: u32 = 2088; // split_part(text, text, integer)

// ─── Date/time ──────────────────────────────────────────────────────────────
//
// `age(timestamp, timestamp)` is the ONLY one of `age`'s four `pg_proc` rows
// implemented here, and the other three are left to fall through to the
// `other =>` arm deliberately — each of them needs session state `eval()` does
// not have:
//
//   * `age(timestamptz)` (1386) and `age(timestamp)` (2059) are the
//     one-argument forms, which are defined as `age(current_date, $1)`.
//     `current_date` is a property of the session's clock and timezone.
//   * `age(timestamptz, timestamptz)` (1199) takes two absolute instants but
//     computes the *symbolic* difference between their renderings in the
//     session timezone, so the answer genuinely depends on that zone.
//     Verified on a live PostgreSQL 18 — the same two instants, differing
//     only in the session's `TimeZone`:
//
//     ```text
//     SET TimeZone='UTC';               age(...) = 1 mon 25 days 21:30:00
//     SET TimeZone='America/New_York';  age(...) = 1 mon 25 days 22:30:00
//     ```
//
//     (the pair straddles the 2024-03-10 US DST transition, which UTC does
//     not have). Basin's physical `timestamptz` is UTC micros with no
//     rendering zone, so answering this one would mean inventing a zone —
//     exactly the silent-wrong-answer shape this file exists to avoid.
//
// `age(timestamp, timestamp)` has no such dependency: both arguments are
// already civil wall-clock readings, and the result is a pure function of
// them.
const OID_AGE_TIMESTAMP: u32 = 2058; // age(timestamp, timestamp)

// `date_trunc` / `date_part`. The three `timestamptz` overloads read the
// session's `TimeZone` out of [`EvalSession`]; the `timestamp`, `date` and
// `interval` ones do not, and are pure functions of their arguments.
const OID_DATE_TRUNC_TIMESTAMPTZ: u32 = 1217; // date_trunc(text, timestamptz)
const OID_DATE_TRUNC_TIMESTAMP: u32 = 2020; // date_trunc(text, timestamp)
const OID_DATE_TRUNC_INTERVAL: u32 = 1218; // date_trunc(text, interval)
const OID_DATE_PART_TIMESTAMPTZ: u32 = 1171; // date_part(text, timestamptz)
const OID_DATE_PART_TIMESTAMP: u32 = 2021; // date_part(text, timestamp)
const OID_DATE_PART_DATE: u32 = 1384; // date_part(text, date)
const OID_DATE_PART_INTERVAL: u32 = 1172; // date_part(text, interval)

// `extract`. **Not `date_part` with a cast** — see [`eval_extract`] and
// `basin_pgtype::func`'s `extract` block for the measured pair of answers
// that differ, in both the return type (`numeric`, not `float8`) and the set
// of units each argument type accepts.
const OID_EXTRACT_DATE: u32 = 6199; // extract(text, date)
const OID_EXTRACT_TIME: u32 = 6200; // extract(text, time)
const OID_EXTRACT_TIMETZ: u32 = 6201; // extract(text, timetz)
const OID_EXTRACT_TIMESTAMP: u32 = 6202; // extract(text, timestamp)
const OID_EXTRACT_TIMESTAMPTZ: u32 = 6203; // extract(text, timestamptz)
const OID_EXTRACT_INTERVAL: u32 = 6204; // extract(text, interval)

// `to_char`, the two temporal overloads. There is deliberately no
// `to_char(date, text)` constant because **PostgreSQL has no such row** — a
// `date` argument reaches 2049 through the implicit `date -> timestamp`
// cast; see [`eval_to_char_datetime`].
const OID_TO_CHAR_TIMESTAMPTZ: u32 = 1770; // to_char(timestamptz, text)
const OID_TO_CHAR_TIMESTAMP: u32 = 2049; // to_char(timestamp, text)

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

// ─── The DataFusion orphans ─────────────────────────────────────────────────
//
// `docs/migration/df-removal/17-udf-rehosting.md` §3 and
// `22-removal-surface-measured.md` §8 name the `pg_catalog` functions Basin
// answers today ONLY because `datafusion-functions` answers them: no Basin
// code registers them and none implements them, so they vanish silently the
// day `datafusion = "53"` leaves the workspace. Commit `c9bd3a68` landed
// their `pg_proc` rows in `basin_pgtype::func::FUNCS` — which made them
// *resolvable* but still unimplemented, so every call reached the `other =>`
// arm below. These are the arms that make them answerable.
//
// Every OID here was read from the live PostgreSQL 18.2 `pg_proc` this
// session, with `pg_get_function_identity_arguments` printed alongside so an
// overload could not be confused with its sibling — never recalled. Every
// edge case each implementation is written around was likewise *asked of the
// server*, not reasoned about; the surprising ones are recorded on the
// individual functions.
//
// The array family. `basin_pgtype::func` monomorphizes these on
// `anycompatiblearray`/`anycompatible` at `int4[]` and `text[]`; the
// implementations below are element-type agnostic (they move elements with
// `take`/`interleave` and compare them with the `not_distinct` kernel), so a
// wider monomorphization in that table needs no change here.
const OID_ARRAY_APPEND: u32 = 378; // array_append(anycompatiblearray, anycompatible)
const OID_ARRAY_PREPEND: u32 = 379; // array_prepend(anycompatible, anycompatiblearray)
const OID_ARRAY_CAT: u32 = 383; // array_cat(anycompatiblearray, anycompatiblearray)
const OID_ARRAY_REMOVE: u32 = 3167; // array_remove(anycompatiblearray, anycompatible)
const OID_ARRAY_REPLACE: u32 = 3168; // array_replace(anycompatiblearray, anycompatible, anycompatible)
const OID_ARRAY_POSITION: u32 = 3277; // array_position(anycompatiblearray, anycompatible)
const OID_ARRAY_POSITION_START: u32 = 3278; // array_position(anycompatiblearray, anycompatible, integer)
const OID_ARRAY_POSITIONS: u32 = 3279; // array_positions(anycompatiblearray, anycompatible)
const OID_ARRAY_NDIMS: u32 = 748; // array_ndims(anyarray)
const OID_CARDINALITY: u32 = 3179; // cardinality(anyarray)
const OID_ARRAY_REVERSE: u32 = 6381; // array_reverse(anyarray)
const OID_ARRAY_SORT_1: u32 = 6388; // array_sort(anyarray)
const OID_ARRAY_SORT_2: u32 = 6389; // array_sort(anyarray, descending boolean)
const OID_ARRAY_SORT_3: u32 = 6390; // array_sort(anyarray, descending boolean, nulls_first boolean)
const OID_ARRAY_TO_STRING_2: u32 = 395; // array_to_string(anyarray, text)
const OID_ARRAY_TO_STRING_3: u32 = 384; // array_to_string(anyarray, text, text)
const OID_STRING_TO_ARRAY_2: u32 = 394; // string_to_array(text, text)
const OID_STRING_TO_ARRAY_3: u32 = 376; // string_to_array(text, text, text)

// String/binary measurement and construction.
const OID_BIT_LENGTH_TEXT: u32 = 1811; // bit_length(text)
const OID_BIT_LENGTH_BYTEA: u32 = 1810; // bit_length(bytea)
const OID_OCTET_LENGTH_TEXT: u32 = 1374; // octet_length(text)
const OID_OCTET_LENGTH_BYTEA: u32 = 720; // octet_length(bytea)
const OID_STARTS_WITH: u32 = 3696; // starts_with(text, text)
const OID_OVERLAY_TEXT_3: u32 = 1405; // overlay(text, text, integer)
const OID_OVERLAY_TEXT_4: u32 = 1404; // overlay(text, text, integer, integer)
const OID_OVERLAY_BYTEA_3: u32 = 752; // overlay(bytea, bytea, integer)
const OID_OVERLAY_BYTEA_4: u32 = 749; // overlay(bytea, bytea, integer, integer)
const OID_TO_HEX_INT4: u32 = 2089; // to_hex(integer)
const OID_TO_HEX_INT8: u32 = 2090; // to_hex(bigint)

// Integer math.
const OID_FACTORIAL: u32 = 1376; // factorial(bigint) -> numeric
const OID_GCD_INT4: u32 = 5044; // gcd(integer, integer)
const OID_GCD_INT8: u32 = 5045; // gcd(bigint, bigint)
const OID_LCM_INT4: u32 = 5046; // lcm(integer, integer)
const OID_LCM_INT8: u32 = 5047; // lcm(bigint, bigint)
const OID_COT_FLOAT8: u32 = 1607; // cot(double precision)

// Float math whose `pg_proc` rows this session added to
// `basin_pgtype::func::FUNCS` — see that table's "the hyperbolics, log10 and
// pow" block. The differential harness reported all eight as UNRESOLVABLE
// (no catalog row) rather than merely unimplemented, so both halves land
// together.
const OID_SINH_FLOAT8: u32 = 2462; // sinh(double precision)
const OID_COSH_FLOAT8: u32 = 2463; // cosh(double precision)
const OID_TANH_FLOAT8: u32 = 2464; // tanh(double precision)
const OID_ASINH_FLOAT8: u32 = 2465; // asinh(double precision)
const OID_ACOSH_FLOAT8: u32 = 2466; // acosh(double precision)
const OID_ATANH_FLOAT8: u32 = 2467; // atanh(double precision)
const OID_LOG10_FLOAT8: u32 = 1194; // log10(double precision) — a separate oid from log(float8) 1340
const OID_POW_FLOAT8: u32 = 1346; // pow(double precision, double precision) — separate from power 1368

// Date construction.
const OID_MAKE_DATE: u32 = 3846; // make_date(year int, month int, day int) -> date

// ─── Orphans deliberately still unimplemented ───────────────────────────────
//
// Named here so their absence is a decision with a measured reason rather
// than an oversight. Each one falls through to the `other =>` arm and errors,
// which is the honest outcome for all of them:
//
//   * `octet_length(character)` (1375). PostgreSQL blank-pads `character(n)`
//     and counts the padding: `octet_length('abc'::char(10))` is **10**,
//     measured live, while `length('abc'::char(10))` is 3. `basin_pgtype`
//     maps `bpchar` to Arrow `Utf8` with no padding at all
//     (`basin_pgtype::physical`, `oid::BPCHAR => DataType::Utf8`), so this
//     function would report 3 where PostgreSQL reports 10 for every value
//     narrower than its declared width. That is a wrong answer, not a missing
//     one. (`bit_length('abc'::char(10))` is 24, not 80 — that call resolves
//     through the *text* overload after an implicit `bpchar -> text` cast,
//     which strips the padding, so 1811 below is unaffected.)
//   * `regexp_count` (6254-6256), `regexp_instr` (6257-6262) and
//     `regexp_like` (6263-6264). Same refusal commit `b9f07643` recorded for
//     the rest of the regex family: PostgreSQL's pattern language is POSIX
//     ARE, which has backreferences and lookaround; Rust's `regex` crate
//     cannot compile either, by design. Basin would therefore *error* on
//     patterns PostgreSQL answers, and — worse — the subset that does compile
//     differs in longest-vs-leftmost alternation semantics. A clean refusal
//     beats a subtly different regex engine.
//   * `md5` (2311, 2321) and `random` (1598, 6339-6341). Both need a
//     dependency `basin-exec`'s manifest does not carry (an MD5
//     implementation; an RNG), and this session does not own that manifest.
//     `md5` is otherwise a pure, exactly-specifiable function and is the
//     cheapest remaining orphan once the dependency question is settled.
//   * `now` (1299). Implementable — [`EvalSession::transaction_timestamp`] is
//     exactly its value — but only against a session that HAS a clock, and
//     `eval()`'s public entry point supplies [`EvalSession::DEFAULT`], which
//     deliberately has none (see that constant's docs). Adding the arm
//     without a way to reach it from `eval()` would be unreachable code.
//   * `percentile_cont`. An *ordered-set* aggregate
//     (`pg_aggregate.aggkind = 'o'`), spelled
//     `percentile_cont(f) WITHIN GROUP (ORDER BY x)`. It is not a scalar
//     function and has no `pg_proc` row in `basin_pgtype::func::FUNCS` for
//     the reason that table's own module docs give: `FuncKind` has no variant
//     for an ordered-set aggregate, and tabulating it as a plain aggregate
//     would assert it is callable as an ordinary two-argument one — which is
//     precisely the "same name, different function" trap DataFusion falls
//     into for this name.

// ─── Session context ────────────────────────────────────────────────────────

/// The session state a scalar expression is evaluated *against*.
///
/// # Why this type exists
///
/// A surprising number of Postgres's scalar functions are not functions of
/// their arguments alone. `date_trunc('day', tstz)` truncates in the
/// **session** timezone; `date_part('hour', tstz)` reads the hour off the
/// **session**-local rendering; `now()` is the **transaction**'s timestamp,
/// not the clock's; `CURRENT_DATE` is today's date **in the session zone**,
/// which near midnight is a different date from today-in-UTC. Every one of
/// those was previously left unimplemented in this file with the note
/// "`eval()` has no session context" — correctly, because answering them
/// against a hard-coded UTC would be a silent wrong answer for every session
/// that is not UTC, which is the failure mode this file exists to refuse.
///
/// `EvalSession` is that missing context, and it is deliberately a *value*:
/// it is snapshotted once per statement and read (never written) during
/// evaluation, so it needs no interior mutability and can be shared by
/// reference across every operator in a plan.
///
/// # What it carries, and what it does not
///
/// Only the state some scalar function actually consumes today:
///
/// * [`time_zone`](Self::time_zone) — the `TimeZone` GUC.
/// * [`transaction_timestamp`](Self::transaction_timestamp) — what `now()`,
///   `transaction_timestamp()` and `CURRENT_TIMESTAMP` return.
/// * [`statement_timestamp`](Self::statement_timestamp) — what
///   `statement_timestamp()` returns. Distinct from the above: Postgres
///   advances it per statement while `now()` stays pinned for the whole
///   transaction (verified live — see the two timestamps' own docs).
///
/// Postgres has plenty more session state that a later function will want —
/// `DateStyle` and `IntervalStyle` (output rendering), `search_path`,
/// `current_user`/`session_user`/`current_database`, `extra_float_digits`,
/// `lc_time` (month names in `to_char`) — and each belongs here when the
/// function that reads it lands. None are guessed at in advance: a field no
/// caller populates is a field that will be populated wrongly.
///
/// # Where it enters, and how it reaches `eval`
///
/// The real source is the pgwire session: the `TimeZone` GUC and the
/// transaction's start timestamp both live in `basin-engine`'s session state,
/// which is where a `EvalSession` should be built once per statement and
/// handed down with the plan. That wiring is **not** done here — see the
/// "Not yet wired" note below — so this file provides the shape and two
/// entry points rather than pretending the plumbing exists:
///
/// * [`eval_with`] is the real entry point. It takes the session explicitly,
///   threads it through every recursive step, and is what an operator should
///   call once it holds one.
/// * [`eval`] is the two-argument form every existing call site already
///   uses. It evaluates against [`EvalSession::DEFAULT`] — UTC, with **no**
///   transaction timestamp — and keeps working unchanged.
///
/// There is deliberately no thread-local "ambient session". It would let
/// every existing call site pick up a real zone with no signature change,
/// which is exactly why it is tempting — and it would silently fall back to
/// UTC the moment an operator ran on a worker thread that had not installed
/// one. A wrong answer that depends on which thread ran the batch is worse
/// than a signature change.
///
/// # Not yet wired
///
/// `EvalSession::DEFAULT` is UTC because UTC is the only zone that is right
/// by construction when nobody has said otherwise — not because UTC is a
/// good guess. Until `basin-engine` builds a session from the live GUC and
/// `basin-exec`'s operators carry it, `date_trunc`/`date_part` on
/// `timestamptz` answer as if the session were UTC. That is the same answer
/// they gave before this type existed; what is new is that the answer is now
/// a *parameter* rather than an assumption, and `eval_with` already produces
/// the correct answer for any zone the caller supplies (proved by this
/// file's tests, against live PostgreSQL values, in three zones across both
/// 2024 DST transitions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalSession {
    /// The session's `TimeZone` GUC, as its IANA name.
    ///
    /// Kept as the name rather than only the resolved [`Tz`] because the name
    /// is what `SHOW TimeZone` must echo back and what an error message must
    /// quote. The resolved zone is derived on demand by [`Self::time_zone`].
    time_zone: String,
    /// Microseconds since the Unix epoch, UTC, captured when the transaction
    /// began. `None` means "this evaluation has no transaction" — see
    /// [`Self::transaction_timestamp`].
    transaction_timestamp: Option<i64>,
    /// Microseconds since the Unix epoch, UTC, captured when the current
    /// statement began.
    statement_timestamp: Option<i64>,
}

impl EvalSession {
    /// The session an evaluation runs against when the caller supplies none:
    /// UTC, and no clock at all.
    ///
    /// The absent clock is the important half. A session with no transaction
    /// timestamp makes `now()` **error** rather than return a fresh
    /// `Utc::now()` per call — because a `now()` that changes between two
    /// rows of the same query is not `now()`, it is `clock_timestamp()`
    /// wearing its name, and Postgres's `now()` is stable for the whole
    /// transaction (verified live: two `now()` calls in one `BEGIN` block
    /// return the identical value while `clock_timestamp()` advances between
    /// them). Refusing is honest; guessing is not.
    pub const DEFAULT: EvalSession = EvalSession {
        time_zone: String::new(),
        transaction_timestamp: None,
        statement_timestamp: None,
    };

    /// A session in `time_zone`, with no clock.
    ///
    /// The zone is **not** validated here — an unknown name is reported by
    /// the function that needs it, with Postgres's own message, rather than
    /// at construction time, so that a session carrying a bad `TimeZone` can
    /// still run every query that does not read the clock.
    pub fn with_time_zone(time_zone: impl Into<String>) -> Self {
        EvalSession {
            time_zone: time_zone.into(),
            ..EvalSession::DEFAULT
        }
    }

    /// Pin this session's transaction and statement timestamps, both in
    /// microseconds since the Unix epoch, UTC.
    ///
    /// Both are supplied together because Postgres always has both, and a
    /// session that knew one but not the other could answer `now()` while
    /// refusing `statement_timestamp()`, which no real session ever does.
    pub fn at(mut self, transaction_timestamp: i64, statement_timestamp: i64) -> Self {
        self.transaction_timestamp = Some(transaction_timestamp);
        self.statement_timestamp = Some(statement_timestamp);
        self
    }

    /// The session's `TimeZone`, resolved against the IANA database.
    ///
    /// `""` (the [`DEFAULT`](Self::DEFAULT) session's zone) resolves to UTC.
    /// An unrecognised name is Postgres's `invalid_parameter_value`, with its
    /// message text.
    pub fn time_zone(&self) -> Result<Tz, ExecError> {
        if self.time_zone.is_empty() {
            // `Tz` has no `UTC` constant; the name is parsed like any
            // other, and "UTC" is always present in the IANA database.
            return "UTC"
                .parse::<Tz>()
                .map_err(|e| ExecError::Internal(format!("the IANA zone \"UTC\" did not parse: {e}")));
        }
        self.time_zone.parse::<Tz>().map_err(|_| {
            ExecError::TypeMismatch(format!(
                "invalid value for parameter \"TimeZone\": \"{}\"",
                self.time_zone
            ))
        })
    }

    /// The session's `TimeZone` GUC as written.
    pub fn time_zone_name(&self) -> &str {
        &self.time_zone
    }

    /// What `now()` / `transaction_timestamp()` / `CURRENT_TIMESTAMP` return,
    /// in microseconds since the Unix epoch.
    ///
    /// `None` when this evaluation has no transaction — see
    /// [`EvalSession::DEFAULT`] for why that is refused rather than faked.
    pub fn transaction_timestamp(&self) -> Option<i64> {
        self.transaction_timestamp
    }

    /// What `statement_timestamp()` returns, in microseconds since the Unix
    /// epoch. `None` under the same conditions as
    /// [`Self::transaction_timestamp`].
    pub fn statement_timestamp(&self) -> Option<i64> {
        self.statement_timestamp
    }
}

impl Default for EvalSession {
    fn default() -> Self {
        EvalSession::DEFAULT
    }
}

/// Evaluate a scalar expression against every row of `batch`, producing one
/// Arrow array of length `batch.num_rows()`.
///
/// Evaluates against [`EvalSession::DEFAULT`] — UTC, no clock. Use
/// [`eval_with`] to supply a real session; see [`EvalSession`] for why this
/// two-argument form still exists and what it is safe for.
pub fn eval(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    eval_with(expr, batch, &EvalSession::DEFAULT)
}

/// Evaluate a scalar expression against every row of `batch`, in `session`.
///
/// The session-dependent functions — `date_trunc`/`date_part` on
/// `timestamptz`, and the clock functions — read `session`; everything else
/// ignores it. It is threaded through *every* recursive step rather than only
/// the arms that consume it, because a session-dependent call can sit
/// arbitrarily deep inside one that is not (`CASE WHEN x THEN date_trunc(…)
/// END`), and a partially-threaded context is a context that is wrong exactly
/// where it is hardest to notice.
pub fn eval_with(
    expr: &Expr,
    batch: &RecordBatch,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    match expr {
        Expr::Column(col) => eval_column(col, batch),
        Expr::Literal(datum, ty) => eval_literal(datum, *ty, batch.num_rows()),
        Expr::Unary { op, arg } => eval_unary(*op, arg, batch, session),
        Expr::Binary { op, lhs, rhs } => eval_binary(*op, lhs, rhs, batch, session),
        Expr::Cast { arg, to, .. } => eval_cast(arg, *to, batch, session),
        Expr::Case {
            operand,
            whens,
            else_,
        } => eval_case(operand, whens, else_, batch, session),
        Expr::Coalesce(exprs) => eval_coalesce(exprs, batch, session),
        Expr::IsNull { arg, negated } => eval_is_null(arg, *negated, batch, session),
        Expr::BoolTest { arg, test } => {
            let a = eval_with(arg, batch, session)?;
            let a = require_bool(&a)?;
            Ok(Arc::new(eval_bool_test(a, *test)))
        }
        Expr::ScalarFn { func, args } => eval_scalar_fn(*func, args, batch, session),
        Expr::ArrayLit(elements) => eval_array_lit(elements, batch, session),
        Expr::Subscript { arg, indices } => eval_subscript(arg, indices, batch, session),
        Expr::DistinctFrom { lhs, rhs, negated } => eval_distinct_from(lhs, rhs, *negated, batch, session),
        Expr::InList { arg, list, negated } => eval_in_list(arg, list, *negated, batch, session),
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => eval_between(arg, low, high, *symmetric, *negated, batch, session),
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => eval_like(arg, pattern, escape, *case_insensitive, *negated, batch, session),

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

/// Read a column out of the batch by position.
///
/// `ColumnRef::relation` must be 0 — "my own input" — because a position is
/// all this function has: there is exactly one batch here, and no second
/// relation to resolve anything against. A non-zero `relation` means either
/// `opt::decorrelate`'s `OUTER_REF` (a correlated reference to an enclosing
/// query's row, which `build.rs` binds to a literal or evaluates per row
/// before eval ever sees it) or a join's right side (which `join.rs`'s
/// `flatten_filter` rewrites to a flat relation-0 position before eval ever
/// sees it). Either one arriving here unresolved is a bug in the layer
/// above — and, until this check existed, a silent one: the index was read
/// against the local batch regardless, so a correlated `x.id = outer.id`
/// evaluated as `x.id = x.id` and every row matched. That is a wrong
/// answer with the right shape, which is the worst kind, so it is refused
/// rather than guessed at.
fn eval_column(col: &ColumnRef, batch: &RecordBatch) -> Result<ArrayRef, ExecError> {
    if col.relation != 0 {
        return Err(ExecError::Internal(format!(
            "column {}.{} ('{}') reaches outside this operator's input — a correlated or \
             join-side reference that should have been resolved before scalar eval",
            col.relation, col.index, col.name
        )));
    }
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
        return eval_untyped_literal(&Expr::Literal(datum.clone(), ty), &DataType::Utf8, len);
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

fn eval_unary(op: OpId, arg: &Expr, batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    let v = eval_with(arg, batch, session)?;
    // NOT is a sentinel, exactly like AND_OP/OR_OP in eval_binary — it has no
    // pg_operator row, so it must be checked before catalog_op_name, which
    // would otherwise report it as an unknown oid. `boolean::not` already
    // does the right thing for NULL (NOT NULL is NULL, not TRUE): it copies
    // the null buffer across and only negates the underlying bits, never
    // manufacturing a value where there was none.
    if op == NOT_OP {
        // A bare `NULL` literal is typed `unknown`, which
        // `basin_pgtype::physical` materializes as `Utf8` — so `NOT NULL`
        // arrives here as a string array and `require_bool` rejects it, even
        // though Postgres resolves the untyped NULL under `NOT` to boolean
        // (`SELECT (NOT NULL) IS NULL` is `t`, measured live). Recognise the
        // LITERAL rather than loosening `require_bool` to accept any all-NULL
        // array: that broader rule would turn `NOT <text column>` — which
        // Postgres rejects outright ("argument of NOT must be type boolean")
        // — from an honest error into a wrong answer on the day every row of
        // that column happens to be NULL.
        if matches!(arg, Expr::Literal(PlanDatum::Null, _)) {
            return Ok(new_null_array(&DataType::Boolean, batch.num_rows()));
        }
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
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    if op == AND_OP {
        let l = eval_with(lhs, batch, session)?;
        let r = eval_with(rhs, batch, session)?;
        let l = require_bool(&l)?;
        let r = require_bool(&r)?;
        // Kleene, not the plain kernel: `NULL AND FALSE` must be FALSE, not
        // NULL — see the module docs' point 3.
        return Ok(Arc::new(
            boolean::and_kleene(l, r).map_err(|e| map_arrow(e, "AND"))?,
        ));
    }
    if op == OR_OP {
        let l = eval_with(lhs, batch, session)?;
        let r = eval_with(rhs, batch, session)?;
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

    let (l, r) = eval_operand_pair(lhs, rhs, batch, session)?;

    // Date/integer arithmetic, before the name-keyed table below: arrow has
    // no kernel for it (`numeric::add` refuses a Date32/Int32 pair with
    // "Invalid date arithmetic operation") and the operator NAME alone cannot
    // tell `date - integer` from `date - date`. See the OID block near the
    // top of this file.
    match op.0.get() {
        OID_OP_DATE_PLI => return date_offset_days(&l, &r, 1),
        OID_OP_INT_PL_DATE => return date_offset_days(&r, &l, 1),
        OID_OP_DATE_MII => return date_offset_days(&l, &r, -1),
        OID_OP_DATE_MI_DATE => return date_diff_days(&l, &r),
        _ => {}
    }

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
        // `^` is EXPONENTIATION in Postgres, not the bitwise XOR the same
        // spelling means in C, Rust, Python and SQLite: `2 ^ 10` is 1024,
        // confirmed live. It is also LEFT-associative, unlike the right
        // associativity `**`/`^` has in most languages — `2 ^ 3 ^ 2` is
        // `(2 ^ 3) ^ 2` = 64 live, not 512. That association is decided by
        // the grammar before this file sees anything: `basin-plan` parses
        // with `pg_query`, which is Postgres's own grammar, so the left
        // operand of the outer `^` arrives already being `2 ^ 3`. Nothing
        // here has to (or could) encode it.
        "^" => eval_exponent(&l, &r),
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
    session: &EvalSession,
) -> Result<(ArrayRef, ArrayRef), ExecError> {
    // Postgres resolves an UNTYPED literal from the other operand: in
    // `SELECT 'x' = col`, the literal is `unknown` until the column types it.
    // Lowering marks such literals `PgType::UNKNOWN` (oid 705) faithfully, and
    // nothing resolved them, so `physical()` correctly refused and the query
    // fell back. Both types are known here, so this is where it costs nothing:
    // evaluate the typed side first, then materialise the literal at its type.
    let (l, r) = match (is_unknown_literal(lhs), is_unknown_literal(rhs)) {
        (true, false) => {
            let r = eval_with(rhs, batch, session)?;
            let l = eval_untyped_literal(lhs, r.data_type(), batch.num_rows())?;
            (l, r)
        }
        (false, true) => {
            let l = eval_with(lhs, batch, session)?;
            let r = eval_untyped_literal(rhs, l.data_type(), batch.num_rows())?;
            (l, r)
        }
        // Both untyped is `'a' = 'b'`, which Postgres resolves to text.
        (true, true) => (
            eval_untyped_literal(lhs, &arrow_schema::DataType::Utf8, batch.num_rows())?,
            eval_untyped_literal(rhs, &arrow_schema::DataType::Utf8, batch.num_rows())?,
        ),
        (false, false) => (eval_with(lhs, batch, session)?, eval_with(rhs, batch, session)?),
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
    session: &EvalSession,
) -> Result<(ArrayRef, ArrayRef), ExecError> {
    let r = if is_unknown_literal(rhs) {
        eval_untyped_literal(rhs, lhs.data_type(), batch.num_rows())?
    } else {
        eval_with(rhs, batch, session)?
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

fn eval_cast(arg: &Expr, to: PgType, batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    let v = eval_with(arg, batch, session)?;
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
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    if whens.is_empty() {
        return match else_ {
            Some(e) => eval_with(e, batch, session),
            None => Err(ExecError::Internal(
                "CASE with no WHEN and no ELSE — a planner bug, not user error".to_string(),
            )),
        };
    }

    // `CASE operand WHEN v THEN …` is Postgres sugar for
    // `CASE WHEN operand = v THEN …`; evaluate `operand` once up front.
    let operand_arr = match operand {
        Some(o) => Some(eval_with(o, batch, session)?),
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
    let mut branch_arrays = eval_branches_unified(&branch_exprs, batch, session)?;
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
                // The same resolution an ordinary `=` gets, for the same
                // reason: `CASE id WHEN 1 THEN …` over a `bigint` column is
                // int8-vs-int4 to arrow's kernel, which demands identical
                // types, and `CASE name WHEN 'a' THEN …` puts an `unknown`
                // literal against `text`. Postgres widens and resolves both
                // — `CASE x WHEN v` IS `CASE WHEN x = v` to it — so routing
                // through [`eval_operand_against`] (rather than a bare
                // `cmp::eq`) is what keeps the two spellings the same query
                // here too. Without it the simple form fell back on data the
                // searched form served.
                let (o, v) = eval_operand_against(o.clone(), cond_expr, batch, session)?;
                cmp::eq(&o, &v).map_err(|e| map_arrow(e, "CASE"))?
            }
            None => {
                let c = eval_with(cond_expr, batch, session)?;
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
fn eval_branches_unified(exprs: &[&Expr], batch: &RecordBatch,
    session: &EvalSession) -> Result<Vec<ArrayRef>, ExecError> {
    let len = batch.num_rows();

    let mut typed: Vec<Option<ArrayRef>> = Vec::with_capacity(exprs.len());
    for e in exprs {
        typed.push(if is_unknown_literal(e) {
            None
        } else {
            Some(eval_with(e, batch, session)?)
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
/// `ARRAY[e1, e2, ...]` — one list value per row, whose k entries are the k
/// element expressions evaluated at that row.
///
/// The element expressions are arrays *down* the batch (`e1` over every row),
/// but a list value is a run *across* the elements at one row, so this is a
/// transpose. It is done with a single [`interleave`] rather than a per-row
/// builder loop precisely so it stays type-generic: an `ARRAY[...]` of any
/// element type — text, timestamps, decimals — goes through the same code as
/// integers, with no per-type match to fall out of date.
///
/// Element types are unified by the same [`eval_branches_unified`] that CASE
/// and COALESCE use, and for the same reason: `ARRAY['x','y']` is a run of
/// `unknown` literals that Postgres resolves to `text` (a live PostgreSQL
/// 18.2 agrees: `pg_typeof(ARRAY['x','y'])` is `text[]`), and `ARRAY[1, 2]`
/// against a wider sibling needs the widening. A list whose entries were
/// materialized at *different* types could not be one Arrow child array at
/// all, so this is a precondition, not a nicety.
///
/// `NULL` elements are ordinary null slots in the child array — an array
/// *containing* NULL, which is not the same as a NULL array, and Arrow's
/// separate child-validity and list-validity buffers keep them distinct.
fn eval_array_lit(
    elements: &[Expr],
    batch: &RecordBatch,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    // `ARRAY[]` has no element to take a type from. `basin_plan::schema`
    // already declines to type it, so this is unreachable from a planned
    // query; refusing (rather than inventing `text[]` here, out of step with
    // the type the plan never produced) keeps the two ends honest.
    if elements.is_empty() {
        return Err(ExecError::Internal(
            "empty ARRAY[] has no element type — the planner declines to type it, so it \
             should never reach eval"
                .to_string(),
        ));
    }

    let refs: Vec<&Expr> = elements.iter().collect();
    let columns = eval_branches_unified(&refs, batch, session)?;
    let elem_type = columns[0].data_type().clone();

    let k = columns.len();
    let rows = batch.num_rows();
    // Row-major: row 0's k entries, then row 1's, ... Each pair is
    // (which element expression, which row) — the transpose itself.
    let indices: Vec<(usize, usize)> = (0..rows)
        .flat_map(|row| (0..k).map(move |elem| (elem, row)))
        .collect();
    let borrowed: Vec<&dyn Array> = columns.iter().map(|c| c.as_ref()).collect();
    let values =
        interleave::interleave(&borrowed, &indices).map_err(|e| map_arrow(e, "ARRAY[...]"))?;

    // Every list is exactly k long, so the offsets are a fixed stride. The
    // field is `("item", nullable)` because that is what
    // `basin_pgtype::physical` produces for an array type — a different name
    // or nullability here would be a different Arrow type to the schema the
    // plan declared, and the batch would be rejected downstream.
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(k, rows));
    let field = Arc::new(Field::new("item", elem_type, true));
    Ok(Arc::new(
        ListArray::try_new(field, offsets, values, None)
            .map_err(|e| map_arrow(e, "ARRAY[...]"))?,
    ))
}

/// Downcast to the one array layout `basin_pgtype::physical` produces for a
/// Postgres array type. Anything else reaching here is a planner bug, so it
/// is reported rather than improvised around.
fn require_list<'a>(arr: &'a ArrayRef, what: &str) -> Result<&'a ListArray, ExecError> {
    arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        ExecError::TypeMismatch(format!(
            "{what} expects an array, found {:?}",
            arr.data_type()
        ))
    })
}

/// `array_length(a, d)` — the length of array `a` along dimension `d`.
///
/// Postgres answers NULL — not 0, and not an error — in four distinct
/// situations. All six rows below were measured live on PostgreSQL 18.2:
///
/// | call                               | result |
/// |------------------------------------|--------|
/// | `array_length(ARRAY['x','y'], 1)`  | 2      |
/// | `array_length(ARRAY['x','y'], 2)`  | NULL — no such dimension |
/// | `array_length(ARRAY['x','y'], 0)`  | NULL — dimensions are 1-based |
/// | `array_length(ARRAY[]::text[], 1)` | NULL — an EMPTY array has no dimensions at all, so not even dimension 1 exists |
/// | `array_length(NULL::text[], 1)`    | NULL — strict in the array |
/// | `array_length(ARRAY[1,NULL,3], 1)` | 3 — a NULL *element* still occupies a slot |
///
/// The empty-array row is the one worth stating explicitly: `0` is the
/// natural reading of "length" there and it is the wrong answer.
///
/// Only dimension 1 can ever be non-NULL here because Arrow's `ListArray` is
/// the physical form of a one-dimensional Postgres array; a genuinely
/// multi-dimensional `int[][]` has no physical type in `basin_pgtype` yet, so
/// no such value can reach this function.
fn eval_array_length(arr: &ArrayRef, dim: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_length")?;
    let dims = cast::cast(dim, &DataType::Int64).map_err(|e| map_arrow(e, "array_length"))?;
    let dims = dims
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("just cast to Int64");
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| {
            if list.is_null(i) || dims.is_null(i) || dims.value(i) != 1 {
                return None;
            }
            let len = offsets[i + 1] - offsets[i];
            // An empty array has NO dimensions — see the table above.
            if len == 0 {
                None
            } else {
                Some(len)
            }
        })
        .collect();
    Ok(Arc::new(out))
}

// ─── The array family ───────────────────────────────────────────────────────
//
// Twelve `pg_catalog` functions that Basin answered today only through
// DataFusion's `datafusion-functions-nested` (see the `OID_ARRAY_*` block).
// They share three properties that shape everything below:
//
//  1. **They are polymorphic, and the implementation must be too.** Postgres
//     declares them on `anycompatiblearray`/`anyarray`, with one physical oid
//     for every element type. So none of these functions may match on the
//     element type: elements are *moved* with `take`/`interleave` and
//     *compared* with the `not_distinct` kernel, both of which are generic
//     over Arrow's type system. Adding `int8[]`/`numeric[]` rows to
//     `basin_pgtype::func::FUNCS` therefore needs no change here.
//
//  2. **NULL has three distinct meanings and they are not interchangeable.**
//     A NULL *array*, a NULL *element inside* an array, and an *empty* array
//     are three different things, and Postgres answers differently for each.
//     Every one of the rules below was measured on the live PostgreSQL 18.2,
//     and several are the opposite of the natural guess:
//
//     | call | result | the wrong guess |
//     |---|---|---|
//     | `array_append(NULL::int[], 1)` | `{1}` | NULL — the function is NOT strict |
//     | `array_append(NULL::int[], NULL)` | `{NULL}` | NULL — a one-element array *containing* NULL |
//     | `array_cat(NULL::int[], ARRAY[1])` | `{1}` | NULL |
//     | `array_cat(NULL::int[], NULL::int[])` | NULL | `{}` |
//     | `array_position(ARRAY[1,NULL,3], NULL)` | `2` | NULL — a NULL element IS found |
//     | `array_positions(ARRAY[1,2], 9)` | `{}` | NULL — empty array, not NULL |
//     | `array_position(ARRAY[1,2], 9)` | NULL | `{}`/0 — the singular form DOES answer NULL |
//     | `array_remove(ARRAY[1,NULL,3], NULL)` | `{1,3}` | `{1,NULL,3}` — NULLs ARE removed |
//     | `array_ndims(ARRAY[]::int[])` | NULL | 1 — an empty array has ZERO dimensions |
//     | `cardinality(ARRAY[]::int[])` | `0` | NULL — and it disagrees with `array_length`, which IS NULL |
//     | `array_to_string(ARRAY['a',NULL,'c'], ',')` | `a,c` | `a,,c` — NULLs are DROPPED |
//     | `array_sort(ARRAY[3,1,NULL,2], true)` | `{NULL,3,2,1}` | `{3,2,1,NULL}` — NULLS FIRST is the DESC default |
//
//     The `array_position`/`array_positions` row and the
//     `array_length`/`cardinality` row are the two that a shared helper gets
//     wrong, which is why neither pair shares one here.
//
//  3. **Element equality is `IS NOT DISTINCT FROM`, not `=`.** That is what
//     makes `array_position(…, NULL)` find a NULL and `array_remove(…, NULL)`
//     remove one. Arrow's `cmp::not_distinct` kernel is exactly this
//     predicate and returns a null-free `BooleanArray`, so none of the
//     search/replace functions below has a three-valued branch to get wrong.
//
// Postgres arrays are also multi-dimensional and Arrow's `ListArray` is not:
// `basin_pgtype::physical` maps `int4[]` to a one-dimensional `List`, so no
// multi-dimensional value can reach any of these. That is a real gap
// (`array_ndims` can only ever answer 1 or NULL here), and it is the reason
// the differential harness passes multi-dimensional literals as
// "unrepresentable" rather than as a case Basin could fail.

/// Gather a list column's elements into one compact child array in row order,
/// plus each row's `(start, len)` window inside it.
///
/// Exists because `ListArray::values()` is the *whole* child buffer, which for
/// a sliced or out-of-order list is neither compact nor in row order — every
/// function below wants "row `i`'s elements" as a contiguous run, and building
/// that once is both simpler and cheaper than re-deriving it from
/// `value_offsets()` in each.
///
/// A NULL row contributes no elements and gets a zero-length window, so the
/// caller decides what a NULL array means for its own function (they differ —
/// see the table above) rather than inheriting a decision made here.
fn flatten_list(
    list: &ListArray,
    what: &'static str,
) -> Result<(ArrayRef, Vec<(usize, usize)>), ExecError> {
    let offsets = list.value_offsets();
    let mut picks: Vec<u32> = Vec::new();
    let mut spans: Vec<(usize, usize)> = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        let start = picks.len();
        if !list.is_null(i) {
            for k in offsets[i]..offsets[i + 1] {
                picks.push(u32::try_from(k).expect("list offset fits u32"));
            }
        }
        spans.push((start, picks.len() - start));
    }
    let values = take::take(list.values().as_ref(), &UInt32Array::from(picks), None)
        .map_err(|e| map_arrow(e, what))?;
    Ok((values, spans))
}

/// Repeat a one-value-per-row array so it lines up element-for-element with a
/// [`flatten_list`] child array — the shape `cmp::not_distinct` needs to
/// compare every element of row `i` against row `i`'s search value.
fn expand_per_row(
    per_row: &ArrayRef,
    spans: &[(usize, usize)],
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let mut idx: Vec<u32> = Vec::new();
    for (row, (_, len)) in spans.iter().enumerate() {
        let row = u32::try_from(row).expect("row index fits u32");
        idx.extend(std::iter::repeat_n(row, *len));
    }
    take::take(per_row.as_ref(), &UInt32Array::from(idx), None).map_err(|e| map_arrow(e, what))
}

/// `IS NOT DISTINCT FROM`, element by element, between a flattened child array
/// and the per-row search value expanded to match it. Never NULL — that is the
/// whole point of `not_distinct` over `eq`, and it is why
/// `array_position(ARRAY[1,NULL,3], NULL)` is `2` rather than NULL.
fn elements_not_distinct(
    flat: &ArrayRef,
    needle: &ArrayRef,
    spans: &[(usize, usize)],
    what: &'static str,
) -> Result<BooleanArray, ExecError> {
    let expanded = expand_per_row(needle, spans, what)?;
    cmp::not_distinct(flat, &expanded).map_err(|e| map_arrow(e, what))
}

/// Build a `ListArray` from element sources picked by `indices`, cut into rows
/// by `lengths`, with `validity[i] == false` making row `i` a NULL array.
///
/// The output field is `("item", nullable)` because that is what
/// `basin_pgtype::physical` produces for a Postgres array type — a different
/// name or nullability would be a different Arrow type from the one the plan
/// declared, and the batch would be rejected downstream. Same reason
/// [`eval_array_lit`] spells it that way.
fn assemble_list(
    elem_type: &DataType,
    sources: &[&dyn Array],
    indices: &[(usize, usize)],
    lengths: &[usize],
    validity: &[bool],
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let values: ArrayRef = if indices.is_empty() {
        new_empty_array(elem_type)
    } else {
        interleave::interleave(sources, indices).map_err(|e| map_arrow(e, what))?
    };
    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
    let field = Arc::new(Field::new("item", elem_type.clone(), true));
    let nulls = if validity.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(validity.to_vec()))
    };
    Ok(Arc::new(
        ListArray::try_new(field, offsets, values, nulls).map_err(|e| map_arrow(e, what))?,
    ))
}

/// `array_append(a, e)` and `array_prepend(e, a)` — the same function with the
/// element on the other end.
///
/// Neither is strict, and that is the whole subtlety. Measured live:
/// `array_append(NULL::int[], 1)` is `{1}`, not NULL — a NULL array behaves as
/// an empty one — and `array_append(NULL::int[], NULL)` is `{NULL}`, a
/// one-element array whose single element is NULL. So the result is **never**
/// a NULL array, which is why `validity` below is unconditionally true.
fn eval_array_add_element(
    arr: &ArrayRef,
    elem: &ArrayRef,
    at_front: bool,
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, what)?;
    let (flat, spans) = flatten_list(list, what)?;
    if flat.data_type() != elem.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "{what} expects the element type {:?} of its array, found {:?}",
            flat.data_type(),
            elem.data_type()
        )));
    }
    let mut indices = Vec::with_capacity(flat.len() + spans.len());
    let mut lengths = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        if at_front {
            indices.push((1usize, row));
        }
        indices.extend((*start..start + len).map(|k| (0usize, k)));
        if !at_front {
            indices.push((1usize, row));
        }
        lengths.push(len + 1);
    }
    let validity = vec![true; spans.len()];
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref(), elem.as_ref()],
        &indices,
        &lengths,
        &validity,
        what,
    )
}

/// `array_cat(l, r)` — concatenation.
///
/// NULL is absorbed on either side but not on both: `array_cat(NULL, {1})` is
/// `{1}` and `array_cat({1}, NULL)` is `{1}`, while
/// `array_cat(NULL::int[], NULL::int[])` is NULL, not `{}`. All three measured
/// live. The result is a NULL array exactly when *both* inputs are.
fn eval_array_cat(l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let ll = require_list(l, "array_cat")?;
    let rl = require_list(r, "array_cat")?;
    let (lflat, lspans) = flatten_list(ll, "array_cat")?;
    let (rflat, rspans) = flatten_list(rl, "array_cat")?;
    if lflat.data_type() != rflat.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "array_cat expects two arrays of the same element type, found {:?} and {:?}",
            lflat.data_type(),
            rflat.data_type()
        )));
    }
    let mut indices = Vec::with_capacity(lflat.len() + rflat.len());
    let mut lengths = Vec::with_capacity(lspans.len());
    let mut validity = Vec::with_capacity(lspans.len());
    for row in 0..ll.len() {
        let (ls, ln) = lspans[row];
        let (rs, rn) = rspans[row];
        indices.extend((ls..ls + ln).map(|k| (0usize, k)));
        indices.extend((rs..rs + rn).map(|k| (1usize, k)));
        lengths.push(ln + rn);
        validity.push(!(ll.is_null(row) && rl.is_null(row)));
    }
    assemble_list(
        &lflat.data_type().clone(),
        &[lflat.as_ref(), rflat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_cat",
    )
}

/// `array_remove(a, e)` — every element not distinct from `e`, dropped.
///
/// `array_remove(ARRAY[1,NULL,3], NULL)` is `{1,3}`, measured live: a NULL
/// search value removes the NULL elements rather than matching nothing. A NULL
/// *array* still answers NULL.
fn eval_array_remove(arr: &ArrayRef, elem: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_remove")?;
    let (flat, spans) = flatten_list(list, "array_remove")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_remove")?;
    let mut indices = Vec::new();
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        let before = indices.len();
        indices.extend((*start..start + len).filter(|k| !matches.value(*k)).map(|k| (0usize, k)));
        lengths.push(indices.len() - before);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_remove",
    )
}

/// `array_replace(a, from, to)` — every element not distinct from `from`,
/// replaced by `to`.
///
/// Both `from` and `to` may be NULL and both are meaningful:
/// `array_replace(ARRAY[1,NULL,3], NULL, 9)` is `{1,9,3}` and
/// `array_replace(ARRAY[1,2,3], 2, NULL)` is `{1,NULL,3}`, measured live.
fn eval_array_replace(
    arr: &ArrayRef,
    from: &ArrayRef,
    to: &ArrayRef,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_replace")?;
    let (flat, spans) = flatten_list(list, "array_replace")?;
    if flat.data_type() != to.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "array_replace expects the element type {:?} of its array, found {:?}",
            flat.data_type(),
            to.data_type()
        )));
    }
    let matches = elements_not_distinct(&flat, from, &spans, "array_replace")?;
    let mut indices = Vec::with_capacity(flat.len());
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        for k in *start..start + len {
            if matches.value(k) {
                indices.push((1usize, row));
            } else {
                indices.push((0usize, k));
            }
        }
        lengths.push(*len);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref(), to.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_replace",
    )
}

/// `array_position(a, e)` and `array_position(a, e, start)` — the 1-based
/// index of the first element not distinct from `e`, or NULL if there is none.
///
/// Measured live, and each row is a case a plausible implementation gets
/// wrong:
///
/// | call | result |
/// |---|---|
/// | `array_position(ARRAY[1,NULL,3], NULL)` | 2 — a NULL element IS matched |
/// | `array_position(ARRAY[1,2], 9)` | NULL — absent is NULL, not 0 |
/// | `array_position(NULL::int[], 1)` | NULL |
/// | `array_position(ARRAY[]::int[], 1)` | NULL |
/// | `array_position(ARRAY[1,2,1], 1, 0)` | 1 — a start below 1 clamps |
/// | `array_position(ARRAY[1,2,1], 1, -5)` | 1 — so does a negative one |
/// | `array_position(ARRAY[1,2,1], 1, 9)` | NULL — past the end finds nothing |
/// | `array_position(ARRAY[1,2,1], 1, NULL)` | **ERROR**: `initial position must not be null` |
///
/// The last one is the only input any of these functions *rejects*, and it
/// rejects it rather than returning NULL even though the function is not
/// strict.
fn eval_array_position(
    arr: &ArrayRef,
    elem: &ArrayRef,
    start: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_position")?;
    let (flat, spans) = flatten_list(list, "array_position")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_position")?;
    let from = match start {
        None => None,
        Some(s) => {
            let s = cast::cast(s, &DataType::Int64).map_err(|e| map_arrow(e, "array_position"))?;
            let s = s
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("just cast to Int64")
                .clone();
            if s.null_count() > 0 {
                return Err(ExecError::TypeMismatch(
                    "initial position must not be null".to_string(),
                ));
            }
            Some(s)
        }
    };
    let out: Int32Array = spans
        .iter()
        .enumerate()
        .map(|(row, (start_off, len))| {
            if list.is_null(row) {
                return None;
            }
            // 1-based, clamped: a start below 1 means "from the beginning",
            // not an error and not an empty search.
            let skip = from
                .as_ref()
                .map(|s| (s.value(row) - 1).max(0))
                .unwrap_or(0);
            let skip = usize::try_from(skip).unwrap_or(usize::MAX);
            if skip >= *len {
                return None;
            }
            (start_off + skip..start_off + len)
                .find(|k| matches.value(*k))
                .map(|k| i32::try_from(k - start_off + 1).expect("array position fits i32"))
        })
        .collect();
    Ok(Arc::new(out))
}

/// `array_positions(a, e)` — *all* the 1-based indices, as an `int4[]`.
///
/// It does not share [`eval_array_position`]'s "absent is NULL" rule:
/// `array_positions(ARRAY[1,2], 9)` is `{}`, an empty array, while the
/// singular form is NULL. A NULL array is still NULL. Both measured live, and
/// this disagreement is why the two are separate functions here.
fn eval_array_positions(arr: &ArrayRef, elem: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_positions")?;
    let (flat, spans) = flatten_list(list, "array_positions")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_positions")?;
    let mut values: Vec<i32> = Vec::new();
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        let before = values.len();
        for k in *start..start + len {
            if matches.value(k) {
                values.push(i32::try_from(k - start + 1).expect("array position fits i32"));
            }
        }
        lengths.push(values.len() - before);
        validity.push(!list.is_null(row));
    }
    let child = Int32Array::from(values);
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let nulls = if validity.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    Ok(Arc::new(
        ListArray::try_new(field, offsets, Arc::new(child), nulls)
            .map_err(|e| map_arrow(e, "array_positions"))?,
    ))
}

/// `cardinality(a)` — the total number of elements.
///
/// Deliberately NOT sharing [`eval_array_length`]: `cardinality(ARRAY[]::int[])`
/// is **0** while `array_length(ARRAY[]::int[], 1)` is **NULL**, both measured
/// live. The two functions disagree on exactly the input a shared helper would
/// unify. NULL in, NULL out.
///
/// Postgres's `cardinality` counts elements across *all* dimensions; Arrow's
/// `ListArray` is one-dimensional, so here it is the row's length.
fn eval_cardinality(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "cardinality")?;
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| (!list.is_null(i)).then(|| offsets[i + 1] - offsets[i]))
        .collect();
    Ok(Arc::new(out))
}

/// `array_ndims(a)` — the number of dimensions.
///
/// `array_ndims(ARRAY[]::int[])` is **NULL**, not 1 and not 0: an empty array
/// has no dimensions at all, the same rule that makes `array_length` NULL for
/// it (measured live). A NULL array is NULL.
///
/// Only ever 1 or NULL here, because `basin_pgtype::physical` has no
/// multi-dimensional array type — see this family's header comment.
fn eval_array_ndims(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_ndims")?;
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| {
            if list.is_null(i) || offsets[i + 1] == offsets[i] {
                None
            } else {
                Some(1)
            }
        })
        .collect();
    Ok(Arc::new(out))
}

/// `array_reverse(a)`.
fn eval_array_reverse(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_reverse")?;
    let (flat, spans) = flatten_list(list, "array_reverse")?;
    let mut indices = Vec::with_capacity(flat.len());
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        indices.extend((*start..start + len).rev().map(|k| (0usize, k)));
        lengths.push(*len);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_reverse",
    )
}

/// `array_sort(a)`, `array_sort(a, descending)` and
/// `array_sort(a, descending, nulls_first)`.
///
/// The default for `nulls_first` is **`descending` itself**, not `false`:
/// measured live, `array_sort(ARRAY[3,1,NULL,2])` is `{1,2,3,NULL}` but
/// `array_sort(ARRAY[3,1,NULL,2], true)` is `{NULL,3,2,1}`. That is Postgres's
/// ordinary `ORDER BY` rule (NULLS LAST for ASC, NULLS FIRST for DESC) applied
/// to an array, and getting it wrong puts the NULL at the wrong end of every
/// descending sort. The three-argument form overrides it and was checked in
/// all four combinations.
///
/// All three overloads are strict, so a NULL `descending` or `nulls_first`
/// makes the whole call NULL rather than falling back to a default —
/// `array_sort(ARRAY[3,1,NULL,2], NULL::bool)` is NULL, measured live.
///
/// > **Collation.** Postgres orders text by the database collation
/// > (`en_US.UTF-8` on the server this was measured against, which sorts
/// > `{1,a,A,ä,b,B}`), while Arrow's sort kernel orders by Unicode code point
/// > (`{1,A,B,a,b,ä}`). So `array_sort` on a text array diverges from
/// > PostgreSQL under any collation but `C`. This is not a new divergence
/// > introduced here: `basin-exec` has no collation support anywhere, so
/// > `ORDER BY textcol` already answers in code-point order. It is recorded
/// > here rather than fixed here, because fixing it is a crate-wide collation
/// > facility, not an argument to this function.
fn eval_array_sort(
    arr: &ArrayRef,
    descending: Option<&ArrayRef>,
    nulls_first: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_sort")?;
    let (flat, spans) = flatten_list(list, "array_sort")?;
    let desc = match descending {
        None => None,
        Some(d) => Some(downcast_array::<BooleanArray>(d, "boolean")?.clone()),
    };
    let nf = match nulls_first {
        None => None,
        Some(n) => Some(downcast_array::<BooleanArray>(n, "boolean")?.clone()),
    };

    let mut indices = Vec::with_capacity(flat.len());
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        // Strict in every argument: a NULL flag makes the row NULL, it does
        // not fall back to the default.
        let flags_null = desc.as_ref().is_some_and(|d| d.is_null(row))
            || nf.as_ref().is_some_and(|n| n.is_null(row));
        if list.is_null(row) || flags_null {
            lengths.push(0);
            validity.push(false);
            continue;
        }
        let descending = desc.as_ref().is_some_and(|d| d.value(row));
        let nulls_first = nf
            .as_ref()
            .map(|n| n.value(row))
            // The default that is easy to get wrong — see the doc comment.
            .unwrap_or(descending);
        let options = SortOptions {
            descending,
            nulls_first,
        };
        let slice = flat.slice(*start, *len);
        let order = sort_to_indices(slice.as_ref(), Some(options), None)
            .map_err(|e| map_arrow(e, "array_sort"))?;
        indices.extend((0..order.len()).map(|k| (0usize, start + order.value(k) as usize)));
        lengths.push(*len);
        validity.push(true);
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_sort",
    )
}

/// `array_to_string(a, delim)` and `array_to_string(a, delim, null_string)`.
///
/// Measured live, and the NULL rules differ between the two overloads in a way
/// the shared body below has to keep straight:
///
/// | call | result |
/// |---|---|
/// | `array_to_string(ARRAY['a',NULL,'c'], ',')` | `a,c` — NULLs are DROPPED, not rendered as empty |
/// | `array_to_string(ARRAY['a',NULL,'c'], ',', 'X')` | `a,X,c` |
/// | `array_to_string(ARRAY['a',NULL,'c'], ',', NULL)` | `a,c` — a NULL `null_string` drops them again |
/// | `array_to_string(ARRAY[]::text[], ',')` | `` — the empty string, not NULL |
/// | `array_to_string(NULL::text[], ',')` | NULL |
/// | `array_to_string(ARRAY[1,2], NULL)` | NULL — a NULL delimiter is NULL even for the non-strict 3-argument form |
///
/// Elements are rendered by casting the child array to `Utf8`, so the same
/// code serves `int4[]` and `text[]` (the two element types
/// `basin_pgtype::func::FUNCS` monomorphizes this family at) without a
/// per-type match.
fn eval_array_to_string(
    arr: &ArrayRef,
    delim: &ArrayRef,
    null_string: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_to_string")?;
    let (flat, spans) = flatten_list(list, "array_to_string")?;
    let rendered =
        cast::cast(&flat, &DataType::Utf8).map_err(|e| map_arrow(e, "array_to_string"))?;
    let rendered = downcast_array::<StringArray>(&rendered, "text")?;
    let delim = downcast_array::<StringArray>(delim, "text")?;
    let null_string = match null_string {
        None => None,
        Some(n) => Some(downcast_array::<StringArray>(n, "text")?.clone()),
    };

    let out: StringArray = spans
        .iter()
        .enumerate()
        .map(|(row, (start, len))| {
            if list.is_null(row) || delim.is_null(row) {
                return None;
            }
            let sep = delim.value(row);
            let replacement = null_string
                .as_ref()
                .and_then(|n| n.is_valid(row).then(|| n.value(row)));
            let mut buf = String::new();
            let mut first = true;
            for k in *start..start + len {
                let piece = if rendered.is_null(k) {
                    match replacement {
                        // A NULL element with no replacement is dropped
                        // entirely — separator and all.
                        None => continue,
                        Some(r) => r,
                    }
                } else {
                    rendered.value(k)
                };
                if !first {
                    buf.push_str(sep);
                }
                buf.push_str(piece);
                first = false;
            }
            Some(buf)
        })
        .collect();
    Ok(Arc::new(out))
}

/// `string_to_array(s, delim)` and `string_to_array(s, delim, null_string)`.
///
/// The complement of [`eval_array_to_string`], and its NULL rules are stranger
/// still. All measured live:
///
/// | call | result |
/// |---|---|
/// | `string_to_array('a,b,c', NULL)` | `{a,",",b,",",c}` — a NULL delimiter splits into single CHARACTERS |
/// | `string_to_array('a,b,c', '')` | `{"a,b,c"}` — an empty delimiter does not split at all |
/// | `string_to_array('', ',')` | `{}` — an empty array, NOT `{""}` |
/// | `string_to_array('', '')` | `{}` |
/// | `string_to_array(NULL, ',')` | NULL |
/// | `string_to_array(',a,', ',')` | `{"",a,""}` — leading/trailing empties ARE kept |
/// | `string_to_array('a,,c', ',', '')` | `{a,NULL,c}` — `null_string` turns matching pieces into NULL |
///
/// The NULL-delimiter rule is the one worth stating twice: it is not "return
/// NULL" and it is not "do not split", it is "split into characters", and
/// characters means `char`s, not bytes — `string_to_array('héllo', NULL)` is
/// five elements, not six.
fn eval_string_to_array(
    s: &ArrayRef,
    delim: &ArrayRef,
    null_string: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let delim = downcast_array::<StringArray>(delim, "text")?;
    let null_string = match null_string {
        None => None,
        Some(n) => Some(downcast_array::<StringArray>(n, "text")?.clone()),
    };

    let mut pieces: Vec<Option<String>> = Vec::new();
    let mut lengths = Vec::with_capacity(s.len());
    let mut validity = Vec::with_capacity(s.len());
    for row in 0..s.len() {
        if s.is_null(row) {
            lengths.push(0);
            validity.push(false);
            continue;
        }
        let text = s.value(row);
        let replacement = null_string
            .as_ref()
            .and_then(|n| n.is_valid(row).then(|| n.value(row)));
        let split: Vec<String> = if text.is_empty() {
            // The empty input is an empty ARRAY, not a one-element array
            // holding the empty string — for every delimiter.
            Vec::new()
        } else if delim.is_null(row) {
            text.chars().map(|c| c.to_string()).collect()
        } else if delim.value(row).is_empty() {
            vec![text.to_string()]
        } else {
            text.split(delim.value(row)).map(str::to_string).collect()
        };
        lengths.push(split.len());
        validity.push(true);
        pieces.extend(split.into_iter().map(|p| match replacement {
            Some(r) if p == r => None,
            _ => Some(p),
        }));
    }
    let child = StringArray::from(pieces);
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let nulls = if validity.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    Ok(Arc::new(
        ListArray::try_new(field, offsets, Arc::new(child), nulls)
            .map_err(|e| map_arrow(e, "string_to_array"))?,
    ))
}

/// `a[i]` — a single array subscript.
///
/// Postgres subscripts are 1-based and, unlike almost everything else in the
/// language, do NOT error when they miss: `(ARRAY['x','y'])[9]`,
/// `(ARRAY['x','y'])[0]` and `(ARRAY['x','y'])[-1]` are all NULL, measured
/// live, and so is a subscript of a NULL array. That is why the whole thing
/// is expressed as a `take` with a null-carrying index vector rather than as
/// a bounds check that raises.
///
/// Only the single-index form is built. `a[i:j]` (a slice) and chained
/// subscripts fall through to the caller's catch-all, so the bridge keeps
/// falling back for them rather than answering a shape this does not
/// implement.
fn eval_subscript(
    arg: &Expr,
    indices: &[basin_plan::Subscript],
    batch: &RecordBatch,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let [basin_plan::Subscript::Index(idx_expr)] = indices else {
        return Err(ExecError::Internal(format!(
            "array subscript {indices:?} is not implemented in eval yet"
        )));
    };
    let arr = eval_with(arg, batch, session)?;
    let list = require_list(&arr, "array subscript")?;
    let idx = eval_with(idx_expr, batch, session)?;
    let idx = cast::cast(&idx, &DataType::Int64).map_err(|e| map_arrow(e, "array subscript"))?;
    let idx = idx
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("just cast to Int64");

    // `value_offsets()` is already relative to this array's own window, and
    // `values()` is the full child buffer those offsets index into, so a
    // sliced ListArray needs no extra adjustment here.
    let offsets = list.value_offsets();
    let picks: UInt32Array = (0..list.len())
        .map(|i| {
            if list.is_null(i) || idx.is_null(i) {
                return None;
            }
            let one_based = idx.value(i);
            let len = i64::from(offsets[i + 1] - offsets[i]);
            if one_based < 1 || one_based > len {
                return None;
            }
            Some(u32::try_from(i64::from(offsets[i]) + one_based - 1).expect("offset fits u32"))
        })
        .collect();
    take::take(list.values().as_ref(), &picks, None)
        .map_err(|e| map_arrow(e, "array subscript"))
}

/// `date + integer` / `date - integer` — Postgres's `date_pli`/`date_mii`
/// (and `integer + date`, `integer_pl_date`), which have no Arrow kernel:
/// `numeric::add` refuses a `Date32`/`Int32` pair outright with "Invalid date
/// arithmetic operation". `Date32` is a count of days since the epoch, so all
/// of it is integer arithmetic on that day number.
///
/// `sign` is `1` for `+` and `-1` for `-`. Measured live:
/// `'2024-01-15'::DATE + 1` is `2024-01-16` and `- 1` is `2024-01-14`, both
/// of type `date` — not `timestamp`, which is what `date + interval` yields.
fn date_offset_days(date: &ArrayRef, days: &ArrayRef, sign: i32) -> Result<ArrayRef, ExecError> {
    let d = date
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "date arithmetic expects a date, found {:?}",
                date.data_type()
            ))
        })?;
    let n = cast::cast(days, &DataType::Int32).map_err(|e| map_arrow(e, "date arithmetic"))?;
    let n = n
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("just cast to Int32");
    let out: Date32Array = (0..d.len())
        .map(|i| {
            if d.is_null(i) || n.is_null(i) {
                return Ok(None);
            }
            n.value(i)
                .checked_mul(sign)
                .and_then(|off| d.value(i).checked_add(off))
                .map(Some)
                .ok_or(ExecError::Overflow("date"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect();
    Ok(Arc::new(out))
}

/// `date - date` — Postgres's `date_mi`, which yields an `integer` count of
/// days, not another date and not an interval. Measured live:
/// `'2024-01-15'::DATE - '2024-01-01'::DATE` is `14`.
fn date_diff_days(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let cast_date = |a: &ArrayRef| -> Result<Date32Array, ExecError> {
        a.as_any()
            .downcast_ref::<Date32Array>()
            .cloned()
            .ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "date subtraction expects a date, found {:?}",
                    a.data_type()
                ))
            })
    };
    let l = cast_date(lhs)?;
    let r = cast_date(rhs)?;
    let out: Int32Array = (0..l.len())
        .map(|i| {
            if l.is_null(i) || r.is_null(i) {
                return Ok(None);
            }
            l.value(i)
                .checked_sub(r.value(i))
                .map(Some)
                .ok_or(ExecError::Overflow("integer"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect();
    Ok(Arc::new(out))
}

fn eval_coalesce(exprs: &[Expr], batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
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
    let arrays = eval_branches_unified(&arg_refs, batch, session)?;
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

fn eval_is_null(arg: &Expr, negated: bool, batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    let a = eval_with(arg, batch, session)?;
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
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let (l, r) = eval_operand_pair(lhs, rhs, batch, session)?;
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
    session: &EvalSession,
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
            Some(typed) => eval_with(typed, batch, session)?.data_type().clone(),
            None => arrow_schema::DataType::Utf8,
        };
        eval_untyped_literal(arg, &target, batch.num_rows())?
    } else {
        eval_with(arg, batch, session)?
    };

    let mut acc = eval_in_list_test(&x, first, negated, batch, session)?;
    for item in rest {
        let test = eval_in_list_test(&x, item, negated, batch, session)?;
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
    session: &EvalSession,
) -> Result<BooleanArray, ExecError> {
    // Resolved per element rather than once for the list: `x IN (1, 'a')` is a
    // type error in Postgres, but `x IN (1, 2)` where x is bigint is not, and
    // each element widens against x independently.
    let (x, v) = eval_operand_against(Arc::clone(x), item, batch, session)?;
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
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let x = eval_with(arg, batch, session)?;
    let low_v = eval_with(low, batch, session)?;
    let high_v = eval_with(high, batch, session)?;

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
    session: &EvalSession,
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
    let (a, p) = eval_operand_pair(arg, pattern, batch, session)?;
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
fn eval_scalar_fn(func: FuncId, args: &[Expr], batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    // Registry first; the `match` below is the fallback. See
    // `docs/migration/df-removal/27-function-hosting-abi.md` and
    // `crate::funcs`.
    //
    // This ordering is what makes the function tranche parallelisable at all:
    // moving a function off the `match` is write the impl, register it, delete
    // its arm — three edits that need no coordination with whoever is moving a
    // different oid, and every intermediate state runs. While the registry is
    // empty this branch is never taken and behaviour is bit-identical to
    // before it existed.
    //
    // Arguments are evaluated HERE, once, rather than inside each
    // implementation. The `&[Expr]` the arms below receive is generality
    // nothing uses — no PostgreSQL *function* is lazy; `CASE`, `COALESCE` and
    // short-circuiting `AND`/`OR` are `Expr` variants, not `pg_proc` rows —
    // and holding unevaluated arguments lets an implementation evaluate one
    // twice, which is how `now()` can change inside a single statement (#151).
    if let Some(hosted) = crate::funcs::builtins().scalar(func.0) {
        let evaluated = args
            .iter()
            .map(|e| eval_with(e, batch, session))
            .collect::<Result<Vec<_>, ExecError>>()?;
        return hosted.invoke(&evaluated, session);
    }

    let oid = func.0.get();
    let a = |i: usize| -> Result<ArrayRef, ExecError> {
        let e = args.get(i).ok_or_else(|| {
            ExecError::Internal(format!(
                "scalar function oid {oid} called with only {} argument(s) — a planner bug, \
                 not user error",
                args.len()
            ))
        })?;
        eval_with(e, batch, session)
    };

    match oid {
        // OID_LOWER's arm is GONE — `lower` is hosted in
        // `crate::funcs::str_fns::Lower` and answered by the registry above.
        // Deleting the arm is step 3 of a port and is not optional: the
        // registry is consulted first, so a left-behind arm is unreachable
        // code that still reads as live.
        OID_UPPER => text_unary(&a(0)?, str::to_uppercase),
        OID_LENGTH_TEXT | OID_CHAR_LENGTH_TEXT | OID_CHARACTER_LENGTH_TEXT => {
            text_char_length(&a(0)?)
        }
        OID_ARRAY_LENGTH => eval_array_length(&a(0)?, &a(1)?),

        OID_SUBSTR_2 | OID_SUBSTRING_2 => eval_substr(&a(0)?, &a(1)?, None),
        OID_SUBSTR_3 | OID_SUBSTRING_3 => {
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

        OID_CONCAT => eval_concat(args, batch, session),
        OID_CONCAT_WS => eval_concat_ws(args, batch, session),

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
        // Same implementation, same argument order — see [`OID_POSITION`].
        OID_STRPOS | OID_POSITION => {
            let s = a(0)?;
            let needle = a(1)?;
            eval_strpos(&s, &needle)
        }

        OID_INITCAP => text_unary(&a(0)?, pg_initcap),

        // The two-argument forms are not separate algorithms: on a live
        // PostgreSQL 18 `lpad(text,int)` is a SQL-language function whose body
        // is `lpad($1, $2, ' ')` (its errors even arrive with `CONTEXT: SQL
        // function "lpad" statement 1`), so `None` here means "the default
        // fill", not "a different code path".
        OID_LPAD_2 => {
            let s = a(0)?;
            let len = a(1)?;
            eval_pad(&s, &len, None, PadSide::Left)
        }
        OID_LPAD_3 => {
            let s = a(0)?;
            let len = a(1)?;
            let fill = a(2)?;
            eval_pad(&s, &len, Some(&fill), PadSide::Left)
        }
        OID_RPAD_2 => {
            let s = a(0)?;
            let len = a(1)?;
            eval_pad(&s, &len, None, PadSide::Right)
        }
        OID_RPAD_3 => {
            let s = a(0)?;
            let len = a(1)?;
            let fill = a(2)?;
            eval_pad(&s, &len, Some(&fill), PadSide::Right)
        }
        OID_REPEAT => {
            let s = a(0)?;
            let count = a(1)?;
            eval_repeat(&s, &count)
        }
        OID_SPLIT_PART => {
            let s = a(0)?;
            let delim = a(1)?;
            let field = a(2)?;
            eval_split_part(&s, &delim, &field)
        }

        OID_AGE_TIMESTAMP => {
            let lhs = a(0)?;
            let rhs = a(1)?;
            eval_age(&lhs, &rhs)
        }

        OID_DATE_TRUNC_TIMESTAMP => {
            let unit = a(0)?;
            let value = a(1)?;
            eval_date_trunc_timestamp(&unit, &value)
        }
        OID_DATE_TRUNC_TIMESTAMPTZ => {
            let unit = a(0)?;
            let value = a(1)?;
            eval_date_trunc_timestamptz(&unit, &value, session)
        }
        OID_DATE_TRUNC_INTERVAL => {
            let unit = a(0)?;
            let value = a(1)?;
            eval_date_trunc_interval(&unit, &value)
        }
        OID_DATE_PART_TIMESTAMP => {
            let field = a(0)?;
            let value = a(1)?;
            eval_date_part_timestamp(&field, &value)
        }
        OID_DATE_PART_TIMESTAMPTZ => {
            let field = a(0)?;
            let value = a(1)?;
            eval_date_part_timestamptz(&field, &value, session)
        }
        OID_DATE_PART_DATE => {
            let field = a(0)?;
            let value = a(1)?;
            eval_date_part_date(&field, &value)
        }
        OID_DATE_PART_INTERVAL => {
            let field = a(0)?;
            let value = a(1)?;
            eval_date_part_interval(&field, &value)
        }

        // All six `extract` oids share ONE implementation, which reads the
        // argument's actual Arrow type rather than trusting the oid. See
        // [`eval_extract`] for why that is the correct dispatch and not a
        // shortcut.
        OID_EXTRACT_DATE
        | OID_EXTRACT_TIME
        | OID_EXTRACT_TIMETZ
        | OID_EXTRACT_TIMESTAMP
        | OID_EXTRACT_TIMESTAMPTZ
        | OID_EXTRACT_INTERVAL => {
            let value = a(1)?;
            eval_extract(args.first(), &value, session)
        }

        // Both `to_char` oids likewise share one argument-type-dispatched
        // implementation — which is also what lets a `date` argument work at
        // all, since PostgreSQL reaches 2049 from a `date` by an implicit
        // cast that Basin's lowering does not insert.
        OID_TO_CHAR_TIMESTAMP | OID_TO_CHAR_TIMESTAMPTZ => {
            let value = a(0)?;
            let format = a(1)?;
            eval_to_char_datetime(&value, &format, session)
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
        OID_EXP_FLOAT8 => float8_unary_checked(&a(0)?, pg_exp_f64),
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
        OID_DEGREES_FLOAT8 => float8_unary_checked(&a(0)?, pg_degrees_f64),
        OID_RADIANS_FLOAT8 => float8_unary_checked(&a(0)?, pg_radians_f64),
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
        OID_COS_FLOAT8 => float8_unary_checked(&a(0)?, |x| pg_trig_of_radians(x, f64::cos)),
        OID_SIN_FLOAT8 => float8_unary_checked(&a(0)?, |x| pg_trig_of_radians(x, f64::sin)),
        OID_TAN_FLOAT8 => float8_unary_checked(&a(0)?, |x| pg_trig_of_radians(x, f64::tan)),

        // ─── The DataFusion orphans ─────────────────────────────────────────
        //
        // The array family. See its header comment for the three NULLs these
        // functions distinguish and the measured table of every rule.
        OID_ARRAY_APPEND => {
            eval_array_add_element(&a(0)?, &a(1)?, false, "array_append")
        }
        OID_ARRAY_PREPEND => {
            // Note the argument order: `array_prepend(element, array)`, the
            // reverse of `array_append`. `basin_pgtype::func::FUNCS` has a
            // test asserting `array_prepend(array, element)` does NOT resolve,
            // for exactly this reason.
            eval_array_add_element(&a(1)?, &a(0)?, true, "array_prepend")
        }
        OID_ARRAY_CAT => eval_array_cat(&a(0)?, &a(1)?),
        OID_ARRAY_REMOVE => eval_array_remove(&a(0)?, &a(1)?),
        OID_ARRAY_REPLACE => eval_array_replace(&a(0)?, &a(1)?, &a(2)?),
        OID_ARRAY_POSITION => eval_array_position(&a(0)?, &a(1)?, None),
        OID_ARRAY_POSITION_START => {
            let start = a(2)?;
            eval_array_position(&a(0)?, &a(1)?, Some(&start))
        }
        OID_ARRAY_POSITIONS => eval_array_positions(&a(0)?, &a(1)?),
        OID_ARRAY_NDIMS => eval_array_ndims(&a(0)?),
        OID_CARDINALITY => eval_cardinality(&a(0)?),
        OID_ARRAY_REVERSE => eval_array_reverse(&a(0)?),
        OID_ARRAY_SORT_1 => eval_array_sort(&a(0)?, None, None),
        OID_ARRAY_SORT_2 => {
            let desc = a(1)?;
            eval_array_sort(&a(0)?, Some(&desc), None)
        }
        OID_ARRAY_SORT_3 => {
            let desc = a(1)?;
            let nulls_first = a(2)?;
            eval_array_sort(&a(0)?, Some(&desc), Some(&nulls_first))
        }
        OID_ARRAY_TO_STRING_2 => eval_array_to_string(&a(0)?, &a(1)?, None),
        OID_ARRAY_TO_STRING_3 => {
            let null_string = a(2)?;
            eval_array_to_string(&a(0)?, &a(1)?, Some(&null_string))
        }
        OID_STRING_TO_ARRAY_2 => eval_string_to_array(&a(0)?, &a(1)?, None),
        OID_STRING_TO_ARRAY_3 => {
            let null_string = a(2)?;
            eval_string_to_array(&a(0)?, &a(1)?, Some(&null_string))
        }

        // String and binary measurement. `bit_length` is `octet_length` times
        // eight and neither is `length` — see [`text_byte_length`].
        OID_OCTET_LENGTH_TEXT => text_byte_length(&a(0)?, false),
        OID_BIT_LENGTH_TEXT => text_byte_length(&a(0)?, true),
        OID_OCTET_LENGTH_BYTEA => bytea_byte_length(&a(0)?, false),
        OID_BIT_LENGTH_BYTEA => bytea_byte_length(&a(0)?, true),
        OID_STARTS_WITH => eval_starts_with(&a(0)?, &a(1)?),
        OID_TO_HEX_INT4 => eval_to_hex_i32(&a(0)?),
        OID_TO_HEX_INT8 => eval_to_hex_i64(&a(0)?),
        OID_OVERLAY_TEXT_3 => eval_overlay_text(&a(0)?, &a(1)?, &a(2)?, None),
        OID_OVERLAY_TEXT_4 => {
            let len = a(3)?;
            eval_overlay_text(&a(0)?, &a(1)?, &a(2)?, Some(&len))
        }
        OID_OVERLAY_BYTEA_3 => eval_overlay_bytea(&a(0)?, &a(1)?, &a(2)?, None),
        OID_OVERLAY_BYTEA_4 => {
            let len = a(3)?;
            eval_overlay_bytea(&a(0)?, &a(1)?, &a(2)?, Some(&len))
        }

        // Integer math. The overflow boundaries are the whole difficulty —
        // see [`pg_gcd_i64`]'s measured truth table.
        OID_FACTORIAL => eval_factorial(&a(0)?),
        OID_GCD_INT4 => eval_gcd_lcm_i32(&a(0)?, &a(1)?, true),
        OID_LCM_INT4 => eval_gcd_lcm_i32(&a(0)?, &a(1)?, false),
        OID_GCD_INT8 => eval_gcd_lcm_i64(&a(0)?, &a(1)?, true),
        OID_LCM_INT8 => eval_gcd_lcm_i64(&a(0)?, &a(1)?, false),

        // `cot` shares `sin`/`cos`/`tan`'s shape exactly, including the part
        // that is easy to miss: `cot('NaN')` is `NaN` and `cot('Infinity')` is
        // an ERROR, while `cot(0)` is `Infinity` — not an error, not NULL.
        // All three measured live.
        OID_COT_FLOAT8 => {
            float8_unary_checked(&a(0)?, |x| pg_trig_of_radians(x, |v| 1.0 / f64::tan(v)))
        }
        // The six hyperbolics. `f64`'s implementations are the same libm
        // routines Postgres calls, so these are exact rather than approximate.
        // `sinh`/`cosh`/`tanh`/`asinh` have no domain to leave — measured
        // live, `sinh(1e308)` is `Infinity` (not an error), `tanh('Infinity')`
        // is `1`, and a `NaN` passes straight through — so they need no
        // checked path at all.
        OID_SINH_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::sinh,
        ))),
        OID_COSH_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::cosh,
        ))),
        OID_TANH_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::tanh,
        ))),
        OID_ASINH_FLOAT8 => Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(
            downcast_array::<Float64Array>(&a(0)?, "double precision")?,
            f64::asinh,
        ))),
        OID_ACOSH_FLOAT8 => float8_unary_checked(&a(0)?, pg_acosh_f64),
        OID_ATANH_FLOAT8 => float8_unary_checked(&a(0)?, pg_atanh_f64),
        // `log10(float8)` and `pow(float8, float8)` are separate `pg_proc`
        // rows from `log(float8)` (1340) and `power(float8, float8)` (1368),
        // with identical semantics — Postgres's `log` on `double precision` is
        // already base 10. Sharing the implementation is therefore exact, not
        // an approximation.
        OID_LOG10_FLOAT8 => float8_unary_checked(&a(0)?, pg_log10_f64),
        OID_POW_FLOAT8 => {
            let base = a(0)?;
            let exp = a(1)?;
            float8_binary_checked(&base, &exp, pg_power_f64)
        }

        // `make_date`. Year zero is rejected; a negative year is BC.
        OID_MAKE_DATE => eval_make_date(&a(0)?, &a(1)?, &a(2)?),

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
pub(crate) fn downcast_array<'a, T: Array + 'static>(
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
/// `right('hello', -2) = 'llo'`). The two are *not* mirror images at the
/// extreme, though — see [`pg_right`] for the one argument where Postgres's
/// own arithmetic overflows and `right` stops behaving like `left`.
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

/// Postgres's `right` does not compute "how many characters to take" — it
/// computes "how many characters to SKIP from the front", and then returns
/// everything after them. For `n >= 0` the skip is `len - n`; for `n < 0` it
/// is `-n`; either way a negative skip is clamped to zero. Written the
/// obvious way instead ("take the last `n`") the two forms agree on every
/// input but one.
///
/// **The exception is `n = i32::MIN`, and this function reproduces it on
/// purpose.** `text_right` negates `n` in a C `int`, and `-(-2147483648)` is
/// not representable, so the negation wraps back to `-2147483648`; the
/// clamp then sees a negative skip and skips nothing, returning the WHOLE
/// string. Verified on a live PostgreSQL 18 — and note the discontinuity, a
/// single step in the argument flipping the answer completely:
///
/// ```text
/// right('abcdef', -2147483647) = ''          -- skip 2147483647 chars
/// right('abcdef', -2147483648) = 'abcdef'    -- negation overflowed
/// ```
///
/// This is an upstream integer overflow, not a designed rule, and Basin used
/// to return `''` for both. Matching it is a deliberate policy decision, not
/// an oversight: `function_equivalence.rs` is an oracle, and an oracle with a
/// standing "known intentional divergence" entry teaches everyone reading it
/// that red is sometimes fine. Six call sites there depend on this. **Do not
/// "fix" this to return `''` — that reintroduces the divergence.**
///
/// `wrapping_neg` is what makes it explicit rather than a debug-build panic:
/// plain `-n` on `i32::MIN` panics under `overflow-checks`, which is how this
/// case would otherwise announce itself.
fn pg_right(s: &str, n: i32) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i32;
    let skip = if n >= 0 { len - n } else { n.wrapping_neg() };
    let skip = skip.clamp(0, len);
    chars[skip as usize..].iter().collect()
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

/// `initcap(text)`: uppercase the first character of each "word", lowercase
/// everything else. The rule is not "after whitespace" — Postgres starts a
/// new word after ANY non-alphanumeric character, and it lowercases every
/// character that follows an alphanumeric one. Verified against a live
/// PostgreSQL 18:
///
/// ```text
/// initcap('hELLo-woRLD foo_bar')  = 'Hello-World Foo_Bar'  -- '-' and '_' both split
/// initcap('a''b c.d')             = 'A'B C.D'              -- so do quote and dot
/// initcap('3abc a2b')             = '3abc A2b'             -- digits are alphanumeric,
///                                                          -- so they do NOT split
/// initcap('世界abc')              = '世界abc'              -- nor do CJK letters
/// initcap('привет МИР')           = 'Привет Мир'
/// ```
///
/// **A known, narrow divergence, stated rather than hidden.** Postgres calls
/// the C library's `iswalnum` under the database's `lc_ctype` (`en_US.UTF-8`
/// here), which counts only Unicode category `Nd` as a digit. Rust's
/// [`char::is_alphanumeric`] is `Alphabetic | Nd | Nl | No`, so the two
/// disagree for the "other number" and "letter number" categories:
/// `initcap('½abc')` is `'½Abc'` on the live server (`½` does not split a
/// word for glibc, so `a` begins one) but `'½abc'` here. Confirmed live for
/// `½`, `¹`, `²`, `①` (all `No`) and `Ⅷ` (`Nl`); `٣` (Arabic-Indic three,
/// `Nd`) agrees. std exposes no `Nd`-only predicate, and pulling in a Unicode
/// property table for five glyph classes is not a trade this crate should
/// make — see the module docs on staying lean. Nothing in the corpora that
/// diff this file against Postgres contains such a character.
///
/// The case mapping itself is Rust's full (possibly multi-character) mapping,
/// matching what [`text_unary`]-based `upper`/`lower` already do in this same
/// file rather than introducing a second, inconsistent notion of case here.
fn pg_initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if prev_alnum {
            out.extend(c.to_lowercase());
        } else {
            out.extend(c.to_uppercase());
        }
        prev_alnum = c.is_alphanumeric();
    }
    out
}

/// Which end [`pg_pad`] adds fill characters to.
#[derive(Clone, Copy)]
enum PadSide {
    Left,
    Right,
}

/// The fill the two-argument `lpad`/`rpad` use: a single ASCII space, taken
/// from the SQL body of Postgres's own two-argument wrapper.
const PAD_DEFAULT_FILL: &str = " ";

/// Postgres's `MaxAllocSize` (`0x3fffffff`), the ceiling `palloc` enforces.
const PG_MAX_ALLOC_SIZE: i64 = 0x3fff_ffff;

/// The 4-byte varlena header every `text` datum carries, which counts against
/// [`PG_MAX_ALLOC_SIZE`].
const PG_VARHDRSZ: i64 = 4;

/// The largest `len` `lpad`/`rpad` will accept before raising `requested
/// length too large`: `(MaxAllocSize - VARHDRSZ) / 4`, where 4 is UTF-8's
/// maximum bytes per character. The boundary was confirmed from both sides on
/// a live PostgreSQL 18 — `lpad('hello', 268435455)` raises the error, while
/// `lpad('hello', 268435454)` does not (it runs on until a `statement_timeout`
/// cuts it off, which is Postgres genuinely building a gigabyte of padding).
const PAD_MAX_LEN: i32 = ((PG_MAX_ALLOC_SIZE - PG_VARHDRSZ) / 4) as i32;

/// `lpad(text, int [, text])` / `rpad(text, int [, text])`. `fill` is `None`
/// for the two-argument forms, meaning [`PAD_DEFAULT_FILL`].
///
/// Strict in all three arguments, and that strictness is load-bearing for the
/// length check: `lpad(NULL, 2147483647)` is NULL on the live server, NOT the
/// `requested length too large` error, because a strict function is never
/// entered when an argument is NULL. That is why the ceiling is tested per
/// row inside [`pg_pad`] rather than swept over the whole `len` array up
/// front the way [`eval_substr`] checks its negative length — a whole-array
/// pre-check would turn that NULL into an error.
fn eval_pad(
    text: &ArrayRef,
    len: &ArrayRef,
    fill: Option<&ArrayRef>,
    side: PadSide,
) -> Result<ArrayRef, ExecError> {
    let t = downcast_array::<StringArray>(text, "text")?;
    let l = downcast_array::<Int32Array>(len, "integer")?;
    let f = fill
        .map(|f| downcast_array::<StringArray>(f, "text"))
        .transpose()?;

    let n = t.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        let fill_null = f.map(|f| f.is_null(i)).unwrap_or(false);
        if t.is_null(i) || l.is_null(i) || fill_null {
            out.push(None);
            continue;
        }
        let fill = f.map(|f| f.value(i)).unwrap_or(PAD_DEFAULT_FILL);
        out.push(Some(pg_pad(t.value(i), l.value(i), fill, side)?));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// Postgres's `text_pad`, on already-unwrapped values. `len` is a count of
/// *characters*, not bytes (`lpad('héllo', 8, 'é') = 'éééhéllo'` — verified
/// live), so this walks `chars()` rather than indexing the byte slice.
///
/// The order of the three steps is what makes the awkward cases fall out, and
/// every one was verified against a live PostgreSQL 18:
///
/// 1. **Truncate first.** `len` shorter than the input is a truncation, not a
///    no-op: `lpad('hello', 3) = 'hel'`. A negative `len` clamps to zero, so
///    `lpad('hello', -3) = ''` — not an error and not `'hello'`.
/// 2. **Then check the fill.** An empty fill cannot pad, so the *already
///    truncated* string is returned as-is: `lpad('hello', 10, '') = 'hello'`
///    (untouched, because there was nothing to truncate) but
///    `lpad('hello', 3, '') = 'hel'` (truncated, then not padded). Reading
///    the empty-fill rule as "return the input unchanged" would get the
///    second of those wrong.
/// 3. **Then pad, cycling the fill character by character** — not by
///    repeating the whole fill string and trimming. `lpad('a', 6, 'xyz')` is
///    `'xyzxya'`: the pad is five characters long, so the fill wraps around
///    mid-string.
fn pg_pad(s: &str, len: i32, fill: &str, side: PadSide) -> Result<String, ExecError> {
    if len > PAD_MAX_LEN {
        return Err(ExecError::TypeMismatch(
            "requested length too large".to_string(),
        ));
    }
    let len = len.max(0) as usize;

    let chars: Vec<char> = s.chars().collect();
    let keep = chars.len().min(len);
    let body: String = chars[..keep].iter().collect();

    let fill: Vec<char> = fill.chars().collect();
    if fill.is_empty() {
        return Ok(body);
    }

    let pad: String = (0..len - keep).map(|i| fill[i % fill.len()]).collect();
    Ok(match side {
        PadSide::Left => pad + &body,
        PadSide::Right => body + &pad,
    })
}

/// `repeat(text, int)`. Strict, and — like [`eval_pad`] — checked per row
/// rather than over the whole array, so a NULL argument stays NULL instead of
/// being turned into a size error.
fn eval_repeat(s: &ArrayRef, count: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let c = downcast_array::<Int32Array>(count, "integer")?;
    let n = s.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if s.is_null(i) || c.is_null(i) {
            out.push(None);
            continue;
        }
        out.push(Some(pg_repeat(s.value(i), c.value(i))?));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// Postgres's `repeat`, on already-unwrapped values.
///
/// The count is clamped to zero *before* the size check, and the order
/// matters: a negative count is not an error, it is an empty string.
/// Verified live that `repeat('ab', -3)`, `repeat('ab', 0)` and even
/// `repeat('ab', -2147483648)` all return `''` rather than raising anything.
///
/// The size check itself is on *bytes* (Postgres measures the datum, not its
/// character count) and the boundary was confirmed from both sides on a live
/// PostgreSQL 18: `repeat('a', 1073741820)` raises `requested length too
/// large` because `1 * 1073741820 + 4` exceeds `MaxAllocSize`, while
/// `repeat('a', 1073741819)` lands exactly on it and is accepted. A 3-byte
/// character moves the boundary by the same factor, as the byte-based reading
/// predicts (`repeat('世', 536870910)` errors).
///
/// Doing the arithmetic in `i64` rather than `i32` is deliberate. Postgres
/// reaches the same verdict by *detecting* `int32` overflow in the multiply
/// and the add; widening first cannot overflow at all, and any product that
/// would have wrapped a signed 32-bit int is necessarily already past
/// [`PG_MAX_ALLOC_SIZE`], so the two agree on every input while this one has
/// no wrapping to reason about.
fn pg_repeat(s: &str, count: i32) -> Result<String, ExecError> {
    let count = i64::from(count.max(0));
    if s.len() as i64 * count + PG_VARHDRSZ > PG_MAX_ALLOC_SIZE {
        return Err(ExecError::TypeMismatch(
            "requested length too large".to_string(),
        ));
    }
    Ok(s.repeat(count as usize))
}

/// `split_part(text, text, int)`: split on `delim` and return one field.
///
/// Strict, and the zero-field error is raised per row *after* the NULL check
/// for the same reason [`eval_pad`]'s size check is: `split_part(NULL, ',', 0)`
/// is NULL on the live server, not `field position must not be zero`, because
/// a strict function never runs on a NULL argument.
fn eval_split_part(
    s: &ArrayRef,
    delim: &ArrayRef,
    field: &ArrayRef,
) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let d = downcast_array::<StringArray>(delim, "text")?;
    let f = downcast_array::<Int32Array>(field, "integer")?;
    let n = s.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if s.is_null(i) || d.is_null(i) || f.is_null(i) {
            out.push(None);
            continue;
        }
        out.push(Some(pg_split_part(s.value(i), d.value(i), f.value(i))?));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// Postgres's `split_part`, on already-unwrapped values. Verified against a
/// live PostgreSQL 18 on every branch below:
///
/// ```text
/// split_part('a,b,c', ',',  0) -- ERROR: field position must not be zero
/// split_part('a,b,c', ',',  1) = 'a'
/// split_part('a,b,c', ',',  4) = ''       -- past the end is empty, not an error
/// split_part('a,b,c', ',', -1) = 'c'      -- negative counts from the right (PG 14+)
/// split_part('a,b,c', ',', -3) = 'a'
/// split_part('a,b,c', ',', -4) = ''
/// split_part('a,b,c', '',   1) = 'a,b,c'  -- an empty delimiter makes ONE field,
/// split_part('a,b,c', '',  -1) = 'a,b,c'  -- reachable from either end,
/// split_part('a,b,c', '',   2) = ''       -- and nothing else
/// ```
///
/// A field position of zero is the one input that errors, and it errors for
/// both signs of "out of range" being legal — without it, `0` would have to
/// mean either the first or the last field and neither reading is safe.
///
/// The index arithmetic is done in `i64`. `field` reaches this function as a
/// full `i32`, so a negative field of `i32::MIN` would overflow a 32-bit
/// negation; widening first makes it fall out of range and return `''`, which
/// is what the live server does (`split_part('a,b,c', ',', -2147483648)` is
/// `''`, not an error).
fn pg_split_part(s: &str, delim: &str, field: i32) -> Result<String, ExecError> {
    if field == 0 {
        return Err(ExecError::TypeMismatch(
            "field position must not be zero".to_string(),
        ));
    }
    if delim.is_empty() {
        return Ok(if field == 1 || field == -1 {
            s.to_string()
        } else {
            String::new()
        });
    }

    let parts: Vec<&str> = s.split(delim).collect();
    let field = i64::from(field);
    let index = if field > 0 {
        field - 1
    } else {
        parts.len() as i64 + field
    };
    Ok(usize::try_from(index)
        .ok()
        .and_then(|i| parts.get(i))
        .map(|p| (*p).to_string())
        .unwrap_or_default())
}

/// `age(timestamp, timestamp)`. Strict in both arguments.
///
/// This is the first function in this file to *produce* an interval, so the
/// output is an `IntervalMonthDayNanoArray` — the physical layout
/// `basin_pgtype` maps `oid::INTERVAL` onto. Postgres's own interval is a
/// `(months, days, microseconds)` triple; Arrow's is `(months, days,
/// nanoseconds)`, so only the last component is rescaled, and the month and
/// day components are carried across untouched rather than being normalised
/// into some canonical duration. That matters: `age` deliberately returns a
/// *symbolic* difference, and "1 mon 1 day" is not interchangeable with any
/// fixed number of nanoseconds.
fn eval_age(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let l = downcast_array::<TimestampMicrosecondArray>(lhs, "timestamp")?;
    let r = downcast_array::<TimestampMicrosecondArray>(rhs, "timestamp")?;
    let n = l.len();
    let mut out: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(n);
    for i in 0..n {
        if l.is_null(i) || r.is_null(i) {
            out.push(None);
            continue;
        }
        out.push(Some(pg_age(l.value(i), r.value(i))));
    }
    Ok(Arc::new(IntervalMonthDayNanoArray::from(out)))
}

/// Postgres's `timestamp_age`, on microseconds since the Unix epoch.
///
/// `age` is **not** a duration. It is the *symbolic* difference between two
/// broken-down calendar readings: subtract field by field (year from year,
/// month from month, day from day, …), then carry each negative field into
/// the next larger one. That is why `age` can answer in months at all, and
/// why its answer is not a function of the elapsed microseconds alone:
///
/// ```text
/// age('2024-03-31', '2024-01-31') = 2 mons        -- 60 days elapsed
/// age('2024-03-01', '2024-01-31') = 1 mon 1 day   -- 30 days elapsed
/// ```
///
/// **The borrow uses the EARLIER timestamp's month, never the later one's,
/// and never the month the borrow lands in.** This is the detail worth
/// pinning, because two plausible alternative readings give the same answer
/// on most inputs. Verified live:
///
/// ```text
/// age('2000-03-01', '2000-01-31') = 1 mon 1 day
/// age('1900-03-01', '1900-01-31') = 1 mon 1 day
/// ```
///
/// 2000 is a leap year and 1900 is not, so if the borrow took its day count
/// from *February* — the month between the two, and the one a "how many days
/// in the intervening month" reading would pick — those two answers would
/// differ by a day. They do not: both borrow 31 days from **January**, the
/// month the earlier argument is in.
///
/// The sign handling mirrors Postgres's: flip the raw field differences up
/// front when `dt1 < dt2`, carry on non-negative numbers, then flip back. The
/// result is `-1 years -1 mons -25 days -21:30:00`-style — every component
/// negative — rather than a mixed-sign interval, which is why the sign is
/// applied to the assembled components at the end.
fn pg_age(dt1: i64, dt2: i64) -> IntervalMonthDayNano {
    let (y1, mo1, d1, h1, mi1, s1, us1) = civil_from_micros(dt1);
    let (y2, mo2, d2, h2, mi2, s2, us2) = civil_from_micros(dt2);

    // `sign` folds Postgres's two negations (once before the carries, once
    // after) into one multiplication: carrying always runs on the larger-minus-
    // smaller ordering, so every field below is driven non-negative.
    let sign = if dt1 < dt2 { -1 } else { 1 };
    let mut usec = (us1 - us2) * sign;
    let mut sec = (s1 - s2) * sign;
    let mut min = (mi1 - mi2) * sign;
    let mut hour = (h1 - h2) * sign;
    let mut mday = (d1 - d2) * sign;
    let mut mon = (mo1 - mo2) * sign;
    let mut year = (y1 - y2) * sign;

    // The month to borrow days from: the earlier argument's. `sign` already
    // encodes which that is.
    let (borrow_year, borrow_mon) = if sign < 0 { (y1, mo1) } else { (y2, mo2) };

    while usec < 0 {
        usec += 1_000_000;
        sec -= 1;
    }
    while sec < 0 {
        sec += 60;
        min -= 1;
    }
    while min < 0 {
        min += 60;
        hour -= 1;
    }
    while hour < 0 {
        hour += 24;
        mday -= 1;
    }
    while mday < 0 {
        mday += days_in_month(borrow_year, borrow_mon);
        mon -= 1;
    }
    while mon < 0 {
        mon += 12;
        year -= 1;
    }

    let micros = ((hour * 60 + min) * 60 + sec) * 1_000_000 + usec;
    IntervalMonthDayNanoType::make_value(
        ((year * 12 + mon) * sign) as i32,
        (mday * sign) as i32,
        micros * sign * 1_000,
    )
}

/// Break microseconds since the Unix epoch into a civil `(year, month, day,
/// hour, minute, second, microsecond)` reading, with no timezone involved —
/// the input is a `timestamp without time zone`, which is already a wall-clock
/// reading rather than an instant.
///
/// `div_euclid`/`rem_euclid` rather than `/` and `%` because timestamps before
/// 1970 are negative, and truncating division would put the time-of-day of
/// every pre-epoch instant on the wrong day.
fn civil_from_micros(micros: i64) -> (i64, i64, i64, i64, i64, i64, i64) {
    const USECS_PER_DAY: i64 = 86_400_000_000;
    let days = micros.div_euclid(USECS_PER_DAY);
    let time_of_day = micros.rem_euclid(USECS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let (secs, usec) = (time_of_day / 1_000_000, time_of_day % 1_000_000);
    (
        year,
        month,
        day,
        secs / 3600,
        (secs / 60) % 60,
        secs % 60,
        usec,
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`.
///
/// This is Howard Hinnant's `civil_from_days`, which works by shifting the
/// epoch to 0000-03-01 so that the leap day lands at the end of the year and
/// the 400-year cycle ("era") becomes exact integer arithmetic with no
/// special cases. The unexplained-looking constants are all consequences of
/// that shift: 719468 is the day offset between the two epochs, 146097 is the
/// number of days in 400 Gregorian years, and 36524/1460 are the 100- and
/// 4-year cycle lengths used to undo the leap-day compression.
///
/// The `if z >= 0` guard on `era` is a floor-division fixup: Rust's `/`
/// truncates toward zero, which is wrong for the negative day numbers that
/// pre-1970 timestamps produce.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // day of era, [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365], from March 1
    let mp = (5 * doy + 2) / 153; // month shifted so March is 0
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Proleptic Gregorian: every fourth year is a leap year, except centuries,
/// except every fourth century. Matches Postgres's `isleap` macro exactly.
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

// ─── date_trunc / date_part ─────────────────────────────────────────────────
//
// The first functions in this file that are not functions of their arguments
// alone: the `timestamptz` overloads read the session's `TimeZone` out of
// [`EvalSession`]. See that type's docs for why that context exists at all.
//
// Everything below — the unit vocabulary, the truncation rules, the
// re-determination rule, the ambiguity/gap rules, and every error string —
// was read off a live PostgreSQL 18.2 rather than recalled. Where the answer
// was surprising it is quoted at the definition that implements it.

/// A `date_trunc`/`date_part` unit, after alias resolution.
///
/// The two functions share Postgres's one unit vocabulary (`deltatktbl` in
/// `datetime.c`), which is why this is one enum: `date_trunc('dow', …)` and
/// `date_part('dow', …)` do not fail the same way. `dow` is *recognized* by
/// both — `date_trunc` rejects it with "not recognized" only because the
/// truncation switch has no arm for it, while `timezone` is rejected with a
/// different message ("not supported") by both. Those two message strings are
/// Postgres's own, and the distinction between them is preserved below.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DateUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    Millennium,
    Dow,
    IsoDow,
    Doy,
    IsoYear,
    Julian,
    Epoch,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

/// Resolve a unit name the way Postgres does: lowercase it, then look it up
/// exactly. No trimming — `date_trunc(' day ', …)` is an error on a live
/// PostgreSQL 18 (`unit " day " not recognized`), which is exactly the case a
/// `trim()` here would silently "fix" into a wrong success.
///
/// The alias set is not guessed at. Every spelling below was accepted by a
/// live PostgreSQL 18.2 for both `date_trunc` and `date_part`; the near
/// misses that were tried and *rejected* are recorded too, because they are
/// what a plausible-looking alias table gets wrong:
/// `quarters`, `dayofweek`, `dayofyear`, `yday`, `tz` are all "not
/// recognized" even though `quarter`, `dow`, `doy` and `timezone` are.
fn parse_date_unit(raw: &str) -> Option<DateUnit> {
    let lowered = raw.to_lowercase();
    Some(match lowered.as_str() {
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" => DateUnit::Microsecond,
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => DateUnit::Millisecond,
        "s" | "sec" | "secs" | "second" | "seconds" => DateUnit::Second,
        "min" | "mins" | "minute" | "minutes" => DateUnit::Minute,
        "hr" | "hrs" | "hour" | "hours" => DateUnit::Hour,
        "d" | "day" | "days" => DateUnit::Day,
        "w" | "week" | "weeks" => DateUnit::Week,
        "mon" | "mons" | "month" | "months" => DateUnit::Month,
        "qtr" | "quarter" => DateUnit::Quarter,
        "y" | "yr" | "yrs" | "year" | "years" => DateUnit::Year,
        "dec" | "decs" | "decade" | "decades" => DateUnit::Decade,
        "c" | "cent" | "century" | "centuries" => DateUnit::Century,
        "mil" | "mils" | "millennia" | "millennium" | "millenniums" => DateUnit::Millennium,
        "dow" => DateUnit::Dow,
        "isodow" => DateUnit::IsoDow,
        "doy" => DateUnit::Doy,
        "isoyear" => DateUnit::IsoYear,
        "julian" | "jd" => DateUnit::Julian,
        "epoch" => DateUnit::Epoch,
        "timezone" => DateUnit::Timezone,
        "timezone_hour" | "timezone_h" => DateUnit::TimezoneHour,
        "timezone_minute" | "timezone_m" => DateUnit::TimezoneMinute,
        _ => return None,
    })
}

/// Postgres's `unit "…" not recognized for type …` — the unit is not in the
/// vocabulary at all.
fn unit_not_recognized(raw: &str, ty: &str) -> ExecError {
    ExecError::TypeMismatch(format!("unit \"{raw}\" not recognized for type {ty}"))
}

/// Postgres's `unit "…" not supported for type …` — the unit is a real unit,
/// but means nothing for this argument type. A different message from
/// [`unit_not_recognized`], and the difference is load-bearing:
/// `date_part('timezone', ts)` on a `timestamp without time zone` says "not
/// supported", while `date_part('fortnight', ts)` says "not recognized".
fn unit_not_supported(raw: &str, ty: &str) -> ExecError {
    ExecError::TypeMismatch(format!("unit \"{raw}\" not supported for type {ty}"))
}

const TY_TIMESTAMP: &str = "timestamp without time zone";
const TY_TIMESTAMPTZ: &str = "timestamp with time zone";
const TY_INTERVAL: &str = "interval";

/// Microseconds since the Unix epoch → a `NaiveDateTime` reading of the same
/// instant in UTC.
fn naive_from_micros(micros: i64) -> Result<NaiveDateTime, ExecError> {
    DateTime::from_timestamp_micros(micros)
        .map(|d| d.naive_utc())
        .ok_or(ExecError::Overflow("timestamp"))
}

/// The inverse of [`naive_from_micros`].
fn micros_from_naive(dt: NaiveDateTime) -> Result<i64, ExecError> {
    dt.and_utc()
        .timestamp_micros()
        .checked_abs()
        .map(|_| dt.and_utc().timestamp_micros())
        .ok_or(ExecError::Overflow("timestamp"))
}

/// Truncate a broken-down local reading to `unit`.
///
/// `None` means "this unit does not truncate" — the caller turns that into
/// Postgres's "not recognized"/"not supported" message, which one depending
/// on the unit.
///
/// The decade/century/millennium arithmetic is Postgres's, not the obvious
/// one. `date_trunc('century', '2024-03-10')` is **2001**-01-01, not
/// 2000-01-01: the 21st century starts in 2001, and Postgres computes
/// `((year + 99) / 100) * 100 - 99` to say so. Millennium is the same shape
/// (`2001`, not 2000). Decade *is* the obvious one (`(year / 10) * 10` →
/// 2020). All three verified live, including the case that proves the
/// signs are handled: `date_trunc('decade', '0001-01-01')` is
/// `0001-01-01 BC` — year 1 truncates to astronomical year 0, which is 1 BC.
fn truncate_local(local: NaiveDateTime, unit: DateUnit) -> Option<NaiveDateTime> {
    let date = local.date();
    let midnight = |d: NaiveDate| d.and_hms_opt(0, 0, 0);
    match unit {
        // Arrow's timestamps are already microsecond-resolution, so this is
        // the identity — as it is in Postgres, whose timestamps are too.
        DateUnit::Microsecond => Some(local),
        DateUnit::Millisecond => {
            let ns = local.and_utc().timestamp_subsec_nanos();
            local.with_nanosecond((ns / 1_000_000) * 1_000_000)
        }
        DateUnit::Second => local.with_nanosecond(0),
        DateUnit::Minute => local.with_second(0)?.with_nanosecond(0),
        DateUnit::Hour => local.with_minute(0)?.with_second(0)?.with_nanosecond(0),
        DateUnit::Day => midnight(date),
        // ISO weeks start on Monday, so this is "back up to the most recent
        // Monday", not "back up to a multiple of 7 days from the epoch".
        DateUnit::Week => {
            let back = i64::from(date.weekday().num_days_from_monday());
            midnight(date - TimeDelta::days(back))
        }
        DateUnit::Month => midnight(date.with_day(1)?),
        DateUnit::Quarter => {
            let first = ((date.month() - 1) / 3) * 3 + 1;
            midnight(date.with_day(1)?.with_month(first)?)
        }
        DateUnit::Year | DateUnit::Decade | DateUnit::Century | DateUnit::Millennium => {
            let y = date.year();
            let truncated = match unit {
                DateUnit::Year => y,
                DateUnit::Decade => {
                    if y > 0 {
                        (y / 10) * 10
                    } else {
                        -(((8 - (y - 1)) / 10) * 10)
                    }
                }
                DateUnit::Century => {
                    if y > 0 {
                        ((y + 99) / 100) * 100 - 99
                    } else {
                        -(((99 - (y - 1)) / 100) * 100) + 1
                    }
                }
                _ => {
                    if y > 0 {
                        ((y + 999) / 1000) * 1000 - 999
                    } else {
                        -(((999 - (y - 1)) / 1000) * 1000) + 1
                    }
                }
            };
            midnight(NaiveDate::from_ymd_opt(truncated, 1, 1)?)
        }
        DateUnit::Dow
        | DateUnit::IsoDow
        | DateUnit::Doy
        | DateUnit::IsoYear
        | DateUnit::Julian
        | DateUnit::Epoch
        | DateUnit::Timezone
        | DateUnit::TimezoneHour
        | DateUnit::TimezoneMinute => None,
    }
}

/// Does truncating to `unit` re-derive the UTC offset from the *truncated*
/// local time, or keep the offset the input instant had?
///
/// This is the single subtlest thing about `date_trunc` on a `timestamptz`,
/// it is invisible except across a DST transition, and getting it wrong
/// produces an answer that is off by exactly the DST step. Postgres
/// (`timestamptz_trunc_internal`) sets its `redotz` flag for units of a day
/// and larger and leaves it clear for hour and below.
///
/// Verified live in `Australia/Lord_Howe`, whose DST step is **30 minutes**,
/// on the instant `2024-04-06 15:00:00+00` — the first instant after that
/// zone's 2024 fall-back, so the input's own offset is `+10:30` while the
/// truncated local time sits back in `+11:00`:
///
/// ```text
/// date_trunc('hour', …) = 2024-04-07 01:30:00+11   -- offset KEPT (+10:30)
/// date_trunc('day',  …) = 2024-04-07 00:00:00+11   -- offset REDERIVED
/// ```
///
/// The `hour` answer is the tell. Local time is `01:30:00`; truncating to
/// the hour gives local `01:00:00`; re-attaching the *input's* `+10:30`
/// gives `14:30Z`, which renders back as `01:30+11` — an answer whose
/// displayed minutes are not zero even though the unit was `hour`. A
/// naive implementation that re-derives the offset for every unit produces
/// `01:00:00+11` here and is wrong.
fn unit_redetermines_zone(unit: DateUnit) -> bool {
    matches!(
        unit,
        DateUnit::Day
            | DateUnit::Week
            | DateUnit::Month
            | DateUnit::Quarter
            | DateUnit::Year
            | DateUnit::Decade
            | DateUnit::Century
            | DateUnit::Millennium
    )
}

/// Postgres's `DetermineTimeZoneOffset`: which UTC offset does a *local* wall
/// reading correspond to?
///
/// Unambiguous local times are the easy case. The other two are not, and both
/// were settled by asking the server rather than by choosing a convention:
///
/// * **Ambiguous** (a fall-back repeats an hour, so two instants render the
///   same). Postgres takes the **second** occurrence — the smaller UTC
///   offset, i.e. standard time. Verified in `America/Asuncion`, whose 2024
///   fall-back happens at local midnight so `date_trunc('day', …)` lands
///   exactly on the repeated reading: the answer is `2024-03-24 00:00:00-04`
///   (`1711252800`), the `-04` occurrence, not the `-03` one an hour earlier.
///
/// * **Nonexistent** (a spring-forward skips the reading entirely).
///   Postgres uses the offset in force **before** the transition, which
///   yields an instant at or after it — so the result renders as a *different*
///   local time than the one asked for. Verified in `America/Havana`, whose
///   2024 DST start skips local midnight: `date_trunc('day', …)` answers
///   `2024-03-10 01:00:00-04` (`1710046800`), i.e. local `00:00` resolved
///   with the pre-transition `-05`.
///
/// Both cases collapse to one rule — *prefer the smaller UTC offset* — since
/// an ambiguity always has the larger offset first and a gap always has the
/// smaller offset before it. It is written out as three arms anyway, because
/// the two situations are genuinely different and a future reader should not
/// have to rediscover that they coincide.
fn determine_time_zone_offset(tz: &Tz, local: NaiveDateTime) -> Result<i32, ExecError> {
    match tz.offset_from_local_datetime(&local) {
        MappedLocalTime::Single(o) => Ok(o.fix().local_minus_utc()),
        MappedLocalTime::Ambiguous(a, b) => {
            Ok(a.fix().local_minus_utc().min(b.fix().local_minus_utc()))
        }
        // The reading does not exist, so there is no offset to *look up* —
        // the pre-transition one has to be found by probing away from the
        // gap. 26 hours is comfortably wider than any DST step (the largest
        // in the IANA database is one hour; Lord Howe's is half of one) and
        // comfortably narrower than the gap between two transitions in any
        // zone that has them, so the probe lands in the interval before the
        // transition and reads its offset.
        MappedLocalTime::None => {
            let probe = local - TimeDelta::hours(26);
            Ok(tz.offset_from_utc_datetime(&probe).fix().local_minus_utc())
        }
    }
}

/// `date_trunc(text, timestamp)` — oid 2020. No session involvement: a
/// `timestamp without time zone` is already a civil reading, so truncating
/// it is pure calendar arithmetic.
fn eval_date_trunc_timestamp(units: &ArrayRef, values: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let u = downcast_array::<StringArray>(units, "text")?;
    let v = downcast_array::<TimestampMicrosecondArray>(values, "timestamp")?;
    let mut out: Vec<Option<i64>> = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        if u.is_null(i) || v.is_null(i) {
            out.push(None);
            continue;
        }
        let unit = trunc_unit(u.value(i), TY_TIMESTAMP)?;
        let local = naive_from_micros(v.value(i))?;
        let truncated = truncate_local(local, unit)
            .ok_or_else(|| unit_not_recognized(u.value(i), TY_TIMESTAMP))?;
        out.push(Some(micros_from_naive(truncated)?));
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)))
}

/// Resolve a unit for `date_trunc`, rejecting the two ways Postgres rejects.
fn trunc_unit(raw: &str, ty: &str) -> Result<DateUnit, ExecError> {
    let unit = parse_date_unit(raw).ok_or_else(|| unit_not_recognized(raw, ty))?;
    match unit {
        // Recognized units that name a *field*, not a resolution — Postgres
        // has no truncation arm for them and reports "not recognized".
        DateUnit::Dow
        | DateUnit::IsoDow
        | DateUnit::Doy
        | DateUnit::IsoYear
        | DateUnit::Julian
        | DateUnit::Epoch => Err(unit_not_recognized(raw, ty)),
        // Recognized, but a zone offset is not something a timestamp can be
        // truncated to — Postgres reports "not supported" here, a different
        // message from the arm above.
        DateUnit::Timezone | DateUnit::TimezoneHour | DateUnit::TimezoneMinute => {
            Err(unit_not_supported(raw, ty))
        }
        other => Ok(other),
    }
}

/// `date_trunc(text, timestamptz)` — oid 1217. **Session-timezone
/// dependent**: the truncation happens on the session-local rendering, not on
/// the UTC instant. See [`unit_redetermines_zone`] and
/// [`determine_time_zone_offset`] for the two rules that make it correct
/// across a DST transition.
fn eval_date_trunc_timestamptz(
    units: &ArrayRef,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let u = downcast_array::<StringArray>(units, "text")?;
    let v = downcast_array::<TimestampMicrosecondArray>(values, "timestamp with time zone")?;
    let tz = session.time_zone()?;
    let mut out: Vec<Option<i64>> = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        if u.is_null(i) || v.is_null(i) {
            out.push(None);
            continue;
        }
        let unit = trunc_unit(u.value(i), TY_TIMESTAMPTZ)?;
        let utc = naive_from_micros(v.value(i))?;
        let offset = tz.offset_from_utc_datetime(&utc).fix().local_minus_utc();
        let local = utc + TimeDelta::seconds(i64::from(offset));
        let truncated = truncate_local(local, unit)
            .ok_or_else(|| unit_not_recognized(u.value(i), TY_TIMESTAMPTZ))?;
        let out_offset = if unit_redetermines_zone(unit) {
            determine_time_zone_offset(&tz, truncated)?
        } else {
            offset
        };
        out.push(Some(micros_from_naive(
            truncated - TimeDelta::seconds(i64::from(out_offset)),
        )?));
    }
    // `timestamptz` is physically UTC micros (`basin_pgtype`'s mapping), so
    // the output carries the same "UTC" marker the input did — dropping it
    // would silently turn the result into a `timestamp without time zone`.
    Ok(Arc::new(
        TimestampMicrosecondArray::from(out).with_timezone("UTC"),
    ))
}

/// `date_trunc(text, interval)` — oid 1218. Session-independent: an interval
/// has no position on the calendar, so there is no zone to truncate in.
///
/// Two things here are not what the timestamp version does, both verified
/// live. `week` is **not supported** for an interval at all (an interval has
/// no weekday to back up to). And the year-scale units are plain division on
/// the month component rather than Postgres's off-by-one century arithmetic:
/// `date_trunc('century', interval '137 years 5 mons')` is `100 years`, not
/// `101 years`, because there is no year 1 to count from.
fn eval_date_trunc_interval(units: &ArrayRef, values: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let u = downcast_array::<StringArray>(units, "text")?;
    let v = downcast_array::<IntervalMonthDayNanoArray>(values, "interval")?;
    let mut out: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        if u.is_null(i) || v.is_null(i) {
            out.push(None);
            continue;
        }
        let raw = u.value(i);
        let unit = parse_date_unit(raw).ok_or_else(|| unit_not_recognized(raw, TY_INTERVAL))?;
        let iv = v.value(i);
        let (mut months, mut days, mut nanos) = (iv.months, iv.days, iv.nanoseconds);
        // Month-scale units zero everything below the month, then round the
        // month count down to a multiple of the unit's month width.
        let month_width = match unit {
            DateUnit::Millennium => Some(12_000),
            DateUnit::Century => Some(1_200),
            DateUnit::Decade => Some(120),
            DateUnit::Year => Some(12),
            DateUnit::Quarter => Some(3),
            DateUnit::Month => Some(1),
            _ => None,
        };
        if let Some(w) = month_width {
            months = (months / w) * w;
            days = 0;
            nanos = 0;
        } else {
            let ns_width = match unit {
                DateUnit::Day => Some(86_400_000_000_000_i64),
                DateUnit::Hour => Some(3_600_000_000_000),
                DateUnit::Minute => Some(60_000_000_000),
                DateUnit::Second => Some(1_000_000_000),
                DateUnit::Millisecond => Some(1_000_000),
                DateUnit::Microsecond => Some(1_000),
                _ => None,
            };
            match ns_width {
                // `day` keeps the day count and drops the time of day; it is
                // not a division, because an interval's days are already a
                // separate component from its time.
                Some(w) if unit == DateUnit::Day => {
                    let _ = w;
                    nanos = 0;
                }
                Some(w) => nanos = (nanos / w) * w,
                None => return Err(unit_not_supported(raw, TY_INTERVAL)),
            }
        }
        out.push(Some(IntervalMonthDayNano::new(months, days, nanos)));
    }
    Ok(Arc::new(IntervalMonthDayNanoArray::from(out)))
}

/// One `date_part` answer, off a broken-down local reading.
///
/// `offset` is the local reading's UTC offset in seconds, present only for a
/// `timestamptz`; `epoch_micros` is what the `epoch` field reports. Splitting
/// those two out is what lets the `timestamp`, `timestamptz` and `date`
/// overloads share this function while still disagreeing where Postgres does:
/// `epoch` on a `timestamp` counts from `1970-01-01 00:00:00` read as if it
/// were UTC (no zone is applied), whereas on a `timestamptz` it is the real
/// instant.
fn date_part_of(
    unit: DateUnit,
    raw: &str,
    local: NaiveDateTime,
    offset: Option<i32>,
    epoch_micros: i64,
    ty: &str,
) -> Result<f64, ExecError> {
    // Postgres reports years before 1 AD as negative and has no year zero,
    // while chrono counts astronomically (its year 0 *is* 1 BC). The two
    // agree for every year from 1 AD on; below that Postgres's number is one
    // further from zero.
    let pg_year = |y: i32| -> i32 {
        if y <= 0 {
            y - 1
        } else {
            y
        }
    };
    let secs = f64::from(local.second());
    let frac = f64::from(local.and_utc().timestamp_subsec_nanos()) / 1e9;
    Ok(match unit {
        DateUnit::Epoch => epoch_micros as f64 / 1e6,
        DateUnit::Year => f64::from(pg_year(local.year())),
        DateUnit::IsoYear => f64::from(pg_year(local.iso_week().year())),
        DateUnit::Quarter => f64::from((local.month() - 1) / 3 + 1),
        DateUnit::Month => f64::from(local.month()),
        DateUnit::Week => f64::from(local.iso_week().week()),
        DateUnit::Day => f64::from(local.day()),
        DateUnit::Doy => f64::from(local.ordinal()),
        DateUnit::Dow => f64::from(local.weekday().num_days_from_sunday()),
        DateUnit::IsoDow => f64::from(local.weekday().number_from_monday()),
        DateUnit::Hour => f64::from(local.hour()),
        DateUnit::Minute => f64::from(local.minute()),
        DateUnit::Second => secs + frac,
        DateUnit::Millisecond => (secs + frac) * 1e3,
        DateUnit::Microsecond => (secs + frac) * 1e6,
        DateUnit::Decade => {
            let y = local.year();
            f64::from(if y >= 0 { y / 10 } else { -((8 - (y - 1)) / 10) })
        }
        DateUnit::Century => {
            let y = local.year();
            f64::from(if y > 0 {
                (y + 99) / 100
            } else {
                -((99 - (y - 1)) / 100)
            })
        }
        DateUnit::Millennium => {
            let y = local.year();
            f64::from(if y > 0 {
                (y + 999) / 1000
            } else {
                -((999 - (y - 1)) / 1000)
            })
        }
        // Julian date: the Julian day number of the local date, plus the
        // fraction of the day elapsed. `num_days_from_ce` counts from
        // 0001-01-01 as day 1, whose (proleptic Gregorian) Julian day number
        // is 1721426, so the two differ by the constant below. Checked
        // against the server:
        // `date_part('julian', '2024-03-10 12:34:56.789012')` is
        // 2460380.5242683915.
        DateUnit::Julian => {
            let jdn = f64::from(local.date().num_days_from_ce()) + 1_721_425.0;
            let secs_of_day = f64::from(local.num_seconds_from_midnight()) + frac;
            jdn + secs_of_day / 86_400.0
        }
        DateUnit::Timezone | DateUnit::TimezoneHour | DateUnit::TimezoneMinute => {
            let off = offset.ok_or_else(|| unit_not_supported(raw, ty))?;
            match unit {
                DateUnit::Timezone => f64::from(off),
                DateUnit::TimezoneHour => f64::from(off / 3600),
                _ => f64::from((off % 3600) / 60),
            }
        }
    })
}

/// The shared body of all three `date_part` overloads. `reading` turns one
/// row's stored value into `(local reading, UTC offset if any, epoch
/// micros)`.
fn eval_date_part_rows(
    units: &ArrayRef,
    len: usize,
    is_null: impl Fn(usize) -> bool,
    reading: impl Fn(usize) -> Result<(NaiveDateTime, Option<i32>, i64), ExecError>,
    ty: &str,
) -> Result<ArrayRef, ExecError> {
    let u = downcast_array::<StringArray>(units, "text")?;
    let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
    for i in 0..len {
        if u.is_null(i) || is_null(i) {
            out.push(None);
            continue;
        }
        let raw = u.value(i);
        let unit = parse_date_unit(raw).ok_or_else(|| unit_not_recognized(raw, ty))?;
        let (local, offset, epoch_micros) = reading(i)?;
        out.push(Some(date_part_of(
            unit,
            raw,
            local,
            offset,
            epoch_micros,
            ty,
        )?));
    }
    Ok(Arc::new(Float64Array::from(out)))
}

/// `date_part(text, timestamp)` — oid 2021.
fn eval_date_part_timestamp(units: &ArrayRef, values: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let v = downcast_array::<TimestampMicrosecondArray>(values, "timestamp")?.clone();
    let n = v.len();
    let w = v.clone();
    eval_date_part_rows(
        units,
        n,
        |i| v.is_null(i),
        move |i| Ok((naive_from_micros(w.value(i))?, None, w.value(i))),
        TY_TIMESTAMP,
    )
}

/// `date_part(text, timestamptz)` — oid 1171. **Session-timezone
/// dependent**: every field except `epoch` is read off the session-local
/// rendering, and `timezone`/`timezone_hour`/`timezone_minute` report that
/// rendering's UTC offset. Verified live in `Australia/Lord_Howe`, where the
/// same instant gives `timezone = 37800` (`+10:30`) and `timezone_minute =
/// 30` — a zone whose offset is not a whole number of hours, which is the
/// case an implementation that stores offsets in hours gets wrong.
fn eval_date_part_timestamptz(
    units: &ArrayRef,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let v = downcast_array::<TimestampMicrosecondArray>(values, "timestamp with time zone")?
        .clone();
    let tz = session.time_zone()?;
    let n = v.len();
    let w = v.clone();
    eval_date_part_rows(
        units,
        n,
        |i| v.is_null(i),
        move |i| {
            let micros = w.value(i);
            let utc = naive_from_micros(micros)?;
            let offset = tz.offset_from_utc_datetime(&utc).fix().local_minus_utc();
            Ok((
                utc + TimeDelta::seconds(i64::from(offset)),
                Some(offset),
                micros,
            ))
        },
        TY_TIMESTAMPTZ,
    )
}

/// `date_part(text, date)` — oid 1384.
///
/// Postgres implements this by casting the `date` to a `timestamp` and
/// reusing that path, which is visible in its error messages: an unknown unit
/// on a `date` argument is reported against `timestamp without time zone`,
/// not against `date`. That is reproduced here rather than corrected.
fn eval_date_part_date(units: &ArrayRef, values: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let v = downcast_array::<Date32Array>(values, "date")?.clone();
    let n = v.len();
    let w = v.clone();
    eval_date_part_rows(
        units,
        n,
        |i| v.is_null(i),
        move |i| {
            let micros = i64::from(w.value(i)) * 86_400 * 1_000_000;
            Ok((naive_from_micros(micros)?, None, micros))
        },
        TY_TIMESTAMP,
    )
}

// ─── date_part(text, interval) ──────────────────────────────────────────────
//
// DESIGN, written before the implementation because every number in it was
// measured on a live PostgreSQL 18.2 and none of it can be recovered by
// reasoning from the timestamp version.
//
// oid 1172 is `interval_part`, a C function in its own right — NOT
// `extract_interval` with a cast, and NOT `date_part(text, timestamp)` with a
// conversion. `pg_proc.prosrc` says so, and so does the arithmetic:
//
//   date_part('second', interval '56.789123 sec')  = 56.789123000000004
//   extract  (second from interval '56.789123 sec') = 56.789123
//
// The trailing `...004` is the signature of `tm_sec + fsec / 1000000.0` done
// in `double`. A cast of the `numeric` would have produced the clean value.
// So this cannot share [`date_part_of`]'s call path, and it cannot share
// [`extract_value`]'s either.
//
// WHY IT CANNOT REUSE [`date_part_of`]: an interval is not an instant. Basin
// stores it as `Interval(MonthDayNano)` with months, days and nanos kept
// separately and deliberately unnormalised, and Postgres does the same
// (`months`, `days`, `time`). `INTERVAL '1 day'` is not `INTERVAL '24 hours'`,
// and every field below reads the *stored component* rather than a calendar:
//
//   date_part('day',  interval '1 month')     = 0     -- not 30
//   date_part('hour', interval '1 day -2 hours') = -2 -- days keep their sign
//   date_part('hour', interval '100000 hours')   = 100000  -- no mod 24
//
// `justify_days`/`justify_hours` are NOT applied implicitly: the mixed-sign
// `interval '1 day -2 hours'` reports day = 1 and hour = -2, and its epoch is
// 79200 (= 86400 - 7200), all measured.
//
// THE FIELD TABLE, every entry measured (`m` = months, `d` = days, `n` =
// nanos; `/` and `%` truncate toward zero, as C does):
//
//   year         m / 12                        '-13 mons' -> -1
//   month        m % 12                        '-13 mons' -> -1
//   decade       (m / 12) / 10                 '1999 years' -> 199
//   century      (m / 12) / 100                '1999 years' -> 19, '-250 y' -> -2
//   millennium   (m / 12) / 1000               '2500 years' -> 2, '-2500 y' -> -2
//   quarter      see below                     '-1 mons' -> -1, '0' -> 1
//   day          d                             '1 day -2 hours' -> 1
//   week         d / 7                          '20 days' -> 2, '-20 days' -> -2
//   hour         n / 3600e9                     '25 hours' -> 25 (no wrap)
//   minute       (n / 60e9) % 60                '90 minutes' -> 30
//   second       s + f/1e6                      '-0.5 s' -> -0.5
//   milliseconds s*1000 + f/1000                '56.789123 s' -> 56789.123
//   microseconds s*1e6 + f                      '56.789123 s' -> 56789123
//   epoch        see below                      full convention below
//
// where `s` is the whole seconds inside the minute (`(n % 60e9) / 1e9`) and
// `f` is the remaining microseconds (`(n % 1e9) / 1000`), both signed with
// `n`. The three sub-minute units are one integer read at three scales, and
// the `f64` expressions above are Postgres's own — writing `s as f64 +
// frac_ns as f64 / 1e9` instead would produce a *different last bit* on
// `56.789123`, which the differential battery would flag.
//
// QUARTER is the one field whose formula is not the timestamp one and not the
// obvious one. Measured across `make_interval(months => n)` for n in -26..26,
// with `mm = m % 12`:
//
//   mm   0  1  2  3  4  5  6  7  8  9 10 11
//   q    1  1  1  2  2  2  3  3  3  4  4  4      -> mm / 3 + 1
//   mm  -1 -2 -3 -4 -5 -6 -7 -8 -9 -10 -11
//   q   -1 -1 -2 -2 -2 -3 -3 -3 -4  -4  -4      -> mm / 3 - 1
//
// A plain `mm / 3 + 1` gives 1 for `mm = -1`; Postgres gives -1. The branch,
// measured, is on the sign of `m` and NOT on the sign of `mm` — the two part
// company exactly on the whole-year negatives, where `mm` is zero:
//
//   date_part('quarter', interval '-24 months') = -1
//   date_part('quarter', interval '0')          =  1
//   date_part('quarter', interval '-1000 years')= -1
//
// Days and nanos do not participate at all: `date_part('quarter', interval
// '-5 days')` is 1, and `interval '-2 years +5 days 03:00:00'` is still -1.
//
// EPOCH is a fixed convention, not an instant, and it is the number this
// function exists to get right. Postgres's `interval_part` computes, in this
// order and in `double`:
//
//   time / 1e6                              -- time is MICROseconds
//     + 86400 * 365.25 * (months / 12)      -- DAYS_PER_YEAR, truncating div
//     + 86400 * 30     * (months % 12)      -- DAYS_PER_MONTH
//     + 86400 * days
//
// The order — year, then month, then day, with the time term first — is not
// cosmetic. A differential sweep against the live server found exactly one
// interval where the day-term-first spelling lands one ulp away (months -453,
// days 1037, nanos -393922588884000: Postgres says -1101756322.588884, the
// reassociated sum says -1101756322.5888839), and dividing nanos by 1e9
// rather than micros by 1e6 does the same. With both spellings matched, every
// unit in this file's table is **bit-exact** against PostgreSQL 18.2 over
// 47,782 sampled `(months, days, nanos, unit)` call sites.
//
// A year is 365.25 days and a month is 30 days, and the split between them is
// integer division of the month count by 12 — so 14 months is *one 365.25-day
// year plus two 30-day months*, not fourteen 30-day months. Measured:
//
//   date_part('epoch', interval '1 year 2 mons 3 days 4:05:06.789012')
//     = 37015506.789012          -- 31557600 + 5184000 + 259200 + 14706.789012
//   date_part('epoch', interval '13 mons')  =  34149600
//   date_part('epoch', interval '-13 mons') = -34149600   -- trunc, not floor
//   date_part('epoch', interval '178000000 years') = 5.6172528e+15
//
// Reproducing that summation order matters: reassociating it changes the last
// bit on the fractional cases.
//
// UNITS POSTGRES REFUSES, all message strings measured verbatim. The
// timestamp version accepts every one of these:
//
//   dow, doy, isodow, isoyear, julian (and its alias `jd`),
//   timezone, timezone_hour, timezone_minute (and `timezone_h`/`timezone_m`)
//     -> unit "X" not supported for type interval
//
// `week` is the surprise in the other direction: `date_trunc(text, interval)`
// refuses it, but `date_part('week', interval)` is ACCEPTED and returns
// `days / 7`. The hint from commit 6264603c does not carry over, and the two
// functions genuinely disagree on this one unit.
//
// The unit *vocabulary* is shared with the timestamp version — `millenium`
// (misspelt) and `fortnight` are `not recognized for type interval`, while
// every alias in [`parse_date_unit`] resolves. So [`parse_date_unit`] is
// reused unchanged and only the per-unit answer differs.
//
// EXTRACT(… FROM interval) — oid 6204 — was compared while measuring, as the
// brief asked. It agrees with `date_part` on every accepted unit's *value*
// and on every rejection, differing only in returning `numeric`. It is NOT
// implemented here: `extract`'s Decimal128 path needs a per-unit scale, and
// the interval scales were not measured (only the values were), so declaring
// them would be guessing. See the note at [`temporal_kind`], which continues
// to refuse intervals.

/// `date_part(text, interval)` — oid 1172. Session-independent: an interval
/// has no position on the calendar.
///
/// Postgres's `interval_part`. Reads the three stored components and never
/// normalises between them; see the design block above this function for the
/// full measured field table, the `quarter` sign rule, the 365.25/30-day
/// `epoch` convention and the list of refused units.
fn eval_date_part_interval(units: &ArrayRef, values: &ArrayRef) -> Result<ArrayRef, ExecError> {
    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

    let u = downcast_array::<StringArray>(units, "text")?;
    let v = downcast_array::<IntervalMonthDayNanoArray>(values, "interval")?;
    let mut out: Vec<Option<f64>> = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        if u.is_null(i) || v.is_null(i) {
            out.push(None);
            continue;
        }
        let raw = u.value(i);
        let unit = parse_date_unit(raw).ok_or_else(|| unit_not_recognized(raw, TY_INTERVAL))?;
        let iv = v.value(i);
        let (months, days, nanos) = (iv.months, iv.days, iv.nanoseconds);

        // Postgres's `tm_sec` and `fsec`: the whole seconds inside the
        // minute, and the leftover as microseconds. Both carry the sign of
        // `nanos`, because `%` truncates toward zero in Rust as in C — which
        // is what makes `interval '-0.5 sec'` report second = -0.5 rather
        // than 59.5.
        let whole_sec = (nanos % NS_PER_MIN) / NS_PER_SEC;
        // `frac_ns / 1000.0` rather than an integer `/ 1000`: for any nanos
        // Postgres could have produced (whole microseconds) the two agree
        // bit-for-bit, and for a sub-microsecond nanos — which Postgres
        // cannot represent at all — this keeps the digits instead of
        // silently dropping them.
        let fsec = (nanos % NS_PER_SEC) as f64 / 1000.0;
        let years = months / 12;
        let rem_months = months % 12;

        let answer = match unit {
            DateUnit::Year => f64::from(years),
            DateUnit::Month => f64::from(rem_months),
            DateUnit::Decade => f64::from(years / 10),
            DateUnit::Century => f64::from(years / 100),
            DateUnit::Millennium => f64::from(years / 1000),
            // The sign rule measured across months -26..26; see the design
            // block. The branch is on `months`, NOT on `rem_months`:
            // `interval '-24 months'` has `rem_months == 0` and reports -1,
            // while `interval '0'` reports 1. Days and nanos do not
            // participate — `interval '-5 days'` is quarter 1.
            DateUnit::Quarter => f64::from(if months < 0 {
                rem_months / 3 - 1
            } else {
                rem_months / 3 + 1
            }),
            DateUnit::Day => f64::from(days),
            // Accepted for `date_part` even though `date_trunc(text,
            // interval)` refuses it. Measured: 20 days -> 2, -20 days -> -2.
            DateUnit::Week => f64::from(days / 7),
            // Not reduced mod 24: an interval's days are a separate
            // component, so `interval '100000 hours'` reports 100000.
            DateUnit::Hour => (nanos / NS_PER_HOUR) as f64,
            DateUnit::Minute => ((nanos / NS_PER_MIN) % 60) as f64,
            DateUnit::Second => whole_sec as f64 + fsec / 1_000_000.0,
            DateUnit::Millisecond => whole_sec as f64 * 1000.0 + fsec / 1000.0,
            DateUnit::Microsecond => whole_sec as f64 * 1_000_000.0 + fsec,
            // Postgres's summation order, kept verbatim — reassociating it
            // moves the last bit on the fractional cases.
            DateUnit::Epoch => {
                // Postgres divides its microsecond `time` by 1e6; dividing
                // nanos by 1e9 instead differs by one ulp on some values
                // (measured: months -453, days 1037, nanos -393922588884000).
                // The `% 1000` term is zero for every interval Postgres can
                // represent and only carries a sub-microsecond nanos through.
                let mut result = (nanos / 1000) as f64 / 1e6 + (nanos % 1000) as f64 / 1e9;
                // Year, then month, then day — Postgres's order, and it is
                // load-bearing: summing the day term first moves the last bit
                // on the case named above.
                result += 86_400.0 * 365.25 * f64::from(years);
                result += 86_400.0 * 30.0 * f64::from(rem_months);
                result += 86_400.0 * f64::from(days);
                result
            }
            // Every unit the timestamp version answers and the interval one
            // does not. All eight messages measured verbatim on 18.2.
            DateUnit::Dow
            | DateUnit::IsoDow
            | DateUnit::Doy
            | DateUnit::IsoYear
            | DateUnit::Julian
            | DateUnit::Timezone
            | DateUnit::TimezoneHour
            | DateUnit::TimezoneMinute => return Err(unit_not_supported(raw, TY_INTERVAL)),
        };
        out.push(Some(answer));
    }
    Ok(Arc::new(Float64Array::from(out)))
}

// ─── extract ────────────────────────────────────────────────────────────────
//
// `extract(FIELD FROM value)` is NOT `date_part` with a cast, and the two
// differences were both measured on a live PostgreSQL 18.2:
//
//  1. **`extract` returns `numeric`; `date_part` returns `float8`.** That is
//     not a rendering difference, it is digits:
//
//     ```text
//     extract  (epoch  from '4024-03-15 12:34:56.654321+02'::timestamptz)
//       = 64824402896.654321   -- numeric
//     date_part('epoch',       '4024-03-15 12:34:56.654321+02'::timestamptz)
//       = 64824402896.65432    -- float8, the last digit is gone
//
//     extract  (second from '2024-03-15 12:34:56.789123'::timestamp)
//       = 56.789123
//     date_part('second',   '2024-03-15 12:34:56.789123'::timestamp)
//       = 56.789123000000004
//     ```
//
//  2. **They accept different units on a `date`.** `date_part(text, date)`
//     (1384) is a SQL wrapper whose body casts to `timestamp` first, so
//     `date_part('hour', DATE '2024-03-15')` is `0`. `extract(text, date)`
//     (6199) is the C function `extract_date`, which has no such cast, so
//     `extract(hour FROM DATE '2024-03-15')` is the error `unit "hour" not
//     supported for type date`. [`extract_scale`] is where that difference
//     lives.
//
// The `numeric` return is the reason this is a separate implementation rather
// than a cast of [`eval_date_part_rows`]'s `f64`: going through `f64` would
// reintroduce exactly the rounding the first difference above is about.

/// Which of the temporal types an `extract` call's second argument actually
/// is, read from the argument's **Arrow type** rather than from the resolved
/// `pg_proc` oid.
///
/// Reading the runtime type is not a shortcut around the catalog, it is the
/// only sound dispatch available here. `basin_plan::lower::expr::
/// best_effort_type` reports `unknown` (oid 705) for any expression that is
/// not a literal, cast or parameter — a bare `ColumnRef` carries no type —
/// so `extract(EPOCH FROM ts)` and `extract(YEAR FROM day)` both reach
/// `basin_pgtype::func::resolve` as `extract(unknown, unknown)` and both come
/// back as the *same* oid, whichever row is first in the table. The oid is
/// therefore known-unreliable input, while the Arrow type of the evaluated
/// argument is ground truth. Postgres's six `extract` oids are one function
/// differing only by argument type, so dispatching on the type that is
/// actually present reproduces Postgres's answer for every one of them;
/// dispatching on the oid would reproduce it for at most one.
///
/// The same reasoning does not rescue `date_part`, whose three oids each
/// downcast to a fixed array type — `date_part('month', day)` still resolves
/// to 1171 (`timestamptz`) and still fails on a `Date32` argument. That is a
/// pre-existing gap, left alone here rather than widened by accident.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TemporalKind {
    Date,
    Timestamp,
    TimestampTz,
}

impl TemporalKind {
    /// Postgres's own spelling of the type, for the error messages
    /// [`unit_not_supported`] and [`unit_not_recognized`] build.
    fn type_name(self) -> &'static str {
        match self {
            TemporalKind::Date => TY_DATE,
            TemporalKind::Timestamp => TY_TIMESTAMP,
            TemporalKind::TimestampTz => TY_TIMESTAMPTZ,
        }
    }
}

const TY_DATE: &str = "date";

/// Classify an evaluated temporal argument, or explain what is missing.
///
/// `time`, `timetz` and `interval` are real `extract`/`to_char` argument
/// types in Postgres (oids 6200, 6201, 6204, 1768) and are deliberately NOT
/// handled: each needs its own field-extraction rules, and answering them
/// from the `timestamp` rules would be a wrong answer rather than a missing
/// one. They fail here, which falls back.
fn temporal_kind(values: &ArrayRef, what: &'static str) -> Result<TemporalKind, ExecError> {
    match values.data_type() {
        DataType::Date32 => Ok(TemporalKind::Date),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            Ok(TemporalKind::Timestamp)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_)) => {
            Ok(TemporalKind::TimestampTz)
        }
        other => Err(ExecError::Internal(format!(
            "{what} on {other:?} is not implemented — only date, timestamp and timestamptz are"
        ))),
    }
}

/// One row's reading of a temporal value: the local (session-rendered, for a
/// `timestamptz`) civil time, the UTC offset if the type has one, and the
/// microseconds since the Unix epoch. The same triple
/// [`eval_date_part_rows`] passes to [`date_part_of`], so the two functions
/// agree on every field either of them derives from it.
type TemporalReading = (NaiveDateTime, Option<i32>, i64);

/// Read every row of `values` as a [`TemporalReading`], `None` for SQL NULL.
fn temporal_readings(
    kind: TemporalKind,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<Vec<Option<TemporalReading>>, ExecError> {
    let n = values.len();
    let mut out = Vec::with_capacity(n);
    match kind {
        TemporalKind::Date => {
            let v = downcast_array::<Date32Array>(values, "date")?;
            for i in 0..n {
                if v.is_null(i) {
                    out.push(None);
                    continue;
                }
                let micros = i64::from(v.value(i)) * 86_400 * 1_000_000;
                out.push(Some((naive_from_micros(micros)?, None, micros)));
            }
        }
        TemporalKind::Timestamp => {
            let v = downcast_array::<TimestampMicrosecondArray>(values, "timestamp")?;
            for i in 0..n {
                if v.is_null(i) {
                    out.push(None);
                    continue;
                }
                let micros = v.value(i);
                out.push(Some((naive_from_micros(micros)?, None, micros)));
            }
        }
        TemporalKind::TimestampTz => {
            let v =
                downcast_array::<TimestampMicrosecondArray>(values, "timestamp with time zone")?;
            let tz = session.time_zone()?;
            for i in 0..n {
                if v.is_null(i) {
                    out.push(None);
                    continue;
                }
                let micros = v.value(i);
                let utc = naive_from_micros(micros)?;
                let offset = tz.offset_from_utc_datetime(&utc).fix().local_minus_utc();
                out.push(Some((
                    utc + TimeDelta::seconds(i64::from(offset)),
                    Some(offset),
                    micros,
                )));
            }
        }
    }
    Ok(out)
}

/// The `numeric` scale `extract` produces for this unit and argument type —
/// and, in the same pass, the rejection of units this argument type does not
/// accept.
///
/// Arrow's `Decimal128` carries ONE scale for a whole array, while Postgres's
/// `numeric` carries a per-value `dscale`. That is workable only because the
/// scale is a function of the unit and the argument type, both fixed for the
/// whole call — which is why [`eval_extract`] insists on a literal unit. Every
/// scale below was read off a live PostgreSQL 18.2 with
/// `scale(pg_catalog.extract(<unit>, <value>))`:
///
/// ```text
/// unit          date   timestamp / timestamptz
/// epoch            0   6          (1710506096.789123)
/// second         n/a   6          (56.789123, and 56.000000 on a whole second)
/// milliseconds   n/a   3          (56789.123)
/// microseconds   n/a   0          (56789123)
/// everything     0     0
/// ```
///
/// `julian` is the one unit refused for a reason other than Postgres refusing
/// it: on a `timestamp` its scale is neither fixed nor small — measured 20 for
/// `2024-03-15 12:34:56.789123` and 28 for the same date at midnight, because
/// Postgres computes it as a `numeric` division whose `dscale` floats. No
/// single Arrow scale reproduces that, so it fails closed instead of answering
/// to the wrong precision. On a `date` there is no division and the answer is
/// the integer Julian day, which IS produced.
fn extract_scale(unit: DateUnit, kind: TemporalKind, raw: &str) -> Result<i8, ExecError> {
    let ty = kind.type_name();
    if kind == TemporalKind::Date {
        // `extract_date` accepts no sub-day unit and no zone field at all,
        // which is exactly where it parts company with `date_part(text,
        // date)`. All four messages measured live.
        return match unit {
            DateUnit::Hour
            | DateUnit::Minute
            | DateUnit::Second
            | DateUnit::Millisecond
            | DateUnit::Microsecond
            | DateUnit::Timezone
            | DateUnit::TimezoneHour
            | DateUnit::TimezoneMinute => Err(unit_not_supported(raw, ty)),
            _ => Ok(0),
        };
    }
    if unit == DateUnit::Julian {
        return Err(ExecError::Internal(format!(
            "extract(julian from {ty}) has no fixed numeric scale in Postgres (measured 20 and \
             28 digits for two values of the same column) and cannot be represented as one \
             Decimal128 scale"
        )));
    }
    Ok(match unit {
        DateUnit::Epoch | DateUnit::Second => 6,
        DateUnit::Millisecond => 3,
        _ => 0,
    })
}

/// The unscaled `Decimal128` value for one row, at the scale
/// [`extract_scale`] already fixed for the whole array.
///
/// The four sub-second-bearing units are computed here in exact integer
/// microseconds. Every other unit — including the calendar arithmetic for
/// `decade`/`century`/`millennium`/`julian` and the `timezone*` fields, each
/// of which has its own already-verified Postgres rule — is delegated to
/// [`date_part_of`], whose answers are integers for those units and so
/// survive `f64` exactly. Sharing that function is deliberate: `extract` and
/// `date_part` must not drift apart on the units where Postgres says they
/// agree.
fn extract_value(
    unit: DateUnit,
    raw: &str,
    kind: TemporalKind,
    local: NaiveDateTime,
    offset: Option<i32>,
    epoch_micros: i64,
) -> Result<i128, ExecError> {
    // Seconds-and-fraction of the minute, in exact microseconds.
    let sec_micros = i128::from(local.second()) * 1_000_000
        + i128::from(local.and_utc().timestamp_subsec_micros());
    Ok(match unit {
        // scale 6 on a timestamp (whole microseconds), scale 0 on a date
        // (where Postgres reports whole seconds — the value is always a whole
        // number of days from the epoch, so the division is exact).
        DateUnit::Epoch => match kind {
            TemporalKind::Date => i128::from(epoch_micros) / 1_000_000,
            _ => i128::from(epoch_micros),
        },
        // All three are the same integer microsecond count read at three
        // different scales: 56.789123 (scale 6), 56789.123 (scale 3) and
        // 56789123 (scale 0) are one number, formatted three ways.
        DateUnit::Second | DateUnit::Millisecond | DateUnit::Microsecond => sec_micros,
        _ => {
            let as_f64 = date_part_of(unit, raw, local, offset, epoch_micros, kind.type_name())?;
            as_f64 as i128
        }
    })
}

/// `extract(FIELD FROM value)` — Postgres oids 6199-6204, one implementation.
///
/// The unit must be a literal. Postgres's SQL-standard `EXTRACT(YEAR FROM x)`
/// syntax can spell it no other way, and `pg_query` lowers that keyword to a
/// string constant; the function-call spelling `extract(col, x)` with a
/// *varying* unit is legal Postgres but is refused here, because the result's
/// `numeric` scale depends on the unit (see [`extract_scale`]) and an Arrow
/// array has one scale for every row. Refusing falls back, which answers it
/// correctly by another route; guessing a scale would not.
fn eval_extract(
    field: Option<&Expr>,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let raw = match field {
        Some(Expr::Literal(PlanDatum::Utf8(s), _)) => s.as_str(),
        _ => {
            return Err(ExecError::Internal(
                "extract with a non-literal field is not implemented — the result's numeric \
                 scale depends on the field, and one Arrow array carries one scale"
                    .into(),
            ))
        }
    };
    let kind = temporal_kind(values, "extract")?;
    let unit = parse_date_unit(raw).ok_or_else(|| unit_not_recognized(raw, kind.type_name()))?;
    let scale = extract_scale(unit, kind, raw)?;

    let readings = temporal_readings(kind, values, session)?;
    let mut out: Vec<Option<i128>> = Vec::with_capacity(readings.len());
    for reading in readings {
        match reading {
            None => out.push(None),
            Some((local, offset, epoch_micros)) => out.push(Some(extract_value(
                unit,
                raw,
                kind,
                local,
                offset,
                epoch_micros,
            )?)),
        }
    }
    // Precision 38 is the widest `Decimal128` holds and is what
    // `basin_pgtype::physical` already maps an unconstrained `numeric` to, so
    // this reports the same width the rest of the engine uses for a `numeric`
    // with no declared precision.
    let array = Decimal128Array::from(out)
        .with_precision_and_scale(38, scale)
        .map_err(|e| map_arrow(e, "extract"))?;
    Ok(Arc::new(array))
}

// ─── to_char (date/time) ────────────────────────────────────────────────────

/// `to_char(timestamp, text)` (2049) and `to_char(timestamptz, text)` (1770).
///
/// **A `date` argument is handled here too, and that is not an extra
/// overload.** PostgreSQL has no `to_char(date, text)` — enumerating
/// `pg_catalog` on a live server gives exactly eight `to_char` rows, none of
/// them taking a `date` — so `to_char(day, 'YYYY-MM-DD')` resolves to 2049 by
/// the implicit `date -> timestamp` cast (`pg_cast.castcontext = 'i'`, read
/// off the same server). Basin's lowering does not insert that cast, so the
/// argument arrives as a `Date32` under oid 2049 and the widening happens
/// here instead: midnight of that day, which is precisely what the cast
/// Postgres inserts produces — `to_char(DATE '2024-03-05', 'YYYY-MM-DD
/// HH24:MI:SS')` is `2024-03-05 00:00:00` live.
///
/// A `timestamptz` argument renders in the session's `TimeZone`, which is why
/// both oids are `provolatile = 's'` in `pg_proc`.
fn eval_to_char_datetime(
    values: &ArrayRef,
    formats: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let kind = temporal_kind(values, "to_char")?;
    let f = downcast_array::<StringArray>(formats, "text")?;
    let readings = temporal_readings(kind, values, session)?;

    let mut out: Vec<Option<String>> = Vec::with_capacity(readings.len());
    for (i, reading) in readings.into_iter().enumerate() {
        match reading {
            Some((local, _, _)) if !f.is_null(i) => {
                out.push(Some(format_datetime(f.value(i), local)?))
            }
            _ => out.push(None),
        }
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// One `to_char` datetime template pattern: the literal Postgres spells it
/// with, and how to render it — or `None` for a pattern that is **real in
/// Postgres but not implemented here**.
///
/// The `None` entries are not padding. Matching is longest-first, and a
/// pattern this table did not know about would be silently chewed up by
/// shorter ones that happen to be prefixes of it: `SSSS` (seconds past
/// midnight) matched as `SS` twice renders `2024-03-10 06:30:00` as `0000`
/// instead of `23400` — a wrong answer, produced without any error, and
/// caught by [`to_char_refuses_an_unimplemented_template_pattern`] only
/// because that test asserts the refusal rather than the digits. Listing
/// every pattern Postgres has, implemented or not, is what makes
/// longest-first matching safe.
type DatePattern = (&'static str, Option<fn(NaiveDateTime) -> String>);

/// Postgres's month names, in the `C`/English locale `lc_time` defaults to.
const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// Postgres's day names, Sunday first — the order `to_char`'s own `D` field
/// numbers them in.
const DAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// Postgres's year numbering, which has no year zero: chrono counts
/// astronomically (its year 0 IS 1 BC), so anything at or below zero is one
/// further from zero in Postgres's numbering. The same adjustment
/// [`date_part_of`] makes for the `year` field.
fn pg_year_of(dt: NaiveDateTime) -> i32 {
    let y = dt.year();
    if y <= 0 {
        y - 1
    } else {
        y
    }
}

/// The absolute value of the Postgres year: `to_char` prints the digits and
/// leaves the era to the `BC`/`AD` patterns (which are not implemented).
/// Confirmed live: `to_char(DATE '0044-03-15 BC', 'YYYY')` is `0044`.
fn abs_pg_year(dt: NaiveDateTime) -> u32 {
    pg_year_of(dt).unsigned_abs()
}

/// Blank-pad to the width Postgres pads its full month and day names to,
/// measured live: `length(to_char(DATE '2024-03-05', 'MONTH'))` is 9, and so
/// is `DAY`'s — the width of `September` and `Wednesday`.
fn pad9(s: &str) -> String {
    format!("{s:<9}")
}

/// Every datetime template pattern PostgreSQL 18 has, longest first, with the
/// subset this evaluator renders. `None` means Postgres has it and this does
/// not — see [`DatePattern`] for why those entries are load-bearing rather
/// than documentation.
///
/// Each implemented rendering was checked against a live PostgreSQL 18.2 with
/// `to_char(TIMESTAMP '2024-03-05 14:07:09.123456', <pattern>)`, whose
/// answers are quoted per group. The `None` list is Postgres's own template
/// table: the era, Roman-numeral, ISO-year, Julian-day, century,
/// seconds-past-midnight, fractional-second and time-zone patterns, plus the
/// `FM`/`FX`/`TM`/`TH`/`th`/`SP` modifiers.
const DATE_PATTERNS: &[DatePattern] = &[
    // ── 5 ──
    ("SSSSS", None), // seconds past midnight
    ("MONTH", Some(|d| {
        pad9(MONTH_NAMES[d.month0() as usize]).to_uppercase()
    })), // "MARCH    "
    ("Month", Some(|d| pad9(MONTH_NAMES[d.month0() as usize]))), // "March    "
    ("month", Some(|d| {
        pad9(MONTH_NAMES[d.month0() as usize]).to_lowercase()
    })), // "march    "
    ("Y,YYY", None), // year with a comma
    // ── 4 ──
    ("HH24", Some(|d| format!("{:02}", d.hour()))), // "14"
    ("HH12", Some(|d| format!("{:02}", hour12(d)))), // "02"
    ("YYYY", Some(|d| format!("{:04}", abs_pg_year(d)))), // "2024"
    ("IYYY", None), // ISO 8601 week-numbering year
    ("IDDD", None), // ISO day of the week-numbering year
    ("SSSS", None), // seconds past midnight
    ("A.M.", None),
    ("a.m.", None),
    ("P.M.", None),
    ("p.m.", None),
    ("B.C.", None),
    ("b.c.", None),
    ("A.D.", None),
    ("a.d.", None),
    // ── 3 ──
    ("DDD", Some(|d| format!("{:03}", d.ordinal()))), // "065"
    ("DAY", Some(|d| {
        pad9(DAY_NAMES[d.weekday().num_days_from_sunday() as usize]).to_uppercase()
    })), // "TUESDAY  "
    ("Day", Some(|d| {
        pad9(DAY_NAMES[d.weekday().num_days_from_sunday() as usize])
    })), // "Tuesday  "
    ("day", Some(|d| {
        pad9(DAY_NAMES[d.weekday().num_days_from_sunday() as usize]).to_lowercase()
    })), // "tuesday  "
    ("MON", Some(|d| {
        MONTH_NAMES[d.month0() as usize][..3].to_uppercase()
    })), // "MAR"
    ("Mon", Some(|d| MONTH_NAMES[d.month0() as usize][..3].to_string())), // "Mar"
    ("mon", Some(|d| {
        MONTH_NAMES[d.month0() as usize][..3].to_lowercase()
    })), // "mar"
    ("YYY", Some(|d| format!("{:03}", abs_pg_year(d) % 1_000))), // "024"
    ("IYY", None),
    ("TZH", None),
    ("TZM", None),
    ("FF1", None),
    ("FF2", None),
    ("FF3", None),
    ("FF4", None),
    ("FF5", None),
    ("FF6", None),
    // ── 2 ──
    // `HH` is `HH12`, NOT `HH24` — the trap this group exists to get right:
    // "02" for 14:07, measured.
    ("MM", Some(|d| format!("{:02}", d.month()))), // "03"
    ("DD", Some(|d| format!("{:02}", d.day()))),   // "05"
    ("HH", Some(|d| format!("{:02}", hour12(d)))), // "02"
    ("MI", Some(|d| format!("{:02}", d.minute()))), // "07"
    ("SS", Some(|d| format!("{:02}", d.second()))), // "09"
    ("MS", Some(|d| {
        format!("{:03}", d.and_utc().timestamp_subsec_millis())
    })), // "123"
    ("US", Some(|d| {
        format!("{:06}", d.and_utc().timestamp_subsec_micros())
    })), // "123456"
    ("YY", Some(|d| format!("{:02}", abs_pg_year(d) % 100))), // "24"
    ("WW", Some(|d| format!("{:02}", (d.ordinal() - 1) / 7 + 1))), // "10"
    ("IW", Some(|d| format!("{:02}", d.iso_week().week()))), // "10"
    ("ID", Some(|d| d.weekday().number_from_monday().to_string())), // "2"
    ("DY", Some(|d| {
        DAY_NAMES[d.weekday().num_days_from_sunday() as usize][..3].to_uppercase()
    })), // "TUE"
    ("Dy", Some(|d| {
        DAY_NAMES[d.weekday().num_days_from_sunday() as usize][..3].to_string()
    })), // "Tue"
    ("dy", Some(|d| {
        DAY_NAMES[d.weekday().num_days_from_sunday() as usize][..3].to_lowercase()
    })), // "tue"
    ("AM", Some(meridiem_upper)), // "PM" — the pattern's case, the value's meaning
    ("PM", Some(meridiem_upper)),
    ("am", Some(meridiem_lower)),
    ("pm", Some(meridiem_lower)),
    ("IY", None),
    ("BC", None),
    ("bc", None),
    ("AD", None),
    ("ad", None),
    ("CC", None), // century
    ("RM", None), // Roman-numeral month
    ("rm", None),
    ("TZ", None),
    ("tz", None),
    ("OF", None),
    ("FM", None), // fill mode (suppress padding/zeroes)
    ("FX", None), // fixed format global option
    ("TM", None), // translation mode (localized names)
    ("TH", None), // ordinal suffix
    ("th", None),
    ("SP", None), // spell mode
    // ── 1 ──
    ("Y", Some(|d| (abs_pg_year(d) % 10).to_string())), // "4"
    ("D", Some(|d| {
        (d.weekday().num_days_from_sunday() + 1).to_string()
    })), // "3" — 1 is Sunday
    ("Q", Some(|d| ((d.month() - 1) / 3 + 1).to_string())), // "1"
    ("I", None), // last digit of the ISO week-numbering year
    ("W", None), // week of the month
    ("J", None), // Julian day
];

/// 12-hour clock, with midnight and noon both printing `12` — confirmed live:
/// `to_char(TIMESTAMP '2024-03-05 00:07:09', 'HH12 AM')` is `12 AM` and the
/// same instant at 12:07 is `12 PM`.
fn hour12(d: NaiveDateTime) -> u32 {
    match d.hour() % 12 {
        0 => 12,
        h => h,
    }
}

/// `AM`/`PM` both print the actual half of the day, in the case of whichever
/// of the two the format spelled — measured: `to_char(<14:07>, 'AM')` is
/// `PM`.
fn meridiem_upper(d: NaiveDateTime) -> String {
    if d.hour() < 12 { "AM" } else { "PM" }.to_string()
}

fn meridiem_lower(d: NaiveDateTime) -> String {
    if d.hour() < 12 { "am" } else { "pm" }.to_string()
}

/// Render one value with one datetime template.
///
/// **An ASCII letter that starts no pattern is an error, not a literal.**
/// Postgres does copy genuinely unrecognized characters through
/// (`to_char(<any>, 'ZZZ')` is `ZZZ`), but copying letters through here would
/// silently corrupt the patterns [`DATE_PATTERNS`] carries as `None`: `RM` is
/// the Roman-numeral month (`III` for March), `J` the Julian day, `CC` the
/// century, `SSSS` the seconds past midnight, `TZ` the zone abbreviation.
/// `to_char(<2024-03-05>, 'XYZ')` is `X4Z` on a live server, not `XYZ` — the
/// same trap from the other side: `Y` is a pattern even in the middle of
/// nonsense. So an unimplemented pattern (and any unrecognized letter) fails,
/// and failing falls back to an engine that has the whole template language.
///
/// Non-letters pass through as themselves (`-`, `/`, `:`, spaces), and
/// double-quoted runs pass through verbatim, which is Postgres's own escape
/// for text inside a template: `to_char(<2024-03-05>, '"Year:" YYYY')` is
/// `Year: 2024`.
fn format_datetime(fmt: &str, dt: NaiveDateTime) -> Result<String, ExecError> {
    let mut out = String::with_capacity(fmt.len() + 8);
    let mut rest = fmt;
    'outer: while !rest.is_empty() {
        if let Some(after_open) = rest.strip_prefix('"') {
            let Some(close) = after_open.find('"') else {
                return Err(ExecError::Internal(format!(
                    "to_char format `{fmt}` has an unterminated double-quoted section"
                )));
            };
            out.push_str(&after_open[..close]);
            rest = &after_open[close + 1..];
            continue;
        }
        for (pattern, render) in DATE_PATTERNS {
            if let Some(tail) = rest.strip_prefix(pattern) {
                let Some(render) = render else {
                    return Err(ExecError::Internal(format!(
                        "to_char datetime template pattern `{pattern}` is not implemented"
                    )));
                };
                out.push_str(&render(dt));
                rest = tail;
                continue 'outer;
            }
        }
        let ch = rest.chars().next().expect("rest is non-empty");
        if ch.is_ascii_alphabetic() {
            return Err(ExecError::Internal(format!(
                "to_char datetime template pattern starting at `{rest}` is not implemented"
            )));
        }
        out.push(ch);
        rest = &rest[ch.len_utf8()..];
    }
    Ok(out)
}

// ─── `^` (exponentiation) ───────────────────────────────────────────────────

/// `a ^ b`, Postgres oid 965 (`float8 ^ float8`, `dpow`).
///
/// Integer operands reach this after [`unify_numeric`] has already made both
/// sides one integer width; Postgres's own resolution widens them to `float8`
/// too (there is no `int ^ int` operator, and `pg_typeof(2::int8 ^ 2::int4)`
/// is `double precision` live), so the cast here is the same one Postgres
/// performs, not an approximation of it.
///
/// A `numeric` operand is REFUSED rather than routed through `f64`. Postgres
/// resolves that to a different operator with a different result type — oid
/// 1038, `numeric ^ numeric` returning `numeric` — and answering it in
/// double precision would be a wrong answer with the right shape.
/// [`eval_binary`] dispatches on the operator *name*, so this is the only
/// place the distinction can be made.
fn eval_exponent(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, ExecError> {
    for (side, arr) in [("left", lhs), ("right", rhs)] {
        match arr.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => {}
            DataType::Decimal128(..) | DataType::Decimal256(..) => {
                return Err(ExecError::Internal(format!(
                    "`^` with a numeric {side} operand is Postgres's numeric_power (oid 1038), \
                     which returns numeric; this evaluator implements only the float8 operator \
                     (oid 965)"
                )))
            }
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "operator does not exist: {other:?} ^ ..."
                )))
            }
        }
    }
    let l = cast::cast(lhs, &DataType::Float64).map_err(|e| map_arrow(e, "^"))?;
    let r = cast::cast(rhs, &DataType::Float64).map_err(|e| map_arrow(e, "^"))?;
    float8_binary_checked(&l, &r, pg_power_f64)
}

/// Evaluate one argument of a `VARIADIC "any"` function and materialize it
/// as text. `"any"` means the argument arrives at whatever type it was
/// written as, so arrow's `cast` kernel does the numeric/bool/date rendering
/// and already-`Utf8` arguments pass through untouched. `what` names the
/// calling function for the error message only.
fn eval_as_text(
    arg: &Expr,
    batch: &RecordBatch,
    session: &EvalSession,
    what: &'static str,
) -> Result<StringArray, ExecError> {
    let v = eval_with(arg, batch, session)?;
    let v: ArrayRef = if v.data_type() == &DataType::Utf8 {
        v
    } else {
        Arc::new(cast::cast(&v, &DataType::Utf8).map_err(|e| map_arrow(e, what))?)
    };
    Ok(downcast_array::<StringArray>(&v, "text (after the cast to text)")?.clone())
}

/// `concat(VARIADIC "any")`. Every argument is cast to text (arrow's `cast`
/// kernel handles the numeric/bool/etc. cases; text arguments pass through
/// unchanged) and NULL arguments are skipped rather than propagated — the
/// opposite of `||`, which is an ordinary strict operator. Verified against a
/// live PostgreSQL 18: `concat('a', NULL, 'b') = 'ab'`, while
/// `'a' || NULL || 'b'` is NULL. `concat(NULL, NULL)` is `''`, not NULL:
/// this function never itself returns NULL.
fn eval_concat(args: &[Expr], batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    let n = batch.num_rows();
    let mut cols: Vec<StringArray> = Vec::with_capacity(args.len());
    for arg in args {
        cols.push(eval_as_text(arg, batch, session, "CONCAT")?);
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

/// `concat_ws(text, VARIADIC "any")` — "concatenate with separator". The
/// first argument is the separator; the rest are the values being joined.
///
/// Three behaviours that are each easy to get wrong, all verified against a
/// live PostgreSQL 18:
///
/// ```text
/// concat_ws('-', NULL, 'b') = 'b'      -- NULL values are SKIPPED …
/// concat_ws('-', NULL, NULL) = ''      -- … and skipping them all yields ''
/// concat_ws(NULL, 'a', 'b') = NULL     -- but a NULL SEPARATOR is strict
/// concat_ws('-', '', 'b')  = '-b'      -- '' is a value, not a NULL
/// concat_ws('-', 1, true)  = '1-t'     -- non-text values are cast to text
/// ```
///
/// The separator being strict while the values are not is the asymmetry: a
/// skipped value contributes no separator either, so the separator lands
/// *between surviving values only* — which is why this cannot be written as
/// [`eval_concat`] with a separator interleaved up front.
fn eval_concat_ws(args: &[Expr], batch: &RecordBatch,
    session: &EvalSession) -> Result<ArrayRef, ExecError> {
    let n = batch.num_rows();
    let sep_expr = args.first().ok_or_else(|| {
        ExecError::Internal(
            "concat_ws called with no arguments — it takes a separator plus the values to \
             join; a planner bug, not user error"
                .to_string(),
        )
    })?;
    let sep = eval_as_text(sep_expr, batch, session, "CONCAT_WS")?;

    let mut cols: Vec<StringArray> = Vec::with_capacity(args.len().saturating_sub(1));
    for arg in &args[1..] {
        cols.push(eval_as_text(arg, batch, session, "CONCAT_WS")?);
    }

    let out: StringArray = (0..n)
        .map(|i| {
            if sep.is_null(i) {
                return None;
            }
            let sep = sep.value(i);
            let mut buf = String::new();
            let mut first = true;
            for col in &cols {
                if col.is_null(i) {
                    continue;
                }
                if !first {
                    buf.push_str(sep);
                }
                buf.push_str(col.value(i));
                first = false;
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
// `arity::unary`. Two separate reasons, and the module docs' "The float8
// policy" section states the order they apply in:
//
//   - Domain: `sqrt`/`ln`/`log`/`asin`/`acos`/`power` have real domains
//     narrower than "every finite f64", and Postgres ERRORS outside them
//     rather than returning `NaN`/`-inf` the way the underlying libm call
//     would. `sin`/`cos`/`tan` are the mirror image — infinite input is the
//     error, finite input is always in domain.
//   - Range: `exp`/`degrees`/`radians`/`power` can push a perfectly in-domain
//     input off the end of the f64 range, and Postgres raises 22003 rather
//     than handing back the `±Infinity`/`0` libm produces. That check is
//     [`check_float8_result`], applied to the result and never to the input.
//
// Routing all of it through the fallible [`float8_unary_checked`] /
// [`float8_binary_checked`] shape is what turns both into a catchable
// [`ExecError`] instead of a silently wrong numeric answer reaching the
// client.

/// Postgres's `check_float8_val(val, inf_is_valid, zero_is_valid)`, the range
/// check every float8 function applies to its *result* — see the module
/// docs' "The float8 policy" section for why the two validity flags are
/// per-function rather than constant, and for the live-verified evidence
/// behind each caller's choice.
///
/// Both failures are SQLSTATE 22003 in Postgres with these exact message
/// strings; see the module docs for why they ride [`ExecError::TypeMismatch`]
/// for now.
fn check_float8_result(
    result: f64,
    inf_is_valid: bool,
    zero_is_valid: bool,
) -> Result<f64, ExecError> {
    if result.is_infinite() && !inf_is_valid {
        return Err(ExecError::TypeMismatch(
            "value out of range: overflow".to_string(),
        ));
    }
    // `result == 0.0` is true for `-0.0` as well, which is what Postgres's C
    // comparison does too — an underflow that lands on negative zero is still
    // an underflow.
    if result == 0.0 && !zero_is_valid {
        return Err(ExecError::TypeMismatch(
            "value out of range: underflow".to_string(),
        ));
    }
    Ok(result)
}

/// Apply a fallible `f64 -> f64` closure elementwise, NULL-in/NULL-out (the
/// null slot is never passed to `f`, same guarantee [`arity::try_unary`]
/// gives every other decimal path in this file — see the module docs' point
/// 7). Shared by every float8 math function below that can fail at all —
/// on domain (`sqrt`, `ln`, `log`, `asin`, `acos`, `sin`, `cos`, `tan`) or on
/// range (`exp`, `degrees`, `radians`).
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
/// float8)` — the one float8 math function here whose domain restriction and
/// range check both depend on the two arguments together (negative base with
/// a non-integer exponent; an infinity in *either* argument legitimising an
/// infinite or zero result), not on one argument in isolation.
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

/// `power(double precision, double precision)`. The one function here that
/// exercises all three phases of the module docs' float8 policy, so it is
/// written as three explicitly labelled blocks. Every claim below was read
/// off a live PostgreSQL 18.2 by evaluating the full 12x12 matrix of
/// `{0, -0, 1, -1, 0.5, -0.5, DBL_MIN, DBL_MAX, ±Inf, NaN, subnormal}`
/// against itself, not from the C source.
///
/// Phase 1, IEEE-754/POSIX special cases, *before* the domain guards:
/// `power('NaN', 0) = 1` and `power(1, 'NaN') = 1`, every other `NaN`-touching
/// pair is `NaN` — including `power(0, 'NaN')`, which is why this block has to
/// come first: the zero-base guard below would otherwise claim it.
///
/// Phase 2, domain guards:
/// - `power(0, negative) `raises `ERROR: 2201F: zero raised to a negative
///   power is undefined` (confirmed for `-0.0` as the base and for
///   `-Infinity` as the exponent), where `f64::powf` returns `Infinity`.
/// - a negative base raised to a non-integer exponent is a complex number:
///   `ERROR: 2201F: a negative number raised to a non-integer power yields a
///   complex result`. "Integer exponent" is `exponent.floor() == exponent`,
///   Postgres's own `floor(arg2) != arg2` — NOT `exponent.fract() != 0.0`,
///   which misclassifies `±Infinity` (see the module docs). `power(-2, 3)`
///   and `power(-1, 'Infinity')` must both survive this guard; live Postgres
///   answers `-8` and `1`.
///
/// Phase 3, range check: `power(0, 0) = 1` still needs no special case, but
/// `power(0.5, DBL_MAX)` (libm `0`) and `power(0.5, DBL_MIN)` (libm
/// `Infinity`) are both 22003 errors. A zero or infinite result is only
/// legitimate when an infinity or a zero base put it there — see
/// [`check_float8_result`] and the three-part rule in the module docs.
fn pg_power_f64(base: f64, exponent: f64) -> Result<f64, ExecError> {
    // Phase 1 — IEEE-754 special cases.
    if base.is_nan() {
        // `exponent == 0.0` is deliberately not `exponent.abs() == 0.0`:
        // `-0.0 == 0.0` already, and `power('NaN', -0.0)` is 1 live.
        return Ok(if exponent == 0.0 { 1.0 } else { f64::NAN });
    }
    if exponent.is_nan() {
        return Ok(if base == 1.0 { 1.0 } else { f64::NAN });
    }

    // Phase 2 — domain guards.
    if base == 0.0 && exponent < 0.0 {
        return Err(ExecError::TypeMismatch(
            "zero raised to a negative power is undefined".to_string(),
        ));
    }
    if base < 0.0 && exponent.floor() != exponent {
        return Err(ExecError::TypeMismatch(
            "a negative number raised to a non-integer power yields a complex result".to_string(),
        ));
    }

    // Phase 3 — range check the finite computation.
    let infinite_input = base.is_infinite() || exponent.is_infinite();
    check_float8_result(
        base.powf(exponent),
        infinite_input,
        infinite_input || base == 0.0,
    )
}

/// `exp(double precision)`. No domain restriction at all — the whole reason
/// this is not `arity::unary(f64::exp)` is phase 3. Verified live:
/// `exp('Infinity')` is `Infinity` and `exp('-Infinity')` is `0`, but
/// `exp(1.8e308)` raises `ERROR: 22003: value out of range: overflow` and
/// `exp(-1.8e308)` raises the matching underflow — the same bit patterns,
/// legitimate only because the *input* was infinite. The underflow boundary
/// is sharp and was checked either side of it: `exp(-745)` is `5e-324`,
/// `exp(-746)` is an error rather than `0`.
fn pg_exp_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.exp(), x.is_infinite(), x.is_infinite())
}

/// `degrees(double precision)`: radians to degrees, a scale-up, so only
/// overflow is reachable in practice. Verified live: `degrees('Infinity')` is
/// `Infinity` but `degrees(1.797e308)` raises `ERROR: 22003: value out of
/// range: overflow`, and `degrees(0)` is a legitimate `0` (as is
/// `degrees(-0.0)`, which stays `-0`).
fn pg_degrees_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.to_degrees(), x.is_infinite(), x == 0.0)
}

/// `radians(double precision)`: degrees to radians, a scale-down, so it is
/// underflow that bites. Verified live: `radians(4.9e-324)` — the smallest
/// subnormal — raises `ERROR: 22003: value out of range: underflow`, while
/// `radians(0)` is `0` and `radians(1.11e-308)` is a perfectly good
/// `1.94e-310`. Kept symmetric with [`pg_degrees_f64`] rather than
/// underflow-only, because Postgres's `float8_mul` checks both directions.
fn pg_radians_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.to_radians(), x.is_infinite(), x == 0.0)
}

/// `sin`/`cos`/`tan(double precision)`, which share one shape: `NaN` passes
/// straight through, an infinite argument is a hard error, and every finite
/// argument is in domain. Verified live: `sin('NaN')` is `NaN` but
/// `sin('Infinity')` raises `ERROR: 22003: input is out of range` — the
/// libm answer for an infinite argument is `NaN`, and returning that would
/// silently claim Postgres computed something.
///
/// No phase-3 check: `sin`/`cos` are bounded by construction and `tan`'s
/// worst case is finite (`tan(pi()/2)` is `1.633123935319537e+16` live, not
/// an infinity, because `pi()/2` is not exactly a pole in binary floating
/// point).
fn pg_trig_of_radians(x: f64, f: impl Fn(f64) -> f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    if x.is_infinite() {
        return Err(ExecError::TypeMismatch("input is out of range".to_string()));
    }
    Ok(f(x))
}

/// `asin(double precision)`. Verified live: `SELECT asin(2::float8)` raises
/// `ERROR: 22003: input is out of range` — SQLSTATE 22003
/// (`numeric_value_out_of_range`), a genuinely different code from the
/// `ln`/`sqrt`/`power` domain errors above, not the same one reused.
/// `f64::asin` outside `[-1, 1]` silently returns `NaN`, which is why this
/// needs the checked path rather than `arity::unary(f64::asin)`.
///
/// `asin('NaN')` is `NaN`, not an error — the phase-1/phase-2 ordering from
/// the module docs, and the reason [`reject_out_of_trig_domain`] is never
/// reached with a `NaN`. Postgres gets the same answer without an explicit
/// branch, since its `arg < -1.0 || arg > 1.0` is false for `NaN`; the
/// branch is written out here because this file's guard is phrased as a
/// range `contains`, which *would* reject it.
fn pg_asin_f64(x: f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    reject_out_of_trig_domain(x)?;
    Ok(x.asin())
}

/// `acos(double precision)`. See [`pg_asin_f64`] — same domain `[-1, 1]`,
/// same `NaN`-before-domain ordering, same SQLSTATE 22003 "input is out of
/// range" on a live PostgreSQL 18.
fn pg_acos_f64(x: f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    reject_out_of_trig_domain(x)?;
    Ok(x.acos())
}

/// `acosh(double precision)`. Domain is `[1, ∞)`, and leaving it is an ERROR,
/// not a `NaN`: `acosh(0.5)` raises `input is out of range` on the live server
/// while `f64::acosh(0.5)` silently returns `NaN`. `acosh(1)` is `0` and
/// `acosh('Infinity')` is `Infinity`, both measured — so the guard is `x < 1`,
/// not a two-sided range. `NaN` passes through ahead of the guard, the same
/// phase-1/phase-2 ordering [`pg_acos_f64`] uses and for the same reason
/// (`NaN < 1.0` is false, so Postgres needs no explicit branch and neither
/// would this — the branch is here so the ordering is legible).
fn pg_acosh_f64(x: f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    if x < 1.0 {
        return Err(ExecError::TypeMismatch("input is out of range".to_string()));
    }
    Ok(x.acosh())
}

/// `atanh(double precision)`. Domain is `[-1, 1]`, and the *endpoints are in
/// it*: `atanh(1)` is `Infinity` and `atanh(-1)` is `-Infinity` (measured
/// live), while `atanh(2)` raises `input is out of range`. An implementation
/// that rejects the endpoints alongside the outside is wrong for exactly two
/// inputs, which is why this reuses [`reject_out_of_trig_domain`]'s inclusive
/// range rather than an exclusive one.
fn pg_atanh_f64(x: f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    reject_out_of_trig_domain(x)?;
    Ok(x.atanh())
}

/// Shared `[-1, 1]` domain guard for `asin`/`acos`. Callers must have handled
/// `NaN` before calling: `(-1.0..=1.0).contains(&NaN)` is false, so this
/// would report a `NaN` as out of range where Postgres returns it unchanged.
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

// ─── The string / integer-math orphans ──────────────────────────────────────
//
// The non-array half of the DataFusion orphan list (see the `OID_*` block near
// the top). Same discipline as the array family: every rule below was asked of
// the live PostgreSQL 18.2, and the ones that surprised are stated.

/// `octet_length(text)` and `bit_length(text)` — byte length, and eight times
/// it.
///
/// Deliberately not [`text_char_length`]: `length('héllo')` is 5 (characters)
/// while `octet_length('héllo')` is 6 and `bit_length('héllo')` is 48 (bytes,
/// and bytes×8), all measured live. The two are one function apart and answer
/// differently for every non-ASCII string, which is why this does not reuse
/// the character-counting one.
///
/// `octet_length(character)` (oid 1375) is NOT implemented — see the "Orphans
/// deliberately still unimplemented" block for the measured reason.
fn text_byte_length(arr: &ArrayRef, times_eight: bool) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<StringArray>(arr, "text")?;
    let out: Int32Array = a
        .iter()
        .map(|v| {
            v.map(|s| {
                let bytes = i32::try_from(s.len()).expect("string length fits i32");
                if times_eight {
                    bytes.saturating_mul(8)
                } else {
                    bytes
                }
            })
        })
        .collect();
    Ok(Arc::new(out))
}

/// `octet_length(bytea)` and `bit_length(bytea)`.
fn bytea_byte_length(arr: &ArrayRef, times_eight: bool) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<BinaryArray>(arr, "bytea")?;
    let out: Int32Array = a
        .iter()
        .map(|v| {
            v.map(|b| {
                let bytes = i32::try_from(b.len()).expect("bytea length fits i32");
                if times_eight {
                    bytes.saturating_mul(8)
                } else {
                    bytes
                }
            })
        })
        .collect();
    Ok(Arc::new(out))
}

/// `starts_with(a, b)`.
///
/// `starts_with('abc', '')` and `starts_with('', '')` are both **true**,
/// measured live — the empty prefix is a prefix of everything, including of
/// the empty string. Rust's `str::starts_with` agrees, which is why this is a
/// one-liner and not a hand-rolled loop.
fn eval_starts_with(a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<StringArray>(a, "text")?;
    let b = downcast_array::<StringArray>(b, "text")?;
    let out: BooleanArray = a
        .iter()
        .zip(b.iter())
        .map(|(s, p)| match (s, p) {
            (Some(s), Some(p)) => Some(s.starts_with(p)),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

/// `to_hex(integer)` / `to_hex(bigint)` — hexadecimal in the argument's own
/// **two's complement**, not a signed rendering.
///
/// `to_hex(-1)` is `ffffffff` for `int4` and `ffffffffffffffff` for `int8`,
/// `to_hex(-2147483648)` is `80000000`, and `to_hex(0)` is `0` — all measured
/// live. A `format!("{:x}", n)` on the signed value would print `-1`, so the
/// cast to the unsigned type of the same width is the whole implementation.
fn eval_to_hex_i32(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int32Array>(arr, "integer")?;
    let out: StringArray = a
        .iter()
        .map(|v| v.map(|n| format!("{:x}", n as u32)))
        .collect();
    Ok(Arc::new(out))
}

/// `to_hex(bigint)`. See [`eval_to_hex_i32`].
fn eval_to_hex_i64(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int64Array>(arr, "bigint")?;
    let out: StringArray = a
        .iter()
        .map(|v| v.map(|n| format!("{:x}", n as u64)))
        .collect();
    Ok(Arc::new(out))
}

/// `factorial(bigint) -> numeric`.
///
/// `factorial(0)` is 1 and `factorial(-1)` **errors** with "factorial of a
/// negative number is undefined" (measured live) — it does not return NULL and
/// it does not return 1.
///
/// Postgres's result is an arbitrary-precision `numeric`; Basin's is a
/// `Decimal128`, whose 38 digits run out at **33!**
/// (`8683317618811886495518194401280000000`, 37 digits — 34! is 39 digits).
/// So `factorial(34)` and above error here where PostgreSQL answers. That is
/// the crate-wide `Decimal128` ceiling `basin_pgtype`'s own docs record
/// (`DECIMAL128_MAX_PRECISION`), surfaced rather than wrapped: a silently
/// truncated factorial would be far worse than a refusal. PostgreSQL itself
/// stops at `factorial(9223372036854775807)` with "value overflows numeric
/// format", so the *shape* of the two answers agrees, only the threshold
/// differs.
fn eval_factorial(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int64Array>(arr, "bigint")?;
    let mut out: Vec<Option<i128>> = Vec::with_capacity(a.len());
    for v in a.iter() {
        match v {
            None => out.push(None),
            Some(n) if n < 0 => {
                return Err(ExecError::TypeMismatch(
                    "factorial of a negative number is undefined".to_string(),
                ))
            }
            Some(n) => {
                let mut acc: i128 = 1;
                for k in 2..=n {
                    let k = i128::from(k);
                    acc = acc
                        .checked_mul(k)
                        .filter(|p| p.abs() < DECIMAL128_LIMIT)
                        .ok_or(ExecError::Overflow("factorial"))?;
                }
                out.push(Some(acc));
            }
        }
    }
    let arr = Decimal128Array::from(out)
        .with_precision_and_scale(38, 0)
        .map_err(|e| map_arrow(e, "factorial"))?;
    Ok(Arc::new(arr))
}

/// One more than the largest 38-digit value, the ceiling
/// `DataType::Decimal128(38, 0)` can hold. `i128` itself goes three digits
/// further (`1.7e38`), so an `i128` overflow check alone would admit values
/// Arrow then rejects.
const DECIMAL128_LIMIT: i128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

/// `gcd(a, b)` for `int4`/`int8`, and `lcm(a, b)` on top of it.
///
/// Both follow PostgreSQL's own algorithm, because the *overflow* cases are
/// where a reimplementation goes wrong and they are not where they look. The
/// full `int4` truth table over `{0, ±1, 3, INT_MIN, INT_MAX}` was measured
/// live; the entries that matter:
///
/// | call | result |
/// |---|---|
/// | `gcd(0, 0)` | 0 |
/// | `gcd(-12, 18)` | 6 — the result is always non-negative |
/// | `gcd(INT_MIN, 0)` | **ERROR** integer out of range — the result would be `|INT_MIN|` |
/// | `gcd(INT_MIN, INT_MIN)` | **ERROR** — same reason |
/// | `gcd(INT_MIN, 1)` | 1 — no error: the *result* fits |
/// | `lcm(0, 5)` | 0 |
/// | `lcm(INT_MIN, 0)` | **0**, NOT an error — the zero test comes FIRST |
/// | `lcm(INT_MIN, 1)` | **ERROR** — `|INT_MIN|` again |
/// | `lcm(-4, 6)` | 12 — non-negative |
///
/// `lcm(INT_MIN, 0)` is the trap: an implementation that computes the gcd
/// before checking for zero raises where PostgreSQL answers 0.
fn pg_gcd_i64(mut a: i64, mut b: i64, width_min: i64) -> Result<i64, ExecError> {
    while b != 0 {
        let t = b;
        // `wrapping_rem`, not `%`: `i64::MIN % -1` *panics* in a debug build
        // (the quotient overflows even though the remainder does not), and
        // `gcd(-9223372036854775808, -1)` is a perfectly good `1` on the live
        // server. The wrapping form yields the mathematically correct 0 here.
        b = a.wrapping_rem(b);
        a = t;
    }
    if a < 0 {
        if a == width_min {
            return Err(ExecError::Overflow("integer"));
        }
        a = -a;
    }
    Ok(a)
}

/// See [`pg_gcd_i64`] for the measured truth table, including why the zero
/// test must precede the gcd.
fn pg_lcm_i64(a: i64, b: i64, width_min: i64, width_max: i64) -> Result<i64, ExecError> {
    if a == 0 || b == 0 {
        return Ok(0);
    }
    let g = pg_gcd_i64(a, b, width_min)?;
    let mut r = (a / g)
        .checked_mul(b)
        .ok_or(ExecError::Overflow("integer"))?;
    if r < 0 {
        if r == width_min {
            return Err(ExecError::Overflow("integer"));
        }
        r = -r;
    }
    if r > width_max {
        return Err(ExecError::Overflow("integer"));
    }
    Ok(r)
}

/// `gcd`/`lcm` over an `int4` column, computed in `i64` and range-checked back
/// to `i32` — the `int4` overflow boundary is `i32::MIN`, not `i64::MIN`, so
/// the width has to be passed in rather than inferred from the accumulator.
fn eval_gcd_lcm_i32(a: &ArrayRef, b: &ArrayRef, is_gcd: bool) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int32Array>(a, "integer")?;
    let b = downcast_array::<Int32Array>(b, "integer")?;
    let mut out: Vec<Option<i32>> = Vec::with_capacity(a.len());
    for (x, y) in a.iter().zip(b.iter()) {
        match (x, y) {
            (Some(x), Some(y)) => {
                let min = i64::from(i32::MIN);
                let r = if is_gcd {
                    pg_gcd_i64(i64::from(x), i64::from(y), min)?
                } else {
                    pg_lcm_i64(i64::from(x), i64::from(y), min, i64::from(i32::MAX))?
                };
                out.push(Some(
                    i32::try_from(r).map_err(|_| ExecError::Overflow("integer"))?,
                ));
            }
            _ => out.push(None),
        }
    }
    Ok(Arc::new(Int32Array::from(out)))
}

/// `gcd`/`lcm` over an `int8` column. See [`pg_gcd_i64`].
fn eval_gcd_lcm_i64(a: &ArrayRef, b: &ArrayRef, is_gcd: bool) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Int64Array>(a, "bigint")?;
    let b = downcast_array::<Int64Array>(b, "bigint")?;
    let mut out: Vec<Option<i64>> = Vec::with_capacity(a.len());
    for (x, y) in a.iter().zip(b.iter()) {
        match (x, y) {
            (Some(x), Some(y)) => out.push(Some(if is_gcd {
                pg_gcd_i64(x, y, i64::MIN)?
            } else {
                pg_lcm_i64(x, y, i64::MIN, i64::MAX)?
            })),
            _ => out.push(None),
        }
    }
    Ok(Arc::new(Int64Array::from(out)))
}

/// `overlay(s placing r from start [for len])` — replace a stretch of `s`.
///
/// PostgreSQL defines it as, exactly:
///
/// ```text
/// substring(s from 1 for start - 1) || r || substring(s from start + len)
/// ```
///
/// with `len` defaulting to the length of `r`. Implementing it as that
/// composition rather than as an index-juggling rewrite is what makes the odd
/// cases fall out for free; all measured live:
///
/// | call | result |
/// |---|---|
/// | `overlay('abcdef' placing 'XY' from 3)` | `abXYef` |
/// | `overlay('abcdef' placing 'XY' from 3 for 1)` | `abXYdef` |
/// | `overlay('abcdef' placing 'XY' from 99)` | `abcdefXY` — past the end APPENDS |
/// | `overlay('abcdef' placing 'XY' from 3 for -1)` | `abXYbcdef` — a negative `for` does NOT error and DUPLICATES text |
/// | `overlay('abcdef' placing 'XY' from 0)` | **ERROR** negative substring length not allowed |
/// | `overlay('abcdef' placing 'XY' from -2)` | **ERROR** — same message, not "negative start" |
/// | `overlay('abcdef' placing 'XY' from 2147483647)` | **ERROR** integer out of range — `start + len` overflows |
///
/// The `for -1` row is the one worth stating: `start + len` moves the tail
/// *backwards*, so characters appear twice, and Postgres allows it. Only
/// `start <= 0` and an overflowing `start + len` are rejected.
fn pg_overlay_bounds(start: i32, len: i32) -> Result<(i64, i64), ExecError> {
    if start <= 0 {
        return Err(ExecError::TypeMismatch(
            "negative substring length not allowed".to_string(),
        ));
    }
    let tail = start
        .checked_add(len)
        .ok_or(ExecError::Overflow("integer"))?;
    Ok((i64::from(start), i64::from(tail)))
}

/// `overlay(text, text, integer [, integer])`. See [`pg_overlay_bounds`].
fn eval_overlay_text(
    s: &ArrayRef,
    r: &ArrayRef,
    start: &ArrayRef,
    len: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let r = downcast_array::<StringArray>(r, "text")?;
    let start = downcast_array::<Int32Array>(start, "integer")?;
    let len = match len {
        None => None,
        Some(l) => Some(downcast_array::<Int32Array>(l, "integer")?.clone()),
    };
    let mut out: Vec<Option<String>> = Vec::with_capacity(s.len());
    for i in 0..s.len() {
        let explicit_len_null = len.as_ref().is_some_and(|l| l.is_null(i));
        if s.is_null(i) || r.is_null(i) || start.is_null(i) || explicit_len_null {
            out.push(None);
            continue;
        }
        let replacement = r.value(i);
        let sl = match len.as_ref() {
            Some(l) => l.value(i),
            None => i32::try_from(replacement.chars().count())
                .map_err(|_| ExecError::Overflow("integer"))?,
        };
        let (sp, tail) = pg_overlay_bounds(start.value(i), sl)?;
        let head = pg_substr(s.value(i), 1, Some(sp - 1));
        let rest = pg_substr(s.value(i), tail, None);
        out.push(Some(format!("{head}{replacement}{rest}")));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// `overlay(bytea, bytea, integer [, integer])` — the same definition over
/// bytes rather than characters.
fn eval_overlay_bytea(
    s: &ArrayRef,
    r: &ArrayRef,
    start: &ArrayRef,
    len: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<BinaryArray>(s, "bytea")?;
    let r = downcast_array::<BinaryArray>(r, "bytea")?;
    let start = downcast_array::<Int32Array>(start, "integer")?;
    let len = match len {
        None => None,
        Some(l) => Some(downcast_array::<Int32Array>(l, "integer")?.clone()),
    };
    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(s.len());
    for i in 0..s.len() {
        let explicit_len_null = len.as_ref().is_some_and(|l| l.is_null(i));
        if s.is_null(i) || r.is_null(i) || start.is_null(i) || explicit_len_null {
            out.push(None);
            continue;
        }
        let replacement = r.value(i);
        let sl = match len.as_ref() {
            Some(l) => l.value(i),
            None => i32::try_from(replacement.len()).map_err(|_| ExecError::Overflow("integer"))?,
        };
        let (sp, tail) = pg_overlay_bounds(start.value(i), sl)?;
        let bytes = s.value(i);
        let n = bytes.len() as i64;
        let head_end = (sp - 1).clamp(0, n) as usize;
        let tail_start = (tail - 1).clamp(0, n) as usize;
        let mut buf = Vec::with_capacity(head_end + replacement.len() + (n as usize - tail_start));
        buf.extend_from_slice(&bytes[..head_end]);
        buf.extend_from_slice(replacement);
        buf.extend_from_slice(&bytes[tail_start..]);
        out.push(Some(buf));
    }
    Ok(Arc::new(BinaryArray::from_iter(out.iter().map(
        |v| v.as_deref(),
    ))))
}

/// `make_date(year, month, day) -> date`.
///
/// Rejects what Postgres rejects, with Postgres's own message: a day the month
/// does not have (`make_date(2024, 2, 30)` → `date field value out of range:
/// 2024-02-30`, measured live) and **year zero** (`make_date(0, 1, 1)` →
/// the same message), which has no proleptic-Gregorian meaning in Postgres.
/// A negative year is BC and is accepted: `make_date(-1, 1, 1)` is
/// `0001-01-01 BC`.
///
/// The BC handling is the reason this converts through `chrono`'s
/// astronomical year numbering rather than treating the year as-is: Postgres's
/// year `-1` is chrono's year `0` (there is no year 0 in Postgres, so every BC
/// year shifts by one), and getting that wrong is a one-year error nothing
/// downstream would catch.
fn eval_make_date(
    year: &ArrayRef,
    month: &ArrayRef,
    day: &ArrayRef,
) -> Result<ArrayRef, ExecError> {
    let y = downcast_array::<Int32Array>(year, "integer")?;
    let m = downcast_array::<Int32Array>(month, "integer")?;
    let d = downcast_array::<Int32Array>(day, "integer")?;
    let mut out: Vec<Option<i32>> = Vec::with_capacity(y.len());
    for i in 0..y.len() {
        if y.is_null(i) || m.is_null(i) || d.is_null(i) {
            out.push(None);
            continue;
        }
        let (year, month, day) = (y.value(i), m.value(i), d.value(i));
        let bad = || {
            ExecError::TypeMismatch(format!(
                "date field value out of range: {year}-{month:02}-{day:02}"
            ))
        };
        if year == 0 {
            return Err(bad());
        }
        // Postgres has no year 0: its -1 is chrono's 0, -2 is chrono's -1, …
        let astronomical = if year < 0 { year + 1 } else { year };
        let date = NaiveDate::from_ymd_opt(
            astronomical,
            u32::try_from(month).map_err(|_| bad())?,
            u32::try_from(day).map_err(|_| bad())?,
        )
        .ok_or_else(bad)?;
        let days = date
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a date"))
            .num_days();
        out.push(Some(
            i32::try_from(days).map_err(|_| ExecError::Overflow("date"))?,
        ));
    }
    Ok(Arc::new(Date32Array::from(out)))
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

    /// `CASE id WHEN 1 THEN … END` over a `bigint` column: the WHEN values
    /// lower as `int4` literals, the operand is `int8`, and arrow's `eq`
    /// kernel rejects a mismatched pair outright. Postgres widens instead —
    /// values below read off a live PostgreSQL 18.2:
    ///
    /// ```text
    /// CREATE TEMP TABLE tt(id BIGINT, name TEXT);
    /// INSERT INTO tt VALUES (1,'a'),(2,'b'),(3,'c'),(100,NULL),(101,'a');
    /// SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM tt;
    ///  -- one, two, many, many, many
    /// ```
    ///
    /// Before the widening this errored with
    /// `Invalid comparison operation: Int64 == Int32`, which the owned
    /// engine turned into a fallback for the whole statement.
    #[test]
    fn case_simple_form_widens_an_int4_when_against_an_int8_operand() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1i64),
                Some(2),
                Some(3),
                Some(100),
                Some(101),
            ]))],
        )
        .unwrap();
        let expr = Expr::Case {
            operand: Some(Box::new(col(0, "id"))),
            whens: vec![
                (lit_i32(1), lit_text("one")),
                (lit_i32(2), lit_text("two")),
            ],
            else_: Some(Box::new(lit_text("many"))),
        };
        let result = eval(&expr, &batch).unwrap();
        let got = str_array(&result);
        assert_eq!(
            (0..5).map(|i| got.value(i)).collect::<Vec<_>>(),
            vec!["one", "two", "many", "many", "many"]
        );
    }

    /// A companion guard, not a second reproduction: a bare `'a'` WHEN value
    /// is `unknown` until the operand types it, and this case already passed
    /// before the widening above (a `Datum::Utf8` materializes as `Utf8`
    /// whatever its `PgType` says). It is pinned here anyway because routing
    /// the operand through [`eval_operand_against`] moved this path, and a
    /// resolution that silently stopped resolving would otherwise only show
    /// up as a fallback in the probe. Values from the same live PostgreSQL
    /// 18.2 table as above:
    ///
    /// ```text
    /// SELECT CASE name WHEN 'a' THEN 'A!' WHEN 'b' THEN 'B!' END FROM tt;
    ///  -- A!, B!, NULL, NULL, A!
    /// ```
    ///
    /// The NULL-named row lands on the no-ELSE NULL, not on a match: a NULL
    /// operand compares NULL to every WHEN, which never matches.
    #[test]
    fn case_simple_form_resolves_an_unknown_when_against_a_text_operand() {
        let batch = batch_str1(
            "name",
            vec![Some("a"), Some("b"), Some("c"), None, Some("a")],
        );
        let expr = Expr::Case {
            operand: Some(Box::new(col(0, "name"))),
            whens: vec![
                (lit_text_unknown("a"), lit_text("A!")),
                (lit_text_unknown("b"), lit_text("B!")),
            ],
            else_: None,
        };
        let result = eval(&expr, &batch).unwrap();
        let got = str_array(&result);
        let seen: Vec<Option<&str>> = (0..5)
            .map(|i| (!got.is_null(i)).then(|| got.value(i)))
            .collect();
        assert_eq!(
            seen,
            vec![Some("A!"), Some("B!"), None, None, Some("A!")]
        );
    }

    /// A one-column `text[]` batch modelled on the fallback probe's `e.tags`:
    /// row 0 is `{x,y}`, row 1 is `{}` (an EMPTY array, which is not the same
    /// thing as NULL), row 2 is a NULL array.
    fn batch_text_list() -> RecordBatch {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let values = Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 2].into());
        let nulls = arrow::buffer::NullBuffer::from(vec![true, true, false]);
        let list = ListArray::try_new(item, offsets, values, Some(nulls)).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            list.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
    }

    /// `char_length` and `character_length` are the SQL-standard spellings of
    /// `length`, and Postgres gives each its own `pg_proc` row (1381 and 1369
    /// against `length`'s 1317) rather than aliasing them — so a dispatch
    /// keyed on OID answers `length(s)` and falls back on `char_length(s)`
    /// unless all three are named. That is exactly what the probe measured on
    /// `SELECT length(s), char_length(s) FROM mb`.
    ///
    /// All four expectations are live PostgreSQL 18.2, and all four are
    /// CHARACTER counts, not byte counts — `héllo` is 6 bytes and 5
    /// characters, `日本語` is 9 bytes and 3, `🎉party🎉` is 13 bytes and 7
    /// (each emoji is one astral-plane code point), `naïve café` is 12 bytes
    /// and 10.
    #[test]
    fn char_length_and_character_length_count_the_same_characters_as_length() {
        for (input, expected) in [
            ("héllo", 5),
            ("日本語", 3),
            ("🎉party🎉", 7),
            ("naïve café", 10),
        ] {
            for oid in [OID_LENGTH_TEXT, OID_CHAR_LENGTH_TEXT, OID_CHARACTER_LENGTH_TEXT] {
                let got = eval(&sf(oid, vec![lit_text(input)]), &one_row()).unwrap();
                assert_eq!(
                    i32_array(&got).value(0),
                    expected,
                    "oid {oid} on {input:?}"
                );
            }
        }
    }

    /// `array_length(a, d)`. Every row here is live PostgreSQL 18.2, and the
    /// interesting ones are the NULLs: an EMPTY array has no dimensions at
    /// all, so `array_length(ARRAY[]::text[], 1)` is NULL and not 0 —
    /// returning 0 is the natural reading and the wrong answer.
    #[test]
    fn array_length_is_null_for_an_empty_array_and_for_a_missing_dimension() {
        let batch = batch_text_list();
        let tags = col(0, "tags");
        let got = eval(&sf(OID_ARRAY_LENGTH, vec![tags.clone(), lit_i32(1)]), &batch).unwrap();
        let got = i32_array(&got);
        assert_eq!(got.value(0), 2, "array_length(ARRAY['x','y'], 1) = 2");
        assert!(got.is_null(1), "an EMPTY array has no dimension 1, so NULL — not 0");
        assert!(got.is_null(2), "array_length(NULL::text[], 1) is NULL");

        // Dimensions are 1-based, and a 1-D array has no dimension 2.
        for dim in [0, 2, -1] {
            let got = eval(
                &sf(OID_ARRAY_LENGTH, vec![tags.clone(), lit_i32(dim)]),
                &batch,
            )
            .unwrap();
            assert!(
                i32_array(&got).is_null(0),
                "array_length(ARRAY['x','y'], {dim}) is NULL"
            );
        }
    }

    /// `a[i]`. Postgres subscripts are 1-based and, unlike almost everything
    /// else in the language, do NOT error when they miss — live PostgreSQL
    /// 18.2 gives NULL for `(ARRAY['x','y'])[9]`, `[0]` and `[-1]` alike, and
    /// for any subscript of a NULL array.
    #[test]
    fn array_subscript_is_one_based_and_null_outside_the_bounds() {
        let batch = batch_text_list();
        let sub = |i: i32| Expr::Subscript {
            arg: Box::new(col(0, "tags")),
            indices: vec![basin_plan::Subscript::Index(lit_i32(i))],
        };

        let got = eval(&sub(1), &batch).unwrap();
        let s = str_array(&got);
        assert_eq!(s.value(0), "x", "(ARRAY['x','y'])[1] is 'x'");
        assert!(s.is_null(1), "an empty array has no element 1");
        assert!(s.is_null(2), "(NULL::text[])[1] is NULL");

        let got = eval(&sub(2), &batch).unwrap();
        assert_eq!(str_array(&got).value(0), "y");

        for miss in [9, 0, -1] {
            let got = eval(&sub(miss), &batch).unwrap();
            assert!(
                str_array(&got).is_null(0),
                "(ARRAY['x','y'])[{miss}] is NULL, not an error"
            );
        }
    }

    /// A two-`date` batch, for the `date`/`integer` operators.
    fn batch_two_dates(a: Option<i32>, b: Option<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Date32, true),
            Field::new("r", DataType::Date32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![a])),
                Arc::new(Date32Array::from(vec![b])),
            ],
        )
        .unwrap()
    }

    fn date_array(v: &ArrayRef) -> &Date32Array {
        v.as_any().downcast_ref::<Date32Array>().unwrap()
    }

    /// `date + integer` and `date - integer` (`date_pli` 1100, `date_mii`
    /// 1101, `integer_pl_date` 2555). Arrow has no kernel for the mixed pair
    /// — `numeric::add` refuses it with "Invalid date arithmetic operation:
    /// Date32 + Int32", which is exactly what the probe measured on `SELECT
    /// '2024-01-15'::DATE + 1`.
    ///
    /// Live PostgreSQL 18.2: `'2024-01-15'::DATE + 1` is `2024-01-16` and
    /// `- 1` is `2024-01-14`, and `pg_typeof` of each is `date` — NOT the
    /// `timestamp` that `date + interval` produces. As day numbers since the
    /// epoch (`'…'::date - '1970-01-01'::date`, live): 19737, 19738, 19736.
    #[test]
    fn date_plus_or_minus_an_integer_is_another_date() {
        let batch = batch_two_dates(Some(19737), None);
        let d = col(0, "l");

        let plus = Expr::Binary {
            op: op(OID_OP_DATE_PLI),
            lhs: Box::new(d.clone()),
            rhs: Box::new(lit_i32(1)),
        };
        let got = eval(&plus, &batch).unwrap();
        assert_eq!(got.data_type(), &DataType::Date32, "date + int stays a date");
        assert_eq!(date_array(&got).value(0), 19738, "2024-01-15 + 1 = 2024-01-16");

        let minus = Expr::Binary {
            op: op(OID_OP_DATE_MII),
            lhs: Box::new(d.clone()),
            rhs: Box::new(lit_i32(1)),
        };
        assert_eq!(
            date_array(&eval(&minus, &batch).unwrap()).value(0),
            19736,
            "2024-01-15 - 1 = 2024-01-14"
        );

        // `integer + date` is a DIFFERENT pg_operator row (2555) with the
        // operands the other way round; it must agree with 1100.
        let flipped = Expr::Binary {
            op: op(OID_OP_INT_PL_DATE),
            lhs: Box::new(lit_i32(1)),
            rhs: Box::new(d),
        };
        assert_eq!(date_array(&eval(&flipped, &batch).unwrap()).value(0), 19738);
    }

    /// `date - date` (`date_mi`, 1099) yields an INTEGER count of days — not
    /// another date, and not an interval. The operator NAME `-` cannot tell
    /// this apart from `date - integer`, which is why the dispatch is keyed
    /// on the OID. Live PostgreSQL 18.2: `'2024-01-15'::DATE -
    /// '2024-01-01'::DATE` is `14`.
    #[test]
    fn date_minus_date_is_an_integer_count_of_days() {
        let batch = batch_two_dates(Some(19737), Some(19723));
        let expr = Expr::Binary {
            op: op(OID_OP_DATE_MI_DATE),
            lhs: Box::new(col(0, "l")),
            rhs: Box::new(col(1, "r")),
        };
        let got = eval(&expr, &batch).unwrap();
        assert_eq!(got.data_type(), &DataType::Int32, "date - date is an integer");
        assert_eq!(i32_array(&got).value(0), 14);

        // Strict in both operands.
        let batch = batch_two_dates(Some(19737), None);
        assert!(i32_array(&eval(&expr, &batch).unwrap()).is_null(0));
    }

    /// `NOT NULL` over a bare, untyped NULL literal. Lowering types it
    /// `unknown`, which `basin_pgtype::physical` materializes as `Utf8`, so
    /// `require_bool` rejected it and `SELECT (NOT NULL) IS NULL` fell back —
    /// even though Postgres resolves the untyped NULL under `NOT` to boolean
    /// and answers `t` (live PostgreSQL 18.2; `NOT NULL` itself is NULL).
    #[test]
    fn not_of_an_untyped_null_literal_is_a_boolean_null() {
        let expr = Expr::Unary {
            op: NOT_OP,
            arg: Box::new(Expr::Literal(Datum::Null, PgType::UNKNOWN)),
        };
        let got = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            got.data_type(),
            &DataType::Boolean,
            "NOT of an untyped NULL is boolean, not text"
        );
        assert!(bool_array(&got).is_null(0), "NOT NULL is NULL");
    }

    /// The narrow fix above must NOT become "any all-NULL array is
    /// acceptable to NOT". Postgres rejects `NOT <text>` outright ("argument
    /// of NOT must be type boolean, not type text"), so a text COLUMN under
    /// `NOT` has to stay an error here even when every row of it happens to
    /// be NULL — otherwise the day the data is all-NULL is the day the query
    /// silently starts answering.
    #[test]
    fn not_of_an_all_null_text_column_is_still_an_error() {
        let batch = batch_str1("s", vec![None, None]);
        let expr = Expr::Unary {
            op: NOT_OP,
            arg: Box::new(col(0, "s")),
        };
        assert!(
            eval(&expr, &batch).is_err(),
            "NOT over a text column must stay an error, all-NULL or not"
        );
    }

    /// Pull one row's list out of a `ListArray` as an owned `ArrayRef`, so
    /// the array-literal tests below can assert on entries rather than on
    /// offset arithmetic.
    fn list_row(v: &ArrayRef, row: usize) -> ArrayRef {
        v.as_any()
            .downcast_ref::<ListArray>()
            .expect("expected a ListArray")
            .value(row)
    }

    /// `SELECT ARRAY[1,2,3]` — the shape the fallback probe reported as
    /// `ArrayLit(...) is not implemented in eval yet`. From a live
    /// PostgreSQL 18.2: `SELECT ARRAY[1,2,3]` is `{1,2,3}` and
    /// `pg_typeof(ARRAY[1,2,3])` is `integer[]`.
    #[test]
    fn array_literal_of_ints_builds_one_list_per_row() {
        let expr = Expr::ArrayLit(vec![lit_i32(1), lit_i32(2), lit_i32(3)]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            result.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            "must match what basin_pgtype::physical produces for int4[]"
        );
        let row = list_row(&result, 0);
        let got = i32_array(&row);
        assert_eq!((0..3).map(|i| got.value(i)).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    /// `SELECT ARRAY['x','y']` — every element is an `unknown` literal, which
    /// Postgres resolves to `text` (live PostgreSQL 18.2:
    /// `pg_typeof(ARRAY['x','y'])` is `text[]`, value `{x,y}`). Without the
    /// shared unification this could not pick an element type at all.
    #[test]
    fn array_literal_of_unknown_literals_resolves_to_text() {
        let expr = Expr::ArrayLit(vec![lit_text_unknown("x"), lit_text_unknown("y")]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(
            result.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        let row = list_row(&result, 0);
        let got = str_array(&row);
        assert_eq!((0..2).map(|i| got.value(i)).collect::<Vec<_>>(), vec!["x", "y"]);
    }

    /// An array *containing* NULL is not a NULL array. Live PostgreSQL 18.2:
    /// `SELECT ARRAY[1,NULL,3]` is `{1,NULL,3}`,
    /// `array_length(ARRAY[1,NULL,3], 1)` is 3, and `(ARRAY[1,NULL,3])[2] IS
    /// NULL` is true — the NULL occupies a slot, it does not shorten the
    /// array or nullify it.
    #[test]
    fn array_literal_keeps_a_null_element_as_a_slot() {
        let expr = Expr::ArrayLit(vec![
            lit_i32(1),
            Expr::Literal(Datum::Null, PgType::INT4),
            lit_i32(3),
        ]);
        let result = eval(&expr, &one_row()).unwrap();
        assert!(!result.is_null(0), "the array itself is not NULL");
        let row = list_row(&result, 0);
        assert_eq!(row.len(), 3, "array_length is 3, NULL included");
        let got = i32_array(&row);
        assert_eq!(got.value(0), 1);
        assert!(got.is_null(1), "(ARRAY[1,NULL,3])[2] IS NULL");
        assert_eq!(got.value(2), 3);
    }

    /// The transpose, which is the whole of the implementation: element
    /// expressions run *down* the batch, a list value runs *across* the
    /// elements at one row. With a column as an element, every row must get
    /// its own list — a bug here would repeat row 0's value everywhere, or
    /// return the column itself.
    #[test]
    fn array_literal_transposes_per_row_rather_than_per_element() {
        let batch = batch_i32("x", vec![Some(10), Some(20), Some(30)]);
        let expr = Expr::ArrayLit(vec![col(0, "x"), lit_i32(7)]);
        let result = eval(&expr, &batch).unwrap();
        assert_eq!(result.len(), 3, "one list per row");
        for (row, expected_first) in [(0, 10), (1, 20), (2, 30)] {
            let got_row = list_row(&result, row);
            let got = i32_array(&got_row);
            assert_eq!(got.len(), 2);
            assert_eq!(got.value(0), expected_first);
            assert_eq!(got.value(1), 7);
        }
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

    // ─── substring (oids 936/937): substr's SQL-standard-named twin ─────

    /// `substring` and `substr` are different `pg_proc` rows, not one row
    /// with an alias, so a dispatch table that carried only 877/883 would
    /// leave `SUBSTRING(x FROM y FOR z)` unevaluatable. Each value below was
    /// read off a live PostgreSQL 18 through the `substring` spelling
    /// specifically, not assumed from `substr`.
    #[test]
    fn substring_matches_substr_on_both_arities() {
        for (expr, want) in [
            (
                sf(
                    OID_SUBSTRING_3,
                    vec![lit_text("hello"), lit_i32(-3), lit_i32(5)],
                ),
                "h",
            ),
            (
                sf(
                    OID_SUBSTRING_3,
                    vec![lit_text("hello"), lit_i32(0), lit_i32(3)],
                ),
                "he",
            ),
            (
                sf(OID_SUBSTRING_2, vec![lit_text("hello"), lit_i32(2)]),
                "ello",
            ),
            (
                sf(
                    OID_SUBSTRING_3,
                    vec![lit_text("héllo世界"), lit_i32(6), lit_i32(2)],
                ),
                "世界",
            ),
        ] {
            let result = eval(&expr, &one_row()).unwrap();
            assert_eq!(str_array(&result).value(0), want, "for {expr:?}");
        }
    }

    /// A `length` that runs past the end of the string is NOT an error and
    /// NOT a clamped-to-`length`-characters answer — it simply stops at the
    /// end. Verified live: `substring('hello', 2, 10) = 'ello'` (4
    /// characters, not 10), and the same holds at `i32::MAX`:
    /// `substring('abcdef', 2, 2147483647) = 'bcdef'`. The second case is the
    /// one that catches an implementation computing `start + length` in
    /// `i32` — that addition overflows and panics under `overflow-checks`,
    /// which is why [`pg_substr`] does its arithmetic in `i64`.
    #[test]
    fn substring_length_past_the_end_stops_at_the_end_even_at_i32_max() {
        let expr = sf(
            OID_SUBSTRING_3,
            vec![lit_text("hello"), lit_i32(2), lit_i32(10)],
        );
        assert_eq!(
            str_array(&eval(&expr, &one_row()).unwrap()).value(0),
            "ello"
        );

        let expr = sf(
            OID_SUBSTRING_3,
            vec![lit_text("abcdef"), lit_i32(2), lit_i32(i32::MAX)],
        );
        assert_eq!(
            str_array(&eval(&expr, &one_row()).unwrap()).value(0),
            "bcdef",
            "start + length overflows i32 here; the answer is still the rest of \
             the string"
        );
    }

    /// The other end of the same arithmetic. With no `length`, a `start` of
    /// `i32::MIN` clamps to 1 and returns the WHOLE string; *with* a
    /// `length`, the end offset (`start + length - 1`) is still far below 1,
    /// so the answer is `''`. Both verified live:
    /// `substring('abcdef', -2147483648) = 'abcdef'` but
    /// `substring('abcdef', -2147483648, 3) = ''`.
    #[test]
    fn substring_i32_min_start_clamps_but_still_consumes_the_length() {
        let two = sf(OID_SUBSTRING_2, vec![lit_text("abcdef"), lit_i32(i32::MIN)]);
        assert_eq!(
            str_array(&eval(&two, &one_row()).unwrap()).value(0),
            "abcdef"
        );

        let three = sf(
            OID_SUBSTRING_3,
            vec![lit_text("abcdef"), lit_i32(i32::MIN), lit_i32(3)],
        );
        assert_eq!(
            str_array(&eval(&three, &one_row()).unwrap()).value(0),
            "",
            "the clamped start is 1 but the end offset is still negative, so \
             nothing is selected"
        );
    }

    /// A negative `length` errors under the `substring` spelling too — the
    /// asymmetry with a negative `start` (which clamps) is a property of the
    /// function, not of the name it was called by. Verified live:
    /// `substring('hello', 1, -1)` is `22011 negative substring length not
    /// allowed`.
    #[test]
    fn substring_negative_length_is_a_hard_error() {
        let expr = sf(
            OID_SUBSTRING_3,
            vec![lit_text("hello"), lit_i32(1), lit_i32(-1)],
        );
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)), "got {err:?}");
    }

    // ─── left / right ───────────────────────────────────────────────────

    #[test]
    fn left_and_right_take_from_the_correct_end() {
        for (expr, want) in [
            (sf(OID_LEFT, vec![lit_text("abcdef"), lit_i32(2)]), "ab"),
            (sf(OID_RIGHT, vec![lit_text("abcdef"), lit_i32(2)]), "ef"),
            // A negative count means "all but the |n| at the other end".
            (sf(OID_LEFT, vec![lit_text("abcdef"), lit_i32(-2)]), "abcd"),
            (sf(OID_RIGHT, vec![lit_text("abcdef"), lit_i32(-2)]), "cdef"),
            // Characters, not bytes: 'héllo世界' is 7 characters.
            (
                sf(OID_RIGHT, vec![lit_text("héllo世界"), lit_i32(-6)]),
                "界",
            ),
        ] {
            let result = eval(&expr, &one_row()).unwrap();
            assert_eq!(str_array(&result).value(0), want, "for {expr:?}");
        }
    }

    /// **This test pins a reproduced PostgreSQL overflow, on purpose.**
    /// `text_right` negates its argument in a C `int`; `-(-2147483648)` is
    /// not representable, so the negation wraps and the "skip this many
    /// characters" count comes out negative, i.e. skip nothing. The result is
    /// a one-step discontinuity, verified on a live PostgreSQL 18:
    ///
    /// ```text
    /// right('abcdef', -2147483647) = ''
    /// right('abcdef', -2147483648) = 'abcdef'
    /// ```
    ///
    /// Basin used to return `''` for both, which is arguably the more
    /// defensible answer — and it was still a divergence, at 6 call sites in
    /// `crates/basin-exec/tests/function_equivalence.rs`. That suite is an
    /// oracle: a standing "known intentional divergence" entry in it teaches
    /// every later reader that a red differential result can be fine, which
    /// costs more than reproducing one upstream overflow. So this is the
    /// adopted policy, not an accident. **Do not "fix" the `i32::MIN` case to
    /// return `''`.**
    #[test]
    fn right_reproduces_postgres_int_min_negation_overflow() {
        let just_above = sf(OID_RIGHT, vec![lit_text("abcdef"), lit_i32(-2147483647)]);
        assert_eq!(
            str_array(&eval(&just_above, &one_row()).unwrap()).value(0),
            "",
            "skipping 2147483647 characters leaves nothing"
        );

        let at_min = sf(OID_RIGHT, vec![lit_text("abcdef"), lit_i32(i32::MIN)]);
        assert_eq!(
            str_array(&eval(&at_min, &one_row()).unwrap()).value(0),
            "abcdef",
            "one step lower, postgres's own negation overflows and it returns the \
             whole string — reproduced deliberately; see pg_right"
        );
    }

    /// `left` does NOT share the overflow: `text_left` adds rather than
    /// negates, and `len + i32::MIN` is representable. Verified live:
    /// `left('abcdef', -2147483648) = ''`. Pinned so nobody "makes left
    /// consistent with right" at the extreme.
    #[test]
    fn left_at_i32_min_is_empty_unlike_right() {
        let expr = sf(OID_LEFT, vec![lit_text("abcdef"), lit_i32(i32::MIN)]);
        assert_eq!(str_array(&eval(&expr, &one_row()).unwrap()).value(0), "");
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

    // ─── concat_ws (oid 3059) ───────────────────────────────────────────

    /// The separator lands between *surviving* values only, so skipping a
    /// NULL skips its separator too. Verified live against PostgreSQL 18:
    ///
    /// ```text
    /// concat_ws('-', 'a', 'b')  = 'a-b'
    /// concat_ws('-', NULL, 'b') = 'b'      -- not '-b'
    /// concat_ws('-', 'a', NULL) = 'a'      -- not 'a-'
    /// concat_ws('-', '', 'b')   = '-b'     -- '' is a VALUE, not a NULL
    /// ```
    ///
    /// The last line is the one that separates "skip NULLs" from "skip empty
    /// strings": an implementation that skipped `''` would return `'b'` for
    /// it.
    #[test]
    fn concat_ws_skips_null_values_but_not_empty_ones() {
        for (args, want) in [
            (vec![lit_text("-"), lit_text("a"), lit_text("b")], "a-b"),
            (vec![lit_text("-"), lit_text_null(), lit_text("b")], "b"),
            (vec![lit_text("-"), lit_text("a"), lit_text_null()], "a"),
            (vec![lit_text("-"), lit_text(""), lit_text("b")], "-b"),
        ] {
            let result = eval(&sf(OID_CONCAT_WS, args.clone()), &one_row()).unwrap();
            let arr = str_array(&result);
            assert!(
                !arr.is_null(0),
                "concat_ws with a non-NULL separator is \
                                      never NULL — for {args:?}"
            );
            assert_eq!(arr.value(0), want, "for {args:?}");
        }
    }

    /// The asymmetry that makes `concat_ws` more than "`concat` with a
    /// separator": the *values* are NULL-skipping, but the *separator* is
    /// strict. Verified live: `concat_ws(NULL, 'a', 'b')` is NULL, even
    /// though `concat_ws('-', NULL, NULL)` is `''`.
    #[test]
    fn concat_ws_null_separator_makes_the_whole_result_null() {
        let expr = sf(
            OID_CONCAT_WS,
            vec![lit_text_null(), lit_text("a"), lit_text("b")],
        );
        let result = eval(&expr, &one_row()).unwrap();
        assert!(
            str_array(&result).is_null(0),
            "a NULL separator is strict — 'ab' or 'a-b' would both be wrong"
        );

        let all_values_null = sf(
            OID_CONCAT_WS,
            vec![lit_text("-"), lit_text_null(), lit_text_null()],
        );
        let result = eval(&all_values_null, &one_row()).unwrap();
        let arr = str_array(&result);
        assert!(!arr.is_null(0), "NULL VALUES do not make the result NULL");
        assert_eq!(arr.value(0), "");
    }

    /// Non-text values are cast to text, the same as `concat`'s. Verified
    /// live: `concat_ws('-', 1, 2) = '1-2'`.
    #[test]
    fn concat_ws_casts_non_text_values() {
        let expr = sf(OID_CONCAT_WS, vec![lit_text("-"), lit_i32(1), lit_i32(2)]);
        let result = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&result).value(0), "1-2");
    }

    /// Row-by-row over real columns, not just a single-literal broadcast:
    /// the separator, the skipped values and the NULL-separator row all have
    /// to be decided per row.
    #[test]
    fn concat_ws_operates_row_by_row_over_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sep", DataType::Utf8, true),
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("-"), Some(":"), None])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])),
                Arc::new(StringArray::from(vec![Some("b"), Some("b"), Some("b")])),
            ],
        )
        .unwrap();
        let expr = sf(OID_CONCAT_WS, vec![col(0, "sep"), col(1, "a"), col(2, "b")]);
        let result = eval(&expr, &batch).unwrap();
        let arr = str_array(&result);
        assert_eq!(arr.value(0), "a-b");
        assert_eq!(
            arr.value(1),
            "b",
            "the skipped NULL takes its separator with it"
        );
        assert!(
            arr.is_null(2),
            "row 2's separator is NULL, so row 2 is NULL"
        );
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

    // ─── initcap (oid 872) ──────────────────────────────────────────────

    /// Every expectation below was read off a live PostgreSQL 18. The point
    /// of the table is that "capitalise each word" is not the rule: a word
    /// begins after ANY non-alphanumeric character, so `-`, `_`, `'` and `.`
    /// all start one — and a digit does NOT, which is the case an
    /// implementation written around `char::is_whitespace` gets wrong.
    #[test]
    fn initcap_starts_a_word_after_any_non_alphanumeric_not_just_whitespace() {
        for (input, expected) in [
            ("hello world", "Hello World"),
            ("hELLo-woRLD foo_bar 3abc a2b", "Hello-World Foo_Bar 3abc A2b"),
            ("a'b c.d e1f", "A'B C.D E1f"),
            ("The quick brown fox", "The Quick Brown Fox"),
            ("hELLo WoRLD", "Hello World"),
            ("", ""),
            ("   ", "   "),
            ("a", "A"),
        ] {
            let expr = sf(OID_INITCAP, vec![lit_text(input)]);
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "initcap({input:?})"
            );
        }
    }

    /// Non-ASCII letters are alphanumeric, so they neither start a word nor
    /// resist case folding. All four verified live:
    /// `initcap('éclair ÉCLAIR') = 'Éclair Éclair'`,
    /// `initcap('привет МИР') = 'Привет Мир'`,
    /// `initcap('世界abc') = '世界abc'` (CJK letters are alphanumeric, so the
    /// `a` is mid-word and stays lowercase) and `initcap('İSTANBUL') =
    /// 'İstanbul'`.
    #[test]
    fn initcap_treats_non_ascii_letters_as_alphanumeric() {
        for (input, expected) in [
            ("éclair ÉCLAIR", "Éclair Éclair"),
            ("привет МИР", "Привет Мир"),
            ("世界abc", "世界abc"),
            ("héllo世界", "Héllo世界"),
            ("İSTANBUL", "İstanbul"),
        ] {
            let expr = sf(OID_INITCAP, vec![lit_text(input)]);
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "initcap({input:?})"
            );
        }
    }

    #[test]
    fn initcap_of_null_is_null() {
        let expr = sf(OID_INITCAP, vec![lit_text_null()]);
        assert!(eval(&expr, &one_row()).unwrap().is_null(0));
    }

    // ─── lpad / rpad (oids 879/873, 880/874) ────────────────────────────

    /// The three-step order — truncate, then check the fill, then pad —
    /// spelled out as a table of live-server answers. `lpad('hello', 10, '')`
    /// and `lpad('hello', 3, '')` are the pair that pins step 2: an empty
    /// fill does not mean "return the input unchanged", it means "skip the
    /// padding", and the truncation has already happened by then.
    #[test]
    fn lpad_truncates_before_it_checks_whether_the_fill_can_pad() {
        for (len, fill, expected) in [
            (10i32, None, "     hello"),
            (3, None, "hel"),
            (0, None, ""),
            (-3, None, ""),
            (5, Some("xy"), "hello"),
            (11, Some("xy"), "xyxyxyhello"),
            (10, Some(""), "hello"),
            (3, Some(""), "hel"),
        ] {
            let mut args = vec![lit_text("hello"), lit_i32(len)];
            if let Some(f) = fill {
                args.push(lit_text(f));
            }
            let expr = sf(
                if fill.is_some() { OID_LPAD_3 } else { OID_LPAD_2 },
                args,
            );
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "lpad('hello', {len}, {fill:?})"
            );
        }
    }

    /// `rpad`'s counterpart. Note that the truncating and empty-fill rows are
    /// *identical* to `lpad`'s — the two differ only in where the padding
    /// goes, so a test that only checked the padded rows would not notice an
    /// implementation that truncated from the wrong end.
    #[test]
    fn rpad_pads_on_the_right_but_truncates_from_the_same_end_as_lpad() {
        for (len, fill, expected) in [
            (10i32, None, "hello     "),
            (3, None, "hel"),
            (-3, None, ""),
            (11, Some("xy"), "helloxyxyxy"),
            (10, Some(""), "hello"),
            (3, Some(""), "hel"),
        ] {
            let mut args = vec![lit_text("hello"), lit_i32(len)];
            if let Some(f) = fill {
                args.push(lit_text(f));
            }
            let expr = sf(
                if fill.is_some() { OID_RPAD_3 } else { OID_RPAD_2 },
                args,
            );
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "rpad('hello', {len}, {fill:?})"
            );
        }
    }

    /// The fill cycles character by character rather than being repeated
    /// whole and trimmed. Verified live: `lpad('a', 6, 'xyz') = 'xyzxya'` and
    /// `rpad('a', 6, 'xyz') = 'axyzxy'`. Repeating `'xyz'` twice and taking
    /// the first five characters happens to give the same answer here, but
    /// the character-wise reading is the one Postgres implements and the two
    /// part company as soon as the fill is padded from the far end.
    #[test]
    fn pad_fill_wraps_around_mid_string() {
        let l = sf(OID_LPAD_3, vec![lit_text("a"), lit_i32(6), lit_text("xyz")]);
        assert_eq!(str_array(&eval(&l, &one_row()).unwrap()).value(0), "xyzxya");
        let r = sf(OID_RPAD_3, vec![lit_text("a"), lit_i32(6), lit_text("xyz")]);
        assert_eq!(str_array(&eval(&r, &one_row()).unwrap()).value(0), "axyzxy");
    }

    /// `len` counts characters, not bytes. Verified live:
    /// `lpad('héllo', 8, 'é') = 'éééhéllo'` — eight characters, twelve bytes.
    /// A byte-based implementation would produce five pad characters here
    /// instead of three.
    #[test]
    fn pad_length_is_in_characters_not_bytes() {
        let expr = sf(
            OID_LPAD_3,
            vec![lit_text("héllo"), lit_i32(8), lit_text("é")],
        );
        let out = eval(&expr, &one_row()).unwrap();
        assert_eq!(str_array(&out).value(0), "éééhéllo");
        assert_eq!(str_array(&out).value(0).chars().count(), 8);

        let trunc = sf(OID_LPAD_2, vec![lit_text("héllo"), lit_i32(2)]);
        assert_eq!(str_array(&eval(&trunc, &one_row()).unwrap()).value(0), "hé");
    }

    /// Past `(MaxAllocSize - VARHDRSZ) / 4` Postgres refuses rather than
    /// allocating. Both sides of the boundary were checked live; only the
    /// erroring side is asserted here, because the accepted side would have
    /// this test build a gigabyte of padding.
    #[test]
    fn pad_beyond_the_allocation_ceiling_errors_instead_of_allocating() {
        assert_eq!(PAD_MAX_LEN, 268_435_454);
        let expr = sf(OID_LPAD_2, vec![lit_text("hello"), lit_i32(i32::MAX)]);
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(
            format!("{err}").contains("requested length too large"),
            "got {err}"
        );
    }

    /// **Strictness beats the size check.** `lpad(NULL, 2147483647)` is NULL
    /// on the live server, not an error: a strict function is never entered
    /// when an argument is NULL, so the length it would have rejected is
    /// never looked at. A whole-array pre-check of `len` — the shape
    /// [`eval_substr`] uses for its negative-length rejection — would get
    /// this wrong, which is why the ceiling is tested per row.
    #[test]
    fn pad_of_null_is_null_even_at_a_length_that_would_error() {
        let expr = sf(OID_LPAD_2, vec![lit_text_null(), lit_i32(i32::MAX)]);
        assert!(eval(&expr, &one_row()).unwrap().is_null(0));

        let null_fill = sf(
            OID_LPAD_3,
            vec![lit_text("a"), lit_i32(5), Expr::Literal(Datum::Null, PgType::TEXT)],
        );
        assert!(eval(&null_fill, &one_row()).unwrap().is_null(0));
    }

    // ─── repeat (oid 1622) ──────────────────────────────────────────────

    /// A non-positive count is an empty string, not an error and not the
    /// input. `i32::MIN` is included because clamping via negation would
    /// overflow there; verified live that `repeat('ab', -2147483648) = ''`.
    #[test]
    fn repeat_with_a_non_positive_count_is_the_empty_string() {
        for (count, expected) in [
            (3i32, "ababab"),
            (1, "ab"),
            (0, ""),
            (-3, ""),
            (i32::MIN, ""),
        ] {
            let expr = sf(OID_REPEAT, vec![lit_text("ab"), lit_i32(count)]);
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "repeat('ab', {count})"
            );
        }
    }

    /// The size ceiling is measured in bytes, and the boundary was confirmed
    /// from both sides live: `repeat('a', 1073741820)` errors while
    /// `repeat('a', 1073741819)` does not. Only the erroring side is asserted
    /// — the accepted side is a gigabyte.
    ///
    /// The empty-string row is the one that shows the check is a *product*
    /// and not a bound on the count alone: `repeat('', 2147483647)` succeeds
    /// (the live server returns `''`, slowly) because zero bytes repeated any
    /// number of times still fits.
    #[test]
    fn repeat_checks_bytes_times_count_not_the_count_alone() {
        let too_big = sf(OID_REPEAT, vec![lit_text("a"), lit_i32(1_073_741_820)]);
        let err = eval(&too_big, &one_row()).unwrap_err();
        assert!(
            format!("{err}").contains("requested length too large"),
            "got {err}"
        );

        let empty = sf(OID_REPEAT, vec![lit_text(""), lit_i32(i32::MAX)]);
        assert_eq!(str_array(&eval(&empty, &one_row()).unwrap()).value(0), "");
    }

    #[test]
    fn repeat_of_null_is_null() {
        let expr = sf(OID_REPEAT, vec![lit_text_null(), lit_i32(3)]);
        assert!(eval(&expr, &one_row()).unwrap().is_null(0));
    }

    // ─── split_part (oid 2088) ──────────────────────────────────────────

    /// The full live-verified table, including the negative indices Postgres
    /// grew in version 14. Out of range in *either* direction is an empty
    /// string, not an error — only a field position of zero errors, and that
    /// is covered separately below.
    #[test]
    fn split_part_indexes_from_either_end_and_runs_off_quietly() {
        for (field, expected) in [
            (1i32, "a"),
            (2, "b"),
            (3, "c"),
            (4, ""),
            (i32::MAX, ""),
            (-1, "c"),
            (-2, "b"),
            (-3, "a"),
            (-4, ""),
            (i32::MIN, ""),
        ] {
            let expr = sf(
                OID_SPLIT_PART,
                vec![lit_text("a,b,c"), lit_text(","), lit_i32(field)],
            );
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "split_part('a,b,c', ',', {field})"
            );
        }
    }

    /// Zero is the one field position that errors — `ERROR: field position
    /// must not be zero` on the live server — because it is the one value
    /// with no non-arbitrary reading once negative positions count from the
    /// right.
    #[test]
    fn split_part_field_zero_errors() {
        let expr = sf(
            OID_SPLIT_PART,
            vec![lit_text("a,b,c"), lit_text(","), lit_i32(0)],
        );
        let err = eval(&expr, &one_row()).unwrap_err();
        assert!(
            format!("{err}").contains("field position must not be zero"),
            "got {err}"
        );
    }

    /// **Strictness beats the zero-field error**, exactly as it beats the
    /// size check in [`pad_of_null_is_null_even_at_a_length_that_would_error`]:
    /// `split_part(NULL, ',', 0)` is NULL on the live server, not an error.
    #[test]
    fn split_part_of_null_is_null_even_at_the_field_position_that_errors() {
        let expr = sf(
            OID_SPLIT_PART,
            vec![lit_text_null(), lit_text(","), lit_i32(0)],
        );
        assert!(eval(&expr, &one_row()).unwrap().is_null(0));
    }

    /// An empty delimiter does not split at every character boundary the way
    /// Rust's `str::split("")` would — Postgres treats the whole subject as a
    /// single field, reachable as field `1` or `-1` and nowhere else. Rust's
    /// own behaviour here would yield `''` for field 1 (its leading empty
    /// match), so this is a genuine divergence that has to be special-cased,
    /// not an accident that works out.
    #[test]
    fn split_part_with_an_empty_delimiter_makes_exactly_one_field() {
        for (field, expected) in [(1i32, "a,b,c"), (-1, "a,b,c"), (2, ""), (-2, "")] {
            let expr = sf(
                OID_SPLIT_PART,
                vec![lit_text("a,b,c"), lit_text(""), lit_i32(field)],
            );
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                expected,
                "split_part('a,b,c', '', {field})"
            );
        }
    }

    /// A multi-character delimiter is matched as a unit, and matching is on
    /// characters not bytes. Verified live: `split_part('axxbxxc', 'xx', 2) =
    /// 'b'` and `split_part('a世b世c', '世', 2) = 'b'`.
    #[test]
    fn split_part_delimiter_can_be_multi_character_and_multi_byte() {
        let ascii = sf(
            OID_SPLIT_PART,
            vec![lit_text("axxbxxc"), lit_text("xx"), lit_i32(2)],
        );
        assert_eq!(str_array(&eval(&ascii, &one_row()).unwrap()).value(0), "b");

        let utf8 = sf(
            OID_SPLIT_PART,
            vec![lit_text("a世b世c"), lit_text("世"), lit_i32(2)],
        );
        assert_eq!(str_array(&eval(&utf8, &one_row()).unwrap()).value(0), "b");
    }

    /// A trailing delimiter makes a trailing EMPTY field, not one fewer
    /// field. Verified live that both `split_part('a,b,', ',', 3)` and
    /// `split_part('a,b,', ',', -1)` are `''` — the third field exists and is
    /// empty, so counting from the right lands on it rather than on `'b'`.
    #[test]
    fn split_part_counts_the_empty_field_a_trailing_delimiter_creates() {
        for field in [3i32, -1] {
            let expr = sf(
                OID_SPLIT_PART,
                vec![lit_text("a,b,"), lit_text(","), lit_i32(field)],
            );
            assert_eq!(
                str_array(&eval(&expr, &one_row()).unwrap()).value(0),
                "",
                "split_part('a,b,', ',', {field})"
            );
        }

        let second = sf(
            OID_SPLIT_PART,
            vec![lit_text("a,b,"), lit_text(","), lit_i32(-2)],
        );
        assert_eq!(str_array(&eval(&second, &one_row()).unwrap()).value(0), "b");
    }

    // ─── age (oid 2058) ─────────────────────────────────────────────────

    /// A one-row batch of two `timestamp` columns, in microseconds since the
    /// Unix epoch — the physical layout `basin_pgtype` maps `oid::TIMESTAMP`
    /// onto.
    fn batch_ts2(a: Option<i64>, b: Option<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "b",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![a])),
                Arc::new(TimestampMicrosecondArray::from(vec![b])),
            ],
        )
        .unwrap()
    }

    fn age_of(a: i64, b: i64) -> (i32, i32, i64) {
        let expr = sf(OID_AGE_TIMESTAMP, vec![col(0, "a"), col(1, "b")]);
        let out = eval(&expr, &batch_ts2(Some(a), Some(b))).unwrap();
        let arr = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("age must produce an interval");
        let v = arr.value(0);
        (v.months, v.days, v.nanoseconds / 1_000)
    }

    /// The whole battery below was read off a live PostgreSQL 18 in one
    /// query, as `(months, days, microseconds)` triples — the components
    /// Postgres's own interval is made of, rather than a rendered string,
    /// so the comparison does not depend on `IntervalStyle`.
    ///
    /// The randomly-generated rows (dates from 1948 to 2030, both signs, all
    /// with sub-second parts) are there to catch an arithmetic slip that a
    /// hand-picked table would miss; the named rows underneath them each pin
    /// one specific rule.
    #[test]
    fn age_matches_postgres_on_a_live_generated_battery() {
        // (dt1_micros, dt2_micros, months, days, microseconds)
        for (a, b, months, days, micros) in [
            (294133569060641i64, 1649464880103772i64, -515i32, -10i32, -60911043131i64),
            (1196480441821458, -297619335288745, 568, 4, 70977110203),
            (986860800000000, -396144000000000, 525, 27, 0),
            (1710496800000000, 1674217800000000, 13, 25, 77400000000),
            (1674217800000000, 1710496800000000, -13, -25, -77400000000),
            (-679436915203395, 1106406793005636, -679, -1, -42108209031),
            (1591011357052563, -389228188903659, 753, 0, 37945956222),
            (728458469454512, -128433918397942, 325, 24, 63587852454),
            (-283367683917185, -180557464851359, -39, -3, -80619065826),
            (-614266883503917, 828173074396397, -548, -14, -78357900314),
        ] {
            assert_eq!(
                age_of(a, b),
                (months, days, micros),
                "age({a}, {b}) as (months, days, microseconds)"
            );
        }
    }

    /// **The borrow takes its day count from the earlier argument's month.**
    /// These two rows are the discriminating pair: 2000 is a leap year and
    /// 1900 is not, so a borrow that counted the days of the intervening
    /// *February* would answer `1 mon 1 day` for one and `1 mon 2 days` for
    /// the other. The live server answers `1 mon 1 day` for both — it borrows
    /// 31 days from January either way.
    #[test]
    fn age_borrows_days_from_the_earlier_arguments_month_not_the_month_between() {
        // 2000-03-01 vs 2000-01-31, then 1900-03-01 vs 1900-01-31.
        assert_eq!(age_of(951868800000000, 949276800000000), (1, 1, 0));
        assert_eq!(age_of(-2203891200000000, -2206396800000000), (1, 1, 0));
    }

    /// `age` is a symbolic difference, not an elapsed duration: these two
    /// pairs are 60 and 30 days apart respectively, and the answers are
    /// `2 mons` and `1 mon 1 day`. Any implementation that divided a
    /// microsecond count by an average month length would get both wrong.
    #[test]
    fn age_is_symbolic_not_an_elapsed_duration() {
        // 2024-03-31 vs 2024-01-31 (60 days apart) is a whole 2 months.
        assert_eq!(age_of(1711843200000000, 1706659200000000), (2, 0, 0));
        // 2024-03-01 vs 2024-01-31 (30 days apart) is NOT 1 month.
        assert_eq!(age_of(1709251200000000, 1706659200000000), (1, 1, 0));
    }

    /// Reversing the arguments negates every component — Postgres does not
    /// return a mixed-sign interval. Verified live:
    /// `age('2023-01-20 12:30','2024-03-15 10:00')` is
    /// `-1 years -1 mons -25 days -21:30:00`.
    #[test]
    fn age_reversed_negates_every_component() {
        let forwards = age_of(1710496800000000, 1674217800000000);
        let backwards = age_of(1674217800000000, 1710496800000000);
        assert_eq!(forwards, (13, 25, 77400000000));
        assert_eq!(
            backwards,
            (-forwards.0, -forwards.1, -forwards.2),
            "every component flips sign together"
        );
    }

    /// Equal arguments are a zero interval, and a sub-second difference is
    /// carried all the way down without disturbing the date fields.
    /// `age('2020-03-01', '2020-02-29 23:59:59.999999')` is one microsecond
    /// on the live server — across a leap day, in a leap year.
    #[test]
    fn age_of_equal_timestamps_is_zero_and_a_microsecond_survives() {
        assert_eq!(age_of(1710460800000000, 1710460800000000), (0, 0, 0));
        assert_eq!(age_of(1583020800000000, 1583020799999999), (0, 0, 1));
    }

    #[test]
    fn age_is_strict_in_both_arguments() {
        let expr = sf(OID_AGE_TIMESTAMP, vec![col(0, "a"), col(1, "b")]);
        for (a, b) in [(None, Some(0i64)), (Some(0i64), None), (None, None)] {
            let out = eval(&expr, &batch_ts2(a, b)).unwrap();
            assert!(out.is_null(0), "age({a:?}, {b:?}) must be NULL");
        }
    }

    /// The three `age` overloads that need session state must NOT resolve
    /// here — falling through to the "not implemented" arm is the intended
    /// outcome, not an oversight. `age(timestamptz, timestamptz)` is the
    /// dangerous one: it looks like a pure function of two instants but its
    /// answer depends on the session timezone (verified live across the US
    /// DST boundary), so a plausible-looking implementation here would return
    /// a confidently wrong answer instead of falling back.
    #[test]
    fn the_session_dependent_age_overloads_are_left_unimplemented() {
        for oid_val in [1199u32, 1386, 2059] {
            let expr = sf(oid_val, vec![col(0, "a"), col(1, "b")]);
            let err = eval(&expr, &batch_ts2(Some(0), Some(0))).unwrap_err();
            assert!(
                format!("{err}").contains("is not implemented in eval yet"),
                "oid {oid_val} must fall through, got {err}"
            );
        }
    }

    /// The calendar decomposition `age` is built on, checked directly at the
    /// boundaries most likely to be off by one: the epoch itself, the day
    /// before it (a negative day number, where truncating division would put
    /// the time of day on the wrong date), and both century leap rules.
    #[test]
    fn civil_from_micros_handles_pre_epoch_and_the_century_leap_rules() {
        assert_eq!(civil_from_micros(0), (1970, 1, 1, 0, 0, 0, 0));
        assert_eq!(civil_from_micros(-1), (1969, 12, 31, 23, 59, 59, 999_999));
        assert_eq!(
            civil_from_micros(-86_400_000_000),
            (1969, 12, 31, 0, 0, 0, 0)
        );
        assert!(is_leap_year(2000), "divisible by 400");
        assert!(!is_leap_year(1900), "divisible by 100 but not 400");
        assert!(is_leap_year(2024));
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(1900, 2), 28);
    }

    /// The new string functions are per-row over real columns, not just
    /// literals, and each keeps NULL rows NULL without disturbing the
    /// alignment of the rows around them.
    #[test]
    fn the_new_string_functions_are_row_wise_over_a_column() {
        let batch = batch_str1("s", vec![Some("hello"), None, Some("ab")]);
        for (oid_val, args, expected) in [
            (
                OID_INITCAP,
                vec![col(0, "s")],
                vec![Some("Hello"), None, Some("Ab")],
            ),
            (
                OID_LPAD_3,
                vec![col(0, "s"), lit_i32(6), lit_text("*")],
                vec![Some("*hello"), None, Some("****ab")],
            ),
            (
                OID_RPAD_3,
                vec![col(0, "s"), lit_i32(6), lit_text("*")],
                vec![Some("hello*"), None, Some("ab****")],
            ),
            (
                OID_REPEAT,
                vec![col(0, "s"), lit_i32(2)],
                vec![Some("hellohello"), None, Some("abab")],
            ),
        ] {
            let expr = sf(oid_val, args);
            let out = eval(&expr, &batch).unwrap();
            let arr = str_array(&out);
            assert_eq!(arr.len(), 3);
            for (i, want) in expected.iter().enumerate() {
                match want {
                    Some(w) => assert_eq!(arr.value(i), *w, "oid {oid_val} row {i}"),
                    None => assert!(arr.is_null(i), "oid {oid_val} row {i} must be NULL"),
                }
            }
        }
    }

    // ─── position (oid 849) ─────────────────────────────────────────────

    /// **The argument-order trap.** `POSITION(needle IN haystack)` is
    /// grammar; the `pg_proc` row it desugars to takes `(haystack, needle)`,
    /// the same order as `strpos`. So a call written in functional notation
    /// reads backwards relative to the syntax, and both live-server answers
    /// below are needed to tell a correct implementation from a reversed one
    /// — a single example would pass either way round:
    ///
    /// ```text
    /// pg_catalog.position('abc', 'b') = 2   -- ('abc' is the haystack)
    /// pg_catalog.position('b', 'abc') = 0   -- 'abc' does not occur in 'b'
    /// ```
    #[test]
    fn position_takes_haystack_then_needle_not_the_in_syntax_order() {
        let forwards = sf(OID_POSITION, vec![lit_text("abc"), lit_text("b")]);
        assert_eq!(i32_array(&eval(&forwards, &one_row()).unwrap()).value(0), 2);

        let reversed = sf(OID_POSITION, vec![lit_text("b"), lit_text("abc")]);
        assert_eq!(
            i32_array(&eval(&reversed, &one_row()).unwrap()).value(0),
            0,
            "pg_catalog.position('b', 'abc') is 0 — reading the arguments the \
             POSITION(x IN y) way would wrongly give 2"
        );
    }

    /// `position` shares `strpos`'s other two rules, pinned under this OID so
    /// a future divergence between the two implementations is caught here.
    /// Verified live: `pg_catalog.position('héllo世界', '世') = 6`
    /// (characters, not bytes — the byte offset would be 8) and
    /// `pg_catalog.position('abc', '') = 1` (an empty needle is found at the
    /// start, not "not found").
    #[test]
    fn position_counts_characters_and_finds_the_empty_needle_at_one() {
        let multibyte = sf(OID_POSITION, vec![lit_text("héllo世界"), lit_text("世")]);
        assert_eq!(
            i32_array(&eval(&multibyte, &one_row()).unwrap()).value(0),
            6
        );

        let empty = sf(OID_POSITION, vec![lit_text("abc"), lit_text("")]);
        assert_eq!(i32_array(&eval(&empty, &one_row()).unwrap()).value(0), 1);
    }

    #[test]
    fn position_of_a_null_argument_is_null() {
        let expr = sf(OID_POSITION, vec![lit_text("abc"), lit_text_null()]);
        let result = eval(&expr, &one_row()).unwrap();
        assert!(i32_array(&result).is_null(0));
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
        assert_eq!(
            f64_array(&log_result).value(0),
            2.0,
            "log(100) must be 2 (base 10)"
        );

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
        assert_eq!(
            arr.len(),
            3,
            "pi() is niladic but must still fill every row"
        );
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
        assert_eq!(
            arr.value(3),
            0.0,
            "sign(-0.0) must be 0, not -1 (f64::signum's answer)"
        );
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

    // ── The float8 policy (see the module docs' "IEEE-754 first, domain
    // second, range last"). Every expectation below was read off a live
    // PostgreSQL 18.2, not from the C source. ─────────────────────────────

    /// Phase 1 before phase 2: `asin('NaN')`/`acos('NaN')` are `NaN` live,
    /// NOT "input is out of range" — even though `NaN` is not inside
    /// `[-1, 1]` by this file's `contains` phrasing of the guard. Ordering
    /// the guard ahead of the special case is exactly the defect this pins.
    #[test]
    fn asin_and_acos_of_nan_are_nan_not_a_domain_error() {
        let batch = one_row();
        for oid in [OID_ASIN_FLOAT8, OID_ACOS_FLOAT8] {
            let r = eval(&sf(oid, vec![lit_f64(f64::NAN)]), &batch).unwrap();
            assert!(
                f64_array(&r).value(0).is_nan(),
                "oid {oid} of NaN must be NaN, not an out-of-range error"
            );
        }
    }

    /// The mirror image for `sin`/`cos`/`tan`: `NaN` passes through, but an
    /// infinite argument is `ERROR: 22003: input is out of range` live —
    /// libm's `NaN` answer would be a silently invented result.
    #[test]
    fn trig_of_infinity_errors_while_nan_passes_through() {
        let batch = one_row();
        for oid in [OID_SIN_FLOAT8, OID_COS_FLOAT8, OID_TAN_FLOAT8] {
            let nan = eval(&sf(oid, vec![lit_f64(f64::NAN)]), &batch).unwrap();
            assert!(f64_array(&nan).value(0).is_nan(), "oid {oid} of NaN is NaN");

            for inf in [f64::INFINITY, f64::NEG_INFINITY] {
                let err = eval(&sf(oid, vec![lit_f64(inf)]), &batch).unwrap_err();
                assert_type_mismatch_contains(err, "input is out of range");
            }
        }
    }

    /// Phase 3 for `exp`: an infinite input legitimises an infinite or zero
    /// result, a finite one does not. The underflow boundary is checked
    /// either side — `exp(-745)` is `5e-324` live, `exp(-746)` is an error.
    #[test]
    fn exp_range_checks_the_result_but_lets_infinite_input_through() {
        let batch = one_row();
        let inf = eval(&sf(OID_EXP_FLOAT8, vec![lit_f64(f64::INFINITY)]), &batch).unwrap();
        assert_eq!(f64_array(&inf).value(0), f64::INFINITY);
        let neg_inf = eval(
            &sf(OID_EXP_FLOAT8, vec![lit_f64(f64::NEG_INFINITY)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&neg_inf).value(0), 0.0, "exp(-Infinity) = 0 live");

        let over = eval(&sf(OID_EXP_FLOAT8, vec![lit_f64(f64::MAX)]), &batch).unwrap_err();
        assert_type_mismatch_contains(over, "value out of range: overflow");
        let under = eval(&sf(OID_EXP_FLOAT8, vec![lit_f64(f64::MIN)]), &batch).unwrap_err();
        assert_type_mismatch_contains(under, "value out of range: underflow");

        let just_inside = eval(&sf(OID_EXP_FLOAT8, vec![lit_f64(-745.0)]), &batch).unwrap();
        assert_ne!(
            f64_array(&just_inside).value(0),
            0.0,
            "exp(-745) is 5e-324 live, a real subnormal, not an underflow"
        );
        let just_outside = eval(&sf(OID_EXP_FLOAT8, vec![lit_f64(-746.0)]), &batch).unwrap_err();
        assert_type_mismatch_contains(just_outside, "value out of range: underflow");
    }

    /// `degrees` scales up, so overflow is the reachable end; `radians`
    /// scales down, so underflow is. Both let an infinite input through and
    /// both treat a zero *input* as licence for a zero result.
    #[test]
    fn degrees_overflows_and_radians_underflows_but_zero_and_infinity_are_fine() {
        let batch = one_row();

        let deg_over = eval(&sf(OID_DEGREES_FLOAT8, vec![lit_f64(f64::MAX)]), &batch).unwrap_err();
        assert_type_mismatch_contains(deg_over, "value out of range: overflow");
        let deg_inf = eval(
            &sf(OID_DEGREES_FLOAT8, vec![lit_f64(f64::INFINITY)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&deg_inf).value(0), f64::INFINITY);
        let deg_zero = eval(&sf(OID_DEGREES_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap();
        assert_eq!(f64_array(&deg_zero).value(0), 0.0);

        // 4.9e-324 is the smallest positive subnormal; scaling it by
        // pi/180 lands on zero, which live Postgres calls an underflow.
        let rad_under = eval(&sf(OID_RADIANS_FLOAT8, vec![lit_f64(5e-324)]), &batch).unwrap_err();
        assert_type_mismatch_contains(rad_under, "value out of range: underflow");
        let rad_zero = eval(&sf(OID_RADIANS_FLOAT8, vec![lit_f64(0.0)]), &batch).unwrap();
        assert_eq!(
            f64_array(&rad_zero).value(0),
            0.0,
            "radians(0) = 0 is a genuine zero, not an underflow"
        );
    }

    /// `power`'s phase 1. `power('NaN', 0) = 1` and `power(1, 'NaN') = 1`
    /// (POSIX), everything else touching `NaN` is `NaN` — including
    /// `power(0, 'NaN')`, which the zero-base domain guard would otherwise
    /// claim, and `power(-1, 'NaN')`, which the negative-base guard would.
    #[test]
    fn power_nan_rules_run_before_the_domain_guards() {
        let batch = one_row();
        let val = |b: f64, e: f64| {
            let r = eval(&sf(OID_POWER_FLOAT8, vec![lit_f64(b), lit_f64(e)]), &batch)
                .unwrap_or_else(|e| panic!("power({b}, {e}) must not error: {e}"));
            f64_array(&r).value(0)
        };
        assert_eq!(val(f64::NAN, 0.0), 1.0);
        assert_eq!(val(f64::NAN, -0.0), 1.0);
        assert_eq!(val(1.0, f64::NAN), 1.0);
        assert!(val(f64::NAN, f64::NAN).is_nan());
        assert!(
            val(0.0, f64::NAN).is_nan(),
            "not 'zero to a negative power'"
        );
        assert!(
            val(-1.0, f64::NAN).is_nan(),
            "not 'a complex result' — NaN is decided first"
        );
    }

    /// `power`'s phase 2. `power(0, negative)` is its own SQLSTATE (2201F,
    /// "zero raised to a negative power is undefined"), where `f64::powf`
    /// hands back `Infinity`. True for `-0.0` as the base and for
    /// `-Infinity` as the exponent, both confirmed live.
    #[test]
    fn power_zero_to_a_negative_power_errors_rather_than_returning_infinity() {
        let batch = one_row();
        for (b, e) in [
            (0.0, -1.0),
            (0.0, -0.5),
            (0.0, f64::NEG_INFINITY),
            (-0.0, -1.0),
            (-0.0, f64::MIN),
        ] {
            let err =
                eval(&sf(OID_POWER_FLOAT8, vec![lit_f64(b), lit_f64(e)]), &batch).unwrap_err();
            assert_type_mismatch_contains(err, "zero raised to a negative power is undefined");
        }
        // `power(0, 0)` and `power(0, positive)` are untouched by the guard.
        let ok = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64(0.0), lit_f64(0.0)]),
            &batch,
        )
        .unwrap();
        assert_eq!(f64_array(&ok).value(0), 1.0);
    }

    /// `power`'s "is the exponent integral" test must be `floor(e) == e`, not
    /// `e.fract() == 0.0`: `f64::fract` of an infinity is `NaN`, so a
    /// `fract`-based guard rejects every infinite exponent as non-integral.
    /// Live Postgres answers all four of these without erroring.
    #[test]
    fn power_negative_base_with_an_infinite_exponent_is_not_a_complex_result() {
        let batch = one_row();
        let val = |b: f64, e: f64| {
            let r = eval(&sf(OID_POWER_FLOAT8, vec![lit_f64(b), lit_f64(e)]), &batch)
                .unwrap_or_else(|err| panic!("power({b}, {e}) must not error: {err}"));
            f64_array(&r).value(0)
        };
        assert_eq!(val(-1.0, f64::INFINITY), 1.0);
        assert_eq!(val(-1.0, f64::NEG_INFINITY), 1.0);
        assert_eq!(val(-0.5, f64::INFINITY), 0.0);
        assert_eq!(val(-0.5, f64::NEG_INFINITY), f64::INFINITY);

        // A finite non-integer exponent on a negative base still errors.
        let err = eval(
            &sf(OID_POWER_FLOAT8, vec![lit_f64(-2.0), lit_f64(0.5)]),
            &batch,
        )
        .unwrap_err();
        assert_type_mismatch_contains(
            err,
            "a negative number raised to a non-integer power yields a complex result",
        );
    }

    /// `power`'s phase 3, and the three-part zero rule the module docs spell
    /// out: an infinite or zero result is legitimate only when an infinite
    /// input or a zero base put it there.
    #[test]
    fn power_range_checks_the_result_unless_an_input_was_infinite_or_zero() {
        let batch = one_row();
        let call =
            |b: f64, e: f64| eval(&sf(OID_POWER_FLOAT8, vec![lit_f64(b), lit_f64(e)]), &batch);

        for (b, e) in [(0.5, f64::MIN), (f64::MIN, f64::MAX), (-2.0, 1e300)] {
            assert_type_mismatch_contains(call(b, e).unwrap_err(), "value out of range: overflow");
        }
        for (b, e) in [(0.5, f64::MAX), (f64::MAX, f64::MIN), (2.0, -1e300)] {
            assert_type_mismatch_contains(call(b, e).unwrap_err(), "value out of range: underflow");
        }

        // Same bit patterns, all legitimate live: infinite base, infinite
        // exponent, and a zero base respectively.
        assert_eq!(
            f64_array(&call(f64::INFINITY, f64::MIN).unwrap()).value(0),
            0.0,
            "power('Infinity', -DBL_MAX) = 0 live — the base was infinite"
        );
        assert_eq!(
            f64_array(&call(0.5, f64::INFINITY).unwrap()).value(0),
            0.0,
            "power(0.5, 'Infinity') = 0 live — the exponent was infinite"
        );
        assert_eq!(
            f64_array(&call(0.0, 5.0).unwrap()).value(0),
            0.0,
            "power(0, 5) = 0 live — a zero base makes zero an achievable answer"
        );
        assert_eq!(
            f64_array(&call(f64::INFINITY, 2.0).unwrap()).value(0),
            f64::INFINITY
        );
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
        let ceiling_n = eval(&sf(OID_CEILING_NUMERIC, vec![col(0, "x")]), &numeric_batch).unwrap();
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
        assert_eq!(
            arr.value(1),
            -3.0,
            "trunc(-3.7) must be -3, not -4 (floor's answer)"
        );
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

    // ─── date_trunc / date_part, and the session context they need ──────
    //
    // Every expected value in this section was produced by a live
    // PostgreSQL 18.2 and is pasted here verbatim — never computed by this
    // file, and never recalled. The generating query, per row, was
    //
    // ```sql
    // SET TimeZone TO '<zone>';
    // SELECT (extract(epoch from date_trunc('<unit>', '<instant>'::timestamptz))
    //         * 1000000)::bigint;
    // ```
    //
    // and the `date_part` rows are the same shape with `date_part` and no
    // scaling. Zones are the three the differential battery
    // (`tests/orphan_functions.rs`) runs these functions under: `UTC`,
    // `America/New_York` (a whole-hour DST step) and
    // `Australia/Lord_Howe` (a **half-hour** DST step, from `+10:30` to
    // `+11:00`). Instants include both 2024 US transitions and both 2024
    // Lord Howe transitions.

    fn ts_batch(dt: DataType, micros: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", dt.clone(), true)]));
        let arr: ArrayRef = match &dt {
            DataType::Timestamp(_, Some(tz)) => Arc::new(
                TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone(tz.clone()),
            ),
            _ => Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)])),
        };
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn tstz_batch(micros: i64) -> RecordBatch {
        ts_batch(
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            micros,
        )
    }

    fn plain_ts_batch(micros: i64) -> RecordBatch {
        ts_batch(
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            micros,
        )
    }

    fn ts_micros(v: &ArrayRef) -> i64 {
        v.as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("date_trunc must produce a timestamp")
            .value(0)
    }

    fn f64_at0(v: &ArrayRef) -> f64 {
        v.as_any()
            .downcast_ref::<Float64Array>()
            .expect("date_part must produce double precision")
            .value(0)
    }

    /// `date_trunc(text, timestamptz)` (oid 1217) in three session zones,
    /// across both 2024 DST transitions in each — the case that was
    /// previously unimplementable because `eval()` had no session at all.
    #[test]
    fn date_trunc_timestamptz_matches_postgres_in_every_session_zone() {
        const CASES: &[(&str, i64, &str, i64)] = &[
        ("UTC", 0, "microseconds", 0),
        ("UTC", 0, "milliseconds", 0),
        ("UTC", 0, "second", 0),
        ("UTC", 0, "minute", 0),
        ("UTC", 0, "hour", 0),
        ("UTC", 0, "day", 0),
        ("UTC", 0, "week", -259200000000),
        ("UTC", 0, "month", 0),
        ("UTC", 0, "quarter", 0),
        ("UTC", 0, "year", 0),
        ("UTC", 0, "decade", 0),
        ("UTC", 0, "century", -2177452800000000),
        ("UTC", 0, "millennium", -30578688000000000),
        ("UTC", 1710052200000000, "microseconds", 1710052200000000),
        ("UTC", 1710052200000000, "milliseconds", 1710052200000000),
        ("UTC", 1710052200000000, "second", 1710052200000000),
        ("UTC", 1710052200000000, "minute", 1710052200000000),
        ("UTC", 1710052200000000, "hour", 1710050400000000),
        ("UTC", 1710052200000000, "day", 1710028800000000),
        ("UTC", 1710052200000000, "week", 1709510400000000),
        ("UTC", 1710052200000000, "month", 1709251200000000),
        ("UTC", 1710052200000000, "quarter", 1704067200000000),
        ("UTC", 1710052200000000, "year", 1704067200000000),
        ("UTC", 1710052200000000, "decade", 1577836800000000),
        ("UTC", 1710052200000000, "century", 978307200000000),
        ("UTC", 1710052200000000, "millennium", 978307200000000),
        ("UTC", 1730611800000000, "microseconds", 1730611800000000),
        ("UTC", 1730611800000000, "milliseconds", 1730611800000000),
        ("UTC", 1730611800000000, "second", 1730611800000000),
        ("UTC", 1730611800000000, "minute", 1730611800000000),
        ("UTC", 1730611800000000, "hour", 1730610000000000),
        ("UTC", 1730611800000000, "day", 1730592000000000),
        ("UTC", 1730611800000000, "week", 1730073600000000),
        ("UTC", 1730611800000000, "month", 1730419200000000),
        ("UTC", 1730611800000000, "quarter", 1727740800000000),
        ("UTC", 1730611800000000, "year", 1704067200000000),
        ("UTC", 1730611800000000, "decade", 1577836800000000),
        ("UTC", 1730611800000000, "century", 978307200000000),
        ("UTC", 1730611800000000, "millennium", 978307200000000),
        ("UTC", 1719835200000000, "microseconds", 1719835200000000),
        ("UTC", 1719835200000000, "milliseconds", 1719835200000000),
        ("UTC", 1719835200000000, "second", 1719835200000000),
        ("UTC", 1719835200000000, "minute", 1719835200000000),
        ("UTC", 1719835200000000, "hour", 1719835200000000),
        ("UTC", 1719835200000000, "day", 1719792000000000),
        ("UTC", 1719835200000000, "week", 1719792000000000),
        ("UTC", 1719835200000000, "month", 1719792000000000),
        ("UTC", 1719835200000000, "quarter", 1719792000000000),
        ("UTC", 1719835200000000, "year", 1704067200000000),
        ("UTC", 1719835200000000, "decade", 1577836800000000),
        ("UTC", 1719835200000000, "century", 978307200000000),
        ("UTC", 1719835200000000, "millennium", 978307200000000),
        ("UTC", 1712415600000000, "microseconds", 1712415600000000),
        ("UTC", 1712415600000000, "milliseconds", 1712415600000000),
        ("UTC", 1712415600000000, "second", 1712415600000000),
        ("UTC", 1712415600000000, "minute", 1712415600000000),
        ("UTC", 1712415600000000, "hour", 1712415600000000),
        ("UTC", 1712415600000000, "day", 1712361600000000),
        ("UTC", 1712415600000000, "week", 1711929600000000),
        ("UTC", 1712415600000000, "month", 1711929600000000),
        ("UTC", 1712415600000000, "quarter", 1711929600000000),
        ("UTC", 1712415600000000, "year", 1704067200000000),
        ("UTC", 1712415600000000, "decade", 1577836800000000),
        ("UTC", 1712415600000000, "century", 978307200000000),
        ("UTC", 1712415600000000, "millennium", 978307200000000),
        ("UTC", 1728142800000000, "microseconds", 1728142800000000),
        ("UTC", 1728142800000000, "milliseconds", 1728142800000000),
        ("UTC", 1728142800000000, "second", 1728142800000000),
        ("UTC", 1728142800000000, "minute", 1728142800000000),
        ("UTC", 1728142800000000, "hour", 1728140400000000),
        ("UTC", 1728142800000000, "day", 1728086400000000),
        ("UTC", 1728142800000000, "week", 1727654400000000),
        ("UTC", 1728142800000000, "month", 1727740800000000),
        ("UTC", 1728142800000000, "quarter", 1727740800000000),
        ("UTC", 1728142800000000, "year", 1704067200000000),
        ("UTC", 1728142800000000, "decade", 1577836800000000),
        ("UTC", 1728142800000000, "century", 978307200000000),
        ("UTC", 1728142800000000, "millennium", 978307200000000),
        ("America/New_York", 0, "microseconds", 0),
        ("America/New_York", 0, "milliseconds", 0),
        ("America/New_York", 0, "second", 0),
        ("America/New_York", 0, "minute", 0),
        ("America/New_York", 0, "hour", 0),
        ("America/New_York", 0, "day", -68400000000),
        ("America/New_York", 0, "week", -241200000000),
        ("America/New_York", 0, "month", -2660400000000),
        ("America/New_York", 0, "quarter", -7934400000000),
        ("America/New_York", 0, "year", -31518000000000),
        ("America/New_York", 0, "decade", -315601200000000),
        ("America/New_York", 0, "century", -2177434800000000),
        ("America/New_York", 0, "millennium", -30578670238000000),
        ("America/New_York", 1710052200000000, "microseconds", 1710052200000000),
        ("America/New_York", 1710052200000000, "milliseconds", 1710052200000000),
        ("America/New_York", 1710052200000000, "second", 1710052200000000),
        ("America/New_York", 1710052200000000, "minute", 1710052200000000),
        ("America/New_York", 1710052200000000, "hour", 1710050400000000),
        ("America/New_York", 1710052200000000, "day", 1710046800000000),
        ("America/New_York", 1710052200000000, "week", 1709528400000000),
        ("America/New_York", 1710052200000000, "month", 1709269200000000),
        ("America/New_York", 1710052200000000, "quarter", 1704085200000000),
        ("America/New_York", 1710052200000000, "year", 1704085200000000),
        ("America/New_York", 1710052200000000, "decade", 1577854800000000),
        ("America/New_York", 1710052200000000, "century", 978325200000000),
        ("America/New_York", 1710052200000000, "millennium", 978325200000000),
        ("America/New_York", 1730611800000000, "microseconds", 1730611800000000),
        ("America/New_York", 1730611800000000, "milliseconds", 1730611800000000),
        ("America/New_York", 1730611800000000, "second", 1730611800000000),
        ("America/New_York", 1730611800000000, "minute", 1730611800000000),
        ("America/New_York", 1730611800000000, "hour", 1730610000000000),
        ("America/New_York", 1730611800000000, "day", 1730606400000000),
        ("America/New_York", 1730611800000000, "week", 1730088000000000),
        ("America/New_York", 1730611800000000, "month", 1730433600000000),
        ("America/New_York", 1730611800000000, "quarter", 1727755200000000),
        ("America/New_York", 1730611800000000, "year", 1704085200000000),
        ("America/New_York", 1730611800000000, "decade", 1577854800000000),
        ("America/New_York", 1730611800000000, "century", 978325200000000),
        ("America/New_York", 1730611800000000, "millennium", 978325200000000),
        ("America/New_York", 1719835200000000, "microseconds", 1719835200000000),
        ("America/New_York", 1719835200000000, "milliseconds", 1719835200000000),
        ("America/New_York", 1719835200000000, "second", 1719835200000000),
        ("America/New_York", 1719835200000000, "minute", 1719835200000000),
        ("America/New_York", 1719835200000000, "hour", 1719835200000000),
        ("America/New_York", 1719835200000000, "day", 1719806400000000),
        ("America/New_York", 1719835200000000, "week", 1719806400000000),
        ("America/New_York", 1719835200000000, "month", 1719806400000000),
        ("America/New_York", 1719835200000000, "quarter", 1719806400000000),
        ("America/New_York", 1719835200000000, "year", 1704085200000000),
        ("America/New_York", 1719835200000000, "decade", 1577854800000000),
        ("America/New_York", 1719835200000000, "century", 978325200000000),
        ("America/New_York", 1719835200000000, "millennium", 978325200000000),
        ("America/New_York", 1712415600000000, "microseconds", 1712415600000000),
        ("America/New_York", 1712415600000000, "milliseconds", 1712415600000000),
        ("America/New_York", 1712415600000000, "second", 1712415600000000),
        ("America/New_York", 1712415600000000, "minute", 1712415600000000),
        ("America/New_York", 1712415600000000, "hour", 1712415600000000),
        ("America/New_York", 1712415600000000, "day", 1712376000000000),
        ("America/New_York", 1712415600000000, "week", 1711944000000000),
        ("America/New_York", 1712415600000000, "month", 1711944000000000),
        ("America/New_York", 1712415600000000, "quarter", 1711944000000000),
        ("America/New_York", 1712415600000000, "year", 1704085200000000),
        ("America/New_York", 1712415600000000, "decade", 1577854800000000),
        ("America/New_York", 1712415600000000, "century", 978325200000000),
        ("America/New_York", 1712415600000000, "millennium", 978325200000000),
        ("America/New_York", 1728142800000000, "microseconds", 1728142800000000),
        ("America/New_York", 1728142800000000, "milliseconds", 1728142800000000),
        ("America/New_York", 1728142800000000, "second", 1728142800000000),
        ("America/New_York", 1728142800000000, "minute", 1728142800000000),
        ("America/New_York", 1728142800000000, "hour", 1728140400000000),
        ("America/New_York", 1728142800000000, "day", 1728100800000000),
        ("America/New_York", 1728142800000000, "week", 1727668800000000),
        ("America/New_York", 1728142800000000, "month", 1727755200000000),
        ("America/New_York", 1728142800000000, "quarter", 1727755200000000),
        ("America/New_York", 1728142800000000, "year", 1704085200000000),
        ("America/New_York", 1728142800000000, "decade", 1577854800000000),
        ("America/New_York", 1728142800000000, "century", 978325200000000),
        ("America/New_York", 1728142800000000, "millennium", 978325200000000),
        ("Australia/Lord_Howe", 0, "microseconds", 0),
        ("Australia/Lord_Howe", 0, "milliseconds", 0),
        ("Australia/Lord_Howe", 0, "second", 0),
        ("Australia/Lord_Howe", 0, "minute", 0),
        ("Australia/Lord_Howe", 0, "hour", 0),
        ("Australia/Lord_Howe", 0, "day", -36000000000),
        ("Australia/Lord_Howe", 0, "week", -295200000000),
        ("Australia/Lord_Howe", 0, "month", -36000000000),
        ("Australia/Lord_Howe", 0, "quarter", -36000000000),
        ("Australia/Lord_Howe", 0, "year", -36000000000),
        ("Australia/Lord_Howe", 0, "decade", -36000000000),
        ("Australia/Lord_Howe", 0, "century", -2177488800000000),
        ("Australia/Lord_Howe", 0, "millennium", -30578726180000000),
        ("Australia/Lord_Howe", 1710052200000000, "microseconds", 1710052200000000),
        ("Australia/Lord_Howe", 1710052200000000, "milliseconds", 1710052200000000),
        ("Australia/Lord_Howe", 1710052200000000, "second", 1710052200000000),
        ("Australia/Lord_Howe", 1710052200000000, "minute", 1710052200000000),
        ("Australia/Lord_Howe", 1710052200000000, "hour", 1710050400000000),
        ("Australia/Lord_Howe", 1710052200000000, "day", 1709989200000000),
        ("Australia/Lord_Howe", 1710052200000000, "week", 1709470800000000),
        ("Australia/Lord_Howe", 1710052200000000, "month", 1709211600000000),
        ("Australia/Lord_Howe", 1710052200000000, "quarter", 1704027600000000),
        ("Australia/Lord_Howe", 1710052200000000, "year", 1704027600000000),
        ("Australia/Lord_Howe", 1710052200000000, "decade", 1577797200000000),
        ("Australia/Lord_Howe", 1710052200000000, "century", 978267600000000),
        ("Australia/Lord_Howe", 1710052200000000, "millennium", 978267600000000),
        ("Australia/Lord_Howe", 1730611800000000, "microseconds", 1730611800000000),
        ("Australia/Lord_Howe", 1730611800000000, "milliseconds", 1730611800000000),
        ("Australia/Lord_Howe", 1730611800000000, "second", 1730611800000000),
        ("Australia/Lord_Howe", 1730611800000000, "minute", 1730611800000000),
        ("Australia/Lord_Howe", 1730611800000000, "hour", 1730610000000000),
        ("Australia/Lord_Howe", 1730611800000000, "day", 1730552400000000),
        ("Australia/Lord_Howe", 1730611800000000, "week", 1730034000000000),
        ("Australia/Lord_Howe", 1730611800000000, "month", 1730379600000000),
        ("Australia/Lord_Howe", 1730611800000000, "quarter", 1727703000000000),
        ("Australia/Lord_Howe", 1730611800000000, "year", 1704027600000000),
        ("Australia/Lord_Howe", 1730611800000000, "decade", 1577797200000000),
        ("Australia/Lord_Howe", 1730611800000000, "century", 978267600000000),
        ("Australia/Lord_Howe", 1730611800000000, "millennium", 978267600000000),
        ("Australia/Lord_Howe", 1719835200000000, "microseconds", 1719835200000000),
        ("Australia/Lord_Howe", 1719835200000000, "milliseconds", 1719835200000000),
        ("Australia/Lord_Howe", 1719835200000000, "second", 1719835200000000),
        ("Australia/Lord_Howe", 1719835200000000, "minute", 1719835200000000),
        ("Australia/Lord_Howe", 1719835200000000, "hour", 1719833400000000),
        ("Australia/Lord_Howe", 1719835200000000, "day", 1719754200000000),
        ("Australia/Lord_Howe", 1719835200000000, "week", 1719754200000000),
        ("Australia/Lord_Howe", 1719835200000000, "month", 1719754200000000),
        ("Australia/Lord_Howe", 1719835200000000, "quarter", 1719754200000000),
        ("Australia/Lord_Howe", 1719835200000000, "year", 1704027600000000),
        ("Australia/Lord_Howe", 1719835200000000, "decade", 1577797200000000),
        ("Australia/Lord_Howe", 1719835200000000, "century", 978267600000000),
        ("Australia/Lord_Howe", 1719835200000000, "millennium", 978267600000000),
        ("Australia/Lord_Howe", 1712415600000000, "microseconds", 1712415600000000),
        ("Australia/Lord_Howe", 1712415600000000, "milliseconds", 1712415600000000),
        ("Australia/Lord_Howe", 1712415600000000, "second", 1712415600000000),
        ("Australia/Lord_Howe", 1712415600000000, "minute", 1712415600000000),
        ("Australia/Lord_Howe", 1712415600000000, "hour", 1712413800000000),
        ("Australia/Lord_Howe", 1712415600000000, "day", 1712408400000000),
        ("Australia/Lord_Howe", 1712415600000000, "week", 1711890000000000),
        ("Australia/Lord_Howe", 1712415600000000, "month", 1711890000000000),
        ("Australia/Lord_Howe", 1712415600000000, "quarter", 1711890000000000),
        ("Australia/Lord_Howe", 1712415600000000, "year", 1704027600000000),
        ("Australia/Lord_Howe", 1712415600000000, "decade", 1577797200000000),
        ("Australia/Lord_Howe", 1712415600000000, "century", 978267600000000),
        ("Australia/Lord_Howe", 1712415600000000, "millennium", 978267600000000),
        ("Australia/Lord_Howe", 1728142800000000, "microseconds", 1728142800000000),
        ("Australia/Lord_Howe", 1728142800000000, "milliseconds", 1728142800000000),
        ("Australia/Lord_Howe", 1728142800000000, "second", 1728142800000000),
        ("Australia/Lord_Howe", 1728142800000000, "minute", 1728142800000000),
        ("Australia/Lord_Howe", 1728142800000000, "hour", 1728140400000000),
        ("Australia/Lord_Howe", 1728142800000000, "day", 1728135000000000),
        ("Australia/Lord_Howe", 1728142800000000, "week", 1727616600000000),
        ("Australia/Lord_Howe", 1728142800000000, "month", 1727703000000000),
        ("Australia/Lord_Howe", 1728142800000000, "quarter", 1727703000000000),
        ("Australia/Lord_Howe", 1728142800000000, "year", 1704027600000000),
        ("Australia/Lord_Howe", 1728142800000000, "decade", 1577797200000000),
        ("Australia/Lord_Howe", 1728142800000000, "century", 978267600000000),
        ("Australia/Lord_Howe", 1728142800000000, "millennium", 978267600000000),
        ];
        for (zone, input, unit, expected) in CASES {
            let session = EvalSession::with_time_zone(*zone);
            let batch = tstz_batch(*input);
            let expr = sf(
                OID_DATE_TRUNC_TIMESTAMPTZ,
                vec![lit_text(unit), col(0, "v")],
            );
            let got = eval_with(&expr, &batch, &session).unwrap();
            assert_eq!(
                ts_micros(&got),
                *expected,
                "date_trunc('{unit}', {input}) under TimeZone={zone}"
            );
        }
    }

    /// The result of `date_trunc` on a `timestamptz` must stay a
    /// `timestamptz`. Arrow encodes that as the timestamp's zone marker, and
    /// dropping it would silently demote the value to `timestamp without
    /// time zone` — a type error the planner would not catch because the
    /// physical width is identical.
    #[test]
    fn date_trunc_timestamptz_keeps_the_timestamptz_marker() {
        let batch = tstz_batch(1_710_052_200_000_000);
        let expr = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text("day"), col(0, "v")]);
        let got = eval_with(&expr, &batch, &EvalSession::with_time_zone("UTC")).unwrap();
        assert_eq!(
            got.data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    /// The half-hour DST step, spelled out. `Australia/Lord_Howe` moves from
    /// `+11:00` to `+10:30` at `2024-04-06 15:00:00+00`, and this instant is
    /// the first one after it — so the input's own offset (`+10:30`) and the
    /// offset of the truncated local time (`+11:00`) differ by exactly the
    /// 30-minute step.
    ///
    /// `hour` keeps the input's offset, `day` re-derives it. Both numbers
    /// below are the live server's; between them they pin the
    /// [`unit_redetermines_zone`] rule, which is the one thing here that a
    /// plausible implementation gets wrong in a way no UTC test can see.
    #[test]
    fn lord_howe_half_hour_dst_step_distinguishes_hour_from_day() {
        let session = EvalSession::with_time_zone("Australia/Lord_Howe");
        let batch = tstz_batch(1_712_415_600_000_000); // 2024-04-06 15:00:00+00
        let trunc = |unit: &str| {
            let expr = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text(unit), col(0, "v")]);
            ts_micros(&eval_with(&expr, &batch, &session).unwrap())
        };
        // 2024-04-07 01:30:00+11 (= 2024-04-06 14:30:00Z) — local 01:30
        // truncated to 01:00, with the input's own +10:30 re-attached, lands
        // back inside +11:00 and so renders with non-zero minutes despite
        // the unit being `hour`. Re-deriving the offset here would give
        // 14:00Z instead, half an hour out.
        assert_eq!(trunc("hour"), 1_712_413_800_000_000);
        // 2024-04-07 00:00:00+11 (= 2024-04-06 13:00:00Z) — offset
        // re-derived from the truncated local midnight.
        assert_eq!(trunc("day"), 1_712_408_400_000_000);
    }

    /// The same instant under three zones must give three different answers.
    /// If the session context were ignored (or hard-coded to UTC), these
    /// would all be equal — which is exactly the silent wrong answer the
    /// context exists to prevent.
    #[test]
    fn the_session_zone_actually_changes_the_answer() {
        let batch = tstz_batch(1_710_052_200_000_000); // 2024-03-10 06:30:00+00
        let day = |zone: &str| {
            let expr = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text("day"), col(0, "v")]);
            ts_micros(&eval_with(&expr, &batch, &EvalSession::with_time_zone(zone)).unwrap())
        };
        let utc = day("UTC");
        let ny = day("America/New_York");
        let lh = day("Australia/Lord_Howe");
        assert_ne!(utc, ny);
        assert_ne!(utc, lh);
        assert_ne!(ny, lh);
    }

    /// `date_part(text, timestamptz)` (oid 1171) in the same three zones.
    /// Includes `timezone`/`timezone_hour`/`timezone_minute`, whose answers
    /// are the session offset itself — `37800`/`10`/`30` in Lord Howe, the
    /// case a zone-offset-in-whole-hours implementation gets wrong.
    #[test]
    fn date_part_timestamptz_matches_postgres_in_every_session_zone() {
        const CASES: &[(&str, i64, &str, f64)] = &[
        ("UTC", 1710052200000000, "epoch", 1710052200.0),
        ("UTC", 1710052200000000, "year", 2024.0),
        ("UTC", 1710052200000000, "isoyear", 2024.0),
        ("UTC", 1710052200000000, "quarter", 1.0),
        ("UTC", 1710052200000000, "month", 3.0),
        ("UTC", 1710052200000000, "week", 10.0),
        ("UTC", 1710052200000000, "day", 10.0),
        ("UTC", 1710052200000000, "doy", 70.0),
        ("UTC", 1710052200000000, "dow", 0.0),
        ("UTC", 1710052200000000, "isodow", 7.0),
        ("UTC", 1710052200000000, "hour", 6.0),
        ("UTC", 1710052200000000, "minute", 30.0),
        ("UTC", 1710052200000000, "second", 0.0),
        ("UTC", 1710052200000000, "milliseconds", 0.0),
        ("UTC", 1710052200000000, "microseconds", 0.0),
        ("UTC", 1710052200000000, "julian", 2460380.2708333335),
        ("UTC", 1710052200000000, "timezone", 0.0),
        ("UTC", 1710052200000000, "timezone_hour", 0.0),
        ("UTC", 1710052200000000, "timezone_minute", 0.0),
        ("UTC", 1710052200000000, "decade", 202.0),
        ("UTC", 1710052200000000, "century", 21.0),
        ("UTC", 1710052200000000, "millennium", 3.0),
        ("UTC", 1712415600000000, "epoch", 1712415600.0),
        ("UTC", 1712415600000000, "year", 2024.0),
        ("UTC", 1712415600000000, "isoyear", 2024.0),
        ("UTC", 1712415600000000, "quarter", 2.0),
        ("UTC", 1712415600000000, "month", 4.0),
        ("UTC", 1712415600000000, "week", 14.0),
        ("UTC", 1712415600000000, "day", 6.0),
        ("UTC", 1712415600000000, "doy", 97.0),
        ("UTC", 1712415600000000, "dow", 6.0),
        ("UTC", 1712415600000000, "isodow", 6.0),
        ("UTC", 1712415600000000, "hour", 15.0),
        ("UTC", 1712415600000000, "minute", 0.0),
        ("UTC", 1712415600000000, "second", 0.0),
        ("UTC", 1712415600000000, "milliseconds", 0.0),
        ("UTC", 1712415600000000, "microseconds", 0.0),
        ("UTC", 1712415600000000, "julian", 2460407.625),
        ("UTC", 1712415600000000, "timezone", 0.0),
        ("UTC", 1712415600000000, "timezone_hour", 0.0),
        ("UTC", 1712415600000000, "timezone_minute", 0.0),
        ("UTC", 1712415600000000, "decade", 202.0),
        ("UTC", 1712415600000000, "century", 21.0),
        ("UTC", 1712415600000000, "millennium", 3.0),
        ("UTC", 0, "epoch", 0.0),
        ("UTC", 0, "year", 1970.0),
        ("UTC", 0, "isoyear", 1970.0),
        ("UTC", 0, "quarter", 1.0),
        ("UTC", 0, "month", 1.0),
        ("UTC", 0, "week", 1.0),
        ("UTC", 0, "day", 1.0),
        ("UTC", 0, "doy", 1.0),
        ("UTC", 0, "dow", 4.0),
        ("UTC", 0, "isodow", 4.0),
        ("UTC", 0, "hour", 0.0),
        ("UTC", 0, "minute", 0.0),
        ("UTC", 0, "second", 0.0),
        ("UTC", 0, "milliseconds", 0.0),
        ("UTC", 0, "microseconds", 0.0),
        ("UTC", 0, "julian", 2440588.0),
        ("UTC", 0, "timezone", 0.0),
        ("UTC", 0, "timezone_hour", 0.0),
        ("UTC", 0, "timezone_minute", 0.0),
        ("UTC", 0, "decade", 197.0),
        ("UTC", 0, "century", 20.0),
        ("UTC", 0, "millennium", 2.0),
        ("America/New_York", 1710052200000000, "epoch", 1710052200.0),
        ("America/New_York", 1710052200000000, "year", 2024.0),
        ("America/New_York", 1710052200000000, "isoyear", 2024.0),
        ("America/New_York", 1710052200000000, "quarter", 1.0),
        ("America/New_York", 1710052200000000, "month", 3.0),
        ("America/New_York", 1710052200000000, "week", 10.0),
        ("America/New_York", 1710052200000000, "day", 10.0),
        ("America/New_York", 1710052200000000, "doy", 70.0),
        ("America/New_York", 1710052200000000, "dow", 0.0),
        ("America/New_York", 1710052200000000, "isodow", 7.0),
        ("America/New_York", 1710052200000000, "hour", 1.0),
        ("America/New_York", 1710052200000000, "minute", 30.0),
        ("America/New_York", 1710052200000000, "second", 0.0),
        ("America/New_York", 1710052200000000, "milliseconds", 0.0),
        ("America/New_York", 1710052200000000, "microseconds", 0.0),
        ("America/New_York", 1710052200000000, "julian", 2460380.0625),
        ("America/New_York", 1710052200000000, "timezone", -18000.0),
        ("America/New_York", 1710052200000000, "timezone_hour", -5.0),
        ("America/New_York", 1710052200000000, "timezone_minute", 0.0),
        ("America/New_York", 1710052200000000, "decade", 202.0),
        ("America/New_York", 1710052200000000, "century", 21.0),
        ("America/New_York", 1710052200000000, "millennium", 3.0),
        ("America/New_York", 1712415600000000, "epoch", 1712415600.0),
        ("America/New_York", 1712415600000000, "year", 2024.0),
        ("America/New_York", 1712415600000000, "isoyear", 2024.0),
        ("America/New_York", 1712415600000000, "quarter", 2.0),
        ("America/New_York", 1712415600000000, "month", 4.0),
        ("America/New_York", 1712415600000000, "week", 14.0),
        ("America/New_York", 1712415600000000, "day", 6.0),
        ("America/New_York", 1712415600000000, "doy", 97.0),
        ("America/New_York", 1712415600000000, "dow", 6.0),
        ("America/New_York", 1712415600000000, "isodow", 6.0),
        ("America/New_York", 1712415600000000, "hour", 11.0),
        ("America/New_York", 1712415600000000, "minute", 0.0),
        ("America/New_York", 1712415600000000, "second", 0.0),
        ("America/New_York", 1712415600000000, "milliseconds", 0.0),
        ("America/New_York", 1712415600000000, "microseconds", 0.0),
        ("America/New_York", 1712415600000000, "julian", 2460407.4583333335),
        ("America/New_York", 1712415600000000, "timezone", -14400.0),
        ("America/New_York", 1712415600000000, "timezone_hour", -4.0),
        ("America/New_York", 1712415600000000, "timezone_minute", 0.0),
        ("America/New_York", 1712415600000000, "decade", 202.0),
        ("America/New_York", 1712415600000000, "century", 21.0),
        ("America/New_York", 1712415600000000, "millennium", 3.0),
        ("America/New_York", 0, "epoch", 0.0),
        ("America/New_York", 0, "year", 1969.0),
        ("America/New_York", 0, "isoyear", 1970.0),
        ("America/New_York", 0, "quarter", 4.0),
        ("America/New_York", 0, "month", 12.0),
        ("America/New_York", 0, "week", 1.0),
        ("America/New_York", 0, "day", 31.0),
        ("America/New_York", 0, "doy", 365.0),
        ("America/New_York", 0, "dow", 3.0),
        ("America/New_York", 0, "isodow", 3.0),
        ("America/New_York", 0, "hour", 19.0),
        ("America/New_York", 0, "minute", 0.0),
        ("America/New_York", 0, "second", 0.0),
        ("America/New_York", 0, "milliseconds", 0.0),
        ("America/New_York", 0, "microseconds", 0.0),
        ("America/New_York", 0, "julian", 2440587.7916666665),
        ("America/New_York", 0, "timezone", -18000.0),
        ("America/New_York", 0, "timezone_hour", -5.0),
        ("America/New_York", 0, "timezone_minute", 0.0),
        ("America/New_York", 0, "decade", 196.0),
        ("America/New_York", 0, "century", 20.0),
        ("America/New_York", 0, "millennium", 2.0),
        ("Australia/Lord_Howe", 1710052200000000, "epoch", 1710052200.0),
        ("Australia/Lord_Howe", 1710052200000000, "year", 2024.0),
        ("Australia/Lord_Howe", 1710052200000000, "isoyear", 2024.0),
        ("Australia/Lord_Howe", 1710052200000000, "quarter", 1.0),
        ("Australia/Lord_Howe", 1710052200000000, "month", 3.0),
        ("Australia/Lord_Howe", 1710052200000000, "week", 10.0),
        ("Australia/Lord_Howe", 1710052200000000, "day", 10.0),
        ("Australia/Lord_Howe", 1710052200000000, "doy", 70.0),
        ("Australia/Lord_Howe", 1710052200000000, "dow", 0.0),
        ("Australia/Lord_Howe", 1710052200000000, "isodow", 7.0),
        ("Australia/Lord_Howe", 1710052200000000, "hour", 17.0),
        ("Australia/Lord_Howe", 1710052200000000, "minute", 30.0),
        ("Australia/Lord_Howe", 1710052200000000, "second", 0.0),
        ("Australia/Lord_Howe", 1710052200000000, "milliseconds", 0.0),
        ("Australia/Lord_Howe", 1710052200000000, "microseconds", 0.0),
        ("Australia/Lord_Howe", 1710052200000000, "julian", 2460380.7291666665),
        ("Australia/Lord_Howe", 1710052200000000, "timezone", 39600.0),
        ("Australia/Lord_Howe", 1710052200000000, "timezone_hour", 11.0),
        ("Australia/Lord_Howe", 1710052200000000, "timezone_minute", 0.0),
        ("Australia/Lord_Howe", 1710052200000000, "decade", 202.0),
        ("Australia/Lord_Howe", 1710052200000000, "century", 21.0),
        ("Australia/Lord_Howe", 1710052200000000, "millennium", 3.0),
        ("Australia/Lord_Howe", 1712415600000000, "epoch", 1712415600.0),
        ("Australia/Lord_Howe", 1712415600000000, "year", 2024.0),
        ("Australia/Lord_Howe", 1712415600000000, "isoyear", 2024.0),
        ("Australia/Lord_Howe", 1712415600000000, "quarter", 2.0),
        ("Australia/Lord_Howe", 1712415600000000, "month", 4.0),
        ("Australia/Lord_Howe", 1712415600000000, "week", 14.0),
        ("Australia/Lord_Howe", 1712415600000000, "day", 7.0),
        ("Australia/Lord_Howe", 1712415600000000, "doy", 98.0),
        ("Australia/Lord_Howe", 1712415600000000, "dow", 0.0),
        ("Australia/Lord_Howe", 1712415600000000, "isodow", 7.0),
        ("Australia/Lord_Howe", 1712415600000000, "hour", 1.0),
        ("Australia/Lord_Howe", 1712415600000000, "minute", 30.0),
        ("Australia/Lord_Howe", 1712415600000000, "second", 0.0),
        ("Australia/Lord_Howe", 1712415600000000, "milliseconds", 0.0),
        ("Australia/Lord_Howe", 1712415600000000, "microseconds", 0.0),
        ("Australia/Lord_Howe", 1712415600000000, "julian", 2460408.0625),
        ("Australia/Lord_Howe", 1712415600000000, "timezone", 37800.0),
        ("Australia/Lord_Howe", 1712415600000000, "timezone_hour", 10.0),
        ("Australia/Lord_Howe", 1712415600000000, "timezone_minute", 30.0),
        ("Australia/Lord_Howe", 1712415600000000, "decade", 202.0),
        ("Australia/Lord_Howe", 1712415600000000, "century", 21.0),
        ("Australia/Lord_Howe", 1712415600000000, "millennium", 3.0),
        ("Australia/Lord_Howe", 0, "epoch", 0.0),
        ("Australia/Lord_Howe", 0, "year", 1970.0),
        ("Australia/Lord_Howe", 0, "isoyear", 1970.0),
        ("Australia/Lord_Howe", 0, "quarter", 1.0),
        ("Australia/Lord_Howe", 0, "month", 1.0),
        ("Australia/Lord_Howe", 0, "week", 1.0),
        ("Australia/Lord_Howe", 0, "day", 1.0),
        ("Australia/Lord_Howe", 0, "doy", 1.0),
        ("Australia/Lord_Howe", 0, "dow", 4.0),
        ("Australia/Lord_Howe", 0, "isodow", 4.0),
        ("Australia/Lord_Howe", 0, "hour", 10.0),
        ("Australia/Lord_Howe", 0, "minute", 0.0),
        ("Australia/Lord_Howe", 0, "second", 0.0),
        ("Australia/Lord_Howe", 0, "milliseconds", 0.0),
        ("Australia/Lord_Howe", 0, "microseconds", 0.0),
        ("Australia/Lord_Howe", 0, "julian", 2440588.4166666665),
        ("Australia/Lord_Howe", 0, "timezone", 36000.0),
        ("Australia/Lord_Howe", 0, "timezone_hour", 10.0),
        ("Australia/Lord_Howe", 0, "timezone_minute", 0.0),
        ("Australia/Lord_Howe", 0, "decade", 197.0),
        ("Australia/Lord_Howe", 0, "century", 20.0),
        ("Australia/Lord_Howe", 0, "millennium", 2.0),
        ];
        for (zone, input, field, expected) in CASES {
            let session = EvalSession::with_time_zone(*zone);
            let batch = tstz_batch(*input);
            let expr = sf(OID_DATE_PART_TIMESTAMPTZ, vec![lit_text(field), col(0, "v")]);
            let got = f64_at0(&eval_with(&expr, &batch, &session).unwrap());
            assert!(
                (got - *expected).abs() <= expected.abs() * 1e-12 + 1e-9,
                "date_part('{field}', {input}) under TimeZone={zone}: \
                 basin {got}, postgres {expected}"
            );
        }
    }

    /// `date_trunc(text, timestamp)` (oid 2020) — no session involvement.
    /// `0001-01-01` is in the battery because it is where Postgres's
    /// decade/century/millennium arithmetic crosses into BC: truncating year
    /// 1 to a decade gives astronomical year 0, which Postgres prints as
    /// `0001-01-01 BC`.
    #[test]
    fn date_trunc_timestamp_matches_postgres() {
        const CASES: &[(i64, &str, i64)] = &[
        (1710074096789012, "microseconds", 1710074096789012),
        (946684799999999, "microseconds", 946684799999999),
        (-62135596800000000, "microseconds", -62135596800000000),
        (1710074096789012, "milliseconds", 1710074096789000),
        (946684799999999, "milliseconds", 946684799999000),
        (-62135596800000000, "milliseconds", -62135596800000000),
        (1710074096789012, "second", 1710074096000000),
        (946684799999999, "second", 946684799000000),
        (-62135596800000000, "second", -62135596800000000),
        (1710074096789012, "minute", 1710074040000000),
        (946684799999999, "minute", 946684740000000),
        (-62135596800000000, "minute", -62135596800000000),
        (1710074096789012, "hour", 1710072000000000),
        (946684799999999, "hour", 946681200000000),
        (-62135596800000000, "hour", -62135596800000000),
        (1710074096789012, "day", 1710028800000000),
        (946684799999999, "day", 946598400000000),
        (-62135596800000000, "day", -62135596800000000),
        (1710074096789012, "week", 1709510400000000),
        (946684799999999, "week", 946252800000000),
        (-62135596800000000, "week", -62135596800000000),
        (1710074096789012, "month", 1709251200000000),
        (946684799999999, "month", 944006400000000),
        (-62135596800000000, "month", -62135596800000000),
        (1710074096789012, "quarter", 1704067200000000),
        (946684799999999, "quarter", 938736000000000),
        (-62135596800000000, "quarter", -62135596800000000),
        (1710074096789012, "year", 1704067200000000),
        (946684799999999, "year", 915148800000000),
        (-62135596800000000, "year", -62135596800000000),
        (1710074096789012, "decade", 1577836800000000),
        (946684799999999, "decade", 631152000000000),
        (-62135596800000000, "decade", -62167219200000000),
        (1710074096789012, "century", 978307200000000),
        (946684799999999, "century", -2177452800000000),
        (-62135596800000000, "century", -62135596800000000),
        (1710074096789012, "millennium", 978307200000000),
        (946684799999999, "millennium", -30578688000000000),
        (-62135596800000000, "millennium", -62135596800000000),
        ];
        for (input, unit, expected) in CASES {
            let batch = plain_ts_batch(*input);
            let expr = sf(OID_DATE_TRUNC_TIMESTAMP, vec![lit_text(unit), col(0, "v")]);
            let got = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap();
            assert_eq!(ts_micros(&got), *expected, "date_trunc('{unit}', {input})");
        }
    }

    /// `date_part(text, timestamp)` (oid 2021).
    #[test]
    fn date_part_timestamp_matches_postgres() {
        const CASES: &[(i64, &str, f64)] = &[
        (1710074096789012, "epoch", 1710074096.789012),
        (946684799999999, "epoch", 946684799.999999),
        (1710074096789012, "year", 2024.0),
        (946684799999999, "year", 1999.0),
        (1710074096789012, "isoyear", 2024.0),
        (946684799999999, "isoyear", 1999.0),
        (1710074096789012, "quarter", 1.0),
        (946684799999999, "quarter", 4.0),
        (1710074096789012, "month", 3.0),
        (946684799999999, "month", 12.0),
        (1710074096789012, "week", 10.0),
        (946684799999999, "week", 52.0),
        (1710074096789012, "day", 10.0),
        (946684799999999, "day", 31.0),
        (1710074096789012, "doy", 70.0),
        (946684799999999, "doy", 365.0),
        (1710074096789012, "dow", 0.0),
        (946684799999999, "dow", 5.0),
        (1710074096789012, "isodow", 7.0),
        (946684799999999, "isodow", 5.0),
        (1710074096789012, "hour", 12.0),
        (946684799999999, "hour", 23.0),
        (1710074096789012, "minute", 34.0),
        (946684799999999, "minute", 59.0),
        (1710074096789012, "second", 56.789012),
        (946684799999999, "second", 59.999999),
        (1710074096789012, "milliseconds", 56789.012),
        (946684799999999, "milliseconds", 59999.999),
        (1710074096789012, "microseconds", 56789012.0),
        (946684799999999, "microseconds", 59999999.0),
        (1710074096789012, "julian", 2460380.5242683915),
        (946684799999999, "julian", 2451545.0),
        (1710074096789012, "decade", 202.0),
        (946684799999999, "decade", 199.0),
        (1710074096789012, "century", 21.0),
        (946684799999999, "century", 20.0),
        (1710074096789012, "millennium", 3.0),
        (946684799999999, "millennium", 2.0),
        ];
        for (input, field, expected) in CASES {
            let batch = plain_ts_batch(*input);
            let expr = sf(OID_DATE_PART_TIMESTAMP, vec![lit_text(field), col(0, "v")]);
            let got = f64_at0(&eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap());
            assert!(
                (got - *expected).abs() <= expected.abs() * 1e-12 + 1e-9,
                "date_part('{field}', {input}): basin {got}, postgres {expected}"
            );
        }
    }

    /// `date_part(text, date)` (oid 1384). Postgres implements this by
    /// casting the `date` up to a `timestamp`, so `julian` comes back whole
    /// (a date has no time of day to contribute a fraction).
    #[test]
    fn date_part_date_matches_postgres() {
        const CASES: &[(i32, &str, f64)] = &[
        (19792, "epoch", 1710028800.0),
        (19792, "year", 2024.0),
        (19792, "quarter", 1.0),
        (19792, "month", 3.0),
        (19792, "week", 10.0),
        (19792, "day", 10.0),
        (19792, "doy", 70.0),
        (19792, "dow", 0.0),
        (19792, "isodow", 7.0),
        (19792, "julian", 2460380.0),
        ];
        for (days, field, expected) in CASES {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Date32, true)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Date32Array::from(vec![Some(*days)])) as ArrayRef],
            )
            .unwrap();
            let expr = sf(OID_DATE_PART_DATE, vec![lit_text(field), col(0, "v")]);
            let got = f64_at0(&eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap());
            assert!(
                (got - *expected).abs() <= expected.abs() * 1e-12 + 1e-9,
                "date_part('{field}', date {days}): basin {got}, postgres {expected}"
            );
        }
    }

    /// `date_part(text, interval)` (oid 1172). Every expectation below is the
    /// answer a live PostgreSQL 18.2 gave with `extra_float_digits = 3`, and
    /// the query that produced it is quoted at each block.
    ///
    /// The interval is `1 year 2 mons 3 days 04:05:06.789012` — months and
    /// days and time all non-zero, so a field that reached across components
    /// would be caught.
    #[test]
    fn date_part_interval_matches_postgres() {
        // select date_part(u, interval '1 year 2 mons 3 days 4:05:06.789012')
        const MONTHS: i32 = 14;
        const DAYS: i32 = 3;
        const NANOS: i64 = 14_706_789_012_000;
        const CASES: &[(&str, f64)] = &[
            ("year", 1.0),
            ("month", 2.0),
            ("decade", 0.0),
            ("century", 0.0),
            ("millennium", 0.0),
            ("quarter", 1.0),
            // 0, NOT 30: an interval's days are its own component, and the
            // month never contributes to them.
            ("day", 3.0),
            ("week", 0.0),
            ("hour", 4.0),
            ("minute", 5.0),
            ("second", 6.789012),
            ("milliseconds", 6789.012),
            ("microseconds", 6789012.0),
            // 31557600 (365.25-day year) + 5184000 (two 30-day months)
            //   + 259200 (3 days) + 14706.789012
            ("epoch", 37015506.789012),
        ];
        for (unit, expected) in CASES {
            let got = date_part_iv(MONTHS, DAYS, NANOS, unit);
            assert!(
                (got - *expected).abs() <= expected.abs() * 1e-12 + 1e-9,
                "date_part('{unit}', interval '1 year 2 mons 3 days 4:05:06.789012'): \
                 basin {got}, postgres {expected}"
            );
        }
    }

    /// The cases the timestamp version's rules would get wrong: mixed signs,
    /// negative month counts, an hour count past 24, and the year-scale
    /// units. Each `(months, days, nanos, unit) -> value` row is a live
    /// PostgreSQL 18.2 answer; the interval each row stands for is named.
    #[test]
    fn date_part_interval_signs_and_magnitudes_match_postgres() {
        const CASES: &[(i32, i32, i64, &str, f64)] = &[
            // interval '1 day -2 hours' — both signs kept, no implicit
            // justify_hours: day is 1 while hour is -2, and epoch is
            // 86400 - 7200.
            (0, 1, -7_200_000_000_000, "day", 1.0),
            (0, 1, -7_200_000_000_000, "hour", -2.0),
            (0, 1, -7_200_000_000_000, "epoch", 79200.0),
            // interval '-13 months' — truncating division, not flooring.
            (-13, 0, 0, "year", -1.0),
            (-13, 0, 0, "month", -1.0),
            (-13, 0, 0, "epoch", -34149600.0),
            // interval '-1 mons' — quarter is -1, not the +1 that a plain
            // `month / 3 + 1` produces.
            (-1, 0, 0, "quarter", -1.0),
            (-1, 0, 0, "epoch", -2592000.0),
            // interval '0' — quarter takes the positive branch at zero.
            (0, 0, 0, "quarter", 1.0),
            (0, 0, 0, "epoch", 0.0),
            // The whole-year negatives, where `months % 12` is zero and only
            // the sign of `months` itself distinguishes them from `interval
            // '0'`. Days and time never contribute to `quarter`.
            (-24, 0, 0, "quarter", -1.0),
            (-24, 5, 10_800_000_000_000, "quarter", -1.0), // '-2 years +5 days 03:00:00'
            (-12000, 0, 0, "quarter", -1.0),               // '-1000 years'
            (0, -5, 0, "quarter", 1.0),                    // '-5 days'
            (24, -5, 0, "quarter", 1.0),                   // '2 years -5 days'
            (-12, 11, 0, "quarter", -1.0),                 // '-1 years +11 days'
            // interval '100000:00:00' — hour is not reduced mod 24.
            (0, 0, 360_000_000_000_000_000, "hour", 100000.0),
            (0, 0, 360_000_000_000_000_000, "day", 0.0),
            (0, 0, 360_000_000_000_000_000, "epoch", 360000000.0),
            // interval '-20 days' — week truncates toward zero.
            (0, -20, 0, "week", -2.0),
            (0, -20, 0, "day", -20.0),
            (0, -20, 0, "epoch", -1728000.0),
            // interval '1999 years 11 mons' — decade/century/millennium are
            // plain division on the year count, with no off-by-one.
            (23999, 0, 0, "year", 1999.0),
            (23999, 0, 0, "month", 11.0),
            (23999, 0, 0, "decade", 199.0),
            (23999, 0, 0, "century", 19.0),
            (23999, 0, 0, "millennium", 1.0),
            (23999, 0, 0, "quarter", 4.0),
            (23999, 0, 0, "epoch", 63112154400.0),
            // interval '2500 years'
            (30000, 0, 0, "millennium", 2.0),
            (30000, 0, 0, "epoch", 78894000000.0),
            // interval '-00:00:00.5' — the sub-second fields carry the sign.
            (0, 0, -500_000_000, "second", -0.5),
            (0, 0, -500_000_000, "milliseconds", -500.0),
            (0, 0, -500_000_000, "microseconds", -500000.0),
            (0, 0, -500_000_000, "epoch", -0.5),
            // interval '-04:05:06.789012'
            (0, 0, -14_706_789_012_000, "hour", -4.0),
            (0, 0, -14_706_789_012_000, "minute", -5.0),
            (0, 0, -14_706_789_012_000, "second", -6.789012),
            (0, 0, -14_706_789_012_000, "epoch", -14706.789012),
            // interval '90 minutes', normalised by Postgres to 01:30:00 —
            // minute is the count inside the hour.
            (0, 0, 5_400_000_000_000, "minute", 30.0),
            (0, 0, 5_400_000_000_000, "hour", 1.0),
            // interval '20 days' / '7 days'
            (0, 20, 0, "week", 2.0),
            (0, 7, 0, "week", 1.0),
        ];
        for (m, d, n, unit, expected) in CASES {
            let got = date_part_iv(*m, *d, *n, unit);
            assert!(
                (got - *expected).abs() <= expected.abs() * 1e-12 + 1e-9,
                "date_part('{unit}', interval({m}, {d}, {n})): basin {got}, postgres {expected}"
            );
        }
    }

    /// The quarter sign rule in full, swept over `make_interval(months => n)`
    /// for n in -26..=26 on a live PostgreSQL 18.2. This is the field that a
    /// reasonable-looking `(month % 12) / 3 + 1` gets wrong for every
    /// negative interval.
    #[test]
    fn date_part_interval_quarter_sweep_matches_postgres() {
        // select n, date_part('quarter', make_interval(months => n))
        //   from generate_series(-26, 26) n;
        const EXPECTED: &[f64] = &[
            -1.0, -1.0, -1.0, // -26, -25, -24
            -4.0, -4.0, -4.0, -3.0, -3.0, -3.0, -2.0, -2.0, -2.0, -1.0, -1.0, // -23 ..= -13
            -1.0, // -12, where `months % 12` is 0 and only `months`'s sign says -1
            -4.0, -4.0, -4.0, -3.0, -3.0, -3.0, -2.0, -2.0, -2.0, -1.0, -1.0, // -11 ..= -1
            1.0,  // 0
            1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, // 1 ..= 11
            1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, // 12 ..= 23
            1.0, 1.0, 1.0, // 24, 25, 26
        ];
        assert_eq!(EXPECTED.len(), 53);
        for (idx, expected) in EXPECTED.iter().enumerate() {
            let months = idx as i32 - 26;
            let got = date_part_iv(months, 0, 0, "quarter");
            assert_eq!(
                got, *expected,
                "date_part('quarter', make_interval(months => {months}))"
            );
        }
    }

    /// The units Postgres refuses for an interval, with its two different
    /// messages reproduced verbatim.
    ///
    /// `week` is deliberately absent from the refusal list and present in the
    /// success tests above: `date_trunc('week', interval)` is an error while
    /// `date_part('week', interval)` is `days / 7`. The two functions
    /// genuinely disagree on this one unit, measured both ways.
    #[test]
    fn date_part_interval_refuses_the_units_postgres_refuses() {
        // select date_part(u, interval '1 day') for each u, on 18.2
        const NOT_SUPPORTED: &[&str] = &[
            "dow",
            "doy",
            "isodow",
            "isoyear",
            "julian",
            "jd",
            "timezone",
            "timezone_hour",
            "timezone_minute",
            "timezone_h",
            "timezone_m",
        ];
        for unit in NOT_SUPPORTED {
            let err = date_part_iv_err(0, 1, 0, unit);
            assert_eq!(
                err,
                format!("type mismatch: unit \"{unit}\" not supported for type interval"),
                "date_part('{unit}', interval)"
            );
        }
        // Unknown units are a *different* message, and the misspelt
        // `millenium` is one of them even though `millennium` resolves.
        for unit in ["fortnight", "millenium", "qtrs"] {
            let err = date_part_iv_err(0, 1, 0, unit);
            assert_eq!(
                err,
                format!("type mismatch: unit \"{unit}\" not recognized for type interval"),
                "date_part('{unit}', interval)"
            );
        }
    }

    /// One interval row through `date_part(text, interval)`, returning the
    /// `float8` answer.
    fn date_part_iv(months: i32, days: i32, nanos: i64, unit: &str) -> f64 {
        f64_at0(&date_part_iv_result(months, days, nanos, unit).unwrap())
    }

    /// The same call's error string.
    fn date_part_iv_err(months: i32, days: i32, nanos: i64, unit: &str) -> String {
        date_part_iv_result(months, days, nanos, unit)
            .unwrap_err()
            .to_string()
    }

    fn date_part_iv_result(
        months: i32,
        days: i32,
        nanos: i64,
        unit: &str,
    ) -> Result<ArrayRef, ExecError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
                IntervalMonthDayNano::new(months, days, nanos),
            )])) as ArrayRef],
        )
        .unwrap();
        let expr = sf(OID_DATE_PART_INTERVAL, vec![lit_text(unit), col(0, "v")]);
        eval_with(&expr, &batch, &EvalSession::DEFAULT)
    }

    /// `date_trunc(text, interval)` (oid 1218). Session-independent, and
    /// deliberately *not* the same arithmetic as the timestamp version:
    /// `century` on `interval '137 years 5 mons'` is `100 years`, not the
    /// `101 years` Postgres's off-by-one century rule would give a date.
    /// `week` is not supported for an interval at all.
    #[test]
    fn date_trunc_interval_matches_postgres() {
        // interval '137 years 5 mons 17 days 04:05:06.789012'
        const MONTHS: i32 = 137 * 12 + 5;
        const DAYS: i32 = 17;
        const NANOS: i64 = 14_706_789_012_000;
        const CASES: &[(&str, i32, i32, i64)] = &[
            ("millennium", 0, 0, 0),
            ("century", 1200, 0, 0),
            ("decade", 1560, 0, 0),
            ("year", 1644, 0, 0),
            ("quarter", 1647, 0, 0),
            ("month", MONTHS, 0, 0),
            ("day", MONTHS, DAYS, 0),
            ("hour", MONTHS, DAYS, 14_400_000_000_000),
            ("minute", MONTHS, DAYS, 14_700_000_000_000),
            ("second", MONTHS, DAYS, 14_706_000_000_000),
            ("milliseconds", MONTHS, DAYS, 14_706_789_000_000),
            ("microseconds", MONTHS, DAYS, NANOS),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            true,
        )]));
        for (unit, m, d, n) in CASES {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
                    IntervalMonthDayNano::new(MONTHS, DAYS, NANOS),
                )])) as ArrayRef],
            )
            .unwrap();
            let expr = sf(OID_DATE_TRUNC_INTERVAL, vec![lit_text(unit), col(0, "v")]);
            let got = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap();
            let iv = got
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .expect("date_trunc on an interval must produce an interval")
                .value(0);
            assert_eq!(
                (iv.months, iv.days, iv.nanoseconds),
                (*m, *d, *n),
                "date_trunc('{unit}', interval)"
            );
        }
        // `week` is recognized but not supported for an interval.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
                IntervalMonthDayNano::new(MONTHS, DAYS, NANOS),
            )])) as ArrayRef],
        )
        .unwrap();
        let expr = sf(OID_DATE_TRUNC_INTERVAL, vec![lit_text("week"), col(0, "v")]);
        let err = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap_err();
        assert_eq!(
            err.to_string(),
            "type mismatch: unit \"week\" not supported for type interval"
        );
    }

    /// Postgres's two *different* rejections, both reproduced verbatim. The
    /// distinction matters: a unit can be unknown ("not recognized") or known
    /// but meaningless for the argument type ("not supported"), and a single
    /// catch-all message would agree with Postgres on neither. `' day '` is
    /// in here because Postgres does **not** trim — a `trim()` in the unit
    /// parser would turn this error into a wrong success.
    #[test]
    fn unit_errors_match_postgres_word_for_word() {
        let cases: &[(&str, u32, &str)] = &[
            (
                "fortnight",
                OID_DATE_TRUNC_TIMESTAMP,
                "type mismatch: unit \"fortnight\" not recognized for type timestamp without time zone",
            ),
            (
                " day ",
                OID_DATE_TRUNC_TIMESTAMP,
                "type mismatch: unit \" day \" not recognized for type timestamp without time zone",
            ),
            (
                "",
                OID_DATE_TRUNC_TIMESTAMP,
                "type mismatch: unit \"\" not recognized for type timestamp without time zone",
            ),
            (
                "epoch",
                OID_DATE_TRUNC_TIMESTAMP,
                "type mismatch: unit \"epoch\" not recognized for type timestamp without time zone",
            ),
            (
                "timezone",
                OID_DATE_TRUNC_TIMESTAMP,
                "type mismatch: unit \"timezone\" not supported for type timestamp without time zone",
            ),
            (
                "timezone",
                OID_DATE_PART_TIMESTAMP,
                "type mismatch: unit \"timezone\" not supported for type timestamp without time zone",
            ),
            (
                "fortnight",
                OID_DATE_TRUNC_TIMESTAMPTZ,
                "type mismatch: unit \"fortnight\" not recognized for type timestamp with time zone",
            ),
        ];
        for (unit, oid, expected) in cases {
            let batch = if *oid == OID_DATE_TRUNC_TIMESTAMPTZ {
                tstz_batch(0)
            } else {
                plain_ts_batch(0)
            };
            let expr = sf(*oid, vec![lit_text(unit), col(0, "v")]);
            let err = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap_err();
            assert_eq!(err.to_string(), *expected, "unit {unit:?} on oid {oid}");
        }
    }

    /// `date_trunc` on a `date` argument reports its errors against
    /// `timestamp without time zone`, because that is the type Postgres
    /// actually evaluates after its implicit cast. Reproduced rather than
    /// corrected.
    #[test]
    fn date_part_on_a_date_reports_errors_against_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Date32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![Some(19_792)])) as ArrayRef],
        )
        .unwrap();
        let expr = sf(OID_DATE_PART_DATE, vec![lit_text("timezone"), col(0, "v")]);
        let err = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap_err();
        assert_eq!(
            err.to_string(),
            "type mismatch: unit \"timezone\" not supported for type timestamp without time zone"
        );
    }

    /// NULL in either argument is NULL out, for every overload — Postgres's
    /// strictness, and the thing a per-row loop most easily gets wrong by
    /// evaluating the unit before checking the value.
    #[test]
    fn date_trunc_and_date_part_are_strict() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>])) as ArrayRef],
        )
        .unwrap();
        for oid in [OID_DATE_TRUNC_TIMESTAMP, OID_DATE_PART_TIMESTAMP] {
            let expr = sf(oid, vec![lit_text("day"), col(0, "v")]);
            let got = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap();
            assert!(got.is_null(0), "oid {oid} must be NULL for a NULL value");
        }
        // A NULL *unit* with a non-NULL value is also NULL, not an error —
        // note this means an invalid unit is never reported for a NULL row.
        let batch = plain_ts_batch(0);
        let expr = sf(
            OID_DATE_TRUNC_TIMESTAMP,
            vec![lit_text_null(), col(0, "v")],
        );
        let got = eval_with(&expr, &batch, &EvalSession::DEFAULT).unwrap();
        assert!(got.is_null(0));
    }

    /// An unknown `TimeZone` is Postgres's `invalid_parameter_value`, raised
    /// by the function that needs the zone rather than at session
    /// construction — so a session carrying a bad zone still answers every
    /// query that does not read it.
    #[test]
    fn an_unknown_session_zone_is_reported_not_guessed() {
        let session = EvalSession::with_time_zone("Mars/Olympus_Mons");
        let batch = tstz_batch(0);
        let expr = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text("day"), col(0, "v")]);
        let err = eval_with(&expr, &batch, &session).unwrap_err();
        assert_eq!(
            err.to_string(),
            "type mismatch: invalid value for parameter \"TimeZone\": \"Mars/Olympus_Mons\""
        );
        // …but a session-independent overload is unaffected.
        let batch = plain_ts_batch(0);
        let expr = sf(OID_DATE_TRUNC_TIMESTAMP, vec![lit_text("day"), col(0, "v")]);
        assert!(eval_with(&expr, &batch, &session).is_ok());
    }

    /// The default session is UTC with **no** clock, and the two-argument
    /// [`eval`] uses it. A default that carried a wall clock would make
    /// `now()`答 differ row to row; a default that carried a non-UTC zone
    /// would be a guess.
    #[test]
    fn the_default_session_is_utc_with_no_clock() {
        assert_eq!(EvalSession::DEFAULT.transaction_timestamp(), None);
        assert_eq!(EvalSession::DEFAULT.statement_timestamp(), None);
        assert_eq!(EvalSession::DEFAULT.time_zone_name(), "");
        let batch = tstz_batch(1_710_052_200_000_000);
        let expr = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text("day"), col(0, "v")]);
        assert_eq!(
            ts_micros(&eval(&expr, &batch).unwrap()),
            ts_micros(&eval_with(&expr, &batch, &EvalSession::with_time_zone("UTC")).unwrap())
        );
    }

    /// A session-dependent call nested inside an expression that is not
    /// session-dependent still sees the session. This is what
    /// [`eval_with`]'s full threading buys: a partially-threaded context
    /// would answer this one under the default zone.
    #[test]
    fn the_session_reaches_a_nested_call() {
        let session = EvalSession::with_time_zone("Australia/Lord_Howe");
        let batch = tstz_batch(1_712_415_600_000_000);
        let inner = sf(OID_DATE_TRUNC_TIMESTAMPTZ, vec![lit_text("day"), col(0, "v")]);
        let nested = Expr::Case {
            operand: None,
            whens: vec![(
                Expr::Literal(Datum::Bool(true), PgType::BOOL),
                inner.clone(),
            )],
            else_: None,
        };
        let direct = eval_with(&inner, &batch, &session).unwrap();
        let through_case = eval_with(&nested, &batch, &session).unwrap();
        assert_eq!(ts_micros(&direct), ts_micros(&through_case));
        assert_eq!(ts_micros(&direct), 1_712_408_400_000_000);
    }

    // ── extract / to_char / `^` ────────────────────────────────────────
    //
    // Every expectation below was read off a live PostgreSQL 18.2, the
    // `extract` ones through
    //
    // ```sql
    // SELECT trunc(x * power(10, scale(x))::numeric), scale(x)
    //   FROM (SELECT pg_catalog.extract(<unit>, <value>) AS x) _;
    // ```
    //
    // which reports the `numeric` as the exact (unscaled integer, scale)
    // pair a `Decimal128` stores, rather than as text — the point of these
    // rows is that the scale is data, not formatting.

    fn date_batch(days: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Date32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Date32Array::from(vec![Some(days)]))]).unwrap()
    }

    /// The unscaled value and the scale of a one-row `numeric` result — the
    /// two halves of what Postgres's `numeric` actually carries.
    fn decimal_at0(v: &ArrayRef) -> (i128, i8) {
        let a = v
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("extract must produce numeric, not double precision");
        (a.value(0), a.scale())
    }

    fn text_at0(v: &ArrayRef) -> String {
        downcast_array::<StringArray>(v, "text").unwrap().value(0).to_string()
    }

    /// `extract` on a `timestamp without time zone` (oid 6202), value AND
    /// scale. The scale is the half a `float8`-returning implementation
    /// cannot have: `second` is scale 6 even when the seconds are whole
    /// (`56.000000`), because that is what Postgres's `numeric` carries.
    #[test]
    fn extract_on_a_timestamp_matches_postgres_value_and_scale() {
        const CASES: &[(i64, &str, i128, i8)] = &[
            (1710052200000000, "epoch", 1710052200000000, 6),
            (1710052200000000, "year", 2024, 0),
            (1710052200000000, "isoyear", 2024, 0),
            (1710052200000000, "quarter", 1, 0),
            (1710052200000000, "month", 3, 0),
            (1710052200000000, "week", 10, 0),
            (1710052200000000, "day", 10, 0),
            (1710052200000000, "doy", 70, 0),
            (1710052200000000, "dow", 0, 0),
            (1710052200000000, "isodow", 7, 0),
            (1710052200000000, "hour", 6, 0),
            (1710052200000000, "minute", 30, 0),
            (1710052200000000, "second", 0, 6),
            (1710052200000000, "milliseconds", 0, 3),
            (1710052200000000, "microseconds", 0, 0),
            (1710052200000000, "decade", 202, 0),
            (1710052200000000, "century", 21, 0),
            (1710052200000000, "millennium", 3, 0),
            // A sub-second instant: 2024-02-28 12:30:56.789123.
            (1709123456789123, "epoch", 1709123456789123, 6),
            (1709123456789123, "second", 56789123, 6),
            (1709123456789123, "milliseconds", 56789123, 3),
            (1709123456789123, "microseconds", 56789123, 0),
            (1709123456789123, "minute", 30, 0),
            (1709123456789123, "doy", 59, 0),
            (1709123456789123, "week", 9, 0),
            // One second BEFORE the epoch — the sign case, where a
            // truncating-toward-zero implementation goes wrong.
            (-1000000, "epoch", -1000000, 6),
            (-1000000, "year", 1969, 0),
            (-1000000, "month", 12, 0),
            (-1000000, "day", 31, 0),
            (-1000000, "hour", 23, 0),
            (-1000000, "minute", 59, 0),
            (-1000000, "second", 59000000, 6),
            (-1000000, "milliseconds", 59000000, 3),
            (-1000000, "microseconds", 59000000, 0),
            (-1000000, "decade", 196, 0),
            (-1000000, "century", 20, 0),
            (-1000000, "millennium", 2, 0),
        ];
        for (micros, unit, unscaled, scale) in CASES {
            let batch = plain_ts_batch(*micros);
            let expr = sf(OID_EXTRACT_TIMESTAMP, vec![lit_text(unit), col(0, "v")]);
            let got = eval(&expr, &batch).unwrap();
            assert_eq!(
                decimal_at0(&got),
                (*unscaled, *scale),
                "extract({unit} from timestamp {micros})"
            );
        }
    }

    /// The headline difference from `date_part`, in one assertion: the same
    /// field of the same value, exact as `numeric` and rounded as `float8`.
    /// Live, `extract(second from '2024-02-28 12:30:56.789123')` is
    /// `56.789123` while `date_part('second', ...)` is `56.789123000000004`
    /// — the `float8` cannot represent it, and `56789123 / 10^6` can.
    #[test]
    fn extract_keeps_the_microseconds_date_part_rounds_away() {
        let batch = plain_ts_batch(1_709_123_456_789_123);
        let exact = eval(
            &sf(OID_EXTRACT_TIMESTAMP, vec![lit_text("second"), col(0, "v")]),
            &batch,
        )
        .unwrap();
        assert_eq!(decimal_at0(&exact), (56_789_123, 6));

        let rounded = eval(
            &sf(
                OID_DATE_PART_TIMESTAMP,
                vec![lit_text("second"), col(0, "v")],
            ),
            &batch,
        )
        .unwrap();
        let as_f64 = f64_at0(&rounded);
        assert_ne!(
            as_f64, 56.789_123,
            "date_part's float8 is the imprecise one — if this ever becomes exact, \
             the premise of extract being a separate implementation is gone"
        );
        assert_eq!(as_f64.to_string(), "56.789123000000004");
    }

    /// `extract` on a `date` (oid 6199) is where it parts company with
    /// `date_part(text, date)` (1384) on which units are legal at all:
    /// `date_part` casts to `timestamp` first and answers `0` for `hour`,
    /// `extract_date` has no such cast and raises "not supported". Both
    /// behaviours measured; both asserted here so neither drifts onto the
    /// other.
    #[test]
    fn extract_on_a_date_rejects_the_sub_day_units_date_part_accepts() {
        const CASES: &[(i32, &str, i128)] = &[
            (19792, "year", 2024),
            (19792, "month", 3),
            (19792, "day", 10),
            (19792, "dow", 0),
            (19792, "isodow", 7),
            (19792, "doy", 70),
            (19792, "week", 10),
            (19792, "quarter", 1),
            (19792, "julian", 2460380),
            (19792, "epoch", 1710028800),
            (19792, "decade", 202),
            (19792, "century", 21),
            (19792, "millennium", 3),
            (0, "year", 1970),
            (0, "epoch", 0),
            (0, "julian", 2440588),
            (0, "dow", 4),
            (-1, "year", 1969),
            (-1, "epoch", -86400),
            (-1, "julian", 2440587),
            (-1, "doy", 365),
            (19158, "isoyear", 2022),
            (19158, "week", 24),
            (19158, "epoch", 1655251200),
        ];
        for (days, unit, unscaled) in CASES {
            let batch = date_batch(*days);
            let expr = sf(OID_EXTRACT_DATE, vec![lit_text(unit), col(0, "v")]);
            let got = eval(&expr, &batch).unwrap();
            assert_eq!(
                decimal_at0(&got),
                (*unscaled, 0),
                "extract({unit} from date {days}) — every unit a date accepts is scale 0"
            );
        }

        // `epoch` on a date is whole SECONDS (scale 0), not microseconds:
        // 1710028800, not 1710028800000000. A shared code path with the
        // timestamp overload would get this wrong by six orders of
        // magnitude.
        let epoch = eval(
            &sf(OID_EXTRACT_DATE, vec![lit_text("epoch"), col(0, "v")]),
            &date_batch(19792),
        )
        .unwrap();
        assert_eq!(decimal_at0(&epoch), (1_710_028_800, 0));

        for unit in [
            "hour",
            "minute",
            "second",
            "milliseconds",
            "microseconds",
            "timezone",
            "timezone_hour",
            "timezone_minute",
        ] {
            let batch = date_batch(19792);
            let err = eval(
                &sf(OID_EXTRACT_DATE, vec![lit_text(unit), col(0, "v")]),
                &batch,
            )
            .unwrap_err();
            assert_eq!(
                err,
                ExecError::TypeMismatch(format!("unit \"{unit}\" not supported for type date")),
                "extract({unit} from date) must be refused the way a live server refuses it"
            );

            // The same unit through `date_part`, which DOES accept it —
            // this is the difference, not a coincidence.
            let via_part = eval(
                &sf(OID_DATE_PART_DATE, vec![lit_text(unit), col(0, "v")]),
                &batch,
            );
            if unit.starts_with("timezone") {
                assert!(via_part.is_err(), "no date has a zone under either function");
            } else {
                assert_eq!(
                    f64_at0(&via_part.unwrap()),
                    0.0,
                    "date_part('{unit}', date) is 0 live, because its body casts to timestamp"
                );
            }
        }
    }

    /// `extract` on a `timestamptz` (oid 6203) reads the SESSION zone for
    /// every field but `epoch`, exactly as `date_part` does — including on a
    /// zone whose offset is not a whole number of hours.
    #[test]
    fn extract_on_a_timestamptz_reads_the_session_zone() {
        const CASES: &[(&str, i64, &str, i128, i8)] = &[
            ("UTC", 1710052200000000, "hour", 6, 0),
            ("UTC", 1710052200000000, "epoch", 1710052200000000, 6),
            ("UTC", 1710052200000000, "timezone", 0, 0),
            ("America/New_York", 1710052200000000, "hour", 1, 0),
            ("America/New_York", 1710052200000000, "day", 10, 0),
            ("America/New_York", 1710052200000000, "timezone", -18000, 0),
            ("America/New_York", 1710052200000000, "timezone_hour", -5, 0),
            ("America/New_York", 1710052200000000, "timezone_minute", 0, 0),
            // The epoch is an absolute instant and does NOT move with the
            // session zone — the same number under all three zones.
            ("America/New_York", 1710052200000000, "epoch", 1710052200000000, 6),
            ("Australia/Lord_Howe", 1710052200000000, "epoch", 1710052200000000, 6),
            // April 6 15:00 UTC, after New York's DST start: -4, not -5.
            ("America/New_York", 1712415600000000, "timezone_hour", -4, 0),
            ("America/New_York", 1712415600000000, "hour", 11, 0),
            // Lord Howe's HALF-hour offset, the case an implementation that
            // stores offsets in whole hours gets wrong.
            ("Australia/Lord_Howe", 1709123456789123, "hour", 23, 0),
            ("Australia/Lord_Howe", 1709123456789123, "second", 56789123, 6),
            ("Australia/Lord_Howe", 1709123456789123, "milliseconds", 56789123, 3),
        ];
        for (zone, micros, unit, unscaled, scale) in CASES {
            let session = EvalSession::with_time_zone(*zone);
            let batch = tstz_batch(*micros);
            let expr = sf(OID_EXTRACT_TIMESTAMPTZ, vec![lit_text(unit), col(0, "v")]);
            let got = eval_with(&expr, &batch, &session).unwrap();
            assert_eq!(
                decimal_at0(&got),
                (*unscaled, *scale),
                "extract({unit} from timestamptz {micros}) under TimeZone={zone}"
            );
        }
    }

    /// The six `extract` oids are one function in Postgres, differing only in
    /// argument type — and lowering cannot yet tell this evaluator which one
    /// a bare column meant (`best_effort_type` reports `unknown`, so every
    /// call resolves to the same first-in-table oid). Dispatching on the
    /// argument's Arrow type is what makes that harmless: the answer must be
    /// identical no matter which of the six oids carried the call.
    #[test]
    fn every_extract_oid_dispatches_on_the_argument_type_it_actually_gets() {
        let batch = date_batch(19792);
        for oid_val in [
            OID_EXTRACT_DATE,
            OID_EXTRACT_TIME,
            OID_EXTRACT_TIMETZ,
            OID_EXTRACT_TIMESTAMP,
            OID_EXTRACT_TIMESTAMPTZ,
            OID_EXTRACT_INTERVAL,
        ] {
            let got = eval(&sf(oid_val, vec![lit_text("year"), col(0, "v")]), &batch).unwrap();
            assert_eq!(
                decimal_at0(&got),
                (2024, 0),
                "oid {oid_val} was handed a Date32 and must read it as a date"
            );
        }
    }

    /// `julian` on a `timestamp` is refused rather than answered. Its
    /// Postgres `dscale` is not fixed — measured 20 digits for
    /// `2024-03-15 12:34:56.789123` and 28 for the same date at midnight,
    /// because Postgres computes it as a `numeric` division — so no single
    /// Arrow scale reproduces it. On a `date` there is no division and it IS
    /// answered (see the date test above).
    #[test]
    fn extract_julian_on_a_timestamp_fails_closed_rather_than_picking_a_scale() {
        let batch = plain_ts_batch(1_710_052_200_000_000);
        let err = eval(
            &sf(OID_EXTRACT_TIMESTAMP, vec![lit_text("julian"), col(0, "v")]),
            &batch,
        )
        .unwrap_err();
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("no fixed numeric scale")),
            "expected a refusal naming the reason, got {err:?}"
        );
    }

    /// A NULL value yields NULL, and the array still carries the scale — a
    /// zero-row or all-NULL batch is exactly what `Project`'s schema probe
    /// evaluates, so the scale must not depend on there being data.
    #[test]
    fn extract_of_null_is_null_and_still_carries_its_scale() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]))],
        )
        .unwrap();
        let got = eval(
            &sf(OID_EXTRACT_TIMESTAMP, vec![lit_text("epoch"), col(0, "v")]),
            &batch,
        )
        .unwrap();
        assert!(got.is_null(0));
        assert_eq!(got.data_type(), &DataType::Decimal128(38, 6));

        let empty = RecordBatch::new_empty(schema);
        let probe = eval(
            &sf(OID_EXTRACT_TIMESTAMP, vec![lit_text("epoch"), col(0, "v")]),
            &empty,
        )
        .unwrap();
        assert_eq!(probe.data_type(), &DataType::Decimal128(38, 6));
    }

    /// `to_char(timestamp, text)` (oid 2049). Every expectation read off a
    /// live PostgreSQL 18.2.
    #[test]
    fn to_char_of_a_timestamp_matches_postgres() {
        const CASES: &[(i64, &str, &str)] = &[
            (1710052200000000, "YYYY-MM-DD", "2024-03-10"),
            (1710052200000000, "YYYY-MM-DD HH24:MI:SS", "2024-03-10 06:30:00"),
            (1710052200000000, "DD/MM/YYYY", "10/03/2024"),
            (1710052200000000, "Mon DD, YYYY", "Mar 10, 2024"),
            (1710052200000000, "MONTH", "MARCH    "),
            (1710052200000000, "Month", "March    "),
            (1710052200000000, "month", "march    "),
            (1710052200000000, "MON", "MAR"),
            (1710052200000000, "Mon", "Mar"),
            (1710052200000000, "mon", "mar"),
            (1710052200000000, "DAY", "SUNDAY   "),
            (1710052200000000, "Day", "Sunday   "),
            (1710052200000000, "day", "sunday   "),
            (1710052200000000, "DY", "SUN"),
            (1710052200000000, "Dy", "Sun"),
            (1710052200000000, "dy", "sun"),
            (1710052200000000, "HH12:MI AM", "06:30 AM"),
            (1710052200000000, "HH:MI pm", "06:30 am"),
            (1710052200000000, "Q", "1"),
            (1710052200000000, "WW", "10"),
            (1710052200000000, "IW", "10"),
            (1710052200000000, "ID", "7"),
            (1710052200000000, "D", "1"),
            (1710052200000000, "DDD", "070"),
            (1710052200000000, "YYY", "024"),
            (1710052200000000, "YY", "24"),
            (1710052200000000, "Y", "4"),
            (1710052200000000, "\"Year:\" YYYY", "Year: 2024"),
            (1710052200000000, "YYYY\"y\"MM\"m\"DD\"d\"", "2024y03m10d"),
            (1710052200000000, "YYYY-MM-DD HH24:MI:SS.MS", "2024-03-10 06:30:00.000"),
            (1710052200000000, "YYYY-MM-DD HH24:MI:SS.US", "2024-03-10 06:30:00.000000"),
            // 2024-02-28 12:30:56.789123 — noon, where a 12-hour clock that
            // forgets `12 % 12 == 0` prints `00`.
            (1709123456789123, "HH12:MI AM", "12:30 PM"),
            (1709123456789123, "HH:MI pm", "12:30 pm"),
            (1709123456789123, "YYYY-MM-DD HH24:MI:SS.MS", "2024-02-28 12:30:56.789"),
            (1709123456789123, "YYYY-MM-DD HH24:MI:SS.US", "2024-02-28 12:30:56.789123"),
            (1709123456789123, "DAY", "WEDNESDAY"),
            (1709123456789123, "MONTH", "FEBRUARY "),
            (1709123456789123, "DDD", "059"),
            (1709123456789123, "D", "4"),
            (1709123456789123, "ID", "3"),
            (1709123456789123, "IW", "09"),
            (1709123456789123, "WW", "09"),
            (1712415600000000, "YYYY-MM-DD HH24:MI:SS", "2024-04-06 15:00:00"),
            (1712415600000000, "HH12:MI AM", "03:00 PM"),
            (1712415600000000, "Day", "Saturday "),
            (1712415600000000, "Q", "2"),
            (1712415600000000, "D", "7"),
            (1712415600000000, "ID", "6"),
            (1712415600000000, "Mon DD, YYYY", "Apr 06, 2024"),
        ];
        for (micros, fmt, expected) in CASES {
            let batch = plain_ts_batch(*micros);
            let expr = sf(OID_TO_CHAR_TIMESTAMP, vec![col(0, "v"), lit_text(fmt)]);
            let got = eval(&expr, &batch).unwrap();
            assert_eq!(text_at0(&got), *expected, "to_char({micros}, '{fmt}')");
        }
    }

    /// **There is no `to_char(date, text)` in Postgres.** A `date` argument
    /// reaches oid 2049 by the implicit `date -> timestamp` cast, so this
    /// evaluator has to accept a `Date32` under that oid and read it as
    /// midnight — which is what the inserted cast produces. Confirmed live:
    /// `to_char(DATE '2024-03-05', 'YYYY-MM-DD HH24:MI:SS')` is
    /// `2024-03-05 00:00:00`.
    #[test]
    fn to_char_of_a_date_is_midnight_under_the_timestamp_overload() {
        const CASES: &[(i32, &str, &str)] = &[
            (19792, "YYYY-MM-DD", "2024-03-10"),
            (19792, "YYYY-MM-DD HH24:MI:SS", "2024-03-10 00:00:00"),
            (19792, "HH12:MI AM", "12:00 AM"),
            (19792, "Day", "Sunday   "),
            (0, "YYYY-MM-DD", "1970-01-01"),
            (-1, "YYYY-MM-DD", "1969-12-31"),
        ];
        for (days, fmt, expected) in CASES {
            let batch = date_batch(*days);
            let expr = sf(OID_TO_CHAR_TIMESTAMP, vec![col(0, "v"), lit_text(fmt)]);
            let got = eval(&expr, &batch).unwrap();
            assert_eq!(text_at0(&got), *expected, "to_char(date {days}, '{fmt}')");
        }
    }

    /// `to_char(timestamptz, text)` (1770) renders in the SESSION zone —
    /// which is why both `to_char` oids are `provolatile = 's'`.
    #[test]
    fn to_char_of_a_timestamptz_renders_in_the_session_zone() {
        let batch = tstz_batch(1_710_052_200_000_000);
        let expr = sf(
            OID_TO_CHAR_TIMESTAMPTZ,
            vec![col(0, "v"), lit_text("YYYY-MM-DD HH24:MI")],
        );
        for (zone, expected) in [
            ("UTC", "2024-03-10 06:30"),
            ("America/New_York", "2024-03-10 01:30"),
            ("Australia/Lord_Howe", "2024-03-10 17:30"),
        ] {
            let got = eval_with(&expr, &batch, &EvalSession::with_time_zone(zone)).unwrap();
            assert_eq!(text_at0(&got), expected, "TimeZone={zone}");
        }
    }

    /// An unimplemented template pattern FAILS rather than passing its
    /// letters through, and the reason is that Postgres would not have
    /// passed them through either: `RM` is the Roman-numeral month (`III`
    /// for March, live), `J` the Julian day, `CC` the century, `SSSS` the
    /// seconds past midnight. Emitting `RM` verbatim would be a silently
    /// wrong answer; failing falls back to an engine that implements them.
    ///
    /// The same trap seen from the other side: `to_char(<2024-03-05>,
    /// 'XYZ')` is `X4Z` on a live server, because `Y` is a pattern even
    /// inside nonsense. This evaluator refuses that too rather than
    /// half-matching it.
    #[test]
    fn to_char_refuses_an_unimplemented_template_pattern() {
        let batch = plain_ts_batch(1_710_052_200_000_000);
        for fmt in ["RM", "J", "CC", "SSSS", "TZ", "yyyy", "FMDay", "IYYY", "XYZ"] {
            let expr = sf(OID_TO_CHAR_TIMESTAMP, vec![col(0, "v"), lit_text(fmt)]);
            let err = eval(&expr, &batch).unwrap_err();
            assert!(
                matches!(err, ExecError::Internal(ref m) if m.contains("not implemented")),
                "to_char with '{fmt}' must fail closed, got {err:?}"
            );
        }
        // Non-letters are NOT patterns and pass through, which is what makes
        // the ordinary separator-laden formats above work.
        let punct = sf(
            OID_TO_CHAR_TIMESTAMP,
            vec![col(0, "v"), lit_text("[YYYY] (MM) {DD} <>#@!")],
        );
        assert_eq!(
            text_at0(&eval(&punct, &batch).unwrap()),
            "[2024] (03) {10} <>#@!"
        );
    }

    /// [`DATE_PATTERNS`] is matched longest-first, and that is the ONLY
    /// reason `SSSS` is refused instead of being chewed into `SS` twice (the
    /// bug this test was written after finding: it rendered `0000`). An entry
    /// added out of order would silently reopen it for whichever pattern the
    /// shorter one is a prefix of, so the ordering is pinned.
    #[test]
    fn date_patterns_are_ordered_longest_first_so_no_pattern_shadows_a_longer_one() {
        for pair in DATE_PATTERNS.windows(2) {
            assert!(
                pair[0].0.len() >= pair[1].0.len(),
                "`{}` (len {}) precedes `{}` (len {}) — a shorter pattern before a longer \
                 one can consume the longer one's prefix",
                pair[0].0,
                pair[0].0.len(),
                pair[1].0,
                pair[1].0.len(),
            );
        }
        // And no two entries claim the same spelling.
        for (i, (pattern, _)) in DATE_PATTERNS.iter().enumerate() {
            assert!(
                !DATE_PATTERNS[i + 1..].iter().any(|(p, _)| p == pattern),
                "`{pattern}` appears twice; only the first would ever be reached"
            );
        }
    }

    /// `^` is EXPONENTIATION in Postgres, not the bitwise XOR the same
    /// spelling means in C, Rust, Python and SQLite. `SELECT 2 ^ 10` is
    /// 1024 live; an XOR would answer 8.
    #[test]
    fn caret_is_exponentiation_not_bitwise_xor() {
        let batch = batch_i32("x", vec![Some(2), Some(3), Some(10)]);
        let expr = Expr::Binary {
            op: op(965), // float8 ^ float8 (dpow)
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(2)),
        };
        let got = eval(&expr, &batch).unwrap();
        let f = downcast_array::<Float64Array>(&got, "double precision").unwrap();
        assert_eq!(
            (f.value(0), f.value(1), f.value(2)),
            (4.0, 9.0, 100.0),
            "x ^ 2 is x squared; a bitwise XOR would answer 0, 1 and 8"
        );
        assert_eq!(
            got.data_type(),
            &DataType::Float64,
            "integer operands widen to float8, exactly as Postgres's own \
             resolution does — there is no int ^ int operator"
        );
    }

    /// `^` is LEFT-associative in Postgres, unlike the right associativity
    /// the same spelling has in most languages: `2 ^ 3 ^ 2` is `(2 ^ 3) ^ 2`
    /// = **64** live, not `2 ^ (3 ^ 2)` = 512.
    ///
    /// That association is decided by the grammar, not by this file —
    /// `basin-plan` parses with `pg_query`, which is Postgres's own grammar,
    /// so the left operand of the outer `^` arrives already being `2 ^ 3`.
    /// What is asserted here is that evaluating that shape gives 64, i.e.
    /// that nothing downstream re-associates it.
    #[test]
    fn caret_left_association_evaluates_to_64_not_512() {
        let batch = batch_i32("x", vec![Some(2)]);
        let inner = Expr::Binary {
            op: op(965),
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(3)),
        };
        let outer = Expr::Binary {
            op: op(965),
            lhs: Box::new(inner),
            rhs: Box::new(lit_i32(2)),
        };
        let got = eval(&outer, &batch).unwrap();
        assert_eq!(
            downcast_array::<Float64Array>(&got, "double precision")
                .unwrap()
                .value(0),
            64.0
        );
    }

    /// A `numeric` operand is refused, not routed through `f64`. Postgres
    /// resolves that to a DIFFERENT operator with a different result type —
    /// oid 1038, `numeric ^ numeric` returning `numeric`
    /// (`pg_typeof(2::numeric ^ 2::int4)` is `numeric` live) — and answering
    /// it in double precision would be a wrong answer with the right shape.
    #[test]
    fn caret_refuses_a_numeric_operand_rather_than_answering_in_float8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "n",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let arr = Decimal128Array::from(vec![Some(200_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let expr = Expr::Binary {
            op: op(965),
            lhs: Box::new(col(0, "n")),
            rhs: Box::new(lit_i32(2)),
        };
        let err = eval(&expr, &batch).unwrap_err();
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("numeric_power")),
            "expected a refusal naming the operator it would have to be, got {err:?}"
        );
    }

    /// Postgres's `power`/`^` domain errors apply to `^` too, because both
    /// go through the same `dpow`: `0 ^ -1` is `division_by_zero` and a
    /// negative base with a fractional exponent is out of range. Sharing
    /// [`pg_power_f64`] with `power(float8, float8)` is what guarantees the
    /// operator and the function cannot drift apart.
    #[test]
    fn caret_shares_powers_domain_errors_with_the_power_function() {
        let batch = batch_i32("x", vec![Some(0)]);
        let expr = Expr::Binary {
            op: op(965),
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(-1)),
        };
        let via_operator = eval(&expr, &batch).unwrap_err();
        let via_function = eval(
            &sf(
                OID_POWER_FLOAT8,
                vec![
                    Expr::Literal(Datum::Float64(0.0), PgType::FLOAT8),
                    Expr::Literal(Datum::Float64(-1.0), PgType::FLOAT8),
                ],
            ),
            &batch,
        )
        .unwrap_err();
        assert_eq!(via_operator, via_function);
    }
}
