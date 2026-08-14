//! Numeric functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! # What is here and why these twelve
//!
//! The magnitude-and-rounding family: `abs` at all six of its `pg_proc`
//! overloads, and the six `double precision` shaping functions `round`,
//! `ceil`, `ceiling`, `floor`, `trunc` and `sign`.
//!
//! They were chosen for the same reason `lower` went first (see
//! [`crate::funcs::str_fns`], the Phase 1 template): **they already have
//! tests.** All twelve are in the `crates/basin-exec/tests/function_equivalence.rs`
//! battery, which does not assert what someone decided was correct — it
//! diffs `eval()` against a live PostgreSQL 18 cell by cell over an
//! edge-case battery (`0`, `-0`, ties, subnormals, `MIN`/`MAX`, `±Inf`,
//! `NaN`, `i32::MIN`-class overflow triggers). Ten of the twelve also have
//! unit tests inside `eval.rs` itself. After the port, every one of those
//! runs through the registry instead of the `match`, unchanged — which is
//! the evidence a port is supposed to produce.
//!
//! They were also chosen for being **closed over their helpers**: none of
//! them needs `eval.rs`'s private `float8_unary_checked` / `check_float8_result`
//! / `pg_*_f64` cluster, and none needs the `decimal_round_value` /
//! `decimal_trunc_value` / `pow10` cluster. So this slice can be lifted out
//! whole, and the arms it replaces leave behind helpers that are dead rather
//! than half-used. See the report at the bottom of this doc for exactly
//! which.
//!
//! # What is deliberately NOT here
//!
//! * **`pi()` (oid 1610) cannot be ported to this ABI as it stands.** It is
//!   niladic, and its `match` arm reads `batch.num_rows()` to size its
//!   output:
//!
//!   ```text
//!   OID_PI => Ok(Arc::new(Float64Array::from(vec![
//!       std::f64::consts::PI;
//!       batch.num_rows()
//!   ]))),
//!   ```
//!
//!   [`ScalarFunc::invoke`] receives `&[ArrayRef]` and an [`EvalSession`],
//!   neither of which carries a row count, so a niladic function hosted here
//!   has no way to satisfy invariant 3 (output length equals input length) —
//!   it would have to return a length-1 array into an N-row batch. This is
//!   an ABI gap, not a property of `pi`: every niladic `pg_proc` row
//!   (`random()`, `now()`, `current_database()`) has it. Reported rather
//!   than worked around, because guessing 1 here is exactly the silent
//!   wrong answer the seam exists to prevent.
//!
//! * **The `numeric` shaping overloads** (`round`/`ceil`/`ceiling`/`floor`/
//!   `trunc`/`sign` on `numeric`) share `decimal_round_value`,
//!   `decimal_trunc_value` and `pow10` with each other and with the
//!   two-argument `round(numeric, int)` / `trunc(numeric, int)` forms.
//!   Porting a subset would mean duplicating those helpers across two
//!   modules while both halves stayed live. They are one slice and should
//!   move as one. `abs(numeric)` is the exception and IS here: it shares
//!   nothing with them.
//!
//! # Measured against live PostgreSQL 18.2
//!
//! Every value below was read off `postgres://pc@127.0.0.1:5432/postgres`
//! (`PostgreSQL 18.2 (Homebrew) on aarch64-apple-darwin24.6.0`) this
//! session, not reasoned about. The full `float8` battery, which is what
//! the per-function doc comments cite:
//!
//! ```text
//!  x        | abs      | round | ceil | ceiling | floor | trunc | sign
//! ----------+----------+-------+------+---------+-------+-------+------
//!  0        | 0        | 0     | 0    | 0       | 0     | 0     | 0
//!  -0       | 0        | -0    | -0   | -0      | -0    | -0    | 0
//!  0.5      | 0.5      | 0     | 1    | 1       | 0     | 0     | 1
//!  -0.5     | 0.5      | -0    | -0   | -0      | -1    | -0    | -1
//!  1.5      | 1.5      | 2     | 2    | 2       | 1     | 1     | 1
//!  2.5      | 2.5      | 2     | 3    | 3       | 2     | 2     | 1
//!  -2.5     | 2.5      | -2    | -2   | -2      | -3    | -2    | -1
//!  3.5      | 3.5      | 4     | 4    | 4       | 3     | 3     | 1
//!  -3.7     | 3.7      | -4    | -3   | -3      | -4    | -3    | -1
//!  5e-324   | 5e-324   | 0     | 1    | 1       | 0     | 0     | 1
//!  -5e-324  | 5e-324   | -0    | -0   | -0      | -1    | -0    | -1
//!  Infinity | Infinity | Inf   | Inf  | Inf     | Inf   | Inf   | 1
//!  -Infinity| Infinity | -Inf  | -Inf | -Inf    | -Inf  | -Inf  | -1
//!  NaN      | NaN      | NaN   | NaN  | NaN     | NaN   | NaN   | 0
//! ```
//!
//! Three rows in that table are the ones Rust stdlib semantics could have
//! got wrong, and the reason each implementation below is the function it is
//! rather than the obvious one:
//!
//! 1. **`round` ties.** `round(2.5) = 2` and `round(-2.5) = -2` — half to
//!    **even**, because PostgreSQL's `dround` is libm `rint`. `f64::round`
//!    would answer `3` and `-3`. Only `f64::round_ties_even` matches. Note
//!    this is the OPPOSITE tie direction from `round(numeric)`, which is
//!    half **away from zero** (`round(2.5::numeric) = 3`, measured) — the
//!    two must never share an implementation.
//! 2. **`sign` of zero and NaN.** `sign(0) = 0`, `sign(-0) = 0`,
//!    `sign('NaN') = 0`. `f64::signum` reports the *sign bit*, so it answers
//!    `1` for `+0.0`, `-1` for `-0.0` and `NaN` for `NaN` — wrong on all
//!    three. Hence this module's own `pg_sign_f64`.
//! 3. **The sign of zero results.** `ceil(-0.5)`, `trunc(-0.5)` and
//!    `round(-0.5)` are all `-0`, not `0`. Rust's `f64::ceil`/`trunc`/
//!    `round_ties_even` agree — but a hand-rolled implementation via
//!    `as i64 as f64` would not, and the differential harness compares
//!    floats with a relative epsilon, under which `-0.0 == 0.0`, so it
//!    would never catch the loss.
//!
//! Integer `abs` overflow was checked too: `abs((-32768)::smallint)`,
//! `abs((-2147483648)::int4)` and `abs((-9223372036854775808)::int8)` each
//! raise `ERROR: 22003: <type> out of range` on the server. Basin errors at
//! all three as well — see [`AbsInt2`] for the message/SQLSTATE divergence
//! that remains.

use std::sync::Arc;

use arrow::compute::kernels::arity;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use arrow_array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow_schema::ArrowError;
use basin_pgtype::Oid;

use crate::eval::{downcast_array, EvalSession};
// `arg` lives in the template module because the template is where it was
// written. It is `pub(crate)` and family-agnostic, so it is reused here
// rather than copied — a second copy would be a second place for the
// planner-bug wording to drift.
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// Translate an arrow kernel failure into an [`ExecError`].
///
/// A verbatim copy of `eval.rs`'s `map_arrow`, which is private to that
/// module and cannot be reached from here. Copied rather than re-derived so
/// the ported functions keep raising *exactly* the errors their `match` arms
/// raised — `abs(i32::MIN)` must stay [`ExecError::Overflow`], not become a
/// `TypeMismatch` because a rewrite mapped the variants differently.
///
/// It should end up shared (`pub(crate)` on the original, or a
/// `crate::funcs::error` module) once more than one family needs it. That is
/// a change to `eval.rs`, which this slice does not touch.
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

/// The shape shared by every infallible `float8 -> float8` function in this
/// module: downcast, apply, done.
///
/// `arity::unary` is the kernel the `match` arms used and is kept, not
/// replaced with an iterator: it copies the input's null buffer straight
/// through, which is what gives invariants 2 (NULL in, NULL out) and 3
/// (length alignment) for free. `f` may be applied to whatever bytes sit
/// behind a null slot, which is harmless for all six callers here — none of
/// them can fail or observe anything.
fn f64_unary(arr: &ArrayRef, f: impl Fn(f64) -> f64) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float64Array>(arr, "double precision")?;
    Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(a, f)))
}

// ─── abs ────────────────────────────────────────────────────────────────────

/// `abs(smallint) -> smallint`, `pg_proc` oid 1398.
///
/// `checked_abs` catches the one input with no representable answer
/// (`abs(i16::MIN)`, whose magnitude does not fit in an `i16`) and raises
/// [`ExecError::Overflow`] rather than wrapping back to a negative number.
///
/// **KNOWN DIVERGENCE, inherited and unchanged by this port.** PostgreSQL
/// raises this as SQLSTATE `22003` with the message `smallint out of range`:
///
/// ```text
/// SELECT abs((-32768)::smallint);  ->  ERROR:  smallint out of range
/// ```
///
/// Basin raises `ExecError::Overflow("abs")`, which `Display`s as `abs out
/// of range` and — because `basin-router`'s `error.rs` has no arm for it —
/// reaches the client as SQLSTATE `XX000` (internal error), not `22003`.
/// The error-vs-success *outcome* matches, which is all
/// `function_equivalence.rs` compares, so the battery is green on this input
/// while a client sees the wrong class of error. Fixing it means giving
/// `ExecError::Overflow` a `22003` arm in `basin-router` and carrying the
/// PostgreSQL type name rather than the operation name, which is a change to
/// two crates this slice does not touch. Reported, not fixed.
pub struct AbsInt2;

impl ScalarFunc for AbsInt2 {
    fn oid(&self) -> Oid {
        Oid(1398)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int16Array>(arg(args, 0, 1398)?, "smallint")?;
        let out = arity::try_unary::<Int16Type, _, Int16Type>(a, |v| {
            v.checked_abs()
                .ok_or_else(|| ArrowError::ArithmeticOverflow("smallint abs".to_string()))
        })
        .map_err(|e| map_arrow(e, "abs"))?;
        Ok(Arc::new(out))
    }
}

/// `abs(integer) -> integer`, `pg_proc` oid 1397. See [`AbsInt2`] for the
/// overflow rule and the SQLSTATE divergence.
///
/// Measured: `SELECT abs((-2147483648)::int4)` -> `ERROR: integer out of
/// range`; every other `int4` in the battery, `i32::MAX` included, is
/// answered.
pub struct AbsInt4;

impl ScalarFunc for AbsInt4 {
    fn oid(&self) -> Oid {
        Oid(1397)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int32Array>(arg(args, 0, 1397)?, "integer")?;
        let out = arity::try_unary::<Int32Type, _, Int32Type>(a, |v| {
            v.checked_abs()
                .ok_or_else(|| ArrowError::ArithmeticOverflow("integer abs".to_string()))
        })
        .map_err(|e| map_arrow(e, "abs"))?;
        Ok(Arc::new(out))
    }
}

/// `abs(bigint) -> bigint`, `pg_proc` oid 1396. See [`AbsInt2`].
///
/// Measured: `SELECT abs((-9223372036854775808)::int8)` -> `ERROR: bigint
/// out of range`.
pub struct AbsInt8;

impl ScalarFunc for AbsInt8 {
    fn oid(&self) -> Oid {
        Oid(1396)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int64Array>(arg(args, 0, 1396)?, "bigint")?;
        let out = arity::try_unary::<Int64Type, _, Int64Type>(a, |v| {
            v.checked_abs()
                .ok_or_else(|| ArrowError::ArithmeticOverflow("bigint abs".to_string()))
        })
        .map_err(|e| map_arrow(e, "abs"))?;
        Ok(Arc::new(out))
    }
}

/// `abs(real) -> real`, `pg_proc` oid 1394.
///
/// Infallible, unlike the integer overloads — every `f32`, finite or not,
/// has a representable magnitude — so this uses the plain `unary` kernel.
/// Measured over the `float4` battery: `abs(-0)` is `0` (positive zero, not
/// `-0`), `abs('-Infinity')` is `Infinity`, `abs('NaN')` is `NaN`,
/// `abs(-1.4e-45)` (the smallest subnormal) is `1.4e-45`. `f32::abs` clears
/// the sign bit and agrees on all four.
pub struct AbsFloat4;

impl ScalarFunc for AbsFloat4 {
    fn oid(&self) -> Oid {
        Oid(1394)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Float32Array>(arg(args, 0, 1394)?, "real")?;
        Ok(Arc::new(arity::unary::<Float32Type, _, Float32Type>(
            a,
            f32::abs,
        )))
    }
}

/// `abs(double precision) -> double precision`, `pg_proc` oid 1395. The
/// `f64` twin of [`AbsFloat4`]; the module doc's battery is the measurement.
pub struct AbsFloat8;

impl ScalarFunc for AbsFloat8 {
    fn oid(&self) -> Oid {
        Oid(1395)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 1395)?, f64::abs)
    }
}

/// `abs(numeric) -> numeric`, `pg_proc` oid 1705.
///
/// The output keeps the input's physical precision and scale. That is the
/// convention the whole decimal family in `eval.rs` follows, and for `abs`
/// specifically it is also what the server does — measured:
/// `abs((-5.50)::numeric(10,2))` is `5.50`, `abs((-0.00)::numeric)` is
/// `0.00`, both retaining their trailing zeros. (The *other* decimal shaping
/// functions do NOT agree with PostgreSQL on scale — `round(2.5::numeric)`
/// prints `3` on the server and `3.0` in Basin. `abs` is the one that
/// matches, which is a further reason it is portable on its own.)
///
/// Uses `try_unary` rather than the infallible `unary` even though
/// `checked_abs` on an `i128` mantissa essentially never fails, purely so
/// the closure is applied only to non-null slots — the property `eval.rs`'s
/// module docs point 7 asks every decimal path to preserve.
///
/// **Gap, not a divergence introduced here:** PostgreSQL's `numeric` has a
/// `NaN` value (`abs('NaN'::numeric)` is `NaN`) and Arrow's `Decimal128` has
/// no bit pattern for it, so that input cannot be represented in Basin at
/// all. Nothing to preserve or fix at this layer.
pub struct AbsNumeric;

impl ScalarFunc for AbsNumeric {
    fn oid(&self) -> Oid {
        Oid(1705)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Decimal128Array>(arg(args, 0, 1705)?, "numeric")?;
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
}

// ─── float8 shaping: round / ceil / ceiling / floor / trunc / sign ──────────

/// `round(double precision) -> double precision`, `pg_proc` oid 1342.
///
/// **`f64::round_ties_even`, never `f64::round`.** PostgreSQL's `dround`
/// calls libm `rint`, which under the IEEE-754 default rounding mode breaks
/// ties toward even. Measured:
///
/// ```text
/// SELECT round(2.5::float8), round(-2.5::float8),
///        round(0.5::float8), round(3.5::float8);
///   ->   2 | -2 | 0 | 4
/// ```
///
/// `f64::round` would answer `3`, `-3`, `1`, `4` — three wrong out of four.
/// And `round(-0.5::float8)` is `-0`, which `round_ties_even` also
/// preserves.
///
/// This is the opposite tie direction from `round(numeric)` (oid 1708),
/// which rounds half away from zero: `round(2.5::numeric)` is `3`. Two
/// functions, deliberately, not one shared one.
pub struct RoundFloat8;

impl ScalarFunc for RoundFloat8 {
    fn oid(&self) -> Oid {
        Oid(1342)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 1342)?, f64::round_ties_even)
    }
}

/// `ceil(double precision) -> double precision`, `pg_proc` oid 2308.
///
/// Measured: `ceil(4.1::float8)` is `5`, `ceil(-4.1::float8)` is `-4`,
/// `ceil(-0.5::float8)` is `-0` (negative zero, not `0`), and
/// `ceil(4.9e-324::float8)` — the smallest subnormal — is `1`. `f64::ceil`
/// agrees on all four including the sign of the zero.
pub struct CeilFloat8;

impl ScalarFunc for CeilFloat8 {
    fn oid(&self) -> Oid {
        Oid(2308)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 2308)?, f64::ceil)
    }
}

/// `ceiling(double precision) -> double precision`, `pg_proc` oid 2320.
///
/// The SQL-standard spelling of `ceil`. PostgreSQL gives it a genuinely
/// separate `pg_proc` row rather than aliasing, so a dispatch keyed on oid
/// must register both — but the behaviour is identical, verified by
/// evaluating the same battery through both names (the module doc's table
/// has them as two columns for exactly this reason, and they agree row for
/// row). Registering [`CeilFloat8`] under both oids is not possible:
/// `FuncRegistry::register_scalar` keys on `f.oid()`, so an alias needs its
/// own type.
pub struct CeilingFloat8;

impl ScalarFunc for CeilingFloat8 {
    fn oid(&self) -> Oid {
        Oid(2320)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 2320)?, f64::ceil)
    }
}

/// `floor(double precision) -> double precision`, `pg_proc` oid 2309.
///
/// Measured: `floor(4.1::float8)` is `4`, `floor(-4.1::float8)` is `-5`,
/// `floor(-0.5::float8)` is `-1` — note this is the one shaping function
/// where `-0.5` does *not* produce a negative zero, and the contrast with
/// `trunc(-0.5) = -0` is what distinguishes the two.
pub struct FloorFloat8;

impl ScalarFunc for FloorFloat8 {
    fn oid(&self) -> Oid {
        Oid(2309)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 2309)?, f64::floor)
    }
}

/// `trunc(double precision) -> double precision`, `pg_proc` oid 1343.
///
/// Truncates toward zero, which is `floor` for positives and `ceil` for
/// negatives. Measured: `trunc(3.7::float8)` is `3` and `trunc(-3.7::float8)`
/// is `-3`, where `floor` answers `-4`. `trunc('NaN')` is `NaN` and
/// `trunc(-0.5)` is `-0`; `f64::trunc` agrees with both.
pub struct TruncFloat8;

impl ScalarFunc for TruncFloat8 {
    fn oid(&self) -> Oid {
        Oid(1343)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 1343)?, f64::trunc)
    }
}

/// `sign(double precision) -> double precision`, `pg_proc` oid 2310.
pub struct SignFloat8;

impl ScalarFunc for SignFloat8 {
    fn oid(&self) -> Oid {
        Oid(2310)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 2310)?, pg_sign_f64)
    }
}

/// PostgreSQL's `dsign`, which is **not** `f64::signum`.
///
/// Rust's `signum` reports the sign *bit*: it answers `1.0` for `+0.0`,
/// `-1.0` for `-0.0`, and `NaN` for `NaN`. PostgreSQL asks "is this
/// positive", so all three of those are `0`. Measured:
///
/// ```text
/// SELECT sign(0::float8), sign('-0'::float8), sign('NaN'::float8),
///        sign('-Infinity'::float8), sign('Infinity'::float8);
///   ->   0 | 0 | 0 | -1 | 1
/// ```
///
/// The `NaN` answer falls out of the ordering rather than needing a branch:
/// every comparison against `NaN` is false, so both guards fail and the
/// `else` returns `0.0` — which is what the server does, measured, not an
/// assumption about an undocumented input.
fn pg_sign_f64(x: f64) -> f64 {
    if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::builtins;
    // `is_null`/`len` are `Array` trait methods, not inherent ones.
    use arrow_array::Array;

    fn f64s(v: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
    }

    fn out_f64(a: &ArrayRef) -> &Float64Array {
        a.as_any()
            .downcast_ref::<Float64Array>()
            .expect("double precision out")
    }

    /// Every oid this module implements must actually be reachable through
    /// the registry. Without this, a port that forgot step 2 (registration)
    /// would still pass every behavioural test below, because `eval.rs`'s
    /// `match` arm would answer instead — the port would look done and have
    /// changed nothing.
    ///
    /// The list is written out rather than derived from the registry's
    /// length, so adding an unrelated function elsewhere cannot make this
    /// pass for the wrong reason.
    #[test]
    fn every_function_in_this_module_is_hosted_by_the_registry() {
        for oid in [
            1398, // abs(smallint)
            1397, // abs(integer)
            1396, // abs(bigint)
            1394, // abs(real)
            1395, // abs(double precision)
            1705, // abs(numeric)
            1342, // round(double precision)
            2308, // ceil(double precision)
            2320, // ceiling(double precision)
            2309, // floor(double precision)
            1343, // trunc(double precision)
            2310, // sign(double precision)
        ] {
            assert!(
                builtins().scalar(Oid(oid)).is_some(),
                "oid {oid} must be reachable through the registry, not only \
                 the match — register it in crate::funcs::builtins"
            );
        }
    }

    /// The whole `float8` battery from the module doc, in one pass, so a
    /// change to any of the six shaping functions is caught against the
    /// values the live server actually returned rather than against six
    /// separately-remembered facts.
    ///
    /// `-0.0` is asserted through `is_sign_negative`, not `==`: `-0.0 == 0.0`
    /// is true in IEEE-754, so an equality assertion would pass for either
    /// sign and the differential harness (which compares floats with a
    /// relative epsilon) cannot see the difference either.
    #[test]
    fn the_float8_shaping_family_matches_the_measured_server_battery() {
        let xs = f64s(vec![
            Some(0.0),
            Some(-0.0),
            Some(0.5),
            Some(-0.5),
            Some(2.5),
            Some(-2.5),
            Some(3.5),
            Some(-3.7),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ]);
        let s = &EvalSession::DEFAULT;

        // round: ties to EVEN — 0.5 -> 0, 2.5 -> 2, -2.5 -> -2, 3.5 -> 4.
        let r = RoundFloat8.invoke(&[xs.clone()], s).expect("round");
        let r = out_f64(&r);
        assert_eq!(r.len(), 12, "output length must match input length");
        assert_eq!(r.value(2), 0.0, "round(0.5) is 0 live, not 1");
        assert_eq!(r.value(4), 2.0, "round(2.5) is 2 live — half to EVEN");
        assert_eq!(r.value(5), -2.0, "round(-2.5) is -2 live");
        assert_eq!(r.value(6), 4.0, "round(3.5) is 4 live — also even");
        assert!(
            r.value(3).is_sign_negative() && r.value(3) == 0.0,
            "round(-0.5) is -0 live, a negative zero"
        );
        assert!(r.value(10).is_nan(), "round('NaN') is NaN live");
        assert!(r.is_null(11), "NULL in, NULL out");

        // ceil / ceiling: identical, two oids.
        for (label, out) in [
            ("ceil", CeilFloat8.invoke(&[xs.clone()], s).expect("ceil")),
            (
                "ceiling",
                CeilingFloat8.invoke(&[xs.clone()], s).expect("ceiling"),
            ),
        ] {
            let c = out_f64(&out);
            assert_eq!(c.len(), 12, "{label}: length alignment");
            assert_eq!(c.value(2), 1.0, "{label}(0.5) is 1 live");
            assert!(
                c.value(3).is_sign_negative() && c.value(3) == 0.0,
                "{label}(-0.5) is -0 live, not 0"
            );
            assert_eq!(c.value(7), -3.0, "{label}(-3.7) is -3 live");
            assert!(c.value(10).is_nan(), "{label}('NaN') is NaN live");
            assert!(c.is_null(11), "{label}: NULL in, NULL out");
        }

        // floor: the one that answers -1 for -0.5.
        let f = FloorFloat8.invoke(&[xs.clone()], s).expect("floor");
        let f = out_f64(&f);
        assert_eq!(f.value(2), 0.0, "floor(0.5) is 0 live");
        assert_eq!(f.value(3), -1.0, "floor(-0.5) is -1 live, not -0");
        assert_eq!(f.value(7), -4.0, "floor(-3.7) is -4 live");
        assert_eq!(f.value(8), f64::INFINITY);

        // trunc: toward zero, so -3.7 -> -3 where floor gave -4.
        let t = TruncFloat8.invoke(&[xs.clone()], s).expect("trunc");
        let t = out_f64(&t);
        assert_eq!(t.value(7), -3.0, "trunc(-3.7) is -3 live, floor's is -4");
        assert!(
            t.value(3).is_sign_negative() && t.value(3) == 0.0,
            "trunc(-0.5) is -0 live"
        );

        // abs.
        let ab = AbsFloat8.invoke(&[xs.clone()], s).expect("abs");
        let ab = out_f64(&ab);
        assert_eq!(ab.value(5), 2.5);
        assert_eq!(ab.value(9), f64::INFINITY, "abs('-Infinity') is Infinity");
        assert!(ab.value(10).is_nan(), "abs('NaN') is NaN live");
        assert!(
            !ab.value(1).is_sign_negative(),
            "abs(-0) is a POSITIVE zero live"
        );
    }

    /// `sign` is the one where Rust's obvious answer is wrong three times
    /// over, so it gets its own test naming each.
    #[test]
    fn sign_float8_is_not_signum_at_zero_or_nan() {
        let xs = f64s(vec![
            Some(-5.0),
            Some(0.0),
            Some(-0.0),
            Some(5.0),
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(f64::INFINITY),
            None,
        ]);
        let out = SignFloat8
            .invoke(&[xs], &EvalSession::DEFAULT)
            .expect("sign");
        let a = out_f64(&out);

        assert_eq!(a.value(0), -1.0);
        assert_eq!(a.value(1), 0.0, "sign(0) is 0 live; f64::signum says 1");
        assert_eq!(a.value(2), 0.0, "sign(-0) is 0 live; f64::signum says -1");
        assert_eq!(a.value(3), 1.0);
        assert_eq!(
            a.value(4),
            0.0,
            "sign('NaN') is 0 on live 18.2; f64::signum returns NaN"
        );
        assert_eq!(a.value(5), -1.0, "sign('-Infinity') is -1 live");
        assert_eq!(a.value(6), 1.0, "sign('Infinity') is 1 live");
        assert!(a.is_null(7), "NULL in, NULL out");
        assert_eq!(a.len(), 8, "output length must match input length");
    }

    /// Integer `abs` at each width: the ordinary case, and the one input per
    /// width whose magnitude is not representable. PostgreSQL raises
    /// `22003 <type> out of range` for all three; Basin raises
    /// [`ExecError::Overflow`] — same outcome, different SQLSTATE, see
    /// [`AbsInt2`].
    #[test]
    fn integer_abs_errors_at_type_min_rather_than_wrapping() {
        let s = &EvalSession::DEFAULT;

        let i2: ArrayRef = Arc::new(Int16Array::from(vec![Some(-5i16), Some(0), None]));
        let out = AbsInt2.invoke(&[i2], s).expect("abs(smallint)");
        let out = out
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("smallint out");
        assert_eq!(out.value(0), 5);
        assert!(out.is_null(2), "NULL in, NULL out");
        assert_eq!(out.len(), 3, "output length must match input length");

        let min2: ArrayRef = Arc::new(Int16Array::from(vec![Some(i16::MIN)]));
        assert!(
            matches!(AbsInt2.invoke(&[min2], s), Err(ExecError::Overflow(_))),
            "abs((-32768)::smallint) must error, not wrap back to -32768"
        );

        let min4: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MIN)]));
        assert!(
            matches!(AbsInt4.invoke(&[min4], s), Err(ExecError::Overflow(_))),
            "abs(-2147483648) must error"
        );
        let ok4: ArrayRef = Arc::new(Int32Array::from(vec![Some(-5i32)]));
        let out = AbsInt4.invoke(&[ok4], s).expect("abs(integer)");
        assert_eq!(
            out.as_any()
                .downcast_ref::<Int32Array>()
                .expect("integer out")
                .value(0),
            5
        );

        let min8: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN)]));
        assert!(
            matches!(AbsInt8.invoke(&[min8], s), Err(ExecError::Overflow(_))),
            "abs(-9223372036854775808) must error"
        );
        let ok8: ArrayRef = Arc::new(Int64Array::from(vec![Some(-5i64)]));
        let out = AbsInt8.invoke(&[ok8], s).expect("abs(bigint)");
        assert_eq!(
            out.as_any()
                .downcast_ref::<Int64Array>()
                .expect("bigint out")
                .value(0),
            5
        );
    }

    /// `abs(real)`: sign bit cleared, including for `-0` and `-Infinity`.
    #[test]
    fn abs_real_clears_the_sign_bit_for_every_float4() {
        let xs: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(-1.0f32),
            Some(-0.0),
            Some(f32::NEG_INFINITY),
            Some(f32::NAN),
            None,
        ]));
        let out = AbsFloat4
            .invoke(&[xs], &EvalSession::DEFAULT)
            .expect("abs(real)");
        let a = out
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("real out");
        assert_eq!(a.value(0), 1.0);
        assert!(
            !a.value(1).is_sign_negative(),
            "abs(-0::real) is a positive 0 live"
        );
        assert_eq!(a.value(2), f32::INFINITY);
        assert!(a.value(3).is_nan());
        assert!(a.is_null(4), "NULL in, NULL out");
        assert_eq!(a.len(), 5, "output length must match input length");
    }

    /// `abs(numeric)` keeps the input's physical precision and scale — and
    /// here that agrees with the server, which prints `abs((-5.50)::numeric(10,2))`
    /// as `5.50`, trailing zero retained.
    #[test]
    fn abs_numeric_preserves_precision_and_scale() {
        let xs: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(-550i128), Some(0), None])
                .with_precision_and_scale(10, 2)
                .expect("numeric(10,2)"),
        );
        let out = AbsNumeric
            .invoke(&[xs], &EvalSession::DEFAULT)
            .expect("abs(numeric)");
        let a = out
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("numeric out");
        assert_eq!(a.value(0), 550, "-5.50 -> 5.50 at scale 2");
        assert_eq!((a.precision(), a.scale()), (10, 2));
        assert!(a.is_null(2), "NULL in, NULL out");
        assert_eq!(a.len(), 3, "output length must match input length");
    }

    /// Arity is guaranteed by resolution, so a short call is a planner bug
    /// and must say so rather than silently returning something. Checked on
    /// one representative per argument type, since every implementation here
    /// reaches it through the same shared `arg` helper.
    #[test]
    fn a_missing_argument_is_reported_as_a_planner_bug() {
        let s = &EvalSession::DEFAULT;
        for (name, err) in [
            ("abs(smallint)", AbsInt2.invoke(&[], s)),
            ("abs(numeric)", AbsNumeric.invoke(&[], s)),
            ("round(float8)", RoundFloat8.invoke(&[], s)),
            ("sign(float8)", SignFloat8.invoke(&[], s)),
        ] {
            let err = err.expect_err("no arguments must fail");
            assert!(
                matches!(err, ExecError::Internal(ref m) if m.contains("planner bug")),
                "{name}: expected a planner-bug Internal error, got {err:?}"
            );
        }
    }

    /// A wrong physical type is a planner/type-system failure, not a wrong
    /// answer: the downcast must report it rather than silently reinterpret
    /// the buffer.
    #[test]
    fn a_wrongly_typed_argument_is_a_type_mismatch_not_a_wrong_answer() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1i32)]));
        let err = RoundFloat8
            .invoke(&[ints], &EvalSession::DEFAULT)
            .expect_err("an int32 array is not double precision");
        assert!(
            matches!(err, ExecError::TypeMismatch(ref m) if m.contains("double precision")),
            "expected a TypeMismatch naming double precision, got {err:?}"
        );
    }
}
