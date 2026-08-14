//! Trigonometric, logarithmic, exponential and power functions on
//! `double precision`, hosted through [`crate::funcs::ScalarFunc`].
//!
//! # What is here
//!
//! The 24 `float8` math functions that lived in `eval_scalar_fn`'s `match`:
//!
//! ```text
//! sqrt cbrt exp ln log log10 power pow degrees radians
//! sin cos tan cot asin acos atan atan2
//! sinh cosh tanh asinh acosh atanh
//! ```
//!
//! `pi()` (oid 1610) was in this slice's brief and is **not** here. See
//! "The `pi()` gap" below; it is the same ABI gap
//! [`crate::funcs::num_fns`] already reported, re-confirmed by this slice.
//!
//! # The float8 policy, restated because it is the whole content of this file
//!
//! Almost none of these are `arity::unary(f64::whatever)`, and the reason is
//! that **PostgreSQL raises errors exactly where IEEE-754 hands back a quiet
//! `NaN`, `±Infinity` or `0`.** `eval.rs`'s module docs state the rule as
//! three ordered phases and this file transcribes it unchanged:
//!
//! 1. **IEEE-754 special cases first.** A `NaN` argument returns `NaN` for
//!    every function here, *before* any domain guard runs — otherwise
//!    `asin('NaN')` would be reported as out of range, since
//!    `(-1.0..=1.0).contains(&NaN)` is `false`. Measured live: `asin('NaN')`,
//!    `acos('NaN')`, `acosh('NaN')`, `atanh('NaN')`, `sin('NaN')`,
//!    `cot('NaN')`, `ln('NaN')`, `sqrt('NaN')`, `exp('NaN')`,
//!    `degrees('NaN')`, `radians('NaN')` are all `NaN`. And for `power` the
//!    POSIX pair `power('NaN', 0) = 1`, `power(1, 'NaN') = 1` is decided ahead
//!    of the zero-base guard, which is why `power(0, 'NaN')` is `NaN` and not
//!    "zero raised to a negative power".
//! 2. **Domain guards second.** `sqrt(-1)`, `ln(0)`, `ln(-1)`, `log(0)`,
//!    `log(-1)`, `log10(0)`, `asin(2)`, `acos(2)`, `acosh(0.5)`, `atanh(2)`,
//!    `power(0, -1)` and `power(-2, 0.5)` are all ERRORS on the live server,
//!    not `NaN`/`Infinity` — every one measured this session, table below.
//!    `sin`/`cos`/`tan`/`cot` are the mirror image: a *finite* argument is
//!    always in domain and an *infinite* one is the error.
//! 3. **Range-check the result last** — [`check_float8_result`], Postgres's
//!    `check_float8_val(val, inf_is_valid, zero_is_valid)`. An infinite result
//!    is legal only when an input was already infinite; a zero result is legal
//!    only when zero was reachable for that input. `exp('Infinity')` is
//!    `Infinity` but `exp(1.8e308)` is `22003`; `exp(-745)` is `5e-324` but
//!    `exp(-746)` is an underflow *error*, both measured.
//!
//! # Measured against live PostgreSQL 18.2
//!
//! `postgres://pc@127.0.0.1:5432/postgres`, `PostgreSQL 18.2 (Homebrew) on
//! aarch64-apple-darwin24.6.0`, read off the server this session. The error
//! battery — every one of these is a place a naive `arity::unary` port would
//! have silently returned a number:
//!
//! ```text
//!  expression                | live PostgreSQL 18.2                                       | libm/Rust would say
//! ---------------------------+------------------------------------------------------------+---------------------
//!  ln(0)                     | ERROR cannot take logarithm of zero                        | -Infinity
//!  ln(-1)                    | ERROR cannot take logarithm of a negative number           | NaN
//!  log(0)                    | ERROR cannot take logarithm of zero                        | -Infinity
//!  log(-1)                   | ERROR cannot take logarithm of a negative number           | NaN
//!  log10(0)                  | ERROR cannot take logarithm of zero                        | -Infinity
//!  sqrt(-1)                  | ERROR cannot take square root of a negative number         | NaN
//!  acos(2)                   | ERROR input is out of range                                | NaN
//!  asin(2)                   | ERROR input is out of range                                | NaN
//!  atanh(2)                  | ERROR input is out of range                                | NaN
//!  acosh(0.5)                | ERROR input is out of range                                | NaN
//!  sin('Infinity')           | ERROR input is out of range                                | NaN
//!  cot('Infinity')           | ERROR input is out of range                                | NaN
//!  power(0, -1)              | ERROR zero raised to a negative power is undefined         | Infinity
//!  power(-2, 0.5)            | ERROR a negative number raised to a non-integer power ...  | NaN
//!  exp(-746)                 | ERROR value out of range: underflow                        | 0
//!  degrees(1.797e308)        | ERROR value out of range: overflow                         | Infinity
//!  radians(4.9e-324)         | ERROR value out of range: underflow                        | 0
//! ```
//!
//! And the values that are NOT errors, which is the half that is easy to get
//! wrong by over-guarding:
//!
//! ```text
//!  atanh(1)      Infinity     atanh(-1)     -Infinity    acosh(1)          0
//!  cot(0)        Infinity     cot(-0)       -Infinity    acosh('Infinity') Infinity
//!  sqrt(-0)      -0           cbrt(-8)      -2           cbrt(-0)          -0
//!  exp('Inf')    Infinity     exp('-Inf')   0            sinh(1e308)       Infinity
//!  tanh('Inf')   1            asinh('Inf')  Infinity     tan(pi()/2)       1.633123935319537e+16
//!  power(0,0)    1            power(-2,3)   -8           power(-1,'Inf')   1
//!  atan('Inf')   1.5707963267948966          atan2('Inf','Inf')  0.7853981633974483
//! ```
//!
//! **`power(-2, 0.5)` and `power(0, -1)` DO already error in Basin** — the
//! slice brief carried a stale claim that they did not. [`pg_power_f64`]
//! below is the arm's implementation, moved verbatim, and its phase-2 guards
//! are exactly these two. `eval.rs`'s own tests at
//! `power_negative_base_fractional_exponent_errors` and
//! `power_zero_to_negative_errors` pin them.
//!
//! # The one divergence this slice FOUND
//!
//! **`degrees(x)` is off by one ULP for roughly one input in eight.**
//! PostgreSQL's `ddegrees` *divides*: `float8_div(arg1, RADIANS_PER_DEGREE)`
//! with `RADIANS_PER_DEGREE = 0.0174532925199432957692`. Rust's
//! `f64::to_degrees` *multiplies* by the precomputed reciprocal
//! `57.29577951308232`. Division and multiplication-by-reciprocal are not the
//! same operation in binary floating point. Measured live with
//! `extra_float_digits = 3`:
//!
//! ```text
//!  x                    | PostgreSQL 18.2        | Basin (f64::to_degrees)
//! ----------------------+------------------------+------------------------
//!  0.1                  |  5.729577951308232     |  5.729577951308233
//!  100000               |  5729577.951308232     |  5729577.951308233
//!  1e300                |  5.729577951308232e301 |  5.729577951308233e301
//!  -132.70863267522827  | -7603.644557242511     | -7603.644557242512
//!  -206.63905069843963  | -11839.545487610436    | -11839.545487610438
//!  -383.03635179613127  | -21946.36635800657     | -21946.366358006573
//!  -880.7976600675347   | -50465.9885268683      | -50465.98852686831
//!  -144.81538866119422  | -8297.310578833105     | -8297.310578833107
//!  171.12372701527738   |  9804.667332524223 (*) |  9804.667332524221
//! ```
//!
//! (*) columns are `PostgreSQL | Basin`; the `171.12…` row is the one where
//! Basin reads low rather than high — the error is not one-directional.
//!
//! Over 300 uniform random draws from `[-1000, 1000]`, **38 disagreed** —
//! 12.7%. Every disagreement is a single ULP.
//!
//! `radians` has the *same* shape of definition and does **not** diverge:
//! PostgreSQL multiplies (`float8_mul(arg1, RADIANS_PER_DEGREE)`) and so does
//! `f64::to_radians`, by a constant that rounds to the same `f64`. Checked
//! across the same battery, every value identical. So this is specifically
//! `degrees`, not "the angle conversions".
//!
//! **NOT FIXED HERE.** A port moves behaviour; it does not improve it. The
//! divergence is inherited from the `match` arm and is pinned in
//! [`tests::degrees_diverges_from_postgresql_by_one_ulp`] stating BOTH
//! answers. The fix — writing `x / (std::f64::consts::PI / 180.0)` — is a
//! one-line behaviour change and belongs in its own commit, so that a bisect
//! blames it and not this port.
//!
//! # The `pi()` gap, re-confirmed
//!
//! `pi()` (oid 1610) is niladic. Its `match` arm is
//!
//! ```text
//! OID_PI => Ok(Arc::new(Float64Array::from(vec![
//!     std::f64::consts::PI;
//!     batch.num_rows()
//! ]))),
//! ```
//!
//! and it reads `batch.num_rows()` to size its output. [`ScalarFunc::invoke`]
//! receives `&[ArrayRef]` and an [`EvalSession`]; with zero arguments the
//! slice is empty and neither parameter carries a row count, so a niladic
//! function hosted here cannot satisfy invariant 3 (output length equals
//! input length). It would have to return a length-1 array into an N-row
//! batch. [`crate::funcs::num_fns`] reported this first; this slice hit it
//! independently and agrees. **`pi()` stays in the `match`**, and its arm must
//! NOT be deleted.
//!
//! # Helper duplication
//!
//! [`check_float8_result`], [`f64_unary_checked`], [`f64_binary_checked`] and
//! [`map_arrow`] are copies of `eval.rs` privates. Copied, not shared, for the
//! reason [`crate::funcs::num_fns`] gives for its own copy of `map_arrow`:
//! making them `pub(crate)` is an edit to `eval.rs`, and this slice owns
//! neither `eval.rs` nor `funcs/mod.rs`. Three modules now carry a `map_arrow`
//! copy, which is the point at which a `crate::funcs::error` module is owed.
//!
//! Note that `float8_binary_checked` and `pg_power_f64` do **not** become dead
//! in `eval.rs` when this slice's arms are deleted: `eval_exponent` (the `^`
//! operator, oid 965) calls both. See `trig_fns.integration.md` §3.

use std::sync::Arc;

use arrow::compute::kernels::arity;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array};
use arrow_schema::ArrowError;
use basin_pgtype::Oid;

use crate::eval::{downcast_array, EvalSession};
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

// ─── Shared machinery, copied verbatim from `eval.rs` ───────────────────────

/// Translate an arrow kernel failure into an [`ExecError`]. A verbatim copy of
/// `eval.rs`'s private `map_arrow`; only [`Atan2Float8`] needs it, since it is
/// the one function here that reaches a fallible arrow kernel.
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

/// Postgres's `check_float8_val(val, inf_is_valid, zero_is_valid)` — the range
/// check applied to a *result*, never to an input. Phase 3 of the module doc's
/// policy.
///
/// Both failures are SQLSTATE `22003` in Postgres with these exact message
/// strings. **KNOWN DIVERGENCE, inherited and shared by every error in this
/// module:** they ride [`ExecError::TypeMismatch`], and
/// `owned_engine.rs`'s `Fallback::into_error_after_publishing` maps only
/// `ExecError::Cancelled` (57014) and `ExecError::CardinalityViolation`
/// (21000) to an established SQLSTATE — everything else becomes
/// `BasinError::Internal`, which reaches the client as `XX000`. So a client
/// sees an internal error where PostgreSQL sends `22003` / `2201E` / `2201F`.
/// The error-vs-success *outcome* matches, which is all the differential
/// batteries compare. Exactly the shape
/// [`crate::funcs::num_fns::AbsInt2`] reports for `Overflow`, and not fixed
/// here for the same reason: it is a change to `basin-engine`/`basin-router`,
/// and a port moves behaviour rather than improving it.
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
    // `result == 0.0` is true for `-0.0` too, which is what Postgres's C
    // comparison does — an underflow landing on negative zero is still an
    // underflow.
    if result == 0.0 && !zero_is_valid {
        return Err(ExecError::TypeMismatch(
            "value out of range: underflow".to_string(),
        ));
    }
    Ok(result)
}

/// Apply a fallible `f64 -> f64` elementwise, NULL in / NULL out.
///
/// A copy of `eval.rs`'s `float8_unary_checked`. It walks indices rather than
/// using `arity::try_unary` for one load-bearing reason: `f` must never see
/// the bytes behind a NULL slot. `arity::unary` may apply its closure to
/// garbage in a null slot, which is harmless for `abs` but not here — a
/// leftover `-1.0` under a NULL would make `sqrt` raise "cannot take square
/// root of a negative number" for a row that is NULL. Invariant 2 and
/// invariant 3 both come from pushing exactly one output per input index.
fn f64_unary_checked(
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

/// [`f64_unary_checked`]'s two-argument counterpart, for `power`/`pow` — the
/// functions here whose domain restriction and range check depend on both
/// arguments together. A copy of `eval.rs`'s `float8_binary_checked`, which
/// stays there because `eval_exponent` (the `^` operator) still calls it.
///
/// A NULL on *either* side yields NULL, which is what the server does:
/// `power(NULL, 2)` and `power(2, NULL)` are both NULL.
fn f64_binary_checked(
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

/// The infallible `float8 -> float8` shape, for the five functions here that
/// genuinely have no domain and no reachable range failure: `cbrt`, `atan`,
/// `sinh`, `cosh`, `tanh`, `asinh`. `arity::unary` copies the input's null
/// buffer straight through, giving invariants 2 and 3 for free; it is safe
/// precisely *because* these closures cannot fail or observe anything, which
/// is not true of the checked family above.
fn f64_unary(arr: &ArrayRef, f: impl Fn(f64) -> f64) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<Float64Array>(arr, "double precision")?;
    Ok(Arc::new(arity::unary::<Float64Type, _, Float64Type>(a, f)))
}

// ─── roots, exponential, logarithms ─────────────────────────────────────────

/// `sqrt(double precision) -> double precision`, `pg_proc` oid 1344.
///
/// Measured live: `sqrt(-1)` raises `ERROR: cannot take square root of a
/// negative number` (SQLSTATE `2201F`, `invalid_argument_for_power_function` —
/// the *same* code `power`'s domain errors use, not a dedicated one).
/// `f64::sqrt(-1.0)` is a quiet `NaN`, which is the whole reason this is not
/// `arity::unary(f64::sqrt)`.
///
/// The guard is `x < 0.0`, deliberately not `x.is_sign_negative()`: measured
/// live, `sqrt(-0.0)` is `-0`, an answer and not an error, and `-0.0 < 0.0` is
/// false so it survives. `sqrt('NaN')` is `NaN` (the comparison is false) and
/// `sqrt('Infinity')` is `Infinity`, both measured.
///
/// No phase-3 range check: Postgres has one (`dsqrt` tests the result for
/// infinity and zero) but it is unreachable — `sqrt` of a finite non-zero
/// input is finite and non-zero. The `match` arm omitted it and so does this.
pub struct SqrtFloat8;

impl ScalarFunc for SqrtFloat8 {
    fn oid(&self) -> Oid {
        Oid(1344)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1344)?, pg_sqrt_f64)
    }
}

/// See [`SqrtFloat8`].
fn pg_sqrt_f64(x: f64) -> Result<f64, ExecError> {
    if x < 0.0 {
        return Err(ExecError::TypeMismatch(
            "cannot take square root of a negative number".to_string(),
        ));
    }
    Ok(x.sqrt())
}

/// `cbrt(double precision) -> double precision`, `pg_proc` oid 1345.
///
/// The one root function with **no domain restriction at all**: a cube root of
/// a negative number is real. Measured live, `cbrt(-8)` is `-2`, `cbrt(-27)`
/// is `-3`, `cbrt(-0)` is `-0` (negative zero preserved), `cbrt('NaN')` is
/// `NaN` and `cbrt('Infinity')` is `Infinity`. `f64::cbrt` is the same libm
/// routine Postgres calls and agrees on all five, so this is the infallible
/// kernel — the only member of the root/log/exp group that is.
pub struct CbrtFloat8;

impl ScalarFunc for CbrtFloat8 {
    fn oid(&self) -> Oid {
        Oid(1345)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary(arg(args, 0, 1345)?, f64::cbrt)
    }
}

/// `exp(double precision) -> double precision`, `pg_proc` oid 1347.
///
/// No domain restriction; this is a pure phase-3 function, and the reason it
/// is not `arity::unary(f64::exp)` is that libm's saturation to `±Infinity`
/// and `0` is an *error* in Postgres. Measured live:
///
/// ```text
/// exp('Infinity')   Infinity     exp(1.8e308)   ERROR value out of range: overflow
/// exp('-Infinity')  0            exp(-746)      ERROR value out of range: underflow
/// exp('NaN')        NaN          exp(-745)      5e-324
/// ```
///
/// The `exp(-745)` / `exp(-746)` pair is the sharp edge: the first is a
/// legitimate subnormal, the second is an error rather than `0`. Both validity
/// flags are `x.is_infinite()`, so the `±Infinity` inputs keep their answers
/// while the finite inputs that produce the same bits do not.
pub struct ExpFloat8;

impl ScalarFunc for ExpFloat8 {
    fn oid(&self) -> Oid {
        Oid(1347)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1347)?, pg_exp_f64)
    }
}

/// See [`ExpFloat8`].
fn pg_exp_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.exp(), x.is_infinite(), x.is_infinite())
}

/// `ln(double precision) -> double precision`, `pg_proc` oid 1341 — the
/// **natural** logarithm.
///
/// Measured live: `ln(0)` raises `ERROR: cannot take logarithm of zero` and
/// `ln(-1)` raises `ERROR: cannot take logarithm of a negative number` — two
/// distinct messages under one SQLSTATE (`2201E`,
/// `invalid_argument_for_logarithm`), matched exactly rather than collapsed
/// into one wording. `ln('NaN')` is `NaN` and `ln('Infinity')` is `Infinity`,
/// both measured; both fall out of the guards without a branch, since every
/// comparison against `NaN` is false.
///
/// Not to be confused with [`LogFloat8`], which is base 10 — see its doc.
pub struct LnFloat8;

impl ScalarFunc for LnFloat8 {
    fn oid(&self) -> Oid {
        Oid(1341)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1341)?, pg_ln_f64)
    }
}

/// See [`LnFloat8`].
fn pg_ln_f64(x: f64) -> Result<f64, ExecError> {
    reject_nonpositive_log_argument(x)?;
    Ok(x.ln())
}

/// `log(double precision) -> double precision`, `pg_proc` oid 1340 — **base
/// 10, not natural log.**
///
/// This is the single most confusable row in the family. Confirmed live:
/// `log(100::float8)` is `2`, `ln(100::float8)` is `4.605170185988092`. There
/// is no two-argument `log(float8, float8)`; only `numeric` has an
/// explicit-base form. So oid 1340 is base 10 and shares its implementation
/// with [`Log10Float8`] (oid 1194) exactly, not approximately.
///
/// Domain errors are `ln`'s, verified separately rather than assumed:
/// `log(0)` raises `cannot take logarithm of zero`, `log(-1)` raises `cannot
/// take logarithm of a negative number`, `log('NaN')` is `NaN`.
pub struct LogFloat8;

impl ScalarFunc for LogFloat8 {
    fn oid(&self) -> Oid {
        Oid(1340)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1340)?, pg_log10_f64)
    }
}

/// See [`LogFloat8`].
fn pg_log10_f64(x: f64) -> Result<f64, ExecError> {
    reject_nonpositive_log_argument(x)?;
    Ok(x.log10())
}

/// Shared domain guard for `ln`/`log`/`log10`. Zero and negative are two
/// different Postgres messages, both SQLSTATE `2201E` — matched exactly rather
/// than collapsed. A `NaN` argument passes through: both comparisons are
/// false, which is what makes `ln('NaN')` a `NaN` without an explicit branch,
/// and is what Postgres's own C guard does.
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

/// `log10(double precision) -> double precision`, `pg_proc` oid 1194.
///
/// A **separate `pg_proc` row** from `log(float8)` (oid 1340) with identical
/// semantics, since Postgres's one-argument `log` on `double precision` is
/// already base 10. Sharing [`pg_log10_f64`] is therefore exact rather than an
/// approximation — confirmed live, `log(100::float8)` and
/// `log10(100::float8)` are both `2`, and `log10(0)` raises the same `cannot
/// take logarithm of zero`.
///
/// It needs its own type rather than registering [`LogFloat8`] twice:
/// `FuncRegistry::register_scalar` keys on `f.oid()` and panics on a duplicate,
/// so an alias cannot share an instance. Same reason
/// [`crate::funcs::num_fns::CeilingFloat8`] exists next to `CeilFloat8`.
pub struct Log10Float8;

impl ScalarFunc for Log10Float8 {
    fn oid(&self) -> Oid {
        Oid(1194)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1194)?, pg_log10_f64)
    }
}

// ─── power ──────────────────────────────────────────────────────────────────

/// `power(double precision, double precision) -> double precision`,
/// `pg_proc` oid 1368.
///
/// The one function in this file that exercises all three phases of the module
/// doc's policy, so [`pg_power_f64`] is written as three labelled blocks.
///
/// **The slice brief claimed `power(-2, 0.5)` and `power(0, -1)` "should error
/// and currently do not". That claim is stale — they DO error**, in the arm
/// this replaces and here. Measured live and preserved exactly:
///
/// ```text
/// power(0, -1)     ERROR  zero raised to a negative power is undefined
/// power(-2, 0.5)   ERROR  a negative number raised to a non-integer power
///                         yields a complex result
/// ```
///
/// `f64::powf` answers `Infinity` and `NaN` for those two respectively.
pub struct PowerFloat8;

impl ScalarFunc for PowerFloat8 {
    fn oid(&self) -> Oid {
        Oid(1368)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_binary_checked(arg(args, 0, 1368)?, arg(args, 1, 1368)?, pg_power_f64)
    }
}

/// `pow(double precision, double precision) -> double precision`,
/// `pg_proc` oid 1346 — a separate catalog row from `power` (1368) with
/// identical semantics, the same relationship [`Log10Float8`] has to
/// [`LogFloat8`]. Confirmed live: `pow(2, 10)` is `1024` and
/// `pow(-2, 0.5)` raises the same complex-result error `power` does.
pub struct PowFloat8;

impl ScalarFunc for PowFloat8 {
    fn oid(&self) -> Oid {
        Oid(1346)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_binary_checked(arg(args, 0, 1346)?, arg(args, 1, 1346)?, pg_power_f64)
    }
}

/// Shared by [`PowerFloat8`] and [`PowFloat8`]. Every claim below was read off
/// live PostgreSQL 18.2.
///
/// **Phase 1 — IEEE-754/POSIX special cases, before the domain guards.**
/// `power('NaN', 0) = 1` and `power(1, 'NaN') = 1`; every other `NaN`-touching
/// pair is `NaN`. This block *must* come first: `power(0, 'NaN')` is `NaN`
/// live, and the zero-base guard below would otherwise claim it as "zero
/// raised to a negative power". Getting this order backwards is, per
/// `eval.rs`'s module docs, the single defect that produced most of the
/// float8 divergences the policy was written to close.
///
/// **Phase 2 — domain guards.**
/// * `power(0, negative)` raises `zero raised to a negative power is
///   undefined`, confirmed for `-0.0` as the base and `-Infinity` as the
///   exponent. `f64::powf` returns `Infinity`.
/// * A negative base with a non-integer exponent is a complex number:
///   `a negative number raised to a non-integer power yields a complex
///   result`.
///
/// "Integer exponent" is `exponent.floor() == exponent`, Postgres's own
/// `floor(arg2) != arg2`. **Deliberately NOT `exponent.fract() != 0.0`**,
/// which is equivalent for finite values and wrong for `±Infinity`:
/// `f64::fract` of an infinity is `NaN`, so a `fract`-based guard would reject
/// every infinite exponent — and `power(-1, 'Infinity')` is `1` live.
/// `power(-2, 3)` is `-8` live and must also survive.
///
/// **Phase 3 — range check.** `power(0, 0)` is `1` and needs no special case,
/// but `power(0.5, 1.8e308)` is a `22003` error rather than the `0` libm
/// produces. A zero or infinite *result* is legitimate only when an infinite
/// input or a zero base put it there — `power(0, 5) = 0`,
/// `power('Infinity', -2) = 0`, `power(0.5, 'Infinity') = 0` are all answers.
fn pg_power_f64(base: f64, exponent: f64) -> Result<f64, ExecError> {
    // Phase 1 — IEEE-754 special cases.
    if base.is_nan() {
        // `exponent == 0.0`, not `exponent.abs() == 0.0`: `-0.0 == 0.0`
        // already, and `power('NaN', -0.0)` is 1 live.
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

// ─── angle conversion ───────────────────────────────────────────────────────

/// `degrees(double precision) -> double precision`, `pg_proc` oid 1608.
///
/// **KNOWN DIVERGENCE, FOUND BY THIS SLICE, inherited and deliberately NOT
/// fixed.** PostgreSQL's `ddegrees` *divides* by `RADIANS_PER_DEGREE`
/// (`float8_div(arg1, 0.0174532925199432957692)`); `f64::to_degrees`
/// *multiplies* by the precomputed reciprocal `57.29577951308232`. Those two
/// are not the same operation in binary floating point, and they disagree by
/// one ULP on roughly one input in eight. Measured live with
/// `extra_float_digits = 3`:
///
/// ```text
///  x                    | PostgreSQL 18.2        | Basin
/// ----------------------+------------------------+------------------------
///  0.1                  |  5.729577951308232     |  5.729577951308233
///  100000               |  5729577.951308232     |  5729577.951308233
///  1e300                |  5.729577951308232e301 |  5.729577951308233e301
///  -206.63905069843963  | -11839.545487610436    | -11839.545487610438
///  171.12372701527738   |  9804.667332524221     |  9804.667332524223
/// ```
///
/// 38 of 300 uniform random draws from `[-1000, 1000]` disagreed — 12.7%,
/// always by exactly one ULP, in both directions. See the module doc and
/// [`tests::degrees_diverges_from_postgresql_by_one_ulp`], which pins BOTH
/// answers. [`RadiansFloat8`] does *not* have this problem.
///
/// The range check is real, not decorative: `degrees` is a scale-*up*, so
/// overflow is the reachable end. Measured, `degrees('Infinity')` is
/// `Infinity` but `degrees(1.797e308)` raises `value out of range: overflow`.
/// `degrees(0)` is a legitimate `0` and `degrees(-0.0)` stays `-0`, which is
/// why `zero_is_valid` is `x == 0.0` rather than `false`.
pub struct DegreesFloat8;

impl ScalarFunc for DegreesFloat8 {
    fn oid(&self) -> Oid {
        Oid(1608)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1608)?, pg_degrees_f64)
    }
}

/// See [`DegreesFloat8`], including the one-ULP divergence `f64::to_degrees`
/// carries.
fn pg_degrees_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.to_degrees(), x.is_infinite(), x == 0.0)
}

/// `radians(double precision) -> double precision`, `pg_proc` oid 1609.
///
/// The mirror of [`DegreesFloat8`] and — checked, not assumed — **exact**.
/// PostgreSQL's `dradians` is `float8_mul(arg1, RADIANS_PER_DEGREE)` and
/// `f64::to_radians` multiplies by `std::f64::consts::PI / 180.0`; the two
/// constants round to the same `f64` (`0.017453292519943295`), so both perform
/// the identical multiplication. Verified bit-for-bit against the live server
/// with `extra_float_digits = 3` on 13 probes spanning
/// `{0.1, 1, 2.5, 7, 1/3, 45, 90, 180, 123.456, 57.29577951308232, 1e5,
/// 1e-300, 1e300}` — every value identical. That is why the divergence
/// [`DegreesFloat8`] reports is specific to `degrees` and not to "the angle
/// conversions".
///
/// `radians` is a scale-*down*, so it is underflow that bites: measured,
/// `radians(4.9e-324)` (the smallest subnormal) raises `value out of range:
/// underflow`, while `radians(0)` is a perfectly good `0`. The check is kept
/// symmetric with `degrees` rather than underflow-only, because Postgres's
/// `float8_mul` checks both directions.
pub struct RadiansFloat8;

impl ScalarFunc for RadiansFloat8 {
    fn oid(&self) -> Oid {
        Oid(1609)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1609)?, pg_radians_f64)
    }
}

/// See [`RadiansFloat8`].
fn pg_radians_f64(x: f64) -> Result<f64, ExecError> {
    check_float8_result(x.to_radians(), x.is_infinite(), x == 0.0)
}

// ─── circular trig: sin / cos / tan / cot ───────────────────────────────────

/// `sin(double precision) -> double precision`, `pg_proc` oid 1604.
///
/// Argument in radians. Measured live: `sin(pi()/2)` is `1`, `sin('NaN')` is
/// `NaN`, and **`sin('Infinity')` is an ERROR** — `input is out of range`,
/// SQLSTATE `22003`. libm answers `NaN` for an infinite argument, and
/// returning that would silently claim Postgres computed something. See
/// [`pg_trig_of_radians`], which all four of this group share.
pub struct SinFloat8;

impl ScalarFunc for SinFloat8 {
    fn oid(&self) -> Oid {
        Oid(1604)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1604)?, |x| pg_trig_of_radians(x, f64::sin))
    }
}

/// `cos(double precision) -> double precision`, `pg_proc` oid 1605. Measured
/// live: `cos(0)` is `1`, `cos('Infinity')` raises `input is out of range`.
/// See [`SinFloat8`].
pub struct CosFloat8;

impl ScalarFunc for CosFloat8 {
    fn oid(&self) -> Oid {
        Oid(1605)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1605)?, |x| pg_trig_of_radians(x, f64::cos))
    }
}

/// `tan(double precision) -> double precision`, `pg_proc` oid 1606.
///
/// Measured live: `tan(0)` is `0`, `tan('Infinity')` raises `input is out of
/// range`, and — the one worth writing down — **`tan(pi()/2)` is
/// `1.633123935319537e+16`, a finite number, not an error and not an
/// infinity.** `pi()/2` is not exactly a pole in binary floating point, so the
/// asymptote is never actually reached. That is why this needs no phase-3
/// range check even though Postgres's `dtan` carries one.
pub struct TanFloat8;

impl ScalarFunc for TanFloat8 {
    fn oid(&self) -> Oid {
        Oid(1606)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1606)?, |x| pg_trig_of_radians(x, f64::tan))
    }
}

/// `cot(double precision) -> double precision`, `pg_proc` oid 1607.
///
/// Shares [`SinFloat8`]'s shape exactly, including the part that is easy to
/// miss. All measured live:
///
/// ```text
/// cot(0)           Infinity      <- an ANSWER, not an error and not NULL
/// cot(-0)          -Infinity     <- the sign of the zero survives
/// cot('NaN')       NaN
/// cot('Infinity')  ERROR  input is out of range
/// cot(1)           0.6420926159343308
/// ```
///
/// `cot(0) = Infinity` is why there is deliberately **no**
/// [`check_float8_result`] here: an infinite result is legitimate for a finite
/// input in this one function. Postgres's `dcot` says so in a comment of its
/// own ("Not checking for overflow because cot(0) == Inf"). Computing it as
/// `1.0 / tan(x)` rather than a dedicated `cot` is also what Postgres does, so
/// the last-bit behaviour matches rather than approximates.
pub struct CotFloat8;

impl ScalarFunc for CotFloat8 {
    fn oid(&self) -> Oid {
        Oid(1607)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1607)?, |x| {
            pg_trig_of_radians(x, |v| 1.0 / f64::tan(v))
        })
    }
}

/// The shape `sin`/`cos`/`tan`/`cot` share: `NaN` passes straight through, an
/// infinite argument is a hard error, every finite argument is in domain.
///
/// This is a domain guard in the *opposite* direction from `asin`/`acos`,
/// which is the thing to notice — here it is the unbounded input that is
/// rejected, not the out-of-range one. Phase 1 before phase 2, as everywhere
/// else in this file: the `NaN` test comes first because `NaN.is_infinite()`
/// is false but a reader should not have to work that out.
fn pg_trig_of_radians(x: f64, f: impl Fn(f64) -> f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    if x.is_infinite() {
        return Err(ExecError::TypeMismatch("input is out of range".to_string()));
    }
    Ok(f(x))
}

// ─── inverse circular trig: asin / acos / atan / atan2 ──────────────────────

/// `asin(double precision) -> double precision`, `pg_proc` oid 1600.
///
/// Domain `[-1, 1]`, endpoints included. Measured live: `asin(1)` is
/// `1.5707963267948966`, `asin(-1)` is `-1.5707963267948966`, `asin('NaN')` is
/// `NaN`, and **`asin(2)` raises `input is out of range`** — SQLSTATE `22003`
/// (`numeric_value_out_of_range`), a genuinely different code from the
/// `2201E`/`2201F` the `ln`/`sqrt`/`power` domain errors use, not the same one
/// reused. `f64::asin` outside the domain returns a quiet `NaN`.
///
/// The explicit `NaN` branch in [`pg_asin_f64`] is load-bearing *here* in a way
/// it is not in Postgres: this file's guard is phrased as
/// `(-1.0..=1.0).contains(&x)`, which is `false` for `NaN` and would therefore
/// report it as out of range. Postgres's `arg < -1.0 || arg > 1.0` is `false`
/// for `NaN` and needs no branch. Same answer, different reason — worth the
/// two lines.
pub struct AsinFloat8;

impl ScalarFunc for AsinFloat8 {
    fn oid(&self) -> Oid {
        Oid(1600)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        f64_unary_checked(arg(args, 0, 1600)?, pg_asin_f64)
    }
}

/// See [`AsinFloat8`].
fn pg_asin_f64(x: f64) -> Result<f64, ExecError> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    reject_out_of_trig_domain(x)?;
    Ok(x.asin())
}
