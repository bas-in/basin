//! Extended numeric functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! # What is here and why these sixteen
//!
//! The numeric arms `eval.rs` still held after the wave-15 slice
//! ([`crate::funcs::num_fns`]) took the magnitude-and-rounding family:
//!
//! * **the `numeric` shaping trio** — `round`, `sign` and `trunc` on
//!   `numeric`, both arities where there are two. `num_fns`'s module doc names
//!   these explicitly as *deliberately left behind*, because they share
//!   `decimal_round_value` / `decimal_trunc_value` / `pow10` with each other
//!   and porting a subset would duplicate those helpers across two live
//!   modules. They are one slice, and this is that slice: all five arms move
//!   together and the helpers come with them.
//! * **integer math** — `factorial`, and `gcd`/`lcm` at both widths.
//! * **`to_hex`** at both widths.
//! * **the measurement quartet** — `bit_length` and `octet_length` over
//!   `text` and `bytea`.
//!
//! `ceil(numeric)`/`ceiling(numeric)`/`floor(numeric)` (oids 1711, 2167 and
//! 1712) are NOT here. They use the same `pow10` helper, so on the
//! shared-helper argument above they belong with this slice — but they were
//! not on this port's list and inventing scope mid-port is how a `match` arm
//! and a registry entry end up both live. Named as the obvious next slice
//! rather than silently absorbed; `decimal_ceil`/`decimal_floor` and `pow10`
//! must therefore stay in `eval.rs` after this port (see the integration
//! notes in `numx_fns.integration.md`).
//!
//! # A port moves behaviour, it does not improve it
//!
//! Every implementation below is the `eval.rs` code, moved. Where it
//! disagrees with PostgreSQL 18.2 the disagreement is *preserved* and pinned
//! in a test that states both answers, because fixing a divergence inside a
//! port makes a later bisect blame the port. The three families that diverge
//! are called out in the per-function docs and collected in
//! `numx_fns.integration.md`; the headline is that **every `numeric` result
//! here comes back at the input's physical scale**, which PostgreSQL does not
//! do.
//!
//! # Measured against live PostgreSQL 18.2
//!
//! `postgres://pc@127.0.0.1:5432/postgres`, `PostgreSQL 18.2 (Homebrew) on
//! aarch64-apple-darwin24.6.0`. Every value quoted in this file was read off
//! that server this session.

use std::sync::Arc;

use arrow::compute::kernels::arity;
use arrow_array::types::{Decimal128Type, Int32Type};
// `Array`'s `len`/`iter`-adjacent methods are trait methods, not inherent
// ones, and `factorial` / `gcd` / `lcm` walk their inputs by iterator so a
// NULL row is skipped *before* a per-row error can be raised.
use arrow_array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, StringArray,
};
use arrow_schema::ArrowError;
use basin_pgtype::Oid;

use crate::eval::{downcast_array, EvalSession};
// `arg` lives in the template module because the template is where it was
// written. It is `pub(crate)` and family-agnostic, so it is reused here
// rather than copied.
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// Translate an arrow kernel failure into an [`ExecError`].
///
/// The third copy of `eval.rs`'s private `map_arrow` (after
/// [`crate::funcs::num_fns`]'s). Copied rather than shared because promoting
/// the original to `pub(crate)` is an edit to `eval.rs`, and this slice's
/// whole point is that it can land without one. It should be hoisted into a
/// `crate::funcs::error` module once a fourth family needs it — noted in the
/// integration document as a follow-up.
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

// ─── bit_length / octet_length ──────────────────────────────────────────────
//
// `bit_length` is exactly `8 * octet_length` — one function with a flag, as
// it was in `eval.rs`, because writing them separately invites the two to
// drift. Measured live, which is where the flag's only subtlety lives:
//
// ```text
//                    length | octet_length | bit_length
//   'héllo'               5 |            6 |         48
//   '😀'                   1 |            4 |         32
//   ''                    0 |            0 |          0
//   '\xdeadbeef'::bytea   -- |            4 |         32
// ```
//
// The first two rows are the reason `octet_length(text)` may NOT be routed
// through `eval.rs`'s `text_char_length`: `length` counts CHARACTERS and
// `octet_length` counts BYTES, and they disagree for every non-ASCII input.

/// The shared body of the two `text` overloads: byte length, optionally
/// times eight.
///
/// `s.len()` is Rust's byte length of a `str`, which is what PostgreSQL's
/// `octet_length(text)` returns — not `chars().count()`.
///
/// **Inherited panic path, unchanged by this port.** The `expect` fires only
/// for a string longer than `i32::MAX` bytes, which Arrow's 32-bit-offset
/// `StringArray` cannot hold in the first place, so it is unreachable through
/// this type. The `saturating_mul(8)` on the `bit_length` path is the real
/// edge: a string of more than 268,435,455 bytes saturates to `i32::MAX`
/// instead of overflowing. PostgreSQL's behaviour at that size is on the
/// NEEDS VERIFICATION list — a 256 MB literal was not constructed on the live
/// server for this port.
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

/// The `bytea` twin of [`text_byte_length`].
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

/// `octet_length(text) -> integer`, `pg_proc` oid 1374.
///
/// Measured: `octet_length('héllo')` is 6 where `length('héllo')` is 5, and
/// `octet_length('😀')` is 4 where `length('😀')` is 1.
///
/// `octet_length(character)` (oid 1375) is a different `pg_proc` row with no
/// arm in `eval.rs` and none here; it is not part of this slice.
pub struct OctetLengthText;

impl ScalarFunc for OctetLengthText {
    fn oid(&self) -> Oid {
        Oid(1374)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        text_byte_length(arg(args, 0, 1374)?, false)
    }
}

/// `bit_length(text) -> integer`, `pg_proc` oid 1811.
///
/// Exactly `8 * octet_length`. Measured: `bit_length('héllo')` is 48,
/// `bit_length('😀')` is 32, `bit_length('')` is 0.
pub struct BitLengthText;

impl ScalarFunc for BitLengthText {
    fn oid(&self) -> Oid {
        Oid(1811)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        text_byte_length(arg(args, 0, 1811)?, true)
    }
}

/// `octet_length(bytea) -> integer`, `pg_proc` oid 720.
///
/// Measured: `octet_length('\xdeadbeef'::bytea)` is 4 — the byte count, not
/// the length of the hex rendering.
pub struct OctetLengthBytea;

impl ScalarFunc for OctetLengthBytea {
    fn oid(&self) -> Oid {
        Oid(720)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        bytea_byte_length(arg(args, 0, 720)?, false)
    }
}

/// `bit_length(bytea) -> integer`, `pg_proc` oid 1810.
///
/// Measured: `bit_length('\xdeadbeef'::bytea)` is 32.
pub struct BitLengthBytea;

impl ScalarFunc for BitLengthBytea {
    fn oid(&self) -> Oid {
        Oid(1810)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        bytea_byte_length(arg(args, 0, 1810)?, true)
    }
}

// ─── to_hex ─────────────────────────────────────────────────────────────────
//
// Hexadecimal in the argument's own TWO'S COMPLEMENT, not a signed rendering
// with a minus sign. This is the whole difficulty, and it is invisible until
// a negative arrives. Measured live:
//
// ```text
//   to_hex(255)                      ff
//   to_hex(0)                        0
//   to_hex((-1)::int4)               ffffffff
//   to_hex((-2147483648)::int4)      80000000
//   to_hex(2147483647::int4)         7fffffff
//   to_hex((-1)::int8)               ffffffffffffffff
//   to_hex((-9223372036854775808))   8000000000000000
//   to_hex(9223372036854775807)      7fffffffffffffff
// ```
//
// `format!("{:x}", n)` on the *signed* value prints `-1`, so the cast to the
// unsigned type of the same width is the entire implementation — and the
// width matters: `-1` is eight `f`s at `int4` and sixteen at `int8`, so the
// two overloads may not share one body through a widening cast.
//
// Resolution note, not a bug in this function: `to_hex('42')` is AMBIGUOUS,
// on the live server as well as in Basin, because an unknown-typed literal
// cannot choose between the two overloads:
//
// ```text
// SELECT to_hex('42');
//   ERROR:  function to_hex(unknown) is not unique
//   HINT:  Could not choose a best candidate function. You might need to add
//          explicit type casts.
// ```
//
// So a call reaching either implementation below always arrived with an
// explicitly typed argument.

/// `to_hex(integer) -> text`, `pg_proc` oid 2089.
pub struct ToHexInt4;

impl ScalarFunc for ToHexInt4 {
    fn oid(&self) -> Oid {
        Oid(2089)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int32Array>(arg(args, 0, 2089)?, "integer")?;
        let out: StringArray = a
            .iter()
            .map(|v| v.map(|n| format!("{:x}", n as u32)))
            .collect();
        Ok(Arc::new(out))
    }
}

/// `to_hex(bigint) -> text`, `pg_proc` oid 2090. See [`ToHexInt4`]; the only
/// difference is the width of the unsigned cast, which is exactly what makes
/// `to_hex(-1)` sixteen `f`s here and eight there.
pub struct ToHexInt8;

impl ScalarFunc for ToHexInt8 {
    fn oid(&self) -> Oid {
        Oid(2090)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int64Array>(arg(args, 0, 2090)?, "bigint")?;
        let out: StringArray = a
            .iter()
            .map(|v| v.map(|n| format!("{:x}", n as u64)))
            .collect();
        Ok(Arc::new(out))
    }
}

// ─── factorial ──────────────────────────────────────────────────────────────

/// One more than the largest 38-digit value, the ceiling
/// `DataType::Decimal128(38, 0)` can hold. `i128` itself goes three digits
/// further (`1.7e38`), so an `i128` overflow check alone would admit values
/// Arrow then rejects. Copied from `eval.rs` with the arm it belongs to.
const DECIMAL128_LIMIT: i128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

/// `factorial(bigint) -> numeric`, `pg_proc` oid 1376.
///
/// Measured live:
///
/// ```text
/// factorial(0)   1
/// factorial(1)   1
/// factorial(20)  2432902008176640000
/// factorial(33)  8683317618811886495518194401280000000        (37 digits)
/// factorial(34)  295232799039604140847618609643520000000      (39 digits)
/// factorial(-1)  ERROR:  22003: factorial of a negative number is undefined
/// ```
///
/// A negative argument **errors**; it does not return NULL and does not
/// return 1.
///
/// **KNOWN DIVERGENCE — the overflow threshold. Inherited, unchanged.**
/// PostgreSQL's result is an arbitrary-precision `numeric`; Basin's is a
/// `Decimal128`, whose 38 digits run out at **33!**. So `factorial(34)` and
/// up raise [`ExecError::Overflow`] here while the server answers. The
/// server's own ceiling is far away and is `numeric`'s 131072-integral-digit
/// limit, not an `int8` one — bisected live this session:
///
/// ```text
/// SELECT length(factorial(32000)::text);   ->  130271
/// SELECT length(factorial(32200)::text);   ->  ERROR:  22003: value
///                                              overflows numeric format
/// ```
///
/// So the *shape* of the two answers agrees — both refuse rather than
/// truncate — and only the threshold differs: 34 here, somewhere in
/// (32000, 32200] there. Surfacing the ceiling is the right call; a silently
/// truncated factorial would be far worse than a refusal. Not fixed here
/// because fixing it means arbitrary-precision decimals, which is its own
/// program.
///
/// **KNOWN DIVERGENCE — the error class for a negative argument.** The server
/// raises SQLSTATE `22003` with the bare text `factorial of a negative number
/// is undefined` (verified with `VERBOSITY=verbose`). Basin raises
/// [`ExecError::TypeMismatch`] carrying that same sentence, which `Display`s
/// as `type mismatch: factorial of a negative number is undefined` and is not
/// a `22003`. Preserved rather than corrected: changing it means adding an
/// `ExecError` variant, which is an edit outside this slice.
///
/// A per-row error aborts the whole batch, and the loop returns on the FIRST
/// negative it reaches. NULL rows push `None` and never reach the body, so
/// invariants 2 and 3 hold on every non-erroring path.
pub struct Factorial;

impl ScalarFunc for Factorial {
    fn oid(&self) -> Oid {
        Oid(1376)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<Int64Array>(arg(args, 0, 1376)?, "bigint")?;
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
}

// ─── gcd / lcm ──────────────────────────────────────────────────────────────

/// `gcd(a, b)` for `int4`/`int8`, and the base [`pg_lcm_i64`] builds on.
///
/// PostgreSQL's own algorithm, because the *overflow* cases are where a
/// reimplementation goes wrong and they are not where they look. The `int4`
/// truth table over `{0, ±1, 3, INT_MIN, INT_MAX}` was re-measured live for
/// this port and every entry still holds:
///
/// | call | result |
/// |---|---|
/// | `gcd(0, 0)` | 0 — not an error, not NULL |
/// | `gcd(-12, 18)`, `gcd(12, -18)`, `gcd(-12, -18)` | 6 — the result is always NON-NEGATIVE, whatever the signs |
/// | `gcd(0, -5)` | 5 — also non-negative |
/// | `gcd(INT_MIN, 0)` | **ERROR** `22003 integer out of range` — the result would be `|INT_MIN|` |
/// | `gcd(INT_MIN, INT_MIN)` | **ERROR** — same reason |
/// | `gcd(INT_MIN, 1)`, `gcd(INT_MIN, -1)` | 1 — no error: the *result* fits |
/// | `gcd(BIGINT_MIN, -1)` | 1 — the `i64` twin, also fine |
///
/// `width_min` is the *type's* minimum, not the accumulator's: an `int4`
/// computation runs in `i64` so it needs `i32::MIN` passed in, or it would
/// only refuse at the `int8` boundary and silently answer `2147483648` for
/// `gcd(INT_MIN, 0)`.
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

/// `lcm(a, b)`. Measured live:
///
/// | call | result |
/// |---|---|
/// | `lcm(0, 5)` | 0 |
/// | `lcm(-4, 6)` | 12 — non-negative, like `gcd` |
/// | `lcm(INT_MIN, 0)` | **0, NOT an error** |
/// | `lcm(INT_MIN, 1)` | **ERROR** `22003 integer out of range` — `|INT_MIN|` again |
/// | `lcm(INT_MAX, 2)` | **ERROR** — the product does not fit |
/// | `lcm(BIGINT_MIN, 1)`, `lcm(BIGINT_MAX, 2)` | **ERROR** `bigint out of range` |
///
/// **`lcm(INT_MIN, 0)` is the trap.** An implementation that computes the gcd
/// before checking for zero raises where PostgreSQL answers 0 — which is why
/// the zero test is the first statement in this function and must stay there.
///
/// `(a / g) * b` rather than `(a * b) / g`: dividing first keeps the product
/// in range for every pair whose lcm is representable at all.
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

/// `gcd`/`lcm` over an `int4` column pair, computed in `i64` and range-checked
/// back to `i32`. See [`pg_gcd_i64`] on why the width has to be passed in.
fn gcd_lcm_i32(a: &ArrayRef, b: &ArrayRef, is_gcd: bool) -> Result<ArrayRef, ExecError> {
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

/// `gcd`/`lcm` over an `int8` column pair. See [`pg_gcd_i64`].
fn gcd_lcm_i64(a: &ArrayRef, b: &ArrayRef, is_gcd: bool) -> Result<ArrayRef, ExecError> {
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

/// `gcd(integer, integer) -> integer`, `pg_proc` oid 5044.
///
/// **KNOWN DIVERGENCE, inherited and unchanged**, shared by all four of these
/// and by [`crate::funcs::num_fns::AbsInt2`]: the server's overflow is
/// SQLSTATE `22003 integer out of range`, while Basin's
/// `ExecError::Overflow("integer")` `Display`s as `integer out of range` —
/// the right sentence — but reaches the client as `XX000`, because nothing
/// maps the variant to `22003`. Error-vs-success matches; the SQLSTATE does
/// not. The `int8` overloads carry the same gap and additionally say
/// `integer out of range` where the server says `bigint out of range`, since
/// [`pg_gcd_i64`]/[`pg_lcm_i64`] hardcode the operation label.
pub struct GcdInt4;

impl ScalarFunc for GcdInt4 {
    fn oid(&self) -> Oid {
        Oid(5044)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        gcd_lcm_i32(arg(args, 0, 5044)?, arg(args, 1, 5044)?, true)
    }
}

/// `gcd(bigint, bigint) -> bigint`, `pg_proc` oid 5045. See [`GcdInt4`].
pub struct GcdInt8;

impl ScalarFunc for GcdInt8 {
    fn oid(&self) -> Oid {
        Oid(5045)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        gcd_lcm_i64(arg(args, 0, 5045)?, arg(args, 1, 5045)?, true)
    }
}

/// `lcm(integer, integer) -> integer`, `pg_proc` oid 5046. See [`pg_lcm_i64`]
/// for the measured truth table, and [`GcdInt4`] for the SQLSTATE gap.
pub struct LcmInt4;

impl ScalarFunc for LcmInt4 {
    fn oid(&self) -> Oid {
        Oid(5046)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        gcd_lcm_i32(arg(args, 0, 5046)?, arg(args, 1, 5046)?, false)
    }
}

/// `lcm(bigint, bigint) -> bigint`, `pg_proc` oid 5047. See [`pg_lcm_i64`].
pub struct LcmInt8;

impl ScalarFunc for LcmInt8 {
    fn oid(&self) -> Oid {
        Oid(5047)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        gcd_lcm_i64(arg(args, 0, 5047)?, arg(args, 1, 5047)?, false)
    }
}
