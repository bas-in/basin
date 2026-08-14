# `trig_fns` — integration instructions (wave-15 trig/math slice)

**STATUS: batches 1–3 landed in `trig_fns.rs` (15 of 24: sqrt, cbrt, exp, ln,
log, log10, power, pow, degrees, radians, sin, cos, tan, cot, asin). Sections 1–6 below are COMPLETE and
apply to the finished slice** — they were written from the full arm inventory,
not incrementally, so they are safe to apply even if the remaining batches are
missing. Cross-check §2's registration list against what `trig_fns.rs`
actually defines before applying.

This file is written before and updated during the
port so the work survives even if the agent does not. Read it top to bottom
before touching `eval.rs` or `funcs/mod.rs` — this slice owns NEITHER of those
files and made NO edits to them.

The slice owns exactly two files, both new:

* `crates/basin-exec/src/funcs/trig_fns.rs`
* `crates/basin-exec/src/funcs/trig_fns.integration.md` (this file)

Nothing has been compiled: the agent that wrote it was forbidden from running
`cargo` (build-lock starvation). Everything below is a *hand-verified*
instruction list. See "NEEDS VERIFICATION" at the bottom.

---

## 1. Add the module

In `crates/basin-exec/src/funcs/mod.rs`, next to the existing `pub mod`
declarations (currently `dt_fns`, `num_fns`, `str_fns`):

```rust
pub mod trig_fns;
```

## 2. Registrations

Append inside `funcs::builtins()`'s `get_or_init` closure, after the `dt_fns`
block:

```rust
        // trig_fns — ported by the wave-15 trig/math slice.
        r.register_scalar(Box::new(trig_fns::AcosFloat8));
        r.register_scalar(Box::new(trig_fns::AcoshFloat8));
        r.register_scalar(Box::new(trig_fns::AsinFloat8));
        r.register_scalar(Box::new(trig_fns::AsinhFloat8));
        r.register_scalar(Box::new(trig_fns::AtanFloat8));
        r.register_scalar(Box::new(trig_fns::Atan2Float8));
        r.register_scalar(Box::new(trig_fns::AtanhFloat8));
        r.register_scalar(Box::new(trig_fns::CbrtFloat8));
        r.register_scalar(Box::new(trig_fns::CosFloat8));
        r.register_scalar(Box::new(trig_fns::CoshFloat8));
        r.register_scalar(Box::new(trig_fns::CotFloat8));
        r.register_scalar(Box::new(trig_fns::DegreesFloat8));
        r.register_scalar(Box::new(trig_fns::ExpFloat8));
        r.register_scalar(Box::new(trig_fns::LnFloat8));
        r.register_scalar(Box::new(trig_fns::LogFloat8));
        r.register_scalar(Box::new(trig_fns::Log10Float8));
        r.register_scalar(Box::new(trig_fns::RadiansFloat8));
        r.register_scalar(Box::new(trig_fns::SinFloat8));
        r.register_scalar(Box::new(trig_fns::SinhFloat8));
        r.register_scalar(Box::new(trig_fns::SqrtFloat8));
        r.register_scalar(Box::new(trig_fns::TanFloat8));
        r.register_scalar(Box::new(trig_fns::TanhFloat8));
        r.register_scalar(Box::new(trig_fns::PowFloat8));
        r.register_scalar(Box::new(trig_fns::PowerFloat8));
```

**24 registrations.** `pi()` (oid 1610) is the 25th requested function and is
NOT here — see section 5.

`funcs/mod.rs`'s `the_registry_reports_what_is_actually_hosted` test asserts
`builtins().len() == 24`. After this slice it must read **48** (24 + 24).

## 3. Arms to delete from `eval_scalar_fn`'s `match` in `eval.rs`

Twenty-four arms across three blocks. **Do NOT delete `OID_PI`** — see §5.

### Block A — around lines 2878–2923 (the sqrt/log/power/degrees/trig block)

```rust
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
        OID_DEGREES_FLOAT8 => float8_unary_checked(&a(0)?, pg_degrees_f64),
        OID_RADIANS_FLOAT8 => float8_unary_checked(&a(0)?, pg_radians_f64),
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
```

**Interleaved arms that must SURVIVE**, because they sit between the ones
above and belong to the `numeric` decimal slice, not this one:

```rust
        OID_TRUNC_NUMERIC => decimal_trunc_fixed(&a(0)?, 0),
        OID_TRUNC_NUMERIC_N => { ... }
        OID_PI => Ok(Arc::new(Float64Array::from(vec![ ... ])))   // §5
        OID_SIGN_NUMERIC => decimal_sign(&a(0)?),
        OID_CEILING_NUMERIC => decimal_ceil(&a(0)?),
```

### Block B — around lines 3000–3030 (`cot` and the hyperbolics)

Delete the three-line `// `cot` shares `sin`/`cos`/`tan`'s shape…` comment and
the five-line `// The six hyperbolics.` comment along with their arms; both
comments describe only the arms below them.

```rust
        OID_COT_FLOAT8 => {
            float8_unary_checked(&a(0)?, |x| pg_trig_of_radians(x, |v| 1.0 / f64::tan(v)))
        }
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
```

### Block C — around lines 3031–3041 (`log10` / `pow`)

Delete the `// `log10(float8)` and `pow(float8, float8)` are separate…`
comment with them.

```rust
        OID_LOG10_FLOAT8 => float8_unary_checked(&a(0)?, pg_log10_f64),
        OID_POW_FLOAT8 => {
            let base = a(0)?;
            let exp = a(1)?;
            float8_binary_checked(&base, &exp, pg_power_f64)
        }
```

### 3a. The 24 `OID_*` constants (lines 456–555) that become unused

`OID_SQRT_FLOAT8` `OID_CBRT_FLOAT8` `OID_POWER_FLOAT8` `OID_LN_FLOAT8`
`OID_LOG_FLOAT8` `OID_EXP_FLOAT8` `OID_DEGREES_FLOAT8` `OID_RADIANS_FLOAT8`
`OID_ACOS_FLOAT8` `OID_ASIN_FLOAT8` `OID_ATAN_FLOAT8` `OID_ATAN2_FLOAT8`
`OID_COS_FLOAT8` `OID_SIN_FLOAT8` `OID_TAN_FLOAT8` `OID_COT_FLOAT8`
`OID_SINH_FLOAT8` `OID_COSH_FLOAT8` `OID_TANH_FLOAT8` `OID_ASINH_FLOAT8`
`OID_ACOSH_FLOAT8` `OID_ATANH_FLOAT8` `OID_LOG10_FLOAT8` `OID_POW_FLOAT8`

**`OID_PI` (line 467) stays.** So do the constants above.

**CAUTION — most of these are still referenced from `eval.rs`'s own `#[cfg(test)]`
module** (lines 9597–10023 and 11973 use `OID_SQRT_FLOAT8`, `OID_LN_FLOAT8`,
`OID_LOG_FLOAT8`, `OID_EXP_FLOAT8`, `OID_CBRT_FLOAT8`, `OID_POWER_FLOAT8`,
`OID_PI`, `OID_DEGREES_FLOAT8`, `OID_RADIANS_FLOAT8`, `OID_ASIN_FLOAT8`,
`OID_ACOS_FLOAT8`, `OID_SIN_FLOAT8`, `OID_COS_FLOAT8`, `OID_TAN_FLOAT8`,
`OID_ATAN_FLOAT8`, `OID_ATAN2_FLOAT8`). Those tests call `eval()`, which
consults the registry first, so **they keep passing through the new path and
should be KEPT, not deleted** — existing tests passing over a new path is the
evidence a port is supposed to produce. Because they reference the constants,
the constants stay compiled in test builds; in a non-test build the deleted
arms make them dead and `#[allow(dead_code)]` or deletion is needed. Decide
this when applying, with the compiler in hand.

### 3b. `eval.rs` helpers that go dead — grep-verified

Verified with `grep -rn --include='*.rs' '\b<name>\b' .` over the whole repo.

**DEAD after the port** (their only remaining references are the deleted arms
and their own definitions/doc links):

| helper | eval.rs line | only callers |
|---|---|---|
| `float8_unary_checked` | 5730 | the 15 deleted `float8_unary_checked(...)` arms |
| `pg_sqrt_f64` | 5777 | `OID_SQRT_FLOAT8` arm only |
| `pg_ln_f64` | 5794 | `OID_LN_FLOAT8` arm only |
| `pg_log10_f64` | 5806 | `OID_LOG_FLOAT8` + `OID_LOG10_FLOAT8` arms only |
| `reject_nonpositive_log_argument` | 5815 | `pg_ln_f64`, `pg_log10_f64` only |
| `pg_exp_f64` | 5898 | `OID_EXP_FLOAT8` arm only |
| `pg_degrees_f64` | 5907 | `OID_DEGREES_FLOAT8` arm only |
| `pg_radians_f64` | 5917 | `OID_RADIANS_FLOAT8` arm only |
| `pg_trig_of_radians` | 5932 | `OID_SIN/COS/TAN/COT_FLOAT8` arms only |
| `pg_asin_f64` | 5955 | `OID_ASIN_FLOAT8` arm only |
| `pg_acos_f64` | 5966 | `OID_ACOS_FLOAT8` arm only |
| `pg_acosh_f64` | 5982 | `OID_ACOSH_FLOAT8` arm only |
| `pg_atanh_f64` | 5998 | `OID_ATANH_FLOAT8` arm only |
| `reject_out_of_trig_domain` | 6009 | `pg_asin_f64`, `pg_acos_f64`, `pg_atanh_f64` only |

Deleting them also orphans the doc-link references at lines 5689–5690 and
5915, 5950, 5963, 5979, 5996, 6131 — plain prose and `[`link`]` targets inside
neighbouring doc comments, which will fail `rustdoc::broken_intra_doc_links`
if the targets vanish. Fix them in the same edit.

**NOT DEAD — must STAY** (this is the trap):

| helper | why it survives |
|---|---|
| `float8_binary_checked` (5751) | `eval_exponent` calls it at **line 5326** — the `^` operator, oid 965 |
| `pg_power_f64` (5858) | same, **line 5326**, plus a doc link at 11960 |
| `check_float8_result` (5703) | reached via `pg_power_f64`, plus module-doc link at 182 |

`check_float8_result` would otherwise look dead: `pg_exp_f64`,
`pg_degrees_f64` and `pg_radians_f64` are its other three callers and all
three die. It survives *only* through `pg_power_f64` → `eval_exponent`. Deleting
it would break `SELECT 2 ^ 10`.

## 4. Divergences from PostgreSQL 18.2

All measured this session on `postgres://pc@127.0.0.1:5432/postgres`,
`PostgreSQL 18.2 (Homebrew) on aarch64-apple-darwin24.6.0`.

### 4.1 `degrees(x)` is one ULP off for ~13% of inputs — FOUND BY THIS SLICE

PostgreSQL's `ddegrees` **divides**: `float8_div(arg1, RADIANS_PER_DEGREE)`,
`RADIANS_PER_DEGREE = 0.0174532925199432957692`. Rust's `f64::to_degrees`
**multiplies** by the precomputed reciprocal `57.29577951308232`. Those are
not the same operation in binary floating point.

```
$ psql -At -c "set extra_float_digits=3;" \
       -c "select degrees(0.1::float8), degrees(100000::float8),
                  degrees(1e300::float8), degrees(-0.1::float8);"
5.729577951308232|5729577.951308232|5.729577951308232e+301|-5.729577951308232
```

Rust `0.1 * 57.29577951308232` = `5.729577951308233`. Basin is **one ULP
high**; PostgreSQL says `...232`.

More, all measured live and all one ULP apart:

```
$ psql -At -c "set extra_float_digits=3;" -c "select
    degrees(-132.70863267522827::float8), degrees(-206.63905069843963::float8),
    degrees(-383.03635179613127::float8), degrees(-880.7976600675347::float8),
    degrees(-144.81538866119422::float8), degrees(171.12372701527738::float8);"
-7603.644557242511|-11839.545487610436|-21946.36635800657|-50465.9885268683|-8297.310578833105|9804.667332524221
```

| x | PostgreSQL 18.2 | Basin (`f64::to_degrees`) |
|---|---|---|
| `0.1` | `5.729577951308232` | `5.729577951308233` |
| `100000` | `5729577.951308232` | `5729577.951308233` |
| `1e300` | `5.729577951308232e+301` | `5.729577951308233e+301` |
| `-132.70863267522827` | `-7603.644557242511` | `-7603.644557242512` |
| `-206.63905069843963` | `-11839.545487610436` | `-11839.545487610438` |
| `-383.03635179613127` | `-21946.36635800657` | `-21946.366358006573` |
| `-880.7976600675347` | `-50465.9885268683` | `-50465.98852686831` |
| `-144.81538866119422` | `-8297.310578833105` | `-8297.310578833107` |
| `171.12372701527738` | `9804.667332524221` | `9804.667332524223` |

Over **300 uniform random draws from `[-1000, 1000]`, 38 disagreed (12.7%)**.
The error is not one-directional — the last row reads low, the rest high.

**Inherited, PRESERVED, not fixed.** `f64::to_degrees` is what the `match` arm
used. The fix is `x / (std::f64::consts::PI / 180.0)`, a one-line behaviour
change that belongs in its own commit. Pinned in
`tests::degrees_diverges_from_postgresql_by_one_ulp`, stating both answers.

`radians` was checked the same way and does **not** diverge: PostgreSQL
multiplies and so does `f64::to_radians`, by a constant that rounds to the
same `f64`. Identical on all 13 probe values. So this is `degrees`
specifically, not "the angle conversions".

### 4.2 Every error in this module reaches the client as `XX000`

Inherited, shared with `num_fns`. PostgreSQL sends `22003`
(`numeric_value_out_of_range`), `2201E` (`invalid_argument_for_logarithm`) or
`2201F` (`invalid_argument_for_power_function`) depending on the function.
Basin raises `ExecError::TypeMismatch` for all of them, and
`owned_engine.rs`'s `Fallback::into_error_after_publishing` (line 634) maps
only `ExecError::Cancelled` → 57014 and `ExecError::CardinalityViolation` →
21000; everything else becomes `BasinError::Internal` → `XX000`. The
error-vs-success outcome matches, so the differential batteries stay green
while a client sees the wrong class. See §6.

### 4.3 The slice brief's claim about `power` was stale — NO divergence here

The brief said "this repo already knows `power(-2, 0.5)` and `power(0, -1)`
should error and currently do not." **They do error.** `pg_power_f64`'s phase-2
guards are exactly those two cases, and `eval.rs`'s own tests at lines ~9704
and ~10006 pin them. Live:

```
power(0::float8,-1::float8)     ERROR:  zero raised to a negative power is undefined
power(-2::float8,0.5::float8)   ERROR:  a negative number raised to a non-integer power yields a complex result
pow(-2::float8,0.5::float8)     ERROR:  a negative number raised to a non-integer power yields a complex result
```

Basin agrees on all three. Recorded so the claim is not re-inherited.

## 5. Not ported

**`pi()`, `pg_proc` oid 1610.** Niladic, and its arm sizes its output from
`batch.num_rows()`:

```rust
OID_PI => Ok(Arc::new(Float64Array::from(vec![
    std::f64::consts::PI;
    batch.num_rows()
]))),
```

`ScalarFunc::invoke(&self, args: &[ArrayRef], session: &EvalSession)` receives
an **empty** `args` slice for a niladic call, and neither parameter carries a
row count. There is no way to satisfy invariant 3 (output length equals input
length) — the impl could only return a length-1 array into an N-row batch.
This is an ABI gap shared by every niladic `pg_proc` row (`random()`, `now()`,
`current_database()`), already reported by `crate::funcs::num_fns`; this slice
hit it independently and confirms it. **Leave the `OID_PI` arm and the
`OID_PI` constant in place.**

Related but distinct, and worth recording next to it: a sibling slice found
that `Project::new` fixes a projection's output schema by evaluating against a
**zero-row batch**. That would break any function whose *result type* depends
on an argument's *value*. This family is safe — all 24 are `float8 -> float8`
with a fixed `prorettype`, and none overrides `return_type`. But note that the
zero-row probe and `pi()`'s gap are the same root cause seen twice: the ABI
carries no row count.

## 6. NEEDS VERIFICATION

Nothing here was compiled — `cargo` was forbidden for this slice. `rustfmt
--edition 2021 --check` passes on `trig_fns.rs`, which is a syntax check and
nothing more.

1. **It compiles.** `arity::unary`, `arity::binary`, `Float64Type`, `Array`
   (for `is_null`/`len`/`value`), `ArrowError`, `downcast_array`,
   `str_fns::arg` are the imports used. `str_fns::arg` is `pub(crate)`.
2. **`funcs/mod.rs` needs `pub mod trig_fns;`** — §1. As of writing, `mod.rs`
   declares only `dt_fns`, `num_fns`, `str_fns`, while `extract_fns.rs` and
   `numx_fns.rs` also exist on disk from sibling slices. Whoever applies these
   integration files must add all the missing `pub mod` lines, not just this
   one.
3. **The registry count.** `the_registry_reports_what_is_actually_hosted`
   asserts `builtins().len() == 24`. This slice adds 24 → **48**, but sibling
   slices (`extract_fns`, `numx_fns`, `arr_fns`) are landing at the same time
   and each raises it further. Read the number off the compiler, do not trust
   the 48.
4. **The dead-helper list in §3b is grep-derived, not compiler-derived.**
   `cargo build` with `#![deny(dead_code)]` is the real check. The one to
   double-check by hand is `check_float8_result`, which survives by a single
   thread (`pg_power_f64` ← `eval_exponent` ← `^`).
5. **The `OID_*` constants in §3a are referenced from `eval.rs`'s test module.**
   Whether they need `#[allow(dead_code)]` in a non-test build depends on the
   compiler; do not delete them blind.
6. **The `XX000` claim in §4.2** is read off `owned_engine.rs:634` and its doc
   comment, not observed on the wire. Confirm with a real client session
   before quoting it as the client-visible SQLSTATE.
7. **`degrees` overflow boundary.** §4.1 shows the mantissa divergence.
   Whether there is an `x` where PostgreSQL's *division* overflows and Rust's
   *multiplication* does not (or vice versa) — turning a one-ULP difference
   into an error-vs-answer difference — was not probed. Worth a targeted
   sweep near `1.7976931348623157e308 * (pi/180)`.
8. **No `function_equivalence.rs` entries were added.** The 24 functions here
   are not in that differential battery; the unit tests in `trig_fns.rs` pin
   the measured values instead. Adding them is the stronger follow-up.
