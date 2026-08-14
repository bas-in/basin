//! Function-level Postgres equivalence harness for every scalar function
//! `crates/basin-exec/src/eval.rs`'s `eval_scalar_fn` actually implements.
//!
//! # Why this exists
//!
//! Basin claims Postgres compatibility. Before this file, the evidence for
//! that claim was 79 hand-written differential tests plus unit tests that
//! assert what the author *decided* was correct — not a measurement against
//! the thing itself. Every real bug found in this migration so far came from
//! mechanically diffing Basin against a live server over an exhaustive input
//! set (a wrong `typcategory` that invented 37 casts, seven `pg_catalog`
//! relations with mispositioned columns, four window function OIDs shifted
//! by an overload block) — never from a test someone thought up by hand.
//! This harness applies that same method to `eval.rs`'s scalar functions
//! specifically: enumerate what is implemented, batter it with edge-case
//! inputs, and diff every result against PostgreSQL 18 cell by cell.
//!
//! # Scope
//!
//! Every OID `eval_scalar_fn`'s `match` recognizes, read directly from that
//! `match` (not from `basin_pgtype::func::FUNCS`, which tabulates several
//! rows — `sqrt(numeric)`, `concat_ws`, `substring`, `position`, `mod` — that
//! have no arm in `eval_scalar_fn` at all and fall back to DataFusion; see
//! that table's own module docs). `func.rs` is used only for each covered
//! OID's real name and argument/return `pg_proc` types, to build the
//! equivalent SQL call. 52 OIDs are covered, matching `eval_scalar_fn`'s
//! `match` arm count exactly:
//!
//!   - String: lower, upper, length, substr/2, substr/3, left, right,
//!     btrim/1, btrim/2, ltrim/1, ltrim/2, rtrim/1, rtrim/2, replace, strpos,
//!     concat
//!   - Integer: abs(int2/int4/int8)
//!   - Float8: sqrt, cbrt, power, ln, log, exp, trunc, degrees, radians, pi,
//!     sign, acos, asin, atan, atan2, cos, sin, tan, round, ceil, floor,
//!     ceiling, abs
//!   - Real: abs
//!   - Numeric: abs, round/1, round/2, ceil, floor, trunc/1, trunc/2, sign,
//!     ceiling
//!
//! # The battery
//!
//! Every argument position is drawn from a battery keyed to its Postgres
//! type (see `*_battery` below), always including: NULL; zero/one/minus-one
//! (integers); the type's MIN/MAX (`i32::MIN` is the classic overflow trap,
//! and it *is* exercised here — see `abs(int4)`); for floats: `0.0`, `-0.0`,
//! `inf`, `-inf`, `NaN`, and a subnormal; for text: empty, a single ASCII
//! char, multibyte UTF-8, an embedded-NUL edge, and whitespace-only; for
//! numeric: half-integer ties (`0.5`, `1.5`, `2.5`, ...) that distinguish
//! half-to-even from half-away-from-zero rounding.
//!
//! One- and two-argument functions get the **full cross product** of their
//! argument batteries. Three-argument functions (`substr/3`, `replace`) get
//! a **one-at-a-time** design instead — a fixed baseline call with each
//! battery value substituted into one position at a time — because a full
//! cross of three ~10-value batteries is >900 round trips per function for
//! marginal extra coverage; OAT still exercises every edge value in every
//! position at least once.
//!
//! # What "equivalent" means
//!
//! Three things are compared, and a mismatch in any one is a divergence:
//! value equality, NULL-ness, and error-vs-success. The last is the one a
//! results-only differential test misses entirely — this project has already
//! found two real bugs of exactly that shape (`"char"` casts that should
//! have errored and didn't; `SELECT DISTINCT ... ORDER BY <expr>` that
//! should have errored and didn't). So `NULL` args, embedded-NUL text, and
//! `i32::MIN`-class overflow triggers are in the battery specifically to
//! provoke a Postgres ERROR and check what Basin does at the same input —
//! not just to see what value comes back. For the embedded-NUL case, "what
//! Basin does" is layered, and the next section is about that.
//!
//! # The NUL byte: one defect, one layer, and what this battery can see
//!
//! Arrow's `Utf8` accepts a NUL byte that PostgreSQL's `text` rejects with
//! `22021 character_not_in_repertoire`. That mismatch used to be reported
//! here as **101 divergences across 16 OIDs** — 12 distinct function names
//! (`concat`, `btrim`, `ltrim`, `rtrim`, `strpos`, `substr`, `left`,
//! `right`, `replace`, `lower`, `upper`, `length`), several of them at two
//! arities — every one of them the same sentence: *basin SUCCEEDED but
//! postgres ERRORED*. It was never 101 defects. It was one input-domain
//! defect seen 101 times, and 101 is what it looks like when a whole class
//! of divergence is counted per call site. (101 is also, measured, exactly
//! how many of the 1,492 call sites carry a NUL at all: every one of them
//! diverged, and nothing else did.)
//!
//! **The defect is fixed, and not here.** It is fixed at the boundary where
//! a client's bytes first become a Basin value:
//!
//!   * `a4749d4e` — `basin-router`'s `decode_param_text` rejects a NUL for
//!     *every* declared type (in text format PostgreSQL's encoding check
//!     runs before type dispatch, so a NUL in an `int4` parameter gives
//!     `22021`, not `22P02`); `decode_param_binary` rejects per-type with
//!     `bytea` deliberately still accepting; COPY rejects for text columns.
//!   * `6d8f38db` — `basin-rest`'s `quote_sql_string` refuses the same byte
//!     as HTTP 400, reusing PostgreSQL's own wording.
//!
//! Sixteen copies of that guard inside `eval_scalar_fn` — one per affected
//! arm — would have been the same check in the wrong layer, sixteen times.
//!
//! **This battery calls `eval(&expr, &batch)` directly.** There is no wire
//! and no `decode_param` between the battery and the function under test —
//! that is the point of it, and it is why it can reach `eval` arms and
//! physical layouts (a `Decimal128(20, 4)` column, `ndigits = i32::MIN`)
//! that are awkward to provoke through a client. The consequence is that
//! its NUL count measures a path no client can reach, and would keep
//! reporting 101 however correct Basin became.
//!
//! So the battery does not bind through the wire (that would turn a
//! unit-level `eval` differential into a second end-to-end one, of which
//! this repository already has several), and it does not carry a
//! hand-written exemption list (a list is a place where unrelated failures
//! go to be forgotten). It **attributes**, mechanically, per call site:
//!
//!   * the input to that call site actually contains a `0x00` byte, **and**
//!   * PostgreSQL's refusal carried SQLSTATE `22021` on this run, **and**
//!   * Basin's `eval` returned a value rather than an error.
//!
//! A divergence matching all three is reported as *attributed to the wire
//! layer* and does not fail the run. Anything else — a value mismatch on
//! NUL input, Basin erroring where Postgres succeeded, a `22021` at a call
//! site with no NUL in it, or any divergence at all on non-NUL input — is
//! **unexplained**, and unexplained is what fails the run. Nothing can be
//! added to the attributed set by editing a list; a case qualifies only by
//! having that exact measured shape, so the moment `eval`'s behaviour or
//! PostgreSQL's changes, the case leaves the set and fails the run.
//!
//! Two further checks make the attribution falsifiable rather than a claim:
//!
//!   1. **Postgres's rejection is pinned, not assumed.** Every call site
//!      carrying a NUL must be refused by the live server with `22021`. If
//!      one is ever accepted, the premise of the attribution is wrong and
//!      the run fails.
//!   2. **The deferral target must still exist.** The attribution says the
//!      check lives one layer up and is tested there. This battery cannot
//!      run `basin-router` (that crate depends on this one), so it verifies
//!      the companion harness — `crates/basin-router/tests/nul_byte_rejection.rs`,
//!      which drives the v3 protocol over real TCP — is still present and
//!      still pins SQLSTATE `22021`. If it is deleted or gutted, this
//!      battery stops attributing and reports every NUL case as unexplained
//!      again, because there is no longer anywhere for them to be explained.
//!
//! The `nul_embedded` inputs stay in the batteries. PostgreSQL's rejection
//! of them is real behaviour worth pinning, and check (1) is now the thing
//! pinning it.
//!
//! Numeric (`Decimal128`) results are compared by parsed value, not by exact
//! text: `eval.rs`'s `round`/`ceil`/`floor`/`trunc`/`sign` on `numeric`
//! deliberately keep the *input's* physical scale rather than narrowing the
//! way real Postgres does (see `decimal_round_value`'s doc comment), so
//! `round(2.5::numeric(20,4))` legitimately prints `3.0000` on one side and
//! `3` on the other — same value, different scale. That is not a divergence
//! this harness is trying to catch, so both sides are normalized (trailing
//! fractional zeros stripped) before comparing. Float results are compared
//! with a relative epsilon (`1e-9`) for the same reason `tests/integration`'s
//! `differential_pg.rs` does: two different libm implementations computing
//! the same transcendental function can differ in the last ULP without
//! either being wrong.
//!
//! # Panics count as divergences, not harness failures
//!
//! `eval.rs`'s decimal `round`/`trunc` compute `scale - ndigits` as a plain
//! `i32` subtraction (see `decimal_round_value`/`decimal_trunc_value`). With
//! `ndigits = i32::MIN` that subtraction overflows and **panics** in a debug
//! build — confirmed by running this harness. A naive test would either
//! crash the whole binary on the first such case or need to know to avoid
//! it. Instead, every Basin call goes through `catch_unwind`: a panic is
//! recorded as `Outcome::Error("PANIC: ...")` like any other failure, so it
//! shows up as a normal divergence entry (Basin panics where Postgres
//! cleanly returns an error) instead of aborting the run before the metric
//! can be printed. The default panic hook is swapped for a silent one for
//! the duration so expected panics do not spam stderr.
//!
//! # Skip behavior
//!
//! Same convention as `crates/basin-pgcatalog/tests/catalog_fidelity.rs` and
//! `crates/basin-engine/tests/differential_pg.rs`: reads `PG_DIFF_TEST_DSN`
//! and skips cleanly (exit 0) when unset, printing an explicit banner so a
//! skip cannot be mistaken for "compared everything and it matched" — see
//! `scripts/check-differential.sh`'s module docs for why that distinction is
//! load-bearing. `harness = false` (see `Cargo.toml`) so this banner is never
//! captured-and-hidden by libtest the way a normal `#[test]`'s stdout is on
//! success.
//!
//! # Dependency placement
//!
//! `tokio-postgres` is a dev-dependency only (see `Cargo.toml`) — a Postgres
//! client must never reach `basin-exec`'s production dependency graph. `tokio`
//! itself is already a normal (non-dev) dependency of this crate.

use std::any::Any;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use basin_exec::eval::eval;
use basin_pgtype::Oid;
use basin_plan::{ColumnRef, Expr, FuncId};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

// ─────────────────────────────────────────────────────────────────────────
// Argument values and the battery generators
// ─────────────────────────────────────────────────────────────────────────

/// One test value for one argument position, tagged with the Postgres type
/// it must be bound/cast as on both sides.
#[derive(Clone, Debug)]
enum ArgVal {
    Int2(Option<i16>),
    Int4(Option<i32>),
    Int8(Option<i64>),
    Float4(Option<f32>),
    Float8(Option<f64>),
    Text(Option<String>),
    /// A `Decimal128` mantissa at the harness-wide [`NUM_SCALE`].
    Numeric(Option<i128>),
}

/// Precision/scale every `numeric` battery value and column uses. Arbitrary
/// but fixed, so every numeric call site in this file is directly
/// comparable — see the module docs on why the *result*'s scale still needs
/// normalizing even though every input scale is pinned.
const NUM_PRECISION: u8 = 20;
const NUM_SCALE: i8 = 4;

fn int2_battery() -> Vec<(&'static str, Option<i16>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("min", Some(i16::MIN)),
        ("max", Some(i16::MAX)),
    ]
}

fn int4_battery() -> Vec<(&'static str, Option<i32>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("min", Some(i32::MIN)),
        ("max", Some(i32::MAX)),
    ]
}

fn int8_battery() -> Vec<(&'static str, Option<i64>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("min", Some(i64::MIN)),
        ("max", Some(i64::MAX)),
    ]
}

/// `ndigits` battery for `round(numeric, int4)` / `trunc(numeric, int4)` —
/// includes `i32::MIN`/`i32::MAX` specifically to hit the overflow panic
/// described in the module docs.
fn ndigits_battery() -> Vec<(&'static str, Option<i32>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("two", Some(2)),
        ("minus_two", Some(-2)),
        ("min", Some(i32::MIN)),
        ("max", Some(i32::MAX)),
    ]
}

fn float8_battery() -> Vec<(&'static str, Option<f64>)> {
    vec![
        ("null", None),
        ("zero", Some(0.0)),
        ("neg_zero", Some(-0.0)),
        ("one", Some(1.0)),
        ("minus_one", Some(-1.0)),
        ("half", Some(0.5)),
        ("neg_half", Some(-0.5)),
        ("min", Some(f64::MIN)),
        ("max", Some(f64::MAX)),
        ("infinity", Some(f64::INFINITY)),
        ("neg_infinity", Some(f64::NEG_INFINITY)),
        ("nan", Some(f64::NAN)),
        ("subnormal", Some(f64::MIN_POSITIVE / 2.0)),
    ]
}

fn float4_battery() -> Vec<(&'static str, Option<f32>)> {
    vec![
        ("null", None),
        ("zero", Some(0.0)),
        ("neg_zero", Some(-0.0)),
        ("one", Some(1.0)),
        ("minus_one", Some(-1.0)),
        ("half", Some(0.5)),
        ("neg_half", Some(-0.5)),
        ("min", Some(f32::MIN)),
        ("max", Some(f32::MAX)),
        ("infinity", Some(f32::INFINITY)),
        ("neg_infinity", Some(f32::NEG_INFINITY)),
        ("nan", Some(f32::NAN)),
        ("subnormal", Some(f32::MIN_POSITIVE / 2.0)),
    ]
}

/// Text battery. `nul_embedded` is the one entry expected to make *Postgres*
/// error (confirmed live, and re-confirmed every run: `invalid byte sequence
/// for encoding "UTF8": 0x00`, SQLSTATE `22021`). `eval` itself does no such
/// validation and is not supposed to — the guard is at the wire boundary, one
/// layer up. See the module docs' NUL section for how the resulting
/// divergences are attributed rather than counted or exempted.
fn text_battery() -> Vec<(&'static str, Option<String>)> {
    vec![
        ("null", None),
        ("empty", Some(String::new())),
        ("single_char", Some("a".to_string())),
        ("multibyte_utf8", Some("héllo世界".to_string())),
        ("nul_embedded", Some("ab\u{0}cd".to_string())),
        ("whitespace_only", Some("   ".to_string())),
        ("whitespace_mixed", Some("\t \n".to_string())),
        ("leading_trailing_spaces", Some("  padded  ".to_string())),
        ("ordinary", Some("The quick brown fox".to_string())),
    ]
}

/// A smaller text battery for a "set of characters to trim" / "needle"
/// argument position — the full [`text_battery`] cross would be quadratic
/// for no extra coverage, since these positions are semantically a small
/// character set, not an arbitrary string. Keeps its own `nul_embedded` so
/// the byte is exercised in the *second* argument position too.
fn text_battery_small() -> Vec<(&'static str, Option<String>)> {
    vec![
        ("null", None),
        ("empty", Some(String::new())),
        ("space", Some(" ".to_string())),
        ("single_char", Some("o".to_string())),
        ("multibyte_utf8", Some("é".to_string())),
        ("nul_embedded", Some("x\u{0}y".to_string())),
    ]
}

/// `numeric` battery, as `Decimal128` mantissas at [`NUM_SCALE`]. Includes
/// `.5`/`.05` ties on both sides of zero — the values that distinguish
/// half-to-even (`round(float8)`) from half-away-from-zero (`round(numeric)`)
/// — plus the largest and smallest magnitudes [`NUM_PRECISION`] can hold.
fn numeric_battery() -> Vec<(&'static str, Option<i128>)> {
    let max_mantissa = 10i128.pow(NUM_PRECISION as u32) - 1;
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(SCALE_UNIT)),
        ("minus_one", Some(-SCALE_UNIT)),
        ("max", Some(max_mantissa)),
        ("min", Some(-max_mantissa)),
        ("half", Some(SCALE_UNIT / 2)),
        ("neg_half", Some(-SCALE_UNIT / 2)),
        ("one_half", Some(SCALE_UNIT + SCALE_UNIT / 2)),
        ("neg_one_half", Some(-(SCALE_UNIT + SCALE_UNIT / 2))),
        ("two_half", Some(2 * SCALE_UNIT + SCALE_UNIT / 2)),
        ("neg_two_half", Some(-(2 * SCALE_UNIT + SCALE_UNIT / 2))),
        ("tiny_tie", Some(5)), // 0.0005 — a tie one digit past NUM_SCALE
    ]
}

/// `10^NUM_SCALE`: the mantissa step corresponding to one whole unit.
const SCALE_UNIT: i128 = 10_000; // NUM_SCALE == 4

// ─────────────────────────────────────────────────────────────────────────
// Building the Basin side: RecordBatch + Expr::ScalarFn
// ─────────────────────────────────────────────────────────────────────────

fn build_batch(args: &[ArgVal]) -> (RecordBatch, Vec<Expr>) {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut exprs = Vec::new();

    if args.is_empty() {
        // Niladic functions (`pi()`) still need a batch with at least one
        // row — `eval_scalar_fn`'s PI arm reads `batch.num_rows()`, not any
        // argument, so a zero-column/zero-row batch would silently produce
        // zero output rows instead of one.
        fields.push(Field::new("_dummy", DataType::Int32, true));
        columns.push(Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef);
    }

    for (i, a) in args.iter().enumerate() {
        let name = format!("a{i}");
        let (dt, col): (DataType, ArrayRef) = match a {
            ArgVal::Int2(v) => (DataType::Int16, Arc::new(Int16Array::from(vec![*v]))),
            ArgVal::Int4(v) => (DataType::Int32, Arc::new(Int32Array::from(vec![*v]))),
            ArgVal::Int8(v) => (DataType::Int64, Arc::new(Int64Array::from(vec![*v]))),
            ArgVal::Float4(v) => (DataType::Float32, Arc::new(Float32Array::from(vec![*v]))),
            ArgVal::Float8(v) => (DataType::Float64, Arc::new(Float64Array::from(vec![*v]))),
            ArgVal::Text(v) => (DataType::Utf8, Arc::new(StringArray::from(vec![v.clone()]))),
            ArgVal::Numeric(v) => {
                let arr = Decimal128Array::from(vec![*v])
                    .with_precision_and_scale(NUM_PRECISION, NUM_SCALE)
                    .expect("battery numeric value must fit NUM_PRECISION/NUM_SCALE");
                (
                    DataType::Decimal128(NUM_PRECISION, NUM_SCALE),
                    Arc::new(arr),
                )
            }
        };
        fields.push(Field::new(&name, dt, true));
        columns.push(col);
        exprs.push(Expr::Column(ColumnRef {
            relation: 0,
            index: (fields.len() - 1) as u16,
            name,
        }));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).expect("building a 1-row test batch");
    (batch, exprs)
}

fn basin_call(oid: u32, args: &[ArgVal]) -> Result<ArrayRef, String> {
    let (batch, exprs) = build_batch(args);
    let expr = Expr::ScalarFn {
        func: FuncId(Oid(oid)),
        args: exprs,
    };
    eval(&expr, &batch).map_err(|e| e.to_string())
}

/// [`basin_call`], guarded against a panic escaping and killing the whole
/// harness — see the module docs on why a panic is treated as just another
/// divergence-producing outcome.
fn safe_basin_call(oid: u32, args: &[ArgVal]) -> Result<ArrayRef, String> {
    let args = args.to_vec();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| basin_call(oid, &args))) {
        Ok(result) => result,
        Err(payload) => Err(format!("PANIC: {}", panic_message(&payload))),
    }
}

fn panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Result extraction (Basin) / comparison value model
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Val {
    I(i64),
    F(f64),
    /// Text as-is, or a normalized numeric string — see [`normalize_numeric`].
    S(String),
}

#[derive(Clone, Debug)]
enum Outcome {
    Value(Option<Val>),
    Error(String),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetKind {
    Int,
    Float,
    Text,
    Numeric,
}

fn extract_int(arr: &ArrayRef) -> Option<Val> {
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return (!a.is_null(0)).then(|| Val::I(a.value(0) as i64));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return (!a.is_null(0)).then(|| Val::I(a.value(0) as i64));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return (!a.is_null(0)).then(|| Val::I(a.value(0)));
    }
    panic!("expected an integer array, got {:?}", arr.data_type());
}

fn extract_float(arr: &ArrayRef) -> Option<Val> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return (!a.is_null(0)).then(|| Val::F(a.value(0)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return (!a.is_null(0)).then(|| Val::F(a.value(0) as f64));
    }
    panic!("expected a float array, got {:?}", arr.data_type());
}

fn extract_text(arr: &ArrayRef) -> Option<Val> {
    let a = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("expected a text array, got {:?}", arr.data_type()));
    (!a.is_null(0)).then(|| Val::S(a.value(0).to_string()))
}

fn extract_numeric(arr: &ArrayRef) -> Option<Val> {
    let a = arr
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap_or_else(|| panic!("expected a decimal array, got {:?}", arr.data_type()));
    (!a.is_null(0)).then(|| {
        Val::S(normalize_numeric(&format_decimal(
            a.value(0),
            a.scale() as i32,
        )))
    })
}

fn extract(kind: RetKind, arr: &ArrayRef) -> Option<Val> {
    match kind {
        RetKind::Int => extract_int(arr),
        RetKind::Float => extract_float(arr),
        RetKind::Text => extract_text(arr),
        RetKind::Numeric => extract_numeric(arr),
    }
}

/// Render a `Decimal128` mantissa at `scale` decimal places as plain
/// (never scientific) decimal text — the same convention Postgres's own
/// `numeric` text output uses, which is what makes [`normalize_numeric`]
/// comparable across both sides.
fn format_decimal(mantissa: i128, scale: i32) -> String {
    let neg = mantissa < 0;
    let abs = mantissa.unsigned_abs();
    let scale_u = scale.max(0) as usize;
    let digits = abs.to_string();
    let digits = if digits.len() <= scale_u {
        format!("{}{digits}", "0".repeat(scale_u + 1 - digits.len()))
    } else {
        digits
    };
    let split_at = digits.len() - scale_u;
    let (int_part, frac_part) = digits.split_at(split_at);
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(int_part);
    if scale_u > 0 {
        s.push('.');
        s.push_str(frac_part);
    }
    s
}

/// Strip trailing fractional zeros (and a bare trailing `.`) so two numeric
/// strings at different scales but the same value compare equal — see the
/// module docs on why `eval.rs` legitimately returns a different display
/// scale than real Postgres for `round`/`ceil`/`floor`/`trunc`/`sign`.
fn normalize_numeric(s: &str) -> String {
    let mut s = s.trim().to_string();
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() || s == "-0" || s == "-" {
        s = "0".to_string();
    }
    s
}

fn floats_equivalent(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff / scale < 1e-9
}

fn fmt_val(v: &Val) -> String {
    match v {
        Val::I(x) => x.to_string(),
        Val::F(x) => format!("{x}"),
        Val::S(x) => format!("{x:?}"),
    }
}

fn fmt_outcome(o: &Outcome) -> String {
    match o {
        Outcome::Value(Some(v)) => fmt_val(v),
        Outcome::Value(None) => "NULL".to_string(),
        Outcome::Error(e) => format!("ERROR[{e}]"),
    }
}

/// `None` if `basin`/`pg` are equivalent, else a human-readable reason.
fn compare(basin: &Outcome, pg: &Outcome) -> Option<String> {
    match (basin, pg) {
        (Outcome::Error(_), Outcome::Error(_)) => None,
        (Outcome::Error(_), Outcome::Value(_)) => Some(format!(
            "basin ERRORED but postgres SUCCEEDED — basin={}, postgres={}",
            fmt_outcome(basin),
            fmt_outcome(pg)
        )),
        (Outcome::Value(_), Outcome::Error(_)) => Some(format!(
            "basin SUCCEEDED but postgres ERRORED — basin={}, postgres={}",
            fmt_outcome(basin),
            fmt_outcome(pg)
        )),
        (Outcome::Value(None), Outcome::Value(None)) => None,
        (Outcome::Value(a), Outcome::Value(b)) => {
            let eq = match (a, b) {
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
                (Some(Val::I(x)), Some(Val::I(y))) => x == y,
                (Some(Val::F(x)), Some(Val::F(y))) => floats_equivalent(*x, *y),
                (Some(Val::S(x)), Some(Val::S(y))) => x == y,
                _ => false,
            };
            if eq {
                None
            } else {
                Some(format!(
                    "value mismatch — basin={}, postgres={}",
                    fmt_outcome(basin),
                    fmt_outcome(pg)
                ))
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The NUL byte: attribution, and the two checks that keep it falsifiable
// ─────────────────────────────────────────────────────────────────────────

/// The SQLSTATE PostgreSQL raises for a NUL byte in a value bound as text:
/// `character_not_in_repertoire`.
const NUL_SQLSTATE: &str = "22021";

/// The harness that owns the other half of the NUL story: it drives the v3
/// wire protocol over real TCP against a real `basin-router` and asserts the
/// `22021` refusal that `eval` (correctly) does not make. Path is relative to
/// this crate's `CARGO_MANIFEST_DIR`.
///
/// This battery cannot *call* that guard — `basin-router` depends on
/// `basin-exec`, so the dependency only runs one way — so it verifies the
/// companion still exists and still pins the same SQLSTATE. See
/// [`wire_guard_companion`] for exactly what that does and does not prove.
const WIRE_GUARD_COMPANION: &str = "../basin-router/tests/nul_byte_rejection.rs";

/// True if any argument at this call site actually carries a `0x00` byte.
///
/// Derived from the value, not from the battery *label*: a case qualifies for
/// NUL attribution because the byte is really there, so renaming or adding a
/// battery entry cannot smuggle a case into the attributed set.
///
/// Only `Text` can carry one. Every other `ArgVal` reaches Postgres as a
/// machine-typed parameter or, for `Numeric`, as [`format_decimal`] output —
/// digits, `-` and `.` only.
fn args_carry_nul(args: &[ArgVal]) -> bool {
    args.iter().any(|a| match a {
        ArgVal::Text(Some(s)) => s.contains('\0'),
        _ => false,
    })
}

/// True if `o` is a PostgreSQL error carrying SQLSTATE `code`.
///
/// Reads the bracketed code [`pg_err`] appends rather than matching message
/// wording: the wording is PostgreSQL's and is translatable, the SQLSTATE is
/// the stable contract. Only meaningful on the Postgres side — a Basin
/// `Outcome::Error` is a Rust error string with no SQLSTATE.
fn pg_sqlstate_is(o: &Outcome, code: &str) -> bool {
    match o {
        Outcome::Error(e) => e.ends_with(&format!("[{code}]")),
        Outcome::Value(_) => false,
    }
}

/// How a divergence is accounted for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Class {
    /// Basin's `eval` returned a value for an input containing a NUL byte
    /// that PostgreSQL refused with `22021`. Attributed to the wire layer,
    /// which is where the guard lives (`a4749d4e`) and where it is tested
    /// ([`WIRE_GUARD_COMPANION`]) — see the module docs. Reported, counted
    /// separately, and does not fail the run.
    NulBelowTheGuard,
    /// Everything else. A defect, or a change, in what `eval` computes.
    /// Fails the run.
    Unexplained,
}

/// Classify a divergence. All three conditions must hold for attribution:
/// the input really contains a NUL, PostgreSQL really refused it with
/// `22021` *on this run*, and Basin really succeeded. A NUL-bearing call site
/// that diverges any *other* way — a value mismatch, or Basin erroring where
/// Postgres succeeded — is unexplained, because the wire guard explains
/// exactly one shape and nothing else.
fn classify(args: &[ArgVal], basin: &Outcome, pg: &Outcome) -> Class {
    if args_carry_nul(args)
        && matches!(basin, Outcome::Value(_))
        && pg_sqlstate_is(pg, NUL_SQLSTATE)
    {
        Class::NulBelowTheGuard
    } else {
        Class::Unexplained
    }
}

/// Check that the companion wire harness still exists and still pins the NUL
/// SQLSTATE, returning a one-line description of what was found.
///
/// **What this proves:** that the file this battery defers its NUL cases to
/// is present, names SQLSTATE `22021`, and still contains rejection tests.
/// **What it does not prove:** that those tests pass — they run under
/// `cargo test -p basin-router`, not here. The point is narrower and still
/// worth having: an attribution that points at a deleted or hollowed-out
/// harness is not an attribution, and this battery must not keep quietly
/// discounting 101 divergences after its justification has been removed.
fn wire_guard_companion() -> Result<String, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(WIRE_GUARD_COMPANION);
    let shown = WIRE_GUARD_COMPANION.trim_start_matches("../");
    let src = std::fs::read_to_string(&path).map_err(|e| {
        format!("cannot read the wire-guard companion harness at crates/{shown}: {e}")
    })?;
    if !src.contains(NUL_SQLSTATE) {
        return Err(format!(
            "crates/{shown} no longer mentions SQLSTATE {NUL_SQLSTATE}"
        ));
    }
    let rejection_tests = src.matches("_with_nul_is_rejected").count();
    if rejection_tests < 3 {
        return Err(format!(
            "crates/{shown} contains {rejection_tests} `*_with_nul_is_rejected` test(s); \
             expected at least 3 (text-format param, binary-format param, COPY)"
        ));
    }
    Ok(format!(
        "crates/{shown} present, pins SQLSTATE {NUL_SQLSTATE}, \
         {rejection_tests} `*_with_nul_is_rejected` test(s)"
    ))
}

// ─────────────────────────────────────────────────────────────────────────
// The Postgres side
// ─────────────────────────────────────────────────────────────────────────

fn arg_cast(a: &ArgVal) -> &'static str {
    match a {
        ArgVal::Int2(_) => "::smallint",
        ArgVal::Int4(_) => "::int4",
        ArgVal::Int8(_) => "::int8",
        ArgVal::Float4(_) => "::real",
        ArgVal::Float8(_) => "::float8",
        ArgVal::Text(_) => "::text",
        // `::text::numeric`, not `::numeric`. `arg_param` binds this parameter
        // as a Rust `String` (see the comment there — tokio-postgres has no
        // numeric encoder without `rust_decimal`). A bare `$1::numeric` makes
        // Postgres infer the *parameter* type as numeric, and tokio-postgres
        // then refuses to encode a String into it: "error serializing
        // parameter 0". Going through `::text` pins the parameter to text —
        // which is what is actually being sent — and performs the numeric
        // conversion server-side, exactly as the binding comment intends.
        ArgVal::Numeric(_) => "::text::numeric",
    }
}

fn arg_param(a: &ArgVal) -> Box<dyn ToSql + Sync> {
    match a.clone() {
        ArgVal::Int2(v) => Box::new(v),
        ArgVal::Int4(v) => Box::new(v),
        ArgVal::Int8(v) => Box::new(v),
        ArgVal::Float4(v) => Box::new(v),
        ArgVal::Float8(v) => Box::new(v),
        ArgVal::Text(v) => Box::new(v),
        // Bound as text, then cast — tokio-postgres has no built-in numeric
        // type without pulling in `rust_decimal`, and casting Basin's own
        // canonical decimal text is exact (no float round-trip).
        ArgVal::Numeric(v) => Box::new(v.map(|m| format_decimal(m, NUM_SCALE as i32))),
    }
}

fn pg_err(e: &tokio_postgres::Error) -> String {
    match e.code() {
        Some(code) => format!("{e} [{}]", code.code()),
        None => e.to_string(),
    }
}

async fn pg_outcome(client: &Client, pg_name: &str, args: &[ArgVal], ret: RetKind) -> Outcome {
    let placeholders: Vec<String> = args
        .iter()
        .enumerate()
        .map(|(i, a)| format!("${}{}", i + 1, arg_cast(a)))
        .collect();
    let call = format!("{pg_name}({})", placeholders.join(", "));
    let expr = match ret {
        RetKind::Int => format!("({call})::bigint"),
        RetKind::Float => format!("({call})::double precision"),
        RetKind::Text => call,
        RetKind::Numeric => format!("({call})::text"),
    };
    let sql = format!("SELECT {expr}");
    let params: Vec<Box<dyn ToSql + Sync>> = args.iter().map(arg_param).collect();
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|b| b.as_ref()).collect();

    match client.query_one(&sql, &param_refs).await {
        Ok(row) => Outcome::Value(match ret {
            RetKind::Int => row.get::<_, Option<i64>>(0).map(Val::I),
            RetKind::Float => row.get::<_, Option<f64>>(0).map(Val::F),
            RetKind::Text => row.get::<_, Option<String>>(0).map(Val::S),
            RetKind::Numeric => row
                .get::<_, Option<String>>(0)
                .map(|s| Val::S(normalize_numeric(&s))),
        }),
        Err(e) => Outcome::Error(pg_err(&e)),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Battery-crossing combinators
// ─────────────────────────────────────────────────────────────────────────

type CallSites = Vec<(String, Vec<ArgVal>)>;

fn cross1<A: Clone>(a: &[(&'static str, A)], wrap: impl Fn(A) -> ArgVal) -> CallSites {
    a.iter()
        .map(|(l, v)| (l.to_string(), vec![wrap(v.clone())]))
        .collect()
}

fn cross2<A: Clone, B: Clone>(
    a: &[(&'static str, A)],
    b: &[(&'static str, B)],
    wa: impl Fn(A) -> ArgVal,
    wb: impl Fn(B) -> ArgVal,
) -> CallSites {
    let mut out = Vec::new();
    for (la, va) in a {
        for (lb, vb) in b {
            out.push((format!("{la},{lb}"), vec![wa(va.clone()), wb(vb.clone())]));
        }
    }
    out
}

/// One-at-a-time design for a 3-argument function — see the module docs.
#[allow(clippy::too_many_arguments)]
fn oat3<A: Clone, B: Clone, C: Clone>(
    a: &[(&'static str, A)],
    base_a: A,
    b: &[(&'static str, B)],
    base_b: B,
    c: &[(&'static str, C)],
    base_c: C,
    wa: impl Fn(A) -> ArgVal,
    wb: impl Fn(B) -> ArgVal,
    wc: impl Fn(C) -> ArgVal,
) -> CallSites {
    let mut out = vec![(
        "baseline".to_string(),
        vec![wa(base_a.clone()), wb(base_b.clone()), wc(base_c.clone())],
    )];
    for (l, v) in a {
        out.push((
            format!("arg0={l}"),
            vec![wa(v.clone()), wb(base_b.clone()), wc(base_c.clone())],
        ));
    }
    for (l, v) in b {
        out.push((
            format!("arg1={l}"),
            vec![wa(base_a.clone()), wb(v.clone()), wc(base_c.clone())],
        ));
    }
    for (l, v) in c {
        out.push((
            format!("arg2={l}"),
            vec![wa(base_a.clone()), wb(base_b.clone()), wc(v.clone())],
        ));
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────
// The function table — every OID eval_scalar_fn's match recognizes
// ─────────────────────────────────────────────────────────────────────────

struct FuncSpec {
    name: &'static str,
    oid: u32,
    ret: RetKind,
    call_sites: CallSites,
}

fn function_table() -> Vec<FuncSpec> {
    let f8 = float8_battery();
    let f4 = float4_battery();
    let i2 = int2_battery();
    let i4 = int4_battery();
    let i8 = int8_battery();
    let txt = text_battery();
    let txt_small = text_battery_small();
    let num = numeric_battery();
    let ndig = ndigits_battery();

    let mut specs = Vec::new();

    // ── float8 -> float8 (see eval.rs's "Math — trig/log/exp/power" and
    // "Math — abs/round/ceil/floor" blocks) ─────────────────────────────
    for (name, oid) in [
        ("sqrt", 1344),
        ("cbrt", 1345),
        ("ln", 1341),
        ("log", 1340),
        ("exp", 1347),
        ("degrees", 1608),
        ("radians", 1609),
        ("acos", 1601),
        ("asin", 1600),
        ("atan", 1602),
        ("cos", 1605),
        ("sin", 1604),
        ("tan", 1606),
        ("ceil", 2308),
        ("floor", 2309),
        ("ceiling", 2320),
        ("round", 1342),
        ("trunc", 1343),
        ("sign", 2310),
        ("abs", 1395),
    ] {
        specs.push(FuncSpec {
            name,
            oid,
            ret: RetKind::Float,
            call_sites: cross1(&f8, ArgVal::Float8),
        });
    }

    // ── abs(real) ────────────────────────────────────────────────────────
    specs.push(FuncSpec {
        name: "abs",
        oid: 1394,
        ret: RetKind::Float,
        call_sites: cross1(&f4, ArgVal::Float4),
    });

    // ── abs(int2/int4/int8) ─────────────────────────────────────────────
    specs.push(FuncSpec {
        name: "abs",
        oid: 1398,
        ret: RetKind::Int,
        call_sites: cross1(&i2, ArgVal::Int2),
    });
    specs.push(FuncSpec {
        name: "abs",
        oid: 1397,
        ret: RetKind::Int,
        call_sites: cross1(&i4, ArgVal::Int4),
    });
    specs.push(FuncSpec {
        name: "abs",
        oid: 1396,
        ret: RetKind::Int,
        call_sites: cross1(&i8, ArgVal::Int8),
    });

    // ── float8, float8 -> float8 ─────────────────────────────────────────
    specs.push(FuncSpec {
        name: "power",
        oid: 1368,
        ret: RetKind::Float,
        call_sites: cross2(&f8, &f8, ArgVal::Float8, ArgVal::Float8),
    });
    specs.push(FuncSpec {
        name: "atan2",
        oid: 1603,
        ret: RetKind::Float,
        call_sites: cross2(&f8, &f8, ArgVal::Float8, ArgVal::Float8),
    });

    // ── numeric -> numeric ───────────────────────────────────────────────
    for (name, oid) in [
        ("abs", 1705),
        ("round", 1708),
        ("ceil", 1711),
        ("floor", 1712),
        ("ceiling", 2167),
        ("trunc", 1710),
        ("sign", 1706),
    ] {
        specs.push(FuncSpec {
            name,
            oid,
            ret: RetKind::Numeric,
            call_sites: cross1(&num, ArgVal::Numeric),
        });
    }

    // ── numeric, int4 -> numeric ─────────────────────────────────────────
    specs.push(FuncSpec {
        name: "round",
        oid: 1707,
        ret: RetKind::Numeric,
        call_sites: cross2(&num, &ndig, ArgVal::Numeric, ArgVal::Int4),
    });
    specs.push(FuncSpec {
        name: "trunc",
        oid: 1709,
        ret: RetKind::Numeric,
        call_sites: cross2(&num, &ndig, ArgVal::Numeric, ArgVal::Int4),
    });

    // ── text -> text ─────────────────────────────────────────────────────
    for (name, oid) in [
        ("lower", 870),
        ("upper", 871),
        ("btrim", 885),
        ("ltrim", 881),
        ("rtrim", 882),
    ] {
        specs.push(FuncSpec {
            name,
            oid,
            ret: RetKind::Text,
            call_sites: cross1(&txt, ArgVal::Text),
        });
    }

    // ── text -> int4 (length) ────────────────────────────────────────────
    specs.push(FuncSpec {
        name: "length",
        oid: 1317,
        ret: RetKind::Int,
        call_sites: cross1(&txt, ArgVal::Text),
    });

    // ── text, text -> text (btrim/2, ltrim/2, rtrim/2) ──────────────────
    for (name, oid) in [("btrim", 884), ("ltrim", 875), ("rtrim", 876)] {
        specs.push(FuncSpec {
            name,
            oid,
            ret: RetKind::Text,
            call_sites: cross2(&txt, &txt_small, ArgVal::Text, ArgVal::Text),
        });
    }

    // ── text, text -> int4 (strpos) ──────────────────────────────────────
    specs.push(FuncSpec {
        name: "strpos",
        oid: 868,
        ret: RetKind::Int,
        call_sites: cross2(&txt, &txt_small, ArgVal::Text, ArgVal::Text),
    });

    // ── text, int4 -> text (substr/2, left, right) ──────────────────────
    specs.push(FuncSpec {
        name: "substr",
        oid: 883,
        ret: RetKind::Text,
        call_sites: cross2(&txt, &i4, ArgVal::Text, ArgVal::Int4),
    });
    specs.push(FuncSpec {
        name: "left",
        oid: 3060,
        ret: RetKind::Text,
        call_sites: cross2(&txt, &i4, ArgVal::Text, ArgVal::Int4),
    });
    specs.push(FuncSpec {
        name: "right",
        oid: 3061,
        ret: RetKind::Text,
        call_sites: cross2(&txt, &i4, ArgVal::Text, ArgVal::Int4),
    });

    // ── text, int4, int4 -> text (substr/3) — one-at-a-time ─────────────
    specs.push(FuncSpec {
        name: "substr",
        oid: 877,
        ret: RetKind::Text,
        call_sites: oat3(
            &txt,
            Some("hello".to_string()),
            &i4,
            Some(2),
            &i4,
            Some(3),
            ArgVal::Text,
            ArgVal::Int4,
            ArgVal::Int4,
        ),
    });

    // ── text, text, text -> text (replace) — one-at-a-time ───────────────
    specs.push(FuncSpec {
        name: "replace",
        oid: 2087,
        ret: RetKind::Text,
        call_sites: oat3(
            &txt,
            Some("hello world".to_string()),
            &txt,
            Some("o".to_string()),
            &txt,
            Some("0".to_string()),
            ArgVal::Text,
            ArgVal::Text,
            ArgVal::Text,
        ),
    });

    // ── concat(any, any) monomorphized at text, text -> text ────────────
    specs.push(FuncSpec {
        name: "concat",
        oid: 3058,
        ret: RetKind::Text,
        call_sites: cross2(&txt, &txt, ArgVal::Text, ArgVal::Text),
    });

    // ── pi() — niladic ────────────────────────────────────────────────────
    specs.push(FuncSpec {
        name: "pi",
        oid: 1610,
        ret: RetKind::Float,
        call_sites: vec![("()".to_string(), vec![])],
    });

    specs
}

// ─────────────────────────────────────────────────────────────────────────
// Driver
// ─────────────────────────────────────────────────────────────────────────

struct Divergence {
    function: String,
    oid: u32,
    case: String,
    reason: String,
    class: Class,
}

/// A call site whose input contains a NUL byte but which PostgreSQL did *not*
/// refuse with [`NUL_SQLSTATE`]. The whole NUL attribution rests on the server
/// refusing every one of them, so this is recorded and fails the run rather
/// than being silently absorbed — see the module docs, check (1).
struct NulAnomaly {
    function: String,
    oid: u32,
    case: String,
    observed: String,
}

/// Everything counted about NUL-bearing call sites, so the reported numbers
/// add up in the output instead of in the reader's head.
#[derive(Default)]
struct NulTally {
    /// Call sites whose input carried a `0x00` byte.
    sites: usize,
    /// …of which PostgreSQL refused with [`NUL_SQLSTATE`].
    pg_rejected: usize,
    /// …of which diverged in the attributed shape (Basin's `eval` succeeded).
    attributed: usize,
    /// …of which produced no divergence at all, because Basin errored too.
    agreed: usize,
    /// …of which diverged in some *other* shape. Counted as unexplained.
    unexplained: usize,
}

fn print_banner(msg: &str) {
    println!("=== function_equivalence: {msg} ===");
}

async fn run() -> Result<(), String> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            print_banner(
                "SKIPPED — PG_DIFF_TEST_DSN is unset. 0 functions and 0 call sites were \
                 compared against a live server this run, and in particular 0 embedded-NUL \
                 inputs were put to the server, so the NUL accounting below is absent rather \
                 than clean. Set PG_DIFF_TEST_DSN to a libpq DSN (e.g. \
                 postgres://postgres:postgres@127.0.0.1:5432/postgres) to actually run this \
                 check.",
            );
            return Ok(());
        }
    };

    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("PG_DIFF_TEST_DSN connect failed: {e}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    print_banner("running against live PostgreSQL (PG_DIFF_TEST_DSN set)");

    // Silence the default panic hook for the duration: eval.rs's decimal
    // round/trunc genuinely panic on ndigits = i32::MIN (see module docs),
    // and that is expected, recorded output, not noise to print per case.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    // Check (2) from the module docs, before any measuring: if the harness
    // that owns the wire-layer NUL guard is gone, this run has nowhere to
    // attribute its NUL divergences to and must report them as unexplained.
    let companion = wire_guard_companion();
    match &companion {
        Ok(desc) => println!("    wire-guard companion: {desc}"),
        Err(why) => println!(
            "    wire-guard companion: MISSING — {why}\n\
             \x20   NUL divergences will NOT be attributed this run."
        ),
    }

    let table = function_table();
    let mut functions_covered = 0usize;
    let mut total_call_sites = 0usize;
    let mut divergences: Vec<Divergence> = Vec::new();
    let mut nul = NulTally::default();
    let mut nul_anomalies: Vec<NulAnomaly> = Vec::new();

    for spec in &table {
        functions_covered += 1;
        for (case, args) in &spec.call_sites {
            total_call_sites += 1;
            let basin_outcome = match safe_basin_call(spec.oid, args) {
                Ok(arr) => Outcome::Value(extract(spec.ret, &arr)),
                Err(e) => Outcome::Error(e),
            };
            let pg = pg_outcome(&client, spec.name, args, spec.ret).await;

            let has_nul = args_carry_nul(args);
            if has_nul {
                nul.sites += 1;
                if pg_sqlstate_is(&pg, NUL_SQLSTATE) {
                    nul.pg_rejected += 1;
                } else {
                    // Check (1): PostgreSQL is supposed to refuse every one
                    // of these. It did not, so the premise is wrong.
                    nul_anomalies.push(NulAnomaly {
                        function: spec.name.to_string(),
                        oid: spec.oid,
                        case: case.clone(),
                        observed: fmt_outcome(&pg),
                    });
                }
            }

            match compare(&basin_outcome, &pg) {
                None => {
                    if has_nul {
                        nul.agreed += 1;
                    }
                }
                Some(reason) => {
                    // Attribution requires the companion harness to exist.
                    let class = match classify(args, &basin_outcome, &pg) {
                        Class::NulBelowTheGuard if companion.is_ok() => Class::NulBelowTheGuard,
                        _ => Class::Unexplained,
                    };
                    if has_nul {
                        match class {
                            Class::NulBelowTheGuard => nul.attributed += 1,
                            Class::Unexplained => nul.unexplained += 1,
                        }
                    }
                    divergences.push(Divergence {
                        function: spec.name.to_string(),
                        oid: spec.oid,
                        case: case.clone(),
                        reason,
                        class,
                    });
                }
            }
        }
    }

    std::panic::set_hook(previous_hook);

    let attributed: Vec<&Divergence> = divergences
        .iter()
        .filter(|d| d.class == Class::NulBelowTheGuard)
        .collect();
    let unexplained: Vec<&Divergence> = divergences
        .iter()
        .filter(|d| d.class == Class::Unexplained)
        .collect();

    // ---- metric: always printed ---------------------------------------
    print_banner(&format!(
        "{functions_covered} functions covered, {total_call_sites} call sites compared, \
         {} unexplained divergence(s), {} attributed to the wire-layer NUL guard",
        unexplained.len(),
        attributed.len()
    ));

    print_nul_accounting(&nul, total_call_sites, &attributed, companion.is_ok());

    if !nul_anomalies.is_empty() {
        println!(
            "\n!!! {} NUL-bearing call site(s) that PostgreSQL did NOT refuse with {NUL_SQLSTATE}.\n\
             This battery attributes its NUL divergences on the premise that the live server \
             refuses every one of them. It did not, so the premise — not just the count — needs \
             re-measuring:",
            nul_anomalies.len()
        );
        for a in &nul_anomalies {
            println!(
                "    {} (oid {}) [{}] — postgres returned {}",
                a.function, a.oid, a.case, a.observed
            );
        }
    }

    if !unexplained.is_empty() {
        println!("\n--- unexplained divergences ------------------------------------------");
        println!("{}", group_by_function(&unexplained, true));
    }

    if unexplained.is_empty() && nul_anomalies.is_empty() {
        print_banner(&format!(
            "PASS — every call site matched PostgreSQL 18 on value, NULL-ness, and \
             error-vs-success, except {} call site(s) where basin's eval accepted an embedded \
             NUL that PostgreSQL refused with {NUL_SQLSTATE}. Those are the wire layer's to \
             refuse and it does (a4749d4e); see the NUL accounting above",
            attributed.len()
        ));
        return Ok(());
    }

    let mut msg = String::new();
    if !unexplained.is_empty() {
        let functions = unexplained
            .iter()
            .map(|d| (d.function.clone(), d.oid))
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        msg.push_str(&format!(
            "{} of {total_call_sites} call sites across {functions} function(s) diverged from \
             PostgreSQL for reasons this battery cannot account for — see the listing above. \
             This is the honest count; do not weaken the battery to make it smaller. The one \
             thing that is discounted, embedded-NUL input, is discounted by measurement and not \
             by exemption: it is counted, printed, and only set aside when the server really \
             refused it with {NUL_SQLSTATE}, basin's eval really returned a value, and the \
             harness that refuses it one layer up really still exists.",
            unexplained.len()
        ));
    }
    if !nul_anomalies.is_empty() {
        if !msg.is_empty() {
            msg.push_str("\n\n");
        }
        msg.push_str(&format!(
            "{} NUL-bearing call site(s) were not refused by PostgreSQL with {NUL_SQLSTATE}. \
             The NUL attribution above is only sound while the server refuses all of them, so \
             this fails the run even if nothing else diverged.",
            nul_anomalies.len()
        ));
    }
    Err(msg)
}

/// The NUL block. Printed on every run, including a clean one — the whole
/// point is that a reader can tell from this output whether Basin is right
/// about NUL handling, and "no NUL cases were exercised" must not look the
/// same as "they were, and they were accounted for".
fn print_nul_accounting(
    nul: &NulTally,
    total_call_sites: usize,
    attributed: &[&Divergence],
    companion_ok: bool,
) {
    println!("\n--- NUL accounting ---------------------------------------------------");
    println!(
        "  {} of {total_call_sites} call sites carried an embedded NUL (0x00) in a text argument.",
        nul.sites
    );
    println!(
        "  {} of those {} were refused by the live server with SQLSTATE {NUL_SQLSTATE} \
         (character_not_in_repertoire) — measured this run, not assumed.",
        nul.pg_rejected, nul.sites
    );
    println!(
        "  Of those {} sites: {} diverged in the one attributed shape (basin's eval SUCCEEDED, \
         postgres ERRORED {NUL_SQLSTATE}), {} did not diverge at all (basin errored too), {} \
         diverged some other way and are counted as unexplained.",
        nul.sites, nul.attributed, nul.agreed, nul.unexplained
    );
    if !companion_ok {
        println!(
            "  ATTRIBUTION SUSPENDED: the wire-guard companion harness is missing (see above), \
             so NUL divergences are reported as unexplained this run."
        );
    }
    if attributed.is_empty() {
        println!("  Nothing attributed this run.");
    } else {
        println!(
            "  The {} attributed divergence(s) are NOT {} bugs. eval() is below the layer that \
             owns this check: basin-router's decode_param rejects a NUL for every type in text \
             format and per-type in binary (a4749d4e), COPY rejects it for text columns, and \
             basin-rest returns HTTP 400 (6d8f38db). No client can reach eval() with a NUL; this \
             battery can, because it calls eval() directly on a RecordBatch. Per function:",
            attributed.len(),
            attributed.len()
        );
        print!("{}", group_by_function(attributed, false));
    }
    println!("----------------------------------------------------------------------");
}

/// Group divergences by `(function, oid)`. With `detail`, every case and its
/// reason is listed; without, only the per-function count — 101 repetitions
/// of one sentence is what made this battery's output unreadable in the
/// first place, and the shape is already stated once above.
fn group_by_function(ds: &[&Divergence], detail: bool) -> String {
    use std::collections::BTreeMap;
    let mut by_function: BTreeMap<(&str, u32), Vec<&&Divergence>> = BTreeMap::new();
    for d in ds {
        by_function
            .entry((d.function.as_str(), d.oid))
            .or_default()
            .push(d);
    }
    let mut out = String::new();
    for ((name, oid), group) in &by_function {
        if detail {
            out.push_str(&format!(
                "\n{name} (oid {oid}) — {} divergence(s):\n",
                group.len()
            ));
            for d in group {
                out.push_str(&format!("    [{}] {}\n", d.case, d.reason));
            }
        } else {
            out.push_str(&format!(
                "    {name} (oid {oid}) — {} call site(s)\n",
                group.len()
            ));
        }
    }
    out
}

fn main() {
    // No libtest here (see module docs on `harness = false`): build a
    // minimal runtime by hand, same as `catalog_fidelity.rs`.
    let rt = tokio::runtime::Runtime::new().expect("building a tokio runtime");
    match rt.block_on(run()) {
        Ok(()) => {}
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
    }
}
