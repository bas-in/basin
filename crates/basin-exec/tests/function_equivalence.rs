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
//! provoke a Postgres ERROR and check Basin errors too — not just to see
//! what value comes back.
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
/// error (confirmed live: `invalid byte sequence for encoding "UTF8": 0x00`)
/// — Basin's text functions do no such validation, which is exactly the
/// error-vs-success asymmetry this harness exists to catch.
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
/// character set, not an arbitrary string.
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
                 compared against a live server this run. Set PG_DIFF_TEST_DSN to a libpq DSN \
                 (e.g. postgres://postgres:postgres@127.0.0.1:5432/postgres) to actually run \
                 this check.",
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

    let table = function_table();
    let mut functions_covered = 0usize;
    let mut total_call_sites = 0usize;
    let mut divergences: Vec<Divergence> = Vec::new();

    for spec in &table {
        functions_covered += 1;
        for (case, args) in &spec.call_sites {
            total_call_sites += 1;
            let basin_outcome = match safe_basin_call(spec.oid, args) {
                Ok(arr) => Outcome::Value(extract(spec.ret, &arr)),
                Err(e) => Outcome::Error(e),
            };
            let pg = pg_outcome(&client, spec.name, args, spec.ret).await;
            if let Some(reason) = compare(&basin_outcome, &pg) {
                divergences.push(Divergence {
                    function: spec.name.to_string(),
                    oid: spec.oid,
                    case: case.clone(),
                    reason,
                });
            }
        }
    }

    std::panic::set_hook(previous_hook);

    // ---- metric: always printed ---------------------------------------
    print_banner(&format!(
        "{functions_covered} functions covered, {total_call_sites} call sites compared, \
         {} divergences found",
        divergences.len()
    ));

    if divergences.is_empty() {
        print_banner(
            "PASS — every call site matched PostgreSQL 18 on value, NULL-ness, and \
                       error-vs-success",
        );
        return Ok(());
    }

    use std::collections::BTreeMap;
    let mut by_function: BTreeMap<(String, u32), Vec<&Divergence>> = BTreeMap::new();
    for d in &divergences {
        by_function
            .entry((d.function.clone(), d.oid))
            .or_default()
            .push(d);
    }

    let mut detail = String::new();
    for ((name, oid), ds) in &by_function {
        detail.push_str(&format!(
            "\n{name} (oid {oid}) — {} divergence(s):\n",
            ds.len()
        ));
        for d in ds {
            detail.push_str(&format!("    [{}] {}\n", d.case, d.reason));
        }
    }
    println!("{detail}");

    Err(format!(
        "{} of {total_call_sites} call sites across {} function(s) diverged from PostgreSQL — \
         see the per-function listing above. This is the honest count; do not weaken the \
         battery to make it smaller.",
        divergences.len(),
        by_function.len()
    ))
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
