//! Differential harness for the **orphaned** `pg_catalog` functions: the ones
//! Basin answers today *only* because DataFusion's builtin registry answers
//! them, with no Basin code on either side to port.
//!
//! # Why this file exists
//!
//! `tests/function_equivalence.rs` diffs the 52 scalar OIDs
//! `crates/basin-exec/src/eval.rs` actually implements. That is the *covered*
//! surface. This file is its complement: the functions that work today,
//! have no owned implementation anywhere in the workspace, and **stop working
//! the moment `datafusion = "53"` is deleted from `Cargo.toml:149`** —
//! silently, because nothing in Basin references them, so no compiler error
//! and no grep of "what Basin registers" can find them.
//!
//! The failure listing is **uncapped on purpose**: it is the specification
//! the implementation tranche works from, so every failing input is printed
//! with the answer PostgreSQL gives for it.
//!
//! `docs/migration/df-removal/17-udf-rehosting.md` §3 named 48 such functions
//! and said explicitly that 48 was a floor. It is: the real number measured
//! for this file is **79**. §"Census method" below states how that was
//! obtained and where it is still a floor.
//!
//! # THESE TESTS ARE EXPECTED TO FAIL TODAY
//!
//! Every function here is unimplemented in `basin-exec`. A red run is the
//! correct state, not a broken harness. The output is written to be read as a
//! **work queue** for the re-hosting tranche: for each function it prints the
//! resolution status, the input, what PostgreSQL 18.2 returned, and what Basin
//! did — and it distinguishes three different kinds of "does not work":
//!
//!   * `UNRESOLVABLE` — `basin_pgtype::func::resolve()` does not know the
//!     name/signature at all, so the owned planner cannot even *build* the
//!     call. 73 of the 79 are in this state: the gap is a missing `pg_proc`
//!     row *plus* a missing implementation, and the row has to come first.
//!   * `UNIMPLEMENTED` — the name resolves to a real OID, but
//!     `eval_scalar_fn`'s `match` has no arm, so every call errors. 6 are
//!     here (`concat_ws`, `date_part`, `date_trunc`, `now`, `position`,
//!     `substring` — `basin-pgtype`'s `FUNCS` already carries their rows).
//!   * `DIVERGENT` — implemented, executes, returns a different answer from
//!     PostgreSQL. Nothing is in this state yet. Entries will move here as the
//!     tranche lands, and moving from DIVERGENT to matching is the real
//!     finish line, not moving out of UNRESOLVABLE.
//!
//! # Census method, and exactly where it is a floor
//!
//! Measured 2026-08-13 on `feat/own-engine-remove-datafusion`. Four steps:
//!
//! 1. **DataFusion's builtin registry, enumerated at runtime, not by grep.**
//!    A scratch binary linked `datafusion-functions`,
//!    `datafusion-functions-aggregate`, `datafusion-functions-nested` and
//!    `datafusion-functions-window` at the pinned `=53.1.0` with the same
//!    feature set `datafusion = "53"` resolves to (crucially including
//!    `crypto_expressions`, which is on by default via the `datafusion`
//!    facade and supplies `md5`/`sha224`/`sha256`/`sha384`/`sha512`), called
//!    each crate's `all_default_*()` collector, and printed `name()` plus
//!    every entry of `aliases()`. That is **287 distinct names**, against the
//!    174 the previous inventory's `fn name()`/`aliases()` source-grep found.
//!    The runtime method has no macro blind spot by construction, which is
//!    what settles §"the heuristic missed `percent_rank`": the window package
//!    registers exactly 11 names — `cume_dist`, `row_number`, `lead`, `lag`,
//!    `rank`, `dense_rank`, `percent_rank`, `ntile`, `first_value`,
//!    `last_value`, `nth_value`. `rank` and `dense_rank` turn out **not** to
//!    be orphans (`build.rs`'s `window_func_of` maps 3101/3102); only
//!    `percent_rank`, `cume_dist` and `ntile` are.
//!    Basin builds its session with `SessionStateBuilder::…with_default_features()`
//!    (`session.rs:2763`), so all 287 are genuinely live today.
//! 2. **Subtract what a Basin registration shadows.** DataFusion's
//!    `register_udf` overwrites by name, so any name `basin-engine` registers
//!    is a name with code to port, not an orphan. Registered names were
//!    harvested from all five declaration forms the inventory documents
//!    (`fn name()` literal, `match` inside `fn name()`, `name:` struct field,
//!    constructor argument, macro literal) plus `aliases`. 31 of the 287
//!    are shadowed. **This step over-approximates** — the harvest also picks
//!    up `ExecutionPlan`/rule names — which biases the orphan count *down*,
//!    the safe direction for a list whose purpose is "these will break".
//! 3. **Subtract what `basin-exec` already implements** — the 52 OIDs in
//!    `eval.rs`, plus `build.rs`'s `agg_func_of` (7 names) and
//!    `window_func_of` (13), plus `cte.rs`'s `srf_kind_of` (2), reduced to
//!    names. Leaves 217 candidates.
//! 4. **Keep only real `pg_catalog` surface.** All 217 were loaded into the
//!    live PostgreSQL 18.2 and joined against `pg_proc` filtered to
//!    `pronamespace = 'pg_catalog'::regnamespace`. **79 matched**; the other
//!    138 are DataFusion inventions or DataFusion spellings (`list_*`,
//!    `array_push_back`, `arrow_cast`, `make_array`, `nanvl`, `datetrunc`, …)
//!    and are deliberately *not* Basin's problem.
//!
//! **Where this is still a floor, stated plainly:**
//!
//!   * Step 2 is textual. If `basin-engine` registers a name in a sixth form
//!     nobody has found, that name is wrongly counted as shadowed and is
//!     missing from this list. The bias is toward *under*-reporting orphans.
//!   * Step 4 asks only "does a `pg_catalog` row with this `proname` exist".
//!     A name can match at the name level and still have no overload with the
//!     argument types DataFusion accepts, and conversely DataFusion's overload
//!     may be a different function wearing the same name (`percentile_cont`
//!     is the sharp case: PostgreSQL's is an *ordered-set* aggregate spelled
//!     `percentile_cont(f) WITHIN GROUP (ORDER BY x)`, DataFusion's is a plain
//!     two-argument aggregate — the same name, not the same function).
//!   * DataFusion also supplies things that are not functions at all — the
//!     `unnest` planner, table functions, the `||`/`@>` operator lowering —
//!     none of which this census counts.
//!   * `basin-exec`'s covered set was read from the working tree, which other
//!     agents are editing. If an OID landed since, its function will show up
//!     here as newly matching, which is a pass, not a false alarm.
//!
//! The 79, with the 48 already named in the inventory marked `*`:
//!
//! ```text
//! acosh asinh atanh cosh sinh tanh cot log10 pow            (math, 9 — 1 named*)
//! factorial* gcd* lcm*                                       (integer math, 3)
//! bit_length* char_length character_length* concat_ws*       (string, 15)
//! initcap* lpad* rpad* repeat* md5* octet_length* overlay*
//! starts_with* substring position to_hex*
//! regexp_count* regexp_instr* regexp_like*                   (regex, 3)
//! sha224 sha256 sha384 sha512                                (crypto, 4)
//! date_part* date_trunc* make_date* now*                     (datetime, 4)
//! random*                                                    (volatile, 1)
//! array_append* array_cat array_length* array_ndims*         (array, 14)
//! array_position* array_positions* array_prepend*
//! array_remove* array_replace* array_reverse* array_sort*
//! array_to_string* cardinality* string_to_array*
//! bit_and bit_or bit_xor bool_and* bool_or* corr* covar_pop* (aggregate, 23)
//! covar_samp* percentile_cont* regr_avgx regr_avgy
//! regr_count regr_intercept regr_r2 regr_slope regr_sxx
//! regr_sxy regr_syy stddev* stddev_pop* stddev_samp
//! var_pop* var_samp
//! cume_dist* ntile* percent_rank                             (window, 3)
//! ```
//!
//! Nothing is listed as *uncertain*: every one of the 79 was confirmed
//! present in the live `pg_proc` and confirmed absent from both
//! `basin-engine`'s registrations and `basin-exec`'s dispatch tables. The
//! uncertainty is all in the *other* direction and is described above — names
//! this method may have wrongly excluded.
//!
//! # What is executed here, and what is only probed
//!
//! `eval()` evaluates **scalar** expressions. Aggregates and window functions
//! live in `aggregate.rs`'s `AggFunc` and `window.rs`'s `WindowFunc`, which
//! are closed enums with no variant for any of the 29 aggregate/window
//! orphans — they cannot be *constructed*, so there is nothing to call. Those
//! are therefore probed at the resolution layer only, and their PostgreSQL
//! answer over a fixed dataset is captured so the work queue carries the value
//! the implementation has to produce. That asymmetry is deliberate and is
//! reported, not hidden: see `AGG_ORPHANS`.
//!
//! # The batteries
//!
//! Same discipline as `function_equivalence.rs`, extended to the types these
//! functions actually take. Every argument position draws from a battery
//! including NULL, empty, zero, negative, `i32::MIN`/`i32::MAX`, multibyte
//! UTF-8 and an embedded NUL. On top of that, per-function edges chosen
//! because the live server was asked and gave a **surprising** answer — each
//! one is a case a plausible reimplementation gets wrong:
//!
//!   * `string_to_array('a,b,c', NULL)` is `{a,",",b,",",c}` — a NULL
//!     delimiter splits into individual *characters*, it does not return NULL.
//!     `string_to_array('a,b,c', '')` is `{"a,b,c"}` — one element.
//!     `string_to_array('', ',')` is `{}` — an empty array, not `{""}`.
//!   * `lpad('hello', -3)` is `''`, not an error and not `'hello'`.
//!     `lpad('hello', 10, '')` is `'hello'` — an empty fill pads nothing.
//!     `lpad('hello', 2)` truncates to `'he'`.
//!     `lpad('hello', 2147483647)` errors with `requested length too large`.
//!   * `overlay('abcdef' placing 'XY' from 0)` **errors** with
//!     `negative substring length not allowed`; so does `from -2`. But
//!     `overlay('abcdef' placing 'XY' from 3 for -1)` does *not* error — it
//!     returns `abXYbcdef`. And `from 99` (past the end) appends: `abcdefXY`.
//!   * `array_length('{}'::int[], 1)` is NULL, but `cardinality('{}')` is
//!     **0** — two functions over the same input disagreeing on empty is
//!     exactly the kind of thing a shared helper gets wrong.
//!     `array_ndims('{}')` is NULL. `array_length(a, 0)` and `(a, -1)` are
//!     NULL, not errors.
//!   * `position(a, b)` in functional notation takes (haystack, needle), the
//!     *reverse* of the `position(needle IN haystack)` syntax it backs:
//!     `pg_catalog.position('b', 'abc')` is `0`, not `2`.
//!   * `to_hex(-1)` is `ffffffff` (two's complement, width-of-type), not
//!     `-1`.
//!   * `date_trunc` on `timestamptz` is **session-timezone dependent**, so
//!     this harness runs those cases under three zones — `UTC`,
//!     `America/New_York` (whole-hour DST) and `Australia/Lord_Howe`
//!     (a `+10:30` zone with a *half-hour* DST step) — and includes both DST
//!     transition instants of 2024. `eval()` has no session context at all
//!     (`docs/migration/df-removal/17-udf-rehosting.md` §5.4), so these cases
//!     are not merely unimplemented, they are unimplementable in the current
//!     shape of `eval` — which is the single most useful thing this file has
//!     to say to the tranche that follows.
//!   * Multi-dimensional arrays are given to `array_length`/`cardinality`/
//!     `array_ndims` as [`ArgVal::Unrepresentable`]: `basin_pgtype::physical`
//!     maps `int4[]` to a **one-dimensional** `List`, so Basin has no value to
//!     pass. That is a real, separate gap and is reported as one rather than
//!     quietly dropped from the battery.
//!
//! # Non-deterministic functions
//!
//! `now()` and `random()` are never compared by value. They are checked
//! against invariants that were themselves verified on the live server:
//! `pg_typeof(random())` is `double precision` and 10,000 draws all satisfy
//! `r >= 0 AND r < 1`; `pg_typeof(now())` is `timestamp with time zone` and
//! `now() = (SELECT now())` is true inside a transaction. Basin's side is
//! checked for return type, range, spread (a constant `random()` must fail),
//! and within-batch stability for `now()`. Cross-call transaction stability
//! for `now()` is *not* checked, because `eval()` has no transaction to be
//! stable within — noted here rather than tested, so the gap stays visible.
//!
//! # What "equivalent" means
//!
//! Value, NULL-ness and error-vs-success, as in `function_equivalence.rs`,
//! with one deliberate difference. `function_equivalence.rs` treats
//! ERROR-on-both-sides as a match; this file does not, when Basin's error is
//! *absence* rather than input rejection. Every function here errors for every
//! input today, so counting those pairs as matches would credit Basin with
//! agreeing that `overlay('abcdef' PLACING 'XY' FROM 0)` is invalid when it
//! has no `overlay` at all. The result is that the failing count equals the
//! total call-site count until real implementations land — which is the honest
//! reading of "nothing here is verified".
//! Floats compare with a `1e-9` relative epsilon; `numeric` compares by value
//! after stripping trailing fractional zeros; arrays compare element-wise as
//! text (never as PostgreSQL's `{…}` literal, whose quoting rules are their
//! own bug surface); `bytea` compares as lowercase hex.
//!
//! # Rendering self-check
//!
//! Basin returns an error for every function here today, so the Basin-side
//! renderers (Date32, Timestamp, Interval, List, Binary) would otherwise be
//! dead code that nobody has ever seen produce a right answer — and would
//! start producing *false* divergences the day the first function lands.
//! `render_self_check()` therefore builds each of those arrow types by hand
//! from the battery's own constants and diffs the rendering against the live
//! server's text for the same literal, before any function is tested. It
//! fails the run loudly if the harness itself is wrong.
//!
//! # Skip behavior
//!
//! Same convention as `function_equivalence.rs` /
//! `crates/basin-pgcatalog/tests/catalog_fidelity.rs`: `PG_DIFF_TEST_DSN`,
//! `harness = false`, and an explicit banner on skip so a skip can never be
//! mistaken for a pass — see `scripts/check-differential.sh`'s module docs.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow_array::types::{IntervalMonthDayNano, IntervalMonthDayNanoType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_exec::eval::eval;
use basin_pgtype::Oid;
use basin_plan::{ColumnRef, Expr, FuncId};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

// ─────────────────────────────────────────────────────────────────────────
// Argument model
// ─────────────────────────────────────────────────────────────────────────

/// `numeric` battery values are carried as decimal text and materialised at
/// this fixed precision/scale. Arbitrary but pinned, so every numeric call
/// site here is directly comparable — the *result* scale is still normalised
/// before comparing (see [`normalize_numeric`]).
const NUM_PRECISION: u8 = 30;
const NUM_SCALE: i8 = 10;

/// One test value for one argument position, tagged with the `pg_proc`
/// argument type it must be bound as on both sides. Temporal values carry
/// both the SQL literal (Postgres side) and the physical arrow value (Basin
/// side); [`render_self_check`] proves the two agree before anything is
/// tested.
#[derive(Clone, Debug)]
enum ArgVal {
    Bool(Option<bool>),
    Int4(Option<i32>),
    Int8(Option<i64>),
    Float8(Option<f64>),
    /// Decimal text, e.g. `"-2.5"`.
    Numeric(Option<&'static str>),
    Text(Option<String>),
    /// Raw bytes; bound to Postgres as hex through `decode(…, 'hex')`.
    Bytea(Option<Vec<u8>>),
    /// `(SQL literal, days since 1970-01-01)`.
    Date(Option<(&'static str, i32)>),
    /// `(SQL literal, microseconds since 1970-01-01)`, no zone.
    Timestamp(Option<(&'static str, i64)>),
    /// `(SQL literal with an explicit offset, microseconds since the epoch)`.
    TimestampTz(Option<(&'static str, i64)>),
    /// `(SQL literal, months, days, microseconds)`.
    Interval(Option<(&'static str, i32, i32, i64)>),
    Int4Array(Option<Vec<Option<i32>>>),
    TextArray(Option<Vec<Option<String>>>),
    /// A value PostgreSQL accepts that Basin's physical type system cannot
    /// represent. `sql` is inlined into the Postgres call (no bind
    /// parameter); the Basin side records `why` as its outcome. Used for
    /// multi-dimensional arrays and `±infinity` timestamps.
    Unrepresentable {
        sql: &'static str,
        why: &'static str,
    },
}

impl ArgVal {
    /// The `pg_proc` argument type this value is bound as, as a SQL cast
    /// suffix applied to a text-bound parameter. Everything is bound as
    /// `text` and cast server-side for the same reason `function_equivalence`
    /// does it for `numeric`: it sidesteps every `tokio-postgres` encoder
    /// question at once, and casting canonical text is exact.
    fn pg_expr(&self, param: usize) -> String {
        match self {
            ArgVal::Bool(_) => format!("${param}::text::boolean"),
            ArgVal::Int4(_) => format!("${param}::text::int4"),
            ArgVal::Int8(_) => format!("${param}::text::int8"),
            ArgVal::Float8(_) => format!("${param}::text::float8"),
            ArgVal::Numeric(_) => format!("${param}::text::numeric"),
            ArgVal::Text(_) => format!("${param}::text"),
            ArgVal::Bytea(_) => format!("decode(${param}::text, 'hex')"),
            ArgVal::Date(_) => format!("${param}::text::date"),
            ArgVal::Timestamp(_) => format!("${param}::text::timestamp"),
            ArgVal::TimestampTz(_) => format!("${param}::text::timestamptz"),
            ArgVal::Interval(_) => format!("${param}::text::interval"),
            ArgVal::Int4Array(_) => format!("${param}::text::int4[]"),
            ArgVal::TextArray(_) => format!("${param}::text::text[]"),
            ArgVal::Unrepresentable { sql, .. } => (*sql).to_string(),
        }
    }

    /// The text sent for this value's bind parameter, or `None` when the
    /// value is inlined into the SQL instead (see
    /// [`ArgVal::Unrepresentable`]).
    fn pg_param(&self) -> Option<Option<String>> {
        let v = match self {
            ArgVal::Bool(v) => v.map(|b| if b { "true" } else { "false" }.to_string()),
            ArgVal::Int4(v) => v.map(|x| x.to_string()),
            ArgVal::Int8(v) => v.map(|x| x.to_string()),
            ArgVal::Float8(v) => v.map(fmt_float_for_pg),
            ArgVal::Numeric(v) => v.map(|s| s.to_string()),
            ArgVal::Text(v) => v.clone(),
            ArgVal::Bytea(v) => v.as_ref().map(|b| hex(b)),
            ArgVal::Date(v) => v.map(|(lit, _)| lit.to_string()),
            ArgVal::Timestamp(v) => v.map(|(lit, _)| lit.to_string()),
            ArgVal::TimestampTz(v) => v.map(|(lit, _)| lit.to_string()),
            ArgVal::Interval(v) => v.map(|(lit, ..)| lit.to_string()),
            ArgVal::Int4Array(v) => v
                .as_ref()
                .map(|xs| pg_array_literal(xs.iter().map(|x| x.map(|n| n.to_string())))),
            ArgVal::TextArray(v) => v.as_ref().map(|xs| pg_array_literal(xs.iter().cloned())),
            ArgVal::Unrepresentable { .. } => return None,
        };
        Some(v)
    }

    /// The arrow column holding this single value, or the reason Basin cannot
    /// hold it at all.
    fn arrow_column(&self) -> Result<(DataType, ArrayRef), String> {
        Ok(match self {
            ArgVal::Bool(v) => (
                DataType::Boolean,
                Arc::new(BooleanArray::from(vec![*v])) as ArrayRef,
            ),
            ArgVal::Int4(v) => (DataType::Int32, Arc::new(Int32Array::from(vec![*v]))),
            ArgVal::Int8(v) => (DataType::Int64, Arc::new(Int64Array::from(vec![*v]))),
            ArgVal::Float8(v) => (DataType::Float64, Arc::new(Float64Array::from(vec![*v]))),
            ArgVal::Numeric(v) => {
                let mantissa = match v {
                    Some(s) => Some(decimal_text_to_mantissa(s)?),
                    None => None,
                };
                let arr = Decimal128Array::from(vec![mantissa])
                    .with_precision_and_scale(NUM_PRECISION, NUM_SCALE)
                    .map_err(|e| format!("numeric battery value out of range: {e}"))?;
                (
                    DataType::Decimal128(NUM_PRECISION, NUM_SCALE),
                    Arc::new(arr),
                )
            }
            ArgVal::Text(v) => (DataType::Utf8, Arc::new(StringArray::from(vec![v.clone()]))),
            ArgVal::Bytea(v) => (
                DataType::Binary,
                Arc::new(BinaryArray::from_iter(vec![v.as_deref()])),
            ),
            ArgVal::Date(v) => (
                DataType::Date32,
                Arc::new(Date32Array::from(vec![v.map(|(_, d)| d)])),
            ),
            ArgVal::Timestamp(v) => (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![v.map(|(_, u)| u)])),
            ),
            ArgVal::TimestampTz(v) => (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![v.map(|(_, u)| u)]).with_timezone("UTC"),
                ),
            ),
            ArgVal::Interval(v) => (
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                Arc::new(IntervalMonthDayNanoArray::from(vec![v.map(
                    |(_, m, d, us)| IntervalMonthDayNanoType::make_value(m, d, us * 1_000),
                )])),
            ),
            ArgVal::Int4Array(v) => {
                let mut b = ListBuilder::new(Int32Builder::new());
                match v {
                    Some(xs) => {
                        for x in xs {
                            b.values().append_option(*x);
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
                let arr = b.finish();
                (arr.data_type().clone(), Arc::new(arr))
            }
            ArgVal::TextArray(v) => {
                let mut b = ListBuilder::new(StringBuilder::new());
                match v {
                    Some(xs) => {
                        for x in xs {
                            b.values().append_option(x.as_deref());
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
                let arr = b.finish();
                (arr.data_type().clone(), Arc::new(arr))
            }
            ArgVal::Unrepresentable { why, .. } => return Err((*why).to_string()),
        })
    }
}

fn fmt_float_for_pg(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v == f64::INFINITY {
        "Infinity".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        format!("{v:?}")
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A PostgreSQL array literal. Elements are always double-quoted (legal for
/// every type used here) so no quoting-rule guesswork is needed; `NULL` is
/// written bare, which is how Postgres spells a NULL element.
fn pg_array_literal(elems: impl Iterator<Item = Option<String>>) -> String {
    let mut out = String::from("{");
    for (i, e) in elems.enumerate() {
        if i > 0 {
            out.push(',');
        }
        match e {
            None => out.push_str("NULL"),
            Some(s) => {
                out.push('"');
                for c in s.chars() {
                    if c == '"' || c == '\\' {
                        out.push('\\');
                    }
                    out.push(c);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    out
}

/// Parse decimal text into a `Decimal128` mantissa at [`NUM_SCALE`].
fn decimal_text_to_mantissa(s: &str) -> Result<i128, String> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((a, b)) => (a, b),
        None => (body, ""),
    };
    let scale = NUM_SCALE as usize;
    if frac_part.len() > scale {
        return Err(format!(
            "battery numeric {s} needs more than {scale} decimals"
        ));
    }
    let mut digits = String::from(int_part);
    digits.push_str(frac_part);
    digits.push_str(&"0".repeat(scale - frac_part.len()));
    if digits.is_empty() {
        digits.push('0');
    }
    let v: i128 = digits
        .parse()
        .map_err(|_| format!("battery numeric {s} is not decimal text"))?;
    Ok(if neg { -v } else { v })
}

// ─────────────────────────────────────────────────────────────────────────
// Batteries
// ─────────────────────────────────────────────────────────────────────────

/// Text battery. `nul_embedded` is the entry expected to make *Postgres*
/// error (`invalid byte sequence for encoding "UTF8": 0x00`, confirmed live in
/// `function_equivalence.rs`); Basin does no such validation, which is exactly
/// the error-vs-success asymmetry these harnesses exist to catch.
fn txt() -> Vec<(&'static str, Option<String>)> {
    vec![
        ("null", None),
        ("empty", Some(String::new())),
        ("single_char", Some("a".to_string())),
        ("multibyte_utf8", Some("héllo世界".to_string())),
        ("combining", Some("e\u{0301}".to_string())),
        ("nul_embedded", Some("ab\u{0}cd".to_string())),
        ("whitespace_only", Some("   ".to_string())),
        ("ordinary", Some("The quick brown fox".to_string())),
        ("upper_mixed", Some("hELLo WoRLD".to_string())),
    ]
}

/// A smaller text battery for a needle / delimiter / fill-character position.
fn txt_small() -> Vec<(&'static str, Option<String>)> {
    vec![
        ("null", None),
        ("empty", Some(String::new())),
        ("space", Some(" ".to_string())),
        ("single_char", Some("o".to_string())),
        ("multibyte_utf8", Some("世".to_string())),
        ("two_char", Some("ab".to_string())),
    ]
}

fn i4() -> Vec<(&'static str, Option<i32>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("three", Some(3)),
        ("min", Some(i32::MIN)),
        ("max", Some(i32::MAX)),
    ]
}

fn i8b() -> Vec<(&'static str, Option<i64>)> {
    vec![
        ("null", None),
        ("zero", Some(0)),
        ("one", Some(1)),
        ("minus_one", Some(-1)),
        ("min", Some(i64::MIN)),
        ("max", Some(i64::MAX)),
    ]
}

/// Length/offset battery for `lpad`/`rpad`/`repeat`/`overlay`/`substring`.
/// `max` is included on purpose: `lpad('hello', 2147483647)` errors with
/// `requested length too large` on the live server rather than allocating,
/// so it is a cheap error-path probe, not an abusive one (verified).
fn len_battery() -> Vec<(&'static str, Option<i32>)> {
    vec![
        ("null", None),
        ("neg_big", Some(-3)),
        ("minus_one", Some(-1)),
        ("zero", Some(0)),
        ("shorter_than_input", Some(2)),
        ("equal_to_input", Some(5)),
        ("longer_than_input", Some(10)),
        ("min", Some(i32::MIN)),
        ("max", Some(i32::MAX)),
    ]
}

fn f8() -> Vec<(&'static str, Option<f64>)> {
    vec![
        ("null", None),
        ("zero", Some(0.0)),
        ("neg_zero", Some(-0.0)),
        ("one", Some(1.0)),
        ("minus_one", Some(-1.0)),
        ("half", Some(0.5)),
        ("two", Some(2.0)),
        ("min", Some(f64::MIN)),
        ("max", Some(f64::MAX)),
        ("infinity", Some(f64::INFINITY)),
        ("neg_infinity", Some(f64::NEG_INFINITY)),
        ("nan", Some(f64::NAN)),
        ("subnormal", Some(f64::MIN_POSITIVE / 2.0)),
    ]
}

fn numeric_battery() -> Vec<(&'static str, Option<&'static str>)> {
    vec![
        ("null", None),
        ("zero", Some("0")),
        ("one", Some("1")),
        ("minus_one", Some("-1")),
        ("half", Some("0.5")),
        ("two_half", Some("2.5")),
        ("tiny", Some("0.0000000001")),
        ("big", Some("12345678901234567890")),
    ]
}

/// `date_trunc` units. Includes the case variants Postgres accepts, the
/// whitespace form it does *not*, and an outright invalid unit — all three
/// are error-vs-success probes.
fn trunc_units() -> Vec<(&'static str, Option<String>)> {
    [
        "microseconds",
        "milliseconds",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "decade",
        "century",
        "millennium",
        "DAY",
        " day ",
        "fortnight",
        "",
    ]
    .iter()
    .map(|u| (*u, Some(u.to_string())))
    .chain(std::iter::once(("null", None)))
    .collect()
}

/// `date_part` fields. `timezone`/`timezone_hour` are here because they are
/// the fields whose answer depends on the session zone even for a
/// `timestamp without time zone` argument.
fn part_fields() -> Vec<(&'static str, Option<String>)> {
    [
        "epoch",
        "year",
        "isoyear",
        "quarter",
        "month",
        "week",
        "day",
        "doy",
        "dow",
        "isodow",
        "hour",
        "minute",
        "second",
        "milliseconds",
        "microseconds",
        "julian",
        "timezone",
        "timezone_hour",
        "fortnight",
        "",
    ]
    .iter()
    .map(|u| (*u, Some(u.to_string())))
    .chain(std::iter::once(("null", None)))
    .collect()
}

/// Timestamps, as `(literal, micros since epoch)`. Every pair was read off
/// the live server (`extract(epoch from …) * 1000000`), never computed here.
fn ts_battery() -> Vec<(&'static str, Option<(&'static str, i64)>)> {
    vec![
        ("null", None),
        ("epoch", Some(("1970-01-01 00:00:00", 0))),
        (
            "sub_second",
            Some(("2024-03-10 12:34:56.789012", 1_710_074_096_789_012)),
        ),
        (
            "dst_fallback_local",
            Some(("2024-11-03 01:30:00", 1_730_597_400_000_000)),
        ),
        (
            "just_before_y2k",
            Some(("1999-12-31 23:59:59.999999", 946_684_799_999_999)),
        ),
        (
            "year_one",
            Some(("0001-01-01 00:00:00", -62_135_596_800_000_000)),
        ),
    ]
}

/// [`ts_battery`] plus the two values Basin's `Timestamp(Microsecond)` cannot
/// hold at all. Postgres's `timestamp` has `infinity`/`-infinity` sentinels;
/// arrow does not, so these are [`ArgVal::Unrepresentable`] rather than a
/// value, and show up in the report as a representation gap distinct from a
/// missing implementation.
fn ts_cases() -> Vec<(&'static str, ArgVal)> {
    let mut out: Vec<(&'static str, ArgVal)> = ts_battery()
        .into_iter()
        .map(|(l, v)| (l, ArgVal::Timestamp(v)))
        .collect();
    out.push((
        "infinity",
        ArgVal::Unrepresentable {
            sql: "'infinity'::timestamp",
            why: "basin has no representation for timestamp 'infinity': \
                  basin_pgtype::physical maps timestamp to arrow \
                  Timestamp(Microsecond), which has no infinite sentinel",
        },
    ));
    out.push((
        "neg_infinity",
        ArgVal::Unrepresentable {
            sql: "'-infinity'::timestamp",
            why: "basin has no representation for timestamp '-infinity' \
                  (see the 'infinity' case)",
        },
    ));
    out
}

/// Timestamptz values chosen for the two 2024 DST transitions in
/// `America/New_York` and the `+10:30`/`+11:00` step in
/// `Australia/Lord_Howe`. Literals carry an explicit `+00` offset so the
/// *input* is session-zone independent and only the *output* varies.
fn tstz_cases() -> Vec<(&'static str, ArgVal)> {
    vec![
        ("null", ArgVal::TimestampTz(None)),
        (
            "epoch_utc",
            ArgVal::TimestampTz(Some(("1970-01-01 00:00:00+00", 0))),
        ),
        (
            "dst_spring_forward",
            ArgVal::TimestampTz(Some(("2024-03-10 06:30:00+00", 1_710_052_200_000_000))),
        ),
        (
            "dst_fall_back",
            ArgVal::TimestampTz(Some(("2024-11-03 05:30:00+00", 1_730_611_800_000_000))),
        ),
        (
            "midyear",
            ArgVal::TimestampTz(Some(("2024-07-01 12:00:00+00", 1_719_835_200_000_000))),
        ),
    ]
}

/// `(SQL literal, months, days, microseconds)` — Postgres's own internal
/// interval decomposition, which is what makes the comparison exact rather
/// than a diff of `IntervalStyle` output.
type IntervalCase = (&'static str, i32, i32, i64);

fn iv_battery() -> Vec<(&'static str, Option<IntervalCase>)> {
    vec![
        ("null", None),
        ("zero", Some(("0", 0, 0, 0))),
        (
            "mixed",
            Some(("1 year 2 mons 3 days 04:05:06.789", 14, 3, 14_706_789_000)),
        ),
        ("negative_day", Some(("-1 day", 0, -1, 0))),
        ("century", Some(("100 years", 1200, 0, 0))),
        (
            "overflowing_minutes",
            Some(("90 minutes", 0, 0, 5_400_000_000)),
        ),
    ]
}

fn date_battery() -> Vec<(&'static str, Option<(&'static str, i32)>)> {
    vec![
        ("null", None),
        ("epoch", Some(("1970-01-01", 0))),
        ("ordinary", Some(("2024-03-10", 19_792))),
        ("year_one", Some(("0001-01-01", -719_162))),
        ("far_future", Some(("2262-04-11", 106_751))),
    ]
}

fn int_array_cases() -> Vec<(&'static str, ArgVal)> {
    vec![
        ("null", ArgVal::Int4Array(None)),
        ("empty", ArgVal::Int4Array(Some(vec![]))),
        ("one_elem", ArgVal::Int4Array(Some(vec![Some(1)]))),
        (
            "with_null_elem",
            ArgVal::Int4Array(Some(vec![Some(1), None, Some(3)])),
        ),
        (
            "duplicates",
            ArgVal::Int4Array(Some(vec![Some(2), Some(1), Some(2), Some(3)])),
        ),
        (
            "extremes",
            ArgVal::Int4Array(Some(vec![Some(i32::MIN), Some(0), Some(i32::MAX)])),
        ),
        (
            "two_dimensional",
            ArgVal::Unrepresentable {
                sql: "'{{1,2,3},{4,5,6}}'::int4[]",
                why: "basin has no multi-dimensional array: basin_pgtype::physical \
                      maps int4[] to a one-dimensional arrow List, so there is no \
                      value to pass. array_length(a,2)/array_ndims(a) are \
                      unanswerable until the physical model carries dimensions",
            },
        ),
    ]
}

fn text_array_cases() -> Vec<(&'static str, ArgVal)> {
    vec![
        ("null", ArgVal::TextArray(None)),
        ("empty", ArgVal::TextArray(Some(vec![]))),
        (
            "ordinary",
            ArgVal::TextArray(Some(vec![
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
            ])),
        ),
        (
            "with_null_elem",
            ArgVal::TextArray(Some(vec![Some("a".into()), None, Some("c".into())])),
        ),
        (
            "embedded_separator",
            ArgVal::TextArray(Some(vec![Some("a,b".into()), Some("".into())])),
        ),
        (
            "multibyte",
            ArgVal::TextArray(Some(vec![Some("héllo".into()), Some("世界".into())])),
        ),
    ]
}

// ─────────────────────────────────────────────────────────────────────────
// Basin side
// ─────────────────────────────────────────────────────────────────────────

fn build_batch(args: &[ArgVal]) -> Result<(RecordBatch, Vec<Expr>), String> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut exprs = Vec::new();

    if args.is_empty() {
        // Niladic functions (`now()`, `random()`) still need a batch with a
        // row: an arm that reads `batch.num_rows()` would otherwise produce
        // zero output rows rather than one. Same reason `pi()` needs it in
        // `function_equivalence.rs`.
        fields.push(Field::new("_dummy", DataType::Int32, true));
        columns.push(Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef);
    }

    for (i, a) in args.iter().enumerate() {
        let (dt, col) = a.arrow_column()?;
        let name = format!("a{i}");
        fields.push(Field::new(&name, dt, true));
        columns.push(col);
        exprs.push(Expr::Column(ColumnRef {
            relation: 0,
            index: (fields.len() - 1) as u16,
            name,
        }));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("harness could not build a 1-row batch: {e}"))?;
    Ok((batch, exprs))
}

fn basin_call(oid: u32, args: &[ArgVal]) -> Result<ArrayRef, String> {
    let (batch, exprs) = build_batch(args)?;
    let expr = Expr::ScalarFn {
        func: FuncId(Oid(oid)),
        args: exprs,
    };
    eval(&expr, &batch).map_err(|e| e.to_string())
}

/// [`basin_call`] with a panic recorded as an outcome rather than allowed to
/// abort the run — same rationale as `function_equivalence.rs`: a panic where
/// Postgres cleanly errors is a divergence to report, not a harness failure.
///
/// The default panic hook is silenced **only for the duration of the call
/// under test**, not for the whole run. `function_equivalence.rs` silences it
/// globally; doing that here hid a harness bug behind an exit code with no
/// message during development, which is precisely the failure mode this repo
/// refuses elsewhere. A panic in the *harness* must still print.
fn safe_basin_call(oid: u32, args: &[ArgVal]) -> Result<ArrayRef, String> {
    let args = args.to_vec();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| basin_call(oid, &args)));
    std::panic::set_hook(previous);
    match r {
        Ok(r) => r,
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
// Rendering an arrow cell the way Postgres renders the same value
// ─────────────────────────────────────────────────────────────────────────

/// Days → `YYYY-MM-DD`, via the standard civil-from-days algorithm. Verified
/// against the live server for every date in the battery by
/// [`render_self_check`], including `0001-01-01`, which is where a naive
/// implementation gets the year padding or the leap rule wrong.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn fmt_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Micros since epoch → Postgres's `timestamp` text form, including its
/// fraction rule: the fractional part is omitted entirely when zero and has
/// trailing zeros stripped otherwise (`…00.100000` prints as `…00.1`,
/// verified live).
fn fmt_timestamp(micros: i64) -> String {
    let mut days = micros.div_euclid(86_400_000_000);
    let mut rem = micros.rem_euclid(86_400_000_000);
    if rem < 0 {
        rem += 86_400_000_000;
        days -= 1;
    }
    let (y, mo, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    let (h, mi, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    let mut out = format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}");
    if frac != 0 {
        let mut f = format!("{frac:06}");
        while f.ends_with('0') {
            f.pop();
        }
        out.push('.');
        out.push_str(&f);
    }
    out
}

/// Interval is compared on its `(months, days, microseconds)` triple, not on
/// text: Postgres's interval output format (`1 year 2 mons`, `04:05:06.789`,
/// `IntervalStyle`) is a display concern with its own rules, and diffing it
/// would test the formatter rather than the function.
fn fmt_interval(v: IntervalMonthDayNano) -> String {
    format!("{}/{}/{}", v.months, v.days, v.nanoseconds / 1_000)
}

fn fmt_decimal(mantissa: i128, scale: i32) -> String {
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

/// Render one cell of any arrow array Basin can return, as the text Postgres
/// would print for the same value. `None` is SQL NULL.
fn render_cell(arr: &dyn Array, i: usize) -> Result<Option<String>, String> {
    if arr.is_null(i) {
        return Ok(None);
    }
    let any = arr.as_any();
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return Ok(Some(if a.value(i) { "t" } else { "f" }.to_string()));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Ok(Some(a.value(i).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Int16Array>() {
        return Ok(Some(a.value(i).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Ok(Some(a.value(i).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Ok(Some(a.value(i).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Ok(Some(format!("{}", a.value(i))));
    }
    if let Some(a) = any.downcast_ref::<Decimal128Array>() {
        return Ok(Some(normalize_numeric(&fmt_decimal(
            a.value(i),
            a.scale() as i32,
        ))));
    }
    if let Some(a) = any.downcast_ref::<BinaryArray>() {
        return Ok(Some(hex(a.value(i))));
    }
    if let Some(a) = any.downcast_ref::<Date32Array>() {
        return Ok(Some(fmt_date(a.value(i))));
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Some(fmt_timestamp(a.value(i))));
    }
    if let Some(a) = any.downcast_ref::<IntervalMonthDayNanoArray>() {
        return Ok(Some(fmt_interval(a.value(i))));
    }
    Err(format!(
        "harness cannot render arrow type {:?} — extend render_cell",
        arr.data_type()
    ))
}

/// Render a `List` cell as a vector of element texts.
fn render_list(arr: &ArrayRef) -> Result<Option<Vec<Option<String>>>, String> {
    let a = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("expected a List result, got {:?}", arr.data_type()))?;
    if a.is_null(0) {
        return Ok(None);
    }
    let items = a.value(0);
    let mut out = Vec::with_capacity(items.len());
    for i in 0..items.len() {
        out.push(render_cell(items.as_ref(), i)?);
    }
    Ok(Some(out))
}

// ─────────────────────────────────────────────────────────────────────────
// Outcomes and comparison
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetKind {
    Bool,
    Int,
    Float,
    Text,
    Numeric,
    Bytea,
    Date,
    Timestamp,
    /// Compared after `AT TIME ZONE 'UTC'` on the Postgres side, because
    /// Basin's physical `timestamptz` is UTC micros with no rendering zone.
    TimestampTz,
    Interval,
    /// Any array; compared element-wise as text.
    Array,
}

#[derive(Clone, Debug, PartialEq)]
enum Val {
    F(f64),
    S(String),
    A(Vec<Option<String>>),
}

#[derive(Clone, Debug)]
enum Outcome {
    Value(Option<Val>),
    Error(String),
    /// Postgres answered, but `tokio-postgres` could not decode the answer
    /// into the shape this harness compares. Never silently equated with
    /// anything: a case the harness cannot read is a case it has not checked,
    /// and is reported as its own failure rather than folded into
    /// [`Outcome::Error`] where it would spuriously "match" Basin's error.
    Undecodable(String),
}

fn fmt_val(v: &Val) -> String {
    match v {
        Val::F(x) => format!("{x}"),
        Val::S(x) => format!("{x:?}"),
        Val::A(xs) => {
            let inner: Vec<String> = xs
                .iter()
                .map(|e| match e {
                    None => "NULL".to_string(),
                    Some(s) => format!("{s:?}"),
                })
                .collect();
            format!("[{}]", inner.join(", "))
        }
    }
}

fn fmt_outcome(o: &Outcome) -> String {
    match o {
        Outcome::Value(Some(v)) => fmt_val(v),
        Outcome::Value(None) => "NULL".to_string(),
        Outcome::Error(e) => format!("ERROR[{e}]"),
        Outcome::Undecodable(e) => format!("UNDECODABLE[{e}]"),
    }
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

/// Whether a Basin-side error says "this function does not exist" rather than
/// "this function rejected this input". See [`compare`]'s ERROR/ERROR arm for
/// why the distinction is load-bearing here and not in
/// `function_equivalence.rs`.
fn is_absence(err: &str) -> bool {
    err.contains("is not implemented in eval yet")
        || err.starts_with("basin has no ")
        || err.starts_with("PANIC")
        || err.starts_with("unrenderable result")
}

/// `None` if the two sides agree. Otherwise a `(kind, basin, postgres)`
/// triple rather than a pre-formatted sentence, so the report can collapse
/// the hundreds of cases that share one Basin outcome (every case of an
/// unimplemented function does) into a single line plus the per-case
/// PostgreSQL answers. Value, NULL-ness and error-vs-success all count.
fn compare(basin: &Outcome, pg: &Outcome) -> Option<(&'static str, String, String)> {
    let kind = match (basin, pg) {
        (_, Outcome::Undecodable(_)) => "HARNESS GAP (this case was NOT checked)",
        (Outcome::Undecodable(_), _) => "HARNESS GAP — basin result undecodable",
        // An ERROR/ERROR pair is only agreement when Basin's error is about
        // the *input*. Today every function here errors for *every* input
        // because it does not exist, so counting those pairs as matches would
        // credit Basin with agreeing that `overlay(… FROM 0)` is invalid when
        // it has no `overlay` at all. `function_equivalence.rs` can treat
        // ERROR/ERROR as a match because the functions it covers are
        // implemented; this file cannot.
        (Outcome::Error(e), Outcome::Error(_)) if is_absence(e) => {
            "basin has NO IMPLEMENTATION; postgres also rejects this input — the agreement \
             is accidental and this input is still unverified"
        }
        (Outcome::Error(_), Outcome::Error(_)) => return None,
        (Outcome::Error(_), Outcome::Value(_)) => "basin ERRORED, postgres SUCCEEDED",
        (Outcome::Value(_), Outcome::Error(_)) => "basin SUCCEEDED, postgres ERRORED",
        (Outcome::Value(a), Outcome::Value(b)) => {
            let eq = match (a, b) {
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
                (Some(Val::F(x)), Some(Val::F(y))) => floats_equivalent(*x, *y),
                (Some(x), Some(y)) => x == y,
            };
            if eq {
                return None;
            }
            "value mismatch"
        }
    };
    Some((kind, fmt_outcome(basin), fmt_outcome(pg)))
}

fn extract_basin(kind: RetKind, arr: &ArrayRef) -> Result<Option<Val>, String> {
    match kind {
        RetKind::Array => Ok(render_list(arr)?.map(Val::A)),
        RetKind::Float => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("expected a float8 result, got {:?}", arr.data_type()))?;
            Ok((!a.is_null(0)).then(|| Val::F(a.value(0))))
        }
        _ => Ok(render_cell(arr.as_ref(), 0)?.map(|s| {
            Val::S(if kind == RetKind::Numeric {
                normalize_numeric(&s)
            } else {
                s
            })
        })),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Postgres side
// ─────────────────────────────────────────────────────────────────────────

/// Postgres's error, spelled out. `tokio_postgres::Error`'s own `Display` is
/// the bare string `"db error"`, which is useless in a work queue — the
/// SQLSTATE *and* the server's message are what say which rule was violated
/// (`22011 negative substring length not allowed` vs `22003 requested length
/// too large` are different bugs to write).
fn pg_err(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => match e.code() {
            Some(code) => format!("{e} [{}]", code.code()),
            None => e.to_string(),
        },
    }
}

/// Build `SELECT <wrapped call>` for `args`, binding every representable
/// argument as text and inlining the rest.
fn pg_sql(call_name: &str, args: &[ArgVal], ret: RetKind) -> (String, Vec<Option<String>>) {
    let mut params = Vec::new();
    let mut arg_exprs = Vec::new();
    for a in args {
        match a.pg_param() {
            Some(p) => {
                params.push(p);
                arg_exprs.push(a.pg_expr(params.len()));
            }
            None => arg_exprs.push(a.pg_expr(0)),
        }
    }
    let call = format!("{call_name}({})", arg_exprs.join(", "));
    let sql = match ret {
        RetKind::Bool | RetKind::Text | RetKind::Numeric | RetKind::Date | RetKind::Timestamp => {
            format!("SELECT ({call})::text")
        }
        RetKind::Int => format!("SELECT ({call})::bigint::text"),
        RetKind::Float => format!("SELECT ({call})::float8"),
        RetKind::Bytea => format!("SELECT encode(({call}), 'hex')"),
        RetKind::TimestampTz => format!("SELECT (({call}) AT TIME ZONE 'UTC')::text"),
        RetKind::Interval => format!(
            "SELECT format('%s/%s/%s', \
               (extract(year from c) * 12 + extract(month from c))::bigint, \
               extract(day from c)::bigint, \
               (extract(hour from c) * 3600000000 + extract(minute from c) * 60000000 \
                + round(extract(second from c) * 1000000))::bigint) \
             FROM (SELECT ({call}) AS c) s"
        ),
        // Two columns: the decodable form, plus the raw array literal as a
        // fallback for results `Vec<Option<String>>` cannot hold (a
        // multi-dimensional array — the shape Basin has no type for either).
        RetKind::Array => format!("SELECT c::text[], c::text FROM (SELECT ({call}) AS c) s"),
    };
    (sql, params)
}

async fn pg_outcome(client: &Client, call_name: &str, args: &[ArgVal], ret: RetKind) -> Outcome {
    let (sql, params) = pg_sql(call_name, args, ret);
    let boxed: Vec<Box<dyn ToSql + Sync>> = params
        .into_iter()
        .map(|p| Box::new(p) as Box<dyn ToSql + Sync>)
        .collect();
    let refs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| b.as_ref()).collect();
    let row = match client.query_one(&sql, &refs).await {
        Ok(row) => row,
        Err(e) => return Outcome::Error(pg_err(&e)),
    };
    match ret {
        RetKind::Float => match row.try_get::<_, Option<f64>>(0) {
            Ok(v) => Outcome::Value(v.map(Val::F)),
            Err(e) => Outcome::Undecodable(e.to_string()),
        },
        RetKind::Array => match row.try_get::<_, Option<Vec<Option<String>>>>(0) {
            Ok(v) => Outcome::Value(v.map(Val::A)),
            // A multi-dimensional array: report the literal so the work queue
            // still carries the answer, flagged as the representation gap it
            // is on both sides.
            Err(_) => match row.try_get::<_, Option<String>>(1) {
                Ok(v) => Outcome::Undecodable(format!(
                    "multi-dimensional array {} — neither Vec<Option<String>> nor \
                     basin's one-dimensional arrow List can hold it",
                    v.unwrap_or_else(|| "NULL".to_string())
                )),
                Err(e) => Outcome::Undecodable(e.to_string()),
            },
        },
        RetKind::Numeric => match row.try_get::<_, Option<String>>(0) {
            Ok(v) => Outcome::Value(v.map(|s| Val::S(normalize_numeric(&s)))),
            Err(e) => Outcome::Undecodable(e.to_string()),
        },
        _ => match row.try_get::<_, Option<String>>(0) {
            Ok(v) => Outcome::Value(v.map(Val::S)),
            Err(e) => Outcome::Undecodable(e.to_string()),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The orphan table
// ─────────────────────────────────────────────────────────────────────────

/// One orphaned scalar function overload.
struct Spec {
    /// `pg_proc.proname`.
    name: &'static str,
    /// How the call is spelled in SQL. Differs from `name` only for
    /// `position`, whose bare form is parser syntax rather than a function
    /// call.
    call: &'static str,
    /// `pg_proc.oid`, dumped from the live PostgreSQL 18.2 — never recalled.
    oid: u32,
    /// `pg_proc.proargtypes`, for the `basin_pgtype::func::resolve` probe.
    /// `None` for a polymorphic signature (`anyarray`, `anycompatible`,
    /// ordered-set aggregates): there is no concrete OID list to resolve
    /// with. `Some(&[])` is a genuinely niladic function — `now()`,
    /// `random()`, `cume_dist()` — and *is* probed.
    arg_types: Option<&'static [u32]>,
    ret: RetKind,
    /// Session `TimeZone` for this spec. Only meaningful for the
    /// `timestamptz` overloads; `UTC` everywhere else.
    tz: &'static str,
    cases: Vec<(String, Vec<ArgVal>)>,
}

const T: u32 = 25; // text
const I4: u32 = 23; // int4
const I8: u32 = 20; // int8
const F8: u32 = 701; // float8
const NUM: u32 = 1700; // numeric
const TS: u32 = 1114; // timestamp
const TSTZ: u32 = 1184; // timestamptz
const IV: u32 = 1186; // interval
const BYTEA: u32 = 17;

fn c1<A: Clone>(a: &[(&'static str, A)], w: impl Fn(A) -> ArgVal) -> Vec<(String, Vec<ArgVal>)> {
    a.iter()
        .map(|(l, v)| ((*l).to_string(), vec![w(v.clone())]))
        .collect()
}

fn c2<A: Clone, B: Clone>(
    a: &[(&'static str, A)],
    b: &[(&'static str, B)],
    wa: impl Fn(A) -> ArgVal,
    wb: impl Fn(B) -> ArgVal,
) -> Vec<(String, Vec<ArgVal>)> {
    let mut out = Vec::new();
    for (la, va) in a {
        for (lb, vb) in b {
            out.push((format!("{la},{lb}"), vec![wa(va.clone()), wb(vb.clone())]));
        }
    }
    out
}

/// One-at-a-time design for 3+ argument functions — a fixed baseline call
/// with each battery value substituted into one position at a time. Same
/// rationale as `function_equivalence.rs`: a full cross of three batteries is
/// hundreds of round trips for marginal coverage, while OAT still exercises
/// every edge value in every position at least once.
fn oat(
    baseline: &[ArgVal],
    positions: &[(usize, Vec<(String, ArgVal)>)],
) -> Vec<(String, Vec<ArgVal>)> {
    let mut out = vec![("baseline".to_string(), baseline.to_vec())];
    for (idx, battery) in positions {
        for (label, v) in battery {
            let mut args = baseline.to_vec();
            args[*idx] = v.clone();
            out.push((format!("arg{idx}={label}"), args));
        }
    }
    out
}

fn labelled<A: Clone>(b: &[(&'static str, A)], w: impl Fn(A) -> ArgVal) -> Vec<(String, ArgVal)> {
    b.iter()
        .map(|(l, v)| ((*l).to_string(), w(v.clone())))
        .collect()
}

fn owned(cases: &[(&'static str, ArgVal)]) -> Vec<(String, ArgVal)> {
    cases
        .iter()
        .map(|(l, v)| ((*l).to_string(), v.clone()))
        .collect()
}

fn scalar_orphans() -> Vec<Spec> {
    let mut s: Vec<Spec> = Vec::new();

    let sp = |name, call, oid, arg_types: &'static [u32], ret, cases| Spec {
        // every `sp` signature is concrete; polymorphic ones are written out
        arg_types: Some(arg_types),
        name,
        call,
        oid,
        ret,
        tz: "UTC",
        cases,
    };

    // ── math: the six hyperbolics the previous inventory missed entirely,
    // plus cot/log10/pow. Every one is float8 -> float8 except the numeric
    // overloads. `acosh(0.5)` and `atanh(2)` error on the live server
    // ("input is out of range"); `cot(0)` is Infinity, not an error; and
    // `log10(0)`/`log10(-1)` error where `ln`/`log` in `eval.rs` today
    // return -inf/NaN — an error-vs-success trap this battery is aimed at.
    for (name, oid) in [
        ("cot", 1607u32),
        ("sinh", 2462),
        ("cosh", 2463),
        ("tanh", 2464),
        ("asinh", 2465),
        ("acosh", 2466),
        ("atanh", 2467),
        ("log10", 1194),
    ] {
        s.push(sp(
            name,
            name,
            oid,
            &[F8],
            RetKind::Float,
            c1(&f8(), ArgVal::Float8),
        ));
    }
    s.push(sp(
        "log10",
        "log10",
        1481,
        &[NUM],
        RetKind::Numeric,
        c1(&numeric_battery(), ArgVal::Numeric),
    ));
    s.push(sp(
        "pow",
        "pow",
        1346,
        &[F8, F8],
        RetKind::Float,
        c2(&f8(), &f8(), ArgVal::Float8, ArgVal::Float8),
    ));
    s.push(sp(
        "pow",
        "pow",
        1738,
        &[NUM, NUM],
        RetKind::Numeric,
        c2(
            &numeric_battery(),
            &numeric_battery(),
            ArgVal::Numeric,
            ArgVal::Numeric,
        ),
    ));

    // ── integer math. `factorial(-1)` errors; `factorial(0)` is 1;
    // `gcd(i32::MIN, 0)` and `lcm(2147483647, 2147483646)` both error with
    // `integer out of range` (verified) — the overflow arm a naive
    // implementation returns a wrapped value for.
    s.push(sp(
        "factorial",
        "factorial",
        1376,
        &[I8],
        RetKind::Numeric,
        c1(&i8b(), ArgVal::Int8),
    ));
    s.push(sp(
        "gcd",
        "gcd",
        5044,
        &[I4, I4],
        RetKind::Int,
        c2(&i4(), &i4(), ArgVal::Int4, ArgVal::Int4),
    ));
    s.push(sp(
        "lcm",
        "lcm",
        5046,
        &[I4, I4],
        RetKind::Int,
        c2(&i4(), &i4(), ArgVal::Int4, ArgVal::Int4),
    ));
    s.push(sp(
        "gcd",
        "gcd",
        5045,
        &[I8, I8],
        RetKind::Int,
        c2(&i8b(), &i8b(), ArgVal::Int8, ArgVal::Int8),
    ));
    s.push(sp(
        "lcm",
        "lcm",
        5047,
        &[I8, I8],
        RetKind::Int,
        c2(&i8b(), &i8b(), ArgVal::Int8, ArgVal::Int8),
    ));

    // ── string, single text argument ─────────────────────────────────────
    s.push(sp(
        "initcap",
        "initcap",
        872,
        &[T],
        RetKind::Text,
        c1(&txt(), ArgVal::Text),
    ));
    s.push(sp(
        "md5",
        "md5",
        2311,
        &[T],
        RetKind::Text,
        c1(&txt(), ArgVal::Text),
    ));
    for (name, oid) in [
        ("octet_length", 1374u32),
        ("bit_length", 1811),
        ("char_length", 1381),
        ("character_length", 1369),
    ] {
        s.push(sp(
            name,
            name,
            oid,
            &[T],
            RetKind::Int,
            c1(&txt(), ArgVal::Text),
        ));
    }

    // ── string, text + int ───────────────────────────────────────────────
    s.push(sp(
        "repeat",
        "repeat",
        1622,
        &[T, I4],
        RetKind::Text,
        c2(&txt(), &len_battery(), ArgVal::Text, ArgVal::Int4),
    ));
    s.push(sp(
        "lpad",
        "lpad",
        879,
        &[T, I4],
        RetKind::Text,
        c2(&txt(), &len_battery(), ArgVal::Text, ArgVal::Int4),
    ));
    s.push(sp(
        "rpad",
        "rpad",
        880,
        &[T, I4],
        RetKind::Text,
        c2(&txt(), &len_battery(), ArgVal::Text, ArgVal::Int4),
    ));
    s.push(sp(
        "substring",
        "substring",
        937,
        &[T, I4],
        RetKind::Text,
        c2(&txt(), &i4(), ArgVal::Text, ArgVal::Int4),
    ));

    // ── string, text + text ──────────────────────────────────────────────
    s.push(sp(
        "starts_with",
        "starts_with",
        3696,
        &[T, T],
        RetKind::Bool,
        c2(&txt(), &txt_small(), ArgVal::Text, ArgVal::Text),
    ));
    // Functional notation reverses the `position(needle IN haystack)` reading:
    // `pg_catalog.position('b', 'abc')` is 0 (verified). Both orders are in
    // the cross, so an implementation that reads the arguments the SQL-syntax
    // way instead of the pg_proc way fails here rather than in production.
    s.push(sp(
        "position",
        "pg_catalog.position",
        849,
        &[T, T],
        RetKind::Int,
        c2(&txt(), &txt_small(), ArgVal::Text, ArgVal::Text),
    ));
    s.push(sp(
        "string_to_array",
        "string_to_array",
        394,
        &[T, T],
        RetKind::Array,
        c2(&txt(), &txt_small(), ArgVal::Text, ArgVal::Text),
    ));

    // ── to_hex: two's-complement over the argument's own width ───────────
    s.push(sp(
        "to_hex",
        "to_hex",
        2089,
        &[I4],
        RetKind::Text,
        c1(&i4(), ArgVal::Int4),
    ));
    s.push(sp(
        "to_hex",
        "to_hex",
        2090,
        &[I8],
        RetKind::Text,
        c1(&i8b(), ArgVal::Int8),
    ));

    // ── regex ────────────────────────────────────────────────────────────
    let patterns: Vec<(&'static str, Option<String>)> = vec![
        ("null", None),
        ("empty", Some(String::new())),
        ("literal", Some("o".to_string())),
        ("anchored", Some("^The".to_string())),
        ("class", Some("[aeiou]".to_string())),
        ("greedy_star", Some(".*".to_string())),
        ("alternation", Some("a|b".to_string())),
        ("invalid", Some("(".to_string())),
        ("backref", Some(r"(\w)\1".to_string())),
    ];
    s.push(sp(
        "regexp_count",
        "regexp_count",
        6254,
        &[T, T],
        RetKind::Int,
        c2(&txt(), &patterns, ArgVal::Text, ArgVal::Text),
    ));
    s.push(sp(
        "regexp_instr",
        "regexp_instr",
        6257,
        &[T, T],
        RetKind::Int,
        c2(&txt(), &patterns, ArgVal::Text, ArgVal::Text),
    ));
    s.push(sp(
        "regexp_like",
        "regexp_like",
        6263,
        &[T, T],
        RetKind::Bool,
        c2(&txt(), &patterns, ArgVal::Text, ArgVal::Text),
    ));

    // ── crypto over bytea. `md5(bytea)` returns *text* (hex), while
    // `sha256(bytea)` returns *bytea* — a return-type asymmetry inside one
    // family that is easy to get backwards.
    let bytes: Vec<(&'static str, Option<Vec<u8>>)> = vec![
        ("null", None),
        ("empty", Some(vec![])),
        ("ascii_abc", Some(b"abc".to_vec())),
        ("nul_byte", Some(vec![0x00])),
        ("high_bytes", Some(vec![0x00, 0xff, 0x41])),
        ("utf8_multibyte", Some("héllo世界".as_bytes().to_vec())),
    ];
    s.push(sp(
        "md5",
        "md5",
        2321,
        &[BYTEA],
        RetKind::Text,
        c1(&bytes, ArgVal::Bytea),
    ));
    for (name, oid) in [
        ("sha224", 3419u32),
        ("sha256", 3420),
        ("sha384", 3421),
        ("sha512", 3422),
    ] {
        s.push(sp(
            name,
            name,
            oid,
            &[BYTEA],
            RetKind::Bytea,
            c1(&bytes, ArgVal::Bytea),
        ));
    }
    s.push(sp(
        "octet_length",
        "octet_length",
        720,
        &[BYTEA],
        RetKind::Int,
        c1(&bytes, ArgVal::Bytea),
    ));

    // ── multi-argument string functions, one-at-a-time ───────────────────
    // lpad/rpad with an explicit fill: `lpad('hello', 10, '')` is 'hello'.
    for (name, oid) in [("lpad", 873u32), ("rpad", 874)] {
        s.push(sp(
            name,
            name,
            oid,
            &[T, I4, T],
            RetKind::Text,
            oat(
                &[
                    ArgVal::Text(Some("hello".into())),
                    ArgVal::Int4(Some(10)),
                    ArgVal::Text(Some("xy".into())),
                ],
                &[
                    (0, labelled(&txt(), ArgVal::Text)),
                    (1, labelled(&len_battery(), ArgVal::Int4)),
                    (2, labelled(&txt_small(), ArgVal::Text)),
                ],
            ),
        ));
    }
    // overlay/3: `from 0` and `from -2` ERROR ("negative substring length not
    // allowed"); `from 99` appends.
    s.push(sp(
        "overlay",
        "overlay",
        1405,
        &[T, T, I4],
        RetKind::Text,
        oat(
            &[
                ArgVal::Text(Some("abcdef".into())),
                ArgVal::Text(Some("XY".into())),
                ArgVal::Int4(Some(3)),
            ],
            &[
                (0, labelled(&txt(), ArgVal::Text)),
                (1, labelled(&txt_small(), ArgVal::Text)),
                (
                    2,
                    labelled(
                        &[
                            ("null", None),
                            ("zero", Some(0)),
                            ("neg", Some(-2)),
                            ("one", Some(1)),
                            ("mid", Some(3)),
                            ("past_end", Some(99)),
                            ("min", Some(i32::MIN)),
                            ("max", Some(i32::MAX)),
                        ],
                        ArgVal::Int4,
                    ),
                ),
            ],
        ),
    ));
    // overlay/4: `for -1` does NOT error — it returns 'abXYbcdef'.
    s.push(sp(
        "overlay",
        "overlay",
        1404,
        &[T, T, I4, I4],
        RetKind::Text,
        oat(
            &[
                ArgVal::Text(Some("abcdef".into())),
                ArgVal::Text(Some("XY".into())),
                ArgVal::Int4(Some(3)),
                ArgVal::Int4(Some(2)),
            ],
            &[
                (0, labelled(&txt(), ArgVal::Text)),
                (2, labelled(&len_battery(), ArgVal::Int4)),
                (3, labelled(&len_battery(), ArgVal::Int4)),
            ],
        ),
    ));
    s.push(sp(
        "substring",
        "substring",
        936,
        &[T, I4, I4],
        RetKind::Text,
        oat(
            &[
                ArgVal::Text(Some("abcdef".into())),
                ArgVal::Int4(Some(2)),
                ArgVal::Int4(Some(3)),
            ],
            &[
                (0, labelled(&txt(), ArgVal::Text)),
                (1, labelled(&i4(), ArgVal::Int4)),
                (2, labelled(&len_battery(), ArgVal::Int4)),
            ],
        ),
    ));
    // concat_ws: a NULL separator makes the whole call NULL; NULL arguments
    // are skipped rather than propagating (verified).
    s.push(sp(
        "concat_ws",
        "concat_ws",
        3059,
        &[T, T, T],
        RetKind::Text,
        oat(
            &[
                ArgVal::Text(Some("-".into())),
                ArgVal::Text(Some("a".into())),
                ArgVal::Text(Some("b".into())),
            ],
            &[
                (0, labelled(&txt_small(), ArgVal::Text)),
                (1, labelled(&txt(), ArgVal::Text)),
                (2, labelled(&txt(), ArgVal::Text)),
            ],
        ),
    ));
    // string_to_array/3: the third argument is the token that becomes NULL.
    s.push(sp(
        "string_to_array",
        "string_to_array",
        376,
        &[T, T, T],
        RetKind::Array,
        oat(
            &[
                ArgVal::Text(Some("a,NULL,c".into())),
                ArgVal::Text(Some(",".into())),
                ArgVal::Text(Some("NULL".into())),
            ],
            &[
                (0, labelled(&txt(), ArgVal::Text)),
                (1, labelled(&txt_small(), ArgVal::Text)),
                (2, labelled(&txt_small(), ArgVal::Text)),
            ],
        ),
    ));

    // ── arrays ───────────────────────────────────────────────────────────
    s.push(Spec {
        name: "array_length",
        call: "array_length",
        oid: 2176,
        arg_types: None,
        ret: RetKind::Int,
        tz: "UTC",
        cases: {
            let dims: Vec<(&'static str, Option<i32>)> = vec![
                ("null", None),
                ("dim_zero", Some(0)),
                ("dim_one", Some(1)),
                ("dim_two", Some(2)),
                ("dim_neg", Some(-1)),
                ("dim_min", Some(i32::MIN)),
            ];
            let mut out = Vec::new();
            for (la, va) in int_array_cases() {
                for (ld, vd) in &dims {
                    out.push((format!("{la},{ld}"), vec![va.clone(), ArgVal::Int4(*vd)]));
                }
            }
            out
        },
    });
    s.push(Spec {
        name: "cardinality",
        call: "cardinality",
        oid: 3179,
        arg_types: None,
        ret: RetKind::Int,
        tz: "UTC",
        cases: owned(&int_array_cases())
            .into_iter()
            .map(|(l, v)| (l, vec![v]))
            .collect(),
    });
    s.push(Spec {
        name: "array_ndims",
        call: "array_ndims",
        oid: 748,
        arg_types: None,
        ret: RetKind::Int,
        tz: "UTC",
        cases: owned(&int_array_cases())
            .into_iter()
            .map(|(l, v)| (l, vec![v]))
            .collect(),
    });
    // array_sort's two-argument overload is the only orphan taking a boolean
    // argument; `descending => NULL` is the case a three-valued signature gets
    // wrong.
    s.push(Spec {
        name: "array_sort",
        call: "array_sort",
        oid: 6389,
        arg_types: None,
        ret: RetKind::Array,
        tz: "UTC",
        cases: {
            let mut out = Vec::new();
            for (la, va) in int_array_cases() {
                for (lb, vb) in [("null", None), ("asc", Some(false)), ("desc", Some(true))] {
                    out.push((format!("{la},{lb}"), vec![va.clone(), ArgVal::Bool(vb)]));
                }
            }
            out
        },
    });
    for (name, oid, ret) in [
        ("array_reverse", 6381u32, RetKind::Array),
        ("array_sort", 6388, RetKind::Array),
    ] {
        s.push(Spec {
            name,
            call: name,
            oid,
            arg_types: None,
            ret,
            tz: "UTC",
            cases: owned(&int_array_cases())
                .into_iter()
                .map(|(l, v)| (l, vec![v]))
                .collect(),
        });
    }
    // array_position / array_positions / array_remove / array_append:
    // element-and-array pairs, including a NULL element (`array_position`
    // finds a NULL element — it returns 2 for `{a,NULL,c}`, it does not
    // return NULL).
    let elems: Vec<(&'static str, Option<i32>)> = vec![
        ("null_elem", None),
        ("present", Some(1)),
        ("absent", Some(99)),
        ("duplicate", Some(2)),
        ("min", Some(i32::MIN)),
    ];
    for (name, oid, ret) in [
        ("array_position", 3277u32, RetKind::Int),
        ("array_positions", 3279, RetKind::Array),
        ("array_remove", 3167, RetKind::Array),
        ("array_append", 378, RetKind::Array),
    ] {
        s.push(Spec {
            name,
            call: name,
            oid,
            arg_types: None,
            ret,
            tz: "UTC",
            cases: {
                let mut out = Vec::new();
                for (la, va) in int_array_cases() {
                    for (le, ve) in &elems {
                        out.push((format!("{la},{le}"), vec![va.clone(), ArgVal::Int4(*ve)]));
                    }
                }
                out
            },
        });
    }
    s.push(Spec {
        name: "array_prepend",
        call: "array_prepend",
        oid: 379,
        arg_types: None,
        ret: RetKind::Array,
        tz: "UTC",
        cases: {
            let mut out = Vec::new();
            for (le, ve) in &elems {
                for (la, va) in int_array_cases() {
                    out.push((format!("{le},{la}"), vec![ArgVal::Int4(*ve), va.clone()]));
                }
            }
            out
        },
    });
    s.push(Spec {
        name: "array_cat",
        call: "array_cat",
        oid: 383,
        arg_types: None,
        ret: RetKind::Array,
        tz: "UTC",
        cases: {
            let mut out = Vec::new();
            for (la, va) in int_array_cases() {
                for (lb, vb) in int_array_cases() {
                    out.push((format!("{la},{lb}"), vec![va.clone(), vb.clone()]));
                }
            }
            out
        },
    });
    s.push(Spec {
        name: "array_replace",
        call: "array_replace",
        oid: 3168,
        arg_types: None,
        ret: RetKind::Array,
        tz: "UTC",
        cases: oat(
            &[
                ArgVal::Int4Array(Some(vec![Some(1), Some(2), Some(2), Some(3)])),
                ArgVal::Int4(Some(2)),
                ArgVal::Int4(Some(9)),
            ],
            &[
                (0, owned(&int_array_cases())),
                (1, labelled(&elems, ArgVal::Int4)),
                (2, labelled(&elems, ArgVal::Int4)),
            ],
        ),
    });
    // array_to_string: NULL elements are *dropped* by the two-argument form
    // and replaced by the third argument when it is given (verified).
    s.push(Spec {
        name: "array_to_string",
        call: "array_to_string",
        oid: 395,
        arg_types: None,
        ret: RetKind::Text,
        tz: "UTC",
        cases: {
            let mut out = Vec::new();
            for (la, va) in text_array_cases() {
                for (ld, vd) in txt_small() {
                    out.push((format!("{la},{ld}"), vec![va.clone(), ArgVal::Text(vd)]));
                }
            }
            out
        },
    });
    s.push(Spec {
        name: "array_to_string",
        call: "array_to_string",
        oid: 384,
        arg_types: None,
        ret: RetKind::Text,
        tz: "UTC",
        cases: oat(
            &[
                ArgVal::TextArray(Some(vec![Some("a".into()), None, Some("c".into())])),
                ArgVal::Text(Some(",".into())),
                ArgVal::Text(Some("X".into())),
            ],
            &[
                (0, owned(&text_array_cases())),
                (1, labelled(&txt_small(), ArgVal::Text)),
                (2, labelled(&txt_small(), ArgVal::Text)),
            ],
        ),
    });

    // ── datetime ─────────────────────────────────────────────────────────
    // date_trunc / date_part on `timestamp without time zone`: session-zone
    // independent, so one UTC pass is enough.
    s.push(sp(
        "date_trunc",
        "date_trunc",
        2020,
        &[T, TS],
        RetKind::Timestamp,
        {
            let mut out = Vec::new();
            for (lu, vu) in trunc_units() {
                for (lt, vt) in ts_cases() {
                    out.push((format!("{lu},{lt}"), vec![ArgVal::Text(vu.clone()), vt]));
                }
            }
            out
        },
    ));
    s.push(sp(
        "date_part",
        "date_part",
        2021,
        &[T, TS],
        RetKind::Float,
        {
            let mut out = Vec::new();
            for (lf, vf) in part_fields() {
                for (lt, vt) in ts_cases() {
                    out.push((format!("{lf},{lt}"), vec![ArgVal::Text(vf.clone()), vt]));
                }
            }
            out
        },
    ));
    s.push(sp(
        "date_part",
        "date_part",
        1172,
        &[T, IV],
        RetKind::Float,
        c2(
            &part_fields(),
            &iv_battery(),
            ArgVal::Text,
            ArgVal::Interval,
        ),
    ));
    s.push(sp(
        "date_trunc",
        "date_trunc",
        1218,
        &[T, IV],
        RetKind::Interval,
        c2(
            &trunc_units(),
            &iv_battery(),
            ArgVal::Text,
            ArgVal::Interval,
        ),
    ));
    // The timezone-dependent overloads. Three zones: UTC, a whole-hour DST
    // zone, and a half-hour-offset zone whose DST step is also half an hour.
    for tz in ["UTC", "America/New_York", "Australia/Lord_Howe"] {
        s.push(Spec {
            name: "date_trunc",
            call: "date_trunc",
            oid: 1217,
            arg_types: Some(&[T, TSTZ]),
            ret: RetKind::TimestampTz,
            tz,
            cases: {
                let mut out = Vec::new();
                for (lu, vu) in trunc_units() {
                    for (lt, vt) in tstz_cases() {
                        out.push((
                            format!("tz={tz},{lu},{lt}"),
                            vec![ArgVal::Text(vu.clone()), vt],
                        ));
                    }
                }
                out
            },
        });
        s.push(Spec {
            name: "date_part",
            call: "date_part",
            oid: 1171,
            arg_types: Some(&[T, TSTZ]),
            ret: RetKind::Float,
            tz,
            cases: {
                let mut out = Vec::new();
                for (lf, vf) in part_fields() {
                    for (lt, vt) in tstz_cases() {
                        out.push((
                            format!("tz={tz},{lf},{lt}"),
                            vec![ArgVal::Text(vf.clone()), vt],
                        ));
                    }
                }
                out
            },
        });
    }
    // make_date: `make_date(0, 1, 1)` and `make_date(2024, 2, 30)` both error
    // ("date field value out of range"), and negative years mean BC.
    s.push(sp(
        "make_date",
        "make_date",
        3846,
        &[I4, I4, I4],
        RetKind::Date,
        oat(
            &[
                ArgVal::Int4(Some(2024)),
                ArgVal::Int4(Some(3)),
                ArgVal::Int4(Some(10)),
            ],
            &[
                (
                    0,
                    labelled(
                        &[
                            ("null", None),
                            ("zero", Some(0)),
                            ("bc", Some(-1)),
                            ("one", Some(1)),
                            ("max", Some(i32::MAX)),
                        ],
                        ArgVal::Int4,
                    ),
                ),
                (
                    1,
                    labelled(
                        &[
                            ("null", None),
                            ("zero", Some(0)),
                            ("feb", Some(2)),
                            ("dec", Some(12)),
                            ("thirteen", Some(13)),
                        ],
                        ArgVal::Int4,
                    ),
                ),
                (
                    2,
                    labelled(
                        &[
                            ("null", None),
                            ("zero", Some(0)),
                            ("leap_29", Some(29)),
                            ("thirty", Some(30)),
                            ("thirty_two", Some(32)),
                        ],
                        ArgVal::Int4,
                    ),
                ),
            ],
        ),
    ));
    // date_part over `date` inputs is not a separate orphan (oid 1384 shares
    // the name), but it is the overload most callers hit, so it is measured.
    s.push(sp(
        "date_part",
        "date_part",
        1384,
        &[T, 1082],
        RetKind::Float,
        c2(&part_fields(), &date_battery(), ArgVal::Text, ArgVal::Date),
    ));

    s
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate and window orphans — resolution probe only
// ─────────────────────────────────────────────────────────────────────────

/// An aggregate or window orphan. There is no runtime probe: `AggFunc`
/// (`aggregate.rs:82`) and `WindowFunc` (`window.rs:243`) are closed enums
/// with no variant for any of these, so the call cannot be *constructed*, let
/// alone evaluated. What is measured is (a) whether
/// `basin_pgtype::func::resolve` knows the signature and (b) the value
/// PostgreSQL 18.2 produces over a fixed dataset, so the work queue carries
/// the number the implementation has to reproduce.
struct AggSpec {
    name: &'static str,
    oid: u32,
    arg_types: Option<&'static [u32]>,
    kind: &'static str,
    /// A complete statement returning one text column.
    pg_sql: &'static str,
}

const AGG_ORPHANS: &[AggSpec] = &[
    AggSpec { name: "bool_and", oid: 2517, arg_types: Some(&[16]), kind: "aggregate",
        pg_sql: "SELECT bool_and(b)::text FROM (VALUES (true),(false),(NULL)) t(b)" },
    AggSpec { name: "bool_or", oid: 2518, arg_types: Some(&[16]), kind: "aggregate",
        pg_sql: "SELECT bool_or(b)::text FROM (VALUES (true),(false),(NULL)) t(b)" },
    AggSpec { name: "stddev", oid: 2158, arg_types: Some(&[F8]), kind: "aggregate",
        pg_sql: "SELECT stddev(x)::text FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "stddev_pop", oid: 2728, arg_types: Some(&[F8]), kind: "aggregate",
        pg_sql: "SELECT stddev_pop(x)::text FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "stddev_samp", oid: 2716, arg_types: Some(&[F8]), kind: "aggregate",
        pg_sql: "SELECT stddev_samp(x)::text FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "var_pop", oid: 2722, arg_types: Some(&[F8]), kind: "aggregate",
        pg_sql: "SELECT var_pop(x)::text FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "var_samp", oid: 2645, arg_types: Some(&[F8]), kind: "aggregate",
        pg_sql: "SELECT var_samp(x)::text FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "corr", oid: 2829, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT corr(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "covar_pop", oid: 2827, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT covar_pop(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "covar_samp", oid: 2828, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT covar_samp(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_slope", oid: 2825, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_slope(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_intercept", oid: 2826, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_intercept(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_count", oid: 2818, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_count(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_r2", oid: 2824, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_r2(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_avgx", oid: 2822, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_avgx(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_avgy", oid: 2823, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_avgy(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_sxx", oid: 2819, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_sxx(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_syy", oid: 2820, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_syy(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "regr_sxy", oid: 2821, arg_types: Some(&[F8, F8]), kind: "aggregate",
        pg_sql: "SELECT regr_sxy(y, x)::text FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y)" },
    AggSpec { name: "bit_and", oid: 2238, arg_types: Some(&[I4]), kind: "aggregate",
        pg_sql: "SELECT bit_and(x)::text FROM (VALUES (12),(10),(NULL)) t(x)" },
    AggSpec { name: "bit_or", oid: 2239, arg_types: Some(&[I4]), kind: "aggregate",
        pg_sql: "SELECT bit_or(x)::text FROM (VALUES (12),(10),(NULL)) t(x)" },
    AggSpec { name: "bit_xor", oid: 6165, arg_types: Some(&[I4]), kind: "aggregate",
        pg_sql: "SELECT bit_xor(x)::text FROM (VALUES (12),(10),(NULL)) t(x)" },
    // The shape trap: PostgreSQL's percentile_cont is an ORDERED-SET
    // aggregate — `percentile_cont(f) WITHIN GROUP (ORDER BY x)` — while
    // DataFusion's is a plain two-argument aggregate. Re-hosting it as
    // `percentile_cont(f, x)` would be a different function with the same
    // name. The SQL below is the Postgres spelling, deliberately.
    AggSpec { name: "percentile_cont", oid: 3974, arg_types: None, kind: "ordered-set aggregate",
        pg_sql: "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)::text \
                 FROM (VALUES (1::float8),(2),(4),(NULL)) t(x)" },
    AggSpec { name: "cume_dist", oid: 3104, arg_types: Some(&[]), kind: "window",
        pg_sql: "SELECT string_agg(cd::text, ',' ORDER BY x) FROM \
                 (SELECT x, cume_dist() OVER (ORDER BY x) cd \
                  FROM (VALUES (1),(2),(2),(4)) t(x)) s" },
    AggSpec { name: "percent_rank", oid: 3103, arg_types: Some(&[]), kind: "window",
        pg_sql: "SELECT string_agg(pr::text, ',' ORDER BY x) FROM \
                 (SELECT x, percent_rank() OVER (ORDER BY x) pr \
                  FROM (VALUES (1),(2),(2),(4)) t(x)) s" },
    AggSpec { name: "ntile", oid: 3105, arg_types: Some(&[I4]), kind: "window",
        pg_sql: "SELECT string_agg(n::text, ',' ORDER BY x) FROM \
                 (SELECT x, ntile(3) OVER (ORDER BY x) n \
                  FROM (VALUES (1),(2),(3),(4),(5)) t(x)) s" },
];

// ─────────────────────────────────────────────────────────────────────────
// Harness self-check
// ─────────────────────────────────────────────────────────────────────────

/// Prove the Basin-side renderers and the battery's hardcoded physical values
/// agree with the live server *before* testing anything. Everything under
/// test errors today, so without this the renderers are unexercised code that
/// would start manufacturing false divergences the day a function lands.
async fn render_self_check(client: &Client) -> Result<usize, String> {
    let mut checked = 0usize;
    let fail = |what: String| -> Result<(), String> { Err(what) };

    // Dates and timestamps: render the hardcoded physical value, ask the
    // server for the literal's text, require equality.
    for v in date_battery().into_iter().filter_map(|(_, v)| v) {
        let (lit, days) = v;
        let want: String = client
            .query_one("SELECT ($1::text::date)::text", &[&lit])
            .await
            .map_err(|e| format!("self-check date {lit}: {e}"))?
            .get(0);
        let got = fmt_date(days);
        if got != want {
            fail(format!(
                "SELF-CHECK FAILED: date literal {lit} (days={days}) renders as {got:?}, \
                 postgres says {want:?}"
            ))?;
        }
        checked += 1;
    }
    for v in ts_battery().into_iter().filter_map(|(_, v)| v) {
        let (lit, micros) = v;
        let want: String = client
            .query_one("SELECT ($1::text::timestamp)::text", &[&lit])
            .await
            .map_err(|e| format!("self-check timestamp {lit}: {e}"))?
            .get(0);
        let got = fmt_timestamp(micros);
        if got != want {
            fail(format!(
                "SELF-CHECK FAILED: timestamp literal {lit} (micros={micros}) renders as \
                 {got:?}, postgres says {want:?}"
            ))?;
        }
        checked += 1;
    }
    for v in iv_battery().into_iter().filter_map(|(_, v)| v) {
        let (lit, months, days, micros) = v;
        let want: String = client
            .query_one(
                "SELECT format('%s/%s/%s', \
                    (extract(year from c) * 12 + extract(month from c))::bigint, \
                    extract(day from c)::bigint, \
                    (extract(hour from c) * 3600000000 + extract(minute from c) * 60000000 \
                     + round(extract(second from c) * 1000000))::bigint) \
                 FROM (SELECT $1::text::interval AS c) s",
                &[&lit],
            )
            .await
            .map_err(|e| format!("self-check interval {lit}: {e}"))?
            .get(0);
        let got = fmt_interval(IntervalMonthDayNanoType::make_value(
            months,
            days,
            micros * 1_000,
        ));
        if got != want {
            fail(format!(
                "SELF-CHECK FAILED: interval literal {lit} ({months},{days},{micros}) renders \
                 as {got:?}, postgres says {want:?}"
            ))?;
        }
        checked += 1;
    }
    // Bytea hex and array element rendering, through the same code paths the
    // comparison uses.
    for bytes in [vec![], vec![0x00u8], vec![0x00, 0xff, 0x41]] {
        let want: String = client
            .query_one(
                "SELECT encode(decode($1::text, 'hex'), 'hex')",
                &[&hex(&bytes)],
            )
            .await
            .map_err(|e| format!("self-check bytea: {e}"))?
            .get(0);
        if hex(&bytes) != want {
            fail(format!(
                "SELF-CHECK FAILED: bytea {:?} renders as {:?}, postgres says {want:?}",
                bytes,
                hex(&bytes)
            ))?;
        }
        checked += 1;
    }
    for (label, v) in text_array_cases() {
        let ArgVal::TextArray(Some(ref elems)) = v else {
            continue;
        };
        let (_, col) = v.arrow_column()?;
        let rendered = render_list(&col)?;
        let want: Option<Vec<Option<String>>> = client
            .query_one(
                "SELECT ($1::text::text[])",
                &[&v.pg_param().flatten().unwrap()],
            )
            .await
            .map_err(|e| format!("self-check text[] {label}: {e}"))?
            .get(0);
        if rendered.as_deref() != want.as_deref() {
            fail(format!(
                "SELF-CHECK FAILED: text[] case {label} ({elems:?}) renders as {rendered:?}, \
                 postgres round-trips to {want:?}"
            ))?;
        }
        checked += 1;
    }
    Ok(checked)
}

// ─────────────────────────────────────────────────────────────────────────
// Non-deterministic probes
// ─────────────────────────────────────────────────────────────────────────

/// `random()` — oid 1598, `provolatile = 'v'`. Never compared by value.
/// Postgres's contract, verified live: the result type is `double precision`
/// and 10,000 draws all satisfy `r >= 0 AND r < 1`.
fn probe_random() -> Vec<String> {
    const DRAWS: usize = 256;
    let mut failures = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut first_error: Option<String> = None;
    for _ in 0..DRAWS {
        match safe_basin_call(1598, &[]) {
            Err(e) => {
                first_error.get_or_insert(e);
                break;
            }
            Ok(arr) => {
                let Some(a) = arr.as_any().downcast_ref::<Float64Array>() else {
                    failures.push(format!(
                        "random() must return float8 (postgres: pg_typeof(random()) = \
                         'double precision'); basin returned {:?}",
                        arr.data_type()
                    ));
                    break;
                };
                if a.is_null(0) {
                    failures.push("random() returned NULL; postgres never does".to_string());
                    break;
                }
                let v = a.value(0);
                if !(0.0..1.0).contains(&v) {
                    failures.push(format!(
                        "random() returned {v}, outside postgres's [0, 1) range"
                    ));
                }
                seen.insert(v.to_bits());
            }
        }
    }
    if let Some(e) = first_error {
        failures.push(format!("random() is not implemented in basin-exec: {e}"));
    } else if seen.len() < DRAWS / 2 {
        failures.push(format!(
            "random() produced only {} distinct values in {DRAWS} draws — a constant or \
             low-entropy generator passes a range check but is not random",
            seen.len()
        ));
    }
    failures
}

/// `now()` — oid 1299, `provolatile = 's'`. Postgres's contract, verified
/// live: type is `timestamp with time zone`, and `now() = (SELECT now())` is
/// true inside a transaction.
///
/// Transaction stability is deliberately *not* asserted: `eval()` has no
/// transaction and no session context to be stable within
/// (`docs/migration/df-removal/17-udf-rehosting.md` §5.4). That is a design
/// prerequisite, not a bug this harness can catch, and saying so here keeps it
/// from being quietly assumed solved.
fn probe_now() -> Vec<String> {
    let mut failures = Vec::new();
    match safe_basin_call(1299, &[]) {
        Err(e) => failures.push(format!("now() is not implemented in basin-exec: {e}")),
        Ok(arr) => {
            match arr.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {}
                other => failures.push(format!(
                    "now() must return timestamptz — basin_pgtype::physical maps it to \
                     Timestamp(Microsecond, Some(\"UTC\")); basin returned {other:?}"
                )),
            }
            if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                if a.is_null(0) {
                    failures.push("now() returned NULL".to_string());
                } else {
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as i64)
                        .unwrap_or(0);
                    let skew = (a.value(0) - now_us).abs();
                    if skew > 300_000_000 {
                        failures.push(format!(
                            "now() returned {} ({}), {} s from the system clock",
                            a.value(0),
                            fmt_timestamp(a.value(0)),
                            skew / 1_000_000
                        ));
                    }
                }
            }
        }
    }
    // Within one batch, every row must see the same instant.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("_d", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(0), Some(1), Some(2)])) as ArrayRef],
    )
    .expect("3-row dummy batch");
    let expr = Expr::ScalarFn {
        func: FuncId(Oid(1299)),
        args: vec![],
    };
    if let Ok(arr) = eval(&expr, &batch) {
        if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            if a.len() == 3 && (a.value(0) != a.value(1) || a.value(1) != a.value(2)) {
                failures.push(
                    "now() returned different instants for different rows of one batch — \
                     postgres's now() is stable for the whole statement"
                        .to_string(),
                );
            }
        }
    }
    failures
}

// ─────────────────────────────────────────────────────────────────────────
// Driver
// ─────────────────────────────────────────────────────────────────────────

struct Divergence {
    case: String,
    kind: &'static str,
    basin: String,
    pg: String,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Class {
    /// Not in `basin_pgtype::func::FUNCS` — the planner cannot name it.
    Unresolvable,
    /// Resolves, but every call errors in `eval`.
    Unimplemented,
    /// Executes and returns a different answer for at least one input.
    Divergent,
    /// Every case matched.
    Matches,
}

struct Report {
    key: String,
    class: Class,
    resolution: String,
    cases: usize,
    divergences: Vec<Divergence>,
}

/// The number of distinct orphaned `pg_catalog` function names the census in
/// this file's module docs measured. Checked against the tables below on every
/// run so a deleted spec shows up as coverage drift rather than as a quietly
/// smaller number.
const CENSUS_ORPHAN_NAMES: usize = 79;

fn print_banner(msg: &str) {
    println!("=== orphan_functions: {msg} ===");
}

/// `basin_pgtype::func::resolve` for this signature, rendered for the report.
/// An empty `arg_types` means the signature is polymorphic in `pg_proc`
/// (`anyarray`/`anycompatible`) and has no concrete OID list to probe with.
fn resolution_of(name: &str, arg_types: Option<&[u32]>, oid: u32) -> (bool, String) {
    let Some(arg_types) = arg_types else {
        // A polymorphic pg_proc signature (anyarray / anycompatible, or an
        // ordered-set aggregate) has no concrete OID list to resolve with.
        // Reported as a fact about the probe, not as a result: what is
        // certain is that FUNCS has no row under this name at all.
        let known = basin_pgtype::func::FUNCS.iter().any(|f| f.name == name);
        return (
            false,
            format!(
                "polymorphic signature — resolve() not probed; \
                 basin_pgtype::func::FUNCS has {} row(s) named {name:?} (postgres oid {oid})",
                if known { "some" } else { "NO" }
            ),
        );
    };
    let args: Vec<Oid> = arg_types.iter().map(|o| Oid(*o)).collect();
    match basin_pgtype::func::resolve(name, &args) {
        None => (
            false,
            format!(
                "basin_pgtype::func::resolve({name:?}, {arg_types:?}) -> None — no pg_proc row; \
                 the owned planner cannot build this call"
            ),
        ),
        Some(f) => (
            true,
            format!(
                "basin_pgtype::func::resolve({name:?}, {arg_types:?}) -> oid {}{}",
                f.oid.get(),
                if f.oid.get() == oid {
                    String::new()
                } else {
                    format!(" (WRONG — postgres says {oid})")
                }
            ),
        ),
    }
}

async fn run() -> Result<(), String> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            print_banner(
                "SKIPPED — PG_DIFF_TEST_DSN is unset. 0 of the 79 orphaned pg_catalog functions \
                 were compared against a live server this run, and NOTHING about DataFusion's \
                 removability was verified. Set PG_DIFF_TEST_DSN to a libpq DSN (e.g. \
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

    let checked = render_self_check(&client).await?;
    print_banner(&format!(
        "harness self-check passed — {checked} renderings agree with the server"
    ));

    let specs = scalar_orphans();
    let mut reports: Vec<Report> = Vec::new();
    let mut total_cases = 0usize;
    let mut current_tz = String::new();

    for spec in &specs {
        if spec.tz != current_tz {
            client
                .batch_execute(&format!("SET TimeZone TO '{}'", spec.tz))
                .await
                .map_err(|e| format!("SET TimeZone TO '{}': {e}", spec.tz))?;
            current_tz = spec.tz.to_string();
        }

        let (resolves, resolution) = resolution_of(spec.name, spec.arg_types, spec.oid);
        let mut divergences = Vec::new();
        let mut any_success = false;

        for (case, args) in &spec.cases {
            total_cases += 1;
            let basin = match safe_basin_call(spec.oid, args) {
                Ok(arr) => match extract_basin(spec.ret, &arr) {
                    Ok(v) => {
                        any_success = true;
                        Outcome::Value(v)
                    }
                    Err(e) => Outcome::Error(format!("unrenderable result: {e}")),
                },
                Err(e) => Outcome::Error(e),
            };
            let pg = pg_outcome(&client, spec.call, args, spec.ret).await;
            if let Some((kind, b, p)) = compare(&basin, &pg) {
                divergences.push(Divergence {
                    case: case.clone(),
                    kind,
                    basin: b,
                    pg: p,
                });
            }
        }

        let class = if divergences.is_empty() {
            Class::Matches
        } else if !resolves {
            Class::Unresolvable
        } else if any_success {
            Class::Divergent
        } else {
            Class::Unimplemented
        };

        reports.push(Report {
            key: format!(
                "{}({}) [oid {}{}]",
                spec.name,
                match spec.arg_types {
                    Some(ts) => ts
                        .iter()
                        .map(|o| o.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    None => "polymorphic".to_string(),
                },
                spec.oid,
                if spec.tz == "UTC" {
                    String::new()
                } else {
                    format!(", TimeZone={}", spec.tz)
                }
            ),
            class,
            resolution,
            cases: spec.cases.len(),
            divergences,
        });
    }

    // ── the two volatile functions ──────────────────────────────────────
    for (name, oid, failures) in [
        ("random", 1598u32, probe_random()),
        ("now", 1299, probe_now()),
    ] {
        total_cases += 1;
        // Both are niladic in pg_proc, so `Some(&[])` — a real probe, not a
        // skipped one.
        let (resolves, resolution) = resolution_of(name, Some(&[]), oid);
        let class = if failures.is_empty() {
            Class::Matches
        } else if resolves {
            Class::Unimplemented
        } else {
            Class::Unresolvable
        };
        reports.push(Report {
            key: format!("{name}() [oid {oid}] — invariant probe, never value-compared"),
            class,
            resolution,
            cases: 1,
            divergences: failures
                .into_iter()
                .map(|reason| Divergence {
                    case: "invariant".to_string(),
                    kind: "invariant violated",
                    basin: reason,
                    pg: "(contract verified on the live server — see this probe's doc \
                         comment)"
                        .to_string(),
                })
                .collect(),
        });
    }

    // ── aggregate / window orphans: resolution + the value to reproduce ──
    client
        .batch_execute("SET TimeZone TO 'UTC'")
        .await
        .map_err(|e| format!("SET TimeZone TO 'UTC': {e}"))?;
    let mut agg_lines = Vec::new();
    for a in AGG_ORPHANS {
        let (_, resolution) = resolution_of(a.name, a.arg_types, a.oid);
        let want = match client.query_one(a.pg_sql, &[]).await {
            Ok(row) => row
                .get::<_, Option<String>>(0)
                .unwrap_or_else(|| "NULL".to_string()),
            Err(e) => format!("<postgres errored: {}>", pg_err(&e)),
        };
        agg_lines.push(format!(
            "  {:<16} oid {:<5} {:<22} postgres = {}\n      {}\n      {}",
            a.name,
            a.oid,
            format!("({})", a.kind),
            want,
            resolution,
            if a.kind == "window" {
                "basin: window.rs's WindowFunc enum has no variant — cannot be constructed"
            } else {
                "basin: aggregate.rs's AggFunc enum has no variant — cannot be constructed"
            }
        ));
    }

    // ── the metric, always printed ──────────────────────────────────────
    let n_unresolvable = reports
        .iter()
        .filter(|r| r.class == Class::Unresolvable)
        .count();
    let n_unimplemented = reports
        .iter()
        .filter(|r| r.class == Class::Unimplemented)
        .count();
    let n_divergent = reports
        .iter()
        .filter(|r| r.class == Class::Divergent)
        .count();
    let n_matches = reports.iter().filter(|r| r.class == Class::Matches).count();
    let n_bad_cases: usize = reports.iter().map(|r| r.divergences.len()).sum();

    // Coverage against the census: the module docs claim 79 orphaned
    // `pg_catalog` names. Recount it from the tables themselves every run, so
    // deleting a spec cannot quietly shrink the measured surface.
    let mut names: std::collections::BTreeSet<&str> = specs.iter().map(|s| s.name).collect();
    names.insert("random");
    names.insert("now");
    for a in AGG_ORPHANS {
        names.insert(a.name);
    }
    let coverage = if names.len() == CENSUS_ORPHAN_NAMES {
        format!("{CENSUS_ORPHAN_NAMES}/{CENSUS_ORPHAN_NAMES} censused names covered")
    } else {
        format!(
            "COVERAGE DRIFT — {} distinct names in this file vs {CENSUS_ORPHAN_NAMES} in the \
             census (see the module docs); re-run the census before trusting either number",
            names.len()
        )
    };

    print_banner(&format!(
        "{} scalar overloads probed, {total_cases} call sites compared, {} aggregate/window \
         orphans catalogued, {coverage} · UNRESOLVABLE {n_unresolvable} · UNIMPLEMENTED \
         {n_unimplemented} · DIVERGENT {n_divergent} · MATCHES {n_matches} · {n_bad_cases} \
         failing call sites",
        reports.len(),
        AGG_ORPHANS.len(),
    ));

    // ── the work queue ──────────────────────────────────────────────────
    let mut by_class: BTreeMap<&str, Vec<&Report>> = BTreeMap::new();
    for r in &reports {
        let k = match r.class {
            Class::Unresolvable => "UNRESOLVABLE — no pg_proc row in basin-pgtype::func::FUNCS",
            Class::Unimplemented => "UNIMPLEMENTED — resolves, but eval_scalar_fn has no arm",
            Class::Divergent => "DIVERGENT — executes and returns a different answer",
            Class::Matches => "MATCHES — done",
        };
        by_class.entry(k).or_default().push(r);
    }

    let mut out = String::new();
    out.push_str("\n╭─ WORK QUEUE ─────────────────────────────────────────────────────────╮\n");
    for (class, rs) in &by_class {
        out.push_str(&format!("\n### {class} — {} overload(s)\n", rs.len()));
        for r in rs {
            out.push_str(&format!(
                "\n{} — {} case(s), {} failing\n    {}\n",
                r.key,
                r.cases,
                r.divergences.len(),
                r.resolution
            ));
            // Group by (kind, basin outcome): every case of an unimplemented
            // function shares one Basin error, so printing it once and then
            // the per-case PostgreSQL answers is what turns a 400-line dump
            // into a readable specification. The counts are always complete
            // even where the case list is capped.
            let mut groups: BTreeMap<(&str, &str), Vec<&Divergence>> = BTreeMap::new();
            for d in &r.divergences {
                groups
                    .entry((d.kind, d.basin.as_str()))
                    .or_default()
                    .push(d);
            }
            for ((kind, basin), ds) in &groups {
                out.push_str(&format!(
                    "    {kind} — {} case(s)\n      basin  = {basin}\n",
                    ds.len()
                ));
                // Deliberately uncapped. This listing IS the specification
                // for the re-hosting tranche — `lpad`'s negative-length case
                // and `overlay`'s from-0 error are exactly the entries a
                // "first N cases" cap would drop, because they sit late in a
                // cross product. A long report is the point.
                for d in ds {
                    out.push_str(&format!("      [{}] postgres = {}\n", d.case, d.pg));
                }
            }
        }
    }
    out.push_str(&format!(
        "\n### AGGREGATE / WINDOW ORPHANS — {} name(s), probed at the resolution layer only\n\
         (AggFunc/WindowFunc are closed enums; these cannot be constructed, so there is no\n\
          runtime comparison to make. The postgres value each must eventually reproduce:)\n\n{}\n",
        AGG_ORPHANS.len(),
        agg_lines.join("\n")
    ));
    out.push_str("╰──────────────────────────────────────────────────────────────────────╯\n");
    println!("{out}");

    if n_bad_cases == 0 && n_unresolvable == 0 && n_unimplemented == 0 {
        print_banner(
            "PASS — every orphaned pg_catalog function is now answered by owned code and \
             matches PostgreSQL 18. `datafusion = \"53\"` is removable as far as scalar \
             function hosting is concerned.",
        );
        return Ok(());
    }

    Err(format!(
        "{n_bad_cases} of {total_cases} call sites across {} scalar overload(s) do not match \
         PostgreSQL 18.2, and {} aggregate/window orphans have no owned implementation at all. \
         THIS IS THE EXPECTED STATE until the re-hosting tranche lands — the listing above is \
         the work queue, not a regression. Do not narrow the battery to shrink this number.",
        reports.len() - n_matches,
        AGG_ORPHANS.len(),
    ))
}

fn main() {
    // No libtest (`harness = false`, see Cargo.toml): build a runtime by
    // hand, same as `function_equivalence.rs` and `catalog_fidelity.rs`.
    let rt = tokio::runtime::Runtime::new().expect("building a tokio runtime");
    match rt.block_on(run()) {
        Ok(()) => {}
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
    }
}
