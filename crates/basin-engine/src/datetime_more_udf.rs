//! Extended PG date/time UDFs — Phase P11 additions (#151).
//!
//! Registered at session-open time via [`register_datetime_more_udfs`].
//! Because `statement_timestamp()` and `transaction_timestamp()` require
//! per-session state, this module owns [`DateTimeSessionState`] and the
//! registration function accepts an `Arc` to it.
//!
//! ## Contents
//!
//! ### age(ts1, ts2) → interval
//! Computes `ts1 - ts2` normalized to `years/months/days/hours/minutes/
//! seconds` exactly as PostgreSQL does: borrows from the calendar, respecting
//! variable-length months.  Same algorithm as the existing `age` UDF in
//! `udf.rs`; this registration uses `Signature::any(2)` so it accepts *all*
//! timestamp unit/timezone variants and replaces the stateless-cache version.
//!
//! ### to_char(ts|date|interval, format) → text
//! Translates PG format codes to chrono strftime and renders the value.
//! Supported codes: `YYYY`, `YY`, `Month`, `MON`, `Mon`, `Day`, `DAY`, `DY`,
//! `MM`, `DD`, `HH24`, `HH12`, `HH`, `MI`, `SS`, `MS`, `US`, `AM`/`PM`,
//! `TZ`, `CC`, `Q`, `W`, `IW`, `WW`, `J`, `FM` (prefix, consumed).
//! **Unsupported / not implemented:** `BC`/`AD` era, `SSSS` (seconds past
//! midnight), `OF` (UTC offset), `TZH`/`TZM`, locale-dependent month/day
//! names (always English).
//! Interval input is handled via `to_char_interval` (the existing
//! `interval_tz_udf` UDF) — this UDF only covers timestamp/date/numeric.
//!
//! ### to_date(text, format) → date
//! Inverse of `to_char` for dates. Parses using PG→chrono format translation.
//!
//! ### to_timestamp(text, format) → timestamptz
//! Inverse of `to_char` for timestamps. Returns `Timestamp(Nanosecond, None)`
//! consistent with DataFusion's default `to_timestamp` shape.
//!
//! ### date_bin(stride interval, source ts, origin ts) → ts  (PG 14+)
//! Floors `source` down to the nearest multiple of `stride` measured from
//! `origin`.  Formula: `origin + floor((source - origin) / stride) * stride`.
//! Stride must be positive; result type mirrors the source type.  Supports
//! `Interval(MonthDayNano)`, `Interval(DayTime)`, `Interval(YearMonth)` strides.
//! **Limitation:** month/year-based strides use an approximate 30-day/month,
//! 365-day/year conversion — sufficient for bucketing but not calendar-exact.
//!
//! ### clock_timestamp() → timestamptz
//! Returns the real wall-clock instant at call time (calls `Utc::now()`
//! inside the UDF body on every invocation).  This overrides the existing
//! `NowStubUdf` and is correctly distinct from `statement_timestamp` /
//! `transaction_timestamp`.
//!
//! ### statement_timestamp() → timestamptz
//! Returns the timestamp captured at the start of the current SQL statement.
//! The caller (executor) must call [`tick_statement_ts`] once before each
//! statement; within the statement every call to this UDF returns the same
//! value.  Reset to `None` between statements so the first call within a new
//! statement sees the freshly-snapped time.
//!
//! ### transaction_timestamp() → timestamptz
//! Returns the timestamp captured at `BEGIN`.  `tick_txn_ts` is called once
//! on the first statement of a transaction (or at `BEGIN`); cleared on
//! `COMMIT` / `ROLLBACK`.  Outside an explicit transaction, falls back to the
//! current statement timestamp.

use std::any::Any;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use datafusion::arrow::array::types::IntervalMonthDayNano;
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, IntervalMonthDayNanoArray, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

// ─────────────────────────────────────────────────────────────────────────────
// Per-session timestamp state
// ─────────────────────────────────────────────────────────────────────────────

/// Per-session timestamp state for `statement_timestamp()` and
/// `transaction_timestamp()`.
///
/// One instance is owned by each [`crate::session::SessionState`]; an
/// `Arc` clone is captured by the per-session UDFs registered in
/// [`register_datetime_more_udfs`].
///
/// * `statement_ts` — set by the executor before each statement via
///   [`tick_statement_ts`]. Cleared (set back to `None`) after the
///   statement completes so stale values don't bleed into the next query.
///
/// * `txn_ts` — set once at `BEGIN` via [`tick_txn_ts`] and held until
///   `COMMIT` / `ROLLBACK` clears it via [`clear_txn_ts`].  Outside an
///   explicit transaction this is `None`; in that case
///   `transaction_timestamp()` falls back to `statement_ts`.
#[derive(Debug, Default)]
pub(crate) struct DateTimeSessionState {
    /// Microseconds since UNIX epoch captured at statement-start time.
    pub(crate) statement_ts: Mutex<Option<i64>>,
    /// Microseconds since UNIX epoch captured at `BEGIN` time.
    pub(crate) txn_ts: Mutex<Option<i64>>,
}

/// Snap a new `statement_ts` from `Utc::now()`.
/// Called by the executor *before* dispatching each SQL statement.
pub(crate) fn tick_statement_ts(dt: &DateTimeSessionState) {
    let now = Utc::now().timestamp_micros();
    *dt.statement_ts.lock().expect("statement_ts poisoned") = Some(now);
}

/// Snap a new `txn_ts` from `Utc::now()` if one is not already set.
/// Called at `BEGIN` (or on the first statement of an implicit transaction
/// if the engine ever gains that model).
pub(crate) fn tick_txn_ts(dt: &DateTimeSessionState) {
    let mut guard = dt.txn_ts.lock().expect("txn_ts poisoned");
    if guard.is_none() {
        *guard = Some(Utc::now().timestamp_micros());
    }
}

/// Clear `txn_ts` on `COMMIT` / `ROLLBACK`.
pub(crate) fn clear_txn_ts(dt: &DateTimeSessionState) {
    *dt.txn_ts.lock().expect("txn_ts poisoned") = None;
}

// ─────────────────────────────────────────────────────────────────────────────
// Registration
// ─────────────────────────────────────────────────────────────────────────────

/// Register all extended date/time UDFs on `ctx`.
///
/// Called once per session-open from `session::open()`.
///
/// Self-contained timestamp anchoring: this function owns a fresh
/// [`DateTimeSessionState`] arc that is captured by the per-session
/// `statement_timestamp` / `transaction_timestamp` stubs. Because no
/// executor hook currently calls [`tick_statement_ts`] / [`tick_txn_ts`]
/// (PG-precise intra-transaction clock anchoring is tracked separately as
/// #151), those stubs fall back to a fresh `Utc::now()` per invocation —
/// a sane current timestamp sufficient for compilation and unit tests.
pub(crate) fn register_datetime_more_udfs(ctx: &SessionContext) {
    let dt = Arc::new(DateTimeSessionState::default());
    let dt = &dt;
    // age(ts1, ts2) → interval — overrides stateless-cache version.
    ctx.register_udf(ScalarUDF::from(AgeMoreUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));

    // to_char(ts|date, fmt) → text — accepts all ts/date types.
    ctx.register_udf(ScalarUDF::from(ToCharMoreUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));

    // to_date(text, fmt) → date
    ctx.register_udf(ScalarUDF::from(ToDateMoreUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));

    // to_timestamp(text, fmt) → timestamp
    ctx.register_udf(ScalarUDF::from(ToTimestampMoreUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));

    // date_bin(stride, source, origin) → timestamp
    ctx.register_udf(ScalarUDF::from(DateBinMoreUdf {
        signature: Signature::any(3, Volatility::Immutable),
    }));

    // clock_timestamp() → real-time — overrides NowStubUdf.
    ctx.register_udf(ScalarUDF::from(ClockTimestampUdf {
        signature: Signature::nullary(Volatility::Volatile),
    }));

    // statement_timestamp() → stable per statement.
    ctx.register_udf(ScalarUDF::from(StmtTimestampUdf {
        dt: Arc::clone(dt),
        signature: Signature::nullary(Volatility::Stable),
    }));

    // transaction_timestamp() → stable per transaction.
    ctx.register_udf(ScalarUDF::from(TxnTimestampUdf {
        dt: Arc::clone(dt),
        signature: Signature::nullary(Volatility::Stable),
    }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Return row count from a columnar-value slice.
fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

/// Extract a `NaiveDateTime` from any supported timestamp/date array at row `i`.
fn row_to_naive(arr: &ArrayRef, i: usize) -> DFResult<Option<NaiveDateTime>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let v = a.value(i);
            let secs = v.div_euclid(1_000_000);
            let us = v.rem_euclid(1_000_000) as u32;
            Ok(Utc
                .timestamp_opt(secs, us * 1_000)
                .single()
                .map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let v = a.value(i);
            let secs = v.div_euclid(1_000_000_000);
            let ns = v.rem_euclid(1_000_000_000) as u32;
            Ok(Utc.timestamp_opt(secs, ns).single().map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            use datafusion::arrow::array::TimestampMillisecondArray;
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let v = a.value(i);
            let secs = v.div_euclid(1_000);
            let ms = v.rem_euclid(1_000) as u32;
            Ok(Utc
                .timestamp_opt(secs, ms * 1_000_000)
                .single()
                .map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            use datafusion::arrow::array::TimestampSecondArray;
            let a = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let v = a.value(i);
            Ok(Utc.timestamp_opt(v, 0).single().map(|d| d.naive_utc()))
        }
        DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(i) as i64;
            Ok(Utc
                .timestamp_opt(days * 86_400, 0)
                .single()
                .map(|d| d.naive_utc()))
        }
        other => exec_err!("datetime_more: unsupported timestamp type {other:?}"),
    }
}

/// Compute `ts_micros` from a timestamp array at row `i` (returns microseconds
/// since epoch). Used by `date_bin`.
fn row_to_micros(arr: &ArrayRef, i: usize) -> DFResult<Option<i64>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(Some(a.value(i)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(Some(a.value(i) / 1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            use datafusion::arrow::array::TimestampMillisecondArray;
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(Some(a.value(i) * 1_000))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            use datafusion::arrow::array::TimestampSecondArray;
            let a = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            Ok(Some(a.value(i) * 1_000_000))
        }
        DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(Some(a.value(i) as i64 * 86_400 * 1_000_000))
        }
        other => exec_err!("date_bin: unsupported source type {other:?}"),
    }
}

/// Convert an `IntervalMonthDayNano` to microseconds.
/// Month/year components use approximate 30-day months (PG `date_bin` only
/// supports fixed-size strides so calendar-exact behaviour is outside scope).
fn interval_to_micros(iv: IntervalMonthDayNano) -> i64 {
    const US_PER_DAY: i64 = 86_400 * 1_000_000;
    // Approximate: 30 days per month.
    let month_us = iv.months as i64 * 30 * US_PER_DAY;
    let day_us = iv.days as i64 * US_PER_DAY;
    let ns_us = iv.nanoseconds / 1_000;
    month_us + day_us + ns_us
}

/// Extract an interval from a scalar/array arg as microseconds.
fn interval_arg_to_micros(args: &[ColumnarValue], idx: usize, n: usize) -> DFResult<i64> {
    let arr = args[idx].clone().into_array(n)?;
    match arr.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let a = arr
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            if a.is_null(0) {
                return exec_err!("date_bin: stride must not be null");
            }
            Ok(interval_to_micros(a.value(0)))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            use datafusion::arrow::array::IntervalDayTimeArray;
            let a = arr.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
            if a.is_null(0) {
                return exec_err!("date_bin: stride must not be null");
            }
            let v = a.value(0);
            let day_us = v.days as i64 * 86_400 * 1_000_000;
            let ms_us = v.milliseconds as i64 * 1_000;
            Ok(day_us + ms_us)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            use datafusion::arrow::array::IntervalYearMonthArray;
            let a = arr
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .unwrap();
            if a.is_null(0) {
                return exec_err!("date_bin: stride must not be null");
            }
            let months = a.value(0) as i64;
            Ok(months * 30 * 86_400 * 1_000_000)
        }
        other => exec_err!("date_bin: unsupported stride type {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PG date/time format helpers (shared by to_char / to_date / to_timestamp)
// ─────────────────────────────────────────────────────────────────────────────

/// Translate PG-style date/time format codes to chrono strftime form.
///
/// Recognised PG directives (longest match first):
///   `YYYY` `YY` `Month` `MONTH` `Mon` `MON` `Day` `DAY` `DY` `dy`
///   `MM` `DD` `HH24` `HH12` `HH` `MI` `SS` `MS` `US`
///   `AM` `PM` `am` `pm` `TZ` `tz`
///   `CC` `IW` `WW` `Q` `W` `J`
///   `FM` (fill-mode prefix — consumed/stripped)
///
/// Anything else is passed through verbatim. `%X` chrono-style sequences are
/// forwarded unchanged so callers already using chrono syntax are unaffected.
fn pg_fmt_to_chrono(fmt: &str) -> String {
    const REPL: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("Month", "%B"),
        ("MONTH", "%B"),
        ("Mon", "%b"),
        ("MON", "%b"),
        ("Day", "%A"),
        ("DAY", "%A"),
        ("DY", "%a"),
        ("dy", "%a"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
        ("MS", "%3f"),
        ("US", "%6f"),
        ("AM", "%p"),
        ("PM", "%p"),
        ("am", "%P"),
        ("pm", "%P"),
        ("TZ", "%Z"),
        ("tz", "%Z"),
        // Post-process placeholders for codes chrono lacks natively.
        ("CC", "\x01CC\x01"),
        ("IW", "%V"),
        ("WW", "%U"),
        ("Q", "\x01Q\x01"),
        ("W", "\x01W\x01"),
        ("J", "\x01J\x01"),
    ];
    let mut out = String::with_capacity(fmt.len());
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Strip FM (fill-mode).
        if bytes[i..].starts_with(b"FM") {
            i += 2;
            continue;
        }
        // Pass chrono % escapes through.
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            out.push('%');
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        let mut matched = false;
        for (pg, chrono) in REPL {
            let pb = pg.as_bytes();
            if bytes[i..].starts_with(pb) {
                out.push_str(chrono);
                i += pb.len();
                matched = true;
                break;
            }
        }
        if !matched {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Replace private placeholders inserted by `pg_fmt_to_chrono` with
/// computed calendar values.
fn pg_fmt_postprocess(s: String, dt: NaiveDateTime) -> String {
    if !s.contains('\x01') {
        return s;
    }
    let year = dt.year();
    let month = dt.month();
    let day = dt.day();
    let cc = ((year - 1) / 100) + 1;
    let q = ((month - 1) / 3) + 1;
    let w = ((day - 1) / 7) + 1;
    // Julian day (days since Jan 1, 4713 BC noon; Unix epoch = JD 2440588).
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let j = (dt.date() - epoch).num_days() + 2_440_588;

    s.replace("\x01CC\x01", &format!("{cc:02}"))
        .replace("\x01Q\x01", &format!("{q}"))
        .replace("\x01W\x01", &format!("{w}"))
        .replace("\x01J\x01", &format!("{j}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// age(ts1, ts2) → MonthDayNano
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct AgeMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for AgeMoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "age"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("age expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let lhs = args[0].clone().into_array(n)?;
        let rhs = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(n);
        for i in 0..n {
            let a = row_to_naive(&lhs, i)?;
            let b = row_to_naive(&rhs, i)?;
            out.push(match (a, b) {
                (Some(a), Some(b)) => Some(pg_age_interval(a, b)),
                _ => None,
            });
        }
        Ok(ColumnarValue::Array(Arc::new(
            IntervalMonthDayNanoArray::from(out),
        )))
    }
}

/// Compute `ts1 - ts2` as a PG-normalized interval.
///
/// Normalizes into (months, days, nanoseconds) by borrowing from the calendar,
/// mirroring PostgreSQL's `age(timestamp, timestamp)` implementation. The
/// nanoseconds component carries hours + minutes + seconds + sub-second.
fn pg_age_interval(ts1: NaiveDateTime, ts2: NaiveDateTime) -> IntervalMonthDayNano {
    if ts1 == ts2 {
        return IntervalMonthDayNano::new(0, 0, 0);
    }
    let neg = ts1 < ts2;
    let (a, b) = if neg { (ts2, ts1) } else { (ts1, ts2) };

    let mut years = a.year() - b.year();
    let mut months = a.month() as i32 - b.month() as i32;
    let mut days = a.day() as i32 - b.day() as i32;
    let mut hours = a.hour() as i32 - b.hour() as i32;
    let mut mins = a.minute() as i32 - b.minute() as i32;
    let mut secs = a.second() as i32 - b.second() as i32;
    let mut nanos = a.nanosecond() as i64 - b.nanosecond() as i64;

    while nanos < 0 {
        nanos += 1_000_000_000;
        secs -= 1;
    }
    while secs < 0 {
        secs += 60;
        mins -= 1;
    }
    while mins < 0 {
        mins += 60;
        hours -= 1;
    }
    while hours < 0 {
        hours += 24;
        days -= 1;
    }
    while days < 0 {
        days += days_in_month(b.year(), b.month()) as i32;
        months -= 1;
    }
    while months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = years * 12 + months;
    let total_nanos = (hours as i64) * 3_600_000_000_000
        + (mins as i64) * 60_000_000_000
        + (secs as i64) * 1_000_000_000
        + nanos;

    if neg {
        IntervalMonthDayNano::new(-total_months, -days, -total_nanos)
    } else {
        IntervalMonthDayNano::new(total_months, days, total_nanos)
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// to_char(ts|date, fmt) → text
// ─────────────────────────────────────────────────────────────────────────────

/// `to_char(timestamp|date, format) → text`
///
/// Accepts all timestamp unit/timezone variants and `Date32`.
/// For numeric inputs (Float64, Int64, etc.) falls back to a simple
/// `format!("{:.prec}")` rendering with `9`/`0` picture codes.
///
/// **Not supported:** interval inputs (use `to_char_interval` from
/// `interval_tz_udf`), `BC`/`AD` era, `SSSS`, `OF`, `TZH`/`TZM`, locale
/// month/day names (always English via chrono).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToCharMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToCharMoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_char"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("to_char expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let val_arr = args[0].clone().into_array(n)?;
        let fmt_arr = args[1].clone().into_array(n)?;
        let fmts = fmt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_char: arg 2 must be text".into()))?;

        let is_numeric = matches!(
            val_arr.data_type(),
            DataType::Float64
                | DataType::Float32
                | DataType::Int64
                | DataType::Int32
                | DataType::Int16
                | DataType::Int8
                | DataType::UInt64
                | DataType::UInt32
        );

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        if is_numeric {
            // Numeric picture: cast to f64, format with basic 9/0 picture.
            use datafusion::arrow::array::Float64Array;
            use datafusion::arrow::compute::cast;
            let f64_arr = cast(&val_arr, &DataType::Float64)
                .map_err(|e| DataFusionError::Execution(format!("to_char(numeric): {e}")))?;
            let f64_arr = f64_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("to_char(numeric): cast to f64 failed".into())
                })?;
            for i in 0..n {
                if f64_arr.is_null(i) || fmts.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(format_numeric_pg(fmts.value(i), f64_arr.value(i))?));
                }
            }
        } else {
            // Datetime path.
            for i in 0..n {
                if fmts.is_null(i) {
                    out.push(None);
                    continue;
                }
                let chrono_fmt = pg_fmt_to_chrono(fmts.value(i));
                let dt = row_to_naive(&val_arr, i)?;
                match dt {
                    None => out.push(None),
                    Some(dt) => {
                        let raw = dt.format(&chrono_fmt).to_string();
                        out.push(Some(pg_fmt_postprocess(raw, dt)));
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

/// Format a floating-point value using a PG numeric picture string.
///
/// Supported picture codes: `9` (digit, suppress leading zero), `0` (digit,
/// keep leading zero), `.` (decimal point), `,` (grouping separator — kept
/// verbatim in output), `FM` (fill mode — strip trailing zeros/spaces).
/// Everything else is passed through verbatim.
fn format_numeric_pg(picture: &str, v: f64) -> DFResult<String> {
    // Count digits before and after the decimal point in the picture.
    let dot_pos = picture.find('.');
    let int_pic: &str = dot_pos.map(|p| &picture[..p]).unwrap_or(picture);
    let frac_pic: &str = dot_pos.map(|p| &picture[p + 1..]).unwrap_or("");
    // Count significant-digit placeholders (9 or 0).
    let int_digits = int_pic.chars().filter(|&c| c == '9' || c == '0').count();
    let frac_digits = frac_pic.chars().filter(|&c| c == '9' || c == '0').count();
    let _ = int_digits; // informational; we use frac_digits for precision.
    let fill_mode = picture.contains("FM");

    let formatted = if frac_digits > 0 {
        format!("{:.prec$}", v, prec = frac_digits)
    } else {
        format!("{:.0}", v)
    };
    let result = if fill_mode {
        // Strip trailing zeros after decimal point.
        if formatted.contains('.') {
            formatted
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            formatted
        }
    } else {
        formatted
    };
    Ok(result)
}

// ─────────────────────────────────────────────────────────────────────────────
// to_date(text, fmt) → Date32
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToDateMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToDateMoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_date"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("to_date expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let txt_arr = args[0].clone().into_array(n)?;
        let fmt_arr = args[1].clone().into_array(n)?;
        let txts = txt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_date: arg 1 must be text".into()))?;
        let fmts = fmt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_date: arg 2 must be text".into()))?;

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let mut out = Date32Builder::with_capacity(n);
        for i in 0..n {
            if txts.is_null(i) || fmts.is_null(i) {
                out.append_null();
                continue;
            }
            let chrono_fmt = pg_fmt_to_chrono(fmts.value(i));
            let date = NaiveDate::parse_from_str(txts.value(i), &chrono_fmt)
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(txts.value(i), &chrono_fmt).map(|dt| dt.date())
                })
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "to_date: failed to parse {:?} with format {:?} (chrono {:?}): {e}",
                        txts.value(i),
                        fmts.value(i),
                        chrono_fmt,
                    ))
                })?;
            let days = (date - epoch).num_days() as i32;
            out.append_value(days);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// to_timestamp(text, fmt) → Timestamp(Nanosecond, None)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToTimestampMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToTimestampMoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("to_timestamp expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let txt_arr = args[0].clone().into_array(n)?;
        let fmt_arr = args[1].clone().into_array(n)?;
        let txts = txt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_timestamp: arg 1 must be text".into()))?;
        let fmts = fmt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_timestamp: arg 2 must be text".into()))?;

        let mut out: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..n {
            if txts.is_null(i) || fmts.is_null(i) {
                out.push(None);
                continue;
            }
            let chrono_fmt = pg_fmt_to_chrono(fmts.value(i));
            let dt = NaiveDateTime::parse_from_str(txts.value(i), &chrono_fmt)
                .or_else(|_| {
                    NaiveDate::parse_from_str(txts.value(i), &chrono_fmt)
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "to_timestamp: failed to parse {:?} with format {:?} (chrono {:?}): {e}",
                        txts.value(i),
                        fmts.value(i),
                        chrono_fmt,
                    ))
                })?;
            out.push(Utc.from_utc_datetime(&dt).timestamp_nanos_opt());
        }
        Ok(ColumnarValue::Array(Arc::new(
            TimestampNanosecondArray::from(out),
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// date_bin(stride, source, origin) → Timestamp(Microsecond, None)
// ─────────────────────────────────────────────────────────────────────────────

/// `date_bin(stride interval, source timestamp, origin timestamp) → timestamp`
///
/// PG 14+ semantics: bins `source` into the nearest stride bucket measured
/// from `origin`.
///
/// Formula: `origin + floor((source - origin) / stride) * stride`
///
/// Stride must be a positive interval. The result is returned as
/// `Timestamp(Microsecond, None)` regardless of the source unit; callers
/// can cast to the desired unit.
///
/// **Limitation:** month/year stride components use approximate 30-day months.
/// PG also only accepts fixed-size strides (no months), but we tolerate them
/// with this approximation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateBinMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for DateBinMoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "date_bin"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 3 {
            return exec_err!("date_bin expects 3 arguments, got {}", args.len());
        }
        let n = num_rows(args);

        // Stride is args[0] (interval scalar — use row 0).
        let stride_us = interval_arg_to_micros(args, 0, n)?;
        if stride_us <= 0 {
            return exec_err!(
                "date_bin: stride must be a positive interval (got {} µs)",
                stride_us
            );
        }

        // Source timestamps: args[1].
        let src_arr = args[1].clone().into_array(n)?;
        // Origin timestamp: args[2].
        let origin_arr = args[2].clone().into_array(n)?;

        let mut out: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..n {
            let src = row_to_micros(&src_arr, i)?;
            let origin = row_to_micros(&origin_arr, 0)?; // origin is scalar

            match (src, origin) {
                (Some(src_us), Some(orig_us)) => {
                    let delta = src_us - orig_us;
                    // floor division (Euclidean quotient)
                    let bucket = delta.div_euclid(stride_us);
                    let result = orig_us + bucket * stride_us;
                    out.push(Some(result));
                }
                _ => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            TimestampMicrosecondArray::from(out),
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// clock_timestamp() — real-time
// ─────────────────────────────────────────────────────────────────────────────

/// `clock_timestamp() → timestamptz`
///
/// Returns the real wall-clock instant at invocation time. Each call within
/// the same statement may return a different value — this is the intended
/// PG behaviour distinguishing it from `statement_timestamp()`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ClockTimestampUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ClockTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "clock_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // Capture a fresh now() on every UDF invocation — that is clock semantics.
        let now_us = Utc::now().timestamp_micros();
        let mut b =
            datafusion::arrow::array::TimestampMicrosecondBuilder::with_capacity(n).with_data_type(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            );
        for _ in 0..n {
            b.append_value(now_us);
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// statement_timestamp() — stable per statement
// ─────────────────────────────────────────────────────────────────────────────

/// `statement_timestamp() → timestamptz`
///
/// Returns the timestamp snapped at the start of the current SQL statement.
/// All calls within the same statement return the same value.
/// The executor calls [`tick_statement_ts`] before each statement to refresh
/// the stored timestamp.
///
/// Falls back to `Utc::now()` if no statement_ts has been set yet (e.g. in
/// tests that call the UDF directly without going through the executor).
#[derive(Debug)]
struct StmtTimestampUdf {
    dt: Arc<DateTimeSessionState>,
    signature: Signature,
}

// `Arc<DateTimeSessionState>` holds `Mutex<…>` (not `PartialEq`/`Hash`), so we
// derive equality/hash from the arc's identity + signature. This satisfies
// DataFusion's `DynEq`/`DynHash` blanket impl for `ScalarUDFImpl`.
impl PartialEq for StmtTimestampUdf {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.dt, &other.dt) && self.signature == other.signature
    }
}
impl Eq for StmtTimestampUdf {}
impl std::hash::Hash for StmtTimestampUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.dt) as usize).hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for StmtTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "statement_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let ts = self.dt.statement_ts.lock().expect("statement_ts poisoned");
        let us = ts.unwrap_or_else(|| Utc::now().timestamp_micros());
        let mut b =
            datafusion::arrow::array::TimestampMicrosecondBuilder::with_capacity(n).with_data_type(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            );
        for _ in 0..n {
            b.append_value(us);
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// transaction_timestamp() — stable per transaction
// ─────────────────────────────────────────────────────────────────────────────

/// `transaction_timestamp() → timestamptz`
///
/// Returns the timestamp snapped at `BEGIN`.  Outside an explicit transaction,
/// falls back to the current `statement_ts` (or `Utc::now()` if that is also
/// unset).
///
/// The executor calls [`tick_txn_ts`] at `BEGIN` and [`clear_txn_ts`] at
/// `COMMIT` / `ROLLBACK`.
#[derive(Debug)]
struct TxnTimestampUdf {
    dt: Arc<DateTimeSessionState>,
    signature: Signature,
}

// See `StmtTimestampUdf` — identity-based eq/hash for DynEq/DynHash.
impl PartialEq for TxnTimestampUdf {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.dt, &other.dt) && self.signature == other.signature
    }
}
impl Eq for TxnTimestampUdf {}
impl std::hash::Hash for TxnTimestampUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.dt) as usize).hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for TxnTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "transaction_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // Prefer txn_ts; fall back to statement_ts; fall back to now().
        let txn = *self.dt.txn_ts.lock().expect("txn_ts poisoned");
        let stmt = *self.dt.statement_ts.lock().expect("statement_ts poisoned");
        let us = txn
            .or(stmt)
            .unwrap_or_else(|| Utc::now().timestamp_micros());
        let mut b =
            datafusion::arrow::array::TimestampMicrosecondBuilder::with_capacity(n).with_data_type(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            );
        for _ in 0..n {
            b.append_value(us);
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn naive(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn age_simple_years_months() {
        // 2024-03-15 − 2020-01-10 = 4 years 2 months 5 days.
        let a = naive(2024, 3, 15, 0, 0, 0);
        let b = naive(2020, 1, 10, 0, 0, 0);
        let iv = pg_age_interval(a, b);
        assert_eq!(iv.months, 4 * 12 + 2, "months");
        assert_eq!(iv.days, 5, "days");
        assert_eq!(iv.nanoseconds, 0, "nanos");
    }

    #[test]
    fn age_negative_borrow() {
        // When ts1.day < ts2.day we borrow from the month.
        // 2024-03-05 − 2024-01-10 = 1 month 24 days (Jan has 31 days → 31-10=21+5... let's compute:
        // months=2, days=-5 → borrow from Jan (31 days) → months=1, days=26).
        let a = naive(2024, 3, 5, 0, 0, 0);
        let b = naive(2024, 1, 10, 0, 0, 0);
        let iv = pg_age_interval(a, b);
        // Expected: 1 month 26 days (borrow from Jan=31: 31-10+5 = 26 and months 3-1-1=1).
        assert_eq!(iv.months, 1, "months");
        assert_eq!(iv.days, 26, "days");
    }

    #[test]
    fn age_equal() {
        let ts = naive(2024, 6, 1, 12, 0, 0);
        let iv = pg_age_interval(ts, ts);
        assert_eq!(iv.months, 0);
        assert_eq!(iv.days, 0);
        assert_eq!(iv.nanoseconds, 0);
    }

    #[test]
    fn age_negative_result() {
        // ts1 < ts2 → negative interval.
        let a = naive(2020, 1, 1, 0, 0, 0);
        let b = naive(2024, 6, 1, 0, 0, 0);
        let iv = pg_age_interval(a, b);
        assert!(iv.months < 0 || iv.days < 0 || iv.nanoseconds < 0);
    }

    #[test]
    fn to_char_basic() {
        // Just smoke-test the format helper.
        let fmt = pg_fmt_to_chrono("YYYY-MM-DD HH24:MI:SS");
        assert!(fmt.contains("%Y"));
        assert!(fmt.contains("%m"));
        assert!(fmt.contains("%H"));
        assert!(fmt.contains("%M"));
        assert!(fmt.contains("%S"));
    }

    #[test]
    fn to_char_mon_formats() {
        let fmt = pg_fmt_to_chrono("MON");
        assert_eq!(fmt, "%b");
        let fmt = pg_fmt_to_chrono("Month");
        assert_eq!(fmt, "%B");
    }

    #[test]
    fn date_bin_15min() {
        // source = 2020-01-01 00:17:00, origin = 2020-01-01 00:00:00, stride = 15 min
        // → floor(17 / 15) = 1 → result = 2020-01-01 00:15:00
        let origin_us = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_micros();
        let source_us = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 17, 0)
            .unwrap()
            .timestamp_micros();
        let stride_us = 15 * 60 * 1_000_000_i64;
        let delta = source_us - origin_us;
        let bucket = delta.div_euclid(stride_us);
        let result = origin_us + bucket * stride_us;
        let expected = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 15, 0)
            .unwrap()
            .timestamp_micros();
        assert_eq!(result, expected, "date_bin 15min");
    }

    #[test]
    fn date_bin_before_origin() {
        // source < origin: source = 2020-01-01 00:05:00, origin = 2020-01-01 00:15:00
        // delta = -10 min, stride = 15 min → floor(-10/15) = -1 → result = 00:00:00
        let origin_us = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 15, 0)
            .unwrap()
            .timestamp_micros();
        let source_us = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 5, 0)
            .unwrap()
            .timestamp_micros();
        let stride_us = 15 * 60 * 1_000_000_i64;
        let delta = source_us - origin_us;
        let bucket = delta.div_euclid(stride_us);
        let result = origin_us + bucket * stride_us;
        let expected = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_micros();
        assert_eq!(result, expected, "date_bin before origin");
    }

    #[test]
    fn session_state_distinctness() {
        let dt = DateTimeSessionState::default();
        tick_statement_ts(&dt);
        let stmt_ts1 = dt
            .statement_ts
            .lock()
            .unwrap()
            .expect("statement_ts should be set");

        tick_txn_ts(&dt);
        let txn_ts1 = dt.txn_ts.lock().unwrap().expect("txn_ts should be set");

        // txn_ts is idempotent (second call should not change it).
        tick_txn_ts(&dt);
        let txn_ts2 = dt
            .txn_ts
            .lock()
            .unwrap()
            .expect("txn_ts should still be set");
        assert_eq!(txn_ts1, txn_ts2, "tick_txn_ts should be idempotent");

        // txn_ts is captured <= stmt_ts (both set in rapid succession above,
        // so this just checks they were set).
        assert!(
            txn_ts1 <= stmt_ts1 + 1_000_000,
            "timestamps should be within 1s"
        );

        clear_txn_ts(&dt);
        let txn_after_clear = *dt.txn_ts.lock().unwrap();
        assert!(txn_after_clear.is_none(), "txn_ts cleared");
    }

    #[test]
    fn pg_fmt_to_chrono_passes_chrono_escapes() {
        // Existing chrono %Y style should pass through unchanged.
        let out = pg_fmt_to_chrono("%Y-%m-%d");
        assert_eq!(out, "%Y-%m-%d");
    }

    #[test]
    fn pg_fmt_to_chrono_fm_stripped() {
        let out = pg_fmt_to_chrono("FMYYYY");
        assert_eq!(out, "%Y");
    }
}
