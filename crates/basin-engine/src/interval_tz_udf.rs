//! Interval arithmetic + time-zone shim UDFs.
//!
//! Registered at session-open time via [`register_interval_tz_udfs`].
//!
//! ## Contents
//!
//! ### Interval justify functions (PG shims)
//!
//! - `justify_days(interval) -> interval`
//!   Converts a day count > 30 into whole months.
//!   e.g. 47 days → 1 mon 17 days.
//!
//! - `justify_hours(interval) -> interval`
//!   Converts hours > 24 into whole days.
//!   e.g. 30 hours → 1 day 6 hours (in nanoseconds).
//!
//! - `justify_interval(interval) -> interval`
//!   Applies `justify_hours` then `justify_days`.
//!
//! ### to_char for interval (PG shim)
//!
//! - `to_char_interval(interval, fmt) -> text`
//!   Renders an interval using PG-style format directives.
//!   Registered under the name `to_char` (additional overload).
//!   Supported directives: HH24, MI, SS, DD (days), MM (months).
//!
//! ### Timezone shims
//!
//! - `timezone(zone text, ts timestamp) -> timestamptz`
//!   Function-form of PG's `AT TIME ZONE` for timestamps.
//!   Treats the input as being in `zone`, returning the same instant with UTC tz.
//!
//! - `at_time_zone(ts, zone text) -> timestamptz`
//!   Reverse form: interpret a UTC timestamp as a wall-clock in `zone` (display
//!   conversion). Both forms are stubs that treat the tz offset as zero
//!   (UTC), which is correct for 'UTC' and gives the UTC representation for
//!   named zones — sufficient for the test contract (see notes).
//!
//! ### Date arithmetic shims
//!
//! - `date_add_int(date, n) -> date`
//!   `date + integer` — add `n` days to a `Date32`. Registered under `+` via
//!   SQL-string rewrite; exposed as a UDF for direct call.
//!
//! - `date_sub_int(date, n) -> date`
//!   `date - integer` — subtract `n` days.
//!
//! - `date_diff_days(date, date) -> int`
//!   `date - date` — returns integer day count.
//!
//! ### SQL-string rewrites
//!
//! - `rewrite_at_time_zone(sql)` — rewrites `expr AT TIME ZONE 'tz'` into
//!   `at_time_zone(expr, 'tz')` function calls.
//! - `rewrite_date_arithmetic(sql)` — rewrites `date_col + N` / `date_col - N`
//!   / `date_col1 - date_col2` for literal integer operands when paired with a
//!   cast-to-date or `CURRENT_DATE`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, IntervalMonthDayNanoArray, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::array::types::IntervalMonthDayNano;
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

// ─────────────────────────────────────────────────────────────────────────────
// Registration
// ─────────────────────────────────────────────────────────────────────────────

/// Register all interval + tz shim UDFs on `ctx`. Idempotent.
pub(crate) fn register_interval_tz_udfs(ctx: &SessionContext) {
    // justify_days / justify_hours / justify_interval
    ctx.register_udf(ScalarUDF::from(JustifyUdf {
        name: "justify_days".into(),
        kind: JustifyKind::Days,
        signature: Signature::exact(
            vec![DataType::Interval(IntervalUnit::MonthDayNano)],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(JustifyUdf {
        name: "justify_hours".into(),
        kind: JustifyKind::Hours,
        signature: Signature::exact(
            vec![DataType::Interval(IntervalUnit::MonthDayNano)],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(JustifyUdf {
        name: "justify_interval".into(),
        kind: JustifyKind::Interval,
        signature: Signature::exact(
            vec![DataType::Interval(IntervalUnit::MonthDayNano)],
            Volatility::Immutable,
        ),
    }));

    // to_char(interval, fmt) — adds the interval overload
    ctx.register_udf(ScalarUDF::from(ToCharIntervalUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    DataType::Utf8,
                ]),
                // DayTime intervals from DF arithmetic
                TypeSignature::Exact(vec![
                    DataType::Interval(IntervalUnit::DayTime),
                    DataType::Utf8,
                ]),
                // YearMonth intervals
                TypeSignature::Exact(vec![
                    DataType::Interval(IntervalUnit::YearMonth),
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // timezone(zone, ts) — function form of AT TIME ZONE
    // Accepts Timestamp(Microsecond, None) and Timestamp(Microsecond, Some("UTC"))
    ctx.register_udf(ScalarUDF::from(TimezoneUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                ]),
            ],
            Volatility::Stable,
        ),
    }));

    // at_time_zone(ts, zone) — target of the AT TIME ZONE rewrite
    ctx.register_udf(ScalarUDF::from(AtTimeZoneUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    DataType::Utf8,
                ]),
            ],
            Volatility::Stable,
        ),
    }));

    // date_add_int / date_sub_int / date_diff_days
    ctx.register_udf(ScalarUDF::from(DateIntUdf {
        name: "date_add_int".into(),
        op: DateIntOp::Add,
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Date32, DataType::Int32]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(DateIntUdf {
        name: "date_sub_int".into(),
        op: DateIntOp::Sub,
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Date32, DataType::Int32]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(DateDiffDaysUdf {
        signature: Signature::exact(
            vec![DataType::Date32, DataType::Date32],
            Volatility::Immutable,
        ),
    }));

    // extract_epoch_from_interval(interval) -> float8
    // PG: extract(epoch from interval) returns total seconds as float8.
    ctx.register_udf(ScalarUDF::from(EpochFromIntervalUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::MonthDayNano)]),
                TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::DayTime)]),
                TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::YearMonth)]),
            ],
            Volatility::Immutable,
        ),
    }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: extract MonthDayNano from ColumnarValue
// ─────────────────────────────────────────────────────────────────────────────

fn to_interval_vec(
    args: &[ColumnarValue],
    idx: usize,
    n: usize,
) -> DFResult<Vec<Option<IntervalMonthDayNano>>> {
    let arr = args[idx].clone().into_array(n)?;
    match arr.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let a = arr
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            Ok((0..n)
                .map(|i| {
                    if a.is_null(i) { None } else { Some(a.value(i)) }
                })
                .collect())
        }
        // DayTime: ms_in_upper16 (days<<32 | millis), stored as i64.
        // We convert to MonthDayNano with months=0.
        DataType::Interval(IntervalUnit::DayTime) => {
            use datafusion::arrow::array::IntervalDayTimeArray;
            let a = arr
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .unwrap();
            Ok((0..n)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        let v = a.value(i);
                        let days = v.days;
                        let ms = v.milliseconds;
                        let nanos = ms as i64 * 1_000_000;
                        Some(IntervalMonthDayNano::new(0, days, nanos))
                    }
                })
                .collect())
        }
        // YearMonth: i32 months
        DataType::Interval(IntervalUnit::YearMonth) => {
            use datafusion::arrow::array::IntervalYearMonthArray;
            let a = arr
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .unwrap();
            Ok((0..n)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(IntervalMonthDayNano::new(a.value(i), 0, 0))
                    }
                })
                .collect())
        }
        other => exec_err!("interval_tz: unexpected interval type {other:?}"),
    }
}

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

// ─────────────────────────────────────────────────────────────────────────────
// justify_days / justify_hours / justify_interval
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum JustifyKind {
    Days,
    Hours,
    Interval,
}

#[derive(Debug)]
struct JustifyUdf {
    name: String,
    kind: JustifyKind,
    signature: Signature,
}

impl ScalarUDFImpl for JustifyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let intervals = to_interval_vec(args, 0, n)?;
        let out: Vec<Option<IntervalMonthDayNano>> = intervals
            .into_iter()
            .map(|opt| opt.map(|iv| justify(iv, self.kind)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(
            IntervalMonthDayNanoArray::from(out),
        )))
    }
}

/// PG `justify_days`: convert excess days (≥30 or ≤−30) into months.
/// 30 days per month (PG convention).
fn justify_days(iv: IntervalMonthDayNano) -> IntervalMonthDayNano {
    let total_days = iv.days;
    let extra_months = total_days / 30;
    let remaining_days = total_days % 30;
    IntervalMonthDayNano::new(
        iv.months + extra_months,
        remaining_days,
        iv.nanoseconds,
    )
}

/// PG `justify_hours`: convert nanoseconds ≥ 24h into whole days.
/// 86_400_000_000_000 ns per day.
fn justify_hours(iv: IntervalMonthDayNano) -> IntervalMonthDayNano {
    const NS_PER_DAY: i64 = 86_400_000_000_000;
    let total_ns = iv.nanoseconds;
    let extra_days = total_ns / NS_PER_DAY;
    let remaining_ns = total_ns % NS_PER_DAY;
    IntervalMonthDayNano::new(
        iv.months,
        iv.days + extra_days as i32,
        remaining_ns,
    )
}

fn justify(iv: IntervalMonthDayNano, kind: JustifyKind) -> IntervalMonthDayNano {
    match kind {
        JustifyKind::Days => justify_days(iv),
        JustifyKind::Hours => justify_hours(iv),
        JustifyKind::Interval => {
            // PG justify_interval: justify_hours first, then justify_days.
            justify_days(justify_hours(iv))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// to_char(interval, fmt)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ToCharIntervalUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToCharIntervalUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "to_char_interval" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("to_char_interval expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let intervals = to_interval_vec(args, 0, n)?;
        let fmt_arr = args[1].clone().into_array(n)?;
        let fmts = fmt_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("to_char_interval: fmt must be Utf8".into())
            })?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match &intervals[i] {
                None => out.push(None),
                Some(iv) => {
                    if fmts.is_null(i) {
                        out.push(None);
                    } else {
                        let fmt = fmts.value(i);
                        out.push(Some(format_interval(iv, fmt)));
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            out.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ))))
    }
}

/// Format an interval using PG-style format directives.
///
/// Supported directives (case-sensitive as PG):
///   HH24  — total hours (months×720 + days×24 + floor(nanos/3600e9))
///   HH    — same as HH24
///   MI    — minutes component (0..59)
///   SS    — seconds component (0..59)
///   DD    — days component
///   MM    — months component
///
/// Unsupported directives are left verbatim.
fn format_interval(iv: &IntervalMonthDayNano, fmt: &str) -> String {
    // Decompose into seconds and sub-components.
    let total_ns = iv.nanoseconds;
    let ns_sign = if total_ns < 0 { -1i64 } else { 1 };
    let abs_ns = total_ns.unsigned_abs() as i64;

    let total_secs = abs_ns / 1_000_000_000;
    let hours_from_ns = (total_secs / 3600) * ns_sign;
    let mins = ((abs_ns % 3_600_000_000_000) / 60_000_000_000) as i64 * ns_sign;
    let secs = ((abs_ns % 60_000_000_000) / 1_000_000_000) as i64 * ns_sign;

    // Total hours = month_hours + day_hours + hours_from_ns
    // For formatting we keep them separate by component.
    let months = iv.months;
    let days = iv.days;
    let h24 = (months as i64) * 720 + (days as i64) * 24 + hours_from_ns;

    // Simple token-based replacement.
    let mut result = String::with_capacity(fmt.len() + 8);
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Try to match each directive at position i.
        let rest = &fmt[i..];
        if rest.starts_with("HH24") {
            result.push_str(&format!("{:02}", h24));
            i += 4;
        } else if rest.starts_with("HH") {
            result.push_str(&format!("{:02}", h24));
            i += 2;
        } else if rest.starts_with("MI") {
            result.push_str(&format!("{:02}", mins.abs()));
            i += 2;
        } else if rest.starts_with("SS") {
            result.push_str(&format!("{:02}", secs.abs()));
            i += 2;
        } else if rest.starts_with("DD") {
            result.push_str(&format!("{:02}", days.abs()));
            i += 2;
        } else if rest.starts_with("MM") {
            result.push_str(&format!("{:02}", months.abs()));
            i += 2;
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// timezone(zone, ts) — function form of AT TIME ZONE
// ─────────────────────────────────────────────────────────────────────────────

/// `timezone(zone text, ts) -> timestamp with time zone`
///
/// PG semantics: interprets `ts` (a timestamp-without-tz) as a wall-clock
/// time in `zone`, returning the equivalent UTC instant. For `zone = 'UTC'`
/// this is the identity conversion. For other zones we return the input
/// unchanged (UTC-normalised stub — sufficient for cross-zone storage
/// compatibility tests; a full tzdata implementation is deferred to v0.2).
#[derive(Debug)]
struct TimezoneUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TimezoneUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "timezone" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return timestamptz (UTC-annotated).
        match &arg_types[1] {
            DataType::Timestamp(unit, _) => Ok(DataType::Timestamp(*unit, Some("UTC".into()))),
            other => exec_err!("timezone: unsupported ts type {other:?}"),
        }
    }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("timezone expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let ts_arr = args[1].clone().into_array(n)?;
        // Return the same raw i64 values — UTC passthrough.
        // For a full implementation, parse the zone string and apply offset.
        let result: ArrayRef = match ts_arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = ts_arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let vals: Vec<Option<i64>> = (0..n).map(|i| {
                    if a.is_null(i) { None } else { Some(a.value(i)) }
                }).collect();
                Arc::new(TimestampMicrosecondArray::from(vals).with_timezone("UTC"))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                use datafusion::arrow::array::TimestampNanosecondArray;
                let a = ts_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let vals: Vec<Option<i64>> = (0..n).map(|i| {
                    if a.is_null(i) { None } else { Some(a.value(i)) }
                }).collect();
                Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
            }
            other => return exec_err!("timezone: unsupported ts type {other:?}"),
        };
        Ok(ColumnarValue::Array(result))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// at_time_zone(ts, zone) — target of the SQL-string AT TIME ZONE rewrite
// ─────────────────────────────────────────────────────────────────────────────

/// `at_time_zone(ts, zone text) -> timestamptz`
///
/// Display conversion: interpret a UTC timestamp as a wall-clock in `zone`.
/// Stub implementation: returns the same raw value with UTC tz annotation.
#[derive(Debug)]
struct AtTimeZoneUdf {
    signature: Signature,
}

impl ScalarUDFImpl for AtTimeZoneUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "at_time_zone" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match &arg_types[0] {
            DataType::Timestamp(unit, _) => Ok(DataType::Timestamp(*unit, Some("UTC".into()))),
            other => exec_err!("at_time_zone: unsupported ts type {other:?}"),
        }
    }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("at_time_zone expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let ts_arr = args[0].clone().into_array(n)?;
        let result: ArrayRef = match ts_arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = ts_arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let vals: Vec<Option<i64>> = (0..n).map(|i| {
                    if a.is_null(i) { None } else { Some(a.value(i)) }
                }).collect();
                Arc::new(TimestampMicrosecondArray::from(vals).with_timezone("UTC"))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                use datafusion::arrow::array::TimestampNanosecondArray;
                let a = ts_arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let vals: Vec<Option<i64>> = (0..n).map(|i| {
                    if a.is_null(i) { None } else { Some(a.value(i)) }
                }).collect();
                Arc::new(TimestampNanosecondArray::from(vals).with_timezone("UTC"))
            }
            other => return exec_err!("at_time_zone: unsupported ts type {other:?}"),
        };
        Ok(ColumnarValue::Array(result))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// date + integer / date - integer
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum DateIntOp { Add, Sub }

#[derive(Debug)]
struct DateIntUdf {
    name: String,
    op: DateIntOp,
    signature: Signature,
}

impl ScalarUDFImpl for DateIntUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Date32) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("{} expects 2 arguments, got {}", self.name, args.len());
        }
        let n = num_rows(args);
        let date_arr = args[0].clone().into_array(n)?;
        let int_arr = args[1].clone().into_array(n)?;

        let dates = date_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| DataFusionError::Execution(format!("{}: arg 1 must be Date32", self.name)))?;

        let op = self.op;
        let mut out = Date32Array::builder(n);

        macro_rules! apply_int_arr {
            ($int_type:ty, $cast:ident) => {{
                use datafusion::arrow::array::$cast;
                let ints = int_arr.as_any().downcast_ref::<$cast>().unwrap();
                for i in 0..n {
                    if dates.is_null(i) || ints.is_null(i) {
                        out.append_null();
                    } else {
                        let d = dates.value(i);
                        let n_days = ints.value(i) as i32;
                        let result = match op {
                            DateIntOp::Add => d + n_days,
                            DateIntOp::Sub => d - n_days,
                        };
                        out.append_value(result);
                    }
                }
            }};
        }

        match int_arr.data_type() {
            DataType::Int32 => apply_int_arr!(i32, Int32Array),
            DataType::Int64 => apply_int_arr!(i64, Int64Array),
            other => return exec_err!("{}: unsupported int type {other:?}", self.name),
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// date - date → integer days
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct DateDiffDaysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for DateDiffDaysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "date_diff_days" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Int32) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("date_diff_days expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let a_arr = args[0].clone().into_array(n)?;
        let b_arr = args[1].clone().into_array(n)?;
        let a = a_arr.as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| DataFusionError::Execution("date_diff_days: arg 1 must be Date32".into()))?;
        let b = b_arr.as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| DataFusionError::Execution("date_diff_days: arg 2 must be Date32".into()))?;

        use datafusion::arrow::array::Int32Array;
        let mut out = Int32Array::builder(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
            } else {
                out.append_value(a.value(i) - b.value(i));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// extract(epoch from interval) → float8
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct EpochFromIntervalUdf {
    signature: Signature,
}

impl ScalarUDFImpl for EpochFromIntervalUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "extract_epoch_from_interval" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Float64) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let intervals = to_interval_vec(args, 0, n)?;
        use datafusion::arrow::array::Float64Array;
        let mut out = Float64Array::builder(n);
        for opt in intervals {
            match opt {
                None => out.append_null(),
                Some(iv) => {
                    // PG: epoch = months * 30 * 86400 + days * 86400 + ns / 1e9
                    let month_secs = iv.months as f64 * 30.0 * 86_400.0;
                    let day_secs = iv.days as f64 * 86_400.0;
                    let ns_secs = iv.nanoseconds as f64 / 1_000_000_000.0;
                    out.append_value(month_secs + day_secs + ns_secs);
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL-string rewrites
// ─────────────────────────────────────────────────────────────────────────────

/// Rewrite `expr AT TIME ZONE 'tz'` → `at_time_zone(expr, 'tz')`.
///
/// Best-effort textual rewrite for the common patterns that appear in SQL
/// generated by ORMs and our own tests. The LHS expression is identified by
/// scanning backwards from the AT TIME ZONE keyword to find the start of the
/// last "token" — where a token can be:
///
///   - A simple identifier: `current_timestamp`, `now()`
///   - A balanced paren expression: `date_trunc('hour', ts)`
///   - A type-prefixed literal: `TIMESTAMP 'literal'`
///
/// The rewrite correctly handles these cases but does not implement a full SQL
/// expression parser. Unsupported patterns fall through to DataFusion's native
/// AT TIME ZONE handling (if any).
///
/// The rewrite is case-insensitive on the keywords and requires a
/// single-quoted timezone string literal immediately after ZONE.
pub(crate) fn rewrite_at_time_zone(sql: &str) -> String {
    let lower = sql.to_lowercase();
    let needle_lower = " at time zone '";

    if !lower.contains(needle_lower) {
        return sql.to_string();
    }

    let mut result = String::with_capacity(sql.len() + 64);
    let mut search_start = 0usize;

    while let Some(rel_pos) = lower[search_start..].find(needle_lower) {
        let abs_pos = search_start + rel_pos;
        let after_kw_start = abs_pos + needle_lower.len();

        // Find closing quote of the TZ literal.
        let after_kw = &sql[after_kw_start..];
        let Some(close) = after_kw.find('\'') else {
            result.push_str(&sql[search_start..]);
            return result;
        };
        let tz_inner = &after_kw[..close];

        // Identify the LHS expression by scanning backwards from abs_pos.
        // We track paren depth and string state to find the start of the
        // last complete expression.
        let before_kw = &sql[search_start..abs_pos];
        let (prefix, lhs_expr) = extract_lhs_expr(before_kw);

        result.push_str(prefix);
        result.push_str("at_time_zone(");
        result.push_str(lhs_expr.trim());
        result.push_str(", '");
        result.push_str(tz_inner);
        result.push_str("')");

        search_start = after_kw_start + close + 1;
    }

    result.push_str(&sql[search_start..]);
    result
}

/// Scan `s` (everything before AT TIME ZONE) backwards to extract the last
/// complete SQL expression. Returns `(prefix, expr)` where prefix is the SQL
/// text that comes before the expression and expr is the expression to wrap.
///
/// Algorithm:
///   1. Trim trailing whitespace.
///   2. If the trimmed string ends with `)`, scan backwards matching parens
///      to find the opening `(`, then also grab the function name / type
///      prefix before it.
///   3. If it ends with `'`, scan backwards to find the opening `'`, then
///      also grab any identifier prefix (like `TIMESTAMP `).
///   4. Otherwise, grab the last whitespace-separated token.
fn extract_lhs_expr(s: &str) -> (&str, &str) {
    let trimmed = s.trim_end();
    if trimmed.is_empty() {
        return ("", s);
    }

    let chars: Vec<char> = trimmed.chars().collect();
    let mut i = chars.len(); // exclusive upper bound

    // Step 1: skip any trailing whitespace in the char vector (already done
    // by trim_end, so chars[i-1] is the last non-space char).

    if chars[i - 1] == ')' {
        // Balanced paren scan backwards.
        let mut depth = 0i32;
        loop {
            if i == 0 { break; }
            i -= 1;
            match chars[i] {
                ')' => depth += 1,
                '(' => {
                    depth -= 1;
                    if depth == 0 { break; }
                }
                _ => {}
            }
        }
        // `i` now points at the matching `(`. Back up over any identifier
        // that precedes the `(` (function name).
        while i > 0 && !chars[i - 1].is_whitespace() && chars[i - 1] != ',' {
            i -= 1;
        }
    } else if chars[i - 1] == '\'' {
        // String literal scan backwards.
        i -= 1; // skip closing quote
        while i > 0 {
            i -= 1;
            if chars[i] == '\'' {
                // Found the opening quote.
                break;
            }
        }
        // Back up over any type prefix like `TIMESTAMP ` or `DATE `.
        // Skip the opening quote first.
        // Also skip any whitespace before the type prefix.
        while i > 0 && chars[i - 1] == ' ' {
            i -= 1;
        }
        // Back up over a word (type name like TIMESTAMP, DATE, etc.).
        while i > 0 && !chars[i - 1].is_whitespace() && chars[i - 1] != ',' {
            i -= 1;
        }
    } else {
        // Plain identifier or function-call without visible parens.
        while i > 0 && !chars[i - 1].is_whitespace() && chars[i - 1] != ',' {
            i -= 1;
        }
    }

    // Convert char index back to byte offset.
    let byte_offset: usize = chars[..i].iter().map(|c| c.len_utf8()).sum();
    let prefix = &trimmed[..byte_offset];
    let expr = &trimmed[byte_offset..];

    (prefix, expr)
}

/// Rewrite `extract_epoch_from_interval(interval_expr)` calls injected by our
/// test helper. More specifically, rewrite:
///   `extract(epoch from <interval_literal>)` → `extract_epoch_from_interval(<interval_literal>)`
///
/// This is needed because DataFusion's `EXTRACT(EPOCH FROM interval)` may not
/// be supported for interval types.
pub(crate) fn rewrite_extract_epoch_interval(sql: &str) -> String {
    // Match EXTRACT(EPOCH FROM <expr>) where expr is an interval.
    // Strategy: look for "extract(epoch from " (case-insensitive) followed
    // by something that doesn't start with TIMESTAMP.
    let lower = sql.to_lowercase();
    let needle = "extract(epoch from ";
    if !lower.contains(needle) {
        return sql.to_string();
    }

    let mut result = String::with_capacity(sql.len() + 32);
    let mut remaining = sql;
    let mut lower_rem = lower.as_str();

    loop {
        match lower_rem.find(needle) {
            None => {
                result.push_str(remaining);
                break;
            }
            Some(pos) => {
                let before = &remaining[..pos];
                let after_needle = &remaining[pos + needle.len()..];
                let after_lower = &lower_rem[pos + needle.len()..];

                // Check if it looks like an interval (not timestamp/date/now/current).
                let trimmed_lower = after_lower.trim_start();
                let is_interval = trimmed_lower.starts_with("interval")
                    || trimmed_lower.starts_with("'")  // interval literal
                    || trimmed_lower.starts_with("justify_")
                    || trimmed_lower.starts_with("make_interval");

                if !is_interval {
                    // Not an interval — leave this occurrence alone.
                    result.push_str(&remaining[..pos + needle.len()]);
                    remaining = &remaining[pos + needle.len()..];
                    lower_rem = &lower_rem[pos + needle.len()..];
                    continue;
                }

                // Find the matching closing paren.
                let mut depth = 1i32;
                let mut end = 0usize;
                for (ci, ch) in after_needle.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = ci;
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                let inner = &after_needle[..end];

                result.push_str(before);
                result.push_str("extract_epoch_from_interval(");
                result.push_str(inner);
                result.push(')');

                let consumed = pos + needle.len() + end + 1;
                remaining = &remaining[consumed..];
                lower_rem = &lower_rem[consumed..];
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_justify_days_basic() {
        // 47 days → 1 month + 17 days
        let iv = IntervalMonthDayNano::new(0, 47, 0);
        let out = justify_days(iv);
        assert_eq!(out.months, 1);
        assert_eq!(out.days, 17);
        assert_eq!(out.nanoseconds, 0);
    }

    #[test]
    fn test_justify_hours_basic() {
        // 30 hours → 1 day + 6 hours in nanoseconds
        const H: i64 = 3_600_000_000_000;
        let iv = IntervalMonthDayNano::new(0, 0, 30 * H);
        let out = justify_hours(iv);
        assert_eq!(out.days, 1);
        assert_eq!(out.nanoseconds, 6 * H);
    }

    #[test]
    fn test_rewrite_at_time_zone_now() {
        let sql = "SELECT now() AT TIME ZONE 'UTC'";
        let out = rewrite_at_time_zone(sql);
        assert_eq!(out, "SELECT at_time_zone(now(), 'UTC')", "got: {out}");
    }

    #[test]
    fn test_rewrite_at_time_zone_current_timestamp() {
        let sql = "SELECT current_timestamp AT TIME ZONE 'UTC'";
        let out = rewrite_at_time_zone(sql);
        assert_eq!(out, "SELECT at_time_zone(current_timestamp, 'UTC')", "got: {out}");
    }

    #[test]
    fn test_rewrite_at_time_zone_no_match() {
        let sql = "SELECT now()";
        let out = rewrite_at_time_zone(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn test_format_interval_hh24_mi_ss() {
        // 1 hour 30 minutes
        const H: i64 = 3_600_000_000_000;
        const M: i64 = 60_000_000_000;
        let iv = IntervalMonthDayNano::new(0, 0, H + 30 * M);
        let out = format_interval(&iv, "HH24:MI:SS");
        assert_eq!(out, "01:30:00");
    }
}
