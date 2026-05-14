//! PG-compat scalar function aliases and thin wrappers — Phase 5.11.B inventory.
//!
//! DataFusion 44 already ships the vast majority of Postgres scalar functions
//! under their canonical names.  This module handles three residual cases:
//!
//! 1. **Name aliases** — functions where PG uses a different name than DF.
//!    e.g. `ceiling` vs `ceil`, `sign` vs `signum`.
//!
//! 2. **Timestamp stubs** — `clock_timestamp`, `transaction_timestamp`,
//!    `statement_timestamp`, `localtime`, `localtimestamp` all return the same
//!    value as `now()` in v0.1 (no statement/clock-tick distinction yet).
//!
//! 3. **Missing scalars** — `make_time`, `make_timestamp`, `make_interval`,
//!    `to_number`, `width_bucket` — functions DataFusion does not ship.
//!
//! ## What is already handled elsewhere
//!
//! | Function | Where |
//! |---|---|
//! | `lower`, `upper`, `length`/`char_length`, `concat`, `concat_ws`, `lpad`, `rpad`, `initcap`, `repeat`, `replace`, `octet_length` | DataFusion builtin |
//! | `trim`/`ltrim`/`rtrim`, `position`, `substring`, `regexp_replace` | DataFusion builtin |
//! | `abs`, `ceil`/`floor`, `round`, `trunc`, `sqrt`, `exp`, `ln`, `log`, `pi`, `random` | DataFusion builtin |
//! | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians` | DataFusion builtin |
//! | `power` (+ alias `pow`) | DataFusion builtin |
//! | `now`, `current_timestamp` (alias), `current_date`, `current_time` | DataFusion builtin |
//! | `make_date`, `to_date`, `to_char`, `to_timestamp` | DataFusion builtin / basin udf.rs |
//! | `coalesce`, `nullif`, `greatest`, `least` | DataFusion builtin |
//! | `mod`, `age`, `power` (float64 override) | `udf::register_pg_compat_udfs` |
//! | `encode`, `decode`, `md5`, `gen_random_uuid` | `udf::register_pg_udfs` |
//! | `split_part`, `format`, `quote_ident`, `quote_literal`, `regexp_match`, `regexp_split_to_array`, `chr`, `ascii`, `translate`, `btrim`, `ltrim`, `rtrim` | `string_dt_udf::register_string_dt_udfs` |
//! | `count`, `sum`, `avg`, `min`, `max`, `array_agg`, `string_agg`, `bool_and`, `bool_or`, `bit_and`, `bit_or` | DataFusion aggregate UDAFs (builtin) |
//!
//! `every` is a synonym for `bool_and` in PG. We register it here as a scalar
//! no-op alias that forwards through the SQL rewriter (see
//! `rewrite_every_to_bool_and` in this module); in practice ORMs emit
//! `every(...)` inside aggregate SELECT lists, which DataFusion plans as a
//! group-by aggregate — the textual rewrite makes the name visible.

use std::any::Any;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime, Timelike, Utc};
use datafusion::arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, StringArray,
    StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// Public registration entry point
// ---------------------------------------------------------------------------

/// Register PG-compat scalar aliases and thin wrappers on `ctx`.
///
/// Safe to call multiple times (DataFusion overwrites by name).
pub(crate) fn register_pg_scalar_aliases(ctx: &SessionContext) {
    // ── Name aliases ──────────────────────────────────────────────────────────
    // `ceiling` is PG's alias for `ceil`.
    ctx.register_udf(make_alias_f64("ceiling", f64::ceil));
    // `sign` is PG's name for DataFusion's `signum`.
    ctx.register_udf(make_alias_f64("sign", |x: f64| {
        if x > 0.0 {
            1.0
        } else if x < 0.0 {
            -1.0
        } else {
            0.0
        }
    }));

    // ── Timestamp stubs (all return now() in v0.1) ───────────────────────────
    for name in &[
        "clock_timestamp",
        "transaction_timestamp",
        "statement_timestamp",
        "localtimestamp",
    ] {
        ctx.register_udf(ScalarUDF::from(NowStubUdf {
            name: name.to_string(),
            signature: Signature::nullary(Volatility::Volatile),
        }));
    }

    // `localtime` — returns a Time64 stub.  Most ORMs only probe this in
    // a DEFAULT or computed expression, never read the value directly.
    ctx.register_udf(ScalarUDF::from(LocaltimeUdf {
        signature: Signature::nullary(Volatility::Volatile),
    }));

    // ── make_time(h, m, s) ───────────────────────────────────────────────────
    ctx.register_udf(ScalarUDF::from(MakeTimeUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── make_timestamp(y, mo, d, h, mi, s) ───────────────────────────────────
    ctx.register_udf(ScalarUDF::from(MakeTimestampUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── make_interval — stub returning 0 interval for now ────────────────────
    // PG: make_interval(years=0, months=0, weeks=0, days=0, hours=0, mins=0, secs=0.0)
    // DataFusion has no native make_interval; register a stub that accepts
    // up to 7 Int64/Float64 args and returns 0 ns as text (sufficient for
    // ORM startup probes that only check the function resolves).
    ctx.register_udf(ScalarUDF::from(MakeIntervalUdf {
        signature: Signature::one_of(
            (0usize..=7)
                .map(|n| {
                    TypeSignature::Exact(
                        (0..n)
                            .map(|_| DataType::Int64)
                            .collect::<Vec<_>>(),
                    )
                })
                .collect(),
            Volatility::Immutable,
        ),
    }));

    // ── width_bucket(op, b1, b2, count) ─────────────────────────────────────
    ctx.register_udf(ScalarUDF::from(WidthBucketUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── to_number(text, fmt) — stub cast ─────────────────────────────────────
    // PG: to_number('12,345.67', '99G999D99') → numeric.
    // We parse the numeric part of the text and return Float64.
    ctx.register_udf(ScalarUDF::from(ToNumberUdf {
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        ),
    }));
}

// ---------------------------------------------------------------------------
// Generic Float64 alias helper
// ---------------------------------------------------------------------------

fn make_alias_f64(name: &str, f: fn(f64) -> f64) -> ScalarUDF {
    ScalarUDF::from(AliasF64Udf {
        name: name.to_string(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Float32]),
            ],
            Volatility::Immutable,
        ),
        func: f,
    })
}

#[derive(Debug)]
struct AliasF64Udf {
    name: String,
    signature: Signature,
    func: fn(f64) -> f64,
}

impl ScalarUDFImpl for AliasF64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("{} expects 1 argument, got {}", self.name, args.len());
        }
        let n = match &args[0] {
            ColumnarValue::Array(a) => a.len(),
            _ => 1,
        };
        let arr = args[0].clone().into_array(n)?;
        let f = self.func;
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let v = match arr.data_type() {
                DataType::Float64 => arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
                DataType::Int64 => arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i) as f64,
                DataType::Int32 => arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(i) as f64,
                _ => {
                    return exec_err!(
                        "{}: unsupported arg type {:?}",
                        self.name,
                        arr.data_type()
                    )
                }
            };
            out.append_value(f(v));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// Now stubs — clock_timestamp / transaction_timestamp / statement_timestamp /
//              localtimestamp all return the current wall-clock instant.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NowStubUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for NowStubUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let now_us = Utc::now().timestamp_micros();
        let mut b = TimestampMicrosecondBuilder::with_capacity(n)
            .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        for _ in 0..n {
            b.append_value(now_us);
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

// ---------------------------------------------------------------------------
// localtime → returns seconds-since-midnight as a string (stub)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct LocaltimeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for LocaltimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "localtime"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // PG returns `time without time zone`. We return Utf8 as a best-effort
        // stub (ORM probes just check the function resolves).
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let t = Utc::now();
        let s = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
        Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(
            Some(s),
        )))
    }
}

// ---------------------------------------------------------------------------
// make_time(h, m, s) → Utf8 "HH:MM:SS.ffffff"
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MakeTimeUdf {
    signature: Signature,
}

fn extract_i64(args: &[ColumnarValue], idx: usize, n: usize) -> DFResult<Vec<Option<i64>>> {
    let arr = args[idx].clone().into_array(n)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let v = match arr.data_type() {
            DataType::Int64 => arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i),
            DataType::Int32 => {
                arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as i64
            }
            _ => return exec_err!("make_time: arg {idx} must be integer"),
        };
        out.push(Some(v));
    }
    Ok(out)
}

fn extract_f64(args: &[ColumnarValue], idx: usize, n: usize) -> DFResult<Vec<Option<f64>>> {
    let arr = args[idx].clone().into_array(n)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let v = match arr.data_type() {
            DataType::Float64 => {
                arr.as_any().downcast_ref::<Float64Array>().unwrap().value(i)
            }
            DataType::Int64 => {
                arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64
            }
            DataType::Int32 => {
                arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as f64
            }
            _ => return exec_err!("make_time: arg {idx} must be numeric"),
        };
        out.push(Some(v));
    }
    Ok(out)
}

impl ScalarUDFImpl for MakeTimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "make_time"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // PG returns `time without time zone`. Utf8 is the practical bridge.
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 3 {
            return exec_err!("make_time expects 3 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let hours = extract_i64(args, 0, n)?;
        let mins = extract_i64(args, 1, n)?;
        let secs = extract_f64(args, 2, n)?;
        let mut out = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            match (hours[i], mins[i], secs[i]) {
                (Some(h), Some(m), Some(s)) => {
                    let whole = s as u64;
                    let frac = (s.fract() * 1_000_000.0).round() as u64;
                    if h < 0 || h > 23 || m < 0 || m > 59 || s < 0.0 || s >= 60.0 {
                        return exec_err!("make_time: value out of range");
                    }
                    if frac == 0 {
                        out.append_value(format!("{:02}:{:02}:{:02}", h, m, whole));
                    } else {
                        out.append_value(format!(
                            "{:02}:{:02}:{:02}.{:06}",
                            h, m, whole, frac
                        ));
                    }
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// make_timestamp(y, mo, d, h, mi, s) → Timestamp(Microsecond, None)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MakeTimestampUdf {
    signature: Signature,
}

impl ScalarUDFImpl for MakeTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "make_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 6 {
            return exec_err!("make_timestamp expects 6 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let years = extract_i64(args, 0, n)?;
        let months = extract_i64(args, 1, n)?;
        let days = extract_i64(args, 2, n)?;
        let hours = extract_i64(args, 3, n)?;
        let mins = extract_i64(args, 4, n)?;
        let secs = extract_f64(args, 5, n)?;
        let mut out = TimestampMicrosecondBuilder::with_capacity(n);
        for i in 0..n {
            match (years[i], months[i], days[i], hours[i], mins[i], secs[i]) {
                (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) => {
                    let date = NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32)
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamp: invalid date".into(),
                            )
                        })?;
                    let whole_s = s as u32;
                    let micros = (s.fract() * 1_000_000.0).round() as u32;
                    let time = NaiveTime::from_hms_micro_opt(h as u32, mi as u32, whole_s, micros)
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamp: invalid time".into(),
                            )
                        })?;
                    let dt = date
                        .and_time(time)
                        .and_utc()
                        .timestamp_micros();
                    out.append_value(dt);
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// make_interval — stub, always returns "0 seconds" as text (ORM probe only)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MakeIntervalUdf {
    signature: Signature,
}

impl ScalarUDFImpl for MakeIntervalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "make_interval"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        // v0.1 stub: returns "0 seconds" — sufficient for ORM startup probes.
        Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(
            Some("0 seconds".into()),
        )))
    }
}

// ---------------------------------------------------------------------------
// width_bucket(operand, b1, b2, count) → Int64
// Computes the bucket number in an equi-width histogram.  PG returns 0 for
// below-range, count+1 for above-range.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct WidthBucketUdf {
    signature: Signature,
}

impl ScalarUDFImpl for WidthBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "width_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 4 {
            return exec_err!("width_bucket expects 4 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let op_vals = extract_f64(args, 0, n)?;
        let b1_vals = extract_f64(args, 1, n)?;
        let b2_vals = extract_f64(args, 2, n)?;
        let cnt_vals = extract_i64(args, 3, n)?;
        let mut out = Int64Array::builder(n);
        for i in 0..n {
            match (op_vals[i], b1_vals[i], b2_vals[i], cnt_vals[i]) {
                (Some(op), Some(b1), Some(b2), Some(cnt)) => {
                    if cnt <= 0 {
                        return exec_err!("width_bucket: count must be positive");
                    }
                    if b1 == b2 {
                        return exec_err!("width_bucket: lower and upper bounds are equal");
                    }
                    let bucket = if b1 < b2 {
                        if op < b1 {
                            0i64
                        } else if op >= b2 {
                            cnt + 1
                        } else {
                            let frac = (op - b1) / (b2 - b1);
                            (frac * cnt as f64).floor() as i64 + 1
                        }
                    } else {
                        // reversed range
                        if op > b1 {
                            0i64
                        } else if op <= b2 {
                            cnt + 1
                        } else {
                            let frac = (b1 - op) / (b1 - b2);
                            (frac * cnt as f64).floor() as i64 + 1
                        }
                    };
                    out.append_value(bucket);
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// to_number(text, fmt) → Float64
// Strips non-numeric characters and parses.  PG's format string defines
// grouping/decimal separators; we simply strip all non-numeric characters
// except `.` and `-` and parse the result.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ToNumberUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToNumberUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_number"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("to_number expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let text_arr = args[0].clone().into_array(n)?;
        let texts = text_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "to_number: first argument must be text".into(),
                )
            })?;
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            if texts.is_null(i) {
                out.append_null();
                continue;
            }
            let raw = texts.value(i);
            // Strip everything except digits, decimal point, and leading minus.
            let cleaned: String = raw
                .chars()
                .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            match cleaned.parse::<f64>() {
                Ok(v) => out.append_value(v),
                Err(_) => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// SQL-string rewriter: `every(...)` → `bool_and(...)`
// ---------------------------------------------------------------------------

/// Rewrite `every(...)` to `bool_and(...)` so DataFusion's aggregate planner
/// resolves the call.  Called from the executor SQL-string pipeline alongside
/// the vector-operator and auth-schema rewriters.
pub(crate) fn rewrite_every_to_bool_and(sql: &str) -> String {
    // Case-insensitive token-boundary match for `every(`.
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        // Look for 'e'/'E' as first char of `every`.
        if (bytes[i] == b'e' || bytes[i] == b'E')
            && i + 5 <= len
            && bytes[i + 1].to_ascii_lowercase() == b'v'
            && bytes[i + 2].to_ascii_lowercase() == b'e'
            && bytes[i + 3].to_ascii_lowercase() == b'r'
            && bytes[i + 4].to_ascii_lowercase() == b'y'
        {
            // Check left boundary: must be start or non-ident character.
            let left_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            // Check right boundary: next char must be '(' or whitespace.
            let right_ok = i + 5 < len
                && (bytes[i + 5] == b'(' || bytes[i + 5] == b' ' || bytes[i + 5] == b'\t');
            if left_ok && right_ok {
                out.push_str("bool_and");
                i += 5;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_every() {
        assert_eq!(
            rewrite_every_to_bool_and("SELECT every(x) FROM t"),
            "SELECT bool_and(x) FROM t"
        );
        assert_eq!(
            rewrite_every_to_bool_and("SELECT EVERY(x) FROM t"),
            "SELECT bool_and(x) FROM t"
        );
        // Should not rewrite when part of a larger identifier.
        assert_eq!(
            rewrite_every_to_bool_and("SELECT noevery(x) FROM t"),
            "SELECT noevery(x) FROM t"
        );
    }
}
