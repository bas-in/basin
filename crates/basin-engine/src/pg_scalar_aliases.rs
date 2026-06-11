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
//! | `count`, `sum`, `avg`, `min`, `max`, `string_agg`, `bool_and`, `bool_or`, `bit_and`, `bit_or` | DataFusion aggregate UDAFs (builtin) |
//! | `array_agg` | `pg_agg_udf::PgArrayAggUdaf` (delegates to the DataFusion builtin; vectorized `ORDER BY` accumulator) |
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
    Array, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, ListArray,
    StringArray, StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

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
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32, DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32, DataType::Int32]),
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

    // ── make_timestamptz(y, mo, d, h, mi, s, tz) → Timestamp(Microsecond, UTC) ──
    // PG: make_timestamptz(year, month, day, hour, min, sec [, timezone])
    // Same arithmetic as make_timestamp but returns timestamptz.
    ctx.register_udf(ScalarUDF::from(MakeTimestamptzUdf {
        signature: Signature::one_of(
            vec![
                // 7-arg form with timezone string.
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64,
                    DataType::Utf8,
                ]),
                // 6-arg form without timezone (uses server timezone = UTC).
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Utf8,
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
                .map(|n| TypeSignature::Exact((0..n).map(|_| DataType::Int64).collect::<Vec<_>>()))
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
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));

    // ── current_database() → TEXT ────────────────────────────────────────────
    // Returns the current database name. Basin uses "postgres" as the default.
    ctx.register_udf(ScalarUDF::from(ConstTextUdf::new(
        "current_database",
        "postgres",
    )));

    // ── current_setting(name [, missing_ok]) → TEXT ──────────────────────────
    // Returns GUC setting. Stub returns empty string for any setting name.
    ctx.register_udf(ScalarUDF::from(CurrentSettingUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]),
            ],
            Volatility::Volatile,
        ),
    }));

    // ── set_config(name, value, is_local) → TEXT ─────────────────────────────
    // Sets a GUC setting. Stub returns the value argument unchanged.
    ctx.register_udf(ScalarUDF::from(SetConfigUdf {
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
            Volatility::Volatile,
        ),
    }));

    // ── pg_backend_pid() → INT4 ───────────────────────────────────────────────
    // Returns the PID of the backend process. Stub returns 0.
    ctx.register_udf(ScalarUDF::from(ConstInt32Udf::new("pg_backend_pid", 0)));

    // ── pg_postmaster_start_time() → TIMESTAMPTZ ─────────────────────────────
    // Returns the server start time. Stub returns now() (Timestamp microseconds UTC).
    ctx.register_udf(ScalarUDF::from(NowStubUdf {
        name: "pg_postmaster_start_time".to_string(),
        signature: Signature::nullary(Volatility::Volatile),
    }));

    // ── pg_is_in_recovery() → BOOL ───────────────────────────────────────────
    // Returns false — Basin is always primary / not a replica.
    ctx.register_udf(ScalarUDF::from(ConstBoolUdf::new(
        "pg_is_in_recovery",
        false,
    )));

    // ── timeofday() → TEXT ───────────────────────────────────────────────────
    // PG returns a human-readable timestamp string. Stub returns a fixed format
    // timestamp matching PG's output (e.g. "Thu Jan 01 00:00:00.000000 2026 UTC").
    ctx.register_udf(ScalarUDF::from(TimeofdayUdf {
        signature: Signature::nullary(Volatility::Volatile),
    }));

    // ── div(a, b) → same integer type ───────────────────────────────────────
    // PG's integer division (truncates toward zero, like Rust `a / b`).
    // DataFusion's `/` operator returns floored division for integers in some
    // contexts; `div(a, b)` is PG's explicit integer-quotient function.
    ctx.register_udf(ScalarUDF::from(DivUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int16, DataType::Int16]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Float32, DataType::Float32]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── array_fill(anyelement, ARRAY[dim1 [,dim2 ...]]) → anyarray ───────────
    // Fills an array with copies of a scalar value.  Accepts any 2-argument
    // call so it can handle both integer and numeric fill values.
    ctx.register_udf(ScalarUDF::from(ArrayFillUdf {
        signature: Signature::any(2, Volatility::Immutable),
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
                DataType::Int64 => {
                    arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64
                }
                DataType::Int32 => {
                    arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as f64
                }
                _ => return exec_err!("{}: unsupported arg type {:?}", self.name, arr.data_type()),
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let now_us = Utc::now().timestamp_micros();
        let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_data_type(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        );
        for _ in 0..n {
            b.append_value(now_us);
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

// ---------------------------------------------------------------------------
// localtime → returns seconds-since-midnight as a string (stub)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let t = Utc::now();
        let s = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some(s)),
        ))
    }
}

// ---------------------------------------------------------------------------
// make_time(h, m, s) → Utf8 "HH:MM:SS.ffffff"
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
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
            DataType::Int32 => arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as i64,
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
            DataType::Float64 => arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i),
            DataType::Int64 => arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64,
            DataType::Int32 => arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as f64,
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
                        out.append_value(format!("{:02}:{:02}:{:02}.{:06}", h, m, whole, frac));
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
                    let date = NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32).ok_or_else(
                        || {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamp: invalid date".into(),
                            )
                        },
                    )?;
                    let whole_s = s as u32;
                    let micros = (s.fract() * 1_000_000.0).round() as u32;
                    let time = NaiveTime::from_hms_micro_opt(h as u32, mi as u32, whole_s, micros)
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamp: invalid time".into(),
                            )
                        })?;
                    let dt = date.and_time(time).and_utc().timestamp_micros();
                    out.append_value(dt);
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// make_timestamptz(y, mo, d, h, mi, s [, tz]) → Timestamp(Microsecond, UTC)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct MakeTimestamptzUdf {
    signature: Signature,
}

impl ScalarUDFImpl for MakeTimestamptzUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "make_timestamptz"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() < 6 || args.len() > 7 {
            return exec_err!(
                "make_timestamptz expects 6 or 7 arguments, got {}",
                args.len()
            );
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
                    let date = NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32).ok_or_else(
                        || {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamptz: invalid date".into(),
                            )
                        },
                    )?;
                    let whole_s = s as u32;
                    let micros = (s.fract() * 1_000_000.0).round() as u32;
                    let time = NaiveTime::from_hms_micro_opt(h as u32, mi as u32, whole_s, micros)
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "make_timestamptz: invalid time".into(),
                            )
                        })?;
                    let dt = date.and_time(time).and_utc().timestamp_micros();
                    out.append_value(dt);
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            out.finish().with_timezone_opt(Some("UTC")),
        )))
    }
}

// ---------------------------------------------------------------------------
// rewrite_make_interval_named_args — convert PG named-arg form to positional
// ---------------------------------------------------------------------------

/// Rewrite `make_interval(years => 1, days => 30)` to positional form
/// `make_interval(1, 0, 0, 30, 0, 0, 0)` so DataFusion's planner accepts it.
///
/// PG positional order: (years, months, weeks, days, hours, mins, secs).
/// All unspecified args default to 0.  Best-effort text rewrite; only fires
/// when `make_interval` is followed by an opening `(` that contains `=>`.
pub(crate) fn rewrite_make_interval_named_args(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("make_interval") {
        return sql.to_string();
    }

    // Named-arg parameter name → positional index (0-based).
    let param_order = ["years", "months", "weeks", "days", "hours", "mins", "secs"];

    let mut out = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower_slice = out[search_from..].to_ascii_lowercase();
        let Some(rel) = lower_slice.find("make_interval") else {
            break;
        };
        let name_start = search_from + rel;
        let name_end = name_start + "make_interval".len();

        // Must be a word boundary: char before must not be ident.
        let before_ok = name_start == 0 || {
            let b = out.as_bytes()[name_start - 1];
            !b.is_ascii_alphanumeric() && b != b'_'
        };
        if !before_ok {
            search_from = name_end;
            continue;
        }

        // Skip whitespace then look for `(`.
        let bytes = out.as_bytes();
        let mut j = name_end;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j >= bytes.len() || bytes[j] != b'(' {
            search_from = name_end;
            continue;
        }
        let open = j;

        // Find the matching closing `)`.
        let mut depth = 1i32;
        let mut k = open + 1;
        while k < bytes.len() && depth > 0 {
            match bytes[k] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'\'' => {
                    k += 1;
                    while k < bytes.len() {
                        if bytes[k] == b'\'' {
                            if k + 1 < bytes.len() && bytes[k + 1] == b'\'' {
                                k += 2;
                                continue;
                            }
                            k += 1;
                            break;
                        }
                        k += 1;
                    }
                    continue;
                }
                _ => {}
            }
            k += 1;
        }
        let close = k - 1; // index of `)`

        let inner = out[open + 1..close].to_string();

        // Only rewrite if the args contain `=>` (named form).
        if !inner.contains("=>") {
            search_from = close + 1;
            continue;
        }

        // Parse named args: split by `,` at depth 0, then parse `name => value`.
        let mut positional = [0.0f64; 7]; // years, months, weeks, days, hours, mins, secs
        let inner_lower = inner.to_ascii_lowercase();
        let parts = split_at_top_level_commas(&inner_lower);
        for part in &parts {
            let part = part.trim();
            if let Some(arrow_pos) = part.find("=>") {
                let name = part[..arrow_pos].trim();
                let val_str = part[arrow_pos + 2..].trim();
                if let Some(idx) = param_order.iter().position(|&p| p == name) {
                    if let Ok(v) = val_str.parse::<f64>() {
                        positional[idx] = v;
                    }
                }
            }
        }

        // Build replacement: make_interval(y, mo, w, d, h, mi, s)
        let replacement = format!(
            "make_interval({}, {}, {}, {}, {}, {}, {})",
            positional[0] as i64,
            positional[1] as i64,
            positional[2] as i64,
            positional[3] as i64,
            positional[4] as i64,
            positional[5] as i64,
            positional[6] as i64,
        );
        out.replace_range(name_start..=close, &replacement);
        search_from = name_start + replacement.len();
    }
    out
}

/// Split a string at top-level commas (not inside parentheses or quotes).
fn split_at_top_level_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b',' if depth == 0 => {
                parts.push(s[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(s[start..].to_string());
    parts
}

// ---------------------------------------------------------------------------
// make_interval — stub, always returns "0 seconds" as text (ORM probe only)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Positional order: years, months, weeks, days, hours, mins, secs.
        // Each arg is Int64 (named-arg rewriter converts to positional ints).
        // Returns an ISO-8601-style interval text that DataFusion can parse
        // (e.g. "1 year 30 days 0 seconds").
        let get_i64 = |idx: usize| -> i64 {
            if idx >= args.args.len() {
                return 0;
            }
            match &args.args[idx] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v,
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v as i64,
                _ => 0,
            }
        };
        let years = get_i64(0);
        let months = get_i64(1);
        let weeks = get_i64(2);
        let days = get_i64(3);
        let hours = get_i64(4);
        let mins = get_i64(5);
        let secs = get_i64(6);

        // Convert to total seconds for a simple interval text representation.
        let total_days = years * 365 + months * 30 + weeks * 7 + days;
        let total_secs = hours * 3600 + mins * 60 + secs;

        let interval_text = if total_days == 0 && total_secs == 0 {
            "0 seconds".to_string()
        } else {
            let mut parts = Vec::new();
            if total_days != 0 {
                parts.push(format!("{total_days} days"));
            }
            if total_secs != 0 {
                parts.push(format!("{total_secs} seconds"));
            }
            parts.join(" ")
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            interval_text,
        ))))
    }
}

// ---------------------------------------------------------------------------
// width_bucket(operand, b1, b2, count) → Int64
// Computes the bucket number in an equi-width histogram.  PG returns 0 for
// below-range, count+1 for above-range.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
// ConstTextUdf — returns a fixed TEXT string regardless of input
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConstTextUdf {
    fn_name: &'static str,
    value: &'static str,
    signature: Signature,
}

impl ConstTextUdf {
    fn new(fn_name: &'static str, value: &'static str) -> Self {
        Self {
            fn_name,
            value,
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for ConstTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.fn_name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.value.to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// ConstInt32Udf — returns a fixed INT32 value regardless of input
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConstInt32Udf {
    fn_name: &'static str,
    value: i32,
    signature: Signature,
}

impl ConstInt32Udf {
    fn new(fn_name: &'static str, value: i32) -> Self {
        Self {
            fn_name,
            value,
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for ConstInt32Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.fn_name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(self.value))))
    }
}

// ---------------------------------------------------------------------------
// ConstBoolUdf — returns a fixed BOOL value regardless of input
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConstBoolUdf {
    fn_name: &'static str,
    value: bool,
    signature: Signature,
}

impl ConstBoolUdf {
    fn new(fn_name: &'static str, value: bool) -> Self {
        Self {
            fn_name,
            value,
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for ConstBoolUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.fn_name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
            self.value,
        ))))
    }
}

// ---------------------------------------------------------------------------
// current_setting(name [, missing_ok]) → TEXT
// Returns the current value of the named GUC parameter. Stub returns "".
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentSettingUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSettingUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "current_setting"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args
            .args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "";
            n
        ]))))
    }
}

// ---------------------------------------------------------------------------
// set_config(name, value, is_local) → TEXT
// Sets a configuration parameter. Stub returns the value argument unchanged.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct SetConfigUdf {
    signature: Signature,
}

impl ScalarUDFImpl for SetConfigUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "set_config"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Return the second argument (the value).
        if args.args.len() < 2 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                Some(String::new()),
            )));
        }
        Ok(args.args[1].clone())
    }
}

// ---------------------------------------------------------------------------
// timeofday() → TEXT
// Returns the current wall-clock time as a PG-formatted text string.
// PG format example: "Thu Jan 01 00:00:00.000000 2026 UTC"
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct TimeofdayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TimeofdayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "timeofday"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let now = Utc::now();
        // Format as PG-compatible: "Thu Jan 01 00:00:00.000000 2026 UTC"
        let formatted = now.format("%a %b %d %H:%M:%S%.6f %Y UTC").to_string();
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(formatted))))
    }
}

// ---------------------------------------------------------------------------
// div(a, b) → same integer type (truncated integer division)
// PG: div(9, 4) = 2  (truncates toward zero, same as Rust integer division)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct DivUdf {
    signature: Signature,
}

impl ScalarUDFImpl for DivUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "div"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        if arg_types.is_empty() {
            return Ok(DataType::Int64);
        }
        Ok(arg_types[0].clone())
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("div expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let a = args[0].clone().into_array(n)?;
        let b = args[1].clone().into_array(n)?;

        // Dispatch on type.
        if let (Some(av), Some(bv)) = (
            a.as_any().downcast_ref::<Int64Array>(),
            b.as_any().downcast_ref::<Int64Array>(),
        ) {
            let mut out = Int64Array::builder(n);
            for i in 0..n {
                if av.is_null(i) || bv.is_null(i) {
                    out.append_null();
                } else if bv.value(i) == 0 {
                    return exec_err!("div: division by zero");
                } else {
                    out.append_value(av.value(i) / bv.value(i));
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        if let (Some(av), Some(bv)) = (
            a.as_any().downcast_ref::<Int32Array>(),
            b.as_any().downcast_ref::<Int32Array>(),
        ) {
            let mut out = Int32Array::builder(n);
            for i in 0..n {
                if av.is_null(i) || bv.is_null(i) {
                    out.append_null();
                } else if bv.value(i) == 0 {
                    return exec_err!("div: division by zero");
                } else {
                    out.append_value(av.value(i) / bv.value(i));
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        if let (Some(av), Some(bv)) = (
            a.as_any().downcast_ref::<Int16Array>(),
            b.as_any().downcast_ref::<Int16Array>(),
        ) {
            let mut out = Int16Array::builder(n);
            for i in 0..n {
                if av.is_null(i) || bv.is_null(i) {
                    out.append_null();
                } else if bv.value(i) == 0 {
                    return exec_err!("div: division by zero");
                } else {
                    out.append_value(av.value(i) / bv.value(i));
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        if let (Some(av), Some(bv)) = (
            a.as_any().downcast_ref::<Float64Array>(),
            b.as_any().downcast_ref::<Float64Array>(),
        ) {
            let mut out = Float64Array::builder(n);
            for i in 0..n {
                if av.is_null(i) || bv.is_null(i) {
                    out.append_null();
                } else if bv.value(i) == 0.0 {
                    return exec_err!("div: division by zero");
                } else {
                    out.append_value((av.value(i) / bv.value(i)).trunc());
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        if let (Some(av), Some(bv)) = (
            a.as_any().downcast_ref::<Float32Array>(),
            b.as_any().downcast_ref::<Float32Array>(),
        ) {
            let mut out = Float32Array::builder(n);
            for i in 0..n {
                if av.is_null(i) || bv.is_null(i) {
                    out.append_null();
                } else if bv.value(i) == 0.0 {
                    return exec_err!("div: division by zero");
                } else {
                    out.append_value((av.value(i) / bv.value(i)).trunc());
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        exec_err!("div: unsupported argument types")
    }
}

// ---------------------------------------------------------------------------
// SQL-string rewriter: `every(...)` → `bool_and(...)`
// ---------------------------------------------------------------------------

/// Rewrite `every(...)` to `bool_and(...) AS every` so DataFusion's aggregate
/// planner resolves the call, but the output column has a distinct name.
/// Without the AS alias, `every(x)` and `bool_and(x)` in the same SELECT
/// would both map to the UDAF named `bool_and` and DataFusion would reject the
/// plan with "Projections require unique expression names".
pub(crate) fn rewrite_every_to_bool_and(sql: &str) -> String {
    // Case-insensitive token-boundary match for `every(`.
    let mut out = String::with_capacity(sql.len() + 32);
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
                // Emit `bool_and` in place of `every`.
                out.push_str("bool_and");
                i += 5;
                // Find the opening `(` and its matching `)`, then append
                // ` AS every` right after the closing paren to give DataFusion
                // a unique output column name.
                // Skip whitespace between function name and `(`.
                let ws_start = i;
                while i < len && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i < len && bytes[i] == b'(' {
                    // Emit whitespace + `(` + body + `)`.
                    let open = i;
                    i += 1;
                    let mut depth = 1i32;
                    while i < len && depth > 0 {
                        match bytes[i] {
                            b'(' => depth += 1,
                            b')' => depth -= 1,
                            b'\'' => {
                                out.push_str(&sql[ws_start..i + 1]);
                                // Advance inside string literal.
                                i += 1;
                                while i < len {
                                    if bytes[i] == b'\'' {
                                        if i + 1 < len && bytes[i + 1] == b'\'' {
                                            i += 2;
                                            continue;
                                        }
                                        i += 1;
                                        break;
                                    }
                                    i += 1;
                                }
                                out.push_str(&sql[open + 1..i]);
                                continue;
                            }
                            _ => {}
                        }
                        i += 1;
                    }
                    // `i` is now past the closing `)`.
                    out.push_str(&sql[ws_start..i]);
                    out.push_str(" AS every");
                } else {
                    // Unexpected — just emit as-is (no closing paren found).
                    out.push_str(&sql[ws_start..i]);
                }
                continue;
            }
        }
        // Pass through one full Unicode scalar value so multi-byte UTF-8
        // chars (em-dash, emoji, accented letters …) are not corrupted.
        // `b as char` treats each byte as a Latin-1 codepoint; pushing the
        // full char (or copying the raw bytes for non-char-boundary positions)
        // preserves the original encoding.
        let char_len = match bytes[i] {
            b if b < 0x80 => {
                out.push(b as char);
                1
            }
            b if b < 0xC0 => {
                out.push(b as char);
                1
            } // stray continuation — pass through
            _ => {
                // Lead byte: find char boundary end and push_str the slice.
                let ch_width = match bytes[i] {
                    b if b < 0xE0 => 2,
                    b if b < 0xF0 => 3,
                    _ => 4,
                };
                let end = (i + ch_width).min(len);
                out.push_str(&sql[i..end]);
                ch_width
            }
        };
        i += char_len;
    }
    out
}

/// Rewrite aggregate function alias calls that would produce duplicate output
/// column names when DataFusion normalises them to the primary UDAF name.
///
/// DataFusion maps `stddev_samp(x)` to the UDAF whose primary name is
/// `stddev`, so a SELECT with both `stddev(x)` and `stddev_samp(x)` produces
/// two columns both named `stddev(t.x)` and DataFusion rejects the plan.
/// Adding an explicit `AS "original_name"` alias avoids the collision.
///
/// Handles:
/// - `stddev_samp(...)` → `stddev_samp(...) AS stddev_samp`
/// - `var_samp(...)` / `var(...)` coexisting with `variance(...)` — handled by
///   the `variance → var(x) AS variance` pass in `rewrite_pg_agg_aliases`.
pub(crate) fn rewrite_agg_unique_aliases(sql: &str) -> String {
    // Pairs of (function_name, alias_to_add). Only add the alias when the
    // name would collide with another function that resolves to the same UDAF.
    // We always add the alias for these known-alias names; the extra AS clause
    // is harmless when only one of the pair appears.
    const ALWAYS_ALIAS: &[(&str, &str)] =
        &[("stddev_samp", "stddev_samp"), ("var_samp", "var_samp")];
    let mut s = sql.to_string();
    for (func_name, alias) in ALWAYS_ALIAS {
        s = add_alias_after_call(&s, func_name, alias);
    }
    s
}

/// For each bare call to `func_name(...)` in `sql` that is **not** already
/// followed by `AS <something>`, append ` AS alias_name` after the closing `)`.
fn add_alias_after_call(sql: &str, func_name: &str, alias: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(func_name) else {
            break;
        };
        let name_start = search_from + rel;
        let name_end = name_start + func_name.len();
        let bytes = s.as_bytes();

        // Word boundary checks.
        let pre_ok = name_start == 0 || {
            let b = bytes[name_start - 1];
            !(b.is_ascii_alphanumeric() || b == b'_')
        };
        let post_ok = name_end >= s.len() || {
            let b = bytes[name_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !pre_ok || !post_ok {
            search_from = name_end;
            continue;
        }

        // Find the opening `(`.
        let mut j = name_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() {
            j += 1;
        }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = name_end;
            continue;
        }
        // Find the matching `)`.
        let open = j;
        let mut k = open + 1;
        let mut depth = 1i32;
        while k < s.len() && depth > 0 {
            match s.as_bytes()[k] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'\'' => {
                    k += 1;
                    while k < s.len() {
                        if s.as_bytes()[k] == b'\'' {
                            if k + 1 < s.len() && s.as_bytes()[k + 1] == b'\'' {
                                k += 2;
                                continue;
                            }
                            k += 1;
                            break;
                        }
                        k += 1;
                    }
                    continue;
                }
                _ => {}
            }
            k += 1;
        }
        let close = k - 1; // index of `)`

        // Check if already has an `AS` alias right after the `)`.
        let after = s[close + 1..].trim_start();
        if after.to_ascii_lowercase().starts_with("as ")
            || after.starts_with("as\t")
            || after.starts_with("as\n")
        {
            // Already aliased — skip.
            search_from = close + 1;
            continue;
        }

        // Insert ` AS alias_name` after the `)`.
        let insert_pos = close + 1;
        let insertion = format!(" AS {alias}");
        s.insert_str(insert_pos, &insertion);
        search_from = insert_pos + insertion.len();
    }
    s
}

// ---------------------------------------------------------------------------
// array_fill(anyelement, ARRAY[dim1 [,dim2 ...]]) → anyarray
// ---------------------------------------------------------------------------

/// `array_fill(val, dims)` — return an array of `dims[0]` copies of `val`
/// (or a nested array for multi-dimensional dims).
///
/// Postgres: `array_fill(0, ARRAY[3])` → `{0,0,0}` (int4[]).
///
/// Basin returns a 1-D `List<Int64>` or `List<Float64>` depending on the
/// fill value type; multi-dimensional dims are supported by flattening into
/// a 1-D list of the total element count (dims[0] * dims[1] * ...).
///
/// The `dims` argument is the result of evaluating `ARRAY[n1, n2, ...]`, which
/// DataFusion materialises as a `List<Int64>` scalar.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayFillUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayFillUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_fill"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return type is List<fill_value_type>.
        let elem_type = if arg_types.is_empty() {
            DataType::Int64
        } else {
            arg_types[0].clone()
        };
        Ok(DataType::List(Arc::new(Field::new_list_field(
            elem_type, true,
        ))))
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!(
                "array_fill requires exactly 2 arguments, got {}",
                args.len()
            );
        }

        // ── arg 0: fill value ──────────────────────────────────────────────
        let fill_scalar = match &args[0] {
            ColumnarValue::Scalar(sv) => sv.clone(),
            ColumnarValue::Array(arr) => {
                if arr.len() == 0 {
                    return exec_err!("array_fill: fill value array is empty");
                }
                ScalarValue::try_from_array(arr.as_ref(), 0)?
            }
        };

        // ── arg 1: dims array ──────────────────────────────────────────────
        // DataFusion evaluates `ARRAY[3]` → List<Int64> scalar containing [3].
        let dims_arr: ArrayRef = args[1].clone().into_array(1)?;
        let dims: Vec<i64> = extract_dims_from_list(&dims_arr)?;

        if dims.is_empty() {
            return exec_err!("array_fill: dims array must have at least 1 element");
        }
        for &d in &dims {
            if d < 0 {
                return exec_err!("array_fill: dimension must be non-negative, got {d}");
            }
        }

        // Total element count = product of all dims.
        let total: i64 = dims.iter().product();
        if total > 10_000_000 {
            return exec_err!("array_fill: requested {total} elements exceeds 10 million limit");
        }

        // Build a list of `total` copies of `fill_scalar`.
        let elements: Vec<ScalarValue> = (0..total as usize).map(|_| fill_scalar.clone()).collect();
        let elem_type = fill_scalar.data_type();
        let list_arr = ScalarValue::new_list(&elements, &elem_type, true);
        Ok(ColumnarValue::Scalar(ScalarValue::List(list_arr)))
    }
}

/// Extract dimension values from the dims argument (a `List<Int64>` scalar or
/// a `Int64Array` with a single row).
fn extract_dims_from_list(arr: &ArrayRef) -> DFResult<Vec<i64>> {
    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "array_fill: dims not a ListArray".into(),
                )
            })?;
            if list.len() == 0 {
                return Ok(vec![]);
            }
            // The first (and only, for a scalar) row.
            let values = list.value(0);
            let ints = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "array_fill: dims list must contain Int64 values".into(),
                    )
                })?;
            Ok((0..ints.len()).map(|i| ints.value(i)).collect())
        }
        DataType::Int64 => {
            let ints = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "array_fill: dims not Int64Array".into(),
                )
            })?;
            Ok((0..ints.len()).map(|i| ints.value(i)).collect())
        }
        other => exec_err!("array_fill: expected List<Int64> for dims, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_every() {
        // The rewriter preserves PG's output column label (`every`) via an
        // explicit alias so result-set column names match Postgres.
        assert_eq!(
            rewrite_every_to_bool_and("SELECT every(x) FROM t"),
            "SELECT bool_and(x) AS every FROM t"
        );
        assert_eq!(
            rewrite_every_to_bool_and("SELECT EVERY(x) FROM t"),
            "SELECT bool_and(x) AS every FROM t"
        );
        // Should not rewrite when part of a larger identifier.
        assert_eq!(
            rewrite_every_to_bool_and("SELECT noevery(x) FROM t"),
            "SELECT noevery(x) FROM t"
        );
    }

    // ── array_fill ────────────────────────────────────────────────────────────

    #[test]
    fn array_fill_1d_int_three_zeros() {
        use datafusion::arrow::array::ListArray;
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::logical_expr::ScalarFunctionArgs;
        use datafusion::scalar::ScalarValue;

        let udf = ArrayFillUdf {
            signature: Signature::any(2, Volatility::Immutable),
        };

        // Simulate: array_fill(0, ARRAY[3])
        // dims = List<Int64> with value [3]
        let fill = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));
        let dim_list =
            ScalarValue::new_list(&[ScalarValue::Int64(Some(3))], &DataType::Int64, true);
        let dims = ColumnarValue::Scalar(ScalarValue::List(dim_list));
        let return_field = Arc::new(Field::new(
            "_",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            true,
        ));

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![fill, dims],
                arg_fields: vec![],
                number_rows: 1,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::List(list_arr)) => {
                assert_eq!(list_arr.len(), 1, "outer list has 1 row");
                let inner = list_arr.value(0);
                let ints = inner
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64Array");
                assert_eq!(ints.len(), 3, "should have 3 elements");
                assert_eq!(ints.value(0), 0);
                assert_eq!(ints.value(1), 0);
                assert_eq!(ints.value(2), 0);
            }
            other => panic!("expected ScalarValue::List, got {other:?}"),
        }
    }

    #[test]
    fn array_fill_2d_total_six_elements() {
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::logical_expr::ScalarFunctionArgs;
        use datafusion::scalar::ScalarValue;

        let udf = ArrayFillUdf {
            signature: Signature::any(2, Volatility::Immutable),
        };

        // Simulate: array_fill(0, ARRAY[2,3]) — total 6 elements
        let fill = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));
        let dim_list = ScalarValue::new_list(
            &[ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(3))],
            &DataType::Int64,
            true,
        );
        let dims = ColumnarValue::Scalar(ScalarValue::List(dim_list));
        let return_field = Arc::new(Field::new(
            "_",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            true,
        ));

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![fill, dims],
                arg_fields: vec![],
                number_rows: 1,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::List(list_arr)) => {
                let inner = list_arr.value(0);
                assert_eq!(inner.len(), 6, "2×3 = 6 elements");
            }
            other => panic!("expected ScalarValue::List, got {other:?}"),
        }
    }

    // ── make_interval named-arg rewriter ─────────────────────────────────────

    #[test]
    fn test_rewrite_make_interval_named_args_years_days() {
        let sql = "SELECT make_interval(years => 1, days => 30)";
        let out = rewrite_make_interval_named_args(sql);
        // years=1 → index 0; days=30 → index 3; rest 0
        assert_eq!(out, "SELECT make_interval(1, 0, 0, 30, 0, 0, 0)");
    }

    #[test]
    fn test_rewrite_make_interval_named_args_passthrough_positional() {
        // Positional call (no =>) must be left unchanged.
        let sql = "SELECT make_interval(1, 0, 0, 30, 0, 0, 0)";
        let out = rewrite_make_interval_named_args(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn test_rewrite_make_interval_named_args_no_match() {
        // No make_interval → fast path, no change.
        let sql = "SELECT 1 + 1";
        let out = rewrite_make_interval_named_args(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn test_rewrite_make_interval_named_args_hours_only() {
        let sql = "SELECT make_interval(hours => 3)";
        let out = rewrite_make_interval_named_args(sql);
        // hours=3 → index 4
        assert_eq!(out, "SELECT make_interval(0, 0, 0, 0, 3, 0, 0)");
    }
}
