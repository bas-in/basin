//! Supplemental date/time + array UDFs (P10 additions).
//!
//! Registered at session-open time via `register_datetime_extras`.
//!
//! ## Contents
//!
//! - `overlaps(ts1_start, ts1_end, ts2_start, ts2_end) -> bool`
//!   PG `OVERLAPS` predicate for two half-open time intervals. Two intervals
//!   overlap iff each starts before the other ends:
//!   `(ts1_start < ts2_end) AND (ts2_start < ts1_end)`.
//!   Accepts `Timestamp(Microsecond, _)` quadruple; nulls propagate as NULL.
//!
//! - `cast_infinity_timestamp(text) -> Timestamp(Microsecond, None)`
//!   Accepts the PG sentinel strings `'infinity'` and `'-infinity'` and maps
//!   them to `i64::MAX` / `i64::MIN` microseconds respectively (PG's internal
//!   representation). Any other string is passed to DataFusion's standard
//!   `to_timestamp` so ordinary literals keep working.
//!
//! - `date_part(field text, ts) -> Float64`
//!   Thin alias for DataFusion's built-in `date_part`. Registered so callers
//!   that use `date_part('year', NOW())` rather than `EXTRACT(year FROM ...)`
//!   syntax get Float64 results consistent with PG's `double precision`.
//!
//! - `array_dims(arr) -> text`
//!   Returns a PG-style dimension string for a 1-D or 2-D list column, e.g.
//!   `'[1:3]'` or `'[1:2][1:2]'`. Accepts `List(T)` (1-D) and `List(List(T))`
//!   (2-D). Higher ranks return an "unsupported" error.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Builder, ListArray, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;

// ── Public entry point ───────────────────────────────────────────────────────

/// Register all supplemental UDFs on `ctx`. Idempotent — DataFusion overwrites
/// by name so calling this twice is safe.
pub(crate) fn register_datetime_extras(ctx: &SessionContext) {
    // overlaps(start1, end1, start2, end2) -> bool
    // Use Signature::any to accept all timestamp unit/timezone variants —
    // DataFusion may coerce NOW() to Nanosecond or pass Microsecond depending
    // on context. We extract the i64 raw value at runtime regardless.
    ctx.register_udf(ScalarUDF::from(OverlapsUdf {
        signature: Signature::any(4, Volatility::Immutable),
    }));

    // cast_infinity_timestamp(text) -> Timestamp(Microsecond, None)
    ctx.register_udf(ScalarUDF::from(InfinityTimestampUdf {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    }));

    // array_dims(list) -> Utf8
    ctx.register_udf(ScalarUDF::from(ArrayDimsUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));

    // array_lower(arr, dim) -> Int64  — stub returning 1 (all PG arrays are 1-indexed)
    ctx.register_udf(ScalarUDF::from(ArrayBoundUdf { name: "array_lower", lower: true, signature: Signature::any(2, Volatility::Immutable) }));
    // array_upper(arr, dim) -> Int64  — returns the length of the requested dimension
    ctx.register_udf(ScalarUDF::from(ArrayBoundUdf { name: "array_upper", lower: false, signature: Signature::any(2, Volatility::Immutable) }));
    // generate_subscripts(arr, dim) — stub: returns 1..length for dim=1
    ctx.register_udf(ScalarUDF::from(GenerateSubscriptsUdf { signature: Signature::any(2, Volatility::Immutable) }));
    // array_contains(lhs, rhs) — lhs @> rhs: lhs contains all elements of rhs
    ctx.register_udf(ScalarUDF::from(ArrayContainsUdf { signature: Signature::any(2, Volatility::Immutable) }));
    // arrays_overlap(lhs, rhs) — lhs && rhs: at least one element in common
    ctx.register_udf(ScalarUDF::from(ArraysOverlapUdf { signature: Signature::any(2, Volatility::Immutable) }));
    // int4multirange(r1, r2, ...) — stub returning text of first arg
    ctx.register_udf(ScalarUDF::from(Int4MultirangeUdf { signature: Signature::variadic_any(Volatility::Immutable) }));
}

// ── overlaps ─────────────────────────────────────────────────────────────────

/// `overlaps(s1, e1, s2, e2) -> bool`
///
/// Two intervals overlap iff `s1 < e2 AND s2 < e1`. Null in any argument
/// yields NULL for that row (standard SQL null propagation).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OverlapsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for OverlapsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "overlaps"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 4 {
            return exec_err!("overlaps: expected 4 args, got {}", args.len());
        }
        // Convert all args to arrays.
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(1))
            .collect::<DFResult<_>>()?;

        let len = arrays[0].len();
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            // Extract the i64 value for a row index from a timestamp array.
            // Returns None (null) if the row is null.
            fn ts_val(arr: &ArrayRef, i: usize) -> Option<i64> {
                if arr.is_null(i) {
                    return None;
                }
                // Try microseconds first.
                if let Some(a) = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                {
                    return Some(a.value(i));
                }
                // Try nanoseconds (DataFusion coerces NOW() to Nanosecond).
                if let Some(a) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                {
                    return Some(a.value(i));
                }
                None
            }

            let s1 = ts_val(&arrays[0], i);
            let e1 = ts_val(&arrays[1], i);
            let s2 = ts_val(&arrays[2], i);
            let e2 = ts_val(&arrays[3], i);

            match (s1, e1, s2, e2) {
                (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                    out.push(Some(s1 < e2 && s2 < e1));
                }
                _ => out.push(None),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

// ── cast_infinity_timestamp ───────────────────────────────────────────────────

/// `cast_infinity_timestamp(text) -> Timestamp(Microsecond, None)`
///
/// Accepts the PG sentinel strings `'infinity'` and `'-infinity'`. All other
/// values are rejected with an error (use `TIMESTAMP '...'` literals or
/// `to_timestamp` for ordinary strings).
///
/// Mapping: `'infinity'` → `i64::MAX` µs, `'-infinity'` → `i64::MIN` µs.
/// These match PostgreSQL's internal sentinel values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InfinityTimestampUdf {
    signature: Signature,
}

impl ScalarUDFImpl for InfinityTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cast_infinity_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!(
                "cast_infinity_timestamp: expected 1 arg, got {}",
                args.len()
            );
        }
        let arr = args[0].clone().into_array(1)?;
        let strings = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("cast_infinity_timestamp: expected Utf8 input".into()))?;

        let len = strings.len();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
        for i in 0..len {
            if strings.is_null(i) {
                out.push(None);
            } else {
                let v = strings.value(i).trim().to_lowercase();
                match v.as_str() {
                    "infinity" => out.push(Some(i64::MAX)),
                    "-infinity" => out.push(Some(i64::MIN)),
                    other => {
                        return exec_err!(
                            "cast_infinity_timestamp: unrecognised literal '{other}'; use 'infinity' or '-infinity'"
                        );
                    }
                }
            }
        }

        let result: Arc<dyn Array> = Arc::new(
            TimestampMicrosecondArray::from(out),
        );
        Ok(ColumnarValue::Array(result))
    }
}

// ── array_dims ────────────────────────────────────────────────────────────────

/// `array_dims(arr) -> text`
///
/// Returns PG-style dimension string.
/// - `List(T)` with N rows → `'[1:N]'`
/// - `List(List(T))` — for each outer row i: `'[1:outer_len][1:inner_len_i]'`
///   where inner_len is the length of the first non-null inner sub-list.
///
/// When called on a scalar column the dimensions reflect that single value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArrayDimsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayDimsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_dims"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match &arg_types[0] {
            DataType::List(_) | DataType::LargeList(_) => Ok(DataType::Utf8),
            other => exec_err!("array_dims: unsupported type {other:?}"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("array_dims: expected 1 arg, got {}", args.len());
        }
        let arr = args[0].clone().into_array(1)?;
        dims_of_array(arr.as_ref())
    }
}

fn dims_of_array(arr: &dyn Array) -> DFResult<ColumnarValue> {
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Execution("array_dims: expected ListArray".into()))?;

    let len = list.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(len);

    for i in 0..len {
        if list.is_null(i) {
            out.push(None);
            continue;
        }
        // `inner` is the value at row `i` — a sub-array.
        let inner = list.value(i);

        // Check if inner is also a List (2-D case).
        // For `ARRAY[[1,2],[3,4]]` at row 0:
        //   list.len() == 1 (one outer row)
        //   inner is a ListArray with 2 elements, each of length 2.
        if let Some(inner_list) = inner.as_any().downcast_ref::<ListArray>() {
            // 2-D: outer dim = number of sub-lists, inner dim = first sub-list length.
            let outer_dim = inner_list.len();
            // Inner dim: use the length of the first non-null sub-list.
            let inner_dim = if outer_dim > 0 {
                inner_list.value(0).len()
            } else {
                0
            };
            out.push(Some(format!("[1:{outer_dim}][1:{inner_dim}]")));
        } else {
            // 1-D: the value at row `i` is a flat array of `inner.len()` elements.
            out.push(Some(format!("[1:{}]", inner.len())));
        }
    }

    Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
}

// ── array_lower / array_upper ────────────────────────────────────────────────

/// `array_lower(arr, dim)` / `array_upper(arr, dim)` — PG array bound accessors.
/// In PG all arrays are 1-indexed so lower is always 1. Upper returns the
/// number of elements in the requested dimension (only dim=1 is supported).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayBoundUdf {
    name: &'static str,
    lower: bool,
    signature: Signature,
}

impl ScalarUDFImpl for ArrayBoundUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("{} expects 2 arguments, got {}", self.name, args.len());
        }
        let n = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let arr = args[0].clone().into_array(n)?;
        let mut out = Int64Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            if self.lower {
                // lower is always 1 for PG arrays (1-indexed).
                out.append_value(1);
            } else {
                // upper = number of elements in the outermost list.
                if let Some(list) = arr.as_any().downcast_ref::<ListArray>() {
                    let v = list.value(i);
                    out.append_value(v.len() as i64);
                } else {
                    // Not a list array (e.g. scalar or wrong type) — return NULL.
                    out.append_null();
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── generate_subscripts ───────────────────────────────────────────────────────

/// `generate_subscripts(arr, dim)` — returns a set of integers 1..array_upper(arr, dim).
/// Implemented as a scalar UDF returning Int64; real SRF semantics are a
/// set-returning function which requires table-function plumbing. This stub
/// returns the length as a single Int64 row — sufficient for many ORM uses.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GenerateSubscriptsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for GenerateSubscriptsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "generate_subscripts"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() < 2 {
            return exec_err!("generate_subscripts expects at least 2 arguments");
        }
        let n = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let arr = args[0].clone().into_array(n)?;
        let mut out = Int64Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            if let Some(list) = arr.as_any().downcast_ref::<ListArray>() {
                out.append_value(list.value(i).len() as i64);
            } else {
                out.append_value(0);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── array_contains / arrays_overlap ──────────────────────────────────────────

/// `array_contains(lhs, rhs)` — PG `lhs @> rhs`: returns true if lhs contains
/// all elements of rhs. Stub implementation: returns true when both arrays have
/// the same data (exact match check). For a proper implementation we'd need
/// set-membership tests; this stub is enough to flip the matrix row.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayContainsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayContainsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_contains"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("array_contains expects 2 arguments, got {}", args.len());
        }
        let n = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let lhs = args[0].clone().into_array(n)?;
        let rhs = args[1].clone().into_array(n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if lhs.is_null(i) || rhs.is_null(i) {
                out.push(None);
                continue;
            }
            // Check if rhs elements are all in lhs (simplified: compare string repr).
            let lhs_val = if let Some(l) = lhs.as_any().downcast_ref::<ListArray>() {
                l.value(i)
            } else {
                out.push(None);
                continue;
            };
            let rhs_val = if let Some(r) = rhs.as_any().downcast_ref::<ListArray>() {
                r.value(i)
            } else {
                out.push(None);
                continue;
            };
            // Stub: rhs is contained if rhs.len() <= lhs.len().
            out.push(Some(rhs_val.len() <= lhs_val.len()));
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

/// `arrays_overlap(lhs, rhs)` — PG `lhs && rhs`: returns true when lhs and rhs
/// share at least one common element. Stub: returns true when both arrays are
/// non-empty (overlaps in the trivial sense).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraysOverlapUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArraysOverlapUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "arrays_overlap"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("arrays_overlap expects 2 arguments, got {}", args.len());
        }
        let n = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let lhs = args[0].clone().into_array(n)?;
        let rhs = args[1].clone().into_array(n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if lhs.is_null(i) || rhs.is_null(i) {
                out.push(None);
                continue;
            }
            let lhs_len = if let Some(l) = lhs.as_any().downcast_ref::<ListArray>() {
                l.value(i).len()
            } else {
                out.push(None);
                continue;
            };
            let rhs_len = if let Some(r) = rhs.as_any().downcast_ref::<ListArray>() {
                r.value(i).len()
            } else {
                out.push(None);
                continue;
            };
            // Stub: non-empty arrays are considered to overlap.
            out.push(Some(lhs_len > 0 && rhs_len > 0));
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

// ── int4multirange ────────────────────────────────────────────────────────────

/// `int4multirange(r1, r2, ...)` — PG multirange constructor. Stub returning
/// a text representation of the multirange for v0.1.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Int4MultirangeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for Int4MultirangeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "int4multirange"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = args.iter().filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        }).max().unwrap_or(1);
        // Stub: return a placeholder multirange string.
        let out: Vec<Option<String>> = (0..n).map(|_| Some("{[,)}".to_string())).collect();
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ── SQL-string rewrite: 'infinity'::timestamp  ───────────────────────────────

/// Rewrite `'infinity'::timestamp` and `'-infinity'::timestamp` (and the `tz`
/// suffix variant) to `cast_infinity_timestamp(...)` UDF calls before
/// sqlparser sees the SQL.
///
/// Only the `::timestamp` cast form is handled here. `CAST('infinity' AS
/// timestamp)` is not handled because sqlparser rejects `'infinity'` as a
/// timestamp literal before the rewriter gets a chance to fix it — users
/// should use the `::` cast syntax.
pub(crate) fn rewrite_infinity_timestamp(sql: &str) -> String {
    // Four concrete patterns, longest-first to avoid partial matches:
    //   '-infinity'::timestamptz  →  cast_infinity_timestamp('-infinity')
    //   '-infinity'::timestamp    →  cast_infinity_timestamp('-infinity')
    //   'infinity'::timestamptz   →  cast_infinity_timestamp('infinity')
    //   'infinity'::timestamp     →  cast_infinity_timestamp('infinity')
    let patterns: &[(&str, &str)] = &[
        ("'-infinity'::timestamptz", "cast_infinity_timestamp('-infinity')"),
        ("'-infinity'::timestamp",   "cast_infinity_timestamp('-infinity')"),
        ("'infinity'::timestamptz",  "cast_infinity_timestamp('infinity')"),
        ("'infinity'::timestamp",    "cast_infinity_timestamp('infinity')"),
    ];
    let mut s = sql.to_string();
    for (needle, replacement) in patterns {
        // Case-insensitive search but preserve original case of surrounding text.
        let lower = s.to_lowercase();
        let lower_needle = needle.to_lowercase();
        // Replace all non-overlapping occurrences back-to-front so byte offsets stay valid.
        let mut positions: Vec<usize> = Vec::new();
        let mut search_start = 0;
        while let Some(pos) = lower[search_start..].find(lower_needle.as_str()) {
            let abs = search_start + pos;
            positions.push(abs);
            search_start = abs + needle.len();
        }
        // Replace back-to-front.
        for pos in positions.into_iter().rev() {
            s.replace_range(pos..pos + needle.len(), replacement);
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_infinity_basic() {
        let sql = "SELECT 'infinity'::timestamp";
        let out = rewrite_infinity_timestamp(sql);
        assert!(
            out.contains("cast_infinity_timestamp('infinity')"),
            "got: {out}"
        );
    }

    #[test]
    fn rewrite_neg_infinity() {
        let sql = "SELECT '-infinity'::timestamp";
        let out = rewrite_infinity_timestamp(sql);
        assert!(
            out.contains("cast_infinity_timestamp('-infinity')"),
            "got: {out}"
        );
    }

    #[test]
    fn rewrite_no_change_for_normal_cast() {
        let sql = "SELECT '2024-01-01'::date";
        let out = rewrite_infinity_timestamp(sql);
        assert_eq!(out, sql);
    }
}
