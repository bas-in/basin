//! DataFusion scalar UDFs for PostgreSQL range type constructors and operators.
//!
//! Range values are stored as UTF-8 JSON strings with the schema:
//! `{"l":<lower>,"u":<upper>,"li":<bool>,"ui":<bool>}`
//! where `li`/`ui` indicate inclusive lower/upper bounds respectively.
//! A null `l` or `u` field means the bound is infinite.
//!
//! Functions registered here:
//! - Constructors: `int4range`, `int8range`, `numrange`, `daterange`,
//!   `tsrange`, `tstzrange`
//! - Accessors: `lower`, `upper`, `isempty`, `lower_inc`, `upper_inc`,
//!   `lower_inf`, `upper_inf`
//! - Predicates: `range_contains_elem`, `range_contains_range`, `range_overlaps`,
//!   `range_strictly_left`, `range_strictly_right`, `range_adjacent`
//! - Set ops: `range_merge` (union of two ranges)
//! - Operator rewriter: [`rewrite_range_operators`] converts PG infix operators
//!   (`@>`, `<@`, `&&`, `<<`, `>>`, `-|-`) to the matching UDF calls.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

fn make_range_ctor_sig(arg_type: DataType) -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![arg_type.clone(), arg_type.clone()]),
            TypeSignature::Exact(vec![arg_type.clone(), arg_type, DataType::Utf8]),
        ],
        Volatility::Immutable,
    )
}

/// Register all range UDFs on `ctx`. Idempotent.
pub(crate) fn register_range_udfs(ctx: &SessionContext) {
    // Constructors — two-argument forms with default bounds `[lower, upper)`.
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "int4range",
        sig: make_range_ctor_sig(DataType::Int64),
    }));
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "int8range",
        sig: make_range_ctor_sig(DataType::Int64),
    }));
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "numrange",
        sig: make_range_ctor_sig(DataType::Float64),
    }));
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "daterange",
        sig: make_range_ctor_sig(DataType::Utf8),
    }));
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "tsrange",
        sig: make_range_ctor_sig(DataType::Utf8),
    }));
    ctx.register_udf(ScalarUDF::from(RangeConstructorUdf {
        name: "tstzrange",
        sig: make_range_ctor_sig(DataType::Utf8),
    }));

    // Multirange constructors — variadic, accept one or more range JSON strings.
    let multirange_sig = Signature::variadic_any(Volatility::Immutable);
    ctx.register_udf(ScalarUDF::from(MultirangeConstructorUdf {
        name: "int4multirange",
        sig: multirange_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(MultirangeConstructorUdf {
        name: "int8multirange",
        sig: multirange_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(MultirangeConstructorUdf {
        name: "nummultirange",
        sig: multirange_sig,
    }));

    let utf8_sig = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

    // Accessors.
    ctx.register_udf(ScalarUDF::from(RangeAccessorUdf {
        field: RangeField::Lower,
        sig: utf8_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RangeAccessorUdf {
        field: RangeField::Upper,
        sig: utf8_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(IsEmptyUdf { sig: utf8_sig.clone() }));
    ctx.register_udf(ScalarUDF::from(BoundFlagUdf {
        name: "lower_inc",
        flag: BoundFlag::LowerInc,
        sig: utf8_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(BoundFlagUdf {
        name: "upper_inc",
        flag: BoundFlag::UpperInc,
        sig: utf8_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(BoundFlagUdf {
        name: "lower_inf",
        flag: BoundFlag::LowerInf,
        sig: utf8_sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(BoundFlagUdf {
        name: "upper_inf",
        flag: BoundFlag::UpperInf,
        sig: utf8_sig,
    }));

    // Predicates.
    ctx.register_udf(ScalarUDF::from(RangeContainsElemUdf {
        sig: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(RangeContainsRangeUdf {
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(RangeOverlapsUdf {
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(RangeRelationalUdf {
        name: "range_strictly_left",
        kind: RangeRelKind::StrictlyLeft,
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(RangeRelationalUdf {
        name: "range_strictly_right",
        kind: RangeRelKind::StrictlyRight,
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(RangeRelationalUdf {
        name: "range_adjacent",
        kind: RangeRelKind::Adjacent,
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    // Set operations.
    ctx.register_udf(ScalarUDF::from(RangeMergeUdf {
        sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));

    // Arithmetic operators (pre-parse rewriter maps +/*/- to these).
    let two_utf8 = Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable);
    ctx.register_udf(ScalarUDF::from(RangeUnionUdf { sig: two_utf8.clone() }));
    ctx.register_udf(ScalarUDF::from(RangeIntersectionUdf { sig: two_utf8.clone() }));
    ctx.register_udf(ScalarUDF::from(RangeDiffUdf { sig: two_utf8 }));

    // Multirange @> scalar containment.
    ctx.register_udf(ScalarUDF::from(MultirangeContainsUdf {
        sig: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
            ],
            Volatility::Immutable,
        ),
    }));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a range JSON string and return a `serde_json::Value`.
fn parse_range(s: &str) -> Option<serde_json::Value> {
    serde_json::from_str(s).ok()
}

/// Format a range value as the canonical JSON string used for storage.
fn format_range(lower: Option<&str>, upper: Option<&str>, li: bool, ui: bool) -> String {
    let l_val = lower
        .map(|v| {
            // Try numeric parse for cleaner output; fall back to string.
            if let Ok(n) = v.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = v.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(v.to_string()))
            } else {
                serde_json::Value::String(v.to_string())
            }
        })
        .unwrap_or(serde_json::Value::Null);
    let u_val = upper
        .map(|v| {
            if let Ok(n) = v.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = v.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(v.to_string()))
            } else {
                serde_json::Value::String(v.to_string())
            }
        })
        .unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "l": l_val,
        "u": u_val,
        "li": li,
        "ui": ui,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Range constructor UDF
// ---------------------------------------------------------------------------

/// Generic two-argument range constructor.  The `arg_type` controls what
/// DataFusion signature we advertise; at evaluation time we just stringify
/// each bound and encode them into JSON. Default bounds are `[lower, upper)`.
#[derive(PartialEq, Eq, Hash)]
struct RangeConstructorUdf {
    name: &'static str,
    sig: Signature,
}

impl std::fmt::Debug for RangeConstructorUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeConstructorUdf({})", self.name)
    }
}

impl ScalarUDFImpl for RangeConstructorUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        if args.len() < 2 {
            return exec_err!("{}: expected at least 2 arguments", self.name);
        }
        let bounds_text = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(sv) => {
                    sv.to_string().trim_matches('\'').to_string()
                }
                ColumnarValue::Array(arr) => {
                    let sarr = arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                            "range: bounds arg must be Utf8".into(),
                        ))?;
                    // Use first row's value (they should all be the same literal).
                    sarr.value(0).to_string()
                }
            }
        } else {
            "[)".to_string()
        };
        let (li, ui) = match bounds_text.as_str() {
            "[]" => (true, true),
            "(]" => (false, true),
            "()" => (false, false),
            _ => (true, false), // default "[)"
        };

        let lower_strings = columnar_to_strings(&args[0], batch_size)?;
        let upper_strings = columnar_to_strings(&args[1], batch_size)?;
        let mut out = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let lo = lower_strings.get(i).and_then(|s| s.as_deref());
            let hi = upper_strings.get(i).and_then(|s| s.as_deref());
            out.push(Some(format_range(lo, hi, li, ui)));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

/// Convert a `ColumnarValue` column to a `Vec<Option<String>>` of length
/// `batch_size`. Scalars are broadcast.
fn columnar_to_strings(cv: &ColumnarValue, n: usize) -> DFResult<Vec<Option<String>>> {
    match cv {
        ColumnarValue::Scalar(sv) => {
            let s = match sv {
                datafusion::scalar::ScalarValue::Utf8(Some(v)) => Some(v.clone()),
                datafusion::scalar::ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
                datafusion::scalar::ScalarValue::Int8(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Int16(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Int32(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Float32(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Float64(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Decimal128(Some(v), p, s) => {
                    let _ = (p, s);
                    Some(v.to_string())
                }
                _ => None,
            };
            Ok(vec![s; n])
        }
        ColumnarValue::Array(arr) => {
            // Try StringArray first, then numeric downcasts.
            if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
                return Ok((0..n)
                    .map(|i| if sa.is_null(i) { None } else { Some(sa.value(i).to_string()) })
                    .collect());
            }
            if let Some(ia) = arr.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                return Ok((0..n)
                    .map(|i| if ia.is_null(i) { None } else { Some(ia.value(i).to_string()) })
                    .collect());
            }
            if let Some(fa) = arr.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>() {
                return Ok((0..n)
                    .map(|i| if fa.is_null(i) { None } else { Some(fa.value(i).to_string()) })
                    .collect());
            }
            exec_err!("range constructor: unsupported bound array type {:?}", arr.data_type())
        }
    }
}

// ---------------------------------------------------------------------------
// Accessor: lower / upper
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RangeField {
    Lower,
    Upper,
}

#[derive(PartialEq, Eq, Hash)]
struct RangeAccessorUdf {
    field: RangeField,
    sig: Signature,
}

impl std::fmt::Debug for RangeAccessorUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeAccessorUdf({:?})", self.field)
    }
}

impl ScalarUDFImpl for RangeAccessorUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        match self.field {
            RangeField::Lower => "lower",
            RangeField::Upper => "upper",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let field_key = match self.field {
            RangeField::Lower => "l",
            RangeField::Upper => "u",
        };
        let range_strs = columnar_to_strings(&args[0], batch_size)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(batch_size);
        for rs in &range_strs {
            let val = rs.as_deref().and_then(|s| {
                // If the value starts with `{` and parses as our range JSON,
                // extract the bound. Otherwise fall back to string case
                // conversion (matching DataFusion's built-in lower/upper
                // semantics so we don't break `lower('Hello')` call sites).
                if s.trim_start().starts_with('{') {
                    if let Some(v) = parse_range(s) {
                        let fv = v.get(field_key)?;
                        if fv.is_null() {
                            return None;
                        }
                        return Some(fv.to_string().trim_matches('"').to_string());
                    }
                }
                // Fallback: string case conversion.
                Some(match self.field {
                    RangeField::Lower => s.to_lowercase(),
                    RangeField::Upper => s.to_uppercase(),
                })
            });
            out.push(val);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// isempty
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct IsEmptyUdf {
    sig: Signature,
}

impl std::fmt::Debug for IsEmptyUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsEmptyUdf")
    }
}

impl ScalarUDFImpl for IsEmptyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isempty"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let range_strs = columnar_to_strings(&args[0], batch_size)?;
        let mut out = Vec::with_capacity(batch_size);
        for rs in &range_strs {
            let empty = rs.as_deref().map(|s| {
                parse_range(s)
                    .map(|v| {
                        let l = v.get("l").cloned().unwrap_or(serde_json::Value::Null);
                        let u = v.get("u").cloned().unwrap_or(serde_json::Value::Null);
                        // Empty when l >= u (both numeric) or either is null (infinite).
                        match (&l, &u) {
                            (serde_json::Value::Number(ln), serde_json::Value::Number(un)) => {
                                ln.as_f64().unwrap_or(0.0) >= un.as_f64().unwrap_or(0.0)
                            }
                            _ => false,
                        }
                    })
                    .unwrap_or(false)
            });
            out.push(empty);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// Bound flag accessors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BoundFlag {
    LowerInc,
    UpperInc,
    LowerInf,
    UpperInf,
}

#[derive(PartialEq, Eq, Hash)]
struct BoundFlagUdf {
    name: &'static str,
    flag: BoundFlag,
    sig: Signature,
}

impl std::fmt::Debug for BoundFlagUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoundFlagUdf({})", self.name)
    }
}

impl ScalarUDFImpl for BoundFlagUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let range_strs = columnar_to_strings(&args[0], batch_size)?;
        let flag = self.flag;
        let mut out = Vec::with_capacity(batch_size);
        for rs in &range_strs {
            let result = rs.as_deref().and_then(|s| {
                let v = parse_range(s)?;
                match flag {
                    BoundFlag::LowerInc => v.get("li").and_then(|b| b.as_bool()),
                    BoundFlag::UpperInc => v.get("ui").and_then(|b| b.as_bool()),
                    BoundFlag::LowerInf => {
                        // Infinite if "l" is null.
                        Some(v.get("l").map(|b| b.is_null()).unwrap_or(true))
                    }
                    BoundFlag::UpperInf => {
                        Some(v.get("u").map(|b| b.is_null()).unwrap_or(true))
                    }
                }
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// range_contains_elem — element-in-range check
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeContainsElemUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeContainsElemUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeContainsElemUdf")
    }
}

impl ScalarUDFImpl for RangeContainsElemUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range_contains_elem"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let range_strs = columnar_to_strings(&args[0], batch_size)?;
        let elem_strs = columnar_to_strings(&args[1], batch_size)?;
        let mut out = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let result = range_strs[i].as_deref().and_then(|rs| {
                let elem_s = elem_strs[i].as_deref()?;
                let elem: f64 = elem_s.parse().ok()?;
                let v = parse_range(rs)?;
                let li = v.get("li").and_then(|b| b.as_bool()).unwrap_or(true);
                let ui = v.get("ui").and_then(|b| b.as_bool()).unwrap_or(false);
                let lower_ok = match v.get("l") {
                    Some(lv) if !lv.is_null() => {
                        let lo: f64 = lv.as_f64().or_else(|| lv.as_str().and_then(|s| s.parse().ok()))?;
                        if li { elem >= lo } else { elem > lo }
                    }
                    _ => true, // -infinity
                };
                let upper_ok = match v.get("u") {
                    Some(uv) if !uv.is_null() => {
                        let hi: f64 = uv.as_f64().or_else(|| uv.as_str().and_then(|s| s.parse().ok()))?;
                        if ui { elem <= hi } else { elem < hi }
                    }
                    _ => true, // +infinity
                };
                Some(lower_ok && upper_ok)
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// range_overlaps — two ranges overlap check
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeOverlapsUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeOverlapsUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeOverlapsUdf")
    }
}

impl ScalarUDFImpl for RangeOverlapsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range_overlaps"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let a_strs = columnar_to_strings(&args[0], batch_size)?;
        let b_strs = columnar_to_strings(&args[1], batch_size)?;
        let mut out = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let result = a_strs[i].as_deref().and_then(|as_| {
                let bs = b_strs[i].as_deref()?;
                let a = parse_range(as_)?;
                let b = parse_range(bs)?;
                // Two ranges [a1, a2] and [b1, b2] overlap iff a1 < b2 && b1 < a2
                // (with bound inclusivity factored in). Simpler: not (a2 <= b1 || b2 <= a1).
                let a_lo = range_bound_f64(&a, "l");
                let a_hi = range_bound_f64(&a, "u");
                let b_lo = range_bound_f64(&b, "l");
                let b_hi = range_bound_f64(&b, "u");
                let a_li = a.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let a_ui = a.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                let b_li = b.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let b_ui = b.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                // a ends before b starts?
                let a_ends_before_b = match (a_hi, b_lo) {
                    (Some(ah), Some(bl)) => {
                        if a_ui && b_li { ah < bl } else { ah <= bl }
                    }
                    _ => false,
                };
                // b ends before a starts?
                let b_ends_before_a = match (b_hi, a_lo) {
                    (Some(bh), Some(al)) => {
                        if b_ui && a_li { bh < al } else { bh <= al }
                    }
                    _ => false,
                };
                Some(!(a_ends_before_b || b_ends_before_a))
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

fn range_bound_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    let fv = v.get(key)?;
    if fv.is_null() {
        return None;
    }
    fv.as_f64()
        .or_else(|| fv.as_str().and_then(|s| s.parse().ok()))
}

// ---------------------------------------------------------------------------
// range_contains_range — range @> range (containment)
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeContainsRangeUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeContainsRangeUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeContainsRangeUdf")
    }
}

impl ScalarUDFImpl for RangeContainsRangeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range_contains_range"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let outer_strs = columnar_to_strings(&args[0], batch_size)?;
        let inner_strs = columnar_to_strings(&args[1], batch_size)?;
        let mut out = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let result = outer_strs[i].as_deref().and_then(|os| {
                let is = inner_strs[i].as_deref()?;
                let outer = parse_range(os)?;
                let inner = parse_range(is)?;
                // outer @> inner iff outer.lo <= inner.lo && inner.hi <= outer.hi
                // with bound inclusivity respected.
                let o_lo = range_bound_f64(&outer, "l");
                let o_hi = range_bound_f64(&outer, "u");
                let i_lo = range_bound_f64(&inner, "l");
                let i_hi = range_bound_f64(&inner, "u");
                let o_li = outer.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let o_ui = outer.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                let i_li = inner.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let i_ui = inner.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                // Check lower: o_lo <= i_lo
                let lower_ok = match (o_lo, i_lo) {
                    (None, _) => true,  // outer is -inf
                    (Some(_), None) => false, // outer bounded, inner -inf
                    (Some(ol), Some(il)) => {
                        if ol < il { true }
                        else if ol == il { o_li || !i_li }
                        else { false }
                    }
                };
                // Check upper: i_hi <= o_hi
                let upper_ok = match (i_hi, o_hi) {
                    (None, _) => false, // inner is +inf, outer can't contain unless also +inf
                    (Some(_), None) => true, // outer is +inf
                    (Some(ih), Some(oh)) => {
                        if ih < oh { true }
                        else if ih == oh { o_ui || !i_ui }
                        else { false }
                    }
                };
                // Handle inner +inf case
                let upper_ok = if inner.get("u").map(|v| v.is_null()).unwrap_or(false) {
                    outer.get("u").map(|v| v.is_null()).unwrap_or(false)
                } else {
                    upper_ok
                };
                Some(lower_ok && upper_ok)
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// Relational operators: strictly_left (<<), strictly_right (>>), adjacent (-|-)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RangeRelKind {
    StrictlyLeft,
    StrictlyRight,
    Adjacent,
}

#[derive(PartialEq, Eq, Hash)]
struct RangeRelationalUdf {
    name: &'static str,
    kind: RangeRelKind,
    sig: Signature,
}

impl std::fmt::Debug for RangeRelationalUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeRelationalUdf({})", self.name)
    }
}

impl ScalarUDFImpl for RangeRelationalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let a_strs = columnar_to_strings(&args[0], batch_size)?;
        let b_strs = columnar_to_strings(&args[1], batch_size)?;
        let kind = self.kind;
        let mut out = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let result = a_strs[i].as_deref().and_then(|as_| {
                let bs = b_strs[i].as_deref()?;
                let a = parse_range(as_)?;
                let b = parse_range(bs)?;
                let a_hi = range_bound_f64(&a, "u");
                let a_lo = range_bound_f64(&a, "l");
                let b_hi = range_bound_f64(&b, "u");
                let b_lo = range_bound_f64(&b, "l");
                let a_ui = a.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                let a_li = a.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let b_ui = b.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                let b_li = b.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                match kind {
                    RangeRelKind::StrictlyLeft => {
                        // A << B: A's upper bound is <= B's lower bound (exclusive)
                        match (a_hi, b_lo) {
                            (Some(ah), Some(bl)) => {
                                if a_ui && b_li { Some(ah < bl) }
                                else { Some(ah <= bl) }
                            }
                            _ => Some(false),
                        }
                    }
                    RangeRelKind::StrictlyRight => {
                        // A >> B: B's upper bound <= A's lower bound
                        match (b_hi, a_lo) {
                            (Some(bh), Some(al)) => {
                                if b_ui && a_li { Some(bh < al) }
                                else { Some(bh <= al) }
                            }
                            _ => Some(false),
                        }
                    }
                    RangeRelKind::Adjacent => {
                        // A -|- B: A's upper bound == B's lower bound (one incl, one excl)
                        // or B's upper == A's lower similarly.
                        let adj_a_b = match (a_hi, b_lo) {
                            (Some(ah), Some(bl)) => {
                                (ah - bl).abs() < 1e-12
                                    && (a_ui != b_li) // exactly one inclusive
                            }
                            _ => false,
                        };
                        let adj_b_a = match (b_hi, a_lo) {
                            (Some(bh), Some(al)) => {
                                (bh - al).abs() < 1e-12
                                    && (b_ui != a_li)
                            }
                            _ => false,
                        };
                        Some(adj_a_b || adj_b_a)
                    }
                }
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// range_merge — bounding range of two ranges (union hull)
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeMergeUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeMergeUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeMergeUdf")
    }
}

impl ScalarUDFImpl for RangeMergeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range_merge"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let batch_size = args.number_rows;
        let _ = batch_size;
        let args = &args.args;
        let a_strs = columnar_to_strings(&args[0], batch_size)?;
        let b_strs = columnar_to_strings(&args[1], batch_size)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let result = a_strs[i].as_deref().and_then(|as_| {
                let bs = b_strs[i].as_deref()?;
                let a = parse_range(as_)?;
                let b = parse_range(bs)?;
                let a_lo = range_bound_f64(&a, "l");
                let a_hi = range_bound_f64(&a, "u");
                let b_lo = range_bound_f64(&b, "l");
                let b_hi = range_bound_f64(&b, "u");
                let a_li = a.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let a_ui = a.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                let b_li = b.get("li").and_then(|v| v.as_bool()).unwrap_or(true);
                let b_ui = b.get("ui").and_then(|v| v.as_bool()).unwrap_or(false);
                // Lower: min of the two lower bounds.
                let (new_lo, new_li) = match (a_lo, b_lo) {
                    (None, _) => (None, false),
                    (_, None) => (None, false),
                    (Some(al), Some(bl)) => {
                        if al < bl { (Some(al), a_li) }
                        else if al > bl { (Some(bl), b_li) }
                        else { (Some(al), a_li || b_li) }
                    }
                };
                // Upper: max of the two upper bounds.
                let (new_hi, new_ui) = match (a_hi, b_hi) {
                    (None, _) => (None, false),
                    (_, None) => (None, false),
                    (Some(ah), Some(bh)) => {
                        if ah > bh { (Some(ah), a_ui) }
                        else if ah < bh { (Some(bh), b_ui) }
                        else { (Some(ah), a_ui || b_ui) }
                    }
                };
                let lo_str = new_lo.map(|v| v.to_string());
                let hi_str = new_hi.map(|v| v.to_string());
                Some(format_range(
                    lo_str.as_deref(),
                    hi_str.as_deref(),
                    new_li,
                    new_ui,
                ))
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// Range arithmetic: union (+), intersection (*), difference (-)
// ---------------------------------------------------------------------------

/// Internal parse result: extracted range bounds as f64 with inclusivity flags.
struct RangeParts {
    lo: Option<f64>,
    hi: Option<f64>,
    li: bool,
    ui: bool,
    empty: bool,
}

fn parse_range_parts(s: &str) -> Option<RangeParts> {
    // Special "empty" sentinel (stored as `{"empty":true}` or the raw string "empty").
    if s.trim() == "empty" {
        return Some(RangeParts { lo: None, hi: None, li: false, ui: false, empty: true });
    }
    let v = parse_range(s)?;
    if v.get("empty").and_then(|e| e.as_bool()).unwrap_or(false) {
        return Some(RangeParts { lo: None, hi: None, li: false, ui: false, empty: true });
    }
    let lo = range_bound_f64(&v, "l");
    let hi = range_bound_f64(&v, "u");
    let li = v.get("li").and_then(|b| b.as_bool()).unwrap_or(true);
    let ui = v.get("ui").and_then(|b| b.as_bool()).unwrap_or(false);
    Some(RangeParts { lo, hi, li, ui, empty: false })
}

fn format_range_parts(p: &RangeParts) -> String {
    if p.empty {
        return r#"{"empty":true}"#.to_string();
    }
    let lo_str = p.lo.map(|v| {
        // Emit as integer if the value is a whole number, otherwise float.
        if v.fract() == 0.0 && v.abs() < 1e15 {
            format!("{}", v as i64)
        } else {
            v.to_string()
        }
    });
    let hi_str = p.hi.map(|v| {
        if v.fract() == 0.0 && v.abs() < 1e15 {
            format!("{}", v as i64)
        } else {
            v.to_string()
        }
    });
    format_range(lo_str.as_deref(), hi_str.as_deref(), p.li, p.ui)
}

/// True if two ranges are overlapping or adjacent (i.e. their union is contiguous).
/// "Adjacent" means: one's upper bound == the other's lower bound, and together
/// the inclusivity covers that point (exactly one of the two bounds is inclusive).
fn ranges_contiguous(a: &RangeParts, b: &RangeParts) -> bool {
    if a.empty || b.empty {
        return true; // empty + anything is always fine
    }
    // Do they overlap?
    let a_ends_before_b = match (a.hi, b.lo) {
        (Some(ah), Some(bl)) => {
            if a.ui && b.li { ah < bl } else { ah <= bl }
        }
        _ => false,
    };
    let b_ends_before_a = match (b.hi, a.lo) {
        (Some(bh), Some(al)) => {
            if b.ui && a.li { bh < al } else { bh <= al }
        }
        _ => false,
    };
    if !a_ends_before_b && !b_ends_before_a {
        return true; // overlapping
    }
    // Adjacent: a_hi == b_lo (exactly one inclusive) or b_hi == a_lo.
    let adj_a_b = match (a.hi, b.lo) {
        (Some(ah), Some(bl)) => {
            (ah - bl).abs() < 1e-12 && (a.ui != b.li)
        }
        _ => false,
    };
    let adj_b_a = match (b.hi, a.lo) {
        (Some(bh), Some(al)) => {
            (bh - al).abs() < 1e-12 && (b.ui != a.li)
        }
        _ => false,
    };
    adj_a_b || adj_b_a
}

fn range_union_impl(a: &RangeParts, b: &RangeParts) -> DFResult<RangeParts> {
    if a.empty { return Ok(RangeParts { lo: b.lo, hi: b.hi, li: b.li, ui: b.ui, empty: b.empty }); }
    if b.empty { return Ok(RangeParts { lo: a.lo, hi: a.hi, li: a.li, ui: a.ui, empty: a.empty }); }
    if !ranges_contiguous(a, b) {
        return exec_err!("result of range union would not be contiguous");
    }
    // Lower: min
    let (new_lo, new_li) = match (a.lo, b.lo) {
        (None, _) | (_, None) => (None, false),
        (Some(al), Some(bl)) => {
            if al < bl { (Some(al), a.li) }
            else if al > bl { (Some(bl), b.li) }
            else { (Some(al), a.li || b.li) }
        }
    };
    // Upper: max
    let (new_hi, new_ui) = match (a.hi, b.hi) {
        (None, _) | (_, None) => (None, false),
        (Some(ah), Some(bh)) => {
            if ah > bh { (Some(ah), a.ui) }
            else if ah < bh { (Some(bh), b.ui) }
            else { (Some(ah), a.ui || b.ui) }
        }
    };
    Ok(RangeParts { lo: new_lo, hi: new_hi, li: new_li, ui: new_ui, empty: false })
}

fn range_intersection_impl(a: &RangeParts, b: &RangeParts) -> RangeParts {
    if a.empty || b.empty {
        return RangeParts { lo: None, hi: None, li: false, ui: false, empty: true };
    }
    // Lower: max of the two lower bounds.
    let (new_lo, new_li) = match (a.lo, b.lo) {
        (None, blo) => (blo, b.li),
        (alo, None) => (alo, a.li),
        (Some(al), Some(bl)) => {
            if al > bl { (Some(al), a.li) }
            else if al < bl { (Some(bl), b.li) }
            else { (Some(al), a.li && b.li) }
        }
    };
    // Upper: min of the two upper bounds.
    let (new_hi, new_ui) = match (a.hi, b.hi) {
        (None, bhi) => (bhi, b.ui),
        (ahi, None) => (ahi, a.ui),
        (Some(ah), Some(bh)) => {
            if ah < bh { (Some(ah), a.ui) }
            else if ah > bh { (Some(bh), b.ui) }
            else { (Some(ah), a.ui && b.ui) }
        }
    };
    // Check if intersection is empty.
    let is_empty = match (new_lo, new_hi) {
        (Some(lo), Some(hi)) => {
            lo > hi || (lo == hi && (!new_li || !new_ui))
        }
        _ => false,
    };
    if is_empty {
        RangeParts { lo: None, hi: None, li: false, ui: false, empty: true }
    } else {
        RangeParts { lo: new_lo, hi: new_hi, li: new_li, ui: new_ui, empty: false }
    }
}

fn range_diff_impl(a: &RangeParts, b: &RangeParts) -> DFResult<RangeParts> {
    if a.empty || b.empty {
        return Ok(RangeParts { lo: a.lo, hi: a.hi, li: a.li, ui: a.ui, empty: a.empty });
    }
    // Compute intersection to determine the overlap.
    let inter = range_intersection_impl(a, b);
    if inter.empty {
        // No overlap — return a unchanged.
        return Ok(RangeParts { lo: a.lo, hi: a.hi, li: a.li, ui: a.ui, empty: a.empty });
    }
    // b fully covers a → result is empty.
    // Check: inter == a (b contains a).
    let b_covers_a = {
        // Lower: inter.lo == a.lo (same or a is -inf when both are None).
        let lo_match = match (inter.lo, a.lo) {
            (None, None) => true,
            (Some(il), Some(al)) => (il - al).abs() < 1e-12 && inter.li == a.li,
            _ => false,
        };
        let hi_match = match (inter.hi, a.hi) {
            (None, None) => true,
            (Some(ih), Some(ah)) => (ih - ah).abs() < 1e-12 && inter.ui == a.ui,
            _ => false,
        };
        lo_match && hi_match
    };
    if b_covers_a {
        return Ok(RangeParts { lo: None, hi: None, li: false, ui: false, empty: true });
    }
    // If b splits a in the middle → contiguity error.
    // b is "in the middle" if both a.lo < inter.lo (a has stuff left of b) AND
    // inter.hi < a.hi (a has stuff right of b).
    let left_remains = match (a.lo, inter.lo) {
        (None, _) => true,      // a has -inf lower
        (_, None) => false,
        (Some(al), Some(il)) => al < il || (al == il && a.li && !inter.li),
    };
    let right_remains = match (a.hi, inter.hi) {
        (None, _) => true,      // a has +inf upper
        (_, None) => false,
        (Some(ah), Some(ih)) => ah > ih || (ah == ih && a.ui && !inter.ui),
    };
    if left_remains && right_remains {
        return exec_err!("result of range difference would not be contiguous");
    }
    // b cuts one end off: return the remaining piece.
    if left_remains {
        // b cuts the right end — result is [a.lo, b.lo)
        let (new_hi, new_ui) = match b.lo {
            None => (None, false),
            Some(bl) => (Some(bl), !b.li), // complement b's lower inclusivity
        };
        Ok(RangeParts { lo: a.lo, hi: new_hi, li: a.li, ui: new_ui, empty: false })
    } else {
        // b cuts the left end — result is (b.hi, a.hi]
        let (new_lo, new_li) = match b.hi {
            None => (None, false),
            Some(bh) => (Some(bh), !b.ui),
        };
        Ok(RangeParts { lo: new_lo, hi: a.hi, li: new_li, ui: a.ui, empty: false })
    }
}

// ---------------------------------------------------------------------------
// RangeUnionUdf — range_union(a, b)  maps to SQL `a + b`
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeUnionUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeUnionUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeUnionUdf")
    }
}

impl ScalarUDFImpl for RangeUnionUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "range_union" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let a_strs = columnar_to_strings(&args.args[0], n)?;
        let b_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = (|| -> DFResult<Option<String>> {
                let as_ = match a_strs[i].as_deref() { Some(s) => s, None => return Ok(None) };
                let bs = match b_strs[i].as_deref() { Some(s) => s, None => return Ok(None) };
                let a = parse_range_parts(as_).ok_or_else(|| datafusion::error::DataFusionError::Execution("range_union: invalid range A".into()))?;
                let b = parse_range_parts(bs).ok_or_else(|| datafusion::error::DataFusionError::Execution("range_union: invalid range B".into()))?;
                let res = range_union_impl(&a, &b)?;
                Ok(Some(format_range_parts(&res)))
            })()?;
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// RangeIntersectionUdf — range_intersection(a, b)  maps to SQL `a * b`
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeIntersectionUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeIntersectionUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeIntersectionUdf")
    }
}

impl ScalarUDFImpl for RangeIntersectionUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "range_intersection" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let a_strs = columnar_to_strings(&args.args[0], n)?;
        let b_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = a_strs[i].as_deref().and_then(|as_| {
                let bs = b_strs[i].as_deref()?;
                let a = parse_range_parts(as_)?;
                let b = parse_range_parts(bs)?;
                Some(format_range_parts(&range_intersection_impl(&a, &b)))
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// RangeDiffUdf — range_diff(a, b)  maps to SQL `a - b`
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct RangeDiffUdf {
    sig: Signature,
}

impl std::fmt::Debug for RangeDiffUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeDiffUdf")
    }
}

impl ScalarUDFImpl for RangeDiffUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "range_diff" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let a_strs = columnar_to_strings(&args.args[0], n)?;
        let b_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = (|| -> DFResult<Option<String>> {
                let as_ = match a_strs[i].as_deref() { Some(s) => s, None => return Ok(None) };
                let bs = match b_strs[i].as_deref() { Some(s) => s, None => return Ok(None) };
                let a = parse_range_parts(as_).ok_or_else(|| datafusion::error::DataFusionError::Execution("range_diff: invalid range A".into()))?;
                let b = parse_range_parts(bs).ok_or_else(|| datafusion::error::DataFusionError::Execution("range_diff: invalid range B".into()))?;
                let res = range_diff_impl(&a, &b)?;
                Ok(Some(format_range_parts(&res)))
            })()?;
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// MultirangeConstructorUdf — int4multirange(range...) → JSON array of ranges
// ---------------------------------------------------------------------------

/// A multirange is stored as a JSON array of range JSON objects, e.g.
/// `[{"l":1,"u":5,"li":true,"ui":false},{"l":10,"u":20,"li":true,"ui":false}]`
/// The ranges are kept in sorted, non-overlapping order (adjacent ranges are
/// NOT merged — PG does merge them on construction, but for our purposes
/// the containment check works correctly with sorted non-overlapping ranges).
#[derive(PartialEq, Eq, Hash)]
struct MultirangeConstructorUdf {
    name: &'static str,
    sig: Signature,
}

impl std::fmt::Debug for MultirangeConstructorUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultirangeConstructorUdf({})", self.name)
    }
}

impl ScalarUDFImpl for MultirangeConstructorUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        // Collect each argument (range JSON string) into a Vec.
        let mut arg_string_cols: Vec<Vec<Option<String>>> = Vec::with_capacity(args.args.len());
        for cv in &args.args {
            arg_string_cols.push(columnar_to_strings(cv, n)?);
        }
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for row in 0..n {
            let mut ranges: Vec<serde_json::Value> = Vec::new();
            let mut any_null = false;
            for col in &arg_string_cols {
                match col[row].as_deref() {
                    None => { any_null = true; break; }
                    Some(s) => {
                        match parse_range(s) {
                            Some(v) => ranges.push(v),
                            None => { any_null = true; break; }
                        }
                    }
                }
            }
            if any_null {
                out.push(None);
            } else {
                // Sort ranges by lower bound (nulls = -inf first).
                ranges.sort_by(|a, b| {
                    let al = range_bound_f64(a, "l");
                    let bl = range_bound_f64(b, "l");
                    match (al, bl) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (None, _) => std::cmp::Ordering::Less,
                        (_, None) => std::cmp::Ordering::Greater,
                        (Some(av), Some(bv)) => av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal),
                    }
                });
                out.push(Some(serde_json::Value::Array(ranges).to_string()));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// MultirangeContainsUdf — multirange_contains_elem(mrange, elem)
// Maps to SQL `int4multirange(...) @> scalar`
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct MultirangeContainsUdf {
    sig: Signature,
}

impl std::fmt::Debug for MultirangeContainsUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultirangeContainsUdf")
    }
}

impl ScalarUDFImpl for MultirangeContainsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "multirange_contains_elem" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let mrange_strs = columnar_to_strings(&args.args[0], n)?;
        let elem_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = mrange_strs[i].as_deref().and_then(|ms| {
                let es = elem_strs[i].as_deref()?;
                let elem: f64 = es.parse().ok()?;
                // Parse as JSON array of range objects.
                let arr: Vec<serde_json::Value> = serde_json::from_str(ms).ok()?;
                for range_val in &arr {
                    let li = range_val.get("li").and_then(|b| b.as_bool()).unwrap_or(true);
                    let ui = range_val.get("ui").and_then(|b| b.as_bool()).unwrap_or(false);
                    let lower_ok = match range_val.get("l") {
                        Some(lv) if !lv.is_null() => {
                            let lo: f64 = lv.as_f64()
                                .or_else(|| lv.as_str().and_then(|s| s.parse().ok()))?;
                            if li { elem >= lo } else { elem > lo }
                        }
                        _ => true,
                    };
                    let upper_ok = match range_val.get("u") {
                        Some(uv) if !uv.is_null() => {
                            let hi: f64 = uv.as_f64()
                                .or_else(|| uv.as_str().and_then(|s| s.parse().ok()))?;
                            if ui { elem <= hi } else { elem < hi }
                        }
                        _ => true,
                    };
                    if lower_ok && upper_ok {
                        return Some(true);
                    }
                }
                Some(false)
            });
            out.push(result);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out)) as ArrayRef))
    }
}

// ---------------------------------------------------------------------------
// Operator rewriter
// ---------------------------------------------------------------------------

/// Range-type keywords that indicate a range constructor call site. Used by
/// the rewriter to distinguish range `@>` / `<@` / `&&` from the JSONB
/// operators of the same name.
const RANGE_CTOR_KEYWORDS: &[&str] = &[
    "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
];

/// Multirange constructor keywords.
const MULTIRANGE_CTOR_KEYWORDS: &[&str] = &[
    "int4multirange", "int8multirange", "nummultirange",
];

/// Rewrite range type cast suffixes like `'[1,10)'::int4range` to just
/// `'[1,10)'`. Basin stores range values as plain Utf8 strings; the cast
/// target is not a type DataFusion understands so we strip it.
pub(crate) fn rewrite_range_casts(sql: &str) -> String {
    let mut s = sql.to_string();
    // Case-insensitive replacement of `::int4range`, `::daterange`, etc.
    // (with or without whitespace between `::` and the type name).
    for kw in RANGE_CTOR_KEYWORDS {
        let pattern = format!("::{}", kw);
        // Case-insensitive search.
        let lower = s.to_lowercase();
        let mut positions: Vec<usize> = Vec::new();
        let mut search_start = 0;
        while let Some(pos) = lower[search_start..].find(pattern.as_str()) {
            positions.push(search_start + pos);
            search_start += pos + pattern.len();
        }
        for pos in positions.into_iter().rev() {
            s.replace_range(pos..pos + pattern.len(), "");
        }
    }
    s
}

/// Rewrite PostgreSQL range infix operators to UDF calls before handing SQL
/// to sqlparser / DataFusion, which don't understand these operators.
///
/// Operators translated:
/// - `A @> B`  → `range_contains_elem(A, B)` or `range_contains_range(A, B)`
/// - `A <@ B`  → `range_contains_elem(B, A)` or `range_contains_range(B, A)`
/// - `A && B`  → `range_overlaps(A, B)`
/// - `A << B`  → `range_strictly_left(A, B)`
/// - `A >> B`  → `range_strictly_right(A, B)`
/// - `A -|- B` → `range_adjacent(A, B)`
///
/// **JSONB / clash note**: `@>` and `&&` are also JSONB operators. This
/// rewriter only triggers when at least one operand textually starts with a
/// known range constructor keyword (`int4range`, `numrange`, etc.). Plain
/// column-reference or string-literal operands are left untouched here and
/// must be dispatched via a future JSONB rewriter.
pub(crate) fn rewrite_range_operators(sql: &str) -> String {
    let ops: &[(&str, &str)] = &[
        ("-|-", "range_adjacent"),
        ("@>", "__range_at_gt"),         // placeholder, see below
        ("<@", "__range_lt_at"),          // placeholder, see below
        ("&&", "range_overlaps"),
        ("<<", "range_strictly_left"),
        (">>", "range_strictly_right"),
        // Arithmetic operators: only rewrite when both operands look like
        // range constructors (heuristic), so we don't rewrite plain numeric
        // `a + b`, `a * b`, `a - b` expressions.
        ("+", "__range_plus"),            // placeholder
        ("*", "__range_star"),            // placeholder
        ("-", "__range_minus"),           // placeholder
    ];
    let mut s = sql.to_string();
    for &(op, func) in ops {
        s = rewrite_range_op_once(&s, op, func);
    }
    s
}

/// Rewrite all occurrences of `op` between two operands to `func(lhs, rhs)`.
/// For `@>` and `<@` (the placeholder names) we pick the right UDF after
/// inspecting whether either operand looks like a range constructor.
fn rewrite_range_op_once(sql: &str, op: &str, func: &str) -> String {
    let mut s = sql.to_string();
    loop {
        let Some(op_start) = find_op_outside_strings(&s, op) else {
            break;
        };
        let op_end = op_start + op.len();
        let (lhs_start, lhs_end) = range_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = range_extract_right(&s, op_end);
        let lhs = &s[lhs_start..lhs_end];
        let rhs = &s[rhs_start..rhs_end];

        // For @> / <@ / +/* /- decide if this is range or leave alone.
        let actual_func = match func {
            "__range_at_gt" => {
                if looks_like_multirange(lhs) {
                    // multirange @> scalar → multirange_contains_elem
                    "multirange_contains_elem"
                } else if looks_like_range(lhs) || looks_like_range(rhs) {
                    // If rhs also looks like a range → range_contains_range;
                    // otherwise → range_contains_elem.
                    if looks_like_range(rhs) {
                        "range_contains_range"
                    } else {
                        "range_contains_elem"
                    }
                } else {
                    // Not range — leave this occurrence alone (skip past it).
                    // We can't just break; there might be more. Advance past
                    // this instance by replacing with a sentinel-free version.
                    // Simplest: leave the op intact, start search after op_end.
                    // Rebuild without infinite loop: temporarily skip.
                    let prefix = &s[..op_end];
                    let suffix = &s[op_end..];
                    s = format!("{prefix}{suffix}");
                    // Prevent infinite loop by replacing the op with an
                    // identical-length stand-in that won't match.
                    // Actually we just need to move past it; since we found at
                    // op_start and the string didn't change, next `find` would
                    // find the same position. We handle this by replacing the
                    // op character sequence with a U+FFFE placeholder, then
                    // restoring at the end. Simpler: handle JSONB skip via a
                    // different strategy: mark processed positions.
                    // For now, if neither operand looks like a range, we stop
                    // rewriting @> entirely (they're all JSONB or unknown).
                    break;
                }
            }
            "__range_lt_at" => {
                if looks_like_range(lhs) || looks_like_range(rhs) {
                    // Swap lhs / rhs for the "contained by" direction.
                    let call = if looks_like_range(lhs) {
                        format!("range_contains_range({rhs}, {lhs})")
                    } else {
                        format!("range_contains_elem({rhs}, {lhs})")
                    };
                    s.replace_range(lhs_start..rhs_end, &call);
                    continue;
                } else {
                    break;
                }
            }
            "__range_plus" => {
                // Only rewrite if BOTH operands look like range constructors.
                if looks_like_range(lhs) && looks_like_range(rhs) {
                    "range_union"
                } else {
                    break; // not a range expression, leave untouched
                }
            }
            "__range_star" => {
                if looks_like_range(lhs) && looks_like_range(rhs) {
                    "range_intersection"
                } else {
                    break;
                }
            }
            "__range_minus" => {
                if looks_like_range(lhs) && looks_like_range(rhs) {
                    "range_diff"
                } else {
                    break;
                }
            }
            other => other,
        };

        // `<@` was handled above via `continue`; normal path for everything else.
        if func == "__range_lt_at" {
            unreachable!("handled above");
        }
        let call = format!("{actual_func}({lhs}, {rhs})");
        s.replace_range(lhs_start..rhs_end, &call);
    }
    s
}

/// Return `true` if `expr` (trimmed) textually starts with a known range
/// constructor name. Heuristic — good enough for the common case.
fn looks_like_range(expr: &str) -> bool {
    let trimmed = expr.trim().to_ascii_lowercase();
    RANGE_CTOR_KEYWORDS.iter().any(|kw| trimmed.starts_with(kw))
}

/// Return `true` if `expr` looks like a multirange constructor call.
fn looks_like_multirange(expr: &str) -> bool {
    let trimmed = expr.trim().to_ascii_lowercase();
    MULTIRANGE_CTOR_KEYWORDS.iter().any(|kw| trimmed.starts_with(kw))
}

/// Find the first occurrence of `op` that is not inside a single-quoted
/// string literal. Returns the byte offset of the first character of `op`.
fn find_op_outside_strings(s: &str, op: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Skip past the quoted string.
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Check for escaped quote `''`.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if s[i..].starts_with(op) {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Walk back from `end` (the start of the operator) and extract the left
/// operand. Returns `(start, end)` byte offsets in `s`.
fn range_extract_left(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    // Skip whitespace.
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    if i == 0 {
        return (0, operand_end);
    }
    let last = bytes[i - 1];
    if last == b')' {
        let mut depth = 1i32;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            match bytes[i] {
                b')' => depth += 1,
                b'(' => depth -= 1,
                _ => {}
            }
        }
        // Capture the function name before the `(`.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else {
        // Identifier / number run.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    }
    (i, operand_end)
}

/// Walk right from `start` (the byte after the end of the operator) and
/// extract the right operand. Returns `(start, end)` byte offsets.
fn range_extract_right(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    // Skip whitespace.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    if i >= bytes.len() {
        return (operand_start, operand_start);
    }
    if bytes[i] == b'(' {
        let mut depth = 1i32;
        i += 1;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
    } else {
        // Identifier / number / string run.
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric()
                || bytes[i] == b'_'
                || bytes[i] == b'.'
                || bytes[i] == b'\'')
        {
            if bytes[i] == b'\'' {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            i += 1;
        }
        // If the identifier is immediately followed by `(`, it's a function call.
        // Consume the entire argument list so that `int4range(5,15)` is captured
        // as a single operand rather than just the bare name `int4range`.
        if i < bytes.len() && bytes[i] == b'(' {
            let mut depth = 1i32;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
        }
    }
    (operand_start, i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::scalar::ScalarValue;

    /// df53 removed `invoke_batch`; tests reach the UDF through this shim,
    /// which builds the `ScalarFunctionArgs` the new `invoke_with_args` takes.
    trait InvokeBatchCompat {
        fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> DFResult<ColumnarValue>;
    }
    impl<T: ScalarUDFImpl> InvokeBatchCompat for T {
        fn invoke_batch(&self, args: &[ColumnarValue], n: usize) -> DFResult<ColumnarValue> {
            let arg_types: Vec<DataType> = args.iter().map(|a| a.data_type()).collect();
            let return_type = self.return_type(&arg_types)?;
            let arg_fields = args
                .iter()
                .enumerate()
                .map(|(i, a)| Arc::new(Field::new(format!("a{i}"), a.data_type(), true)))
                .collect();
            self.invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields,
                number_rows: n,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        }
    }

    fn make_range(lo: i64, hi: i64) -> ColumnarValue {
        let json = format!(r#"{{"l":{},"u":{},"li":true,"ui":false}}"#, lo, hi);
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(json)))
    }

    fn utf8_sig() -> Signature {
        Signature::exact(vec![DataType::Utf8], Volatility::Immutable)
    }

    #[test]
    fn int4range_constructor_smoke() {
        let udf = RangeConstructorUdf {
            name: "int4range",
            sig: make_range_ctor_sig(DataType::Int64),
        };
        let lo = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let hi = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let result = udf.invoke_batch(&[lo, hi], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
            let v: serde_json::Value = serde_json::from_str(sa.value(0)).unwrap();
            assert_eq!(v["l"], 1);
            assert_eq!(v["u"], 10);
            assert_eq!(v["li"], true);
            assert_eq!(v["ui"], false);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn lower_upper_accessor() {
        let range = make_range(5, 15);
        let lower_udf = RangeAccessorUdf { field: RangeField::Lower, sig: utf8_sig() };
        let upper_udf = RangeAccessorUdf { field: RangeField::Upper, sig: utf8_sig() };
        let lo = lower_udf.invoke_batch(&[range.clone()], 1).unwrap();
        let hi = upper_udf.invoke_batch(&[range], 1).unwrap();
        if let (ColumnarValue::Array(la), ColumnarValue::Array(ua)) = (lo, hi) {
            let ls = la.as_any().downcast_ref::<StringArray>().unwrap();
            let us = ua.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(ls.value(0), "5");
            assert_eq!(us.value(0), "15");
        } else {
            panic!("expected arrays");
        }
    }

    #[test]
    fn isempty_false_for_valid_range() {
        let range = make_range(1, 5);
        let udf = IsEmptyUdf { sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn isempty_true_for_empty_range() {
        let json = r#"{"l":5,"u":5,"li":false,"ui":false}"#;
        let range = ColumnarValue::Scalar(ScalarValue::Utf8(Some(json.to_string())));
        let udf = IsEmptyUdf { sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0)); // 5 >= 5 → empty
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn lower_inc_flag() {
        let range = make_range(1, 10); // li=true
        let udf = BoundFlagUdf { name: "lower_inc", flag: BoundFlag::LowerInc, sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn upper_inc_flag() {
        let range = make_range(1, 10); // ui=false
        let udf = BoundFlagUdf { name: "upper_inc", flag: BoundFlag::UpperInc, sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn lower_inf_false_for_bounded() {
        let range = make_range(1, 10);
        let udf = BoundFlagUdf { name: "lower_inf", flag: BoundFlag::LowerInf, sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn lower_inf_true_for_unbounded() {
        let json = r#"{"l":null,"u":10,"li":false,"ui":false}"#;
        let range = ColumnarValue::Scalar(ScalarValue::Utf8(Some(json.to_string())));
        let udf = BoundFlagUdf { name: "lower_inf", flag: BoundFlag::LowerInf, sig: utf8_sig() };
        let result = udf.invoke_batch(&[range], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    fn make_contains_udf() -> RangeContainsElemUdf {
        RangeContainsElemUdf {
            sig: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    fn make_overlaps_udf() -> RangeOverlapsUdf {
        RangeOverlapsUdf {
            sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }

    #[test]
    fn range_contains_elem_in_range() {
        let range = make_range(1, 10); // [1, 10)
        let elem = ColumnarValue::Scalar(ScalarValue::Int64(Some(5)));
        let udf = make_contains_udf();
        let result = udf.invoke_batch(&[range, elem], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn range_contains_elem_out_of_range() {
        let range = make_range(1, 10); // [1, 10)
        let elem = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let udf = make_contains_udf();
        let result = udf.invoke_batch(&[range, elem], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0)); // upper-exclusive
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn range_overlaps_overlapping() {
        let a = make_range(1, 10);
        let b = make_range(5, 15);
        let udf = make_overlaps_udf();
        let result = udf.invoke_batch(&[a, b], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn range_overlaps_disjoint() {
        let a = make_range(1, 5);
        let b = make_range(5, 10); // [1,5) && [5,10) — share only bound, upper-exclusive => no overlap
        let udf = make_overlaps_udf();
        let result = udf.invoke_batch(&[a, b], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    // -----------------------------------------------------------------------
    // Range arithmetic: union (+), intersection (*), difference (-)
    // -----------------------------------------------------------------------

    fn make_union_udf() -> RangeUnionUdf {
        RangeUnionUdf {
            sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }

    fn make_intersection_udf() -> RangeIntersectionUdf {
        RangeIntersectionUdf {
            sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }

    fn make_diff_udf() -> RangeDiffUdf {
        RangeDiffUdf {
            sig: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }
    }

    fn extract_string(result: ColumnarValue) -> String {
        if let ColumnarValue::Array(arr) = result {
            let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
            sa.value(0).to_string()
        } else {
            panic!("expected array result");
        }
    }

    fn parse_bounds(json_str: &str) -> (i64, i64, bool, bool) {
        let v: serde_json::Value = serde_json::from_str(json_str).expect("valid JSON");
        let lo = v["l"].as_i64().unwrap();
        let hi = v["u"].as_i64().unwrap();
        let li = v["li"].as_bool().unwrap();
        let ui = v["ui"].as_bool().unwrap();
        (lo, hi, li, ui)
    }

    fn is_empty_json(json_str: &str) -> bool {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
            v.get("empty").and_then(|e| e.as_bool()).unwrap_or(false)
        } else {
            json_str.trim() == "empty"
        }
    }

    /// `int4range(1,5) + int4range(3,8)` → `[1,8)`
    #[test]
    fn range_union_overlapping() {
        let a = make_range(1, 5);
        let b = make_range(3, 8);
        let udf = make_union_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        let (lo, hi, li, ui) = parse_bounds(&result);
        assert_eq!(lo, 1);
        assert_eq!(hi, 8);
        assert!(li);
        assert!(!ui);
    }

    /// `int4range(1,5) + int4range(5,10)` — adjacent `[1,5)` and `[5,10)` → `[1,10)`
    #[test]
    fn range_union_adjacent() {
        let a = make_range(1, 5);
        let b = make_range(5, 10);
        let udf = make_union_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        let (lo, hi, li, ui) = parse_bounds(&result);
        assert_eq!(lo, 1);
        assert_eq!(hi, 10);
        assert!(li);
        assert!(!ui);
    }

    /// `int4range(1,5) + int4range(10,15)` → contiguity error
    #[test]
    fn range_union_disjoint_errors() {
        let a = make_range(1, 5);
        let b = make_range(10, 15);
        let udf = make_union_udf();
        let err = udf.invoke_batch(&[a, b], 1).unwrap_err();
        assert!(err.to_string().contains("contiguous"), "expected contiguity error, got: {err}");
    }

    /// `int4range(1,10) * int4range(5,15)` → `[5,10)`
    #[test]
    fn range_intersection_overlapping() {
        let a = make_range(1, 10);
        let b = make_range(5, 15);
        let udf = make_intersection_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        let (lo, hi, li, ui) = parse_bounds(&result);
        assert_eq!(lo, 5);
        assert_eq!(hi, 10);
        assert!(li);
        assert!(!ui);
    }

    /// `int4range(1,10) * int4range(20,30)` → empty
    #[test]
    fn range_intersection_disjoint_empty() {
        let a = make_range(1, 10);
        let b = make_range(20, 30);
        let udf = make_intersection_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        assert!(is_empty_json(&result), "expected empty range, got: {result}");
    }

    /// `int4range(1,10) - int4range(5,15)` → `[1,5)`
    #[test]
    fn range_diff_tail_cut() {
        let a = make_range(1, 10);
        let b = make_range(5, 15);
        let udf = make_diff_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        let (lo, hi, li, ui) = parse_bounds(&result);
        assert_eq!(lo, 1);
        assert_eq!(hi, 5);
        assert!(li);
        assert!(!ui); // complement of b's li=true → not-inclusive on upper
    }

    /// `int4range(5,15) - int4range(1,10)` → `[10,15)` (head cut)
    #[test]
    fn range_diff_head_cut() {
        let a = make_range(5, 15);
        let b = make_range(1, 10);
        let udf = make_diff_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        let (lo, hi, li, ui) = parse_bounds(&result);
        assert_eq!(lo, 10);
        assert_eq!(hi, 15);
        assert!(li); // complement of b's ui=false → inclusive
        assert!(!ui);
    }

    /// `int4range(1,10) - int4range(3,7)` → contiguity error (middle split)
    #[test]
    fn range_diff_middle_split_errors() {
        let a = make_range(1, 10);
        let b = make_range(3, 7);
        let udf = make_diff_udf();
        let err = udf.invoke_batch(&[a, b], 1).unwrap_err();
        assert!(err.to_string().contains("contiguous"), "expected contiguity error, got: {err}");
    }

    /// `int4range(1,10) - int4range(0,20)` → empty (b fully covers a)
    #[test]
    fn range_diff_fully_covered_empty() {
        let a = make_range(1, 10);
        let b = make_range(0, 20);
        let udf = make_diff_udf();
        let result = extract_string(udf.invoke_batch(&[a, b], 1).unwrap());
        assert!(is_empty_json(&result), "expected empty range, got: {result}");
    }

    // -----------------------------------------------------------------------
    // Multirange containment (@>)
    // -----------------------------------------------------------------------

    fn make_multirange_contains_udf() -> MultirangeContainsUdf {
        MultirangeContainsUdf {
            sig: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    fn make_multirange_json(ranges: &[(i64, i64)]) -> ColumnarValue {
        let arr: Vec<serde_json::Value> = ranges
            .iter()
            .map(|(lo, hi)| serde_json::json!({ "l": lo, "u": hi, "li": true, "ui": false }))
            .collect();
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            serde_json::Value::Array(arr).to_string(),
        )))
    }

    /// `int4multirange(int4range(1,5), int4range(10,20)) @> 3` → true
    #[test]
    fn multirange_contains_elem_true() {
        let mrange = make_multirange_json(&[(1, 5), (10, 20)]);
        let elem = ColumnarValue::Scalar(ScalarValue::Int64(Some(3)));
        let udf = make_multirange_contains_udf();
        let result = udf.invoke_batch(&[mrange, elem], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    /// `int4multirange(int4range(1,5), int4range(10,20)) @> 7` → false
    #[test]
    fn multirange_contains_elem_false_gap() {
        let mrange = make_multirange_json(&[(1, 5), (10, 20)]);
        let elem = ColumnarValue::Scalar(ScalarValue::Int64(Some(7)));
        let udf = make_multirange_contains_udf();
        let result = udf.invoke_batch(&[mrange, elem], 1).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(!ba.value(0));
        } else {
            panic!("expected array");
        }
    }

    // -----------------------------------------------------------------------
    // Operator rewriter — arithmetic rewrites
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_range_plus_operator() {
        let sql = "SELECT int4range(1,5) + int4range(3,8)";
        let rewritten = rewrite_range_operators(sql);
        assert!(
            rewritten.contains("range_union("),
            "expected range_union rewrite, got: {rewritten}"
        );
        assert!(!rewritten.contains(" + "), "original + should be gone: {rewritten}");
    }

    #[test]
    fn rewrite_range_star_operator() {
        let sql = "SELECT int4range(1,10) * int4range(5,15)";
        let rewritten = rewrite_range_operators(sql);
        assert!(
            rewritten.contains("range_intersection("),
            "expected range_intersection rewrite, got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_range_minus_operator() {
        let sql = "SELECT int4range(1,10) - int4range(5,15)";
        let rewritten = rewrite_range_operators(sql);
        assert!(
            rewritten.contains("range_diff("),
            "expected range_diff rewrite, got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_multirange_at_gt() {
        let sql = "SELECT int4multirange(int4range(1,5)) @> 3";
        let rewritten = rewrite_range_operators(sql);
        assert!(
            rewritten.contains("multirange_contains_elem("),
            "expected multirange_contains_elem rewrite, got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_does_not_touch_plain_addition() {
        // Regular numeric addition should NOT be rewritten.
        let sql = "SELECT 1 + 2";
        let rewritten = rewrite_range_operators(sql);
        assert_eq!(rewritten, sql, "plain numeric + should be unchanged");
    }
}
