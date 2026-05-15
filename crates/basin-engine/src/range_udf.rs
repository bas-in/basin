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
// Operator rewriter
// ---------------------------------------------------------------------------

/// Range-type keywords that indicate a range constructor call site. Used by
/// the rewriter to distinguish range `@>` / `<@` / `&&` from the JSONB
/// operators of the same name.
const RANGE_CTOR_KEYWORDS: &[&str] = &[
    "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
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
        ("@>", "__range_at_gt"),   // placeholder, see below
        ("<@", "__range_lt_at"),   // placeholder, see below
        ("&&", "range_overlaps"),
        ("<<", "range_strictly_left"),
        (">>", "range_strictly_right"),
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

        // For @> / <@ decide if this is range or leave alone.
        let actual_func = match func {
            "__range_at_gt" => {
                if looks_like_range(lhs) || looks_like_range(rhs) {
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
}
