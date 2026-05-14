//! JSONB scalar UDFs for Basin's pg-compat surface.
//!
//! Basin stores JSONB columns as Arrow `LargeBinary` with `BASIN_TYPE=JSONB`
//! metadata. The on-disk payload is canonical JSON bytes (serde_json canonical
//! form — keys sorted alphabetically, no extra whitespace). There is **no**
//! leading version byte; the wire format differs from Postgres's internal
//! binary format.
//!
//! Every UDF here:
//! 1. Accepts one or more `LargeBinary` (JSONB) and/or `Utf8` arguments.
//! 2. Deserialises to `serde_json::Value` via the helpers at the bottom.
//! 3. Operates on the Value.
//! 4. Re-serialises to canonical JSON bytes and returns `LargeBinary`.
//!
//! Set-returning functions (SRFs) — `jsonb_object_keys`, `jsonb_each`,
//! `jsonb_array_elements`, etc. — cannot be true SRFs inside DataFusion's
//! scalar UDF framework; they are implemented as best-effort stubs that
//! return a single scalar (the first key / element / the whole JSON text
//! representation). A comment on each stub notes the limitation.
//!
//! Aggregate UDFs (`jsonb_agg`, `jsonb_object_agg`) require DataFusion's
//! `AggregateUDFImpl` trait and accumulator machinery; they are registered
//! as scalar stubs that return an error message rather than silently mis-
//! evaluating, surfacing the gap clearly in the SQL support matrix.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all JSONB UDFs on `ctx`. Idempotent (DataFusion overwrites by
/// name). Call after `register_pg_compat_udfs` so there are no ordering
/// dependencies.
pub(crate) fn register_jsonb_udfs(ctx: &SessionContext) {
    // jsonb_typeof(jsonb) -> text
    ctx.register_udf(ScalarUDF::from(JsonbTypeofUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_pretty(jsonb) -> text
    ctx.register_udf(ScalarUDF::from(JsonbPrettyUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_array_length(jsonb) -> int8
    ctx.register_udf(ScalarUDF::from(JsonbArrayLengthUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_strip_nulls(jsonb) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbStripNullsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_set(target jsonb, path text[], new_value jsonb [, create_missing bool]) -> jsonb
    // Use any(3)/any(4) so string-literal args (Utf8) are accepted alongside
    // stored JSONB columns (LargeBinary). We do runtime type dispatch inside.
    ctx.register_udf(ScalarUDF::from(JsonbSetUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(3),
                TypeSignature::Any(4),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_insert(target, path, new_value [, insert_after bool]) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbInsertUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(3),
                TypeSignature::Any(4),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_query(target, jsonpath) -> jsonb (best-effort: first match)
    ctx.register_udf(ScalarUDF::from(JsonbPathQueryUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_exists(target, jsonpath) -> bool
    ctx.register_udf(ScalarUDF::from(JsonbPathExistsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_match(target, jsonpath) -> bool  (alias of exists for simple paths)
    ctx.register_udf(ScalarUDF::from(JsonbPathMatchUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_object_keys(jsonb) -> text (SRF stub: returns comma-joined keys)
    ctx.register_udf(ScalarUDF::from(JsonbObjectKeysUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_each(jsonb) -> text (SRF stub: returns JSON representation of record)
    ctx.register_udf(ScalarUDF::from(JsonbEachUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_each_text(jsonb) -> text (SRF stub)
    ctx.register_udf(ScalarUDF::from(JsonbEachTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_array_elements(jsonb) -> jsonb (SRF stub: first element)
    ctx.register_udf(ScalarUDF::from(JsonbArrayElementsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_array_elements_text(jsonb) -> text (SRF stub: first element as text)
    ctx.register_udf(ScalarUDF::from(JsonbArrayElementsTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_build_object(variadic any) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbBuildObjectUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));

    // jsonb_build_array(variadic any) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbBuildArrayUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));

    // to_jsonb(any) -> jsonb
    ctx.register_udf(ScalarUDF::from(ToJsonbUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));

    // row_to_json(record) -> jsonb (stub: returns text representation)
    ctx.register_udf(ScalarUDF::from(RowToJsonUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));

    // array_to_json(array [, pretty bool]) -> jsonb
    ctx.register_udf(ScalarUDF::from(ArrayToJsonUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(1),
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_agg(any) -> jsonb  [aggregate stub]
    ctx.register_udf(ScalarUDF::from(JsonbAggStubUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));

    // jsonb_object_agg(key, value) -> jsonb  [aggregate stub]
    ctx.register_udf(ScalarUDF::from(JsonbObjectAggStubUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}

// ---------------------------------------------------------------------------
// Helpers: JSONB bytes ↔ serde_json::Value
// ---------------------------------------------------------------------------

/// Decode JSONB bytes to a serde_json Value.
/// Basin stores JSONB as canonical JSON bytes (no version prefix).
fn jsonb_to_value(bytes: &[u8]) -> DFResult<Value> {
    // Tolerate an optional leading 0x01 version byte (Postgres wire format)
    // in case data came through the pgwire layer.
    let payload = if bytes.first() == Some(&0x01) && bytes.len() > 1 {
        &bytes[1..]
    } else {
        bytes
    };
    serde_json::from_slice(payload).map_err(|e| {
        DataFusionError::Execution(format!("jsonb decode error: {e}"))
    })
}

/// Encode a serde_json Value to canonical JSONB bytes.
fn value_to_jsonb(v: &Value) -> DFResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| {
        DataFusionError::Execution(format!("jsonb encode error: {e}"))
    })
}

/// Extract a `serde_json::Value` from a ColumnarValue row, handling both
/// `LargeBinary` (stored JSONB) and `Utf8` (JSON string literal) inputs.
fn extract_jsonb_value(arr: &ArrayRef, i: usize, fn_name: &str) -> DFResult<Option<Value>> {
    match arr.data_type() {
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("{fn_name}: not a LargeBinaryArray"))
            })?;
            if a.is_null(i) {
                return Ok(None);
            }
            Ok(Some(jsonb_to_value(a.value(i))?))
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("{fn_name}: not a StringArray"))
            })?;
            if a.is_null(i) {
                return Ok(None);
            }
            serde_json::from_str(a.value(i))
                .map(Some)
                .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json parse: {e}")))
        }
        other => exec_err!("{fn_name}: expected LargeBinary or Utf8, got {other:?}"),
    }
}

/// Utility: get row count from a slice of ColumnarValues.
fn row_count(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

/// Parse a path string like `'{a,b,c}'` or `'a.b.c'` into path segments.
fn parse_path(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    // Handle Postgres array literal syntax: '{key1,key2}'
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inner = &trimmed[1..trimmed.len() - 1];
        return inner
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    // Dot-separated path
    trimmed
        .split('.')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Navigate to a nested Value by path segments, returning a mutable reference
/// to the parent and the last key.  Returns `None` when the path doesn't exist
/// and `create_missing` is false.
fn navigate_mut<'a>(
    root: &'a mut Value,
    path: &[String],
    create_missing: bool,
) -> Option<(&'a mut Value, String)> {
    if path.is_empty() {
        return None;
    }
    let (last, parents) = path.split_last()?;
    let mut cur = root;
    for key in parents {
        // Try numeric index: if cur is an array and the key is a valid index,
        // descend into it. We must do the check and descent in one arm to
        // avoid the borrow-checker conflict between the numeric-index fast path
        // and the fallthrough match below.
        let is_numeric_idx = key.parse::<usize>().ok();
        match (cur, is_numeric_idx) {
            (Value::Array(arr), Some(idx)) if idx < arr.len() => {
                cur = &mut arr[idx];
            }
            (Value::Object(map), _) => {
                if !map.contains_key(key.as_str()) {
                    if !create_missing {
                        return None;
                    }
                    map.insert(key.clone(), Value::Object(Default::default()));
                }
                cur = map.get_mut(key.as_str())?;
            }
            _ => return None,
        }
    }
    Some((cur, last.clone()))
}

/// Recursively strip null-valued keys from objects.
fn strip_nulls(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let filtered = map
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_nulls(v)))
                .collect();
            Value::Object(filtered)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}


fn array_element_to_json(arr: &ArrayRef, i: usize) -> Value {
    if arr.is_null(i) {
        return Value::Null;
    }
    match arr.data_type() {
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>();
            if let Some(a) = a {
                jsonb_to_value(a.value(i)).unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>();
            if let Some(a) = a {
                let s = a.value(i);
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
            } else {
                Value::Null
            }
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>();
            a.map(|a| Value::Bool(a.value(i))).unwrap_or(Value::Null)
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>();
            a.map(|a| Value::Number(a.value(i).into())).unwrap_or(Value::Null)
        }
        _ => {
            // Fallback: use Debug representation as a string
            Value::String(format!("{arr:?}[{i}]"))
        }
    }
}

// ---------------------------------------------------------------------------
// jsonb_typeof
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbTypeofUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbTypeofUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_typeof" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_typeof expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_typeof")? {
                None => out.push(None),
                Some(v) => {
                    let t = match &v {
                        Value::Object(_) => "object",
                        Value::Array(_) => "array",
                        Value::String(_) => "string",
                        Value::Number(_) => "number",
                        Value::Bool(_) => "boolean",
                        Value::Null => "null",
                    };
                    out.push(Some(t.to_string()));
                }
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_pretty
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbPrettyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPrettyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_pretty" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_pretty expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_pretty")? {
                None => out.push(None),
                Some(v) => {
                    let pretty = serde_json::to_string_pretty(&v).map_err(|e| {
                        DataFusionError::Execution(format!("jsonb_pretty: {e}"))
                    })?;
                    out.push(Some(pretty));
                }
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_array_length
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbArrayLengthUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayLengthUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_length" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Int64) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_array_length expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_array_length")? {
                None => out.push(None),
                Some(Value::Array(a)) => out.push(Some(a.len() as i64)),
                Some(_) => out.push(Some(0)),
            }
        }
        let result = Int64Array::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_strip_nulls
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbStripNullsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbStripNullsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_strip_nulls" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_strip_nulls expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_strip_nulls")? {
                None => out.push(None),
                Some(v) => {
                    let stripped = strip_nulls(v);
                    out.push(Some(value_to_jsonb(&stripped)?));
                }
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_set
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbSetUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbSetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_set" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() < 3 || args.len() > 4 {
            return exec_err!("jsonb_set expects 3 or 4 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let new_val_arr = args[2].clone().into_array(n)?;

        // 4th arg: create_missing (default true)
        let create_arr = if args.len() == 4 {
            Some(args[3].clone().into_array(n)?)
        } else {
            None
        };

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let target = extract_jsonb_value(&target_arr, i, "jsonb_set")?;
            let new_val = extract_jsonb_value(&new_val_arr, i, "jsonb_set")?;

            if target.is_none() || new_val.is_none() {
                out.push(None);
                continue;
            }
            let mut root = target.unwrap();
            let new_val = new_val.unwrap();

            // Extract path string
            let path_str = {
                let path_a = path_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("jsonb_set: path must be Utf8".into())
                })?;
                if path_a.is_null(i) {
                    out.push(None);
                    continue;
                }
                path_a.value(i).to_string()
            };

            let create_missing = if let Some(ref ca) = create_arr {
                let ba = ca.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    DataFusionError::Execution("jsonb_set: create_missing must be Boolean".into())
                })?;
                if ba.is_null(i) { true } else { ba.value(i) }
            } else {
                true
            };

            let path = parse_path(&path_str);
            if path.is_empty() {
                // Empty path — replace root
                out.push(Some(value_to_jsonb(&new_val)?));
                continue;
            }

            if let Some((parent, last_key)) = navigate_mut(&mut root, &path, create_missing) {
                if let Ok(idx) = last_key.parse::<usize>() {
                    if let Value::Array(arr) = parent {
                        if idx < arr.len() {
                            arr[idx] = new_val;
                        } else if create_missing {
                            arr.push(new_val);
                        }
                    }
                } else if let Value::Object(map) = parent {
                    map.insert(last_key, new_val);
                }
            }

            out.push(Some(value_to_jsonb(&root)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_insert
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbInsertUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbInsertUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_insert" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() < 3 || args.len() > 4 {
            return exec_err!("jsonb_insert expects 3 or 4 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let new_val_arr = args[2].clone().into_array(n)?;

        let insert_after_arr = if args.len() == 4 {
            Some(args[3].clone().into_array(n)?)
        } else {
            None
        };

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let target = extract_jsonb_value(&target_arr, i, "jsonb_insert")?;
            let new_val = extract_jsonb_value(&new_val_arr, i, "jsonb_insert")?;

            if target.is_none() || new_val.is_none() {
                out.push(None);
                continue;
            }
            let mut root = target.unwrap();
            let new_val = new_val.unwrap();

            let path_str = {
                let path_a = path_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("jsonb_insert: path must be Utf8".into())
                })?;
                if path_a.is_null(i) {
                    out.push(None);
                    continue;
                }
                path_a.value(i).to_string()
            };

            let insert_after = if let Some(ref ia) = insert_after_arr {
                let ba = ia.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    DataFusionError::Execution("jsonb_insert: insert_after must be Boolean".into())
                })?;
                if ba.is_null(i) { false } else { ba.value(i) }
            } else {
                false
            };

            let path = parse_path(&path_str);
            if path.is_empty() {
                out.push(Some(value_to_jsonb(&root)?));
                continue;
            }

            if let Some((parent, last_key)) = navigate_mut(&mut root, &path, true) {
                if let Ok(idx) = last_key.parse::<usize>() {
                    if let Value::Array(arr) = parent {
                        let insert_pos = if insert_after {
                            (idx + 1).min(arr.len())
                        } else {
                            idx.min(arr.len())
                        };
                        arr.insert(insert_pos, new_val);
                    }
                } else if let Value::Object(map) = parent {
                    // For object keys: insert_after ignored; just set
                    map.insert(last_key, new_val);
                }
            }

            out.push(Some(value_to_jsonb(&root)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_query  (best-effort: simple dotted-path navigation)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbPathQueryUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_query" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("jsonb_path_query expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let path_a = path_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("jsonb_path_query: path must be Utf8".into())
        })?;

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            if path_a.is_null(i) {
                out.push(None);
                continue;
            }
            let root = extract_jsonb_value(&target_arr, i, "jsonb_path_query")?;
            if root.is_none() {
                out.push(None);
                continue;
            }
            let root = root.unwrap();
            let path_str = path_a.value(i);
            // Strip leading '$.' or '$' for simple paths
            let stripped = path_str.trim_start_matches('$').trim_start_matches('.');
            let segments = parse_path(stripped);
            let mut cur = &root;
            let mut found = true;
            for seg in &segments {
                if seg.is_empty() {
                    continue;
                }
                match cur {
                    Value::Object(map) => {
                        if let Some(v) = map.get(seg.as_str()) {
                            cur = v;
                        } else {
                            found = false;
                            break;
                        }
                    }
                    Value::Array(arr) => {
                        if let Ok(idx) = seg.parse::<usize>() {
                            if idx < arr.len() {
                                cur = &arr[idx];
                            } else {
                                found = false;
                                break;
                            }
                        } else {
                            found = false;
                            break;
                        }
                    }
                    _ => {
                        found = false;
                        break;
                    }
                }
            }
            if found {
                out.push(Some(value_to_jsonb(cur)?));
            } else {
                out.push(None);
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_exists
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbPathExistsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathExistsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_exists" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("jsonb_path_exists expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let path_a = path_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("jsonb_path_exists: path must be Utf8".into())
        })?;

        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            if path_a.is_null(i) {
                out.push(None);
                continue;
            }
            let root = extract_jsonb_value(&target_arr, i, "jsonb_path_exists")?;
            if root.is_none() {
                out.push(None);
                continue;
            }
            let root = root.unwrap();
            let path_str = path_a.value(i);
            let stripped = path_str.trim_start_matches('$').trim_start_matches('.');
            let segments = parse_path(stripped);
            let mut cur = &root;
            let mut found = true;
            for seg in &segments {
                if seg.is_empty() { continue; }
                match cur {
                    Value::Object(map) => {
                        if let Some(v) = map.get(seg.as_str()) {
                            cur = v;
                        } else { found = false; break; }
                    }
                    Value::Array(arr) => {
                        if let Ok(idx) = seg.parse::<usize>() {
                            if idx < arr.len() { cur = &arr[idx]; }
                            else { found = false; break; }
                        } else { found = false; break; }
                    }
                    _ => { found = false; break; }
                }
            }
            out.push(Some(found));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_match  (alias of jsonb_path_exists for simple paths)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbPathMatchUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathMatchUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_match" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        // Delegate to the same logic as jsonb_path_exists
        JsonbPathExistsUdf {
            signature: self.signature.clone(),
        }.invoke(args)
    }
}

// ---------------------------------------------------------------------------
// jsonb_object_keys  (SRF stub: returns comma-joined keys as Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbObjectKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbObjectKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_object_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_object_keys expects 1 argument, got {}", args.len());
        }
        // SRF stub: return comma-joined key names as a single Utf8 value.
        // True SRF behaviour requires UNNEST/table-function plumbing not yet
        // available in DataFusion's scalar UDF framework.
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_object_keys")? {
                None => out.push(None),
                Some(Value::Object(map)) => {
                    let keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
                    out.push(Some(keys.join(",")));
                }
                Some(_) => out.push(Some(String::new())),
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_each  (SRF stub: returns JSON text of record pairs)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbEachUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbEachUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_each" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        // SRF stub: returns the full JSON string (callers that need real SRF
        // behaviour will hit a "not a table function" error upstream anyway).
        if args.len() != 1 {
            return exec_err!("jsonb_each expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_each")? {
                None => out.push(None),
                Some(v) => {
                    out.push(Some(serde_json::to_string(&v).unwrap_or_default()));
                }
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_each_text  (SRF stub)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbEachTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbEachTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_each_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_each_text expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_each_text")? {
                None => out.push(None),
                Some(v) => {
                    // Return entries as "key=value,..." text pairs (stub)
                    if let Value::Object(map) = v {
                        let pairs: Vec<String> = map
                            .iter()
                            .map(|(k, v)| {
                                let vstr = match v {
                                    Value::String(s) => s.clone(),
                                    other => serde_json::to_string(other).unwrap_or_default(),
                                };
                                format!("{k}={vstr}")
                            })
                            .collect();
                        out.push(Some(pairs.join(",")));
                    } else {
                        out.push(Some(serde_json::to_string(&v).unwrap_or_default()));
                    }
                }
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_array_elements  (SRF stub: returns first element as LargeBinary)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbArrayElementsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayElementsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_elements" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_array_elements expects 1 argument, got {}", args.len());
        }
        // SRF stub: returns first element (or the whole value if not an array)
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_array_elements")? {
                None => out.push(None),
                Some(Value::Array(a)) => {
                    if a.is_empty() {
                        out.push(None);
                    } else {
                        out.push(Some(value_to_jsonb(&a[0])?));
                    }
                }
                Some(v) => out.push(Some(value_to_jsonb(&v)?)),
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_array_elements_text  (SRF stub: first element as Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbArrayElementsTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayElementsTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_elements_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("jsonb_array_elements_text expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "jsonb_array_elements_text")? {
                None => out.push(None),
                Some(Value::Array(a)) => {
                    if a.is_empty() {
                        out.push(None);
                    } else {
                        let s = match &a[0] {
                            Value::String(s) => s.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        };
                        out.push(Some(s));
                    }
                }
                Some(Value::String(s)) => out.push(Some(s)),
                Some(v) => out.push(Some(serde_json::to_string(&v).unwrap_or_default())),
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_build_object(variadic any) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbBuildObjectUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbBuildObjectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_build_object" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() % 2 != 0 {
            return exec_err!(
                "jsonb_build_object requires an even number of arguments (key/value pairs), got {}",
                args.len()
            );
        }
        let n = row_count(args);
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut map = serde_json::Map::new();
            let mut null_row = false;
            for pair in arrays.chunks(2) {
                let key_val = array_element_to_json(&pair[0], i);
                let val_val = array_element_to_json(&pair[1], i);
                let key_str = match &key_val {
                    Value::String(s) => s.clone(),
                    Value::Null => { null_row = true; break; }
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                map.insert(key_str, val_val);
            }
            if null_row {
                out.push(None);
            } else {
                let v = Value::Object(map);
                out.push(Some(value_to_jsonb(&v)?));
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_build_array(variadic any) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbBuildArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbBuildArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_build_array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = row_count(args);
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let items: Vec<Value> = arrays.iter().map(|arr| array_element_to_json(arr, i)).collect();
            let v = Value::Array(items);
            out.push(Some(value_to_jsonb(&v)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// to_jsonb(any) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ToJsonbUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToJsonbUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "to_jsonb" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("to_jsonb expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.push(Some(value_to_jsonb(&Value::Null)?));
                continue;
            }
            let v = array_element_to_json(&arr, i);
            out.push(Some(value_to_jsonb(&v)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// row_to_json(record) -> jsonb  (stub: returns text representation)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RowToJsonUdf {
    signature: Signature,
}

impl ScalarUDFImpl for RowToJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "row_to_json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("row_to_json expects 1 argument, got {}", args.len());
        }
        // Stub: convert any arg to its JSON representation
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let v = array_element_to_json(&arr, i);
            out.push(Some(value_to_jsonb(&v)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// array_to_json(array [, pretty bool]) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ArrayToJsonUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayToJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_to_json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.is_empty() || args.len() > 2 {
            return exec_err!("array_to_json expects 1 or 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.push(None);
                continue;
            }
            // Try to parse as JSONB first, else convert
            let v = match extract_jsonb_value(&arr, i, "array_to_json") {
                Ok(Some(v)) => v,
                _ => array_element_to_json(&arr, i),
            };
            // Wrap in array if not already an array
            let v = match v {
                Value::Array(_) => v,
                other => Value::Array(vec![other]),
            };
            out.push(Some(value_to_jsonb(&v)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_agg(any) -> jsonb  [aggregate stub]
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbAggStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbAggStubUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_agg" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        exec_err!(
            "jsonb_agg requires AggregateUDFImpl; \
             deferred to v0.2 — use jsonb_build_array() for scalar aggregation"
        )
    }
}

// ---------------------------------------------------------------------------
// jsonb_object_agg(key, value) -> jsonb  [aggregate stub]
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonbObjectAggStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbObjectAggStubUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_object_agg" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        exec_err!(
            "jsonb_object_agg requires AggregateUDFImpl; \
             deferred to v0.2 — use jsonb_build_object() for scalar object construction"
        )
    }
}
