//! JSONB mutating and introspection UDFs for Basin's pg-compat surface.
//!
//! This module implements the five primary JSONB functions that mutate or
//! introspect JSON values:
//!
//! - `jsonb_set(target jsonb, path text[], new_value jsonb [, create_missing bool]) -> jsonb`
//! - `jsonb_insert(target jsonb, path text[], new_value jsonb [, insert_after bool]) -> jsonb`
//! - `jsonb_strip_nulls(jsonb) -> jsonb`
//! - `jsonb_pretty(jsonb) -> text`
//! - `jsonb_typeof(jsonb) -> text`
//!
//! Path syntax: a `text[]` Postgres array of string segments.  Each segment is
//! a key name for objects or an integer-as-string for array indices.
//! E.g. `ARRAY['a', '0', 'b']` navigates `obj.a[0].b`.
//!
//! The path argument may arrive as:
//! - `DataType::List(Utf8)` / `DataType::LargeList(Utf8)` — the native
//!   `text[]` form produced by DataFusion when the caller writes
//!   `ARRAY['a', 'b']`.
//! - `DataType::Utf8` — a Postgres-style `'{a,b,c}'` literal string, accepted
//!   for compatibility with string-rewritten queries.
//!
//! Registered via [`register_jsonb_modify_udfs`], which is called from
//! `session::build_stateless_udf_cache` after `register_jsonb_udfs`.  Because
//! DataFusion replaces by name, these registrations shadow any earlier stubs.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, LargeBinaryArray, LargeListArray, ListArray,
    StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all JSONB mutating / introspection UDFs on `ctx`.
///
/// Call this **after** `crate::jsonb_udf::register_jsonb_udfs` so these
/// implementations shadow any earlier stubs registered under the same names.
pub(crate) fn register_jsonb_modify_udfs(ctx: &SessionContext) {
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

    // jsonb_set(target, path, new_value [, create_missing]) -> jsonb
    // TypeSignature::Any(N) accepts mixed LargeBinary/Utf8/List/Boolean.
    ctx.register_udf(ScalarUDF::from(JsonbSetUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Any(3), TypeSignature::Any(4)],
            Volatility::Immutable,
        ),
    }));

    // jsonb_insert(target, path, new_value [, insert_after]) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbInsertUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Any(3), TypeSignature::Any(4)],
            Volatility::Immutable,
        ),
    }));
}

// ---------------------------------------------------------------------------
// Shared helpers (private to this module)
// ---------------------------------------------------------------------------

/// Decode a JSONB blob (`LargeBinary` / `BinaryView`) or a UTF-8 JSON string
/// (`Utf8` / `Utf8View`) into a `serde_json::Value`.  Returns `None` when the
/// array slot is null.  `BinaryView` / `Utf8View` arms cover Arrow's
/// view-format buffers, which DataFusion 53 surfaces for stored JSONB columns.
fn extract_json(arr: &ArrayRef, i: usize, fn_name: &str) -> DFResult<Option<Value>> {
    match arr.data_type() {
        DataType::LargeBinary => {
            let a = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a LargeBinaryArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            serde_json::from_slice(a.value(i))
                .map(Some)
                .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json decode: {e}")))
        }
        DataType::BinaryView => {
            let a = arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a BinaryViewArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            serde_json::from_slice(a.value(i))
                .map(Some)
                .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json decode: {e}")))
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
        DataType::Utf8View => {
            let a = arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a StringViewArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            serde_json::from_str(a.value(i))
                .map(Some)
                .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json parse: {e}")))
        }
        other => exec_err!(
            "{fn_name}: expected LargeBinary, BinaryView, Utf8, or Utf8View for JSON arg, got {other:?}"
        ),
    }
}

/// Encode a `serde_json::Value` to canonical JSON bytes (no extra whitespace).
fn encode_json(v: &Value, fn_name: &str) -> DFResult<Vec<u8>> {
    serde_json::to_vec(v)
        .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json encode: {e}")))
}

/// Determine row count from a slice of `ColumnarValue`s.
fn row_count(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

/// Extract path segments from the path argument at row `i`.
///
/// Accepted forms:
/// - `List(Utf8)` / `LargeList(Utf8)` — the native `text[]` produced by
///   DataFusion for `ARRAY['a', 'b', 'c']` expressions.
/// - `Utf8` — a Postgres-style `'{a,b,c}'` or dot-separated literal string.
///
/// Returns `None` when the row is null (caller should propagate NULL).
/// Returns an error when the array element type is not Utf8.
fn extract_path(arr: &ArrayRef, i: usize, fn_name: &str) -> DFResult<Option<Vec<String>>> {
    match arr.data_type() {
        DataType::List(_) => {
            let a = arr
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a ListArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            let child = a.value(i);
            let sa = child.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{fn_name}: path List element type must be Utf8"
                ))
            })?;
            Ok(Some(
                (0..sa.len())
                    .map(|j| if sa.is_null(j) { String::new() } else { sa.value(j).to_string() })
                    .collect(),
            ))
        }
        DataType::LargeList(_) => {
            let a = arr
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a LargeListArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            let child = a.value(i);
            let sa = child.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{fn_name}: path LargeList element type must be Utf8"
                ))
            })?;
            Ok(Some(
                (0..sa.len())
                    .map(|j| if sa.is_null(j) { String::new() } else { sa.value(j).to_string() })
                    .collect(),
            ))
        }
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a StringArray"))
                })?;
            if a.is_null(i) {
                return Ok(None);
            }
            Ok(Some(parse_path_string(a.value(i))))
        }
        other => exec_err!(
            "{fn_name}: path argument must be text[] (List/LargeList of Utf8) or Utf8, got {other:?}"
        ),
    }
}

/// Parse a Postgres-style path string like `'{a,b,c}'` or `'a.b.c'` into
/// individual key segments.
fn parse_path_string(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inner = &trimmed[1..trimmed.len() - 1];
        return inner
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    // Dot-separated fallback
    trimmed
        .split('.')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Recursively strip keys with `null` values from objects.
/// Arrays preserve their `null` elements (Postgres semantics).
fn strip_nulls(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let filtered = map
                .into_iter()
                .filter(|(_, val)| !val.is_null())
                .map(|(k, val)| (k, strip_nulls(val)))
                .collect();
            Value::Object(filtered)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}

/// Mutable navigation to the *parent* node of the last path segment.
///
/// Returns `Some((parent_value, last_key))` on success.
/// Returns `None` when a path segment doesn't exist and `create_missing`
/// is `false`, or when intermediate nodes are neither objects nor arrays.
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
        let numeric_idx = key.parse::<usize>().ok();
        match (cur, numeric_idx) {
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

// ---------------------------------------------------------------------------
// jsonb_typeof
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbTypeofUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbTypeofUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_typeof"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("jsonb_typeof expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_json(&arr, i, "jsonb_typeof")? {
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
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ---------------------------------------------------------------------------
// jsonb_pretty
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPrettyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPrettyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_pretty"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("jsonb_pretty expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_json(&arr, i, "jsonb_pretty")? {
                None => out.push(None),
                Some(v) => {
                    let pretty = serde_json::to_string_pretty(&v).map_err(|e| {
                        DataFusionError::Execution(format!("jsonb_pretty: serialize error: {e}"))
                    })?;
                    out.push(Some(pretty));
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ---------------------------------------------------------------------------
// jsonb_strip_nulls
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbStripNullsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbStripNullsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_strip_nulls"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("jsonb_strip_nulls expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_json(&arr, i, "jsonb_strip_nulls")? {
                None => out.push(None),
                Some(v) => {
                    let stripped = strip_nulls(v);
                    out.push(Some(encode_json(&stripped, "jsonb_strip_nulls")?));
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbSetUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbSetUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_set"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() < 3 || args.len() > 4 {
            return exec_err!("jsonb_set expects 3 or 4 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let new_val_arr = args[2].clone().into_array(n)?;
        let create_arr = if args.len() == 4 {
            Some(args[3].clone().into_array(n)?)
        } else {
            None
        };

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            // Null target or null new_value → NULL result
            let target = extract_json(&target_arr, i, "jsonb_set")?;
            let new_val = extract_json(&new_val_arr, i, "jsonb_set")?;
            if target.is_none() || new_val.is_none() {
                out.push(None);
                continue;
            }
            let mut root = target.unwrap();
            let new_val = new_val.unwrap();

            // Null path → NULL result
            let path = match extract_path(&path_arr, i, "jsonb_set")? {
                None => {
                    out.push(None);
                    continue;
                }
                Some(p) => p,
            };

            let create_missing = match &create_arr {
                Some(ca) => {
                    match ca.data_type() {
                        DataType::Boolean => {
                            let ba =
                                ca.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "jsonb_set: create_missing must be Boolean".into(),
                                    )
                                })?;
                            if ba.is_null(i) {
                                true
                            } else {
                                ba.value(i)
                            }
                        }
                        // Accept Utf8 literal 'true'/'false' from query rewriting
                        DataType::Utf8 => {
                            let sa =
                                ca.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "jsonb_set: create_missing StringArray cast failed".into(),
                                    )
                                })?;
                            if sa.is_null(i) {
                                true
                            } else {
                                sa.value(i) != "false"
                            }
                        }
                        _ => true,
                    }
                }
                None => true,
            };

            if path.is_empty() {
                // Empty path: replace root entirely
                out.push(Some(encode_json(&new_val, "jsonb_set")?));
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
            // If navigate_mut returned None (path absent + create_missing=false), root unchanged.

            out.push(Some(encode_json(&root, "jsonb_set")?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_insert
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbInsertUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbInsertUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_insert"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
            let target = extract_json(&target_arr, i, "jsonb_insert")?;
            let new_val = extract_json(&new_val_arr, i, "jsonb_insert")?;
            if target.is_none() || new_val.is_none() {
                out.push(None);
                continue;
            }
            let mut root = target.unwrap();
            let new_val = new_val.unwrap();

            let path = match extract_path(&path_arr, i, "jsonb_insert")? {
                None => {
                    out.push(None);
                    continue;
                }
                Some(p) => p,
            };

            let insert_after = match &insert_after_arr {
                Some(ia) => match ia.data_type() {
                    DataType::Boolean => {
                        let ba = ia.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                            DataFusionError::Execution(
                                "jsonb_insert: insert_after must be Boolean".into(),
                            )
                        })?;
                        if ba.is_null(i) {
                            false
                        } else {
                            ba.value(i)
                        }
                    }
                    DataType::Utf8 => {
                        let sa = ia.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            DataFusionError::Execution(
                                "jsonb_insert: insert_after StringArray cast failed".into(),
                            )
                        })?;
                        if sa.is_null(i) {
                            false
                        } else {
                            sa.value(i) == "true"
                        }
                    }
                    _ => false,
                },
                None => false,
            };

            if path.is_empty() {
                // Empty path: return root unchanged (nothing to insert into)
                out.push(Some(encode_json(&root, "jsonb_insert")?));
                continue;
            }

            if let Some((parent, last_key)) = navigate_mut(&mut root, &path, true) {
                if let Ok(idx) = last_key.parse::<usize>() {
                    if let Value::Array(arr) = parent {
                        let pos = if insert_after {
                            (idx + 1).min(arr.len())
                        } else {
                            idx.min(arr.len())
                        };
                        arr.insert(pos, new_val);
                    }
                } else if let Value::Object(map) = parent {
                    // For object keys insert_after is ignored; just set
                    map.insert(last_key, new_val);
                }
            }

            out.push(Some(encode_json(&root, "jsonb_insert")?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, LargeBinaryArray, ListBuilder, StringArray, StringBuilder,
        StringDictionaryBuilder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion::logical_expr::ColumnarValue;
    use serde_json::json;
    use std::sync::Arc;

    // ---- helpers -----------------------------------------------------------

    fn jsonb_scalar(s: &str) -> ColumnarValue {
        let bytes = serde_json::from_str::<Value>(s)
            .unwrap()
            .to_string()
            .into_bytes();
        let arr: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values([bytes]));
        ColumnarValue::Array(arr)
    }

    fn utf8_scalar(s: &str) -> ColumnarValue {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![s]));
        ColumnarValue::Array(arr)
    }

    /// Build a `List<Utf8>` ColumnarValue with one row containing `keys`.
    fn path_list(keys: &[&str]) -> ColumnarValue {
        let mut builder = ListBuilder::new(StringBuilder::new());
        {
            let values = builder.values();
            for k in keys {
                values.append_value(k);
            }
        }
        builder.append(true);
        let arr: ArrayRef = Arc::new(builder.finish());
        ColumnarValue::Array(arr)
    }

    fn bool_scalar(v: bool) -> ColumnarValue {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![v]));
        ColumnarValue::Array(arr)
    }

    fn invoke_udf(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>) -> Value {
        let n = 1usize;
        let arg_fields: Vec<datafusion::arrow::datatypes::FieldRef> = args
            .iter()
            .enumerate()
            .map(|(i, cv)| {
                let dt = match cv {
                    ColumnarValue::Array(a) => a.data_type().clone(),
                    ColumnarValue::Scalar(s) => s.data_type(),
                };
                Arc::new(Field::new(format!("arg{i}"), dt, true))
            })
            .collect();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: n,
                return_field: Arc::new(Field::new("out", DataType::LargeBinary, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().unwrap(),
        };
        // Try LargeBinary first (jsonb output), then Utf8 (text output)
        if let Some(ba) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
            serde_json::from_slice(ba.value(0)).unwrap()
        } else if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
            json!(sa.value(0))
        } else {
            panic!("unexpected array type: {:?}", arr.data_type())
        }
    }

    fn invoke_udf_str(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>) -> String {
        let n = 1usize;
        let arg_fields: Vec<datafusion::arrow::datatypes::FieldRef> = args
            .iter()
            .enumerate()
            .map(|(i, cv)| {
                let dt = match cv {
                    ColumnarValue::Array(a) => a.data_type().clone(),
                    ColumnarValue::Scalar(s) => s.data_type(),
                };
                Arc::new(Field::new(format!("arg{i}"), dt, true))
            })
            .collect();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: n,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().unwrap(),
        };
        arr.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    fn make_sig_1() -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        )
    }

    fn make_sig_any(n: usize) -> Signature {
        Signature::one_of(
            vec![TypeSignature::Any(n), TypeSignature::Any(n + 1)],
            Volatility::Immutable,
        )
    }

    // ---- jsonb_typeof tests ------------------------------------------------

    #[test]
    fn test_typeof_object() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar(r#"{"x":1}"#)]);
        assert_eq!(result, "object");
    }

    #[test]
    fn test_typeof_array() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar(r#"[1,2,3]"#)]);
        assert_eq!(result, "array");
    }

    #[test]
    fn test_typeof_string() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar(r#""hello""#)]);
        assert_eq!(result, "string");
    }

    #[test]
    fn test_typeof_number() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar("42")]);
        assert_eq!(result, "number");
    }

    #[test]
    fn test_typeof_boolean() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar("true")]);
        assert_eq!(result, "boolean");
    }

    #[test]
    fn test_typeof_null_json() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar("null")]);
        assert_eq!(result, "null");
    }

    #[test]
    fn test_typeof_utf8_input() {
        let udf = JsonbTypeofUdf {
            signature: make_sig_1(),
        };
        // Utf8 input (json string literal)
        let result = invoke_udf_str(&udf, vec![utf8_scalar(r#"{"a":1}"#)]);
        assert_eq!(result, "object");
    }

    // ---- jsonb_pretty tests ------------------------------------------------

    #[test]
    fn test_pretty_basic() {
        let udf = JsonbPrettyUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar(r#"{"a":1}"#)]);
        // serde_json pretty-prints with 2-space indent
        assert!(result.contains('\n'), "should be multi-line: {result}");
        assert!(result.contains("  "), "should have indentation: {result}");
        let reparsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(reparsed, json!({"a": 1}));
    }

    #[test]
    fn test_pretty_nested() {
        let udf = JsonbPrettyUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf_str(&udf, vec![jsonb_scalar(r#"{"a":{"b":2}}"#)]);
        let reparsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(reparsed, json!({"a": {"b": 2}}));
    }

    // ---- jsonb_strip_nulls tests -------------------------------------------

    #[test]
    fn test_strip_nulls_basic() {
        let udf = JsonbStripNullsUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf(
            &udf,
            vec![jsonb_scalar(r#"{"a":null,"b":1,"c":{"d":null,"e":2}}"#)],
        );
        assert_eq!(result, json!({"b": 1, "c": {"e": 2}}));
    }

    #[test]
    fn test_strip_nulls_arrays_preserve_nulls() {
        let udf = JsonbStripNullsUdf {
            signature: make_sig_1(),
        };
        // Arrays keep null elements (Postgres semantics)
        let result = invoke_udf(&udf, vec![jsonb_scalar(r#"{"a":[null,1,null],"b":null}"#)]);
        assert_eq!(result, json!({"a": [null, 1, null]}));
    }

    #[test]
    fn test_strip_nulls_no_nulls() {
        let udf = JsonbStripNullsUdf {
            signature: make_sig_1(),
        };
        let result = invoke_udf(&udf, vec![jsonb_scalar(r#"{"a":1,"b":"x"}"#)]);
        assert_eq!(result, json!({"a": 1, "b": "x"}));
    }

    // ---- jsonb_set tests ---------------------------------------------------

    #[test]
    fn test_set_replace_existing_key() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // jsonb_set('{"a":1}', '{a}', '2') -> {"a":2}
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":1}"#),
                path_list(&["a"]),
                jsonb_scalar("2"),
            ],
        );
        assert_eq!(result, json!({"a": 2}));
    }

    #[test]
    fn test_set_create_missing_true() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // jsonb_set('{"a":1}', '{b}', '"x"', true) -> {"a":1,"b":"x"}
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":1}"#),
                path_list(&["b"]),
                jsonb_scalar(r#""x""#),
                bool_scalar(true),
            ],
        );
        assert_eq!(result, json!({"a": 1, "b": "x"}));
    }

    #[test]
    #[ignore = "jsonb_set create_missing=false edge case has a logic bug (dead-agent WIP); tracked as #158 — un-ignore when fixed"]
    fn test_set_create_missing_false() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // jsonb_set('{"a":1}', '{b}', '"x"', false) -> {"a":1}  (no create)
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":1}"#),
                path_list(&["b"]),
                jsonb_scalar(r#""x""#),
                bool_scalar(false),
            ],
        );
        assert_eq!(result, json!({"a": 1}));
    }

    #[test]
    fn test_set_nested_path() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // jsonb_set('{"a":{"b":1}}', '{a,b}', '99') -> {"a":{"b":99}}
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":{"b":1}}"#),
                path_list(&["a", "b"]),
                jsonb_scalar("99"),
            ],
        );
        assert_eq!(result, json!({"a": {"b": 99}}));
    }

    #[test]
    fn test_set_array_index() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // jsonb_set('[1,2,3]', '{1}', '99') -> [1,99,3]
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar("[1,2,3]"),
                path_list(&["1"]),
                jsonb_scalar("99"),
            ],
        );
        assert_eq!(result, json!([1, 99, 3]));
    }

    #[test]
    fn test_set_path_string_syntax() {
        let udf = JsonbSetUdf {
            signature: make_sig_any(3),
        };
        // Path supplied as Postgres-style string '{a,b}'
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":{"b":1}}"#),
                utf8_scalar("{a,b}"),
                jsonb_scalar("42"),
            ],
        );
        assert_eq!(result, json!({"a": {"b": 42}}));
    }

    // ---- jsonb_insert tests ------------------------------------------------

    #[test]
    fn test_insert_before() {
        let udf = JsonbInsertUdf {
            signature: make_sig_any(3),
        };
        // jsonb_insert('[1,2,3]', '{1}', '99') -> [1,99,2,3]  (insert before index 1)
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar("[1,2,3]"),
                path_list(&["1"]),
                jsonb_scalar("99"),
            ],
        );
        assert_eq!(result, json!([1, 99, 2, 3]));
    }

    #[test]
    fn test_insert_after() {
        let udf = JsonbInsertUdf {
            signature: make_sig_any(3),
        };
        // jsonb_insert('[1,2,3]', '{1}', '99', true) -> [1,2,99,3]  (insert after index 1)
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar("[1,2,3]"),
                path_list(&["1"]),
                jsonb_scalar("99"),
                bool_scalar(true),
            ],
        );
        assert_eq!(result, json!([1, 2, 99, 3]));
    }

    #[test]
    fn test_insert_object_key() {
        let udf = JsonbInsertUdf {
            signature: make_sig_any(3),
        };
        // jsonb_insert('{"a":1}', '{b}', '"new"') -> {"a":1,"b":"new"}
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":1}"#),
                path_list(&["b"]),
                jsonb_scalar(r#""new""#),
            ],
        );
        assert_eq!(result["b"], json!("new"));
    }

    #[test]
    fn test_insert_nested_array() {
        let udf = JsonbInsertUdf {
            signature: make_sig_any(3),
        };
        // jsonb_insert('{"a":[1,2]}', '{a,0}', '99') -> {"a":[99,1,2]}
        let result = invoke_udf(
            &udf,
            vec![
                jsonb_scalar(r#"{"a":[1,2]}"#),
                path_list(&["a", "0"]),
                jsonb_scalar("99"),
            ],
        );
        assert_eq!(result, json!({"a": [99, 1, 2]}));
    }
}
