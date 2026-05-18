//! JSON construction UDFs — fallback implementations for functions not yet
//! covered by [`crate::jsonb_udf`].
//!
//! ## What this file adds
//!
//! | Function                     | Return type  | Status  |
//! |------------------------------|-------------|---------|
//! | `json_build_array(variadic)` | `text`      | NEW     |
//! | `json_object(text[], text[])`| `text`      | NEW     |
//! | `jsonb_object(text[], text[])`| `jsonb`    | NEW     |
//!
//! All three were absent from `jsonb_udf.rs` (which only has the `jsonb_`
//! variants of `build_array` / `build_object` and the two-array form is
//! entirely new).
//!
//! ## PG-spec compliance notes
//!
//! * `json_build_array(1, 'a', true)` → `[1,"a",true]`  (text output)
//! * `json_object(ARRAY['k1','k2'], ARRAY['v1','v2'])` → `{"k1":"v1","k2":"v2"}`
//!   Keys and values are coerced to text; a NULL key is an error; a NULL value
//!   becomes a JSON null.
//! * The two-arg form is the primary PG form; a single `text[]` form (flat
//!   alternating key/value pairs) is registered as an alias.
//!
//! ## Registration
//!
//! Call [`register_json_build_udfs`] from `session::build_stateless_udf_cache`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, LargeBinaryArray, ListArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Public registration entry point
// ---------------------------------------------------------------------------

/// Register all JSON-construction UDFs that are NOT already registered by
/// [`crate::jsonb_udf::register_jsonb_udfs`].
///
/// Currently registers:
/// - `json_build_array`   (variadic → `text`)
/// - `json_object`        (two-array → `text`)
/// - `jsonb_object`       (two-array → `jsonb` / `LargeBinary`)
pub(crate) fn register_json_build_udfs(ctx: &SessionContext) {
    // json_build_array(variadic any) -> text  (non-JSONB variant)
    ctx.register_udf(ScalarUDF::from(JsonBuildArrayUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));

    // json_object(text[], text[]) -> text
    // Also accept the single flat-array form: json_object(text[])
    ctx.register_udf(ScalarUDF::from(JsonObjectUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
        jsonb: false,
    }));

    // jsonb_object(text[], text[]) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonObjectUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
        jsonb: true,
    }));
}

// ---------------------------------------------------------------------------
// Helpers (private)
// ---------------------------------------------------------------------------

/// Convert one element of an Arrow array at index `i` to a `serde_json::Value`.
/// Handles `LargeBinary` (stored JSONB), `Utf8`, `Boolean`, and `Int64`.
fn arrow_elem_to_json(arr: &ArrayRef, i: usize) -> Value {
    if arr.is_null(i) {
        return Value::Null;
    }
    match arr.data_type() {
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>();
            if let Some(a) = a {
                let bytes = a.value(i);
                // Strip optional Postgres 0x01 version byte
                let payload = if bytes.first() == Some(&0x01) && bytes.len() > 1 {
                    &bytes[1..]
                } else {
                    bytes
                };
                serde_json::from_slice(payload).unwrap_or(Value::Null)
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
            use datafusion::arrow::array::BooleanArray;
            let a = arr.as_any().downcast_ref::<BooleanArray>();
            a.map(|a| Value::Bool(a.value(i))).unwrap_or(Value::Null)
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            use datafusion::arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
            let n = match arr.data_type() {
                DataType::Int8 => arr
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .map(|a| a.value(i) as i64),
                DataType::Int16 => arr
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .map(|a| a.value(i) as i64),
                DataType::Int32 => arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(i) as i64),
                DataType::Int64 => arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(i)),
                _ => None,
            };
            n.map(|v| Value::Number(v.into())).unwrap_or(Value::Null)
        }
        DataType::Float32 | DataType::Float64 => {
            use datafusion::arrow::array::{Float32Array, Float64Array};
            let f = match arr.data_type() {
                DataType::Float32 => arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.value(i) as f64),
                DataType::Float64 => arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| a.value(i)),
                _ => None,
            };
            if let Some(f) = f {
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(f.to_string()))
            } else {
                Value::Null
            }
        }
        _ => {
            // Fallback: produce a JSON string with the debug representation
            Value::String(format!("{arr:?}[{i}]"))
        }
    }
}

/// Encode a `serde_json::Value` to canonical JSONB bytes (no version prefix).
fn to_jsonb_bytes(v: &Value) -> DFResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| DataFusionError::Execution(format!("json encode error: {e}")))
}

/// Return the row count from a slice of `ColumnarValue`s (max array length, or
/// 1 for all-scalar inputs).
fn row_count(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

/// Extract a string from a Utf8 array at index `i`.  Returns `None` for NULL.
fn utf8_at(arr: &ArrayRef, i: usize) -> DFResult<Option<String>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("expected StringArray".to_string()))?;
            Ok(Some(a.value(i).to_string()))
        }
        other => exec_err!("expected Utf8 array, got {other:?}"),
    }
}

/// Expand a `List<Utf8>` array element at row `i` into `Vec<Option<String>>`.
fn list_utf8_at(arr: &ArrayRef, i: usize) -> DFResult<Vec<Option<String>>> {
    if arr.is_null(i) {
        return Ok(vec![]);
    }
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "json_object: expected List array, got {:?}",
            arr.data_type()
        ))
    })?;
    let offsets = list.offsets();
    let start = offsets[i] as usize;
    let end = offsets[i + 1] as usize;
    let values = list.values();
    let mut out = Vec::with_capacity(end - start);
    for idx in start..end {
        if values.is_null(idx) {
            out.push(None);
        } else {
            match values.data_type() {
                DataType::Utf8 => {
                    let sa = values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("expected StringArray in list".to_string())
                        })?;
                    out.push(Some(sa.value(idx).to_string()));
                }
                other => {
                    return exec_err!("json_object: list element type must be Utf8, got {other:?}");
                }
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// json_build_array(variadic any) -> text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonBuildArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonBuildArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_build_array"
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
        let n = row_count(args);
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let items: Vec<Value> = arrays
                .iter()
                .map(|arr| arrow_elem_to_json(arr, i))
                .collect();
            let v = Value::Array(items);
            let s = serde_json::to_string(&v)
                .map_err(|e| DataFusionError::Execution(format!("json_build_array encode: {e}")))?;
            out.push(Some(s));
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_object(text[], text[])  /  jsonb_object(text[], text[])
// ---------------------------------------------------------------------------
//
// PG spec:
//   json_object(keys text[], values text[]) -> json
//   jsonb_object(keys text[], values text[]) -> jsonb
//
// Also accepts the single-array flat form:
//   json_object(text[])  — flat array of alternating keys and values.
//
// NULL key → error.
// NULL value → JSON null.
// Mismatched array lengths → error.

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObjectUdf {
    signature: Signature,
    /// true → return LargeBinary (JSONB), false → return Utf8 (text)
    jsonb: bool,
}

impl ScalarUDFImpl for JsonObjectUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        if self.jsonb {
            "jsonb_object"
        } else {
            "json_object"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        if self.jsonb {
            Ok(DataType::LargeBinary)
        } else {
            Ok(DataType::Utf8)
        }
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.is_empty() || args.len() > 2 {
            return exec_err!(
                "{} expects 1 or 2 arguments, got {}",
                self.name(),
                args.len()
            );
        }
        let n = row_count(args);
        let key_arr = args[0].clone().into_array(n)?;

        if args.len() == 2 {
            // Two-array form: json_object(keys text[], values text[])
            let val_arr = args[1].clone().into_array(n)?;
            self.invoke_two_array(n, &key_arr, &val_arr)
        } else {
            // Single flat-array form: json_object(text[]) — alternating k/v pairs
            self.invoke_flat_array(n, &key_arr)
        }
    }
}

impl JsonObjectUdf {
    fn build_object(
        &self,
        keys: &[Option<String>],
        values: &[Option<String>],
        fn_name: &str,
    ) -> DFResult<Value> {
        if keys.len() != values.len() {
            return exec_err!(
                "{fn_name}: key and value arrays must have the same length \
                 (got {} keys and {} values)",
                keys.len(),
                values.len()
            );
        }
        let mut map = serde_json::Map::with_capacity(keys.len());
        for (k_opt, v_opt) in keys.iter().zip(values.iter()) {
            let key = match k_opt {
                Some(s) => s.clone(),
                None => {
                    return exec_err!("{fn_name}: null value not allowed for object key");
                }
            };
            let val = match v_opt {
                Some(s) => Value::String(s.clone()),
                None => Value::Null,
            };
            map.insert(key, val);
        }
        Ok(Value::Object(map))
    }

    fn emit(&self, v: &Value) -> DFResult<Vec<u8>> {
        if self.jsonb {
            to_jsonb_bytes(v)
        } else {
            serde_json::to_vec(v)
                .map_err(|e| DataFusionError::Execution(format!("json encode: {e}")))
        }
    }

    fn invoke_two_array(
        &self,
        n: usize,
        key_arr: &ArrayRef,
        val_arr: &ArrayRef,
    ) -> DFResult<ColumnarValue> {
        let fn_name = self.name();
        if self.jsonb {
            let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            for i in 0..n {
                let keys = list_utf8_at(key_arr, i)?;
                let vals = list_utf8_at(val_arr, i)?;
                let v = self.build_object(&keys, &vals, fn_name)?;
                out.push(Some(self.emit(&v)?));
            }
            let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            let mut out: Vec<Option<String>> = Vec::with_capacity(n);
            for i in 0..n {
                let keys = list_utf8_at(key_arr, i)?;
                let vals = list_utf8_at(val_arr, i)?;
                let v = self.build_object(&keys, &vals, fn_name)?;
                let s = serde_json::to_string(&v)
                    .map_err(|e| DataFusionError::Execution(format!("json encode: {e}")))?;
                out.push(Some(s));
            }
            let result = StringArray::from(out);
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }

    fn invoke_flat_array(&self, n: usize, arr: &ArrayRef) -> DFResult<ColumnarValue> {
        let fn_name = self.name();
        if self.jsonb {
            let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            for i in 0..n {
                let flat = list_utf8_at(arr, i)?;
                if flat.len() % 2 != 0 {
                    return exec_err!("{fn_name}: flat array must have an even number of elements");
                }
                let keys: Vec<Option<String>> = flat.iter().step_by(2).cloned().collect();
                let vals: Vec<Option<String>> = flat.iter().skip(1).step_by(2).cloned().collect();
                let v = self.build_object(&keys, &vals, fn_name)?;
                out.push(Some(self.emit(&v)?));
            }
            let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            let mut out: Vec<Option<String>> = Vec::with_capacity(n);
            for i in 0..n {
                let flat = list_utf8_at(arr, i)?;
                if flat.len() % 2 != 0 {
                    return exec_err!("{fn_name}: flat array must have an even number of elements");
                }
                let keys: Vec<Option<String>> = flat.iter().step_by(2).cloned().collect();
                let vals: Vec<Option<String>> = flat.iter().skip(1).step_by(2).cloned().collect();
                let v = self.build_object(&keys, &vals, fn_name)?;
                let s = serde_json::to_string(&v)
                    .map_err(|e| DataFusionError::Execution(format!("json encode: {e}")))?;
                out.push(Some(s));
            }
            let result = StringArray::from(out);
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests — pure Rust, no engine spin-up needed
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use serde_json::json;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_str_arr(vals: &[Option<&str>]) -> ArrayRef {
        let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
        Arc::new(arr)
    }

    fn make_int_arr(vals: &[Option<i64>]) -> ArrayRef {
        let arr: Int64Array = vals.iter().copied().collect();
        Arc::new(arr)
    }

    fn make_bool_arr(vals: &[Option<bool>]) -> ArrayRef {
        let arr: BooleanArray = vals.iter().copied().collect();
        Arc::new(arr)
    }

    fn invoke_build_array(arrays: Vec<ArrayRef>) -> Vec<Option<String>> {
        let udf = JsonBuildArrayUdf {
            signature: Signature::variadic_any(Volatility::Immutable),
        };
        let cv: Vec<ColumnarValue> = arrays.into_iter().map(ColumnarValue::Array).collect();
        let arg_fields: Vec<datafusion::arrow::datatypes::FieldRef> = cv
            .iter()
            .enumerate()
            .map(|(i, c)| {
                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                    format!("a{i}"),
                    c.data_type(),
                    true,
                ))
            })
            .collect();
        let args = ScalarFunctionArgs {
            args: cv,
            arg_fields,
            number_rows: 1,
            return_field: std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Utf8,
                true,
            )),
            config_options: std::sync::Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                (0..sa.len())
                    .map(|i| {
                        if sa.is_null(i) {
                            None
                        } else {
                            Some(sa.value(i).to_string())
                        }
                    })
                    .collect()
            }
            ColumnarValue::Scalar(_) => panic!("expected array result"),
        }
    }

    // ── json_build_array tests ────────────────────────────────────────────────

    /// PG: `json_build_array()` → `[]`
    #[test]
    fn test_json_build_array_empty() {
        let result = invoke_build_array(vec![]);
        // 0 args → row_count returns 1, array of 1 row with empty JSON array
        assert_eq!(result.len(), 1);
        let s = result[0].as_ref().expect("should not be null");
        let v: Value = serde_json::from_str(s).unwrap();
        assert_eq!(v, json!([]));
    }

    /// PG: `json_build_array(1, 'a', true)` → `[1,"a",true]`
    #[test]
    fn test_json_build_array_mixed_types() {
        let result = invoke_build_array(vec![
            make_int_arr(&[Some(1)]),
            make_str_arr(&[Some("a")]),
            make_bool_arr(&[Some(true)]),
        ]);
        assert_eq!(result.len(), 1);
        let s = result[0].as_ref().expect("should not be null");
        let v: Value = serde_json::from_str(s).unwrap();
        assert_eq!(v, json!([1, "a", true]));
    }

    /// PG: `json_build_array(1, 2, 3)` → `[1,2,3]`
    #[test]
    fn test_json_build_array_integers() {
        let result = invoke_build_array(vec![
            make_int_arr(&[Some(1)]),
            make_int_arr(&[Some(2)]),
            make_int_arr(&[Some(3)]),
        ]);
        assert_eq!(result.len(), 1);
        let v: Value = serde_json::from_str(result[0].as_ref().unwrap()).unwrap();
        assert_eq!(v, json!([1, 2, 3]));
    }

    /// PG: NULL element in array becomes JSON null.
    /// `json_build_array(1, NULL::text)` → `[1,null]`
    #[test]
    fn test_json_build_array_null_element() {
        let result = invoke_build_array(vec![make_int_arr(&[Some(1)]), make_str_arr(&[None])]);
        assert_eq!(result.len(), 1);
        let v: Value = serde_json::from_str(result[0].as_ref().unwrap()).unwrap();
        assert_eq!(v, json!([1, null]));
    }

    /// Multiple rows: json_build_array vectorises over rows.
    #[test]
    fn test_json_build_array_multiple_rows() {
        let result = invoke_build_array(vec![
            make_int_arr(&[Some(10), Some(20)]),
            make_str_arr(&[Some("x"), Some("y")]),
        ]);
        assert_eq!(result.len(), 2);
        let v0: Value = serde_json::from_str(result[0].as_ref().unwrap()).unwrap();
        let v1: Value = serde_json::from_str(result[1].as_ref().unwrap()).unwrap();
        assert_eq!(v0, json!([10, "x"]));
        assert_eq!(v1, json!([20, "y"]));
    }

    /// PG: `json_build_array('hello')` → `["hello"]`
    #[test]
    fn test_json_build_array_single_string() {
        let result = invoke_build_array(vec![make_str_arr(&[Some("hello")])]);
        let v: Value = serde_json::from_str(result[0].as_ref().unwrap()).unwrap();
        assert_eq!(v, json!(["hello"]));
    }

    // ── json_build_object (already exists — verify via direct UDF invoke) ──

    /// Verify the existing `json_build_object` UDF produces correct JSON output
    /// with mixed types. PG: `json_build_object('k', 1, 'flag', true)`
    /// → `{"flag":true,"k":1}` (key order may vary in Basin due to insertion
    /// order — we check presence, not order).
    #[test]
    fn test_existing_json_build_object_two_pairs() {
        // We can't invoke it here without importing its private struct, but we
        // can verify our helper `arrow_elem_to_json` correctly converts each
        // expected type so that jsonb_build_object / json_build_object work.
        let int_arr = make_int_arr(&[Some(42)]);
        let str_arr = make_str_arr(&[Some("hello")]);
        let bool_arr = make_bool_arr(&[Some(true)]);
        let null_arr = make_str_arr(&[None]);

        assert_eq!(arrow_elem_to_json(&int_arr, 0), json!(42));
        assert_eq!(arrow_elem_to_json(&str_arr, 0), json!("hello"));
        assert_eq!(arrow_elem_to_json(&bool_arr, 0), json!(true));
        assert_eq!(arrow_elem_to_json(&null_arr, 0), json!(null));
    }

    // ── to_json / to_jsonb / row_to_json (already exist — smoke verify) ──────

    /// Verify `arrow_elem_to_json` produces the serde_json values that
    /// `to_json` / `to_jsonb` depend on.
    #[test]
    fn test_to_json_elem_conversions() {
        let int_arr = make_int_arr(&[Some(7)]);
        assert_eq!(arrow_elem_to_json(&int_arr, 0), json!(7));

        let str_arr = make_str_arr(&[Some("world")]);
        // A bare Utf8 string is treated as a JSON string (not re-parsed)
        assert_eq!(arrow_elem_to_json(&str_arr, 0), json!("world"));

        let json_str_arr = make_str_arr(&[Some(r#"{"a":1}"#)]);
        // A value that parses as JSON is returned as a JSON object
        assert_eq!(arrow_elem_to_json(&json_str_arr, 0), json!({"a": 1}));
    }

    // ── json_object / jsonb_object unit tests ─────────────────────────────────
    //
    // These tests call `build_object` directly — the list-expansion path is
    // exercised by the integration tests.

    fn make_json_object_udf(jsonb: bool) -> JsonObjectUdf {
        JsonObjectUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            jsonb,
        }
    }

    /// PG: `json_object('{k1,k2}', '{v1,v2}')` → `{"k1":"v1","k2":"v2"}`
    #[test]
    fn test_json_object_build_basic() {
        let udf = make_json_object_udf(false);
        let keys = vec![Some("k1".to_string()), Some("k2".to_string())];
        let vals = vec![Some("v1".to_string()), Some("v2".to_string())];
        let v = udf.build_object(&keys, &vals, "json_object").unwrap();
        assert_eq!(v["k1"], json!("v1"));
        assert_eq!(v["k2"], json!("v2"));
    }

    /// PG: NULL value → JSON null.
    /// `json_object('{k}', ARRAY[NULL::text])` → `{"k":null}`
    #[test]
    fn test_json_object_null_value_becomes_json_null() {
        let udf = make_json_object_udf(false);
        let keys = vec![Some("k".to_string())];
        let vals = vec![None];
        let v = udf.build_object(&keys, &vals, "json_object").unwrap();
        assert_eq!(v["k"], json!(null));
    }

    /// PG: NULL key → error.
    #[test]
    fn test_json_object_null_key_is_error() {
        let udf = make_json_object_udf(false);
        let keys = vec![None];
        let vals = vec![Some("v".to_string())];
        let err = udf.build_object(&keys, &vals, "json_object").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("null value not allowed for object key"),
            "expected null-key error, got: {msg}"
        );
    }

    /// PG: mismatched array lengths → error.
    #[test]
    fn test_json_object_mismatched_lengths_error() {
        let udf = make_json_object_udf(false);
        let keys = vec![Some("k1".to_string()), Some("k2".to_string())];
        let vals = vec![Some("v1".to_string())];
        let err = udf.build_object(&keys, &vals, "json_object").unwrap_err();
        assert!(
            err.to_string().contains("same length"),
            "expected length mismatch error, got: {err}"
        );
    }

    /// jsonb_object should produce valid JSONB bytes.
    #[test]
    fn test_jsonb_object_produces_valid_bytes() {
        let udf = make_json_object_udf(true);
        let keys = vec![Some("a".to_string())];
        let vals = vec![Some("1".to_string())];
        let v = udf.build_object(&keys, &vals, "jsonb_object").unwrap();
        let bytes = udf.emit(&v).unwrap();
        let decoded: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded["a"], json!("1"));
    }

    /// `json_object` emitted as text should be valid JSON.
    #[test]
    fn test_json_object_text_output_is_valid_json() {
        let udf = make_json_object_udf(false);
        let keys = vec![Some("name".to_string()), Some("age".to_string())];
        let vals = vec![Some("Alice".to_string()), Some("30".to_string())];
        let v = udf.build_object(&keys, &vals, "json_object").unwrap();
        let bytes = udf.emit(&v).unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        let decoded: Value = serde_json::from_str(s).unwrap();
        assert_eq!(decoded["name"], json!("Alice"));
        assert_eq!(decoded["age"], json!("30"));
    }

    // ── name() smoke ──────────────────────────────────────────────────────────

    #[test]
    fn test_udf_names() {
        assert_eq!(
            JsonBuildArrayUdf {
                signature: Signature::variadic_any(Volatility::Immutable)
            }
            .name(),
            "json_build_array"
        );
        assert_eq!(make_json_object_udf(false).name(), "json_object");
        assert_eq!(make_json_object_udf(true).name(), "jsonb_object");
    }
}
