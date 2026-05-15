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
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::catalog::TableFunctionImpl;
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

    // json_build_object(variadic any) -> text  (Utf8 variant of jsonb_build_object)
    ctx.register_udf(ScalarUDF::from(JsonBuildObjectUdf {
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

    // Note: jsonb_agg and jsonb_object_agg are registered as real AggregateUDFs
    // in crate::pg_agg_udf::register_json_agg_udafs (called after this function).
    // Do NOT register scalar stubs here — they would shadow the real aggregate.

    // ---------------------------------------------------------------------------
    // JSON (non-jsonb) variants — accept Utf8 or LargeBinary
    // ---------------------------------------------------------------------------

    // json_typeof(json) -> text
    ctx.register_udf(ScalarUDF::from(JsonTypeofUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_strip_nulls(json) -> json (text)
    ctx.register_udf(ScalarUDF::from(JsonStripNullsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // to_json(any) -> text  (mirror of to_jsonb returning Utf8)
    ctx.register_udf(ScalarUDF::from(ToJsonUdf {
        signature: Signature::any(1, Volatility::Immutable),
    }));

    // json_each(json) -> text (scalar stub: key=value,... pairs)
    ctx.register_udf(ScalarUDF::from(JsonEachUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_each_text(json) -> text (scalar stub: key=value,... pairs as text)
    ctx.register_udf(ScalarUDF::from(JsonEachTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_object_keys(json) -> text (scalar stub: comma-joined keys)
    ctx.register_udf(ScalarUDF::from(JsonObjectKeysUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_array_elements(json) -> json (scalar stub: first element)
    ctx.register_udf(ScalarUDF::from(JsonArrayElementsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_array_elements_text(json) -> text (scalar stub: first element as text)
    ctx.register_udf(ScalarUDF::from(JsonArrayElementsTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---------------------------------------------------------------------------
    // JSON path extras
    // ---------------------------------------------------------------------------

    // jsonb_path_query_first(jsonb, jsonpath) -> jsonb  (first match)
    ctx.register_udf(ScalarUDF::from(JsonbPathQueryFirstUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_query_array(jsonb, jsonpath) -> jsonb  (all matches as array)
    ctx.register_udf(ScalarUDF::from(JsonbPathQueryArrayUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---------------------------------------------------------------------------
    // JSON operator UDFs — targets for the text-rewriter
    // ---------------------------------------------------------------------------

    // json_get(jsonb, key_or_idx) -> jsonb   (rewrite target for ->)
    ctx.register_udf(ScalarUDF::from(JsonGetUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_get_text(jsonb, key_or_idx) -> text   (rewrite target for ->>)
    ctx.register_udf(ScalarUDF::from(JsonGetTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_path_extract(jsonb, path_array) -> jsonb   (rewrite target for #>)
    ctx.register_udf(ScalarUDF::from(JsonPathExtractUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // json_path_extract_text(jsonb, path_array) -> text   (rewrite target for #>>)
    ctx.register_udf(ScalarUDF::from(JsonPathExtractTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_contains(jsonb, jsonb) -> bool   (rewrite target for jsonb @>)
    ctx.register_udf(ScalarUDF::from(JsonbContainsUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_contained_by(jsonb, jsonb) -> bool   (rewrite target for <@)
    ctx.register_udf(ScalarUDF::from(JsonbContainedByUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_has_key(jsonb, text) -> bool   (rewrite target for ?)
    ctx.register_udf(ScalarUDF::from(JsonbHasKeyUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_has_all_keys(jsonb, text[]) -> bool   (rewrite target for ?&)
    ctx.register_udf(ScalarUDF::from(JsonbHasAllKeysUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_has_any_key(jsonb, text[]) -> bool   (rewrite target for ?|)
    ctx.register_udf(ScalarUDF::from(JsonbHasAnyKeyUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_concat(jsonb, jsonb) -> jsonb   (rewrite target for ||)
    ctx.register_udf(ScalarUDF::from(JsonbConcatUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_delete_key(jsonb, text) -> jsonb   (rewrite target for jsonb - 'key')
    ctx.register_udf(ScalarUDF::from(JsonbDeleteKeyUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_delete_keys(jsonb, text[]) -> jsonb   (rewrite target for jsonb - ARRAY[...])
    ctx.register_udf(ScalarUDF::from(JsonbDeleteKeysUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_delete_index(jsonb, int8) -> jsonb   (rewrite target for jsonb - idx)
    ctx.register_udf(ScalarUDF::from(JsonbDeleteIndexUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // json_to_record / jsonb_to_record (scalar stubs)
    ctx.register_udf(ScalarUDF::from(JsonToRecordStubUdf {
        signature: Signature::any(1, Volatility::Immutable),
        name: "json_to_record",
    }));
    ctx.register_udf(ScalarUDF::from(JsonToRecordStubUdf {
        signature: Signature::any(1, Volatility::Immutable),
        name: "jsonb_to_record",
    }));

    // json_to_recordset / jsonb_to_recordset (scalar stubs)
    ctx.register_udf(ScalarUDF::from(JsonToRecordStubUdf {
        signature: Signature::any(1, Volatility::Immutable),
        name: "json_to_recordset",
    }));
    ctx.register_udf(ScalarUDF::from(JsonToRecordStubUdf {
        signature: Signature::any(1, Volatility::Immutable),
        name: "jsonb_to_recordset",
    }));

    // ---------------------------------------------------------------------------
    // Table-valued functions (UDTFs) — for use in FROM clauses.
    // These require register_udtf (not register_udf) because DataFusion looks
    // them up separately when planning FROM <function>(...) syntax.
    // ---------------------------------------------------------------------------
    ctx.register_udtf("jsonb_each", Arc::new(JsonbEachTf { text_values: false }));
    ctx.register_udtf("jsonb_each_text", Arc::new(JsonbEachTf { text_values: true }));
    ctx.register_udtf("jsonb_array_elements", Arc::new(JsonbArrayElementsTf { text_values: false }));
    ctx.register_udtf("jsonb_array_elements_text", Arc::new(JsonbArrayElementsTf { text_values: true }));
    ctx.register_udtf("jsonb_object_keys", Arc::new(JsonbObjectKeysTf {}));
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbTypeofUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbTypeofUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_typeof" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPrettyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPrettyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_pretty" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbArrayLengthUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayLengthUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_length" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Int64) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbStripNullsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbStripNullsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_strip_nulls" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbSetUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbSetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_set" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbInsertUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbInsertUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_insert" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathQueryUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_query" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathExistsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathExistsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_exists" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathMatchUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathMatchUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_match" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        // Delegate to the same logic as jsonb_path_exists
        JsonbPathExistsUdf {
            signature: self.signature.clone(),
        }.invoke(args)
    }
}

// ---------------------------------------------------------------------------
// jsonb_object_keys  (SRF stub: returns comma-joined keys as Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbObjectKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbObjectKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_object_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbEachUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbEachUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_each" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbEachTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbEachTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_each_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbArrayElementsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayElementsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_elements" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbArrayElementsTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbArrayElementsTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_array_elements_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbBuildObjectUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbBuildObjectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_build_object" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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
// json_build_object(variadic any) -> text  (Utf8 variant)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonBuildObjectUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonBuildObjectUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_build_object" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() % 2 != 0 {
            return exec_err!(
                "json_build_object requires an even number of arguments (key/value pairs), got {}",
                args.len()
            );
        }
        let n = row_count(args);
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
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
                out.push(Some(v.to_string()));
            }
        }
        use datafusion::arrow::array::StringArray;
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_build_array(variadic any) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbBuildArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbBuildArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_build_array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJsonbUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToJsonbUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "to_jsonb" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct RowToJsonUdf {
    signature: Signature,
}

impl ScalarUDFImpl for RowToJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "row_to_json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayToJsonUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayToJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "array_to_json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbAggStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbAggStubUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_agg" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "jsonb_agg requires AggregateUDFImpl; \
             deferred to v0.2 — use jsonb_build_array() for scalar aggregation"
        )
    }
}

// ---------------------------------------------------------------------------
// jsonb_object_agg(key, value) -> jsonb  [aggregate stub]
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbObjectAggStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbObjectAggStubUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_object_agg" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "jsonb_object_agg requires AggregateUDFImpl; \
             deferred to v0.2 — use jsonb_build_object() for scalar object construction"
        )
    }
}

// ---------------------------------------------------------------------------
// Minimal jsonpath evaluator
// ---------------------------------------------------------------------------
//
// Supports:
//   $                  — the whole document
//   $.field            — object key
//   $.field[0]         — array index
//   $.field[*]         — all array elements
//   $.**               — recursive descent (first match returns first leaf)
//   $.field ? (@.x > 1) — filter predicates are stripped and ignored
//
// For jsonb_path_query / jsonb_path_query_array all matching values are
// collected. For jsonb_path_query_first only the first is returned.
// jsonb_path_exists returns true if at least one match exists.
// jsonb_path_match is an alias for jsonb_path_exists (v0.1 scope).

fn jsonpath_query(root: &Value, path: &str) -> Vec<Value> {
    // Strip leading whitespace / $
    let path = path.trim();
    // Strip optional `strict` / `lax` keywords
    let path = path.strip_prefix("strict").map(str::trim).unwrap_or(path);
    let path = path.strip_prefix("lax").map(str::trim).unwrap_or(path);
    let path = path.strip_prefix('$').unwrap_or(path);

    let mut results = vec![root.clone()];
    // Tokenise the path into steps, ignoring filter predicates `? (...)`
    let mut remaining = path.trim_start_matches('.');
    // Strip filter predicate at top level if any
    remaining = strip_filter_predicate(remaining);

    while !remaining.is_empty() {
        remaining = remaining.trim_start_matches('.');
        if remaining.is_empty() { break; }

        // Recursive descent: **
        if remaining.starts_with("**") {
            remaining = &remaining[2..];
            remaining = remaining.trim_start_matches('.');
            remaining = strip_filter_predicate(remaining);
            let mut next: Vec<Value> = Vec::new();
            for val in &results {
                collect_recursive(val, &mut next);
            }
            results = next;
            continue;
        }

        // Extract the next step name (up to `.`, `[`, or `?`)
        let step_end = remaining
            .find(|c: char| c == '.' || c == '[' || c == '?')
            .unwrap_or(remaining.len());
        let step = &remaining[..step_end];
        remaining = &remaining[step_end..];

        // Apply object key descent
        let mut next: Vec<Value> = Vec::new();
        for val in &results {
            match val {
                Value::Object(map) => {
                    if let Some(child) = map.get(step) {
                        next.push(child.clone());
                    }
                }
                _ => {}
            }
        }
        results = next;

        // Handle index subscript `[n]` or `[*]`
        while remaining.starts_with('[') {
            let close = remaining.find(']').unwrap_or(remaining.len().saturating_sub(1));
            let idx_str = &remaining[1..close];
            remaining = &remaining[close + 1..];

            let mut next: Vec<Value> = Vec::new();
            for val in &results {
                match val {
                    Value::Array(arr) => {
                        if idx_str == "*" {
                            next.extend(arr.iter().cloned());
                        } else if let Ok(idx) = idx_str.parse::<usize>() {
                            if let Some(elem) = arr.get(idx) {
                                next.push(elem.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            results = next;
        }

        // Strip filter predicate if present
        remaining = strip_filter_predicate(remaining);
    }
    results
}

/// Strip a leading `? (...)` filter predicate from `s`, returning the rest.
fn strip_filter_predicate(s: &str) -> &str {
    let s = s.trim_start();
    if !s.starts_with('?') { return s; }
    let s = s[1..].trim_start();
    if !s.starts_with('(') { return s; }
    // Find matching close paren
    let mut depth = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &s[i + 1..];
                }
            }
            _ => {}
        }
    }
    s
}

/// Collect all scalar/leaf values recursively (recursive descent `**`).
fn collect_recursive(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(map) => {
            out.push(v.clone());
            for child in map.values() {
                collect_recursive(child, out);
            }
        }
        Value::Array(arr) => {
            out.push(v.clone());
            for elem in arr {
                collect_recursive(elem, out);
            }
        }
        other => out.push(other.clone()),
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_query_first(jsonb, jsonpath) -> jsonb
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathQueryFirstUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryFirstUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_query_first" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_path_query_first expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match (extract_jsonb_value(&doc_arr, i, "jsonb_path_query_first")?,
                   extract_string(&path_arr, i)) {
                (Some(doc), Some(path)) => {
                    let matches = jsonpath_query(&doc, &path);
                    if let Some(first) = matches.into_iter().next() {
                        out.push(Some(value_to_jsonb(&first)?));
                    } else {
                        out.push(None);
                    }
                }
                _ => out.push(None),
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_query_array(jsonb, jsonpath) -> jsonb  (array of all matches)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathQueryArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_path_query_array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_path_query_array expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match (extract_jsonb_value(&doc_arr, i, "jsonb_path_query_array")?,
                   extract_string(&path_arr, i)) {
                (Some(doc), Some(path)) => {
                    let matches = jsonpath_query(&doc, &path);
                    let arr = Value::Array(matches);
                    out.push(Some(value_to_jsonb(&arr)?));
                }
                _ => out.push(None),
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_typeof(json) -> text  (alias of jsonb_typeof for plain json input)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonTypeofUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonTypeofUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_typeof" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_typeof expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_typeof")? {
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
// json_strip_nulls(json) -> text  (strips nulls, returns JSON text)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonStripNullsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonStripNullsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_strip_nulls" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_strip_nulls expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_strip_nulls")? {
                None => out.push(None),
                Some(v) => {
                    let stripped = strip_nulls(v);
                    let s = serde_json::to_string(&stripped).map_err(|e| {
                        DataFusionError::Execution(format!("json_strip_nulls encode: {e}"))
                    })?;
                    out.push(Some(s));
                }
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// to_json(any) -> text  (like to_jsonb but returns Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJsonUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToJsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "to_json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("to_json expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.push(Some("null".to_string()));
                continue;
            }
            let v = array_element_to_json(&arr, i);
            let s = serde_json::to_string(&v).map_err(|e| {
                DataFusionError::Execution(format!("to_json encode: {e}"))
            })?;
            out.push(Some(s));
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_each(json) -> text  (scalar stub: "key=value,..." pairs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonEachUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonEachUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_each" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_each expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_each")? {
                None => out.push(None),
                Some(Value::Object(map)) => {
                    let pairs: Vec<String> = map
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, serde_json::to_string(v).unwrap_or_default()))
                        .collect();
                    out.push(Some(pairs.join(",")));
                }
                Some(v) => out.push(Some(serde_json::to_string(&v).unwrap_or_default())),
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_each_text(json) -> text  (scalar stub: key=text_value pairs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonEachTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonEachTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_each_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_each_text expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_each_text")? {
                None => out.push(None),
                Some(Value::Object(map)) => {
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
                }
                Some(v) => out.push(Some(serde_json::to_string(&v).unwrap_or_default())),
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_object_keys(json) -> text  (scalar stub: comma-joined keys)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObjectKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonObjectKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_object_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_object_keys expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_object_keys")? {
                None => out.push(None),
                Some(Value::Object(map)) => {
                    let keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
                    out.push(Some(keys.join(",")));
                }
                Some(_) => out.push(None),
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_array_elements(json) -> json  (scalar stub: first element as LargeBinary)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayElementsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonArrayElementsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_array_elements" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_array_elements expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_array_elements")? {
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
// json_array_elements_text(json) -> text  (scalar stub: first element as text)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayElementsTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonArrayElementsTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_array_elements_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("json_array_elements_text expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&arr, i, "json_array_elements_text")? {
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
// json_get(jsonb, key_or_idx) -> jsonb   (operator ->)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonGetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_get" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("json_get expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "json_get")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => { out.push(None); continue; }
            };
            let result = match &doc {
                Value::Object(map) => map.get(&key).cloned(),
                Value::Array(arr) => {
                    key.parse::<usize>().ok().and_then(|idx| arr.get(idx).cloned())
                        .or_else(|| key.parse::<i64>().ok().and_then(|idx| {
                            if idx < 0 {
                                let pos = arr.len() as i64 + idx;
                                if pos >= 0 { arr.get(pos as usize).cloned() } else { None }
                            } else { None }
                        }))
                }
                _ => None,
            };
            match result {
                Some(v) => out.push(Some(value_to_jsonb(&v)?)),
                None => out.push(None),
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_get_text(jsonb, key_or_idx) -> text   (operator ->>)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonGetTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_get_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("json_get_text expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "json_get_text")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => { out.push(None); continue; }
            };
            let child = match &doc {
                Value::Object(map) => map.get(&key).cloned(),
                Value::Array(arr) => {
                    key.parse::<usize>().ok().and_then(|idx| arr.get(idx).cloned())
                        .or_else(|| key.parse::<i64>().ok().and_then(|idx| {
                            if idx < 0 {
                                let pos = arr.len() as i64 + idx;
                                if pos >= 0 { arr.get(pos as usize).cloned() } else { None }
                            } else { None }
                        }))
                }
                _ => None,
            };
            let text = match child {
                None => None,
                Some(Value::String(s)) => Some(s),
                Some(Value::Null) => None,
                Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
            };
            out.push(text);
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_path_extract(jsonb, path_array) -> jsonb   (operator #>)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPathExtractUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonPathExtractUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_path_extract" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("json_path_extract expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "json_path_extract")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let path_str = match extract_string(&path_arr, i) {
                Some(s) => s,
                None => { out.push(None); continue; }
            };
            // path_str is like '{a,b,c}' or 'a.b.c'
            let segments = parse_path(&path_str);
            let mut cur = doc;
            let mut found = true;
            for seg in &segments {
                let tmp = std::mem::replace(&mut cur, Value::Null);
                let next = match tmp {
                    Value::Object(mut map) => {
                        map.remove(seg.as_str())
                    }
                    Value::Array(mut arr) => {
                        match seg.parse::<usize>().ok() {
                            Some(idx) if idx < arr.len() => Some(arr.swap_remove(idx)),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                match next {
                    Some(v) => cur = v,
                    None => { found = false; break; }
                }
            }
            if found {
                out.push(Some(value_to_jsonb(&cur)?));
            } else {
                out.push(None);
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_path_extract_text(jsonb, path_array) -> text   (operator #>>)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPathExtractTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonPathExtractTextUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_path_extract_text" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("json_path_extract_text expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "json_path_extract_text")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let path_str = match extract_string(&path_arr, i) {
                Some(s) => s,
                None => { out.push(None); continue; }
            };
            let segments = parse_path(&path_str);
            let mut cur = doc;
            let mut found = true;
            for seg in &segments {
                let tmp = std::mem::replace(&mut cur, Value::Null);
                let next = match tmp {
                    Value::Object(mut map) => {
                        map.remove(seg.as_str())
                    }
                    Value::Array(mut arr) => {
                        match seg.parse::<usize>().ok() {
                            Some(idx) if idx < arr.len() => Some(arr.swap_remove(idx)),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                match next {
                    Some(v) => cur = v,
                    None => { found = false; break; }
                }
            }
            if found {
                let text = match cur {
                    Value::String(s) => s,
                    Value::Null => { out.push(None); continue; }
                    v => serde_json::to_string(&v).unwrap_or_default(),
                };
                out.push(Some(text));
            } else {
                out.push(None);
            }
        }
        let result = StringArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_contains(jsonb, jsonb) -> bool   (operator @> — jsonb containment)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbContainsUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbContainsUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_contains" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_contains expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let left_arr = args[0].clone().into_array(n)?;
        let right_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let left = match extract_jsonb_value(&left_arr, i, "jsonb_contains")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_contains")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            out.push(Some(json_contains(&left, &right)));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Recursive JSON containment check (`left @> right`).
fn json_contains(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Object(lm), Value::Object(rm)) => {
            rm.iter().all(|(k, rv)| {
                lm.get(k).map(|lv| json_contains(lv, rv)).unwrap_or(false)
            })
        }
        (Value::Array(la), Value::Array(ra)) => {
            ra.iter().all(|rv| la.iter().any(|lv| json_contains(lv, rv)))
        }
        (l, r) => l == r,
    }
}

// ---------------------------------------------------------------------------
// jsonb_contained_by(jsonb, jsonb) -> bool   (operator <@)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbContainedByUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbContainedByUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_contained_by" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_contained_by expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let left_arr = args[0].clone().into_array(n)?;
        let right_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let left = match extract_jsonb_value(&left_arr, i, "jsonb_contained_by")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_contained_by")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            // <@ is the reverse of @>
            out.push(Some(json_contains(&right, &left)));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_has_key(jsonb, text) -> bool   (operator ?)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbHasKeyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbHasKeyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_has_key" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_has_key expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "jsonb_has_key")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => { out.push(None); continue; }
            };
            let found = match &doc {
                Value::Object(map) => map.contains_key(&key),
                Value::Array(arr) => arr.iter().any(|v| {
                    matches!(v, Value::String(s) if s == &key)
                }),
                _ => false,
            };
            out.push(Some(found));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_has_all_keys(jsonb, text) -> bool   (operator ?& — check comma-list)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbHasAllKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbHasAllKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_has_all_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_has_all_keys expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let keys_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "jsonb_has_all_keys")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            // keys arg: comma-separated string or JSON array
            let keys_str = match extract_string(&keys_arr, i) {
                Some(s) => s,
                None => { out.push(None); continue; }
            };
            let keys = parse_key_list(&keys_str);
            let found = match &doc {
                Value::Object(map) => keys.iter().all(|k| map.contains_key(k.as_str())),
                _ => false,
            };
            out.push(Some(found));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_has_any_key(jsonb, text) -> bool   (operator ?|)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbHasAnyKeyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbHasAnyKeyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_has_any_key" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_has_any_key expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let keys_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            let doc = match extract_jsonb_value(&doc_arr, i, "jsonb_has_any_key")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let keys_str = match extract_string(&keys_arr, i) {
                Some(s) => s,
                None => { out.push(None); continue; }
            };
            let keys = parse_key_list(&keys_str);
            let found = match &doc {
                Value::Object(map) => keys.iter().any(|k| map.contains_key(k.as_str())),
                _ => false,
            };
            out.push(Some(found));
        }
        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_concat(jsonb, jsonb) -> jsonb   (operator ||)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbConcatUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbConcatUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_concat" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_concat expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let left_arr = args[0].clone().into_array(n)?;
        let right_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let left = match extract_jsonb_value(&left_arr, i, "jsonb_concat")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_concat")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let merged = match (left, right) {
                (Value::Object(mut lm), Value::Object(rm)) => {
                    for (k, v) in rm {
                        lm.insert(k, v);
                    }
                    Value::Object(lm)
                }
                (Value::Array(mut la), Value::Array(ra)) => {
                    la.extend(ra);
                    Value::Array(la)
                }
                // Scalar || object: wrap scalar then merge
                (l, Value::Object(rm)) => {
                    let mut map = serde_json::Map::new();
                    if let Value::Object(lm) = l {
                        for (k, v) in lm { map.insert(k, v); }
                    }
                    for (k, v) in rm { map.insert(k, v); }
                    Value::Object(map)
                }
                (Value::Array(mut la), r) => { la.push(r); Value::Array(la) }
                (l, Value::Array(mut ra)) => { ra.insert(0, l); Value::Array(ra) }
                // Two scalars: make array
                (l, r) => Value::Array(vec![l, r]),
            };
            out.push(Some(value_to_jsonb(&merged)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_delete_key(jsonb, text) -> jsonb   (operator: jsonb - 'key')
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbDeleteKeyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbDeleteKeyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_delete_key" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_delete_key expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut doc = match extract_jsonb_value(&doc_arr, i, "jsonb_delete_key")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let key = match extract_string(&key_arr, i) {
                Some(s) => s,
                None => { out.push(None); continue; }
            };
            if let Value::Object(ref mut map) = doc {
                map.remove(key.as_str());
            }
            out.push(Some(value_to_jsonb(&doc)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_delete_keys(jsonb, text[]) -> jsonb   (operator: jsonb - ARRAY[...])
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbDeleteKeysUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbDeleteKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_delete_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        use datafusion::arrow::array::ListArray;
        use datafusion::arrow::array::LargeListArray;
        if args.len() != 2 {
            return exec_err!("jsonb_delete_keys expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let keys_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut doc = match extract_jsonb_value(&doc_arr, i, "jsonb_delete_keys")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let keys: Vec<String> = match keys_arr.data_type() {
                DataType::Utf8 => {
                    let a = keys_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        DataFusionError::Execution("jsonb_delete_keys: not StringArray".into())
                    })?;
                    if a.is_null(i) { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    parse_key_list(a.value(i))
                }
                DataType::List(_) => {
                    let a = keys_arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Execution("jsonb_delete_keys: not ListArray".into())
                    })?;
                    if a.is_null(i) { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    let values = a.value(i);
                    let sa = values.as_any().downcast_ref::<StringArray>();
                    match sa {
                        Some(sa) => (0..sa.len()).filter_map(|j| if sa.is_null(j) { None } else { Some(sa.value(j).to_string()) }).collect(),
                        None => vec![],
                    }
                }
                DataType::LargeList(_) => {
                    let a = keys_arr.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                        DataFusionError::Execution("jsonb_delete_keys: not LargeListArray".into())
                    })?;
                    if a.is_null(i) { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    let values = a.value(i);
                    let sa = values.as_any().downcast_ref::<StringArray>();
                    match sa {
                        Some(sa) => (0..sa.len()).filter_map(|j| if sa.is_null(j) { None } else { Some(sa.value(j).to_string()) }).collect(),
                        None => vec![],
                    }
                }
                _ => {
                    match extract_string(&keys_arr, i) {
                        Some(s) => parse_key_list(&s),
                        None => { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    }
                }
            };
            if let Value::Object(ref mut map) = doc {
                for k in &keys {
                    map.remove(k.as_str());
                }
            }
            out.push(Some(value_to_jsonb(&doc)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// jsonb_delete_index(jsonb, int8) -> jsonb   (operator: jsonb - idx)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbDeleteIndexUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbDeleteIndexUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "jsonb_delete_index" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::LargeBinary) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        use datafusion::arrow::array::Int32Array;
        if args.len() != 2 {
            return exec_err!("jsonb_delete_index expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let idx_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut doc = match extract_jsonb_value(&doc_arr, i, "jsonb_delete_index")? {
                Some(v) => v,
                None => { out.push(None); continue; }
            };
            let idx: i64 = match idx_arr.data_type() {
                DataType::Int64 => {
                    let a = idx_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Execution("jsonb_delete_index: not Int64Array".into())
                    })?;
                    if a.is_null(i) { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    a.value(i)
                }
                DataType::Int32 => {
                    let a = idx_arr.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Execution("jsonb_delete_index: not Int32Array".into())
                    })?;
                    if a.is_null(i) { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    a.value(i) as i64
                }
                _ => {
                    match extract_string(&idx_arr, i) {
                        Some(s) => s.trim().parse::<i64>().unwrap_or(0),
                        None => { out.push(Some(value_to_jsonb(&doc)?)); continue; }
                    }
                }
            };
            if let Value::Array(ref mut arr) = doc {
                let len = arr.len() as i64;
                let actual = if idx < 0 { len + idx } else { idx };
                if actual >= 0 && actual < len {
                    arr.remove(actual as usize);
                }
            }
            out.push(Some(value_to_jsonb(&doc)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// json_to_record / jsonb_to_record / json_to_recordset / jsonb_to_recordset
// (scalar stubs — SRF table functions require DataFusion TableProvider)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonToRecordStubUdf {
    signature: Signature,
    name: &'static str,
}

impl ScalarUDFImpl for JsonToRecordStubUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }

    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "{} requires DataFusion TableProvider (SRF); \
             deferred to v0.2 — use jsonb_each() or parse keys manually",
            self.name
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers added for JSON path / operator UDFs
// ---------------------------------------------------------------------------

/// Extract a plain string from a StringArray or LargeBinaryArray (as UTF-8).
fn extract_string(arr: &ArrayRef, i: usize) -> Option<String> {
    if arr.is_null(i) { return None; }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>()?;
            Some(a.value(i).to_string())
        }
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            String::from_utf8(a.value(i).to_vec()).ok()
        }
        _ => None,
    }
}

/// Parse a comma/space-separated key list (possibly `{k1,k2}` PG array literal).
fn parse_key_list(s: &str) -> Vec<String> {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        s[1..s.len() - 1]
            .split(',')
            .map(|k| k.trim().trim_matches('"').to_string())
            .filter(|k| !k.is_empty())
            .collect()
    } else {
        s.split(',')
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Table-valued UDFs (UDTFs) — for FROM jsonb_each(...) / jsonb_array_elements()
// ---------------------------------------------------------------------------
//
// DataFusion's UDTF machinery calls `TableFunctionImpl::call()` at *plan time*
// with the argument `Expr` list.  For literal JSON arguments we materialise
// the rows eagerly into a MemTable; for column references we return an error
// (SRF over columns requires proper LATERAL support which is a v0.2 item).
//
// `jsonb_each(jsonb)       -> SETOF (key TEXT, value TEXT)`
// `jsonb_each_text(jsonb)  -> SETOF (key TEXT, value TEXT)`
// `jsonb_array_elements(jsonb)      -> SETOF (value TEXT)`
// `jsonb_array_elements_text(jsonb) -> SETOF (value TEXT)`
// `jsonb_object_keys(jsonb)         -> SETOF TEXT`

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::catalog::Session;

/// Extract JSON text from a literal Expr (Utf8 or LargeBinary).
fn json_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeBinary(Some(b))) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

// ── jsonb_each / jsonb_each_text ────────────────────────────────────────────

#[derive(Debug)]
struct JsonbEachTf {
    text_values: bool,
}

#[derive(Debug)]
struct JsonbEachTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for JsonbEachTable {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?.scan(_state, _projection, _filters, _limit).await
    }
}

impl TableFunctionImpl for JsonbEachTf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_each requires exactly 1 argument");
        }
        let json_str = match json_from_expr(&args[0]) {
            Some(s) => s,
            None => return plan_err!("jsonb_each: argument must be a JSON literal"),
        };
        let v: Value = serde_json::from_str(&json_str)
            .map_err(|e| DataFusionError::Plan(format!("jsonb_each: JSON parse error: {e}")))?;
        let obj = match v {
            Value::Object(m) => m,
            _ => return plan_err!("jsonb_each: argument must be a JSON object"),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let mut keys: Vec<String> = Vec::with_capacity(obj.len());
        let mut vals: Vec<String> = Vec::with_capacity(obj.len());
        for (k, v) in &obj {
            keys.push(k.clone());
            vals.push(if self.text_values {
                match v {
                    Value::String(s) => s.clone(),
                    _ => serde_json::to_string(v).unwrap_or_default(),
                }
            } else {
                serde_json::to_string(v).unwrap_or_default()
            });
        }
        let key_arr: ArrayRef = Arc::new(StringArray::from(keys));
        let val_arr: ArrayRef = Arc::new(StringArray::from(vals));
        let batch = RecordBatch::try_new(schema.clone(), vec![key_arr, val_arr])
            .map_err(|e| DataFusionError::Plan(format!("jsonb_each: RecordBatch error: {e}")))?;
        Ok(Arc::new(JsonbEachTable { schema, batches: vec![batch] }))
    }
}

// ── jsonb_array_elements / jsonb_array_elements_text ────────────────────────

#[derive(Debug)]
struct JsonbArrayElementsTf {
    text_values: bool,
}

#[derive(Debug)]
struct JsonbArrayElementsTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for JsonbArrayElementsTable {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?.scan(_state, _projection, _filters, _limit).await
    }
}

impl TableFunctionImpl for JsonbArrayElementsTf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_array_elements requires exactly 1 argument");
        }
        let json_str = match json_from_expr(&args[0]) {
            Some(s) => s,
            None => return plan_err!("jsonb_array_elements: argument must be a JSON literal"),
        };
        let v: Value = serde_json::from_str(&json_str)
            .map_err(|e| DataFusionError::Plan(format!("jsonb_array_elements: JSON parse error: {e}")))?;
        let arr = match v {
            Value::Array(a) => a,
            _ => return plan_err!("jsonb_array_elements: argument must be a JSON array"),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        let vals: Vec<String> = arr.iter().map(|v| {
            if self.text_values {
                match v {
                    Value::String(s) => s.clone(),
                    _ => serde_json::to_string(v).unwrap_or_default(),
                }
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        }).collect();

        let val_arr: ArrayRef = Arc::new(StringArray::from(vals));
        let batch = RecordBatch::try_new(schema.clone(), vec![val_arr])
            .map_err(|e| DataFusionError::Plan(format!("jsonb_array_elements: RecordBatch error: {e}")))?;
        Ok(Arc::new(JsonbArrayElementsTable { schema, batches: vec![batch] }))
    }
}

// ── jsonb_object_keys (UDTF variant) ────────────────────────────────────────

#[derive(Debug)]
struct JsonbObjectKeysTf {}

#[derive(Debug)]
struct JsonbObjectKeysTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for JsonbObjectKeysTable {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?.scan(_state, _projection, _filters, _limit).await
    }
}

impl TableFunctionImpl for JsonbObjectKeysTf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_object_keys requires exactly 1 argument");
        }
        let json_str = match json_from_expr(&args[0]) {
            Some(s) => s,
            None => return plan_err!("jsonb_object_keys: argument must be a JSON literal"),
        };
        let v: Value = serde_json::from_str(&json_str)
            .map_err(|e| DataFusionError::Plan(format!("jsonb_object_keys: JSON parse error: {e}")))?;
        let obj = match v {
            Value::Object(m) => m,
            _ => return plan_err!("jsonb_object_keys: argument must be a JSON object"),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("jsonb_object_keys", DataType::Utf8, false),
        ]));

        let keys: Vec<String> = obj.keys().cloned().collect();
        let key_arr: ArrayRef = Arc::new(StringArray::from(keys));
        let batch = RecordBatch::try_new(schema.clone(), vec![key_arr])
            .map_err(|e| DataFusionError::Plan(format!("jsonb_object_keys: RecordBatch error: {e}")))?;
        Ok(Arc::new(JsonbObjectKeysTable { schema, batches: vec![batch] }))
    }
}
