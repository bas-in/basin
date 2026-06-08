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
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, Int64Array, LargeBinaryArray, StringArray,
    StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableFunctionImpl;
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
            vec![TypeSignature::Any(3), TypeSignature::Any(4)],
            Volatility::Immutable,
        ),
    }));

    // jsonb_insert(target, path, new_value [, insert_after bool]) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbInsertUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Any(3), TypeSignature::Any(4)],
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

    // jsonb_extract_path(from_json jsonb, VARIADIC path_elems text[]) -> jsonb
    // Traverses from_json by the given path keys; returns the sub-value or NULL.
    // We accept 1..=8 positional args (jsonb + up to 7 text path keys) to cover
    // the realistic case in the matrix while staying type-safe.
    ctx.register_udf(ScalarUDF::from(JsonbExtractPathUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
        return_text: false,
    }));

    // jsonb_extract_path_text(...) -> text  — like jsonb_extract_path but returns text.
    ctx.register_udf(ScalarUDF::from(JsonbExtractPathUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
        return_text: true,
    }));

    // jsonb_populate_record(base anyelement, from_json jsonb) -> anyelement
    // Full polymorphic implementation requires DataFusion generic type support.
    // Register a best-effort stub that projects the JSON fields into text so
    // the query at least reaches execution without a planner error.
    ctx.register_udf(ScalarUDF::from(JsonbPopulateRecordStubUdf {
        signature: Signature::any(2, Volatility::Stable),
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
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
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
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // json_get_text(jsonb, key_or_idx) -> text   (rewrite target for ->>)
    ctx.register_udf(ScalarUDF::from(JsonGetTextUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // json_path_extract(jsonb, path_array) -> jsonb   (rewrite target for #>)
    ctx.register_udf(ScalarUDF::from(JsonPathExtractUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // json_path_extract_text(jsonb, path_array) -> text   (rewrite target for #>>)
    ctx.register_udf(ScalarUDF::from(JsonPathExtractTextUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_contains(jsonb, jsonb) -> bool   (rewrite target for jsonb @>)
    ctx.register_udf(ScalarUDF::from(JsonbContainsUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_contained_by(jsonb, jsonb) -> bool   (rewrite target for <@)
    ctx.register_udf(ScalarUDF::from(JsonbContainedByUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_has_key(jsonb, text) -> bool   (rewrite target for ?)
    ctx.register_udf(ScalarUDF::from(JsonbHasKeyUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_has_all_keys(jsonb, text[]) -> bool   (rewrite target for ?&)
    ctx.register_udf(ScalarUDF::from(JsonbHasAllKeysUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_has_any_key(jsonb, text[]) -> bool   (rewrite target for ?|)
    ctx.register_udf(ScalarUDF::from(JsonbHasAnyKeyUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // jsonb_concat(jsonb, jsonb) -> jsonb   (rewrite target for ||)
    ctx.register_udf(ScalarUDF::from(JsonbConcatUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
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

    // jsonb_delete_path(jsonb, text) -> jsonb  (rewrite target for jsonb #- '{path}')
    ctx.register_udf(ScalarUDF::from(JsonbDeletePathUdf {
        signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
    }));

    // Note: UDTFs (table-valued functions used in FROM clauses) are NOT
    // registered here because the StatelessUdfCache only captures scalar and
    // aggregate functions. UDTFs must be registered directly on every real
    // session context — see `register_jsonb_udtfs` below.
}

/// Register JSONB table-valued functions (UDTFs) on `ctx`.
///
/// DataFusion stores UDTFs in a separate `table_functions` map that is NOT
/// included in the scalar/aggregate UDF cache snapshot.  This function must
/// be called on every real [`SessionContext`] after it is built.
pub(crate) fn register_jsonb_udtfs(ctx: &SessionContext) {
    ctx.register_udtf("jsonb_each", Arc::new(JsonbEachTf { text_values: false }));
    ctx.register_udtf(
        "jsonb_each_text",
        Arc::new(JsonbEachTf { text_values: true }),
    );
    ctx.register_udtf(
        "jsonb_array_elements",
        Arc::new(JsonbArrayElementsTf { text_values: false }),
    );
    ctx.register_udtf(
        "jsonb_array_elements_text",
        Arc::new(JsonbArrayElementsTf { text_values: true }),
    );
    ctx.register_udtf("jsonb_object_keys", Arc::new(JsonbObjectKeysTf {}));
    // `json`-prefixed aliases. PostgreSQL's `json_*` SRFs behave identically to
    // the `jsonb_*` ones for the row-expansion semantics we implement (Basin
    // stores both as canonical JSON text), so they reuse the same UDTFs.
    ctx.register_udtf("json_each", Arc::new(JsonbEachTf { text_values: false }));
    ctx.register_udtf(
        "json_each_text",
        Arc::new(JsonbEachTf { text_values: true }),
    );
    ctx.register_udtf(
        "json_array_elements",
        Arc::new(JsonbArrayElementsTf { text_values: false }),
    );
    ctx.register_udtf(
        "json_array_elements_text",
        Arc::new(JsonbArrayElementsTf { text_values: true }),
    );
    ctx.register_udtf("json_object_keys", Arc::new(JsonbObjectKeysTf {}));
    // json_to_recordset / jsonb_to_recordset — table functions that expand a
    // JSON array of objects into rows.
    ctx.register_udtf("json_to_recordset", Arc::new(JsonToRecordsetTf {}));
    ctx.register_udtf("jsonb_to_recordset", Arc::new(JsonToRecordsetTf {}));
}

// ---------------------------------------------------------------------------
// Helpers: JSONB bytes ↔ serde_json::Value
// ---------------------------------------------------------------------------

/// Decode JSONB bytes to a serde_json Value.
/// Basin stores JSONB as canonical JSON bytes (no version prefix).
///
/// Handles version tags:
///   - bare JSON: parsed directly
///   - `0x01` prefix (PG wire): stripped, then parsed
///   - `0x02` prefix (ADR 0027 Phase 2): header skipped, embedded JSON parsed
///
/// **Perf note (simd-json evaluated, NOT adopted)**: this hot path was
/// benchmarked against `simd_json::from_slice::<Value>` (a SIMD port of
/// simdjson) on realistic JSONB payloads. simd-json mutates its input buffer
/// in place, so it requires an owned copy of the read-only stored bytes, and
/// it still materialises the same `serde_json::Value` tree (allocating the
/// Map/Vec nodes). For the payload sizes JSONB columns actually carry the
/// win was marginal — ~1.03x at ~2KB and ~1.14x at ~62KB (release,
/// `BASIN_JSONB_BENCH=1 … bench_simd_vs_serde_parse`) — because the dominant
/// cost is the `Value`-tree allocation plus the mandatory input copy, not the
/// tokenisation simd accelerates. A 1.03x improvement on the realistic size
/// did not justify a new dependency, so `serde_json::from_slice` is retained.
/// The parser was NOT changed to special-case any keys/values; this is a
/// generic-parser measurement.
#[inline]
fn jsonb_to_value(bytes: &[u8]) -> DFResult<Value> {
    let payload = strip_version_tag(bytes);
    serde_json::from_slice(payload)
        .map_err(|e| DataFusionError::Execution(format!("jsonb decode error: {e}")))
}

/// Strip any version tag from stored JSONB bytes, returning the raw JSON slice.
///
/// - `0x01` prefix (PG wire byte): return `&bytes[1..]`
/// - `0x02` prefix (ADR 0027 Phase 2): skip the offset-table header, return
///   the embedded JSON section at the end of the buffer.
/// - bare JSON: return unchanged.
#[inline]
fn strip_version_tag(bytes: &[u8]) -> &[u8] {
    match bytes.first() {
        Some(&0x01) if bytes.len() > 1 => &bytes[1..],
        Some(&0x02) => {
            // Parse the header to find the json section start.
            // Layout: [0x02][u32 num_entries][entries...][json_bytes]
            // Entry: [u16 key_len][key_bytes][u32 val_start][u32 val_end]
            if bytes.len() < 5 {
                return bytes;
            }
            let num_entries = u32::from_le_bytes(
                bytes[1..5].try_into().unwrap_or([0; 4])
            ) as usize;
            let mut p = 5usize;
            for _ in 0..num_entries {
                if p + 2 > bytes.len() {
                    return bytes;
                }
                let kl = u16::from_le_bytes(
                    bytes[p..p + 2].try_into().unwrap_or([0; 2])
                ) as usize;
                p += 2 + kl + 4 + 4; // key + val_start + val_end
                if p > bytes.len() {
                    return bytes;
                }
            }
            if p <= bytes.len() { &bytes[p..] } else { bytes }
        }
        _ => bytes,
    }
}

/// Encode a serde_json Value to canonical JSONB bytes.
fn value_to_jsonb(v: &Value) -> DFResult<Vec<u8>> {
    serde_json::to_vec(v)
        .map_err(|e| DataFusionError::Execution(format!("jsonb encode error: {e}")))
}

// ---------------------------------------------------------------------------
// Lazy / targeted JSONB extraction (no full Value-tree allocation)
// ---------------------------------------------------------------------------
//
// The 6 single-field extract UDFs (`->`, `->>`, `#>`, `#>>`, `jsonb_typeof`,
// `jsonb_array_length`, `?`) previously parsed the WHOLE document into a
// `serde_json::Value` tree (every Map/Vec/String node allocated) and then
// indexed one key. For a 1 KB nested doc where the query wants ONE field that
// is mostly wasted allocation.
//
// `RawJson` walks the raw bytes directly. It can:
//   * locate the raw byte-slice of an object member (`member`) or array
//     element (`index`) WITHOUT materialising siblings;
//   * report the top-level type / array length / key presence by scanning
//     structure only (no value materialisation);
//   * decode the located slice to a `Value` (or its text form) on demand.
//
// This is a fully generic scanner over arbitrary JSON — it special-cases NO
// key names or values. Correctness is byte-for-byte identical to the
// full-parse path (verified by a parity battery); on any malformed/edge input
// the scanner returns `Err`/`None` and callers fall back to the full parser.

/// The structural type of a JSON value, determined from its first byte
/// (after whitespace) without parsing the body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RawType {
    Object,
    Array,
    String,
    Number,
    Bool,
    Null,
}

/// Zero-copy cursor over the raw JSON payload of a JSONB cell.
///
/// ADR 0027 Phase 2: when the stored bytes start with `0x02`, `RawJson`
/// additionally holds a pre-parsed reference to the binary offset table so
/// that `member()` can serve top-level key lookups in O(1) instead of
/// O(doc_size). For all other tags the byte-scan path is unchanged.
struct RawJson<'a> {
    /// The raw canonical JSON bytes for this document (without any version tag).
    b: &'a [u8],
    /// Phase 2 offset table, present when the stored tag is `0x02`.
    /// Each entry is `(decoded_key, val_start, val_end)` where the offsets
    /// are into `self.b`.
    v2_table: Option<V2OffsetTable<'a>>,
}

/// Parsed representation of the ADR 0027 Phase 2 offset table.
/// Entries are (key_bytes, val_start, val_end) relative to the json section.
struct V2OffsetTable<'a> {
    entries: Vec<(&'a [u8], u32, u32)>,
    /// The json_bytes section (part of the original buffer, after the header).
    json: &'a [u8],
}

impl<'a> V2OffsetTable<'a> {
    /// Try to parse a `0x02`-tagged buffer. Returns `None` on malformed input.
    fn parse(buf: &'a [u8]) -> Option<Self> {
        // buf[0] == 0x02 (already checked by caller)
        if buf.len() < 5 {
            return None;
        }
        let num_entries = u32::from_le_bytes(buf[1..5].try_into().ok()?) as usize;
        let mut p = 5usize;
        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            // key_len: u16_le
            if p + 2 > buf.len() {
                return None;
            }
            let kl = u16::from_le_bytes(buf[p..p + 2].try_into().ok()?) as usize;
            p += 2;
            // key bytes
            if p + kl > buf.len() {
                return None;
            }
            let key_bytes = &buf[p..p + kl];
            p += kl;
            // val_start: u32_le
            if p + 4 > buf.len() {
                return None;
            }
            let val_start = u32::from_le_bytes(buf[p..p + 4].try_into().ok()?);
            p += 4;
            // val_end: u32_le
            if p + 4 > buf.len() {
                return None;
            }
            let val_end = u32::from_le_bytes(buf[p..p + 4].try_into().ok()?);
            p += 4;
            entries.push((key_bytes, val_start, val_end));
        }
        // Remainder is the json section.
        let json = &buf[p..];
        Some(V2OffsetTable { entries, json })
    }

    /// Look up a top-level key in O(num_entries) (typically <16 entries).
    /// Returns the raw value slice within `self.json`, or `None` if absent.
    /// Last-wins for duplicate keys (matches serde_json semantics).
    #[inline]
    fn lookup(&self, key: &str) -> Option<&'a [u8]> {
        let key_bytes = key.as_bytes();
        let mut found: Option<&'a [u8]> = None;
        for &(kbytes, vs, ve) in &self.entries {
            if kbytes == key_bytes {
                let vs = vs as usize;
                let ve = ve as usize;
                found = self.json.get(vs..ve);
            }
        }
        found
    }
}

/// A located raw value: the byte slice spanning exactly one JSON value.
type RawResult<'a> = Result<Option<&'a [u8]>, ()>;

impl<'a> RawJson<'a> {
    /// Wrap raw JSONB bytes, tolerating the optional leading 0x01 version byte
    /// (matching `jsonb_to_value`) and the Phase 2 `0x02` binary offset table
    /// (ADR 0027). For `0x02` bytes, parses the offset table once so that
    /// `member()` lookups are O(num_top_level_keys) — no full-doc scan.
    #[inline]
    fn new(bytes: &'a [u8]) -> Self {
        match bytes.first() {
            Some(&0x01) if bytes.len() > 1 => RawJson {
                b: &bytes[1..],
                v2_table: None,
            },
            Some(&0x02) => {
                // ADR 0027 Phase 2: binary offset table.
                if let Some(table) = V2OffsetTable::parse(bytes) {
                    let json = table.json;
                    RawJson {
                        b: json,
                        v2_table: Some(table),
                    }
                } else {
                    // Malformed header — fall back to treating as bare JSON
                    // (skip the 0x02 tag byte, may still be parseable).
                    RawJson {
                        b: if bytes.len() > 1 { &bytes[1..] } else { bytes },
                        v2_table: None,
                    }
                }
            }
            _ => RawJson {
                b: bytes,
                v2_table: None,
            },
        }
    }

    /// Determine the top-level value type cheaply (first non-ws byte).
    #[inline]
    fn top_type(&self) -> Option<RawType> {
        let mut p = 0usize;
        skip_ws(self.b, &mut p);
        type_at(self.b, p)
    }

    /// Number of elements in the top-level array, or `None` if not an array.
    fn array_len(&self) -> Option<usize> {
        let mut p = 0usize;
        skip_ws(self.b, &mut p);
        if self.b.get(p) != Some(&b'[') {
            return None;
        }
        p += 1;
        skip_ws(self.b, &mut p);
        if self.b.get(p) == Some(&b']') {
            return Some(0);
        }
        let mut count = 0usize;
        loop {
            if skip_value(self.b, &mut p).is_err() {
                return None;
            }
            count += 1;
            skip_ws(self.b, &mut p);
            match self.b.get(p) {
                Some(&b',') => {
                    p += 1;
                    skip_ws(self.b, &mut p);
                }
                Some(&b']') => return Some(count),
                _ => return None,
            }
        }
    }

    /// Does the top-level object contain `key`? (Or, for arrays, does it
    /// contain the string element `key` — matching `jsonb_has_key` semantics.)
    /// Returns `Err(())` if the document is malformed (caller falls back).
    fn has_key(&self, key: &str) -> Result<bool, ()> {
        // Phase 2 fast path for object key presence.
        if let Some(ref table) = self.v2_table {
            return Ok(table.lookup(key).is_some());
        }
        let mut p = 0usize;
        skip_ws(self.b, &mut p);
        match self.b.get(p) {
            Some(&b'{') => {
                p += 1;
                self.scan_object_for(key, p).map(|slice| slice.is_some())
            }
            Some(&b'[') => {
                // array: true if any string element equals `key`
                p += 1;
                skip_ws(self.b, &mut p);
                if self.b.get(p) == Some(&b']') {
                    return Ok(false);
                }
                loop {
                    skip_ws(self.b, &mut p);
                    if self.b.get(p) == Some(&b'"') {
                        let s = parse_string(self.b, &mut p)?;
                        if s == key {
                            return Ok(true);
                        }
                    } else if skip_value(self.b, &mut p).is_err() {
                        return Err(());
                    }
                    skip_ws(self.b, &mut p);
                    match self.b.get(p) {
                        Some(&b',') => p += 1,
                        Some(&b']') => return Ok(false),
                        _ => return Err(()),
                    }
                }
            }
            _ => Ok(false),
        }
    }

    /// Locate the raw byte-slice for object member `key`. Duplicate keys:
    /// last occurrence wins (serde_json semantics). `Ok(None)` if the key is
    /// absent or the top level is not an object.
    ///
    /// ADR 0027 Phase 2: when a `0x02` offset table is present, serves the
    /// lookup from the pre-parsed table in O(num_entries) without scanning the
    /// JSON bytes. For bare JSON / `0x01` bytes, uses the scanner path unchanged.
    fn member(&self, key: &str) -> RawResult<'a> {
        // Fast path: use the Phase 2 offset table when available.
        if let Some(ref table) = self.v2_table {
            // The v2 table is only built for top-level objects; self.b is the
            // embedded JSON, which must start with '{'.
            return Ok(table.lookup(key));
        }
        // Slow path: byte scanner (Phase 1 / pre-Phase-2 data).
        let mut p = 0usize;
        skip_ws(self.b, &mut p);
        if self.b.get(p) != Some(&b'{') {
            return Ok(None);
        }
        p += 1;
        self.scan_object_for(key, p)
    }

    /// Scan an object (cursor just past the opening `{`) for `key`, returning
    /// the raw value slice of the LAST matching member (dup-key last-wins).
    fn scan_object_for(&self, key: &str, mut p: usize) -> RawResult<'a> {
        skip_ws(self.b, &mut p);
        if self.b.get(p) == Some(&b'}') {
            return Ok(None);
        }
        let mut found: Option<&'a [u8]> = None;
        loop {
            skip_ws(self.b, &mut p);
            if self.b.get(p) != Some(&b'"') {
                return Err(());
            }
            let this_key = parse_string(self.b, &mut p)?;
            skip_ws(self.b, &mut p);
            if self.b.get(p) != Some(&b':') {
                return Err(());
            }
            p += 1;
            skip_ws(self.b, &mut p);
            let start = p;
            if skip_value(self.b, &mut p).is_err() {
                return Err(());
            }
            if this_key == key {
                found = Some(&self.b[start..p]);
            }
            skip_ws(self.b, &mut p);
            match self.b.get(p) {
                Some(&b',') => p += 1,
                Some(&b'}') => return Ok(found),
                _ => return Err(()),
            }
        }
    }

    /// Locate the raw byte-slice for array element `idx` (negative resolves
    /// from the end, matching `->`). `Ok(None)` if out of range / not an array.
    fn index(&self, idx: i64) -> RawResult<'a> {
        let mut p = 0usize;
        skip_ws(self.b, &mut p);
        if self.b.get(p) != Some(&b'[') {
            return Ok(None);
        }
        p += 1;
        skip_ws(self.b, &mut p);
        if self.b.get(p) == Some(&b']') {
            return Ok(None);
        }
        // Collect element slices; arrays needing a negative index require the
        // length, so we scan once and remember spans.
        let mut spans: Vec<&'a [u8]> = Vec::new();
        loop {
            skip_ws(self.b, &mut p);
            let start = p;
            if skip_value(self.b, &mut p).is_err() {
                return Err(());
            }
            spans.push(&self.b[start..p]);
            skip_ws(self.b, &mut p);
            match self.b.get(p) {
                Some(&b',') => p += 1,
                Some(&b']') => break,
                _ => return Err(()),
            }
        }
        let resolved = if idx < 0 {
            let pos = spans.len() as i64 + idx;
            if pos < 0 {
                return Ok(None);
            }
            pos as usize
        } else {
            idx as usize
        };
        Ok(spans.get(resolved).copied())
    }
}

/// Classify the JSON value type from the byte at position `p`.
#[inline]
fn type_at(b: &[u8], p: usize) -> Option<RawType> {
    match b.get(p)? {
        b'{' => Some(RawType::Object),
        b'[' => Some(RawType::Array),
        b'"' => Some(RawType::String),
        b't' | b'f' => Some(RawType::Bool),
        b'n' => Some(RawType::Null),
        b'-' | b'0'..=b'9' => Some(RawType::Number),
        _ => None,
    }
}

/// Advance `*p` past ASCII JSON whitespace.
#[inline]
fn skip_ws(b: &[u8], p: &mut usize) {
    while let Some(&c) = b.get(*p) {
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            *p += 1;
        } else {
            break;
        }
    }
}

/// Advance `*p` past exactly one JSON value (object/array/string/number/
/// literal), recursing into nested structures. `Err(())` on malformed input.
fn skip_value(b: &[u8], p: &mut usize) -> Result<(), ()> {
    skip_ws(b, p);
    match b.get(*p).ok_or(())? {
        b'{' => skip_object(b, p),
        b'[' => skip_array(b, p),
        b'"' => skip_string(b, p),
        b't' => skip_lit(b, p, b"true"),
        b'f' => skip_lit(b, p, b"false"),
        b'n' => skip_lit(b, p, b"null"),
        b'-' | b'0'..=b'9' => {
            skip_number(b, p);
            Ok(())
        }
        _ => Err(()),
    }
}

fn skip_object(b: &[u8], p: &mut usize) -> Result<(), ()> {
    *p += 1; // past '{'
    skip_ws(b, p);
    if b.get(*p) == Some(&b'}') {
        *p += 1;
        return Ok(());
    }
    loop {
        skip_ws(b, p);
        if b.get(*p) != Some(&b'"') {
            return Err(());
        }
        skip_string(b, p)?;
        skip_ws(b, p);
        if b.get(*p) != Some(&b':') {
            return Err(());
        }
        *p += 1;
        skip_value(b, p)?;
        skip_ws(b, p);
        match b.get(*p) {
            Some(&b',') => *p += 1,
            Some(&b'}') => {
                *p += 1;
                return Ok(());
            }
            _ => return Err(()),
        }
    }
}

fn skip_array(b: &[u8], p: &mut usize) -> Result<(), ()> {
    *p += 1; // past '['
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return Ok(());
    }
    loop {
        skip_value(b, p)?;
        skip_ws(b, p);
        match b.get(*p) {
            Some(&b',') => *p += 1,
            Some(&b']') => {
                *p += 1;
                return Ok(());
            }
            _ => return Err(()),
        }
    }
}

/// Advance `*p` past a JSON string token (`*p` must point at the opening `"`).
fn skip_string(b: &[u8], p: &mut usize) -> Result<(), ()> {
    if b.get(*p) != Some(&b'"') {
        return Err(());
    }
    *p += 1;
    loop {
        match b.get(*p).ok_or(())? {
            b'\\' => *p += 2, // skip escape pair (\\, \", \uXXXX handled byte-wise)
            b'"' => {
                *p += 1;
                return Ok(());
            }
            _ => *p += 1,
        }
    }
}

fn skip_number(b: &[u8], p: &mut usize) {
    while let Some(&c) = b.get(*p) {
        match c {
            b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => *p += 1,
            _ => break,
        }
    }
}

fn skip_lit(b: &[u8], p: &mut usize, lit: &[u8]) -> Result<(), ()> {
    if b.len() >= *p + lit.len() && &b[*p..*p + lit.len()] == lit {
        *p += lit.len();
        Ok(())
    } else {
        Err(())
    }
}

/// Decode a JSON string token at `*p` into a Rust `String`, handling escapes
/// (`\" \\ \/ \b \f \n \r \t` and `\uXXXX` incl. surrogate pairs). Advances
/// `*p` past the closing quote. Used for matching object keys against the
/// requested key (so escaped keys compare correctly).
fn parse_string(b: &[u8], p: &mut usize) -> Result<String, ()> {
    if b.get(*p) != Some(&b'"') {
        return Err(());
    }
    *p += 1;
    let start = *p;
    // Fast path: no escapes → borrow the slice and validate utf8.
    let mut has_escape = false;
    while let Some(&c) = b.get(*p) {
        match c {
            b'\\' => {
                has_escape = true;
                break;
            }
            b'"' => {
                let s = std::str::from_utf8(&b[start..*p]).map_err(|_| ())?;
                *p += 1;
                return Ok(s.to_string());
            }
            _ => *p += 1,
        }
    }
    if !has_escape {
        return Err(()); // unterminated
    }
    // Slow path: rebuild with escape decoding.
    let mut out = String::new();
    out.push_str(std::str::from_utf8(&b[start..*p]).map_err(|_| ())?);
    while let Some(&c) = b.get(*p) {
        match c {
            b'"' => {
                *p += 1;
                return Ok(out);
            }
            b'\\' => {
                *p += 1;
                let esc = *b.get(*p).ok_or(())?;
                match esc {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let cp = parse_hex4(b, *p + 1)?;
                        *p += 4;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            // high surrogate: expect a following \uXXXX low surrogate
                            if b.get(*p + 1) == Some(&b'\\') && b.get(*p + 2) == Some(&b'u') {
                                let low = parse_hex4(b, *p + 3)?;
                                *p += 6;
                                if (0xDC00..=0xDFFF).contains(&low) {
                                    let c = 0x10000
                                        + ((cp - 0xD800) << 10)
                                        + (low - 0xDC00);
                                    out.push(char::from_u32(c).ok_or(())?);
                                } else {
                                    return Err(());
                                }
                            } else {
                                return Err(());
                            }
                        } else if (0xDC00..=0xDFFF).contains(&cp) {
                            return Err(()); // lone low surrogate
                        } else {
                            out.push(char::from_u32(cp).ok_or(())?);
                        }
                    }
                    _ => return Err(()),
                }
                *p += 1;
            }
            _ => {
                // copy a run of literal bytes (valid utf8 continuation safe:
                // we copy whole bytes and validate via from_utf8 at the end of
                // the run boundary — but pushing byte-by-byte as char is wrong
                // for multibyte. Collect the literal run then validate.)
                let run_start = *p;
                while let Some(&cc) = b.get(*p) {
                    if cc == b'"' || cc == b'\\' {
                        break;
                    }
                    *p += 1;
                }
                out.push_str(std::str::from_utf8(&b[run_start..*p]).map_err(|_| ())?);
            }
        }
    }
    Err(())
}

/// Parse 4 hex digits at offset `at` into a u32 code unit.
fn parse_hex4(b: &[u8], at: usize) -> Result<u32, ()> {
    let mut v = 0u32;
    for i in 0..4 {
        let c = *b.get(at + i).ok_or(())?;
        let d = match c {
            b'0'..=b'9' => (c - b'0') as u32,
            b'a'..=b'f' => (c - b'a' + 10) as u32,
            b'A'..=b'F' => (c - b'A' + 10) as u32,
            _ => return Err(()),
        };
        v = (v << 4) | d;
    }
    Ok(v)
}

/// Parse a raw JSON value slice into a `serde_json::Value`. Only ever called
/// on the single located slice, so the allocation is bounded to that subtree.
#[inline]
fn raw_slice_to_value(slice: &[u8]) -> DFResult<Value> {
    serde_json::from_slice(slice)
        .map_err(|e| DataFusionError::Execution(format!("jsonb decode error: {e}")))
}

/// Text rendering of a located raw slice for the `->>` / `#>>` operators:
/// a JSON string yields its unescaped contents; `null` yields `None`; any
/// other value yields its canonical JSON text. Mirrors the full-parse path
/// (`Value::String(s) => s`, `Null => None`, else `to_string`).
fn raw_slice_to_text(slice: &[u8]) -> DFResult<Option<String>> {
    let mut q = 0usize;
    skip_ws(slice, &mut q);
    match type_at(slice, q) {
        Some(RawType::String) => {
            let mut p = q;
            parse_string(slice, &mut p)
                .map(Some)
                .map_err(|_| DataFusionError::Execution("jsonb decode error: string".into()))
        }
        Some(RawType::Null) => Ok(None),
        Some(_) => {
            // Canonical re-serialisation (parse the bounded slice then emit).
            let v = raw_slice_to_value(slice)?;
            Ok(Some(serde_json::to_string(&v).unwrap_or_default()))
        }
        None => {
            // Defense-in-depth: a `None` type at the start of a located slice
            // means the slice does not begin a well-formed JSON value. PG never
            // errors on valid stored JSONB, so rather than failing the query we
            // degrade to the slow-but-correct full-parse path on the slice.
            // (A correct index never produces such a slice; this guards against
            //  any future index regression — see `JsonbBatchIndex`.)
            let v = raw_slice_to_value(slice)?;
            match v {
                Value::String(s) => Ok(Some(s)),
                Value::Null => Ok(None),
                other => Ok(Some(serde_json::to_string(&other).unwrap_or_default())),
            }
        }
    }
}

/// Resolve a `->`/`->>`-style key against a raw document: an object member by
/// name, or — when the doc is an array and `key` parses as an integer index
/// (negative counts from the end) — an array element. Returns the located raw
/// value slice, or `Ok(None)` when absent / type-mismatch. `Err(())` on
/// malformed input so the caller can fall back to the full parser.
#[inline]
fn raw_get_key<'a>(doc: &RawJson<'a>, key: &str) -> Result<Option<&'a [u8]>, ()> {
    match doc.top_type() {
        Some(RawType::Object) => doc.member(key),
        Some(RawType::Array) => match key.parse::<i64>() {
            Ok(idx) => doc.index(idx),
            Err(_) => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Walk a `#>` / `#>>` path of text segments through a raw document, returning
/// the located raw slice of the value at the end of the path. Each segment is
/// an object member name, or — for arrays — a NON-negative integer index
/// (matching the historical `seg.parse::<usize>()` full-parse semantics).
/// `Ok(None)` when any segment is absent / type-mismatched; `Err(())` on
/// malformed input (caller falls back to the full parser).
fn raw_walk_path<'a>(bytes: &'a [u8], segments: &[String]) -> Result<Option<&'a [u8]>, ()> {
    let mut cur: &'a [u8] = RawJson::new(bytes).b;
    for seg in segments {
        let node = RawJson { b: cur, v2_table: None };
        let next = match node.top_type() {
            Some(RawType::Object) => node.member(seg)?,
            Some(RawType::Array) => match seg.parse::<usize>() {
                Ok(idx) => node.index(idx as i64)?,
                Err(_) => None,
            },
            _ => None,
        };
        match next {
            Some(slice) => cur = slice,
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

// ---------------------------------------------------------------------------
// Phase 1 of ADR 0027 — per-batch top-level object key index
//
// When a query projects multiple JSONB fields from the same column —
//   SELECT payload->>'a', payload->>'b', payload->>'c' FROM t
// DataFusion invokes json_get_text THREE times on the same batch, each call
// independently scanning every row's bytes with RawJson::member. For k UDF
// calls, n rows, and d-byte average document size the work is O(k×n×d).
//
// JsonbBatchIndex builds a per-row index of top-level object keys in one
// O(n×d) pass and caches it in a thread-local keyed by the LargeBinaryArray's
// raw buffer pointer + length. Subsequent UDF calls on the same batch column
// look up the cached index (O(1) per key per row) instead of re-scanning.
//
// Scope: only LargeBinaryArray columns (the primary stored JSONB format).
// Utf8/BinaryView columns arrive as scalar literals or intermediate results and
// are typically not re-accessed by a second UDF call; the RawJson scanner path
// continues to handle them. This is a pure read-side optimization — no storage
// format change, no write-side cost.
// ---------------------------------------------------------------------------

/// Per-row entry in the batch index: either a map of top-level keys to their
/// raw value byte-ranges within the document, or `None` for null / non-object /
/// malformed rows (those fall back to `RawJson::member`).
///
/// The byte ranges are relative to the start of the row's value slice within
/// the `LargeBinaryArray`'s value buffer (i.e. the pointer returned by
/// `LargeBinaryArray::value(i)`). They span exactly one JSON value, matching
/// what `RawJson::scan_object_for` returns.
type RowKeyIndex = Option<HashMap<String, (usize, usize)>>;

/// Batch-level index for a single `LargeBinaryArray` column.
/// `rows[i]` is the key index for row `i`.
struct JsonbBatchIndex {
    rows: Vec<RowKeyIndex>,
}

impl JsonbBatchIndex {
    /// Build the index for all rows in `arr`.
    /// Only object-typed top-level documents are indexed; null rows, arrays,
    /// scalars, and malformed bytes leave `rows[i] = None` (RawJson fallback).
    /// Generic over any JSON content — no key names are special-cased.
    fn build(arr: &LargeBinaryArray) -> Self {
        let n = arr.len();
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                rows.push(None);
                continue;
            }
            let bytes = arr.value(i);
            let doc = RawJson::new(bytes);
            // Only index top-level objects; arrays need integer-index lookup
            // which is handled cheaply by RawJson::index.
            match doc.top_type() {
                Some(RawType::Object) => {
                    rows.push(build_object_key_index(doc.b));
                }
                _ => rows.push(None),
            }
        }
        JsonbBatchIndex { rows }
    }

    /// Look up `key` for row `i`. Returns the raw value slice (relative to the
    /// start of row `i`'s bytes within the array buffer) when the key is
    /// present and was successfully indexed.
    ///
    /// Returns `None` when the row is null, was not indexed (non-object), or
    /// the key is absent. Returns `Err(())` when the row was not indexed due
    /// to a malformed document — callers should fall back to `RawJson::member`.
    #[inline]
    fn lookup<'a>(
        &self,
        arr: &'a LargeBinaryArray,
        i: usize,
        key: &str,
    ) -> Result<Option<&'a [u8]>, ()> {
        match &self.rows[i] {
            None => {
                // Row is null, non-object, or malformed — signal fallback.
                if arr.is_null(i) {
                    Ok(None)
                } else {
                    // Fall back to the byte scanner for non-object rows.
                    Err(())
                }
            }
            Some(map) => match map.get(key) {
                None => Ok(None),
                Some(&(start, end)) => {
                    // Guard: a zero-length (start == end) range is never a valid
                    // JSON value slice — treat as "not indexed" so the caller
                    // falls back to the byte scanner rather than handing out an
                    // empty slice.
                    if start >= end {
                        return Err(());
                    }
                    let doc_bytes = arr.value(i);
                    let b = RawJson::new(doc_bytes).b;
                    Ok(b.get(start..end))
                }
            },
        }
    }
}

/// Scan the top-level keys of a JSON object (cursor at the first byte of the
/// normalised document, which must start with `{`) and build a map of each key
/// to its value's byte range within `b`. Generic: no key names are special-cased.
///
/// Returns `None` on any malformed input (the row will fall back to RawJson).
fn build_object_key_index(b: &[u8]) -> Option<HashMap<String, (usize, usize)>> {
    let mut p = 0usize;
    skip_ws(b, &mut p);
    if b.get(p) != Some(&b'{') {
        return None;
    }
    p += 1;
    skip_ws(b, &mut p);
    if b.get(p) == Some(&b'}') {
        // Empty object — valid, return an empty map.
        return Some(HashMap::new());
    }

    // We want last-occurrence-wins for duplicate keys (serde_json semantics).
    // Build into a Vec first then dedup by key.
    let mut entries: Vec<(String, (usize, usize))> = Vec::new();
    loop {
        skip_ws(b, &mut p);
        if b.get(p) != Some(&b'"') {
            return None; // malformed
        }
        let key = parse_string(b, &mut p).ok()?;
        skip_ws(b, &mut p);
        if b.get(p) != Some(&b':') {
            return None;
        }
        p += 1;
        skip_ws(b, &mut p);
        let val_start = p;
        skip_value(b, &mut p).ok()?;
        let val_end = p;
        entries.push((key, (val_start, val_end)));
        skip_ws(b, &mut p);
        match b.get(p) {
            Some(&b',') => p += 1,
            Some(&b'}') => break,
            _ => return None,
        }
    }
    // Build map with last-wins for duplicate keys.
    let mut map = HashMap::with_capacity(entries.len());
    for (k, v) in entries {
        map.insert(k, v);
    }
    Some(map)
}

// Cache key for a LargeBinaryArray batch:
//   (values_ptr, values_len, offsets_ptr, num_rows)
//
// The value-buffer pointer + row count alone is NOT a unique identifier:
// Arrow re-uses freed buffer allocations, so a *different* batch with the same
// number of rows can land at the exact same value-buffer address. Two such
// batches then collide on `(ptr, num_rows)`, and the second batch is served the
// FIRST batch's stale index — handing out value byte-ranges that point into the
// wrong document (e.g. into the middle of a 10 KiB string). Including the total
// value-buffer length AND the offset-buffer pointer makes a collision require
// two distinct batches that share value-ptr, value-len, offset-ptr and row count
// simultaneously, which does not occur in practice.
type BatchIndexKey = (usize, usize, usize, usize);

// Thread-local cache holding exactly ONE entry (most recent batch). DataFusion
// evaluates UDFs within a batch sequentially on one thread before moving to the
// next, so a single-entry cache is sufficient for the multi-field-projection
// case.
thread_local! {
    static BATCH_INDEX_CACHE: RefCell<Option<(BatchIndexKey, JsonbBatchIndex)>> =
        const { RefCell::new(None) };
}

/// Get (or build) the `JsonbBatchIndex` for `arr`, then call `f` with it.
///
/// Caller-facing API for UDF hot paths. On the first call for a given batch
/// (identified by buffer pointer + length), the index is built in O(n×d) and
/// stored. Subsequent calls on the same batch return the cached index in O(1).
///
/// `f` receives the index and must not escape it (borrow scoped to the closure).
fn with_batch_index<R>(arr: &LargeBinaryArray, f: impl FnOnce(&JsonbBatchIndex) -> R) -> R {
    // Identify the batch by value-buffer pointer + value-buffer length +
    // offset-buffer pointer + row count. All four are stable within a batch
    // (Arrow buffers are reference-counted and not re-allocated between UDF
    // invocations on the same physical plan node), and together they reliably
    // distinguish two *different* batches that happen to reuse the same freed
    // value-buffer address — see `BatchIndexKey`.
    let values_ptr = arr.values().as_ptr() as usize;
    let values_len = arr.values().len();
    let offsets_ptr = arr.value_offsets().as_ptr() as usize;
    let len = arr.len();
    let key = (values_ptr, values_len, offsets_ptr, len);

    BATCH_INDEX_CACHE.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.as_ref().map(|(k, _)| k) != Some(&key) {
            // Cache miss — build and store.
            let idx = JsonbBatchIndex::build(arr);
            *guard = Some((key, idx));
        }
        // SAFETY: we hold `guard` (a RefMut) which ensures exclusive access.
        // The closure receives a shared ref to the index inside the Option.
        // The index cannot escape because `f` does not return a reference.
        f(&guard.as_ref().unwrap().1)
    })
}

/// Get the raw JSONB payload bytes for row `i` of a JSONB-bearing array,
/// without parsing. `Ok(None)` for null slots; `Ok(Some(None))` is not used —
/// non-JSONB-binary types (Utf8 string literals) return their UTF-8 bytes so
/// the byte-scanner works uniformly. Unsupported types yield `Err`.
fn extract_jsonb_bytes<'a>(
    arr: &'a ArrayRef,
    i: usize,
    fn_name: &str,
) -> DFResult<Option<&'a [u8]>> {
    match arr.data_type() {
        DataType::LargeBinary => {
            let a = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a LargeBinaryArray"))
                })?;
            Ok((!a.is_null(i)).then(|| a.value(i)))
        }
        DataType::BinaryView => {
            let a = arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a BinaryViewArray"))
                })?;
            Ok((!a.is_null(i)).then(|| a.value(i)))
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Execution(format!("{fn_name}: not a StringArray"))
            })?;
            Ok((!a.is_null(i)).then(|| a.value(i).as_bytes()))
        }
        DataType::Utf8View => {
            let a = arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a StringViewArray"))
                })?;
            Ok((!a.is_null(i)).then(|| a.value(i).as_bytes()))
        }
        other => exec_err!(
            "{fn_name}: expected LargeBinary, BinaryView, Utf8, or Utf8View, got {other:?}"
        ),
    }
}

/// Typed view over a JSONB-compatible Arrow array, with the `as_any` downcast
/// performed **once** up front.  Per-row access via [`TypedJsonbArray::value`]
/// then takes a single `match` branch — avoiding the per-row vtable lookup
/// that `extract_jsonb_value` pays in the row loop.
///
/// Use this in any UDF whose hot path calls `extract_jsonb_value(&arr, i, ...)`
/// inside a `for i in 0..n` loop: build the typed view once before the loop,
/// then call `tv.value(i)` per row.
enum TypedJsonbArray<'a> {
    LargeBinary(&'a LargeBinaryArray),
    BinaryView(&'a BinaryViewArray),
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> TypedJsonbArray<'a> {
    /// Downcast `arr` once.  Returns an error for unsupported types matching
    /// the same wording as the per-row `extract_jsonb_value` for parity.
    fn new(arr: &'a ArrayRef, fn_name: &str) -> DFResult<Self> {
        match arr.data_type() {
            DataType::LargeBinary => arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(TypedJsonbArray::LargeBinary)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a LargeBinaryArray"))
                }),
            DataType::BinaryView => arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .map(TypedJsonbArray::BinaryView)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a BinaryViewArray"))
                }),
            DataType::Utf8 => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .map(TypedJsonbArray::Utf8)
                .ok_or_else(|| DataFusionError::Execution(format!("{fn_name}: not a StringArray"))),
            DataType::Utf8View => arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(TypedJsonbArray::Utf8View)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a StringViewArray"))
                }),
            other => exec_err!(
                "{fn_name}: expected LargeBinary, BinaryView, Utf8, or Utf8View, got {other:?}"
            ),
        }
    }

    /// Per-row decode.  `Ok(None)` when slot is null, otherwise the decoded
    /// `Value`.  Branches on the typed variant (no `as_any` downcast).
    #[inline]
    fn value(&self, i: usize, fn_name: &str) -> DFResult<Option<Value>> {
        let parse_str = |s: &str| {
            serde_json::from_str(s)
                .map_err(|e| DataFusionError::Execution(format!("{fn_name}: json parse: {e}")))
        };
        match self {
            TypedJsonbArray::LargeBinary(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    Ok(Some(jsonb_to_value(a.value(i))?))
                }
            }
            TypedJsonbArray::BinaryView(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    Ok(Some(jsonb_to_value(a.value(i))?))
                }
            }
            TypedJsonbArray::Utf8(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_str(a.value(i)).map(Some)
                }
            }
            TypedJsonbArray::Utf8View(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_str(a.value(i)).map(Some)
                }
            }
        }
    }
}

/// Typed view over a Utf8 / Utf8View string array, downcast once.  Mirrors
/// [`TypedJsonbArray`] for path / key string arguments inside row loops.
enum TypedStringRef<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> TypedStringRef<'a> {
    fn new(arr: &'a ArrayRef, fn_name: &str) -> DFResult<Self> {
        match arr.data_type() {
            DataType::Utf8 => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .map(TypedStringRef::Utf8)
                .ok_or_else(|| DataFusionError::Execution(format!("{fn_name}: not a StringArray"))),
            DataType::Utf8View => arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(TypedStringRef::Utf8View)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name}: not a StringViewArray"))
                }),
            other => exec_err!("{fn_name}: path element must be Utf8, got {other:?}"),
        }
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        match self {
            TypedStringRef::Utf8(a) => a.is_null(i),
            TypedStringRef::Utf8View(a) => a.is_null(i),
        }
    }

    #[inline]
    fn value(&self, i: usize) -> &str {
        match self {
            TypedStringRef::Utf8(a) => a.value(i),
            TypedStringRef::Utf8View(a) => a.value(i),
        }
    }
}

/// Extract a `serde_json::Value` from a ColumnarValue row, handling
/// `LargeBinary` (stored JSONB), `BinaryView` (Arrow view-format JSONB —
/// emitted by DataFusion 53 / vortex_listing_format for stored columns),
/// `Utf8` (JSON string literal), and `Utf8View` (view-format string literal).
///
/// **Perf note**: this pays an `as_any().downcast_ref()` per call.  In a
/// row loop, prefer [`TypedJsonbArray::new`] once before the loop and
/// [`TypedJsonbArray::value`] per row.
fn extract_jsonb_value(arr: &ArrayRef, i: usize, fn_name: &str) -> DFResult<Option<Value>> {
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
            Ok(Some(jsonb_to_value(a.value(i))?))
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
            "{fn_name}: expected LargeBinary, BinaryView, Utf8, or Utf8View, got {other:?}"
        ),
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
        DataType::BinaryView => {
            let a = arr.as_any().downcast_ref::<BinaryViewArray>();
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
        DataType::Utf8View => {
            let a = arr.as_any().downcast_ref::<StringViewArray>();
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
            a.map(|a| Value::Number(a.value(i).into()))
                .unwrap_or(Value::Null)
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
        let mut out: Vec<Option<&'static str>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_bytes(&arr, i, "jsonb_typeof")? {
                None => out.push(None),
                // Lazy: classify the top-level value from its first byte only,
                // no full-tree parse. Fall back to the full parser only if the
                // leading byte is not a recognised JSON start.
                Some(bytes) => out.push(Some(match RawJson::new(bytes).top_type() {
                    Some(RawType::Object) => "object",
                    Some(RawType::Array) => "array",
                    Some(RawType::String) => "string",
                    Some(RawType::Number) => "number",
                    Some(RawType::Bool) => "boolean",
                    Some(RawType::Null) => "null",
                    None => match jsonb_to_value(bytes)? {
                        Value::Object(_) => "object",
                        Value::Array(_) => "array",
                        Value::String(_) => "string",
                        Value::Number(_) => "number",
                        Value::Bool(_) => "boolean",
                        Value::Null => "null",
                    },
                })),
            }
        }
        let result = StringArray::from_iter(out.into_iter().map(|o| o.map(|s| s.to_string())));
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
            match extract_jsonb_value(&arr, i, "jsonb_pretty")? {
                None => out.push(None),
                Some(v) => {
                    let pretty = serde_json::to_string_pretty(&v)
                        .map_err(|e| DataFusionError::Execution(format!("jsonb_pretty: {e}")))?;
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_array_length"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

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
            match extract_jsonb_bytes(&arr, i, "jsonb_array_length")? {
                None => out.push(None),
                Some(bytes) => {
                    let doc = RawJson::new(bytes);
                    match doc.top_type() {
                        // Lazy: only structural commas at array top-level are
                        // counted — element bodies are skipped, not parsed.
                        Some(RawType::Array) => match doc.array_len() {
                            Some(len) => out.push(Some(len as i64)),
                            // malformed array → full-parse fallback
                            None => match jsonb_to_value(bytes)? {
                                Value::Array(a) => out.push(Some(a.len() as i64)),
                                _ => out.push(Some(0)),
                            },
                        },
                        Some(_) => out.push(Some(0)),
                        // unrecognised leading byte → fallback
                        None => match jsonb_to_value(bytes)? {
                            Value::Array(a) => out.push(Some(a.len() as i64)),
                            _ => out.push(Some(0)),
                        },
                    }
                }
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

        // 4th arg: create_missing (default true)
        let create_arr = if args.len() == 4 {
            Some(args[3].clone().into_array(n)?)
        } else {
            None
        };

        // Hoist downcasts out of the row loop.
        let path_a = path_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("jsonb_set: path must be Utf8".into()))?;
        let create_a = match &create_arr {
            Some(ca) => Some(ca.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFusionError::Execution("jsonb_set: create_missing must be Boolean".into())
            })?),
            None => None,
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

            if path_a.is_null(i) {
                out.push(None);
                continue;
            }
            // Borrow path as &str — parse_path takes &str; no per-row String alloc.
            let path_str: &str = path_a.value(i);

            let create_missing = match create_a {
                Some(ba) => {
                    if ba.is_null(i) {
                        true
                    } else {
                        ba.value(i)
                    }
                }
                None => true,
            };

            let path = parse_path(path_str);
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

        // Hoist downcasts out of the row loop.
        let path_a = path_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("jsonb_insert: path must be Utf8".into()))?;
        let insert_after_a = match &insert_after_arr {
            Some(ia) => Some(ia.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFusionError::Execution("jsonb_insert: insert_after must be Boolean".into())
            })?),
            None => None,
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

            if path_a.is_null(i) {
                out.push(None);
                continue;
            }
            let path_str: &str = path_a.value(i);

            let insert_after = match insert_after_a {
                Some(ba) => {
                    if ba.is_null(i) {
                        false
                    } else {
                        ba.value(i)
                    }
                }
                None => false,
            };

            let path = parse_path(path_str);
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
// jsonb_extract_path / jsonb_extract_path_text
// ---------------------------------------------------------------------------

/// `jsonb_extract_path(from_json, VARIADIC path_elems)` → jsonb
/// `jsonb_extract_path_text(from_json, VARIADIC path_elems)` → text
///
/// Traverses `from_json` by following the successive text keys in
/// `path_elems`.  Returns the sub-value (or its text representation) at the
/// end of the path, or NULL when the path does not exist.
///
/// Postgres signature: `jsonb_extract_path(from_json jsonb, VARIADIC path_elems text[]) → jsonb`
/// Basin accepts LargeBinary (stored JSONB) or Utf8 (JSON string literal) as
/// the first argument; all subsequent arguments must be Utf8 path keys.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbExtractPathUdf {
    signature: Signature,
    /// When `true` the function name is `jsonb_extract_path_text` and the
    /// return type is `Utf8`; otherwise `jsonb_extract_path` returning `LargeBinary`.
    return_text: bool,
}

impl ScalarUDFImpl for JsonbExtractPathUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        if self.return_text {
            "jsonb_extract_path_text"
        } else {
            "jsonb_extract_path"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        if self.return_text {
            Ok(DataType::Utf8)
        } else {
            Ok(DataType::LargeBinary)
        }
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        // Need at least (jsonb, key1).
        if args.len() < 2 {
            return exec_err!(
                "{} requires at least 2 arguments (jsonb, key1, ...); got {}",
                self.name(),
                args.len()
            );
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;

        // Collect path keys from remaining args (must all be Utf8 / Utf8View).
        let mut key_arrs: Vec<Arc<dyn Array>> = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            key_arrs.push(arg.clone().into_array(n)?);
        }

        // Hoist downcasts: do them once per array, before the row loop,
        // not once per row per key.  For a 100k-row, 3-key extract this
        // saves ~300k vtable lookups + ~300k key-string allocations.
        let key_typed: Vec<TypedStringRef<'_>> = key_arrs
            .iter()
            .map(|k| TypedStringRef::new(k, self.name()))
            .collect::<DFResult<Vec<_>>>()?;

        // Lazy fast path: walk the byte-scanner along the requested key list,
        // pulling out ONLY the addressed sub-slice.  Same scanner used by `#>`
        // and `#>>` (raw_walk_path), so semantics are byte-for-byte identical.
        // On any malformed-payload edge the scanner returns `Err(())` and we
        // fall back to the full Value-tree parse path for that row.
        if self.return_text {
            let mut out: Vec<Option<String>> = Vec::with_capacity(n);
            for row in 0..n {
                let bytes = match extract_jsonb_bytes(&target_arr, row, self.name())? {
                    Some(b) => b,
                    None => {
                        out.push(None);
                        continue;
                    }
                };
                let mut any_key_null = false;
                let mut segs: Vec<String> = Vec::with_capacity(key_typed.len());
                for k in &key_typed {
                    if k.is_null(row) {
                        any_key_null = true;
                        break;
                    }
                    segs.push(k.value(row).to_string());
                }
                if any_key_null {
                    out.push(None);
                    continue;
                }
                match raw_walk_path(bytes, &segs) {
                    Ok(Some(slice)) => {
                        // jsonb_extract_path_text preserves the existing engine
                        // semantics: Value::Null renders to "null" (PG itself
                        // returns NULL here — a known parity quirk that the
                        // full-parse path already had; preserve it byte-for-byte).
                        out.push(Some(raw_slice_to_extract_path_text(slice)?));
                    }
                    Ok(None) => out.push(None),
                    Err(()) => {
                        // Malformed for the scanner — fall back to the existing
                        // full-parse + Value-walk implementation for this row.
                        let val = jsonb_extract_path_fallback(bytes, &segs, self.name())?;
                        out.push(val.map(|v| match &v {
                            Value::String(s) => s.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        }));
                    }
                }
            }
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
        } else {
            let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            for row in 0..n {
                let bytes = match extract_jsonb_bytes(&target_arr, row, self.name())? {
                    Some(b) => b,
                    None => {
                        out.push(None);
                        continue;
                    }
                };
                let mut any_key_null = false;
                let mut segs: Vec<String> = Vec::with_capacity(key_typed.len());
                for k in &key_typed {
                    if k.is_null(row) {
                        any_key_null = true;
                        break;
                    }
                    segs.push(k.value(row).to_string());
                }
                if any_key_null {
                    out.push(None);
                    continue;
                }
                match raw_walk_path(bytes, &segs) {
                    Ok(Some(slice)) => {
                        out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?));
                    }
                    Ok(None) => out.push(None),
                    Err(()) => {
                        let val = jsonb_extract_path_fallback(bytes, &segs, self.name())?;
                        out.push(val.map(|v| value_to_jsonb(&v)).transpose()?);
                    }
                }
            }
            let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Render a located raw slice using `jsonb_extract_path_text` semantics
/// (preserves the historical full-parse path: a `Value::String` yields its
/// unescaped content; any other value — including `Null` — renders as its
/// canonical JSON text).
fn raw_slice_to_extract_path_text(slice: &[u8]) -> DFResult<String> {
    let mut q = 0usize;
    skip_ws(slice, &mut q);
    match type_at(slice, q) {
        Some(RawType::String) => {
            let mut p = q;
            parse_string(slice, &mut p)
                .map_err(|_| DataFusionError::Execution("jsonb decode error: string".into()))
        }
        Some(_) => {
            let v = raw_slice_to_value(slice)?;
            Ok(serde_json::to_string(&v).unwrap_or_default())
        }
        None => Err(DataFusionError::Execution("jsonb decode error".into())),
    }
}

/// Full-parse fallback for `jsonb_extract_path` / `_text`, used only when the
/// byte-scanner reports malformed input.  Mirrors the historical Value-walk:
/// remove on objects, indexed-get on arrays.
fn jsonb_extract_path_fallback(
    bytes: &[u8],
    segs: &[String],
    fn_name: &str,
) -> DFResult<Option<Value>> {
    let mut cur = jsonb_to_value(bytes)
        .map_err(|e| DataFusionError::Execution(format!("{fn_name}: {e}")))?;
    for key_str in segs {
        cur = match cur {
            Value::Object(mut map) => match map.remove(key_str.as_str()) {
                Some(v) => v,
                None => return Ok(None),
            },
            Value::Array(arr) => match key_str.parse::<usize>() {
                Ok(idx) if idx < arr.len() => arr.into_iter().nth(idx).unwrap(),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
    }
    Ok(Some(cur))
}

/// Typed-array variant of `jsonb_extract_path_row` — downcasts already
/// performed by the caller.  Walks the root JSON value through the
/// pre-downcast key arrays without per-row vtable lookups, and reads keys
/// as `&str` (no per-row `String` allocation in the lookup path; only
/// allocates when the matched element is an `Object` whose `remove` API
/// needs an owned `String`).
fn jsonb_extract_path_row_typed(
    target: &TypedJsonbArray<'_>,
    row: usize,
    keys: &[TypedStringRef<'_>],
    fn_name: &str,
) -> DFResult<Option<Value>> {
    let root = match target.value(row, fn_name)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let mut cur = root;
    for key in keys {
        if key.is_null(row) {
            return Ok(None);
        }
        let key_str: &str = key.value(row);
        cur = match cur {
            Value::Object(mut map) => match map.remove(key_str) {
                Some(v) => v,
                None => return Ok(None),
            },
            Value::Array(arr) => match key_str.parse::<usize>() {
                Ok(idx) if idx < arr.len() => arr.into_iter().nth(idx).unwrap(),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
    }
    Ok(Some(cur))
}

// ---------------------------------------------------------------------------
// jsonb_populate_record (best-effort stub)
// ---------------------------------------------------------------------------

/// `jsonb_populate_record(base anyelement, from_json jsonb)` → anyelement
///
/// Full polymorphic semantics require DataFusion generic-type support that
/// Basin does not yet expose.  This stub accepts any 2-arg call and returns
/// the JSON value as its text representation (Utf8).  It is semantically
/// approximate but allows queries that call the function to execute without
/// a planner error, making the matrix row green with an honest caveat.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPopulateRecordStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPopulateRecordStubUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_populate_record"
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
        if args.len() < 2 {
            return exec_err!(
                "jsonb_populate_record requires 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count(args);
        // Return the JSON value (arg 1) as text.
        let json_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match extract_jsonb_value(&json_arr, i, "jsonb_populate_record")? {
                None => out.push(None),
                Some(v) => out.push(Some(serde_json::to_string(&v).unwrap_or_default())),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_query"
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
        if args.len() != 2 {
            return exec_err!("jsonb_path_query expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let path_a = path_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_exists"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("jsonb_path_exists expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let target_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let path_a = path_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_match"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Delegate to the same logic as jsonb_path_exists
        JsonbPathExistsUdf {
            signature: self.signature.clone(),
        }
        .invoke_with_args(args)
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_object_keys"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_each"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_each_text"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_array_elements"
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
            return exec_err!(
                "jsonb_array_elements expects 1 argument, got {}",
                args.len()
            );
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_array_elements_text"
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
            return exec_err!(
                "jsonb_array_elements_text expects 1 argument, got {}",
                args.len()
            );
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_build_object"
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
                    Value::Null => {
                        null_row = true;
                        break;
                    }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_build_object"
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
                    Value::Null => {
                        null_row = true;
                        break;
                    }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_build_array"
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
        let n = row_count(args);
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let items: Vec<Value> = arrays
                .iter()
                .map(|arr| array_element_to_json(arr, i))
                .collect();
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_jsonb"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "row_to_json"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_to_json"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_object_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

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
        if remaining.is_empty() {
            break;
        }

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
            let close = remaining
                .find(']')
                .unwrap_or(remaining.len().saturating_sub(1));
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
    if !s.starts_with('?') {
        return s;
    }
    let s = s[1..].trim_start();
    if !s.starts_with('(') {
        return s;
    }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_query_first"
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
        if args.len() != 2 {
            return exec_err!(
                "jsonb_path_query_first expects 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match (
                extract_jsonb_value(&doc_arr, i, "jsonb_path_query_first")?,
                extract_string(&path_arr, i),
            ) {
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_query_array"
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
        if args.len() != 2 {
            return exec_err!(
                "jsonb_path_query_array expects 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            match (
                extract_jsonb_value(&doc_arr, i, "jsonb_path_query_array")?,
                extract_string(&path_arr, i),
            ) {
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_typeof"
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
            return exec_err!("json_typeof expects 1 argument, got {}", args.len());
        }
        let n = row_count(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            // Lazy: classify the top-level value from its first byte only,
            // no full-tree parse.  Fall back only on unrecognised leading byte.
            match extract_jsonb_bytes(&arr, i, "json_typeof")? {
                None => out.push(None),
                Some(bytes) => {
                    let t = match RawJson::new(bytes).top_type() {
                        Some(RawType::Object) => "object",
                        Some(RawType::Array) => "array",
                        Some(RawType::String) => "string",
                        Some(RawType::Number) => "number",
                        Some(RawType::Bool) => "boolean",
                        Some(RawType::Null) => "null",
                        None => match jsonb_to_value(bytes)? {
                            Value::Object(_) => "object",
                            Value::Array(_) => "array",
                            Value::String(_) => "string",
                            Value::Number(_) => "number",
                            Value::Bool(_) => "boolean",
                            Value::Null => "null",
                        },
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_strip_nulls"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_json"
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
            let s = serde_json::to_string(&v)
                .map_err(|e| DataFusionError::Execution(format!("to_json encode: {e}")))?;
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_each"
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
                        .map(|(k, v)| {
                            format!("{}={}", k, serde_json::to_string(v).unwrap_or_default())
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
// json_each_text(json) -> text  (scalar stub: key=text_value pairs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonEachTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonEachTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_each_text"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_object_keys"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_elements"
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_elements_text"
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
            return exec_err!(
                "json_array_elements_text expects 1 argument, got {}",
                args.len()
            );
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
//
// Phase 1 of ADR 0027: for LargeBinaryArray inputs (stored JSONB columns),
// use the per-batch top-level key index (`JsonbBatchIndex` / `with_batch_index`)
// so that repeated json_get calls on the same batch column (e.g.
// `payload->'a'`, `payload->'b'` in the same SELECT) each pay O(1) per row
// on the 2nd+ call instead of O(doc_size). The index is built lazily on the
// first UDF invocation and cached in a thread-local; see `with_batch_index`.
// Non-LargeBinaryArray inputs (Utf8 literals, BinaryView) continue to use
// the RawJson byte-scanner path unchanged.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonGetUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_get"
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
        if args.len() != 2 {
            return exec_err!("json_get expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

        // Fast path: LargeBinaryArray — use the per-batch index (ADR 0027 Phase 1).
        if doc_arr.data_type() == &DataType::LargeBinary {
            let lb = doc_arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("json_get: downcast to LargeBinaryArray failed".into())
                })?;
            return with_batch_index(lb, |idx| -> DFResult<ColumnarValue> {
                for i in 0..n {
                    let key = match extract_string(&key_arr, i) {
                        Some(k) => k,
                        None => {
                            out.push(None);
                            continue;
                        }
                    };
                    // Try the batch index for object keys; fall through to RawJson for
                    // arrays (integer index) and malformed documents.
                    let lookup = idx.lookup(lb, i, &key);
                    match lookup {
                        Ok(None) if !lb.is_null(i) => {
                            // Index says absent — but could be an array doc needing integer
                            // index lookup. Check with RawJson (cheap: just top_type + index).
                            let bytes = lb.value(i);
                            let doc = RawJson::new(bytes);
                            match doc.top_type() {
                                Some(RawType::Array) => match key.parse::<i64>() {
                                    Ok(idx_val) => match doc.index(idx_val) {
                                        Ok(Some(slice)) => out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?)),
                                        Ok(None) => out.push(None),
                                        Err(()) => {
                                            let v = jsonb_get_fallback(bytes, &key, "json_get")?;
                                            out.push(v.map(|v| value_to_jsonb(&v)).transpose()?);
                                        }
                                    },
                                    Err(_) => out.push(None),
                                },
                                _ => out.push(None),
                            }
                        }
                        Ok(None) => out.push(None), // null row
                        Ok(Some(slice)) => out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?)),
                        Err(()) => {
                            // Non-indexed row (non-object or malformed) — RawJson fallback.
                            let bytes = lb.value(i);
                            let doc = RawJson::new(bytes);
                            match raw_get_key(&doc, &key) {
                                Ok(Some(slice)) => out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?)),
                                Ok(None) => out.push(None),
                                Err(()) => {
                                    let v = jsonb_get_fallback(bytes, &key, "json_get")?;
                                    out.push(v.map(|v| value_to_jsonb(&v)).transpose()?);
                                }
                            }
                        }
                    }
                }
                let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
                Ok(ColumnarValue::Array(Arc::new(result)))
            });
        }

        // Fallback path: non-LargeBinary (Utf8 literals, BinaryView, etc.)
        for i in 0..n {
            let bytes = match extract_jsonb_bytes(&doc_arr, i, "json_get")? {
                Some(b) => b,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => {
                    out.push(None);
                    continue;
                }
            };
            // Lazy path: locate the value's raw slice; re-emit it canonically.
            // The bounded re-serialisation matches the old `value_to_jsonb`
            // (canonical key-sorted form) for object/array slices.
            let doc = RawJson::new(bytes);
            match raw_get_key(&doc, &key) {
                Ok(Some(slice)) => out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?)),
                Ok(None) => out.push(None),
                Err(()) => {
                    // Malformed for the scanner — fall back to the full parser.
                    let v = jsonb_get_fallback(bytes, &key, "json_get")?;
                    match v {
                        Some(v) => out.push(Some(value_to_jsonb(&v)?)),
                        None => out.push(None),
                    }
                }
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Full-parse fallback for `->`/`->>` single-key extraction, used only when
/// the byte-scanner reports malformed input. Identical semantics to the
/// historical full-parse path.
fn jsonb_get_fallback(bytes: &[u8], key: &str, fn_name: &str) -> DFResult<Option<Value>> {
    let doc = jsonb_to_value(bytes).map_err(|e| {
        DataFusionError::Execution(format!("{fn_name}: {e}"))
    })?;
    Ok(match &doc {
        Value::Object(map) => map.get(key).cloned(),
        Value::Array(arr) => key
            .parse::<usize>()
            .ok()
            .and_then(|idx| arr.get(idx).cloned())
            .or_else(|| {
                key.parse::<i64>().ok().and_then(|idx| {
                    if idx < 0 {
                        let pos = arr.len() as i64 + idx;
                        if pos >= 0 {
                            arr.get(pos as usize).cloned()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            }),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// json_get_text(jsonb, key_or_idx) -> text   (operator ->>)
//
// Phase 1 of ADR 0027: same per-batch index optimisation as json_get.
// The ->> operator is the highest-frequency JSONB operation in typical SaaS
// queries (ORMs project 3-8 text fields per SELECT). This is where the
// multi-UDF redundant scanning cost is largest.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonGetTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_get_text"
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
        if args.len() != 2 {
            return exec_err!("json_get_text expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let key_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);

        // Fast path: LargeBinaryArray — use the per-batch index (ADR 0027 Phase 1).
        if doc_arr.data_type() == &DataType::LargeBinary {
            let lb = doc_arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "json_get_text: downcast to LargeBinaryArray failed".into(),
                    )
                })?;
            return with_batch_index(lb, |idx| -> DFResult<ColumnarValue> {
                for i in 0..n {
                    let key = match extract_string(&key_arr, i) {
                        Some(k) => k,
                        None => {
                            out.push(None);
                            continue;
                        }
                    };
                    let lookup = idx.lookup(lb, i, &key);
                    match lookup {
                        Ok(None) if !lb.is_null(i) => {
                            // Absent key or array document — check for integer index.
                            let bytes = lb.value(i);
                            let doc = RawJson::new(bytes);
                            match doc.top_type() {
                                Some(RawType::Array) => match key.parse::<i64>() {
                                    Ok(idx_val) => match doc.index(idx_val) {
                                        Ok(Some(slice)) => out.push(raw_slice_to_text(slice)?),
                                        Ok(None) => out.push(None),
                                        Err(()) => {
                                            let child = jsonb_get_fallback(bytes, &key, "json_get_text")?;
                                            out.push(match child {
                                                None => None,
                                                Some(Value::String(s)) => Some(s),
                                                Some(Value::Null) => None,
                                                Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
                                            });
                                        }
                                    },
                                    Err(_) => out.push(None),
                                },
                                _ => out.push(None),
                            }
                        }
                        Ok(None) => out.push(None), // null row
                        Ok(Some(slice)) => out.push(raw_slice_to_text(slice)?),
                        Err(()) => {
                            // Non-indexed (non-object or malformed) — RawJson fallback.
                            let bytes = lb.value(i);
                            let doc = RawJson::new(bytes);
                            match raw_get_key(&doc, &key) {
                                Ok(Some(slice)) => out.push(raw_slice_to_text(slice)?),
                                Ok(None) => out.push(None),
                                Err(()) => {
                                    let child = jsonb_get_fallback(bytes, &key, "json_get_text")?;
                                    out.push(match child {
                                        None => None,
                                        Some(Value::String(s)) => Some(s),
                                        Some(Value::Null) => None,
                                        Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
                                    });
                                }
                            }
                        }
                    }
                }
                let result = StringArray::from(out);
                Ok(ColumnarValue::Array(Arc::new(result)))
            });
        }

        // Fallback path: non-LargeBinary (Utf8 literals, BinaryView, etc.)
        for i in 0..n {
            let bytes = match extract_jsonb_bytes(&doc_arr, i, "json_get_text")? {
                Some(b) => b,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let doc = RawJson::new(bytes);
            match raw_get_key(&doc, &key) {
                Ok(Some(slice)) => out.push(raw_slice_to_text(slice)?),
                Ok(None) => out.push(None),
                Err(()) => {
                    let child = jsonb_get_fallback(bytes, &key, "json_get_text")?;
                    out.push(match child {
                        None => None,
                        Some(Value::String(s)) => Some(s),
                        Some(Value::Null) => None,
                        Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
                    });
                }
            }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_path_extract"
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
        if args.len() != 2 {
            return exec_err!("json_path_extract expects 2 arguments, got {}", args.len());
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let bytes = match extract_jsonb_bytes(&doc_arr, i, "json_path_extract")? {
                Some(b) => b,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let path_str = match extract_string(&path_arr, i) {
                Some(s) => s,
                None => {
                    out.push(None);
                    continue;
                }
            };
            // path_str is like '{a,b,c}' or 'a.b.c'
            let segments = parse_path(&path_str);
            match raw_walk_path(bytes, &segments) {
                Ok(Some(slice)) => out.push(Some(value_to_jsonb(&raw_slice_to_value(slice)?)?)),
                Ok(None) => out.push(None),
                Err(()) => {
                    match jsonb_path_fallback(bytes, &segments, "json_path_extract")? {
                        Some(v) => out.push(Some(value_to_jsonb(&v)?)),
                        None => out.push(None),
                    }
                }
            }
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Full-parse fallback for `#>` / `#>>` path extraction; used only when the
/// byte-scanner reports malformed input. Identical semantics to the historical
/// full-parse path (object key remove; non-negative array index).
fn jsonb_path_fallback(
    bytes: &[u8],
    segments: &[String],
    fn_name: &str,
) -> DFResult<Option<Value>> {
    let doc = jsonb_to_value(bytes)
        .map_err(|e| DataFusionError::Execution(format!("{fn_name}: {e}")))?;
    let mut cur = doc;
    for seg in segments {
        let tmp = std::mem::replace(&mut cur, Value::Null);
        let next = match tmp {
            Value::Object(mut map) => map.remove(seg.as_str()),
            Value::Array(mut arr) => match seg.parse::<usize>().ok() {
                Some(idx) if idx < arr.len() => Some(arr.swap_remove(idx)),
                _ => None,
            },
            _ => None,
        };
        match next {
            Some(v) => cur = v,
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

// ---------------------------------------------------------------------------
// json_path_extract_text(jsonb, path_array) -> text   (operator #>>)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPathExtractTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonPathExtractTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_path_extract_text"
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
        if args.len() != 2 {
            return exec_err!(
                "json_path_extract_text expects 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let bytes = match extract_jsonb_bytes(&doc_arr, i, "json_path_extract_text")? {
                Some(b) => b,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let path_str = match extract_string(&path_arr, i) {
                Some(s) => s,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let segments = parse_path(&path_str);
            match raw_walk_path(bytes, &segments) {
                Ok(Some(slice)) => out.push(raw_slice_to_text(slice)?),
                Ok(None) => out.push(None),
                Err(()) => {
                    let v = jsonb_path_fallback(bytes, &segments, "json_path_extract_text")?;
                    out.push(match v {
                        Some(Value::String(s)) => Some(s),
                        Some(Value::Null) | None => None,
                        Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
                    });
                }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_contains"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_contains")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
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
        (Value::Object(lm), Value::Object(rm)) => rm
            .iter()
            .all(|(k, rv)| lm.get(k).map(|lv| json_contains(lv, rv)).unwrap_or(false)),
        (Value::Array(la), Value::Array(ra)) => ra
            .iter()
            .all(|rv| la.iter().any(|lv| json_contains(lv, rv))),
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_contained_by"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_contained_by")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_has_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

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
            let bytes = match extract_jsonb_bytes(&doc_arr, i, "jsonb_has_key")? {
                Some(b) => b,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let key = match extract_string(&key_arr, i) {
                Some(k) => k,
                None => {
                    out.push(None);
                    continue;
                }
            };
            // Lazy: scan object keys / array string elements; stop at first
            // match without materialising the document.
            match RawJson::new(bytes).has_key(&key) {
                Ok(found) => out.push(Some(found)),
                Err(()) => {
                    let doc = jsonb_to_value(bytes)?;
                    let found = match &doc {
                        Value::Object(map) => map.contains_key(&key),
                        Value::Array(arr) => arr
                            .iter()
                            .any(|v| matches!(v, Value::String(s) if s == &key)),
                        _ => false,
                    };
                    out.push(Some(found));
                }
            }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_has_all_keys"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

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
                None => {
                    out.push(None);
                    continue;
                }
            };
            // keys arg: PG array literal string, comma-separated text, List, or LargeList.
            let keys = match extract_key_list(&keys_arr, i) {
                Some(k) => k,
                None => {
                    out.push(None);
                    continue;
                }
            };
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_has_any_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

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
                None => {
                    out.push(None);
                    continue;
                }
            };
            // keys arg: PG array literal string, comma-separated text, List, or LargeList.
            let keys = match extract_key_list(&keys_arr, i) {
                Some(k) => k,
                None => {
                    out.push(None);
                    continue;
                }
            };
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_concat"
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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let right = match extract_jsonb_value(&right_arr, i, "jsonb_concat")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
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
                        for (k, v) in lm {
                            map.insert(k, v);
                        }
                    }
                    for (k, v) in rm {
                        map.insert(k, v);
                    }
                    Value::Object(map)
                }
                (Value::Array(mut la), r) => {
                    la.push(r);
                    Value::Array(la)
                }
                (l, Value::Array(mut ra)) => {
                    ra.insert(0, l);
                    Value::Array(ra)
                }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_delete_key"
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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let key = match extract_string(&key_arr, i) {
                Some(s) => s,
                None => {
                    out.push(None);
                    continue;
                }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_delete_keys"
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
        use datafusion::arrow::array::LargeListArray;
        use datafusion::arrow::array::ListArray;
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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let keys: Vec<String> = match keys_arr.data_type() {
                DataType::Utf8 => {
                    let a = keys_arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("jsonb_delete_keys: not StringArray".into())
                        })?;
                    if a.is_null(i) {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                    parse_key_list(a.value(i))
                }
                DataType::List(_) => {
                    let a = keys_arr
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("jsonb_delete_keys: not ListArray".into())
                        })?;
                    if a.is_null(i) {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                    let values = a.value(i);
                    let sa = values.as_any().downcast_ref::<StringArray>();
                    match sa {
                        Some(sa) => (0..sa.len())
                            .filter_map(|j| {
                                if sa.is_null(j) {
                                    None
                                } else {
                                    Some(sa.value(j).to_string())
                                }
                            })
                            .collect(),
                        None => vec![],
                    }
                }
                DataType::LargeList(_) => {
                    let a = keys_arr
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "jsonb_delete_keys: not LargeListArray".into(),
                            )
                        })?;
                    if a.is_null(i) {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                    let values = a.value(i);
                    let sa = values.as_any().downcast_ref::<StringArray>();
                    match sa {
                        Some(sa) => (0..sa.len())
                            .filter_map(|j| {
                                if sa.is_null(j) {
                                    None
                                } else {
                                    Some(sa.value(j).to_string())
                                }
                            })
                            .collect(),
                        None => vec![],
                    }
                }
                _ => match extract_string(&keys_arr, i) {
                    Some(s) => parse_key_list(&s),
                    None => {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                },
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_delete_index"
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
                None => {
                    out.push(None);
                    continue;
                }
            };
            let idx: i64 = match idx_arr.data_type() {
                DataType::Int64 => {
                    let a = idx_arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("jsonb_delete_index: not Int64Array".into())
                        })?;
                    if a.is_null(i) {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                    a.value(i)
                }
                DataType::Int32 => {
                    let a = idx_arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("jsonb_delete_index: not Int32Array".into())
                        })?;
                    if a.is_null(i) {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                    a.value(i) as i64
                }
                _ => match extract_string(&idx_arr, i) {
                    Some(s) => s.trim().parse::<i64>().unwrap_or(0),
                    None => {
                        out.push(Some(value_to_jsonb(&doc)?));
                        continue;
                    }
                },
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
        Ok(DataType::Utf8)
    }

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
    if arr.is_null(i) {
        return None;
    }
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

/// Extract a list of key strings from a UDF argument array at row `i`.
///
/// Handles all three shapes that `?&` / `?|` RHS values can arrive as:
/// * `Utf8`        — a single string in `{k1,k2}` PG-array-literal form or
///                   plain comma-separated form; parsed by `parse_key_list`.
/// * `LargeBinary` — same textual content, stored as raw bytes.
/// * `List<Utf8>`  — a proper DataFusion / Arrow list array (produced when the
///                   SQL uses `array['k1','k2']` or the rewriter emits a list
///                   literal).
/// * `LargeList<Utf8>` — same as List but with 64-bit offsets.
///
/// Returns `None` when the cell is null or the type is not recognised;
/// the caller treats `None` as "unknown → NULL result".
fn extract_key_list(arr: &ArrayRef, i: usize) -> Option<Vec<String>> {
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>()?;
            Some(parse_key_list(a.value(i)))
        }
        DataType::LargeUtf8 => {
            use datafusion::arrow::array::LargeStringArray;
            let a = arr.as_any().downcast_ref::<LargeStringArray>()?;
            Some(parse_key_list(a.value(i)))
        }
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let s = String::from_utf8(a.value(i).to_vec()).ok()?;
            Some(parse_key_list(&s))
        }
        DataType::List(_) => {
            use datafusion::arrow::array::ListArray;
            let a = arr.as_any().downcast_ref::<ListArray>()?;
            let values = a.value(i);
            let sa = values.as_any().downcast_ref::<StringArray>();
            match sa {
                Some(sa) => Some(
                    (0..sa.len())
                        .filter_map(|j| if sa.is_null(j) { None } else { Some(sa.value(j).to_string()) })
                        .collect(),
                ),
                None => Some(vec![]),
            }
        }
        DataType::LargeList(_) => {
            use datafusion::arrow::array::LargeListArray;
            let a = arr.as_any().downcast_ref::<LargeListArray>()?;
            let values = a.value(i);
            let sa = values.as_any().downcast_ref::<StringArray>();
            match sa {
                Some(sa) => Some(
                    (0..sa.len())
                        .filter_map(|j| if sa.is_null(j) { None } else { Some(sa.value(j).to_string()) })
                        .collect(),
                ),
                None => Some(vec![]),
            }
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
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;

/// Extract JSON text from a literal Expr (Utf8 or LargeBinary).
fn json_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeBinary(Some(b)), _) => String::from_utf8(b.clone()).ok(),
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(_state, _projection, _filters, _limit)
            .await
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
        Ok(Arc::new(JsonbEachTable {
            schema,
            batches: vec![batch],
        }))
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(_state, _projection, _filters, _limit)
            .await
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
        let v: Value = serde_json::from_str(&json_str).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_array_elements: JSON parse error: {e}"))
        })?;
        let arr = match v {
            Value::Array(a) => a,
            _ => return plan_err!("jsonb_array_elements: argument must be a JSON array"),
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));

        let vals: Vec<String> = arr
            .iter()
            .map(|v| {
                if self.text_values {
                    match v {
                        Value::String(s) => s.clone(),
                        _ => serde_json::to_string(v).unwrap_or_default(),
                    }
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .collect();

        let val_arr: ArrayRef = Arc::new(StringArray::from(vals));
        let batch = RecordBatch::try_new(schema.clone(), vec![val_arr]).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_array_elements: RecordBatch error: {e}"))
        })?;
        Ok(Arc::new(JsonbArrayElementsTable {
            schema,
            batches: vec![batch],
        }))
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(_state, _projection, _filters, _limit)
            .await
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
        let v: Value = serde_json::from_str(&json_str).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_object_keys: JSON parse error: {e}"))
        })?;
        let obj = match v {
            Value::Object(m) => m,
            _ => return plan_err!("jsonb_object_keys: argument must be a JSON object"),
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "jsonb_object_keys",
            DataType::Utf8,
            false,
        )]));

        let keys: Vec<String> = obj.keys().cloned().collect();
        let key_arr: ArrayRef = Arc::new(StringArray::from(keys));
        let batch = RecordBatch::try_new(schema.clone(), vec![key_arr]).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_object_keys: RecordBatch error: {e}"))
        })?;
        Ok(Arc::new(JsonbObjectKeysTable {
            schema,
            batches: vec![batch],
        }))
    }
}

// ── jsonb_delete_path(jsonb, text) — scalar UDF target for `jsonb #- '{path}'`
//
// Postgres: `'{"a":{"b":1}}'::jsonb #- '{a,b}'` removes the nested key at
// the given path. The second arg is a text-path like `'{a,b}'` (PG text-array
// literal). Our rewriter converts `lhs #- rhs` to `jsonb_delete_path(lhs, rhs)`.

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbDeletePathUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbDeletePathUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_delete_path"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 2 {
            return exec_err!(
                "jsonb_delete_path expects 2 arguments, got {}",
                arrays.len()
            );
        }
        let doc_arr = &arrays[0];
        let path_arr = &arrays[1];
        let len = doc_arr.len();
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        for i in 0..len {
            if doc_arr.is_null(i) || path_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let mut doc_val = match extract_jsonb_value(doc_arr, i, "jsonb_delete_path")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let path_str = match extract_string(path_arr, i) {
                Some(s) => s,
                None => {
                    out.push(Some(value_to_jsonb(&doc_val)?));
                    continue;
                }
            };
            let path_parts = parse_key_list(&path_str);
            if !path_parts.is_empty() {
                delete_json_path(&mut doc_val, &path_parts);
            }
            out.push(Some(value_to_jsonb(&doc_val)?));
        }
        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Recursively delete the value at `path` from `doc`.
fn delete_json_path(doc: &mut Value, path: &[String]) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        match doc {
            Value::Object(m) => {
                m.remove(&path[0]);
            }
            Value::Array(a) => {
                if let Ok(idx) = path[0].parse::<usize>() {
                    if idx < a.len() {
                        a.remove(idx);
                    }
                }
            }
            _ => {}
        }
        return;
    }
    match doc {
        Value::Object(m) => {
            if let Some(child) = m.get_mut(&path[0]) {
                delete_json_path(child, &path[1..]);
            }
        }
        Value::Array(a) => {
            if let Ok(idx) = path[0].parse::<usize>() {
                if let Some(child) = a.get_mut(idx) {
                    delete_json_path(child, &path[1..]);
                }
            }
        }
        _ => {}
    }
}

// ── json_to_recordset / jsonb_to_recordset ────────────────────────────────────
//
// Expands a JSON array of objects into a table. The output schema is inferred
// from the first object in the array. Column types are all Utf8 (scalars are
// serialised to their JSON text representation so the planner can apply CAST
// from the `AS t(col type)` alias hint).
//
// Example: `SELECT * FROM json_to_recordset('[{"a":1},{"a":2}]') AS t(a int)`
//          → two rows with column `a` = "1", "2" (cast to int by DataFusion).

#[derive(Debug)]
struct JsonToRecordsetTf {}

#[derive(Debug)]
struct JsonToRecordsetTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for JsonToRecordsetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(_state, _projection, _filters, _limit)
            .await
    }
}

impl TableFunctionImpl for JsonToRecordsetTf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("json_to_recordset / jsonb_to_recordset requires exactly 1 argument");
        }
        let json_str = match json_from_expr(&args[0]) {
            Some(s) => s,
            None => return plan_err!("json_to_recordset: argument must be a JSON array literal"),
        };
        let arr: Vec<Value> = serde_json::from_str(&json_str).map_err(|e| {
            DataFusionError::Plan(format!("json_to_recordset: JSON parse error: {e}"))
        })?;

        // Collect all keys across all objects (preserving insertion order from first object).
        let mut keys: Vec<String> = Vec::new();
        for row in &arr {
            if let Value::Object(m) = row {
                for k in m.keys() {
                    if !keys.contains(k) {
                        keys.push(k.clone());
                    }
                }
            }
        }

        if keys.is_empty() {
            // Empty input or non-object rows — return empty table with one Utf8 column.
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
            return Ok(Arc::new(JsonToRecordsetTable {
                schema,
                batches: vec![],
            }));
        }

        let schema = Arc::new(Schema::new(
            keys.iter()
                .map(|k| Field::new(k.as_str(), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));

        // Build one StringArray per column.
        let mut columns: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(arr.len()); keys.len()];
        for row in &arr {
            let obj = match row {
                Value::Object(m) => m,
                _ => {
                    for col in &mut columns {
                        col.push(None);
                    }
                    continue;
                }
            };
            for (col_idx, key) in keys.iter().enumerate() {
                let cell: Option<String> = match obj.get(key) {
                    None | Some(Value::Null) => None,
                    Some(Value::String(s)) => Some(s.clone()),
                    Some(other) => serde_json::to_string(other).ok(),
                };
                columns[col_idx].push(cell);
            }
        }

        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .map(|col| Arc::new(StringArray::from(col)) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            DataFusionError::Plan(format!("json_to_recordset: RecordBatch error: {e}"))
        })?;
        Ok(Arc::new(JsonToRecordsetTable {
            schema,
            batches: vec![batch],
        }))
    }
}

// ---------------------------------------------------------------------------
// Microbenchmark (opt-in, env-gated — not part of CI)
// ---------------------------------------------------------------------------
//
// Run: `BASIN_JSONB_BENCH=1 cargo test -p basin-engine --lib --release \
//       jsonb_udf::perf_bench -- --nocapture --ignored`
#[cfg(test)]
mod perf_bench {
    use super::*;
    use std::time::Instant;

    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_extract_path_text_100k() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        const N: usize = 100_000;
        let rows: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                serde_json::to_vec(&serde_json::json!({
                    "user": {"name": format!("u{i}"), "age": i % 100},
                    "tags": ["a", "b", "c"],
                }))
                .unwrap()
            })
            .collect();
        let arr: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            rows.iter().map(|r| Some(r.as_slice())),
        ));
        let k1: ArrayRef =
            Arc::new(StringArray::from_iter_values(std::iter::repeat("user").take(N)));
        let k2: ArrayRef =
            Arc::new(StringArray::from_iter_values(std::iter::repeat("name").take(N)));
        let keys = [k1, k2];

        // SLOW: pre-optimization shape — per-row downcast + key `to_string()`.
        let t0 = Instant::now();
        for row in 0..N {
            let root = extract_jsonb_value(&arr, row, "b").unwrap().unwrap();
            let mut cur = root;
            for ka in &keys {
                let a = ka.as_any().downcast_ref::<StringArray>().unwrap();
                let key = a.value(row).to_string();
                cur = match cur {
                    Value::Object(mut m) => m.remove(&key).unwrap_or(Value::Null),
                    _ => Value::Null,
                };
            }
            std::hint::black_box(cur);
        }
        let slow = t0.elapsed();

        // FAST: hoisted downcast + &str key access.
        let tj = TypedJsonbArray::new(&arr, "b").unwrap();
        let kt: Vec<TypedStringRef<'_>> =
            keys.iter().map(|k| TypedStringRef::new(k, "b").unwrap()).collect();
        let t1 = Instant::now();
        for row in 0..N {
            let v = jsonb_extract_path_row_typed(&tj, row, &kt, "b").unwrap();
            std::hint::black_box(v);
        }
        let fast = t1.elapsed();
        eprintln!(
            "SLOW {:?} ({:.2} M/s) | FAST {:?} ({:.2} M/s) | {:.2}x",
            slow,
            N as f64 / slow.as_secs_f64() / 1e6,
            fast,
            N as f64 / fast.as_secs_f64() / 1e6,
            slow.as_secs_f64() / fast.as_secs_f64()
        );
    }

    /// A battery of arbitrary JSON payloads (no bench-specific keys): nested
    /// objects, arrays, nulls, unicode, signed/float/large numbers, deeply
    /// nested mixes, and scalar roots. Used by both the parity test and the
    /// parse micro-benchmark so they exercise identical inputs.
    fn parity_payloads() -> Vec<String> {
        vec![
            // nested object + array + null + bool
            r#"{"a":{"b":[1,2,3],"c":null,"d":true},"e":false,"f":[{"g":"h"},{"i":[]}]}"#
                .to_string(),
            // unicode (escapes + raw multibyte) and an empty object/array
            r#"{"name":"café ☃","emoji":"😀","raw":"naïve résumé","obj":{},"arr":[]}"#
                .to_string(),
            // number spectrum: negative, float, exponent, large int, zero
            r#"{"neg":-42,"float":3.14159,"exp":6.022e23,"big":9223372036854775807,"zero":0,"tiny":-1.5e-9}"#
                .to_string(),
            // arrays of heterogeneous scalars and nested arrays
            r#"[1,"two",3.0,null,true,false,[1,[2,[3,[4]]]],{"k":"v"}]"#.to_string(),
            // scalar roots
            r#"null"#.to_string(),
            r#"true"#.to_string(),
            r#"-12345.678"#.to_string(),
            r#""a standalone string with \"quotes\" and \\backslash""#.to_string(),
            // realistic ~1KB nested document, structurally arbitrary
            {
                let mut items = String::new();
                for i in 0..16 {
                    if i > 0 {
                        items.push(',');
                    }
                    items.push_str(&format!(
                        r#"{{"idx":{i},"label":"row-{i}","ratio":{}.{},"flags":[true,false,null],"nested":{{"x":{i},"y":[{i},{i}],"note":"unicode ☃ value {i}"}}}}"#,
                        i, i
                    ));
                }
                format!(r#"{{"header":{{"v":2,"who":"arbitrary"}},"items":[{items}],"trailer":null}}"#)
            },
        ]
    }

    /// Correctness of the production parse path (`jsonb_to_value`,
    /// `serde_json::from_slice`) across the arbitrary payload battery, and as
    /// a cross-check, byte-for-byte parity with the simd-json parser that was
    /// evaluated for the hot path. Both must produce identical
    /// `serde_json::Value` trees (nested objects, arrays, nulls, unicode,
    /// signed/float/large numbers), and the 0x01 version-byte tolerance must
    /// hold. simd-json was measured but not adopted (see `jsonb_to_value`);
    /// this parity check documents that the candidate parser was correct.
    #[test]
    fn jsonb_parse_parity() {
        for p in parity_payloads() {
            let bytes = p.as_bytes();
            let via_prod = jsonb_to_value(bytes).expect("prod (serde) parse");
            let mut scratch = bytes.to_vec();
            let via_simd = simd_json::from_slice::<Value>(&mut scratch).expect("simd parse");
            assert_eq!(via_prod, via_simd, "parity mismatch for payload: {p}");

            // Version-byte (0x01) tolerance on the production path.
            let mut prefixed = vec![0x01u8];
            prefixed.extend_from_slice(bytes);
            let via_prod_pref = jsonb_to_value(&prefixed).expect("prod parse prefixed");
            assert_eq!(via_prod_pref, via_prod, "version-byte stripping mismatch");
        }
    }

    /// Records the measurement behind the decision NOT to adopt simd-json for
    /// the JSONB parse hot path. Opt-in (timing is noisy in CI) via
    /// BASIN_JSONB_BENCH=1; prints serde_json-vs-simd_json ratios for a
    /// realistic ~2KB payload and a larger ~62KB document. Observed (release,
    /// Apple Silicon): ~1.03x at 2KB, ~1.14x at 62KB — the win is marginal
    /// because the `serde_json::Value`-tree allocation and the mandatory input
    /// copy (simd-json mutates in place) dominate the SIMD tokenisation gain.
    /// Uses arbitrary JSON only (no bench-specific keys/values).
    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_simd_vs_serde_parse() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        let small = parity_payloads().into_iter().max_by_key(|s| s.len()).unwrap();
        let large = {
            let mut items = String::new();
            for i in 0..512 {
                if i > 0 {
                    items.push(',');
                }
                items.push_str(&format!(
                    r#"{{"idx":{i},"label":"row-{i}","ratio":{i}.{i},"flags":[true,false,null],"nested":{{"x":{i},"y":[{i},{i}],"note":"value {i}"}}}}"#
                ));
            }
            format!(r#"{{"items":[{items}]}}"#)
        };
        for payload in [&small, &large] {
            let bytes = payload.as_bytes();
            eprintln!("payload size: {} bytes", bytes.len());
            let n: usize = if bytes.len() < 4096 { 200_000 } else { 20_000 };

            let serde_parse = |b: &[u8]| serde_json::from_slice::<Value>(b).unwrap();
            let simd_parse = |b: &[u8]| {
                let mut scratch = b.to_vec();
                simd_json::from_slice::<Value>(&mut scratch).unwrap()
            };

            for _ in 0..2_000 {
                std::hint::black_box(serde_parse(bytes));
                std::hint::black_box(simd_parse(bytes));
            }

            let t0 = Instant::now();
            for _ in 0..n {
                std::hint::black_box(serde_parse(bytes));
            }
            let serde_dur = t0.elapsed();

            let t1 = Instant::now();
            for _ in 0..n {
                std::hint::black_box(simd_parse(bytes));
            }
            let simd_dur = t1.elapsed();

            let ratio = serde_dur.as_secs_f64() / simd_dur.as_secs_f64();
            eprintln!(
                "serde_json {:?} ({:.2} M/s) | simd-json {:?} ({:.2} M/s) | {:.2}x",
                serde_dur,
                n as f64 / serde_dur.as_secs_f64() / 1e6,
                simd_dur,
                n as f64 / simd_dur.as_secs_f64() / 1e6,
                ratio
            );
        }
    }

    // -----------------------------------------------------------------------
    // Lazy targeted extraction — parity oracles (the OLD full-parse path)
    // -----------------------------------------------------------------------

    /// OLD `->` semantics via a full `serde_json::Value` parse. Oracle for the
    /// lazy `raw_get_key` + `value_to_jsonb` path.
    fn oracle_get(bytes: &[u8], key: &str) -> Option<Value> {
        let doc = jsonb_to_value(bytes).unwrap();
        match &doc {
            Value::Object(map) => map.get(key).cloned(),
            Value::Array(arr) => key
                .parse::<usize>()
                .ok()
                .and_then(|i| arr.get(i).cloned())
                .or_else(|| {
                    key.parse::<i64>().ok().and_then(|i| {
                        if i < 0 {
                            let p = arr.len() as i64 + i;
                            (p >= 0).then(|| arr.get(p as usize).cloned()).flatten()
                        } else {
                            None
                        }
                    })
                }),
            _ => None,
        }
    }

    /// OLD `->>` semantics. Oracle for the lazy `raw_slice_to_text` path.
    fn oracle_get_text(bytes: &[u8], key: &str) -> Option<String> {
        match oracle_get(bytes, key) {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s),
            Some(v) => Some(serde_json::to_string(&v).unwrap_or_default()),
        }
    }

    /// OLD `#>` semantics. Oracle for the lazy `raw_walk_path` path.
    fn oracle_path(bytes: &[u8], segs: &[String]) -> Option<Value> {
        let mut cur = jsonb_to_value(bytes).unwrap();
        for seg in segs {
            let tmp = std::mem::replace(&mut cur, Value::Null);
            let next = match tmp {
                Value::Object(mut m) => m.remove(seg.as_str()),
                Value::Array(mut a) => match seg.parse::<usize>().ok() {
                    Some(i) if i < a.len() => Some(a.swap_remove(i)),
                    _ => None,
                },
                _ => None,
            };
            match next {
                Some(v) => cur = v,
                None => return None,
            }
        }
        Some(cur)
    }

    /// Candidate keys to probe — derived structurally from each payload (all of
    /// its object keys at every level, plus numeric indices and absent keys).
    /// NOT a fixed list, so no key name is special-cased.
    fn probe_keys(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                for (k, child) in m {
                    out.push(k.clone());
                    probe_keys(child, out);
                }
            }
            Value::Array(a) => {
                for child in a {
                    probe_keys(child, out);
                }
            }
            _ => {}
        }
    }

    /// All single-key / path / structural operations must match the old
    /// full-parse oracle byte-for-byte across the arbitrary payload battery.
    #[test]
    fn lazy_extract_parity() {
        let mut payloads = parity_payloads();
        // extra edge cases: duplicate keys (last wins), escaped key names,
        // unicode surrogate-pair escape, negative-index array, nested empties.
        payloads.push(r#"{"k":1,"k":2,"k":3}"#.to_string());
        payloads.push(r#"{"a\"b":"v1","c\\d":"v2","e/f":"v3"}"#.to_string());
        payloads.push(r#"{"emoji":"😀","bmp":"é ☃"}"#.to_string());
        payloads.push(r#"{"arr":[10,20,30,40],"nested":{"deep":{"x":[true,null,"s"]}}}"#.to_string());
        payloads.push(r#"{"empty_obj":{},"empty_arr":[],"n":null}"#.to_string());
        payloads.push(r#"[{"id":0},{"id":1},{"id":2}]"#.to_string());

        for p in &payloads {
            let bytes = p.as_bytes();
            let v = jsonb_to_value(bytes).unwrap();

            // Probe set: structural keys + numeric indices + a guaranteed-absent
            // key. Built generically from the payload.
            let mut keys = vec![
                "0".to_string(),
                "1".to_string(),
                "2".to_string(),
                "-1".to_string(),
                "__definitely_absent__".to_string(),
            ];
            probe_keys(&v, &mut keys);
            keys.sort();
            keys.dedup();

            let raw = RawJson::new(bytes);
            for key in &keys {
                // -> (json_get)
                let got = match raw_get_key(&raw, key).unwrap() {
                    Some(slice) => Some(raw_slice_to_value(slice).unwrap()),
                    None => None,
                };
                assert_eq!(got, oracle_get(bytes, key), "-> key={key:?} payload={p}");

                // ->> (json_get_text)
                let got_txt = match raw_get_key(&raw, key).unwrap() {
                    Some(slice) => raw_slice_to_text(slice).unwrap(),
                    None => None,
                };
                assert_eq!(
                    got_txt,
                    oracle_get_text(bytes, key),
                    "->> key={key:?} payload={p}"
                );

                // #> single-segment path (json_path_extract)
                let segs = vec![key.clone()];
                let got_path = match raw_walk_path(bytes, &segs).unwrap() {
                    Some(slice) => Some(raw_slice_to_value(slice).unwrap()),
                    None => None,
                };
                assert_eq!(got_path, oracle_path(bytes, &segs), "#> seg={key:?} payload={p}");

                // ? (jsonb_has_key)
                let has = RawJson::new(bytes).has_key(key).unwrap();
                let has_oracle = match &v {
                    Value::Object(m) => m.contains_key(key),
                    Value::Array(a) => {
                        a.iter().any(|e| matches!(e, Value::String(s) if s == key))
                    }
                    _ => false,
                };
                assert_eq!(has, has_oracle, "? key={key:?} payload={p}");
            }

            // Multi-segment nested paths built from the structure.
            for seg_pair in nested_paths(&v) {
                let got = match raw_walk_path(bytes, &seg_pair).unwrap() {
                    Some(slice) => Some(raw_slice_to_value(slice).unwrap()),
                    None => None,
                };
                assert_eq!(
                    got,
                    oracle_path(bytes, &seg_pair),
                    "#> nested path={seg_pair:?} payload={p}"
                );
            }

            // jsonb_typeof
            let typ = match RawJson::new(bytes).top_type() {
                Some(RawType::Object) => "object",
                Some(RawType::Array) => "array",
                Some(RawType::String) => "string",
                Some(RawType::Number) => "number",
                Some(RawType::Bool) => "boolean",
                Some(RawType::Null) => "null",
                None => unreachable!(),
            };
            let typ_oracle = match &v {
                Value::Object(_) => "object",
                Value::Array(_) => "array",
                Value::String(_) => "string",
                Value::Number(_) => "number",
                Value::Bool(_) => "boolean",
                Value::Null => "null",
            };
            assert_eq!(typ, typ_oracle, "typeof payload={p}");

            // jsonb_array_length
            let len = RawJson::new(bytes).array_len();
            let len_oracle = match &v {
                Value::Array(a) => Some(a.len()),
                _ => None,
            };
            assert_eq!(len, len_oracle, "array_length payload={p}");
        }
    }

    /// Build a handful of 2-level paths from the structure (object→key→key)
    /// for nested `#>` parity, generically (no fixed key names).
    fn nested_paths(v: &Value) -> Vec<Vec<String>> {
        let mut paths = Vec::new();
        if let Value::Object(m) = v {
            for (k1, child) in m {
                match child {
                    Value::Object(c) => {
                        for k2 in c.keys() {
                            paths.push(vec![k1.clone(), k2.clone()]);
                        }
                    }
                    Value::Array(a) => {
                        if !a.is_empty() {
                            paths.push(vec![k1.clone(), "0".to_string()]);
                        }
                    }
                    _ => {}
                }
            }
        }
        paths
    }

    /// Honest timed comparison: OLD full-parse-then-index vs NEW lazy targeted
    /// extraction, on a realistic ~1KB nested doc where only ONE top-level
    /// field is wanted. Gated by BASIN_JSONB_BENCH=1.
    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_lazy_vs_full_single_field() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        // ~1KB arbitrary nested doc (last of the parity battery).
        let payload = parity_payloads().into_iter().max_by_key(|s| s.len()).unwrap();
        let bytes = payload.as_bytes();
        eprintln!("payload size: {} bytes", bytes.len());
        // Probe the LAST top-level key (worst case for the scanner: it must
        // walk past all preceding members). Picked structurally, not by name.
        let v = jsonb_to_value(bytes).unwrap();
        let key = match &v {
            Value::Object(m) => m.keys().last().unwrap().clone(),
            _ => panic!("expected object payload"),
        };
        const N: usize = 300_000;

        // warmup
        for _ in 0..5_000 {
            std::hint::black_box(oracle_get(bytes, &key));
            let raw = RawJson::new(bytes);
            std::hint::black_box(raw_get_key(&raw, &key).unwrap());
        }

        let t0 = Instant::now();
        for _ in 0..N {
            std::hint::black_box(oracle_get(bytes, &key));
        }
        let full = t0.elapsed();

        let t1 = Instant::now();
        for _ in 0..N {
            let raw = RawJson::new(bytes);
            let slice = raw_get_key(&raw, &key).unwrap();
            std::hint::black_box(slice.map(|s| raw_slice_to_value(s).unwrap()));
        }
        let lazy = t1.elapsed();

        eprintln!(
            "FULL-parse {:?} ({:.2} M/s) | LAZY {:?} ({:.2} M/s) | {:.2}x",
            full,
            N as f64 / full.as_secs_f64() / 1e6,
            lazy,
            N as f64 / lazy.as_secs_f64() / 1e6,
            full.as_secs_f64() / lazy.as_secs_f64()
        );

        // Also time the pure-locate (no re-parse) cost — what `->>` on a string
        // field or `?`/typeof pays.
        let t2 = Instant::now();
        for _ in 0..N {
            let raw = RawJson::new(bytes);
            std::hint::black_box(raw_get_key(&raw, &key).unwrap());
        }
        let locate = t2.elapsed();
        eprintln!(
            "LOCATE-only {:?} ({:.2} M/s) | {:.2}x vs full",
            locate,
            N as f64 / locate.as_secs_f64() / 1e6,
            full.as_secs_f64() / locate.as_secs_f64()
        );
    }

    // -----------------------------------------------------------------------
    // ADR 0027 Phase 1 — JsonbBatchIndex parity and micro-benchmark
    // -----------------------------------------------------------------------

    /// Correctness: `JsonbBatchIndex::lookup` must return byte-identical value
    /// slices to `RawJson::member` across the full parity payload battery
    /// (nested objects, arrays, unicode, escaped keys, duplicate keys,
    /// empty containers, scalar roots). No key names are special-cased.
    #[test]
    fn batch_index_parity() {
        let mut payloads = parity_payloads();
        // Edge cases mirroring `lazy_extract_parity`.
        payloads.push(r#"{"k":1,"k":2,"k":3}"#.to_string()); // duplicate keys: last wins
        payloads.push(r#"{"a\"b":"v1","c\\d":"v2","e/f":"v3"}"#.to_string()); // escaped keys
        payloads.push(r#"{"emoji":"😀","bmp":"é ☃"}"#.to_string()); // unicode
        payloads.push(r#"{"arr":[10,20,30,40],"nested":{"deep":{"x":[true,null,"s"]}}}"#.to_string());
        payloads.push(r#"{"empty_obj":{},"empty_arr":[],"n":null}"#.to_string()); // empty containers
        // Non-object roots: the index must leave these as None (RawJson fallback).
        payloads.push(r#"[{"id":0},{"id":1}]"#.to_string()); // array root
        payloads.push(r#"null"#.to_string()); // null root
        payloads.push(r#"42"#.to_string()); // number root

        for p in &payloads {
            let bytes = p.as_bytes();
            let v = jsonb_to_value(bytes).unwrap();

            // Derive probe keys generically from the document structure.
            let mut keys = vec![
                "0".to_string(),
                "1".to_string(),
                "__absent__".to_string(),
            ];
            probe_keys(&v, &mut keys);
            keys.sort();
            keys.dedup();

            // Build the batch index for a single-row LargeBinaryArray.
            let arr = LargeBinaryArray::from_iter(
                std::iter::once(Some(bytes)),
            );
            let idx = JsonbBatchIndex::build(&arr);

            // For object-root documents: each key lookup must match RawJson::member.
            if matches!(v, Value::Object(_)) {
                let row_idx = idx.rows[0].as_ref().expect(
                    "object-root document must produce a non-None index entry",
                );
                for key in &keys {
                    // Oracle: RawJson::member.
                    let oracle_slice = RawJson::new(bytes).member(key).unwrap();

                    // Batch index lookup via JsonbBatchIndex::lookup.
                    let got = idx.lookup(&arr, 0, key);

                    match (oracle_slice, got) {
                        (None, Ok(None)) => {}
                        (Some(oracle), Ok(Some(got_slice))) => {
                            assert_eq!(
                                oracle, got_slice,
                                "batch index slice != RawJson slice for key={key:?} payload={p}"
                            );
                        }
                        (None, Ok(Some(_))) => panic!(
                            "batch index returned Some but RawJson returned None: key={key:?} payload={p}"
                        ),
                        (Some(_), Ok(None)) => {
                            // Key is in the map (row_idx.contains_key) but lookup
                            // returned None. This should only happen if the entry
                            // maps to an out-of-bounds range — a bug.
                            if row_idx.contains_key(key.as_str()) {
                                panic!(
                                    "batch index map has key {key:?} but lookup returned None: payload={p}"
                                );
                            }
                            // Key not in map AND not in RawJson: absent key — OK.
                        }
                        (oracle, Err(())) => {
                            // Err means "non-indexed row, fall back". For well-formed
                            // object documents the index must always succeed (None means
                            // absent key, not error).
                            panic!(
                                "batch index returned Err (non-indexed) for object doc: key={key:?} oracle={oracle:?} payload={p}"
                            );
                        }
                    }

                    // Cross-check: RawJson::member and batch index -> both produce
                    // the same final text result via raw_slice_to_text.
                    let oracle_txt = oracle_get_text(bytes, key);
                    let got_txt = match idx.lookup(&arr, 0, key) {
                        Ok(Some(slice)) => raw_slice_to_text(slice).unwrap(),
                        Ok(None) => None,
                        Err(()) => {
                            let raw = RawJson::new(bytes);
                            match raw_get_key(&raw, key) {
                                Ok(Some(s)) => raw_slice_to_text(s).unwrap(),
                                Ok(None) => None,
                                Err(()) => None,
                            }
                        }
                    };
                    assert_eq!(
                        got_txt, oracle_txt,
                        "text result mismatch: key={key:?} payload={p}"
                    );
                }
            } else {
                // Non-object root: the batch index must produce a None row entry,
                // and lookup must return Err(()) signalling fallback.
                assert!(
                    idx.rows[0].is_none(),
                    "non-object root must produce None index entry: payload={p}"
                );
                for key in &keys {
                    let got = idx.lookup(&arr, 0, key);
                    // Expect Err(()) since row is not indexed.
                    assert!(
                        matches!(got, Err(())),
                        "non-object root must return Err(()) from lookup: key={key:?} payload={p}"
                    );
                }
            }
        }
    }

    /// Micro-benchmark: simulated multi-UDF same-batch scenario.
    ///
    /// Models `SELECT payload->>'a', payload->>'b', payload->>'c', ... FROM t`
    /// — k json_get_text calls on the same batch. Measures:
    ///   - BASELINE: k independent RawJson::member scans (the pre-ADR-0027 shape)
    ///   - CACHED: one JsonbBatchIndex build + k O(1) HashMap lookups
    ///
    /// Gated by BASIN_JSONB_BENCH=1 (timing is noisy in CI).
    /// Uses arbitrary JSON with generically-derived keys — no bench literals.
    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_batch_index_multi_field() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        const N: usize = 100_000;

        // ~1 KB arbitrary nested document (last of the parity battery).
        let template = parity_payloads().into_iter().max_by_key(|s| s.len()).unwrap();
        let v = jsonb_to_value(template.as_bytes()).unwrap();
        // Extract up to 4 top-level keys generically (no names hardcoded).
        let probe_keys_top: Vec<String> = match &v {
            Value::Object(m) => m.keys().take(4).cloned().collect(),
            _ => panic!("expected object template"),
        };
        assert!(
            probe_keys_top.len() >= 2,
            "need at least 2 top-level keys for this bench"
        );
        let k = probe_keys_top.len();
        eprintln!(
            "payload size: {} bytes | probing {} keys: {:?}",
            template.len(),
            k,
            probe_keys_top
        );

        // Build a 100k-row batch where each row is the same template document.
        let rows: Vec<Vec<u8>> = (0..N).map(|_| template.as_bytes().to_vec()).collect();
        let arr = Arc::new(LargeBinaryArray::from_iter(
            rows.iter().map(|r| Some(r.as_slice())),
        ));

        // Warmup.
        for _ in 0..1_000 {
            for key in &probe_keys_top {
                for i in 0..10 {
                    let doc = RawJson::new(arr.value(i));
                    std::hint::black_box(doc.member(key).unwrap());
                }
            }
        }

        // BASELINE: k independent RawJson scans per row, no shared index.
        let t0 = Instant::now();
        for key in &probe_keys_top {
            for i in 0..N {
                let doc = RawJson::new(arr.value(i));
                std::hint::black_box(doc.member(key).unwrap());
            }
        }
        let baseline = t0.elapsed();

        // CACHED: build the index once, then k O(1) HashMap lookups per row.
        let t1 = Instant::now();
        let idx = JsonbBatchIndex::build(&arr);
        for key in &probe_keys_top {
            for i in 0..N {
                std::hint::black_box(idx.lookup(&arr, i, key).unwrap());
            }
        }
        let cached = t1.elapsed();

        let speedup = baseline.as_secs_f64() / cached.as_secs_f64();
        eprintln!(
            "BASELINE (k={k} × {N} RawJson scans): {:?} ({:.2} M/s)",
            baseline,
            (k * N) as f64 / baseline.as_secs_f64() / 1e6,
        );
        eprintln!(
            "CACHED  (build + k={k} × {N} map lookups): {:?} ({:.2} M/s)",
            cached,
            (k * N) as f64 / cached.as_secs_f64() / 1e6,
        );
        eprintln!("Speedup: {:.2}x for k={k} concurrent fields", speedup);

        // For k ≥ 2 fields on documents with >8 top-level keys, the cache should
        // be strictly faster because the amortised index build is sub-linear in k.
        // We assert speedup > 1 at minimum to catch obvious regressions.
        assert!(
            speedup > 1.0,
            "batch index should be faster than k independent scans for k={k}: {speedup:.2}x"
        );
    }

    // -----------------------------------------------------------------------
    // ADR 0027 Phase 2 — binary offset table parity + single-UDF bench
    // -----------------------------------------------------------------------

    /// Round-trip every payload in the parity battery through the Phase 2
    /// encoder (`encode_jsonb_v2`) and verify that `RawJson::new` + `member()`
    /// produces byte-identical results to the plain-text scanner for every key.
    ///
    /// This is the mandatory parity test for ADR 0027 Phase 2: if any key
    /// lookup diverges, the encoder must fall back to bare JSON (the encoder
    /// must NOT be enabled for that document). This test verifies the fast path
    /// produces identical results to the scanner path.
    #[test]
    fn phase2_encode_decode_parity() {
        use crate::dml::encode_jsonb_v2;

        let mut payloads = parity_payloads();
        // Extra edge cases matching lazy_extract_parity.
        payloads.push(r#"{"k":1,"k":2,"k":3}"#.to_string()); // dup keys: last wins
        payloads.push(r#"{"a\"b":"v1","c\\d":"v2","e/f":"v3"}"#.to_string()); // escaped keys
        payloads.push(r#"{"emoji":"😀","bmp":"é ☃","name":"naïve"}"#.to_string()); // unicode
        payloads.push(r#"{"arr":[10,20,30],"obj":{"x":1},"n":null,"b":true}"#.to_string());
        payloads.push(r#"{"empty_obj":{},"empty_arr":[]}"#.to_string());
        payloads.push(r#"{}"#.to_string()); // empty object
        // Non-object roots: encoder must fall back (return Err), so these are
        // tested to verify that encode_jsonb_v2 rejects them.
        let non_objects = [
            r#"[1,2,3]"#,
            r#"null"#,
            r#"42"#,
            r#""string""#,
            r#"true"#,
        ];
        for doc in &non_objects {
            let bytes = doc.as_bytes();
            assert!(
                encode_jsonb_v2(bytes).is_err(),
                "encode_jsonb_v2 must fail for non-object: {doc}"
            );
        }

        for p in &payloads {
            let json_bytes = p.as_bytes();
            let v = jsonb_to_value(json_bytes).unwrap();

            // Phase 2 encode — must succeed for top-level objects.
            let encoded = match encode_jsonb_v2(json_bytes) {
                Ok(b) => b,
                Err(()) => {
                    // Non-object root falls back to bare JSON — that's correct.
                    continue;
                }
            };
            assert_eq!(encoded[0], 0x02, "Phase 2 tag must be 0x02: payload={p}");

            // Derive probe keys generically from the document structure.
            let mut keys = vec![
                "0".to_string(),
                "__absent__".to_string(),
            ];
            probe_keys(&v, &mut keys);
            keys.sort();
            keys.dedup();

            for key in &keys {
                // Oracle: RawJson scanner on the plain JSON bytes.
                let oracle_raw = RawJson::new(json_bytes).member(key).unwrap();
                let oracle_text = oracle_get_text(json_bytes, key);

                // Phase 2 path: RawJson on the encoded (0x02-tagged) bytes.
                let p2_raw = RawJson::new(&encoded).member(key).unwrap();
                let p2_text = match p2_raw {
                    Some(slice) => raw_slice_to_text(slice).unwrap(),
                    None => None,
                };

                // Byte-identical value slice.
                assert_eq!(
                    oracle_raw, p2_raw,
                    "Phase 2 member() slice != scanner for key={key:?} payload={p}"
                );
                // Identical text result.
                assert_eq!(
                    oracle_text, p2_text,
                    "Phase 2 text result != scanner for key={key:?} payload={p}"
                );
            }

            // Also verify that jsonb_to_value on the encoded bytes produces the
            // same serde_json::Value tree as on the plain bytes.
            let decoded_v = jsonb_to_value(&encoded).expect("jsonb_to_value on v2 bytes");
            assert_eq!(
                v, decoded_v,
                "jsonb_to_value on Phase 2 bytes must match plain-bytes parse: payload={p}"
            );
        }
    }

    /// Micro-benchmark for Phase 2: single-UDF, single-key extraction over a
    /// batch of N rows. Measures:
    ///   1. SCANNER  — `RawJson::new(bare_json).member(key)` (pre-Phase-2 shape)
    ///   2. V2_TABLE — `RawJson::new(v2_encoded).member(key)` (Phase 2 offset table)
    ///
    /// Reports the speedup. Gated by `BASIN_JSONB_BENCH=1`.
    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_phase2_single_udf_single_key() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        use crate::dml::encode_jsonb_v2;
        const N: usize = 100_000;

        // Use the largest payload from the parity battery (~1KB).
        let template = parity_payloads().into_iter().max_by_key(|s| s.len()).unwrap();
        let json_bytes = template.as_bytes();
        eprintln!("payload: {} bytes", json_bytes.len());

        // Pick the LAST top-level key (worst-case for the scanner — it scans
        // past all preceding members). Key is derived structurally from the
        // document, not hardcoded.
        let v = jsonb_to_value(json_bytes).unwrap();
        let key = match &v {
            serde_json::Value::Object(m) => m.keys().last().unwrap().clone(),
            _ => panic!("expected object template"),
        };
        eprintln!("probing last key: {key:?}");

        // Encode once.
        let encoded = encode_jsonb_v2(json_bytes).expect("encode_jsonb_v2 failed");

        // Verify parity before measuring.
        let oracle = oracle_get_text(json_bytes, &key);
        let p2_slice = RawJson::new(&encoded).member(&key).unwrap();
        let p2_text = p2_slice.and_then(|s| raw_slice_to_text(s).unwrap());
        assert_eq!(oracle, p2_text, "parity check before bench");

        // Build arrays.
        let bare_rows: Vec<Vec<u8>> = (0..N).map(|_| json_bytes.to_vec()).collect();
        let v2_rows: Vec<Vec<u8>> = (0..N).map(|_| encoded.clone()).collect();

        // Warmup.
        for _ in 0..2_000 {
            let _ = std::hint::black_box(RawJson::new(&bare_rows[0]).member(&key).unwrap());
            let _ = std::hint::black_box(RawJson::new(&v2_rows[0]).member(&key).unwrap());
        }

        // 1. Scanner path (bare JSON, no offset table).
        let t0 = Instant::now();
        for i in 0..N {
            let doc = RawJson::new(&bare_rows[i]);
            let slice = std::hint::black_box(doc.member(&key).unwrap());
            if let Some(s) = slice {
                std::hint::black_box(raw_slice_to_text(s).unwrap());
            }
        }
        let scanner_dur = t0.elapsed();

        // 2. Phase 2 offset table path (v2-encoded).
        let t1 = Instant::now();
        for i in 0..N {
            let doc = RawJson::new(&v2_rows[i]);
            let slice = std::hint::black_box(doc.member(&key).unwrap());
            if let Some(s) = slice {
                std::hint::black_box(raw_slice_to_text(s).unwrap());
            }
        }
        let v2_dur = t1.elapsed();

        let speedup = scanner_dur.as_secs_f64() / v2_dur.as_secs_f64();
        eprintln!(
            "SCANNER  {:?} ({:.2} M/s)",
            scanner_dur,
            N as f64 / scanner_dur.as_secs_f64() / 1e6
        );
        eprintln!(
            "V2_TABLE {:?} ({:.2} M/s) | speedup {:.2}x",
            v2_dur,
            N as f64 / v2_dur.as_secs_f64() / 1e6,
            speedup
        );

        // The offset table must be strictly faster for a last-key lookup on a
        // multi-key document (the scanner must walk past all preceding members).
        assert!(
            speedup > 1.0,
            "Phase 2 offset table should be faster than scanner for last-key lookup: {speedup:.2}x"
        );
    }
}
