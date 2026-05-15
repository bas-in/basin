//! Stub scalar UDFs for PostgreSQL full-text search (FTS) functions.
//!
//! # Scope
//!
//! These are **STUBS**. Real tokenisation, ranking, and operator semantics are
//! deferred to a future release. The stubs exist so that ORM migration scripts
//! and SQL that reference `to_tsvector`, `to_tsquery`, `ts_rank`, etc. can
//! execute without an "Invalid function" error.  Every function returns a
//! sensible zero/identity value:
//!
//! - Text-producing stubs (`to_tsvector`, `to_tsquery`, …) return their body /
//!   query argument unchanged — ORMs that call these in a SELECT will get the
//!   raw text back rather than an actual lexeme list.
//! - Float-producing stubs (`ts_rank`, `ts_rank_cd`) always return `0.0`.
//! - Integer-producing stubs (`length(tsvector)`) count whitespace-delimited
//!   words as a cheap approximation.
//! - `numnode(tsquery)` always returns `1`.
//! - `tsvector_match(tsvector, tsquery)` always returns `false`.  Retained for
//!   backward compatibility with explicit callers of the function name.
//! - `tsvector_match_udf(tsvector, tsquery)` always returns `true`.  This is
//!   the target of the `@@`-to-UDF rewrite in `pg_ast::rewrite_tsvector_at_at`:
//!   every `lhs @@ rhs` in incoming SQL is lowered to
//!   `tsvector_match_udf(lhs, rhs)` before sqlparser sees it.  Returning `true`
//!   means the query produces rows (match-all stub); real FTS selectivity is
//!   deferred to `basin-fts`.
//!
//! # When to un-stub
//!
//! Promote each stub to a real implementation when Basin adds a tantivy /
//! pg_fts sidecar or integrates DuckDB's FTS extension.  At that point:
//! 1. Remove the word "STUB" from this file and the constant below.
//! 2. Implement real tokenisation in `to_tsvector_impl`.
//! 3. Implement real ranking in `ts_rank_impl`.
//! 4. Remove this notice from the crate changelog entry.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Float32Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// Registration entry point
// ---------------------------------------------------------------------------

/// Register all FTS stub UDFs on `ctx`.  Idempotent — DataFusion overwrites
/// by name so calling this multiple times is safe.
///
/// **STUB** — none of these perform real full-text search.  See module docs.
pub(crate) fn register_fts_udfs(ctx: &SessionContext) {
    register_all(ctx);
}

fn register_all(ctx: &SessionContext) {
    // Reusable TypeSignature constants for 1-text and 2-text forms.
    let ts1 = TypeSignature::Exact(vec![DataType::Utf8]);
    let ts2 = TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]);

    // ----------- to_tsvector -----------
    // 1-arg: to_tsvector(text) -> text
    // 2-arg: to_tsvector(config text, body text) -> text
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "to_tsvector".into(),
        signature: Signature::one_of(
            vec![ts1.clone(), ts2.clone()],
            Volatility::Immutable,
        ),
    }));

    // ----------- to_tsquery -----------
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "to_tsquery".into(),
        signature: Signature::one_of(
            vec![ts1.clone(), ts2.clone()],
            Volatility::Immutable,
        ),
    }));

    // ----------- plainto_tsquery -----------
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "plainto_tsquery".into(),
        signature: Signature::one_of(
            vec![ts1.clone(), ts2.clone()],
            Volatility::Immutable,
        ),
    }));

    // ----------- phraseto_tsquery -----------
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "phraseto_tsquery".into(),
        signature: Signature::one_of(
            vec![ts1.clone(), ts2.clone()],
            Volatility::Immutable,
        ),
    }));

    // ----------- websearch_to_tsquery -----------
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "websearch_to_tsquery".into(),
        signature: Signature::one_of(
            vec![ts1.clone(), ts2.clone()],
            Volatility::Immutable,
        ),
    }));

    // ----------- ts_rank -----------
    // 2-arg: ts_rank(tsvector, tsquery) -> float32
    // 3-arg: ts_rank(weights real[], tsvector, tsquery) -> float32
    // We model tsvector/tsquery as Utf8 internally, weights as a list
    // but we accept Utf8 for the first arg in the 3-arg form too because
    // cast inference may push a string literal.  Use any_n_text for
    // simplicity — the stubs discard all inputs anyway.
    ctx.register_udf(ScalarUDF::from(ZeroFloat32Udf {
        name: "ts_rank".into(),
        signature: sig_ts_rank(),
    }));

    // ----------- ts_rank_cd -----------
    ctx.register_udf(ScalarUDF::from(ZeroFloat32Udf {
        name: "ts_rank_cd".into(),
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));

    // ----------- ts_headline -----------
    // 2-arg: ts_headline(text, tsquery) -> text
    // 3-arg: ts_headline(config text, text, tsquery) -> text
    // 4-arg: ts_headline(config text, text, tsquery, options text) -> text
    ctx.register_udf(ScalarUDF::from(TsHeadlineUdf {
        name: "ts_headline".into(),
        signature: sig_ts_headline(),
    }));

    // ----------- setweight -----------
    // setweight(tsvector, char) -> text (stub: return tsvector unchanged)
    ctx.register_udf(ScalarUDF::from(PassthroughFirstTextUdf {
        name: "setweight".into(),
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));

    // ----------- strip -----------
    // strip(tsvector) -> text (stub: return unchanged)
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "strip".into(),
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // ----------- tsvector_length -----------
    // tsvector_length(tsvector) -> int32 (stub: count whitespace-separated words)
    // NOTE: the PG function is called `length(tsvector)` but DataFusion's
    // built-in `length(Utf8)` has priority over user-registered UDFs.  We
    // expose this as `tsvector_length` to avoid shadowing the string function.
    // Callers should use `tsvector_length(...)` from Basin SQL.
    ctx.register_udf(ScalarUDF::from(TsvectorLengthUdf {
        name: "tsvector_length".into(),
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // ----------- numnode -----------
    ctx.register_udf(ScalarUDF::from(ConstInt32Udf {
        name: "numnode".into(),
        value: 1,
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // ----------- querytree -----------
    ctx.register_udf(ScalarUDF::from(PassthroughLastTextUdf {
        name: "querytree".into(),
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // ----------- tsvector_match (@@-operator substitute) -----------
    // The `@@` binary operator between tsvector and tsquery cannot be
    // rewritten as a ScalarUDF from inside Basin.  Callers that need
    // `@@` semantics should rewrite their SQL to use `tsvector_match`.
    // This stub always returns `false` — v0.1 honesty, not a real match.
    ctx.register_udf(ScalarUDF::from(TsvectorMatchUdf {
        name: "tsvector_match".into(),
        returns_true: false,
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));

    // ----------- tsvector_match_udf (operator-rewrite target) -----------
    // This is the UDF that `pg_ast::rewrite_tsvector_at_at` rewrites `@@`
    // into.  It accepts (tsvector, tsquery) — both stored as Utf8 — and
    // returns `true` so that queries like
    //   SELECT 'a quick fox'::tsvector @@ to_tsquery('english', 'fox')
    // produce a result row rather than being silently filtered out.
    // The v0.1 stub is "always match"; real selectivity is deferred to
    // basin-fts.  The companion `tsvector_match` UDF (above) retains its
    // `false` return so existing tests that explicitly test the negative
    // case remain accurate.
    ctx.register_udf(ScalarUDF::from(TsvectorMatchUdf {
        name: "tsvector_match_udf".into(),
        returns_true: true,
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));

    // Suppress unused variable warning for ts1/ts2 if last use was a clone.
    let _ = (ts1, ts2);
}

// ---------------------------------------------------------------------------
// Shared signature helpers
// ---------------------------------------------------------------------------

fn sig_ts_rank() -> Signature {
    Signature::one_of(
        vec![
            // ts_rank(tsvector, tsquery)
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            // ts_rank(weights, tsvector, tsquery) — weights modelled as Utf8
            // because Arrow List round-trips through casts; real weights are
            // a small real[] that callers pass as a literal. Accept Utf8 for
            // all three to avoid arity/type mismatch from the planner.
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Immutable,
    )
}

fn sig_ts_headline() -> Signature {
    Signature::one_of(
        vec![
            // ts_headline(text, tsquery)
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            // ts_headline(config, text, tsquery)
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            // ts_headline(config, text, tsquery, options)
            TypeSignature::Exact(vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
            ]),
        ],
        Volatility::Immutable,
    )
}

// ---------------------------------------------------------------------------
// Helper: row count from ColumnarValue slice
// ---------------------------------------------------------------------------

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

// ---------------------------------------------------------------------------
// PassthroughLastTextUdf — returns the *last* Utf8 argument unchanged.
// Used for: to_tsvector, to_tsquery, plainto_tsquery, phraseto_tsquery,
//           websearch_to_tsquery, strip, querytree.
//
// For the 1-arg forms the last arg is the only arg (the text body).
// For the 2-arg forms the last arg is the body/query (the config is first).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PassthroughLastTextUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for PassthroughLastTextUdf {
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
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let last = args.last().expect("at least one argument required");
        let arr = last.clone().into_array(n)?;
        // Ensure the type is Utf8; cast if needed (shouldn't happen in practice).
        if arr.data_type() == &DataType::Utf8 {
            Ok(ColumnarValue::Array(arr))
        } else {
            let casted = datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?;
            Ok(ColumnarValue::Array(casted))
        }
    }
}

// ---------------------------------------------------------------------------
// PassthroughFirstTextUdf — returns the *first* Utf8 argument unchanged.
// Used for: setweight (return the tsvector arg, ignoring the weight char).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PassthroughFirstTextUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for PassthroughFirstTextUdf {
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
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let first = args.first().expect("at least one argument required");
        let arr = first.clone().into_array(n)?;
        if arr.data_type() == &DataType::Utf8 {
            Ok(ColumnarValue::Array(arr))
        } else {
            let casted = datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?;
            Ok(ColumnarValue::Array(casted))
        }
    }
}

// ---------------------------------------------------------------------------
// ZeroFloat32Udf — always returns 0.0 as Float32.
// Used for: ts_rank, ts_rank_cd.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ZeroFloat32Udf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for ZeroFloat32Udf {
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
        Ok(DataType::Float32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(Float32Array::from(vec![0.0f32; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// ConstInt32Udf — always returns a fixed Int32 constant.
// Used for: numnode (returns 1).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ConstInt32Udf {
    name: String,
    value: i32,
    signature: Signature,
}

impl ScalarUDFImpl for ConstInt32Udf {
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
        Ok(DataType::Int32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![self.value; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// TsvectorLengthUdf — count whitespace-separated tokens in the input string.
// Used for: tsvector_length(tsvector) -> Int32.
// The approximation is "split on ASCII whitespace and count non-empty pieces".
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TsvectorLengthUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for TsvectorLengthUdf {
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
        Ok(DataType::Int32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr = args[0].clone().into_array(n)?;
        let strings = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tsvector_length expects Utf8");
        let mut counts = Vec::with_capacity(n);
        for i in 0..strings.len() {
            if strings.is_null(i) {
                counts.push(0i32);
            } else {
                let count = strings.value(i).split_whitespace().count() as i32;
                counts.push(count);
            }
        }
        let out: ArrayRef = Arc::new(Int32Array::from(counts));
        Ok(ColumnarValue::Array(out))
    }
}

// ---------------------------------------------------------------------------
// TsHeadlineUdf — return the document body (second arg for 3/4-arg, first
// arg for 2-arg) unchanged.  The "body" is always the penultimate-from-last
// text arg before the tsquery.
//
// For ts_headline(text, tsquery)       → return arg 0
// For ts_headline(config, text, tsq)   → return arg 1
// For ts_headline(config, text, tsq, opts) → return arg 1
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TsHeadlineUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for TsHeadlineUdf {
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
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // 2-arg: (body, tsquery) → body is args[0]
        // 3-arg: (config, body, tsquery) → body is args[1]
        // 4-arg: (config, body, tsquery, options) → body is args[1]
        let body_idx = if args.len() == 2 { 0 } else { 1 };
        let arr = args[body_idx].clone().into_array(n)?;
        if arr.data_type() == &DataType::Utf8 {
            Ok(ColumnarValue::Array(arr))
        } else {
            let casted = datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?;
            Ok(ColumnarValue::Array(casted))
        }
    }
}

// ---------------------------------------------------------------------------
// TsvectorMatchUdf — tsvector_match(tsvector, tsquery) -> bool
//
// Two registrations share this implementation:
//
// * `tsvector_match`     — always returns `false`.  Retained for backward
//   compatibility with callers that explicitly use the function name instead
//   of the `@@` operator.
// * `tsvector_match_udf` — always returns `true`.  This is the target of the
//   `@@`-to-UDF rewrite in `pg_ast::rewrite_tsvector_at_at`.  Returning
//   `true` means that a query like
//     SELECT … WHERE ts_col @@ to_tsquery('english', 'fox')
//   returns rows (the stub matches everything) rather than silently returning
//   zero rows.  Real selectivity is deferred to `basin-fts`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TsvectorMatchUdf {
    name: String,
    /// `true` → return all-true column (match-all stub, used for `tsvector_match_udf`).
    /// `false` → return all-false column (used for `tsvector_match`).
    returns_true: bool,
    signature: Signature,
}

impl ScalarUDFImpl for TsvectorMatchUdf {
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
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![self.returns_true; n]));
        Ok(ColumnarValue::Array(arr))
    }
}
