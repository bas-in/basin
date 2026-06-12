//! DataFusion wiring for the `pg_trgm`-compatible fuzzy-text layer.
//!
//! This module replaces the v0.1 no-op stub with:
//!
//! 1. **Scalar UDFs** registered into every session context:
//!    - `similarity(text, text) -> float4`
//!    - `word_similarity(text, text) -> float4`
//!    - `show_trgm(text) -> text[]`  (returns `List<Utf8>`, matching
//!      DataFusion's closest equivalent to PG's `text[]`)
//!
//! 2. **Pre-parse operator rewrites** in [`rewrite_trgm_operators`]:
//!    - `a % b`   →  `similarity(a, b) >= <similarity_threshold>`
//!    - `a <% b`  →  `word_similarity(a, b) >= <word_similarity_threshold>`
//!    - `a <-> b` →  `(1.0 - similarity(a, b))` (trigram distance for ORDER BY)
//!
//!    **% disambiguation:** `%` is also the integer modulo operator.  The
//!    rewriter only fires when at least one operand looks like a text expression
//!    (quoted string literal, known text-returning function call, or a plain
//!    identifier that is NOT a bare integer literal).  When both operands are
//!    numeric literals (`7 % 3`) the rewriter leaves them untouched.  This
//!    heuristic is the same conservative approach used by the range-operator
//!    rewriter for `+`, `*`, `-`.
//!
//!    **`<->` disambiguation:** `<->` is also claimed by `pgvector` for L2
//!    distance.  The vector rewriter runs BEFORE this one in the executor
//!    pipeline and converts vector `<->` operands (`'[…]'` literals, or
//!    `embedding` column references preceded by a `::vector` cast) to
//!    `l2_distance(…)`.  Any `<->` that survives to this pass is therefore
//!    either a text trigram-distance expression or a tsquery phrase operator
//!    inside a string literal.  The quote-aware scanner (reused from the vector
//!    rewriter via [`find_trgm_op_outside_strings`]) ensures the phrase-operator
//!    form inside `'quick <-> fox'` is never rewritten.
//!
//! 3. **Session GUCs** handled in `executor.rs` SET/SHOW arms and
//!    `noop_accept.rs` fall-through guards:
//!    - `SET pg_trgm.similarity_threshold = <float>`
//!    - `SET pg_trgm.word_similarity_threshold = <float>`
//!    - `SHOW pg_trgm.similarity_threshold`
//!    - `SHOW pg_trgm.word_similarity_threshold`
//!
//!    Defaults come from `basin_trgm::{DEFAULT_SIMILARITY_THRESHOLD,
//!    DEFAULT_WORD_SIMILARITY_THRESHOLD}` (0.3 and 0.6 respectively).
//!
//! ## v0.2 follow-up
//!
//! GIN trigram index storage (trigram → row-id postings, mirrored on the
//! HNSW vector index format) is NOT in scope here.  Without it,
//! `WHERE name % 'foo'` is a full partition scan with a similarity filter.
//! The scoring and threshold math are correct; only the scan is unoptimised.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float32Array, ListArray, StringArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// UDF registration entry point
// ---------------------------------------------------------------------------

/// Register all `pg_trgm`-equivalent scalar UDFs on `ctx`.  Idempotent.
/// Called from [`crate::session::build_stateless_udf_cache`].
pub(crate) fn register_trgm_udfs(ctx: &SessionContext) {
    let two_text = Signature::exact(
        vec![DataType::Utf8, DataType::Utf8],
        Volatility::Immutable,
    );
    let one_text = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

    ctx.register_udf(ScalarUDF::from(SimilarityUdf {
        sig: two_text.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(WordSimilarityUdf {
        sig: two_text,
    }));
    ctx.register_udf(ScalarUDF::from(ShowTrgmUdf { sig: one_text }));
}

// ---------------------------------------------------------------------------
// similarity(text, text) -> float4
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct SimilarityUdf {
    sig: Signature,
}

impl std::fmt::Debug for SimilarityUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimilarityUdf")
    }
}

impl ScalarUDFImpl for SimilarityUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "similarity"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let a_strs = columnar_to_strings(&args.args[0], n)?;
        let b_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<f32>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = match (a_strs[i].as_deref(), b_strs[i].as_deref()) {
                (Some(a), Some(b)) => Some(basin_trgm::similarity(a, b)),
                _ => None,
            };
            out.push(result);
        }
        Ok(ColumnarValue::Array(
            Arc::new(Float32Array::from(out)) as ArrayRef
        ))
    }
}

// ---------------------------------------------------------------------------
// word_similarity(text, text) -> float4
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Hash)]
struct WordSimilarityUdf {
    sig: Signature,
}

impl std::fmt::Debug for WordSimilarityUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WordSimilarityUdf")
    }
}

impl ScalarUDFImpl for WordSimilarityUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "word_similarity"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let needle_strs = columnar_to_strings(&args.args[0], n)?;
        let haystack_strs = columnar_to_strings(&args.args[1], n)?;
        let mut out: Vec<Option<f32>> = Vec::with_capacity(n);
        for i in 0..n {
            let result = match (needle_strs[i].as_deref(), haystack_strs[i].as_deref()) {
                (Some(needle), Some(haystack)) => {
                    Some(basin_trgm::word_similarity(needle, haystack))
                }
                _ => None,
            };
            out.push(result);
        }
        Ok(ColumnarValue::Array(
            Arc::new(Float32Array::from(out)) as ArrayRef
        ))
    }
}

// ---------------------------------------------------------------------------
// show_trgm(text) -> text[] (List<Utf8>)
// ---------------------------------------------------------------------------
//
// PG returns `text[]`; DataFusion's closest native type is `List<Utf8>`.
// The column tag is "item" to match the convention used by other Basin
// list-returning UDFs (e.g. `tsvector_to_array` in fts_udf.rs).

#[derive(PartialEq, Eq, Hash)]
struct ShowTrgmUdf {
    sig: Signature,
}

impl std::fmt::Debug for ShowTrgmUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShowTrgmUdf")
    }
}

impl ScalarUDFImpl for ShowTrgmUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "show_trgm"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let input_strs = columnar_to_strings(&args.args[0], n)?;

        let mut values: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
        offsets.push(0);
        let mut nulls: Vec<bool> = Vec::with_capacity(n);

        for i in 0..n {
            match input_strs[i].as_deref() {
                None => {
                    nulls.push(false);
                    offsets.push(values.len() as i32);
                }
                Some(s) => {
                    nulls.push(true);
                    for tg in basin_trgm::show_trgm(s) {
                        values.push(Some(tg));
                    }
                    offsets.push(values.len() as i32);
                }
            }
        }

        let value_arr: ArrayRef = Arc::new(StringArray::from(values));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            value_arr,
            Some(nulls.into()),
        )
        .map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!("show_trgm: {e}"))
        })?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

// ---------------------------------------------------------------------------
// Columnar string helper
// ---------------------------------------------------------------------------

/// Extract a `Vec<Option<String>>` of length `n` from a `ColumnarValue`.
/// Scalars are broadcast; NULLs become `None`.
fn columnar_to_strings(cv: &ColumnarValue, n: usize) -> DFResult<Vec<Option<String>>> {
    match cv {
        ColumnarValue::Scalar(sv) => {
            let s = match sv {
                datafusion::scalar::ScalarValue::Utf8(Some(v)) => Some(v.clone()),
                datafusion::scalar::ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
                _ => None,
            };
            Ok(vec![s; n])
        }
        ColumnarValue::Array(arr) => {
            if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
                Ok((0..n)
                    .map(|i| {
                        if sa.is_null(i) {
                            None
                        } else {
                            Some(sa.value(i).to_string())
                        }
                    })
                    .collect())
            } else {
                exec_err!(
                    "trgm UDF: expected Utf8 array, got {:?}",
                    arr.data_type()
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Operator rewriter
// ---------------------------------------------------------------------------

/// Pre-parse text rewriter for `pg_trgm` infix operators.
///
/// Rewrites:
/// - `a % b`   →  `(similarity(a, b) >= <threshold>)`
///   where `<threshold>` is the session's `pg_trgm.similarity_threshold`
///   (passed as a float literal string, e.g. `"0.3"`).
///
/// - `a <% b`  →  `(word_similarity(a, b) >= <word_threshold>)`
///   PG semantics for `<%`: `word_similarity(needle, haystack) >= threshold`.
///   The operator is NOT symmetric — `a` is the needle, `b` the haystack.
///
/// - `a <-> b` →  `(1.0 - similarity(a, b))`
///   Trigram distance (smaller = more similar).  Useful in ORDER BY.
///   This rewriter runs AFTER `crate::udf::rewrite_vector_operators`, so any
///   `<->` that reaches here is guaranteed not to be a pgvector distance
///   expression (those are expanded to `l2_distance` first).
///
/// The scan is quote-aware: operator sequences inside single-quoted string
/// literals or double-quoted identifiers are NOT rewritten.  This protects
/// tsquery phrase operators (`'quick <-> fox'`) and percent signs inside
/// string literals (`'50%'`).
///
/// **% / modulo disambiguation:** when both extracted operands are bare
/// integer or float literals (`7 % 3`, `10 % 2.5`) the rewrite is skipped
/// and the expression is left for sqlparser/DataFusion to handle as modulo.
pub(crate) fn rewrite_trgm_operators(
    sql: &str,
    similarity_threshold: f32,
    word_similarity_threshold: f32,
) -> String {
    let sim_thresh = format!("{similarity_threshold}");
    let wsim_thresh = format!("{word_similarity_threshold}");

    // Process `<%` first (longer operator before `%` to avoid partial matches).
    let s = rewrite_trgm_op_once(sql, "<%", &wsim_thresh, TrgmOp::WordSimilarity);
    // Now `%` (modulo disambiguation applied internally).
    let s = rewrite_trgm_op_once(&s, "%", &sim_thresh, TrgmOp::Similarity);
    // Finally `<->` (trigram distance).
    rewrite_trgm_op_once(&s, "<->", "", TrgmOp::Distance)
}

#[derive(Clone, Copy)]
enum TrgmOp {
    /// `a % b` → `(similarity(a, b) >= threshold)`
    Similarity,
    /// `a <% b` → `(word_similarity(a, b) >= threshold)`
    WordSimilarity,
    /// `a <-> b` → `(1.0 - similarity(a, b))`
    Distance,
}

/// Single-pass rewriter for one operator variant.
fn rewrite_trgm_op_once(sql: &str, op: &str, threshold: &str, kind: TrgmOp) -> String {
    let mut s = sql.to_string();
    loop {
        let Some(op_start) = find_trgm_op_outside_strings(&s, op) else {
            break;
        };
        let op_end = op_start + op.len();

        let (lhs_start, lhs_end) = trgm_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = trgm_extract_right(&s, op_end);
        let lhs = s[lhs_start..lhs_end].trim();
        let rhs = s[rhs_start..rhs_end].trim();

        // `%` disambiguation: PG overloads `%` as BOTH integer modulo and the
        // pg_trgm similarity operator. We only know it is the trgm operator
        // when at least one operand is PROVABLY text — i.e. a single-quoted
        // string literal (`col % 'foo'`, `'bar' % col`). Everything else stays
        // modulo: `7 % 3` (both numeric), and critically `int_col % int_col2`
        // (two bare identifiers — which the old "rewrite unless both numeric"
        // gate misfired on, turning integer modulo into a type-error
        // similarity() call). A bare column `%` a string literal is the
        // canonical trgm form, so a single string-literal operand is sufficient
        // and necessary to opt into the rewrite.
        if matches!(kind, TrgmOp::Similarity)
            && !(is_string_literal(lhs) || is_string_literal(rhs))
        {
            // Not a provable-text `%` → leave it for sqlparser/DataFusion to
            // handle as modulo. Skip past this occurrence (recurse on the tail)
            // so any genuine trgm `%` later in the string is still rewritten,
            // without re-matching this one (which would infinite-loop).
            let prefix = s[..op_end].to_string();
            let tail = rewrite_trgm_op_once(&s[op_end..], op, threshold, kind);
            s = format!("{prefix}{tail}");
            break;
        }

        let replacement = match kind {
            TrgmOp::Similarity => {
                format!("(similarity({lhs}, {rhs}) >= {threshold})")
            }
            TrgmOp::WordSimilarity => {
                format!("(word_similarity({lhs}, {rhs}) >= {threshold})")
            }
            TrgmOp::Distance => {
                format!("(1.0 - similarity({lhs}, {rhs}))")
            }
        };

        s.replace_range(lhs_start..rhs_end, &replacement);
        // Continue — there may be more occurrences.
    }
    s
}

/// Return `true` if `expr` is a single-quoted SQL string literal (the
/// provable-text signal for `%` / trgm-vs-modulo disambiguation). A trimmed
/// operand that both starts and ends with `'` and has at least the two quote
/// bytes is text; anything else (identifier, number, function call, cast) is
/// not provably text and leaves `%` as modulo.
fn is_string_literal(expr: &str) -> bool {
    let s = expr.trim();
    s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'')
}

/// Return `true` if `expr` is a bare numeric literal (integer or float).
/// Retained as a classifier (and unit-tested below); the `%` gate now keys
/// off `is_string_literal` (rewrite only on provable text) rather than
/// "not both numeric", so this is no longer on the rewrite hot path.
#[cfg_attr(not(test), allow(dead_code))]
fn is_numeric(expr: &str) -> bool {
    let s = expr.trim();
    if s.is_empty() {
        return false;
    }
    // Allow leading minus.
    let s = s.strip_prefix('-').unwrap_or(s);
    let mut dot = false;
    for b in s.bytes() {
        if b == b'.' {
            if dot {
                return false;
            }
            dot = true;
        } else if !b.is_ascii_digit() {
            return false;
        }
    }
    !s.is_empty()
}

/// Find the first occurrence of `op` that is NOT inside a single-quoted string
/// literal or a double-quoted identifier.  Returns the byte offset of the first
/// character of `op`.
///
/// Quote-awareness: `'quick <-> fox'` and `'50%'` contain operator-like byte
/// sequences that must NOT be rewritten.
pub(crate) fn find_trgm_op_outside_strings(s: &str, op: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            // Single-quoted string literal: skip to the closing quote.
            b'\'' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if i + 1 < b.len() && b[i + 1] == b'\'' {
                            i += 2; // escaped quote
                        } else {
                            i += 1; // closing quote
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // Double-quoted identifier: skip.
            b'"' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if i + 1 < b.len() && b[i + 1] == b'"' {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            _ => {
                if s[i..].starts_with(op) {
                    return Some(i);
                }
                i += 1;
            }
        }
    }
    None
}

/// Walk back from `end` (start of the operator) and capture the left operand.
/// Returns `(start, end)` byte offsets.
fn trgm_extract_left(s: &str, end: usize) -> (usize, usize) {
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
        // Parenthesised group or function call — walk back through matching `(`.
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
        // Capture the function/keyword name before `(`.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else if last == b'\'' {
        // String literal — walk back to opening `'`.
        i -= 1; // consume closing quote
        while i > 0 {
            if bytes[i - 1] == b'\'' {
                if i >= 2 && bytes[i - 2] == b'\'' {
                    i -= 2;
                    continue;
                }
                i -= 1;
                break;
            }
            i -= 1;
        }
        // Consume any `::type` cast suffix before the literal.
        if i >= 2 && bytes[i - 1] == b':' && bytes[i - 2] == b':' {
            i -= 2;
            while i > 0 && bytes[i - 1].is_ascii_whitespace() {
                i -= 1;
            }
            // Skip the type name.
            while i > 0
                && (bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] == b'_')
            {
                i -= 1;
            }
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

/// Walk right from `start` (byte after the end of the operator) and capture
/// the right operand.  Returns `(start, end)` byte offsets.
fn trgm_extract_right(s: &str, start: usize) -> (usize, usize) {
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
        // Parenthesised group.
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
    } else if bytes[i] == b'\'' {
        // String literal.
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                i += 1;
                if i < bytes.len() && bytes[i] == b'\'' {
                    i += 1; // escaped
                } else {
                    break;
                }
            } else {
                i += 1;
            }
        }
        // Consume any trailing `::type` cast.
        if i + 2 <= bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            i += 2;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
        }
    } else {
        // Identifier / number run.
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric()
                || bytes[i] == b'_'
                || bytes[i] == b'.')
        {
            i += 1;
        }
        // If the identifier is followed by `(`, it's a function call.
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

// ---------------------------------------------------------------------------
// Engine::new hook
// ---------------------------------------------------------------------------

/// Called once from [`crate::Engine::new`].
///
/// The trgm UDFs are registered via `build_stateless_udf_cache`, not here.
/// This hook exists for any future process-level initialisation (e.g. loading
/// a trigram GIN index format) and as the named landing spot referenced by the
/// `trgm_glue` module doc.
#[inline]
pub(crate) fn install() {
    // Nothing to do at engine-construction time.  UDF registration happens in
    // `crate::session::build_stateless_udf_cache` via `register_trgm_udfs`.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_trgm::{DEFAULT_SIMILARITY_THRESHOLD, DEFAULT_WORD_SIMILARITY_THRESHOLD};

    const T: f32 = DEFAULT_SIMILARITY_THRESHOLD;      // 0.3
    const WT: f32 = DEFAULT_WORD_SIMILARITY_THRESHOLD; // 0.6

    // Helper: run rewrite with default thresholds.
    fn rw(sql: &str) -> String {
        rewrite_trgm_operators(sql, T, WT)
    }

    // ── similarity % ────────────────────────────────────────────────────────

    #[test]
    fn percent_text_literals_rewrites() {
        let out = rw("SELECT name FROM t WHERE name % 'alice'");
        assert!(
            out.contains("similarity(name, 'alice') >= 0.3"),
            "got: {out}"
        );
    }

    #[test]
    fn percent_integer_modulo_left_alone() {
        let out = rw("SELECT 7 % 3");
        // Must not be touched — both operands are numeric.
        assert!(out.contains("7 % 3"), "modulo was corrupted: {out}");
        assert!(!out.contains("similarity"), "modulo was rewritten as similarity: {out}");
    }

    #[test]
    fn percent_float_modulo_left_alone() {
        let out = rw("SELECT 10.5 % 2.0");
        assert!(out.contains("10.5 % 2.0"), "float modulo corrupted: {out}");
        assert!(!out.contains("similarity"), "float modulo rewritten: {out}");
    }

    #[test]
    fn percent_integer_column_modulo_left_alone() {
        // Two bare identifiers (integer columns) — neither is a string
        // literal, so `%` stays integer modulo. The old "rewrite unless both
        // numeric" gate misfired here, turning `a % b` into a type-error
        // similarity() call. The gate now requires a provable-text operand.
        let out = rw("SELECT * FROM t WHERE a % b = 0");
        assert!(out.contains("a % b = 0"), "integer-column modulo corrupted: {out}");
        assert!(
            !out.contains("similarity"),
            "integer-column modulo wrongly rewritten as similarity: {out}"
        );
    }

    #[test]
    fn percent_column_modulo_literal_left_alone() {
        // `col % <int literal>` (e.g. partition key bucketing) stays modulo —
        // no string operand.
        let out = rw("SELECT * FROM t WHERE id % 4 = 1");
        assert!(out.contains("id % 4 = 1"), "column-modulo-literal corrupted: {out}");
        assert!(!out.contains("similarity"), "column-modulo-literal rewritten: {out}");
    }

    #[test]
    fn percent_string_literal_either_side_rewrites() {
        // A single string-literal operand on EITHER side opts into the rewrite.
        let lhs = rw("SELECT * FROM t WHERE 'bar' % col");
        assert!(lhs.contains("similarity('bar', col) >= 0.3"), "lhs literal: {lhs}");
        let rhs = rw("SELECT * FROM t WHERE col % 'foo'");
        assert!(rhs.contains("similarity(col, 'foo') >= 0.3"), "rhs literal: {rhs}");
    }

    #[test]
    fn percent_inside_string_literal_left_alone() {
        let out = rw("SELECT '50%'");
        assert_eq!(out, "SELECT '50%'", "percent inside string was corrupted: {out}");
    }

    // ── word_similarity <% ────────────────────────────────────────────────────

    #[test]
    fn lt_percent_rewrites() {
        let out = rw("SELECT name FROM t WHERE 'smyth' <% name");
        assert!(
            out.contains("word_similarity('smyth', name) >= 0.6"),
            "got: {out}"
        );
    }

    #[test]
    fn lt_percent_does_not_consume_bare_percent() {
        // A query with both `<%` and `%` in different positions.
        let out = rw("SELECT 7 % 3 FROM t WHERE col <% 'foo'");
        // `<%` should rewrite; `7 % 3` should NOT.
        assert!(out.contains("word_similarity(col, 'foo') >= 0.6"), "got: {out}");
        assert!(out.contains("7 % 3"), "modulo corrupted: {out}");
    }

    // ── distance <-> ──────────────────────────────────────────────────────────

    #[test]
    fn distance_op_rewrites() {
        let out = rw("SELECT name FROM t ORDER BY name <-> 'alice'");
        assert!(
            out.contains("(1.0 - similarity(name, 'alice'))"),
            "got: {out}"
        );
    }

    #[test]
    fn distance_inside_string_literal_left_alone() {
        let out = rw("SELECT to_tsquery('english', 'quick <-> fox')");
        // The `<->` inside the string literal must not be rewritten.
        assert!(
            out.contains("'quick <-> fox'"),
            "phrase operator inside literal was corrupted: {out}"
        );
    }

    // ── custom threshold ──────────────────────────────────────────────────────

    #[test]
    fn custom_threshold_propagates() {
        let out = rewrite_trgm_operators("SELECT col % 'foo'", 0.5, 0.8);
        assert!(out.contains("similarity(col, 'foo') >= 0.5"), "got: {out}");
    }

    // ── is_numeric helper ─────────────────────────────────────────────────────

    #[test]
    fn is_numeric_recognises_integers() {
        assert!(is_numeric("42"));
        assert!(is_numeric("0"));
        assert!(is_numeric("-7"));
    }

    #[test]
    fn is_numeric_recognises_floats() {
        assert!(is_numeric("3.14"));
        assert!(is_numeric("-2.5"));
    }

    #[test]
    fn is_numeric_rejects_identifiers() {
        assert!(!is_numeric("col"));
        assert!(!is_numeric("'string'"));
        assert!(!is_numeric(""));
    }

    // ── quote-aware scanner ───────────────────────────────────────────────────

    #[test]
    fn find_op_skips_single_quoted_strings() {
        // `%` appears inside a string and outside.
        let s = "SELECT '50%' % 'foo'";
        // The first `%` outside a string is the operator between `'50%'` and `'foo'`.
        let pos = find_trgm_op_outside_strings(s, "%");
        assert!(pos.is_some());
        let p = pos.unwrap();
        // The outer `%` is after the closing `'` of `'50%'`.
        assert!(p > s.find("'50%'").unwrap() + 4, "found op inside string at pos {p}");
    }

    #[test]
    fn find_op_skips_double_quoted_identifiers() {
        let s = "SELECT \"a<->b\" <-> 'foo'";
        let pos = find_trgm_op_outside_strings(s, "<->");
        assert!(pos.is_some());
        let p = pos.unwrap();
        // The outer `<->` is past the double-quoted identifier.
        assert!(p > 14, "found op inside double-quoted identifier at pos {p}");
    }
}
