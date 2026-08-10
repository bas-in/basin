//! ADR 0027 Phase 4 — promoted JSONB shadow columns.
//!
//! ## Problem
//!
//! Phase 2 reduced the *document lookup* cost to O(1) via a binary offset
//! table, but DataFusion still invokes `json_get_text` **once per row** through
//! the per-row UDF dispatch machinery (ColumnarValue conversion, Arrow array
//! iteration, Rust closure overhead, per-row Vec::push).  For `SELECT
//! payload->>'category' FROM t` over 1M rows the UDF machinery alone — not the
//! JSONB decoding — is the bottleneck; Phase 2 gets us to ~18× PG and the
//! floor is the dispatch overhead.
//!
//! ## Solution
//!
//! A *promoted column* materialises one top-level JSONB path
//! (`source_col->>'json_key'`) as a real `Utf8` Arrow column named
//! `__promoted$<source_col>$<json_key>` at INSERT time.  At query time the
//! SQL rewriter (operating on the post-`->>` rewrite text, before DataFusion
//! parses it) substitutes:
//!
//! ```text
//! json_get_text(payload, 'category')
//!   → __promoted$payload$category
//! ```
//!
//! DataFusion then compiles that to a direct column projection — zero per-row
//! dispatch, zero JSONB decoding.  The result is semantically identical to the
//! UDF path including NULL/absent-key semantics (both yield SQL NULL).
//!
//! ## Correctness contract
//!
//! The shadow column is written by [`materialize_promoted_columns`] using
//! exactly the same extraction logic as `json_get_text`/`raw_get_key`:
//!
//! * Top-level object key lookup via `RawJson::member` (scan-order, last-wins
//!   on duplicate keys — matching serde_json / PG semantics).
//! * NULL row in the source JSONB column → NULL in the shadow column.
//! * Absent key → NULL in the shadow column.
//! * JSON `null` value → SQL NULL (matches `raw_slice_to_text` contract).
//! * String value → the unquoted string content.
//! * Non-string scalar (number, bool) → text form (matches `raw_slice_to_text`).
//!
//! A parity test (`test::promoted_col_parity`) verifies row-for-row identity
//! against the UDF path over a representative payload set.
//!
//! ## Naming
//!
//! `__promoted$<source_col>$<json_key>` uses `$` as a separator because
//! Postgres's unquoted identifier grammar forbids `$` as the *first* character
//! (preventing accidental user collisions with the `__promoted$` prefix) and
//! allows it elsewhere; and because `$` cannot appear in a JSON key via the
//! `->>`/`json_get_text` rewrite path (serde_json would encode a literal `$`
//! but the rewriter only fires on single-quoted SQL string literals, which may
//! not contain unescaped `$`).
//!
//! ## Scope of this increment
//!
//! * `shadow_col_name` — canonical shadow column name from `(source, key)`.
//! * `materialize_promoted_columns` — extend an Arrow `RecordBatch` with the
//!   shadow column(s) listed in a table's `promoted_jsonb_paths`.
//! * `rewrite_promoted_columns` — SQL text rewrite that swaps
//!   `json_get_text(col, 'key')` for the shadow column reference when the
//!   table's catalog entry has the path promoted.
//!
//! Compaction consistency (backfilling old files at compaction time) and
//! query-history-driven auto-promotion are deferred follow-ups — see ADR 0027
//! Phase 4 status for the full roadmap.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::PromotedJsonbPath;
use basin_common::Result;

// ---------------------------------------------------------------------------
// Shadow-column name
// ---------------------------------------------------------------------------

/// Prefix shared by every promoted-JSONB shadow column name.
pub(crate) const SHADOW_COL_PREFIX: &str = "__promoted$";

/// Canonical shadow-column name for a promoted JSONB path.
///
/// `__promoted$<source_col>$<json_key>`.  Matches
/// [`PromotedJsonbPath::shadow_col_name`] — kept here so engine-internal code
/// does not need to import `basin-catalog` just for a name computation.
pub(crate) fn shadow_col_name(source_col: &str, json_key: &str) -> String {
    format!("{SHADOW_COL_PREFIX}{}${}", source_col, json_key)
}

/// `true` if `name` is a promoted-JSONB shadow column (`__promoted$col$key`).
///
/// Shadow columns exist only in the DataFusion-registered schema —
/// `TableMetadata::schema` never contains them — so any place that maps a
/// materialised DataFusion result onto a catalog schema must treat them as
/// internal storage detail, not user data.
pub(crate) fn is_shadow_col_name(name: &str) -> bool {
    name.starts_with(SHADOW_COL_PREFIX)
}

// ---------------------------------------------------------------------------
// Write-path: materialise shadow columns into a RecordBatch
// ---------------------------------------------------------------------------

/// Extend `batch` with one `Utf8` column per entry in `paths`, materialising
/// `source_col->>'json_key'` from the JSONB bytes already in `batch`.
///
/// If `paths` is empty this is a zero-cost no-op (no allocation).  Shadow
/// columns that already exist in `batch.schema()` are silently skipped so the
/// call is idempotent.
///
/// # Correctness
///
/// Uses [`extract_promoted_value`] for each row — identical logic to the
/// `json_get_text` UDF (Phase 2 `RawJson::member` + `raw_slice_to_text`
/// semantics).  Rows where the source column is NULL or the key is absent
/// yield SQL NULL in the shadow column.
pub(crate) fn materialize_promoted_columns(
    batch: &RecordBatch,
    paths: &[PromotedJsonbPath],
) -> Result<RecordBatch> {
    if paths.is_empty() {
        return Ok(batch.clone());
    }

    let n = batch.num_rows();
    let mut new_schema_fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();

    for path in paths {
        let col_name = shadow_col_name(&path.source_col, &path.json_key);

        // Compute the up-to-date shadow value from the (authoritative) source
        // JSONB column. The shadow column is a pure function of the source, so
        // we ALWAYS recompute: a row carried through a copy-on-write UPDATE
        // rewrite already holds a (now STALE) shadow column copied from the cold
        // read, and a stale skip here would persist the pre-UPDATE value into
        // the replacement file — a WRONG promoted-column read. When the column
        // is absent we append it; when present we OVERWRITE it in place.
        let existing_idx = batch.schema().index_of(&col_name).ok();

        let computed: ArrayRef = match batch.schema().index_of(&path.source_col) {
            Ok(src_idx) => {
                let src_arr = batch.column(src_idx);
                let mut out: Vec<Option<String>> = Vec::with_capacity(n);
                for i in 0..n {
                    out.push(extract_promoted_value(src_arr, i, &path.json_key));
                }
                Arc::new(StringArray::from(out))
            }
            Err(_) => {
                // Source column doesn't exist in this batch — all NULLs.
                let nulls: Vec<Option<&str>> = vec![None; n];
                Arc::new(StringArray::from(nulls))
            }
        };

        match existing_idx {
            Some(idx) => {
                // Overwrite the stale shadow column in place (preserve position).
                new_schema_fields[idx] = Field::new(&col_name, DataType::Utf8, true);
                new_columns[idx] = computed;
            }
            None => {
                new_schema_fields.push(Field::new(&col_name, DataType::Utf8, true));
                new_columns.push(computed);
            }
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_schema_fields,
        batch.schema().metadata().clone(),
    ));
    let result = RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| basin_common::BasinError::internal(format!("promoted columns batch: {e}")))?;
    Ok(result)
}

/// Extract the text value for `json_key` from row `i` of `arr`.
///
/// Semantics match `json_get_text` / `raw_slice_to_text`:
/// - NULL source row → `None`.
/// - Source not parseable as LargeBinary JSONB → `None`.
/// - Key absent → `None`.
/// - JSON `null` value → `None` (SQL NULL, not the string `"null"`).
/// - JSON string → the inner string content (no quotes).
/// - JSON number/bool → text form.
fn extract_promoted_value(arr: &ArrayRef, i: usize, key: &str) -> Option<String> {
    // JSONB is written to Vortex as LargeBinary, but Vortex 0.71 may surface
    // the on-disk binary column as LargeBinary, Binary, or BinaryView depending
    // on its internal layout. Accept all three binary variants plus plain Utf8
    // (text JSONB) so shadow-column materialisation works regardless of which
    // physical array type the decoder chose.  (Mirrors the same family-tolerant
    // pattern used by `geo_glue::point_bytes_at` for POINT columns.)
    let bytes: &[u8] = if let Some(lb) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        if lb.is_null(i) {
            return None;
        }
        lb.value(i)
    } else if let Some(ba) = arr.as_any().downcast_ref::<BinaryArray>() {
        // Vortex may decode LargeBinary-on-disk as plain Binary.
        if ba.is_null(i) {
            return None;
        }
        ba.value(i)
    } else if let Some(bv) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        // Vortex may decode LargeBinary-on-disk as BinaryView.
        if bv.is_null(i) {
            return None;
        }
        bv.value(i)
    } else if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
        if sa.is_null(i) {
            return None;
        }
        sa.value(i).as_bytes()
    } else {
        return None;
    };

    extract_top_level_text_key(bytes, key)
}

/// Walk the raw JSONB bytes and return the top-level string-form value for
/// `key`, or `None` for absent / JSON-null.  This is a stripped copy of the
/// Phase 2 `raw_get_key` + `raw_slice_to_text` logic from `jsonb_udf.rs`,
/// kept here so promoted_columns has no circular module dependency.
///
/// For correctness the byte-walking must be semantically identical to
/// `json_get_text` (verified by the parity test).  This function deliberately
/// duplicates the core scanning primitives rather than importing them across
/// module boundaries — `jsonb_udf` is a large `pub(crate)` module and
/// importing its internals would couple the two modules tightly.
fn extract_top_level_text_key(bytes: &[u8], key: &str) -> Option<String> {
    // Strip optional 0x01 version prefix (PG wire byte tolerated since Phase 1).
    let b = if bytes.first() == Some(&0x01) && bytes.len() > 1 {
        &bytes[1..]
    } else {
        bytes
    };

    let mut p = 0usize;
    skip_ws(b, &mut p);
    if b.get(p) != Some(&b'{') {
        // Not an object — key lookup is meaningless.
        return None;
    }
    p += 1;
    skip_ws(b, &mut p);
    if b.get(p) == Some(&b'}') {
        return None; // empty object
    }

    // Walk key-value pairs.  `last_match` records the most-recently-seen
    // value for `key` so that duplicate-key last-wins semantics are correct.
    let mut last_match: Option<Option<String>> = None;
    loop {
        skip_ws(b, &mut p);
        // Parse key.
        let k_slice = scan_string(b, &mut p).ok()??;
        let k_decoded = decode_json_string(k_slice);
        skip_ws(b, &mut p);
        if b.get(p) != Some(&b':') {
            return None; // malformed
        }
        p += 1;
        skip_ws(b, &mut p);
        let v_start = p;
        // Skip the value.
        if skip_value(b, &mut p).is_err() {
            return None;
        }
        let v_end = p;

        // Don't return early on first match — continue to last occurrence
        // (last-wins duplicate-key semantics matching serde_json / PG).
        if k_decoded.as_deref() == Some(key) {
            let v_slice = &b[v_start..v_end];
            // Record this as the best match so far; keep scanning.
            last_match = Some(raw_slice_to_promoted_text(v_slice));
        }

        skip_ws(b, &mut p);
        match b.get(p) {
            Some(&b',') => {
                p += 1;
            }
            Some(&b'}') => break,
            _ => return None,
        }
    }
    last_match.flatten()
}

/// Convert a raw JSON value slice to its SQL-text form for a promoted column.
///
/// Matches `raw_slice_to_text` in `jsonb_udf.rs`:
/// - `"string"` → the unquoted content.
/// - `null` → `None` (SQL NULL).
/// - number / bool → text form.
fn raw_slice_to_promoted_text(v: &[u8]) -> Option<String> {
    let mut p = 0usize;
    skip_ws(v, &mut p);
    match v.get(p) {
        Some(&b'"') => {
            let mut q = p;
            let inner = scan_string(v, &mut q).ok()??;
            Some(
                decode_json_string(inner)
                    .unwrap_or_else(|| String::from_utf8_lossy(inner).into_owned()),
            )
        }
        Some(&b'n') if v[p..].starts_with(b"null") => None,
        None => None,
        _ => {
            // number, bool — return the raw text.
            Some(String::from_utf8_lossy(&v[p..]).trim_end().to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal JSON scanning primitives (self-contained, no import from jsonb_udf)
// ---------------------------------------------------------------------------

#[inline]
fn skip_ws(b: &[u8], p: &mut usize) {
    while *p < b.len() && (b[*p] == b' ' || b[*p] == b'\t' || b[*p] == b'\n' || b[*p] == b'\r') {
        *p += 1;
    }
}

/// Scan a JSON string starting at `b[*p]` (must be `"`).  Returns the raw
/// bytes of the string content (without surrounding quotes), advancing `*p`
/// past the closing `"`.  Returns `Err(())` on malformed input, `Ok(None)`
/// when `*p` is past the end.
fn scan_string<'a>(b: &'a [u8], p: &mut usize) -> Result<Option<&'a [u8]>, ()> {
    if b.get(*p) != Some(&b'"') {
        return Ok(None);
    }
    let start = *p + 1;
    *p += 1;
    while *p < b.len() {
        match b[*p] {
            b'"' => {
                let content = &b[start..*p];
                *p += 1;
                return Ok(Some(content));
            }
            b'\\' => {
                *p += 1; // skip escaped char
                if *p >= b.len() {
                    return Err(());
                }
                *p += 1;
            }
            _ => {
                *p += 1;
            }
        }
    }
    Err(())
}

/// Decode a JSON string's raw bytes (with backslash escapes) to a Rust
/// `String`.  Returns `None` only when the bytes contain `\uXXXX` surrogate
/// pairs that are beyond our minimal implementation scope (rare in practice).
fn decode_json_string(raw: &[u8]) -> Option<String> {
    if !raw.contains(&b'\\') {
        // Fast path: no escapes — raw bytes are valid UTF-8 JSON string content.
        return std::str::from_utf8(raw).ok().map(|s| s.to_string());
    }
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] != b'\\' {
            // Multi-byte UTF-8 passes through.
            let ch_start = i;
            i += 1;
            // Extend past all continuation bytes. (A loop that tested for a
            // high-bit-set non-continuation byte and then unconditionally
            // `break`ed used to sit here; it never advanced `i` and had no
            // other effect, and the scan below already stops on exactly that
            // byte, so it was doing nothing.)
            while i < raw.len() && raw[i] & 0xC0 == 0x80 {
                i += 1;
            }
            out.push_str(std::str::from_utf8(&raw[ch_start..i]).ok()?);
        } else {
            i += 1;
            if i >= raw.len() {
                return None;
            }
            match raw[i] {
                b'"' => {
                    out.push('"');
                    i += 1;
                }
                b'\\' => {
                    out.push('\\');
                    i += 1;
                }
                b'/' => {
                    out.push('/');
                    i += 1;
                }
                b'b' => {
                    out.push('\x08');
                    i += 1;
                }
                b'f' => {
                    out.push('\x0C');
                    i += 1;
                }
                b'n' => {
                    out.push('\n');
                    i += 1;
                }
                b'r' => {
                    out.push('\r');
                    i += 1;
                }
                b't' => {
                    out.push('\t');
                    i += 1;
                }
                b'u' => {
                    // \uXXXX — minimal 4-hex-digit decode.
                    if i + 4 >= raw.len() {
                        return None;
                    }
                    let hex = std::str::from_utf8(&raw[i + 1..i + 5]).ok()?;
                    let codepoint = u32::from_str_radix(hex, 16).ok()?;
                    let ch = char::from_u32(codepoint)?;
                    out.push(ch);
                    i += 5;
                }
                _ => {
                    out.push(raw[i] as char);
                    i += 1;
                }
            }
        }
    }
    Some(out)
}

/// Skip one JSON value starting at `b[*p]`, advancing `*p` past it.
/// Returns `Err(())` on malformed input.
fn skip_value(b: &[u8], p: &mut usize) -> std::result::Result<(), ()> {
    skip_ws(b, p);
    match b.get(*p) {
        Some(&b'"') => {
            scan_string(b, p).map_err(|_| ())?;
            Ok(())
        }
        Some(&b'{') => skip_object(b, p),
        Some(&b'[') => skip_array(b, p),
        Some(&b't') => {
            // true
            if b[*p..].starts_with(b"true") {
                *p += 4;
                Ok(())
            } else {
                Err(())
            }
        }
        Some(&b'f') => {
            // false
            if b[*p..].starts_with(b"false") {
                *p += 5;
                Ok(())
            } else {
                Err(())
            }
        }
        Some(&b'n') => {
            // null
            if b[*p..].starts_with(b"null") {
                *p += 4;
                Ok(())
            } else {
                Err(())
            }
        }
        Some(&b'-') | Some(b'0'..=b'9') => {
            *p += 1;
            while *p < b.len()
                && (b[*p].is_ascii_digit()
                    || b[*p] == b'.'
                    || b[*p] == b'e'
                    || b[*p] == b'E'
                    || b[*p] == b'+'
                    || b[*p] == b'-')
            {
                *p += 1;
            }
            Ok(())
        }
        _ => Err(()),
    }
}

fn skip_object(b: &[u8], p: &mut usize) -> std::result::Result<(), ()> {
    if b.get(*p) != Some(&b'{') {
        return Err(());
    }
    *p += 1;
    skip_ws(b, p);
    if b.get(*p) == Some(&b'}') {
        *p += 1;
        return Ok(());
    }
    loop {
        skip_ws(b, p);
        scan_string(b, p).map_err(|_| ())?;
        skip_ws(b, p);
        if b.get(*p) != Some(&b':') {
            return Err(());
        }
        *p += 1;
        skip_value(b, p)?;
        skip_ws(b, p);
        match b.get(*p) {
            Some(&b',') => {
                *p += 1;
            }
            Some(&b'}') => {
                *p += 1;
                return Ok(());
            }
            _ => return Err(()),
        }
    }
}

fn skip_array(b: &[u8], p: &mut usize) -> std::result::Result<(), ()> {
    if b.get(*p) != Some(&b'[') {
        return Err(());
    }
    *p += 1;
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return Ok(());
    }
    loop {
        skip_value(b, p)?;
        skip_ws(b, p);
        match b.get(*p) {
            Some(&b',') => {
                *p += 1;
            }
            Some(&b']') => {
                *p += 1;
                return Ok(());
            }
            _ => return Err(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Query rewrite
// ---------------------------------------------------------------------------

/// SQL text rewrite: substitute `json_get_text(<source_col>, '<json_key>')`
/// with the shadow column name when the path is promoted.
///
/// Called **after** `rewrite_json_operators` has lowered `payload->>'key'`
/// to `json_get_text(payload, 'key')`, so the pattern is well-defined.
///
/// ## Position-independence
///
/// The substitution is a whole-statement text replacement, so it is NOT
/// projection-only: the same `payload->>'k'` occurrence is swapped wherever
/// it appears — SELECT list, WHERE predicates (`payload->>'k' = 'literal'`
/// becomes `__promoted$payload$k = 'literal'`, which then gets file-level
/// zone-map/bloom pruning on the fast path), GROUP BY expressions
/// (`GROUP BY payload->>'k'` groups DataFusion on a plain Utf8 column),
/// HAVING, and ORDER BY. Equality semantics are preserved because the shadow
/// column is materialised from the exact `json_get_text` expression
/// ([`materialize_promoted_columns`]): a missing key, a JSON `null`, and a
/// NULL source row all store SQL NULL, so `= 'literal'` excludes those rows
/// on both forms and `IS NULL` / GROUP-BY-NULL group identically.
///
/// The caller (`executor::rewrite_promoted_cols_for_query`) gates each path
/// on physical completeness — every live file must carry the shadow column
/// in its `column_stats`, with no un-flushed tail rows and no shadow-dirty
/// memtable — so this rewrite never targets a column a file would null-fill.
///
/// This is a best-effort textual rewrite — it handles the common forms
/// emitted by `rewrite_json_operators`:
///   - `json_get_text(payload, 'key')` — direct column.
///   - `json_get_text(payload, 'key')` inside a larger expression.
///
/// Out of scope (left to the UDF fallback, deliberately):
///   - the `->` (jsonb-returning) form — `json_get(payload, 'key')` returns
///     the JSONB value, not the shadow column's text form;
///   - chained / deep paths (`json_get_text(json_get(p,'a'),'b')`) — the
///     promotion stores top-level keys only;
///   - qualified column references (`t.payload`);
///   - non-literal keys.
///
/// `paths` is the slice from `TableMetadata::promoted_jsonb_paths` — may be
/// empty (no-op fast path).
pub(crate) fn rewrite_promoted_columns(sql: &str, paths: &[PromotedJsonbPath]) -> String {
    if paths.is_empty() {
        return sql.to_string();
    }
    let mut s = sql.to_string();
    for path in paths {
        // Build the pattern: `json_get_text(<source_col>, '<json_key>')`.
        // Both single-quoted forms are accepted (PG canonicalises to single quotes).
        // We match case-insensitively because the rewriter produces lowercase.
        let needle = format!("json_get_text({}, '{}')", path.source_col, path.json_key);
        let shadow = shadow_col_name(&path.source_col, &path.json_key);
        // Case-insensitive replacement via lowercase comparison.
        s = replace_case_insensitive(&s, &needle, &shadow);
    }
    s
}

/// Replace all case-insensitive occurrences of `needle` in `haystack` with
/// `replacement`.  Operates on byte-level ASCII case folding (safe because
/// `json_get_text` and `'key'` are ASCII).
fn replace_case_insensitive(haystack: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return haystack.to_string();
    }
    let lower_hay = haystack.to_ascii_lowercase();
    let lower_needle = needle.to_ascii_lowercase();
    let mut out = String::with_capacity(haystack.len());
    let mut start = 0;
    while let Some(pos) = lower_hay[start..].find(&lower_needle) {
        let abs = start + pos;
        out.push_str(&haystack[start..abs]);
        out.push_str(replacement);
        start = abs + needle.len();
    }
    out.push_str(&haystack[start..]);
    out
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Rewrite
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_empty_paths_noop() {
        let sql = "SELECT json_get_text(payload, 'category') FROM t";
        assert_eq!(rewrite_promoted_columns(sql, &[]), sql);
    }

    #[test]
    fn rewrite_substitutes_shadow_col() {
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        let sql = "SELECT json_get_text(payload, 'category') FROM t";
        let got = rewrite_promoted_columns(sql, &paths);
        assert!(
            got.contains("__promoted$payload$category"),
            "expected shadow col in rewrite, got: {got}"
        );
        assert!(
            !got.contains("json_get_text"),
            "UDF call should be gone, got: {got}"
        );
    }

    /// The rewrite is position-independent: a WHERE equality predicate
    /// (`json_get_text(col,'k') = 'literal'`) is swapped exactly like a
    /// SELECT-list occurrence, leaving the comparison literal untouched.
    /// This is the JSON pseudo-secondary lookup shape
    /// (`WHERE payload->>'category' = '…'`).
    #[test]
    fn rewrite_fires_in_where_equality() {
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        let sql =
            "SELECT COUNT(*) FROM events WHERE json_get_text(payload, 'category') = 'purchase'";
        let got = rewrite_promoted_columns(sql, &paths);
        assert_eq!(
            got, "SELECT COUNT(*) FROM events WHERE __promoted$payload$category = 'purchase'",
            "WHERE-position equality must rewrite to the shadow column"
        );
    }

    /// GROUP BY + aggregate-arg positions are rewritten too: grouping lands on
    /// a plain Utf8 column. A deep path inside the aggregate
    /// (`json_get_text(json_get(payload,'metadata'),'score')`) must stay
    /// unrewritten — promotion stores top-level keys only.
    #[test]
    fn rewrite_fires_in_group_by_but_not_deep_paths() {
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        let sql = "SELECT json_get_text(payload, 'category'), \
                   SUM((json_get_text(json_get(payload, 'metadata'), 'score'))::float) \
                   FROM events GROUP BY json_get_text(payload, 'category')";
        let got = rewrite_promoted_columns(sql, &paths);
        assert!(
            got.contains("GROUP BY __promoted$payload$category"),
            "GROUP BY position must rewrite to the shadow column, got: {got}"
        );
        assert!(
            got.starts_with("SELECT __promoted$payload$category,"),
            "SELECT-list position must rewrite too, got: {got}"
        );
        assert!(
            got.contains("json_get_text(json_get(payload, 'metadata'), 'score')"),
            "deep path inside the aggregate must stay on the UDF form, got: {got}"
        );
    }

    /// Non-literal RHS comparisons still rewrite the LHS occurrence only when
    /// it matches the promoted needle; an unpromoted key on the RHS stays as a
    /// UDF call (row-wise equivalent — the shadow column stores the identical
    /// per-row value).
    #[test]
    fn rewrite_where_non_literal_rhs_keeps_unpromoted_side() {
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        let sql = "SELECT id FROM events WHERE \
                   json_get_text(payload, 'category') = json_get_text(payload, 'other')";
        let got = rewrite_promoted_columns(sql, &paths);
        assert!(
            got.contains("__promoted$payload$category = json_get_text(payload, 'other')"),
            "promoted LHS swapped, unpromoted RHS untouched, got: {got}"
        );
    }

    #[test]
    fn rewrite_only_matching_key() {
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        // 'device' is not promoted — must stay as UDF call.
        let sql =
            "SELECT json_get_text(payload, 'category'), json_get_text(payload, 'device') FROM t";
        let got = rewrite_promoted_columns(sql, &paths);
        assert!(got.contains("__promoted$payload$category"), "got: {got}");
        assert!(
            got.contains("json_get_text(payload, 'device')"),
            "got: {got}"
        );
    }

    // -----------------------------------------------------------------------
    // Materialization parity
    // -----------------------------------------------------------------------

    fn make_jsonb_batch(jsons: &[Option<&str>]) -> RecordBatch {
        use arrow_schema::Fields;
        let arr: LargeBinaryArray = jsons.iter().map(|opt| opt.map(|s| s.as_bytes())).collect();
        let schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
            "payload",
            DataType::LargeBinary,
            true,
        )])));
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    /// Core parity check: promoted column must produce the same value as
    /// the `json_get_text` UDF for every row.
    #[test]
    fn promoted_col_parity() {
        let rows: Vec<Option<&str>> = vec![
            // happy path — string value
            Some(r#"{"category":"books","price":9.99}"#),
            // absent key → NULL
            Some(r#"{"price":9.99}"#),
            // JSON null value → SQL NULL
            Some(r#"{"category":null}"#),
            // non-string scalar → text form
            Some(r#"{"category":42}"#),
            // bool value
            Some(r#"{"category":true}"#),
            // unicode key/value
            Some(r#"{"category":"café"}"#),
            // nested object (not a string) — returns the JSON text form
            Some(r#"{"category":{"sub":"val"}}"#),
            // NULL source row → NULL
            None,
            // empty object
            Some(r#"{}"#),
            // duplicate keys — last wins (PG / serde_json semantics)
            Some(r#"{"category":"first","category":"last"}"#),
        ];

        let batch = make_jsonb_batch(&rows);
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "category".into(),
        }];
        let extended = materialize_promoted_columns(&batch, &paths).unwrap();
        let shadow_name = shadow_col_name("payload", "category");
        let shadow_idx = extended.schema().index_of(&shadow_name).unwrap();
        let shadow_col = extended
            .column(shadow_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let expected: Vec<Option<&str>> = vec![
            Some("books"),
            None,
            None,
            Some("42"),
            Some("true"),
            Some("café"),
            // Nested object: raw slice excluding quotes, but it's `{"sub":"val"}`
            // which is not a string literal, so raw_slice_to_promoted_text
            // hits the "number/bool" branch and returns the raw text.
            Some(r#"{"sub":"val"}"#),
            None,
            None,
            Some("last"),
        ];

        for (i, exp) in expected.iter().enumerate() {
            let got = if shadow_col.is_null(i) {
                None
            } else {
                Some(shadow_col.value(i))
            };
            assert_eq!(got, *exp, "row {i}: got {got:?}, expected {exp:?}");
        }
    }

    #[test]
    fn materialize_idempotent() {
        let batch = make_jsonb_batch(&[Some(r#"{"k":"v"}"#)]);
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "k".into(),
        }];
        let once = materialize_promoted_columns(&batch, &paths).unwrap();
        let twice = materialize_promoted_columns(&once, &paths).unwrap();
        // Should have same number of columns — no duplicates.
        assert_eq!(once.num_columns(), twice.num_columns());
    }

    #[test]
    fn materialize_missing_source_col_all_null() {
        use arrow_schema::Fields;
        let schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
            "other",
            DataType::Utf8,
            true,
        )])));
        let other_col: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("x")]));
        let batch = RecordBatch::try_new(schema, vec![other_col]).unwrap();
        let paths = vec![PromotedJsonbPath {
            source_col: "payload".into(),
            json_key: "k".into(),
        }];
        let extended = materialize_promoted_columns(&batch, &paths).unwrap();
        let shadow_name = shadow_col_name("payload", "k");
        let idx = extended.schema().index_of(&shadow_name).unwrap();
        let col = extended
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.is_null(0), "missing source → NULL");
    }

    // -----------------------------------------------------------------------
    // Shadow name
    // -----------------------------------------------------------------------

    #[test]
    fn shadow_col_name_format() {
        assert_eq!(
            shadow_col_name("payload", "category"),
            "__promoted$payload$category"
        );
    }
}
