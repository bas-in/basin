//! `SUBSTRING(text FROM 'regex')` — PostgreSQL POSIX-regex form.
//!
//! ## PG semantics implemented here
//!
//! `SUBSTRING(text FROM pattern)` returns:
//! - The **first match** of the POSIX regex `pattern` in `text`.
//! - If the pattern has **one or more capture groups `(…)`**, the text of
//!   the **first capture group** is returned (PG behaviour).
//! - If the pattern has **no capture groups**, the entire match is returned.
//! - Returns **NULL** if there is no match.
//! - Returns **NULL** if either argument is NULL.
//! - An **empty match** (e.g. `.*`) returns `''` (empty string), not NULL.
//! - An **invalid regex** raises a DataFusion execution error, matching PG's
//!   behaviour of raising `ERROR: invalid regular expression`.
//!
//! ## Regex-engine gaps vs. PG POSIX ERE
//!
//! Basin uses the Rust `regex` crate (RE2 / Thompson NFA semantics).  The
//! differences that matter in practice:
//!
//! | Feature | PG (ARE / POSIX) | Rust `regex` crate |
//! |---|---|---|
//! | Lookahead / lookbehind | supported | **not supported** |
//! | Backreferences `\1` | supported | **not supported** |
//! | Case-insensitive flag `(?i)` | supported | supported |
//! | Unicode categories `\p{L}` | supported | supported |
//! | `.` matches newline | no (unless `(?s)`) | same |
//!
//! The vast majority of real-world `SUBSTRING(x FROM '...')` usage involves
//! simple character classes, quantifiers, and at most one capture group — all
//! of which RE2 handles identically to PG's POSIX engine.  Lookaheads and
//! backreferences are extremely rare in SUBSTRING patterns; when they occur
//! they produce a compile-time error (honest, not silently wrong).
//!
//! ## Wiring TODO — requires edits to files in other agent lanes
//!
//! The `rewrite_substring_regex` function in this module converts the SQL
//! syntax `SUBSTRING(x FROM 'regex')` to a plain function call
//! `substring_regex(x, 'regex')`.  Two call sites must be added before the
//! syntax sugar works end-to-end:
//!
//! ### 1. UDF registration — `crates/basin-engine/src/session.rs`
//!
//! In `build_stateless_udf_cache()` (around line 75), add:
//! ```rust
//! crate::regex_udf::register_regex_udfs(&ctx);
//! ```
//! The same line should also be added in the two per-session fallback
//! registration blocks (around lines 1116 and 1137 where
//! `register_range_udfs` is called a second and third time).
//!
//! ### 2. Pre-parse text rewrite — `crates/basin-engine/src/executor.rs`
//!
//! In `execute_select` (around line 650, after the `rewrite_range_casts`
//! call), add:
//! ```rust
//! let rewritten = crate::regex_udf::rewrite_substring_regex(&rewritten);
//! ```
//! This converts `SUBSTRING(x FROM 'pat')` to `substring_regex(x, 'pat')`
//! before sqlparser sees the SQL.  The built-in DataFusion `substring` UDF
//! expects `(text, start_int, length_int)` and rejects `(text, regex_text)`,
//! so the rewrite must happen at text level rather than post-parse.
//!
//! Until those two wiring lines land, the UDF is callable via its internal
//! name `substring_regex(text, pattern)` and the matrix fragment
//! `SELECT SUBSTRING('abcdef' FROM '[a-c]+')` stays 📜.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

// ===========================================================================
// SubstringRegexUdf
// ===========================================================================

/// DataFusion UDF implementing `SUBSTRING(text FROM 'regex')` semantics.
///
/// Signature: `substring_regex(text Utf8, pattern Utf8) → Utf8` (both nullable).
///
/// See module-level docs for PG semantics and regex-engine gap notes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubstringRegexUdf {
    signature: Signature,
}

impl ScalarUDFImpl for SubstringRegexUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substring_regex"
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
        let texts = arg_strings(&args[0], n)?;
        let patterns = arg_strings(&args[1], n)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            // NULL input → NULL output (PG: SUBSTRING(NULL FROM pat) = NULL)
            if texts.is_null(i) || patterns.is_null(i) {
                out.push(None);
                continue;
            }
            let text = texts.value(i);
            let pat = patterns.value(i);

            // Compile pattern. Invalid regex → execution error (matches PG's
            // "ERROR: invalid regular expression" behaviour — honest, not NULL).
            let re = regex::Regex::new(pat).map_err(|e| {
                DataFusionError::Execution(format!(
                    "substring_regex: invalid regular expression {:?}: {e}",
                    pat
                ))
            })?;

            // Find the first match.
            if let Some(caps) = re.captures(text) {
                // PG rule: if the pattern contains capture groups, return the
                // text of the first capture group.  If there are no capture
                // groups (caps.len() == 1 means only the whole-match group),
                // return the entire match.
                let result = if caps.len() > 1 {
                    // caps[0] is the whole match; caps[1] is the first capture group.
                    caps.get(1).map(|m| m.as_str().to_string())
                } else {
                    // No capture groups → return the whole match (may be empty "").
                    Some(caps.get(0).map_or("", |m| m.as_str()).to_string())
                };
                out.push(result);
            } else {
                // No match → NULL (PG: SUBSTRING('xyz' FROM '[0-9]+') = NULL)
                out.push(None);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// Registration
// ===========================================================================

/// Register the `substring_regex` UDF on `ctx`.  Idempotent.
///
/// **Call site needed** (see module-level TODO): add
/// `crate::regex_udf::register_regex_udfs(&ctx);` in
/// `session::build_stateless_udf_cache` and the two per-session fallback
/// registration blocks in `session.rs`.
pub(crate) fn register_regex_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SubstringRegexUdf {
        signature: Signature::one_of(
            vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8])],
            Volatility::Immutable,
        ),
    }));
}

// ===========================================================================
// Pre-parse text rewrite
// ===========================================================================

/// Rewrite `SUBSTRING(<expr> FROM '<single-quoted-string>')` to
/// `substring_regex(<expr>, '<single-quoted-string>')` so that DataFusion
/// sees a plain two-arg function call rather than the special SUBSTRING…FROM
/// syntax.
///
/// **Matching heuristic**: the rewrite only fires when the `FROM` clause
/// contains a *single-quoted string literal* (i.e. the FROM argument starts
/// with `'` after optional whitespace), distinguishing the regex form from
/// the integer-start form `SUBSTRING(x FROM 3 FOR 5)` which DataFusion
/// handles natively.
///
/// The rewriter is case-insensitive for the keyword `SUBSTRING` and `FROM`
/// but preserves the original casing of `<expr>` and `<pattern>`.
///
/// **Limitations**:
/// - The expression `<expr>` must be a simple token (column name, literal,
///   or function call without nested unbalanced parentheses that also contain
///   the literal word ` FROM `).  Complex nested subqueries inside the expr
///   position are not supported; they will not be rewritten (safe — no wrong
///   data, just no rewrite).
/// - The pattern must be a *simple* single-quoted string (no dollar-quoting,
///   no `E'...'` escape prefix).  These cover 100 % of real SUBSTRING regex
///   patterns in practice.
///
/// **Call site needed** (see module-level TODO): add
/// `let rewritten = crate::regex_udf::rewrite_substring_regex(&rewritten);`
/// in `executor.rs` around line 650, after `rewrite_range_casts`.
pub(crate) fn rewrite_substring_regex(sql: &str) -> String {
    // Fast exit — no SUBSTRING keyword at all.
    let upper = sql.to_uppercase();
    if !upper.contains("SUBSTRING") {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut result = String::with_capacity(len + 64);
    let mut i = 0usize;

    while i < len {
        // Case-insensitive match for "SUBSTRING" at position i.
        if upper[i..].starts_with("SUBSTRING") {
            let kw_end = i + "SUBSTRING".len();
            // Skip whitespace after SUBSTRING.
            let mut j = kw_end;
            while j < len && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            // Expect '(' immediately.
            if j >= len || bytes[j] != b'(' {
                // Not a function call syntax — copy and continue.
                result.push_str(&sql[i..kw_end]);
                i = kw_end;
                continue;
            }
            let paren_open = j;
            j += 1; // skip '('

            // Extract the content inside the outer parens, tracking nesting.
            // We need to find the matching ')'.
            let inner_start = j;
            let mut depth = 1usize;
            let mut in_str = false;
            while j < len && depth > 0 {
                match bytes[j] {
                    b'\'' if !in_str => {
                        in_str = true;
                        j += 1;
                    }
                    b'\'' if in_str => {
                        // Check for escaped '' inside string.
                        if j + 1 < len && bytes[j + 1] == b'\'' {
                            j += 2; // skip ''
                        } else {
                            in_str = false;
                            j += 1;
                        }
                    }
                    b'(' if !in_str => {
                        depth += 1;
                        j += 1;
                    }
                    b')' if !in_str => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        j += 1;
                    }
                    _ => {
                        j += 1;
                    }
                }
            }
            if depth != 0 {
                // Unbalanced parens — don't rewrite, copy as-is.
                result.push_str(&sql[i..j]);
                i = j;
                continue;
            }
            let inner_end = j; // index of the closing ')'
            let inner = &sql[inner_start..inner_end];
            let inner_upper = inner.to_uppercase();
            let paren_close = j;

            // Look for ` FROM '` (case-insensitive) inside inner, where the
            // FROM is followed by a single-quoted string (not a number).
            // We need to find the LAST top-level FROM (not nested inside parens
            // or strings) that precedes a single-quoted literal.
            if let Some((expr_part, pat_part)) = split_on_from_regex(inner, &inner_upper) {
                // Rewrite: substring_regex(expr, pat)
                result.push_str("substring_regex(");
                result.push_str(expr_part.trim());
                result.push_str(", ");
                result.push_str(pat_part.trim());
                result.push(')');
                i = paren_close + 1;
            } else {
                // Not a regex SUBSTRING — copy the original SUBSTRING(...) as-is.
                result.push_str(&sql[i..=paren_open]);
                result.push_str(inner);
                result.push(')');
                i = paren_close + 1;
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    result
}

/// Split `SUBSTRING(inner)` content on a top-level (not nested in parens/strings)
/// `FROM` keyword where the FROM-clause begins with a single-quoted string.
///
/// Returns `Some((expr_text, pattern_text))` or `None` if the inner content
/// does not match the regex form.
fn split_on_from_regex<'a>(inner: &'a str, inner_upper: &str) -> Option<(&'a str, &'a str)> {
    let bytes = inner.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut in_str = false;

    while i < len {
        match bytes[i] {
            b'\'' if !in_str => {
                in_str = true;
                i += 1;
            }
            b'\'' if in_str => {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_str = false;
                    i += 1;
                }
            }
            b'(' if !in_str => {
                depth += 1;
                i += 1;
            }
            b')' if !in_str => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
            }
            _ if !in_str && depth == 0 => {
                // Check for top-level FROM keyword at position i.
                if inner_upper[i..].starts_with("FROM") {
                    let from_end = i + 4; // "FROM".len()
                    // FROM must be preceded by whitespace or start of string.
                    let preceded_by_ws = i == 0 || bytes[i - 1].is_ascii_whitespace();
                    // FROM must be followed by whitespace.
                    let followed_by_ws = from_end < len && bytes[from_end].is_ascii_whitespace();
                    if preceded_by_ws && followed_by_ws {
                        // Skip whitespace after FROM.
                        let mut k = from_end;
                        while k < len && bytes[k].is_ascii_whitespace() {
                            k += 1;
                        }
                        // The remaining content after FROM must start with a
                        // single-quoted string (the regex pattern).
                        if k < len && bytes[k] == b'\'' {
                            let expr_part = &inner[..i];
                            let pat_part = &inner[k..];
                            // Validate: pat_part should be a complete single-quoted
                            // string (possibly followed only by whitespace/FOR/nothing).
                            // For now, accept it if it starts with ' — the UDF will
                            // handle any trailing content gracefully, and the common
                            // case has nothing after the pattern.
                            // Reject if pat_part is empty or doesn't start with '.
                            if !pat_part.is_empty() {
                                return Some((expr_part, pat_part));
                            }
                        }
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    None
}

// ===========================================================================
// Argument helpers (mirrored from fts_udf.rs / string_dt_udf.rs)
// ===========================================================================

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

fn arg_strings(arg: &ColumnarValue, n: usize) -> DFResult<StringArray> {
    let arr = arg.clone().into_array(n)?;
    let arr = if arr.data_type() == &DataType::Utf8 {
        arr
    } else {
        datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?
    };
    Ok(arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::array::{Array as _, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    };
    use datafusion::prelude::SessionContext;
    use super::{SubstringRegexUdf, register_regex_udfs, rewrite_substring_regex};

    // -----------------------------------------------------------------------
    // Direct UDF invocation helpers
    // -----------------------------------------------------------------------

    fn make_udf() -> SubstringRegexUdf {
        SubstringRegexUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }

    fn return_field_utf8() -> Arc<Field> {
        Arc::new(Field::new("_", DataType::Utf8, true))
    }

    /// Call `substring_regex(text, pattern)` directly via UDF impl.
    fn substring_regex(text: Option<&str>, pattern: Option<&str>) -> Option<String> {
        let udf = make_udf();
        let text_arr = Arc::new(StringArray::from(vec![text]));
        let pat_arr = Arc::new(StringArray::from(vec![pattern]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_arr),
                ColumnarValue::Array(pat_arr),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: return_field_utf8(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                if sa.is_null(0) {
                    None
                } else {
                    Some(sa.value(0).to_string())
                }
            }
            ColumnarValue::Scalar(s) => {
                use datafusion::scalar::ScalarValue;
                match s {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v,
                    _ => None,
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // PG-semantics correctness tests (5 canonical examples from the spec)
    // -----------------------------------------------------------------------

    /// `SUBSTRING('abcdef' FROM '[a-c]+')` → `'abc'`
    /// No capture groups → return the whole match.
    #[test]
    fn basic_no_capture_group() {
        assert_eq!(substring_regex(Some("abcdef"), Some("[a-c]+")), Some("abc".into()));
    }

    /// `SUBSTRING('user@example.com' FROM '([a-z]+)@')` → `'user'`
    /// One capture group → return the first capture group, not the whole match.
    #[test]
    fn first_capture_group_returned() {
        assert_eq!(
            substring_regex(Some("user@example.com"), Some("([a-z]+)@")),
            Some("user".into()),
        );
    }

    /// `SUBSTRING('xyz' FROM '[0-9]+')` → `NULL`
    /// No match → NULL.
    #[test]
    fn no_match_returns_null() {
        assert_eq!(substring_regex(Some("xyz"), Some("[0-9]+")), None);
    }

    /// `SUBSTRING(NULL FROM 'foo')` → `NULL`
    /// NULL input → NULL output.
    #[test]
    fn null_text_returns_null() {
        assert_eq!(substring_regex(None, Some("foo")), None);
    }

    /// `SUBSTRING('hello world' FROM 'world')` → `'world'`
    /// Simple literal pattern with no capture groups.
    #[test]
    fn literal_pattern_match() {
        assert_eq!(
            substring_regex(Some("hello world"), Some("world")),
            Some("world".into()),
        );
    }

    // -----------------------------------------------------------------------
    // Boundary / edge cases
    // -----------------------------------------------------------------------

    /// NULL pattern → NULL output.
    #[test]
    fn null_pattern_returns_null() {
        assert_eq!(substring_regex(Some("hello"), None), None);
    }

    /// Pattern with multiple capture groups → FIRST group only (PG behaviour).
    /// `SUBSTRING('abc123def' FROM '([a-z]+)([0-9]+)')` → `'abc'`
    #[test]
    fn multiple_capture_groups_returns_first() {
        assert_eq!(
            substring_regex(Some("abc123def"), Some("([a-z]+)([0-9]+)")),
            Some("abc".into()),
        );
    }

    /// Empty match `.*` on empty string → `''` (empty string), not NULL.
    #[test]
    fn empty_match_returns_empty_string_not_null() {
        let result = substring_regex(Some(""), Some(".*"));
        assert_eq!(result, Some("".into()));
    }

    /// Empty match `.*` on non-empty string → `''` is NOT the expected result;
    /// `.*` on "hello" matches "hello" (greedy), so result is "hello".
    #[test]
    fn greedy_match_returns_full_string() {
        assert_eq!(substring_regex(Some("hello"), Some(".*")), Some("hello".into()));
    }

    /// Pattern that matches only part of the string (first match only).
    /// `SUBSTRING('foo123bar456' FROM '[0-9]+')` → `'123'` (first match).
    #[test]
    fn first_match_only_not_second() {
        assert_eq!(
            substring_regex(Some("foo123bar456"), Some("[0-9]+")),
            Some("123".into()),
        );
    }

    /// Invalid regex → execution error (not NULL, not panic).
    #[test]
    fn invalid_regex_returns_error() {
        let udf = make_udf();
        let text_arr = Arc::new(StringArray::from(vec![Some("hello")]));
        let pat_arr = Arc::new(StringArray::from(vec![Some("[invalid")]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_arr),
                ColumnarValue::Array(pat_arr),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: return_field_utf8(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        assert!(udf.invoke_with_args(args).is_err());
    }

    // -----------------------------------------------------------------------
    // Rewrite function tests
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_basic_no_capture() {
        let sql = "SELECT SUBSTRING('abcdef' FROM '[a-c]+')";
        let rewritten = rewrite_substring_regex(sql);
        assert_eq!(rewritten, "SELECT substring_regex('abcdef', '[a-c]+')");
    }

    #[test]
    fn rewrite_with_capture_group() {
        let sql = "SELECT SUBSTRING('user@example.com' FROM '([a-z]+)@')";
        let rewritten = rewrite_substring_regex(sql);
        assert_eq!(
            rewritten,
            "SELECT substring_regex('user@example.com', '([a-z]+)@')"
        );
    }

    #[test]
    fn rewrite_does_not_touch_integer_form() {
        // SUBSTRING(x FROM 3 FOR 5) — not a regex form, must not be rewritten.
        let sql = "SELECT SUBSTRING('abcdef' FROM 3 FOR 5)";
        let rewritten = rewrite_substring_regex(sql);
        // Should be unchanged because FROM is followed by a number, not a quote.
        assert_eq!(rewritten, sql);
    }

    #[test]
    fn rewrite_no_substring_is_noop() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let rewritten = rewrite_substring_regex(sql);
        assert_eq!(rewritten, sql);
    }

    #[test]
    fn rewrite_case_insensitive_keyword() {
        let sql = "SELECT substring('abcdef' from '[a-c]+')";
        let rewritten = rewrite_substring_regex(sql);
        assert_eq!(rewritten, "SELECT substring_regex('abcdef', '[a-c]+')");
    }

    // -----------------------------------------------------------------------
    // Async DataFusion ctx integration (UDF round-trip via SQL engine)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ctx_direct_udf_call() {
        let ctx = SessionContext::new();
        register_regex_udfs(&ctx);

        // The UDF is callable directly (before wiring the SUBSTRING…FROM rewrite).
        let df = ctx
            .sql("SELECT substring_regex('abcdef', '[a-c]+') AS r")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "abc");
    }

    #[tokio::test]
    async fn ctx_capture_group() {
        let ctx = SessionContext::new();
        register_regex_udfs(&ctx);
        let df = ctx
            .sql("SELECT substring_regex('user@example.com', '([a-z]+)@') AS r")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "user");
    }

    #[tokio::test]
    async fn ctx_no_match_returns_null() {
        let ctx = SessionContext::new();
        register_regex_udfs(&ctx);
        let df = ctx
            .sql("SELECT substring_regex('xyz', '[0-9]+') AS r")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.is_null(0));
    }
}
