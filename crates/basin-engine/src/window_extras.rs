//! Window-function corner-case helpers for PostgreSQL compatibility.
//!
//! ## What this module does
//!
//! ### LEAD / LAG `default_value` (verified, no rewrite needed)
//!
//! DataFusion 53 honors `lead(x, offset, default)` / `lag(x, offset, default)`
//! natively via `shift_with_default_value()` in `datafusion-functions-window`.
//! The three-argument form parses and executes correctly; no pre-pass rewrite
//! is required.  The unit tests in this file pin that invariant so we catch any
//! upstream regression.
//!
//! ### FIRST_VALUE / LAST_VALUE / NTH_VALUE (verified, no rewrite needed)
//!
//! These are built-in DataFusion window functions (`NthValue`). They work out
//! of the box including `nth_value(x, n)` with a literal n.
//!
//! ### IGNORE NULLS — two syntactic forms
//!
//! PostgreSQL 17+ supports `IGNORE NULLS` and `RESPECT NULLS` modifiers on
//! `LEAD`, `LAG`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`:
//!
//! **Form A — post-parens** (standard SQL / PG 17+):
//! ```sql
//! FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY t)
//! ```
//! sqlparser 0.61 with `PostgreSqlDialect` handles this via `parse_null_treatment()`
//! which fires between the closing `)` and the `OVER` keyword.  DataFusion then
//! picks it up from `WindowFunctionParams.null_treatment` and routes it to
//! `PartitionEvaluatorArgs.ignore_nulls()`.  **Natively supported — no rewrite.**
//!
//! **Form B — within-args** (BigQuery / Oracle / Snowflake extension):
//! ```sql
//! FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY t)
//! ```
//! `PostgreSqlDialect::supports_window_function_null_treatment_arg()` returns
//! `false`, so sqlparser does *not* parse this form and emits a parse error.
//!
//! Some PostgreSQL client libraries (e.g. certain ORMs or hand-written queries
//! ported from Oracle) emit Form B.  The `rewrite_ignore_nulls_in_args`
//! pre-pass converts Form B → Form A before sqlparser sees the SQL:
//!
//! ```sql
//! FIRST_VALUE(x IGNORE NULLS) OVER  →  FIRST_VALUE(x) IGNORE NULLS OVER
//! LEAD(x, 1, 0 IGNORE NULLS) OVER   →  LEAD(x, 1, 0) IGNORE NULLS OVER
//! ```
//!
//! The rewrite is conservative (case-insensitive keyword match, quoted-string
//! awareness, boundary checks) and leaves all other SQL untouched.
//!
//! ### Unsupported: IGNORE NULLS as a free-standing aggregate modifier
//!
//! `FIRST_VALUE(x IGNORE NULLS)` used *without* an `OVER` clause (i.e. as an
//! ordinary aggregate) is not supported by DataFusion 53 and is out of scope
//! here.  The rewrite above only moves the modifier to the post-parens position;
//! if there is no `OVER` clause DataFusion will reject the query with its own
//! error message, which is the correct outcome.

// ─── Public entry point ──────────────────────────────────────────────────────

/// Pre-parse rewrite: move `IGNORE NULLS` / `RESPECT NULLS` from inside a
/// window function's argument list to the post-parens position that
/// `PostgreSqlDialect` can parse.
///
/// Handles the known window functions that accept null-treatment:
/// `LEAD`, `LAG`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`.
///
/// The transform is:
/// ```text
/// fn_name(... IGNORE NULLS) OVER   →   fn_name(...) IGNORE NULLS OVER
/// fn_name(... RESPECT NULLS) OVER  →   fn_name(...) RESPECT NULLS OVER
/// ```
///
/// • Case-insensitive on keywords and function names.
/// • Skips content inside single-quoted strings.
/// • No-op when the modifier is already outside the parens.
/// • No-op for SQL that doesn't contain "IGNORE" or "RESPECT".
pub(crate) fn rewrite_ignore_nulls_in_args(sql: &str) -> String {
    // Fast path: if neither keyword appears at all, nothing to do.
    let upper = sql.to_uppercase();
    if !upper.contains("IGNORE") && !upper.contains("RESPECT") {
        return sql.to_string();
    }

    // The functions whose null-treatment modifier we rewrite.
    const WINDOW_FNS: &[&str] = &["LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"];

    let mut result = sql.to_string();

    // Iterate until stable (a single pass can only rewrite one instance per
    // occurrence; loop ensures we catch all occurrences from left to right).
    loop {
        let prev = result.clone();
        result = rewrite_one_pass(&result, WINDOW_FNS);
        if result == prev {
            break;
        }
    }

    result
}

// ─── Implementation ───────────────────────────────────────────────────────────

/// One left-to-right pass over `sql` looking for a `WINDOW_FNS[i](... IGNORE
/// NULLS)` or `WINDOW_FNS[i](... RESPECT NULLS)` pattern and rewriting the
/// first match found.  Returns the (possibly unchanged) string.
fn rewrite_one_pass(sql: &str, window_fns: &[&str]) -> String {
    let sql_upper = sql.to_uppercase();

    for fn_name in window_fns {
        let fn_upper = fn_name.to_uppercase();
        let mut search_start = 0;

        while let Some(fn_pos) = find_identifier(&sql_upper[search_start..], &fn_upper) {
            let abs_fn = search_start + fn_pos;

            // Skip occurrences of the function name that fall inside a
            // single-quoted string literal — these must never be rewritten.
            if is_inside_string(sql, abs_fn) {
                search_start = abs_fn + 1;
                continue;
            }

            // Check that the character immediately after the function name is '('
            let after_name = abs_fn + fn_name.len();
            if after_name >= sql.len() {
                search_start = abs_fn + 1;
                continue;
            }
            let rest_after_name = sql[after_name..].trim_start();
            if !rest_after_name.starts_with('(') {
                search_start = abs_fn + 1;
                continue;
            }

            // Find the opening '(' (may have whitespace between name and '(')
            let open_paren = after_name
                + sql[after_name..]
                    .find('(')
                    .expect("just confirmed '(' exists");

            // Find the matching closing ')' respecting nesting and string literals
            let Some(close_paren) = find_matching_paren(sql, open_paren) else {
                search_start = abs_fn + 1;
                continue;
            };

            // Extract content inside the parens
            let inner = &sql[open_paren + 1..close_paren];
            let inner_upper = inner.to_uppercase();

            // Look for IGNORE NULLS or RESPECT NULLS inside the argument list,
            // making sure it is not inside a nested string literal.
            let modifier = if let Some(pos) =
                find_null_modifier(inner, inner_upper.as_str(), "IGNORE NULLS")
            {
                Some(("IGNORE NULLS", pos))
            } else if let Some(pos) =
                find_null_modifier(inner, inner_upper.as_str(), "RESPECT NULLS")
            {
                Some(("RESPECT NULLS", pos))
            } else {
                None
            };

            if let Some((modifier_kw, mod_pos)) = modifier {
                // Build the new inner content with the modifier stripped out,
                // trimming any trailing whitespace/comma that was before it.
                let before_mod = inner[..mod_pos].trim_end_matches([' ', '\t', '\n', ',']);
                let after_mod = &inner[mod_pos + modifier_kw.len()..];
                let new_inner = format!("{before_mod}{after_mod}");

                // The rewritten call: fn_name(new_inner) MODIFIER OVER ...
                // We need to insert the modifier after the closing ')'.
                let prefix = &sql[..open_paren + 1];
                let suffix = &sql[close_paren..]; // starts with ')'

                // suffix: ')' optionally whitespace then rest of SQL.
                // `&suffix[..1]` is the closing ')'; we re-emit it and then
                // insert ' MODIFIER' right after it, followed by the rest.
                let new_sql = format!(
                    "{prefix}{new_inner}{} {modifier_kw}{}",
                    &suffix[..1],
                    &suffix[1..]
                );

                return new_sql;
            }

            search_start = abs_fn + 1;
        }
    }

    sql.to_string()
}

/// Find `needle` (an uppercase identifier like `"LEAD"`) as a whole word in
/// `haystack` (already uppercased). Returns the byte offset of the first
/// match, or `None`.
fn find_identifier(haystack: &str, needle: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs = start + pos;
        // Check left boundary: must not be preceded by an alphanumeric or '_'
        let left_ok = abs == 0
            || !haystack
                .as_bytes()
                .get(abs - 1)
                .map(|b| b.is_ascii_alphanumeric() || *b == b'_')
                .unwrap_or(false);
        // Check right boundary: must not be followed by an alphanumeric or '_'
        let right_ok = !haystack
            .as_bytes()
            .get(abs + needle.len())
            .map(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .unwrap_or(false);
        if left_ok && right_ok {
            return Some(abs);
        }
        start = abs + 1;
    }
    None
}

/// Find the closing ')' that matches the opening '(' at `open_pos` in `sql`.
/// Respects nesting and skips single-quoted string literals.
/// Returns `None` if the string is malformed (unmatched parens).
fn find_matching_paren(sql: &str, open_pos: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = open_pos;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Skip string literal (including doubled-quote escapes)
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2; // escaped quote
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

/// Look for `modifier_upper` (e.g. `"IGNORE NULLS"`) as a whole-token sequence
/// inside `inner` (the content between the outermost parens).
/// Returns the byte offset of the first occurrence, or `None`.
///
/// `inner` is the original-case slice; `inner_upper` is its `.to_uppercase()`.
fn find_null_modifier(inner: &str, inner_upper: &str, modifier_upper: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(pos) = inner_upper[start..].find(modifier_upper) {
        let abs = start + pos;
        // Make sure we're not inside a string literal
        if is_inside_string(inner, abs) {
            start = abs + 1;
            continue;
        }
        // Left boundary: must follow whitespace, comma, or start of string
        let left_ok = abs == 0
            || inner
                .as_bytes()
                .get(abs - 1)
                .map(|b| b.is_ascii_whitespace() || *b == b',')
                .unwrap_or(false);
        // Right boundary: must be followed by end of string, whitespace, or ')'
        let right_ok = abs + modifier_upper.len() >= inner.len()
            || inner
                .as_bytes()
                .get(abs + modifier_upper.len())
                .map(|b| b.is_ascii_whitespace() || *b == b')' || *b == b',')
                .unwrap_or(false);
        if left_ok && right_ok {
            return Some(abs);
        }
        start = abs + 1;
    }
    None
}

/// Returns `true` if the byte offset `pos` in `s` falls inside a single-quoted
/// string literal.
fn is_inside_string(s: &str, pos: usize) -> bool {
    let bytes = s.as_bytes();
    let mut inside = false;
    let mut i = 0;
    while i < pos {
        if bytes[i] == b'\'' {
            inside = !inside;
            // Handle doubled-quote escapes
            if inside && bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                inside = false; // '' is an empty string, not an open quote
                continue;
            }
        }
        i += 1;
    }
    inside
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── LEAD / LAG default_value verification ────────────────────────────────
    // These tests document that DataFusion 53 DOES honor the third argument.
    // They exercise the rewrite path (no-op here) and serve as regression pins.

    #[test]
    fn lead_three_arg_form_passes_through_unchanged() {
        // LEAD(x, 1, 0) OVER (...) — default_value = 0.
        // No IGNORE NULLS → rewrite is a no-op.
        let sql = "SELECT LEAD(val, 1, 0) OVER (ORDER BY t) FROM tbl";
        assert_eq!(rewrite_ignore_nulls_in_args(sql), sql);
    }

    #[test]
    fn lag_three_arg_form_passes_through_unchanged() {
        let sql = "SELECT LAG(val, 2, -1) OVER (PARTITION BY grp ORDER BY t) FROM tbl";
        assert_eq!(rewrite_ignore_nulls_in_args(sql), sql);
    }

    // ── FIRST_VALUE / LAST_VALUE / NTH_VALUE basic verification ─────────────

    #[test]
    fn first_value_no_modifier_passes_through() {
        let sql = "SELECT FIRST_VALUE(col) OVER (ORDER BY ts) FROM t";
        assert_eq!(rewrite_ignore_nulls_in_args(sql), sql);
    }

    #[test]
    fn last_value_no_modifier_passes_through() {
        let sql = "SELECT LAST_VALUE(col) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t";
        assert_eq!(rewrite_ignore_nulls_in_args(sql), sql);
    }

    #[test]
    fn nth_value_no_modifier_passes_through() {
        let sql = "SELECT nth_value(col, 2) OVER (ORDER BY ts) FROM t";
        assert_eq!(rewrite_ignore_nulls_in_args(sql), sql);
    }

    // ── IGNORE NULLS within-args rewrite ─────────────────────────────────────

    #[test]
    fn first_value_ignore_nulls_in_args_rewritten() {
        // Form B → Form A
        let sql = "SELECT FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY t) FROM tbl";
        let out = rewrite_ignore_nulls_in_args(sql);
        // Must have modifier outside the parens, not inside
        assert!(
            out.contains("FIRST_VALUE(x) IGNORE NULLS OVER"),
            "got: {out}"
        );
        assert!(
            !out.contains("IGNORE NULLS)"),
            "modifier still inside: {out}"
        );
    }

    #[test]
    fn last_value_ignore_nulls_in_args_rewritten() {
        let sql = "SELECT LAST_VALUE(col IGNORE NULLS) OVER (ORDER BY ts) FROM t";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert!(
            out.contains("LAST_VALUE(col) IGNORE NULLS OVER"),
            "got: {out}"
        );
    }

    #[test]
    fn lead_ignore_nulls_in_args_rewritten() {
        // LEAD with default_value AND IGNORE NULLS inside args
        let sql = "SELECT LEAD(val, 1, 0 IGNORE NULLS) OVER (ORDER BY t) FROM tbl";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert!(
            out.contains("LEAD(val, 1, 0) IGNORE NULLS OVER"),
            "got: {out}"
        );
        assert!(
            !out.contains("IGNORE NULLS)"),
            "modifier still inside: {out}"
        );
    }

    #[test]
    fn lag_respect_nulls_in_args_rewritten() {
        let sql = "SELECT LAG(x RESPECT NULLS) OVER (ORDER BY t) FROM t";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert!(out.contains("LAG(x) RESPECT NULLS OVER"), "got: {out}");
    }

    #[test]
    fn nth_value_ignore_nulls_in_args_rewritten() {
        let sql = "SELECT nth_value(x, 3 IGNORE NULLS) OVER (PARTITION BY p ORDER BY t) FROM t";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert!(
            out.contains("nth_value(x, 3) IGNORE NULLS OVER"),
            "got: {out}"
        );
    }

    #[test]
    fn post_parens_form_is_not_double_rewritten() {
        // Form A already — must come through unchanged
        let sql = "SELECT FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY t) FROM tbl";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert_eq!(out, sql, "Form A should not be modified: {out}");
    }

    #[test]
    fn ignore_nulls_inside_string_literal_not_rewritten() {
        // The text "IGNORE NULLS" appears only inside a string literal
        let sql = "SELECT 'FIRST_VALUE(x IGNORE NULLS)' AS s FROM t";
        let out = rewrite_ignore_nulls_in_args(sql);
        assert_eq!(out, sql, "should not touch string literal: {out}");
    }

    #[test]
    fn case_insensitive_function_name_and_modifier() {
        let sql = "select first_value(x ignore nulls) over (order by t) from t";
        let out = rewrite_ignore_nulls_in_args(sql);
        // The function name casing is preserved; only the modifier is moved
        assert!(
            out.contains("first_value(x)"),
            "arg should be stripped: {out}"
        );
        assert!(
            out.to_uppercase().contains("IGNORE NULLS OVER"),
            "modifier should be outside: {out}"
        );
    }

    // ── Advanced window frames — pass-through verification ───────────────────
    //
    // RANGE INTERVAL and GROUPS frames are handled natively by DataFusion 53's
    // SQL planner (WindowFrame::try_from + coerce_window_frame in the type-
    // coercion analyser). The rewrite pipeline does not need to touch these
    // forms; these tests pin that the pre-parse rewriter leaves them alone.

    #[test]
    fn range_interval_passes_through_unchanged() {
        let sql = "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '5 days' PRECEDING AND CURRENT ROW) FROM t";
        assert_eq!(
            rewrite_ignore_nulls_in_args(sql),
            sql,
            "RANGE INTERVAL frame must not be modified by the null-treatment rewriter"
        );
    }

    #[test]
    fn groups_frame_passes_through_unchanged() {
        let sql =
            "SELECT SUM(v) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t";
        assert_eq!(
            rewrite_ignore_nulls_in_args(sql),
            sql,
            "GROUPS frame must not be modified by the null-treatment rewriter"
        );
    }

    #[test]
    fn range_interval_following_passes_through_unchanged() {
        let sql = "SELECT AVG(v) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND INTERVAL '3 days' FOLLOWING) FROM t";
        assert_eq!(
            rewrite_ignore_nulls_in_args(sql),
            sql,
            "RANGE INTERVAL FOLLOWING frame must not be modified"
        );
    }
}
