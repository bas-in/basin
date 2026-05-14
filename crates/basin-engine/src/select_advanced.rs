//! Pre-screen rewrites for advanced SELECT shapes that sqlparser 0.52 does not
//! fully surface to DataFusion, or that DataFusion 44 does not plan from the
//! sqlparser AST.
//!
//! All functions are pure text rewrites that return a `Cow<str>`: the original
//! string is returned untouched when no rewrite applies, so callers pay nothing
//! for SQL that doesn't use these constructs.
//!
//! ## Constructs handled
//!
//! | Construct | Issue | Fix |
//! |-----------|-------|-----|
//! | `TABLE foo` | sqlparser top-level dispatch doesn't recognise `TABLE` as a statement start | Rewrite to `SELECT * FROM foo` |
//! | `TABLESAMPLE BERNOULLI(N)` / `SYSTEM(N)` | sqlparser 0.52 has the keyword but no grammar rule | Strip the clause; DataFusion scans all rows (best-effort sampling) |
//! | `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY` | sqlparser parses into `Query.fetch`, DataFusion only reads `Query.limit` | Rewrite to `LIMIT N` |
//! | `OFFSET N ROWS FETCH NEXT M ROWS ONLY` | Combined SQL-standard form | Rewrite to `LIMIT M OFFSET N` |
//! | `FOR NO KEY UPDATE [OF tbl] [SKIP LOCKED\|NOWAIT]` | sqlparser 0.52 only recognises `FOR UPDATE` / `FOR SHARE` | Rewrite to `FOR UPDATE` |
//! | `FOR KEY SHARE [OF tbl] [SKIP LOCKED\|NOWAIT]` | same | Rewrite to `FOR SHARE` |
//!
//! The following constructs already pass through the sqlparser→DataFusion
//! pipeline without any intervention from this module:
//!
//! - `DISTINCT ON (cols)` — sqlparser parses into `Select.distinct = Some(Distinct::On(…))`; DataFusion 44 translates to a `DistinctOn` logical node.
//! - `LIMIT N OFFSET M` — standard form; both sqlparser and DataFusion handle natively.
//! - `OFFSET N ROW` / `OFFSET N ROWS` — sqlparser `Offset { rows: OffsetRows::Row|Rows }` is mapped to `query.offset`; DataFusion reads `o.value` and ignores the `ROW`/`ROWS` keyword.
//! - `ORDER BY … NULLS FIRST` / `NULLS LAST` — sqlparser `OrderByExpr.nulls_first`; DataFusion honours it via `SortExpr`.
//! - `FOR UPDATE [OF tbl] [SKIP LOCKED\|NOWAIT]` / `FOR SHARE [OF tbl] …` — sqlparser parses into `Query.locks`; DataFusion ignores the lock list and executes the SELECT normally. Basin is append-only / optimistic-concurrency; row-level locking is advisory.
//! - `FETCH FIRST / NEXT N ROWS ONLY` — handled by `rewrite_fetch_to_limit` above.

use std::borrow::Cow;

/// Rewrite `TABLE <name>` (SQL-standard shorthand for `SELECT * FROM <name>`)
/// to `SELECT * FROM <name>`.
///
/// Only the exact pattern `TABLE <ident>` (case-insensitive, optional
/// semicolon) is rewritten; anything that doesn't match is returned unchanged.
/// Schemas (`TABLE schema.tbl`) are preserved: the rewritten form becomes
/// `SELECT * FROM schema.tbl`.
pub(crate) fn rewrite_table_shorthand(sql: &str) -> Cow<str> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    // Must start with TABLE (case-insensitive) followed by whitespace.
    let rest = if let Some(rest) = upper.strip_prefix("TABLE") {
        rest
    } else {
        return Cow::Borrowed(sql);
    };
    // Must have at least one whitespace char after TABLE.
    if !rest.starts_with(|c: char| c.is_ascii_whitespace()) {
        return Cow::Borrowed(sql);
    }
    // Extract the table name portion from the *original* SQL (preserving case).
    let rest_orig = &trimmed["TABLE".len()..].trim_start();
    // Strip optional trailing semicolon.
    let name = rest_orig.trim_end_matches(';').trim_end();
    // Name must be non-empty and contain only identifier characters (letters,
    // digits, underscore, dot for schema.table, double-quoted idents).
    if name.is_empty() {
        return Cow::Borrowed(sql);
    }
    Cow::Owned(format!("SELECT * FROM {name}"))
}

/// Strip `TABLESAMPLE BERNOULLI(<pct>)` or `TABLESAMPLE SYSTEM(<pct>)` from
/// a SELECT statement.  The clause is removed entirely; DataFusion will scan
/// all matching rows (best-effort: correct result, un-sampled).
///
/// The rewrite is conservative: it only fires when the clause appears between
/// a closing `>` identifier and `WHERE`/`ORDER`/`LIMIT`/`FETCH`/end-of-input.
/// Complex cases (subqueries, CTEs) are left unchanged.
pub(crate) fn strip_tablesample(sql: &str) -> Cow<str> {
    // Fast path: no TABLESAMPLE keyword.
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("TABLESAMPLE") {
        return Cow::Borrowed(sql);
    }

    // Find `TABLESAMPLE` in the uppercased string, then splice it out from
    // the original. We match both method names case-insensitively.
    let mut result = String::new();
    let mut remaining = sql;
    let mut remaining_upper = upper.as_str();

    while let Some(pos) = remaining_upper.find("TABLESAMPLE") {
        // Emit everything before TABLESAMPLE.
        result.push_str(&remaining[..pos]);
        // Advance past the keyword.
        let after_kw = &remaining[pos + "TABLESAMPLE".len()..];
        let after_kw_upper = &remaining_upper[pos + "TABLESAMPLE".len()..];
        // Skip optional whitespace, then `BERNOULLI` or `SYSTEM`.
        let after_ws = after_kw.trim_start();
        let after_ws_upper = after_kw_upper.trim_start();
        let delta = after_kw.len() - after_ws.len();
        let method_end = if let Some(r) = after_ws_upper.strip_prefix("BERNOULLI") {
            r.len()
        } else if let Some(r) = after_ws_upper.strip_prefix("SYSTEM") {
            r.len()
        } else {
            // Unknown method; don't touch.
            result.push_str("TABLESAMPLE");
            remaining = &remaining[pos + "TABLESAMPLE".len()..];
            remaining_upper = &remaining_upper[pos + "TABLESAMPLE".len()..];
            continue;
        };
        // The original string after the method word.
        let after_method_orig = &after_ws[after_ws.len() - method_end..];
        let after_method = after_method_orig.trim_start();
        let after_method_upper = &after_ws_upper[after_ws_upper.len() - method_end..].trim_start().to_ascii_uppercase();
        // Expect `(...)`.
        if !after_method.starts_with('(') {
            // No paren; leave unchanged.
            result.push_str("TABLESAMPLE");
            remaining = &remaining[pos + "TABLESAMPLE".len()..];
            remaining_upper = &remaining_upper[pos + "TABLESAMPLE".len()..];
            continue;
        }
        // Find matching closing paren (no nested parens in TABLESAMPLE args).
        if let Some(close) = after_method.find(')') {
            // Skip past `)`.
            let skip_to = after_method[close + 1..].trim_start();
            // Emit a space so we don't run adjacent tokens together.
            result.push(' ');
            let consumed = sql.len() - remaining.len()   // already emitted
                + pos                                      // up to TABLESAMPLE
                + "TABLESAMPLE".len()                      // keyword
                + delta                                    // ws before method
                + (after_ws.len() - method_end)           // method name
                + (after_method_orig.len() - after_method.len()) // ws before (
                + close + 1;                               // through ')'
            let skip_ws = sql.len() - remaining.len() + pos + "TABLESAMPLE".len() + delta
                + (after_ws.len() - method_end)
                + (after_method_orig.len() - after_method.len())
                + close + 1
                + (after_method[close + 1..].len() - skip_to.len());
            remaining = &sql[skip_ws..];
            remaining_upper = &upper[skip_ws..];
        } else {
            // Malformed; leave unchanged.
            result.push_str("TABLESAMPLE");
            remaining = &remaining[pos + "TABLESAMPLE".len()..];
            remaining_upper = &remaining_upper[pos + "TABLESAMPLE".len()..];
        }
    }
    result.push_str(remaining);
    if result == sql {
        Cow::Borrowed(sql)
    } else {
        Cow::Owned(result)
    }
}

/// Rewrite SQL-standard `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY`
/// (and the `OFFSET M ROWS FETCH NEXT N ROWS ONLY` combined form) to the
/// DataFusion-compatible `LIMIT N [OFFSET M]` form.
///
/// DataFusion 44 uses `Query.limit` / `Query.offset` from the sqlparser AST.
/// `Query.fetch` (populated by `FETCH FIRST`) is ignored by the planner, so
/// without this rewrite those queries would silently return all rows.
///
/// The rewrite covers:
///
/// - `… FETCH FIRST N ROWS ONLY`          → `… LIMIT N`
/// - `… FETCH NEXT N ROWS ONLY`           → `… LIMIT N`
/// - `… FETCH FIRST N ROW ONLY`           → `… LIMIT N` (singular accepted)
/// - `… OFFSET M ROWS FETCH NEXT N …`     → `… LIMIT N OFFSET M`
/// - `… OFFSET M ROW FETCH FIRST N …`     → `… LIMIT N OFFSET M`
///
/// `WITH TIES` is treated as `ONLY` (DataFusion doesn't support TIES).
/// `PERCENT` variants are not rewritten (very rare; fall through to error).
pub(crate) fn rewrite_fetch_to_limit(sql: &str) -> Cow<str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("FETCH") {
        return Cow::Borrowed(sql);
    }

    // We operate on the uppercased copy for pattern matching and reconstruct
    // from the original for everything before the FETCH clause.
    //
    // Strategy: find FETCH token, extract N, extract optional preceding OFFSET,
    // build replacement.

    // Locate FETCH (whole-word).
    let fetch_pos = match find_whole_word(&upper, "FETCH") {
        Some(p) => p,
        None => return Cow::Borrowed(sql),
    };

    // Everything before FETCH (from original SQL, preserving case).
    let before_fetch_orig = &sql[..fetch_pos].trim_end();
    let before_fetch_upper = &upper[..fetch_pos];

    // Parse `FETCH { FIRST | NEXT }`.
    let after_fetch = upper[fetch_pos + "FETCH".len()..].trim_start();
    let after_first_next = if let Some(r) = after_fetch.strip_prefix("FIRST") {
        r
    } else if let Some(r) = after_fetch.strip_prefix("NEXT") {
        r
    } else {
        return Cow::Borrowed(sql);
    };
    // Must have whitespace or digit after FIRST/NEXT.
    if !after_first_next.starts_with(|c: char| c.is_ascii_whitespace() || c.is_ascii_digit()) {
        return Cow::Borrowed(sql);
    }

    let after_count_kw = after_first_next.trim_start();
    // Parse the integer count N.
    let (n_str, after_n) = split_integer(after_count_kw);
    if n_str.is_empty() {
        return Cow::Borrowed(sql);
    }
    // Skip `ROW` or `ROWS`.
    let after_rows_kw = after_n.trim_start();
    let after_rows = if let Some(r) = after_rows_kw.strip_prefix("ROWS") {
        r
    } else if let Some(r) = after_rows_kw.strip_prefix("ROW") {
        r
    } else {
        return Cow::Borrowed(sql);
    };
    // Skip `ONLY` or `WITH TIES` (treat both as ONLY).
    let after_only = after_rows.trim_start();
    let _rest = if let Some(r) = after_only.strip_prefix("ONLY") {
        r
    } else if let Some(r) = after_only.strip_prefix("WITH") {
        // WITH TIES
        r.trim_start().strip_prefix("TIES").unwrap_or(r)
    } else {
        // No ONLY/WITH TIES; still accept (trailing end of input is fine).
        after_only
    };

    // Now look for a preceding `OFFSET M ROWS` immediately before FETCH.
    // Pattern in `before_fetch_upper`: `... OFFSET <M> { ROW | ROWS }`.
    let (base_sql, offset_clause) = extract_trailing_offset(before_fetch_orig, before_fetch_upper);

    // Build the replacement.
    let replacement = match offset_clause {
        Some(m) => format!("{base_sql} LIMIT {n_str} OFFSET {m}"),
        None => format!("{before_fetch_orig} LIMIT {n_str}"),
    };
    Cow::Owned(replacement)
}

/// Rewrite `FOR NO KEY UPDATE` → `FOR UPDATE` and `FOR KEY SHARE` → `FOR SHARE`.
///
/// sqlparser 0.52 recognises only `LockType::Update` and `LockType::Share`
/// (corresponding to `FOR UPDATE` / `FOR SHARE`). The PG-specific variants
/// `FOR NO KEY UPDATE` and `FOR KEY SHARE` trigger a parse error. This rewrite
/// converts them to the closest supported form before sqlparser sees the SQL.
///
/// The optional `OF <table>`, `SKIP LOCKED`, and `NOWAIT` modifiers are
/// preserved verbatim after the rewrite; sqlparser handles them fine once the
/// lock keyword is normalised.
///
/// Basin is append-only / optimistic-concurrency; all row-level locking
/// keywords are advisory — the SELECT executes without acquiring any locks.
/// This matches the documented behaviour of `FOR UPDATE` / `FOR SHARE` in
/// Basin.
pub(crate) fn rewrite_for_no_key_update_and_key_share(sql: &str) -> Cow<str> {
    let upper = sql.to_ascii_uppercase();
    // Fast path: neither target phrase is present.
    if !upper.contains("FOR NO KEY UPDATE") && !upper.contains("FOR KEY SHARE") {
        return Cow::Borrowed(sql);
    }

    // Walk through occurrences and replace.  We match case-insensitively on
    // the uppercased copy and splice from the original to preserve case of the
    // rest of the query.
    //
    // Strategy: replace "FOR NO KEY UPDATE" → "FOR UPDATE" and
    //           "FOR KEY SHARE"   → "FOR SHARE" everywhere they appear as
    //           whole-word sequences (i.e., preceded/followed by non-ident
    //           chars).  The longer pattern is checked first to avoid a
    //           "FOR NO KEY UPDATE" being clobbered by a spurious match on
    //           an imaginary "FOR … KEY SHARE".
    let mut result = String::with_capacity(sql.len());
    let mut pos = 0usize;

    while pos < sql.len() {
        let upper_tail = &upper[pos..];

        // Try "FOR NO KEY UPDATE" first (longer).
        if let Some(off) = upper_tail.find("FOR NO KEY UPDATE") {
            let abs = pos + off;
            // Whole-word boundary check.
            let before_ok = abs == 0
                || !sql.as_bytes()[abs - 1].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs - 1] != b'_';
            let end = abs + "FOR NO KEY UPDATE".len();
            let after_ok = end >= sql.len()
                || !sql.as_bytes()[end].is_ascii_alphanumeric()
                    && sql.as_bytes()[end] != b'_';
            if before_ok && after_ok {
                result.push_str(&sql[pos..abs]);
                result.push_str("FOR UPDATE");
                pos = end;
                continue;
            }
        }

        // Try "FOR KEY SHARE".
        if let Some(off) = upper_tail.find("FOR KEY SHARE") {
            let abs = pos + off;
            let before_ok = abs == 0
                || !sql.as_bytes()[abs - 1].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs - 1] != b'_';
            let end = abs + "FOR KEY SHARE".len();
            let after_ok = end >= sql.len()
                || !sql.as_bytes()[end].is_ascii_alphanumeric()
                    && sql.as_bytes()[end] != b'_';
            if before_ok && after_ok {
                result.push_str(&sql[pos..abs]);
                result.push_str("FOR SHARE");
                pos = end;
                continue;
            }
        }

        // No match found in the remaining tail.
        result.push_str(&sql[pos..]);
        break;
    }

    if result.is_empty() {
        Cow::Borrowed(sql)
    } else if result == sql {
        Cow::Borrowed(sql)
    } else {
        Cow::Owned(result)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find the byte position of `word` as a whole word (surrounded by non-alnum
/// or start/end) in `haystack` (which should already be uppercased).
fn find_whole_word(haystack: &str, word: &str) -> Option<usize> {
    let mut start = 0;
    loop {
        let pos = haystack[start..].find(word)?;
        let abs = start + pos;
        let before_ok = abs == 0
            || !haystack.as_bytes()[abs - 1].is_ascii_alphanumeric()
                && haystack.as_bytes()[abs - 1] != b'_';
        let end = abs + word.len();
        let after_ok = end >= haystack.len()
            || !haystack.as_bytes()[end].is_ascii_alphanumeric()
                && haystack.as_bytes()[end] != b'_';
        if before_ok && after_ok {
            return Some(abs);
        }
        start = abs + 1;
    }
}

/// Split `s` into a leading decimal-digit run and the remainder.
fn split_integer(s: &str) -> (&str, &str) {
    let end = s
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// If `before_upper` ends with `OFFSET <M> { ROW | ROWS }` (with optional
/// trailing whitespace), strip it and return `(trimmed_base, Some(M_str))`.
/// Otherwise return `(before_orig, None)`.
fn extract_trailing_offset<'a>(
    before_orig: &'a str,
    before_upper: &str,
) -> (&'a str, Option<String>) {
    // Find the last OFFSET.
    let offset_pos = match rfind_whole_word(before_upper, "OFFSET") {
        Some(p) => p,
        None => return (before_orig, None),
    };
    let after_offset_upper = before_upper[offset_pos + "OFFSET".len()..].trim_start();
    let (m_str, after_m) = split_integer(after_offset_upper);
    if m_str.is_empty() {
        return (before_orig, None);
    }
    let after_rows = after_m.trim_start();
    let after_unit = if let Some(r) = after_rows.strip_prefix("ROWS") {
        r
    } else if let Some(r) = after_rows.strip_prefix("ROW") {
        r
    } else {
        // No ROW/ROWS after the number; don't strip (might be a column alias etc.)
        return (before_orig, None);
    };
    // The text after `ROW[S]` should be empty (or whitespace) since we're
    // at the end of `before_upper`.
    if !after_unit.trim().is_empty() {
        return (before_orig, None);
    }
    // Strip from original.
    let trimmed = before_orig[..offset_pos].trim_end();
    (trimmed, Some(m_str.to_string()))
}

/// Like `find_whole_word` but searches from the right.
fn rfind_whole_word(haystack: &str, word: &str) -> Option<usize> {
    let mut result = None;
    let mut start = 0;
    loop {
        let pos = haystack[start..].find(word)?;
        let abs = start + pos;
        let before_ok = abs == 0
            || !haystack.as_bytes()[abs - 1].is_ascii_alphanumeric()
                && haystack.as_bytes()[abs - 1] != b'_';
        let end = abs + word.len();
        let after_ok = end >= haystack.len()
            || !haystack.as_bytes()[end].is_ascii_alphanumeric()
                && haystack.as_bytes()[end] != b'_';
        if before_ok && after_ok {
            result = Some(abs);
        }
        start = abs + 1;
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- TABLE shorthand ---

    #[test]
    fn table_shorthand_simple() {
        assert_eq!(rewrite_table_shorthand("TABLE foo"), "SELECT * FROM foo");
    }

    #[test]
    fn table_shorthand_schema_qualified() {
        assert_eq!(
            rewrite_table_shorthand("TABLE public.events"),
            "SELECT * FROM public.events"
        );
    }

    #[test]
    fn table_shorthand_with_semicolon() {
        assert_eq!(
            rewrite_table_shorthand("TABLE foo;"),
            "SELECT * FROM foo"
        );
    }

    #[test]
    fn table_shorthand_case_insensitive() {
        assert_eq!(rewrite_table_shorthand("table foo"), "SELECT * FROM foo");
    }

    #[test]
    fn table_shorthand_no_match_create_table() {
        // CREATE TABLE should not be rewritten.
        let sql = "CREATE TABLE foo (id BIGINT)";
        assert_eq!(rewrite_table_shorthand(sql), sql);
    }

    #[test]
    fn table_shorthand_no_match_select() {
        let sql = "SELECT * FROM foo";
        assert_eq!(rewrite_table_shorthand(sql), sql);
    }

    // --- TABLESAMPLE strip ---

    #[test]
    fn tablesample_bernoulli_stripped() {
        let sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10)";
        let out = strip_tablesample(sql);
        assert!(!out.contains("TABLESAMPLE"), "should strip TABLESAMPLE: {out}");
        assert!(out.contains("SELECT * FROM t"), "should keep base query: {out}");
    }

    #[test]
    fn tablesample_system_stripped() {
        let sql = "SELECT * FROM t TABLESAMPLE SYSTEM(25)";
        let out = strip_tablesample(sql);
        assert!(!out.contains("TABLESAMPLE"), "should strip TABLESAMPLE: {out}");
    }

    #[test]
    fn tablesample_with_where_clause() {
        let sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10) WHERE id > 5";
        let out = strip_tablesample(sql);
        assert!(!out.contains("TABLESAMPLE"), "should strip TABLESAMPLE: {out}");
        assert!(out.contains("WHERE id > 5"), "should keep WHERE: {out}");
    }

    #[test]
    fn no_tablesample_passthrough() {
        let sql = "SELECT * FROM t WHERE id > 5";
        assert_eq!(strip_tablesample(sql), sql);
    }

    // --- FETCH FIRST/NEXT rewrite ---

    #[test]
    fn fetch_first_rewritten() {
        let sql = "SELECT * FROM t FETCH FIRST 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10 in: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn fetch_next_rewritten() {
        let sql = "SELECT * FROM t FETCH NEXT 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10 in: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn offset_rows_fetch_next_rewritten() {
        let sql = "SELECT * FROM t OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10: {out}");
        assert!(out.contains("OFFSET 5"), "expected OFFSET 5: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn plain_limit_passthrough() {
        let sql = "SELECT * FROM t LIMIT 10 OFFSET 5";
        assert_eq!(rewrite_fetch_to_limit(sql), sql);
    }

    #[test]
    fn no_fetch_passthrough() {
        let sql = "SELECT id, name FROM t ORDER BY id";
        assert_eq!(rewrite_fetch_to_limit(sql), sql);
    }

    #[test]
    fn fetch_row_singular() {
        let sql = "SELECT * FROM t FETCH FIRST 1 ROW ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 1"), "expected LIMIT 1: {out}");
    }

    // --- FOR NO KEY UPDATE / FOR KEY SHARE rewrite ---

    #[test]
    fn for_no_key_update_rewritten_to_for_update() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert!(out.contains("FOR UPDATE"), "expected FOR UPDATE: {out}");
        assert!(
            !out.contains("NO KEY"),
            "NO KEY should be gone: {out}"
        );
    }

    #[test]
    fn for_no_key_update_skip_locked_rewritten() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE SKIP LOCKED";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR UPDATE SKIP LOCKED");
    }

    #[test]
    fn for_no_key_update_nowait_rewritten() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE NOWAIT";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR UPDATE NOWAIT");
    }

    #[test]
    fn for_key_share_rewritten_to_for_share() {
        let sql = "SELECT * FROM t FOR KEY SHARE";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR SHARE");
    }

    #[test]
    fn for_key_share_of_table_rewritten() {
        let sql = "SELECT * FROM t FOR KEY SHARE OF t";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR SHARE OF t");
    }

    #[test]
    fn for_update_passthrough_unchanged() {
        // FOR UPDATE already valid — must not be double-rewritten.
        let sql = "SELECT * FROM t FOR UPDATE";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }

    #[test]
    fn for_share_passthrough_unchanged() {
        let sql = "SELECT * FROM t FOR SHARE";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }

    #[test]
    fn no_lock_clause_passthrough() {
        let sql = "SELECT id FROM t WHERE id = 1";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }
}
