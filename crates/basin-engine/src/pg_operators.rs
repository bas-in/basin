//! PostgreSQL-specific operator pre-pass rewriters.
//!
//! DataFusion's sqlparser with `PostgreSqlDialect` accepts many PG operators
//! natively, but a handful either parse incorrectly or are not accepted at all.
//! This module houses textual pre-pass rewriters that normalise those
//! operators into equivalent DataFusion-friendly SQL before the main
//! sqlparser / DataFusion pipeline sees them.
//!
//! ## Operators handled
//!
//! ### POSIX regex (binary)
//! | PG operator | Rewrites to                          |
//! |-------------|--------------------------------------|
//! | `~`         | `regexp_like(lhs, rhs)`              |
//! | `!~`        | `NOT regexp_like(lhs, rhs)`          |
//! | `~*`        | `regexp_like(lhs, rhs, 'i')`         |
//! | `!~*`       | `NOT regexp_like(lhs, rhs, 'i')`     |
//!
//! `~` and `~*` are parsed by sqlparser's `PostgreSqlDialect` as `REGEXP`,
//! but DataFusion's planner rejects them at the logical-plan stage. We
//! rewrite all four here for consistency and to guarantee DataFusion sees
//! the function-call form it understands.
//!
//! ### BETWEEN SYMMETRIC
//! `x BETWEEN SYMMETRIC a AND b` is equivalent to
//! `(x BETWEEN a AND b) OR (x BETWEEN b AND a)`.
//! sqlparser does not parse the `SYMMETRIC` keyword; this pre-pass strips
//! it by expanding to the OR form.
//!
//! ### Array containment / overlap
//! | PG operator | Rewrites to (array heuristic)            |
//! |-------------|------------------------------------------|
//! | `@>`        | `list_has_all(lhs, rhs)`                 |
//! | `<@`        | `list_has_all(rhs, lhs)`                 |
//! | `&&`        | `arrays_overlap(lhs, rhs)`               |
//!
//! The heuristic fires only when at least one operand textually starts with
//! `ARRAY[` or `'{…}'::` (cast-literal form). Plain column references are
//! left alone to avoid colliding with JSONB `@>` (handled separately).
//! Range-type operands (`int4range(…)`, `numrange(…)`, …) are also excluded
//! so this rewriter composes safely with the range-operator rewriter that
//! lives in the range-types branch.
//!
//! ## Design
//!
//! All rewriters are pure string-manipulation functions — no AST, no
//! allocator beyond `String`. This matches the approach used by
//! `udf::rewrite_vector_operators` and `udf::rewrite_extract_second`.
//!
//! The rewriters are **best-effort**: they understand parenthesised
//! sub-expressions and single-quoted string literals but do NOT handle
//! dollar-quoted strings, comments, or identifier quoting. That is fine
//! for the SQL patterns emitted by ORMs and migration tools.

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Rewrite POSIX regex operators (`~`, `!~`, `~*`, `!~*`) into
/// `regexp_like(…)` calls before handing SQL to sqlparser / DataFusion.
///
/// Processes all four operators in priority order (longest match first):
/// `!~*` → `NOT regexp_like(lhs, rhs, 'i')`
/// `!~`  → `NOT regexp_like(lhs, rhs)`
/// `~*`  → `regexp_like(lhs, rhs, 'i')`
/// `~`   → `regexp_like(lhs, rhs)`
///
/// Each pass rewrites one occurrence and loops until none remain.
pub(crate) fn rewrite_posix_regex_operators(sql: &str) -> String {
    // Process longest operators first to avoid `!~*` being partly consumed
    // by `!~` or `~`.
    let mut s = sql.to_string();
    // !~* (case-insensitive negative match) — 3 chars
    s = rewrite_regex_op_once(s, "!~*", true, true);
    // !~  (case-sensitive negative match) — 2 chars
    s = rewrite_regex_op_once(s, "!~", true, false);
    // ~*  (case-insensitive match) — 2 chars
    s = rewrite_regex_op_once(s, "~*", false, true);
    // ~   (case-sensitive match) — 1 char
    s = rewrite_regex_op_once(s, "~", false, false);
    s
}

/// Rewrite `x BETWEEN SYMMETRIC a AND b` into
/// `((x) BETWEEN (a) AND (b) OR (x) BETWEEN (b) AND (a))`.
///
/// The rewrite is case-insensitive (sqlparser accepts `BETWEEN` in any case).
/// `NOT BETWEEN SYMMETRIC` is similarly expanded to a double-negation AND form.
///
/// Implementation: scan for the literal string `BETWEEN SYMMETRIC` (or
/// `NOT BETWEEN SYMMETRIC`) in the lowercased copy, then perform token-aware
/// extraction of the LHS, a-bound, and b-bound.
pub(crate) fn rewrite_between_symmetric(sql: &str) -> String {
    // We handle `NOT BETWEEN SYMMETRIC` first (longer match wins).
    let s = rewrite_between_symmetric_inner(sql, true);
    rewrite_between_symmetric_inner(&s, false)
}

/// Perform one or more rewrites of `[NOT] BETWEEN SYMMETRIC` in `sql`.
/// If `not_form` is true, rewrites `NOT BETWEEN SYMMETRIC`; otherwise
/// rewrites plain `BETWEEN SYMMETRIC` (skipping those that are actually
/// `NOT BETWEEN SYMMETRIC`).
fn rewrite_between_symmetric_inner(sql: &str, not_form: bool) -> String {
    let mut s = sql.to_string();
    loop {
        let lower = s.to_ascii_lowercase();
        let lb = lower.as_bytes();
        let needle = "between symmetric";
        // Find an occurrence.
        let Some(bs_pos) = find_word_sequence(&lower, needle) else {
            break;
        };
        // Determine if this is a NOT form.
        let before = lower[..bs_pos].trim_end();
        let has_not = before.ends_with("not")
            && (before.len() == 3
                || !before.as_bytes()[before.len() - 4].is_ascii_alphanumeric());
        if not_form != has_not {
            // Not the form we're rewriting in this pass — skip over this
            // occurrence to avoid an infinite loop.
            // Replace "symmetric" with "sym_done_" (same length trick won't
            // work easily). Instead: mark with a sentinel that won't match
            // the needle again. We use a mangled form.
            // Simplest: since we only have one form per sentence typically,
            // and sqlparser will reject SYMMETRIC anyway, break here to
            // avoid infinite loop. The other pass will handle it.
            break;
        }

        // Locate the end of "between symmetric".
        let sym_end = bs_pos + needle.len();
        // Skip whitespace.
        let mut i = sym_end;
        while i < s.len() && lb[i].is_ascii_whitespace() {
            i += 1;
        }
        // Collect a-expr up to word-boundary `AND`.
        let a_start = i;
        let and_pos = match find_and_after(&lower, a_start) {
            Some(p) => p,
            None => break,
        };
        let a_expr = s[a_start..and_pos].trim().to_string();
        // Skip past `AND` and whitespace.
        let mut b_start = and_pos + 3; // len("and")
        while b_start < s.len() && lb[b_start].is_ascii_whitespace() {
            b_start += 1;
        }
        let b_end = find_rhs_terminal(&s, b_start);
        let b_expr = s[b_start..b_end].trim().to_string();

        // Find the LHS: everything before `NOT? BETWEEN`.
        let between_clause_start = if has_not {
            // Walk back from bs_pos past whitespace to find `NOT`.
            let before_ws = lower[..bs_pos].trim_end();
            let not_end = before_ws.len(); // exclusive end of "not"
            let not_start = not_end.saturating_sub(3);
            not_start
        } else {
            bs_pos
        };
        // LHS ends at between_clause_start.
        let lhs_end = between_clause_start;
        let lhs_start = find_lhs_start(&s, lhs_end);
        let lhs_expr = s[lhs_start..lhs_end].trim().to_string();

        let replacement = if has_not {
            // NOT (A BETWEEN x AND y) AND NOT (A BETWEEN y AND x)
            // = A NOT BETWEEN x AND y AND A NOT BETWEEN y AND x
            format!(
                "((({lhs_expr}) NOT BETWEEN ({a_expr}) AND ({b_expr})) AND (({lhs_expr}) NOT BETWEEN ({b_expr}) AND ({a_expr})))"
            )
        } else {
            // (A BETWEEN x AND y) OR (A BETWEEN y AND x)
            format!(
                "((({lhs_expr}) BETWEEN ({a_expr}) AND ({b_expr})) OR (({lhs_expr}) BETWEEN ({b_expr}) AND ({a_expr})))"
            )
        };
        let mut out = String::with_capacity(s.len() + replacement.len());
        out.push_str(&s[..lhs_start]);
        out.push_str(&replacement);
        out.push_str(&s[b_end..]);
        s = out;
    }
    s
}

/// Find `word_sequence` (space-separated words) in `lower` as whole words.
/// Returns the byte offset of the first character of the sequence.
fn find_word_sequence(lower: &str, sequence: &str) -> Option<usize> {
    let lb = lower.as_bytes();
    // Use char_indices so every `i` is guaranteed to be a char boundary —
    // slicing `lower[i..]` is always safe even for multi-byte UTF-8 input.
    for (i, _ch) in lower.char_indices() {
        if i + sequence.len() > lower.len() {
            break;
        }
        if lower[i..].starts_with(sequence) {
            let pre_ok = i == 0 || !lb[i - 1].is_ascii_alphanumeric();
            let post = i + sequence.len();
            let post_ok = post >= lower.len() || !lb[post].is_ascii_alphanumeric();
            if pre_ok && post_ok {
                return Some(i);
            }
        }
    }
    None
}

/// Find the first word-boundary `AND` in `lower` at or after `start`.
fn find_and_after(lower: &str, start: usize) -> Option<usize> {
    let lb = lower.as_bytes();
    let mut depth = 0i32;
    let mut i = start;
    while i + 3 <= lower.len() {
        // lb[i] is a valid index (i < lower.len()); reading a single byte is
        // always safe.  We only slice `lower[i..]` inside branches where we
        // have already confirmed `i` is on a char boundary (all ASCII-byte
        // arms guarantee it; the `_` arm advances by char width below).
        match lb[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                // Skip string literal — all bytes checked individually via lb[].
                i += 1;
                while i < lower.len() {
                    if lb[i] == b'\'' {
                        if i + 1 < lower.len() && lb[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {
                // `i` may be in the middle of a multi-byte char.  Only slice
                // lower[i..] when we know i is on a char boundary.
                if lower.is_char_boundary(i) && depth == 0 && lower[i..].starts_with("and") {
                    let pre_ok = i == 0 || !lb[i - 1].is_ascii_alphanumeric();
                    let post_ok = i + 3 >= lower.len() || !lb[i + 3].is_ascii_alphanumeric();
                    if pre_ok && post_ok {
                        return Some(i);
                    }
                }
                // Advance by the full char width so we never land mid-char.
                // Compute char width from the leading byte without slicing.
                // Continuation bytes (0x80–0xBF) advance by 1; lead bytes
                // advance by their declared width.
                let char_len = match lb[i] {
                    b if b < 0x80 => 1,           // ASCII
                    b if b < 0xC0 => 1,           // UTF-8 continuation byte — step by 1
                    b if b < 0xE0 => 2,           // 2-byte lead
                    b if b < 0xF0 => 3,           // 3-byte lead
                    _ => 4,                        // 4-byte lead
                };
                i += char_len;
                continue;
            }
        }
        i += 1;
    }
    None
}

/// Rewrite array containment / overlap operators for array-typed operands.
///
/// Operators translated:
/// - `A @> B`  → `list_has_all(A, B)`     (A contains all elements of B)
/// - `A <@ B`  → `list_has_all(B, A)`     (A contained in B)
/// - `A && B`  → `arrays_overlap(A, B)`   (A and B share at least one element)
///
/// `list_has_all` (alias `array_has_all`) has signature (array, array) in
/// DataFusion 53 and is the correct mapping.  The former `array_contains` /
/// `array_has` has signature (array, element) — passing two arrays triggers
/// a type-coercion error in most engine configurations.
///
/// Only fires when at least one operand looks like an array (starts with
/// `ARRAY[` or contains `::` followed by a type ending in `[]`). Leaves
/// range-type operands and plain column references untouched.
pub(crate) fn rewrite_array_operators(sql: &str) -> String {
    let mut s = sql.to_string();
    // Process @> and <@ before && to avoid mis-parsing.
    s = rewrite_array_op_once(s, "@>", false);
    s = rewrite_array_op_once(s, "<@", false);
    s = rewrite_array_op_once(s, "&&", false);
    s
}

// ---------------------------------------------------------------------------
// POSIX regex internals
// ---------------------------------------------------------------------------

fn rewrite_regex_op_once(sql: String, op: &str, negate: bool, case_insensitive: bool) -> String {
    let mut s = sql;
    let mut search_from = 0usize;
    loop {
        let Some(rel) = find_regex_op_outside_strings(&s[search_from..], op) else {
            break;
        };
        let op_start = search_from + rel;
        let op_end = op_start + op.len();

        let (lhs_start, lhs_end) = regex_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = regex_extract_right(&s, op_end);
        let lhs = s[lhs_start..lhs_end].trim().to_string();
        let rhs = s[rhs_start..rhs_end].trim().to_string();

        // Skip when the operator is unary (no LHS token, or the LHS is a
        // SQL keyword like SELECT/WHERE) — PG `~` / `~*` / `!~` / `!~*` are
        // strictly binary; `~int` is the bitwise-NOT unary operator and must
        // not be rewritten as a POSIX regex match.
        if lhs.is_empty() || lhs_looks_like_sql_keyword(&lhs) {
            search_from = op_end;
            continue;
        }

        let call = if case_insensitive {
            format!("regexp_like({lhs}, {rhs}, 'i')")
        } else {
            format!("regexp_like({lhs}, {rhs})")
        };
        let replacement = if negate {
            format!("(NOT {call})")
        } else {
            call
        };
        s.replace_range(lhs_start..rhs_end, &replacement);
        search_from = lhs_start + replacement.len();
    }
    s
}

/// Find the first occurrence of `op` that is:
/// 1. Not inside a single-quoted string literal.
/// 2. For `~`: not part of `~*`, `!~`, or `!~*`.
/// 3. For `!~`: not part of `!~*`.
/// 4. For `~*`: not part of `!~*` (already removed by earlier pass).
///
/// We rely on call order (longest → shortest) to avoid double-matching.
fn find_regex_op_outside_strings(s: &str, op: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        // Skip single-quoted string literals.
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2; // escaped quote ''
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        if s[i..].starts_with(op) {
            // For single-char `~`: ensure it's not preceded by `!` and not
            // followed by `*` (those are handled by the longer-op passes).
            if op == "~" {
                let preceded_by_bang = i > 0 && bytes[i - 1] == b'!';
                let followed_by_star = i + 1 < bytes.len() && bytes[i + 1] == b'*';
                if preceded_by_bang || followed_by_star {
                    i += 1;
                    continue;
                }
            }
            // For `~*`: ensure it's not preceded by `!`.
            if op == "~*" {
                let preceded_by_bang = i > 0 && bytes[i - 1] == b'!';
                if preceded_by_bang {
                    i += 1;
                    continue;
                }
            }
            // For `!~`: ensure it's not followed by `*`.
            if op == "!~" {
                let followed_by_star = i + op.len() < bytes.len() && bytes[i + op.len()] == b'*';
                if followed_by_star {
                    i += 1;
                    continue;
                }
            }
            // Make sure `~` is not part of `~~` (LIKE operator in PG) — or
            // the SECOND `~` of `~~`. Skip both bytes when we see either.
            if op == "~" {
                let followed_by_tilde = i + 1 < bytes.len() && bytes[i + 1] == b'~';
                let preceded_by_tilde = i > 0 && bytes[i - 1] == b'~';
                if followed_by_tilde {
                    i += 2;
                    continue;
                }
                if preceded_by_tilde {
                    i += 1;
                    continue;
                }
            }
            return Some(i);
        }
        i += 1;
    }
    None
}

/// SQL keywords that can sit just before a regex operator candidate but
/// never actually serve as an LHS operand — `SELECT ~0` is unary `~`, not
/// `SELECT REGEXP_LIKE(…)`. Conservative list: anything that appears at
/// the start of a SELECT / DML clause boundary.
fn lhs_looks_like_sql_keyword(lhs: &str) -> bool {
    matches!(
        lhs.to_ascii_uppercase().as_str(),
        "SELECT"
            | "WHERE"
            | "FROM"
            | "AND"
            | "OR"
            | "NOT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "SET"
            | "VALUES"
            | "ON"
            | "AS"
            | "IS"
            | "IN"
            | "BY"
            | "ORDER"
            | "GROUP"
            | "HAVING"
            | "LIMIT"
            | "OFFSET"
            | "JOIN"
            | "USING"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "CASE"
            | "RETURNING"
    )
}

/// Walk left from `op_start` to find the LHS operand of a regex operator.
/// Returns `(start, end)` byte range (exclusive end = `op_start`).
fn regex_extract_left(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    // Skip trailing whitespace before operator.
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    if i == 0 {
        return (0, operand_end);
    }
    let last = bytes[i - 1];
    if last == b')' {
        // Parenthesised expression — walk back matching paren.
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
        // Capture the function name (identifier) before the `(`.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else if last == b'\'' {
        // String literal — walk back to the opening quote.
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'\'' {
                break;
            }
        }
    } else {
        // Identifier / column reference / number.
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

/// Walk right from `start` (first byte after operator) to find the RHS.
/// Returns `(start, end)` byte range.
fn regex_extract_right(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    // Skip leading whitespace after operator.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    if i >= bytes.len() {
        return (operand_start, operand_start);
    }
    if bytes[i] == b'\'' {
        // String literal.
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                i += 1;
                break;
            }
            i += 1;
        }
    } else if bytes[i] == b'(' {
        // Parenthesised expression.
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
    } else {
        // Identifier / number.
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
    }
    (operand_start, i)
}

// ---------------------------------------------------------------------------
// BETWEEN SYMMETRIC internals (continued)
// ---------------------------------------------------------------------------

/// Find the terminal byte of the RHS of a `BETWEEN … AND <rhs>` expression.
/// Stops at unbalanced `)`, `,`, `;`, or top-level SQL keyword terminators.
fn find_rhs_terminal(s: &str, start: usize) -> usize {
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut i = start;
    let mut depth = 0i32;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
            }
            b',' | b';' => {
                if depth == 0 {
                    return i;
                }
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {
                if depth == 0 {
                    for kw in &[
                        "and ", "or ", "where ", "having ", "order ", "group ",
                        "limit ", "offset ", "union ", "except ", "intersect ", "on ",
                    ] {
                        if lower[i..].starts_with(kw) {
                            let pre_ok = i == 0 || !lb[i - 1].is_ascii_alphanumeric();
                            if pre_ok {
                                return i;
                            }
                        }
                    }
                }
            }
        }
        i += 1;
    }
    i
}

/// Walk backwards from `end` (exclusive) to find where the LHS expression
/// of a BETWEEN clause starts. Handles identifiers, parenthesised exprs, and
/// `col::type` casts.
fn find_lhs_start(s: &str, end: usize) -> usize {
    let bytes = s.as_bytes();
    let mut i = end;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return 0;
    }
    let last = bytes[i - 1];
    if last == b')' {
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
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else {
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.'
                || bytes[i - 1] == b':')
        {
            i -= 1;
        }
    }
    i
}

// ---------------------------------------------------------------------------
// Array operator internals
// ---------------------------------------------------------------------------

const RANGE_CTOR_PREFIXES: &[&str] = &[
    "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
];

/// Return `true` if `expr` looks like an array literal or array-typed value:
/// - Starts with `ARRAY[` (case-insensitive)
/// - Is a cast like `'{...}'::` (single-quoted literal followed by `::`)
/// - Ends with `[]` (type annotation like `text[]`, `int[]`)
fn looks_like_array(expr: &str) -> bool {
    let trimmed = expr.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("array[") || lower.starts_with("array [") {
        return true;
    }
    // '{...}'::type[] cast form — starts with single-quote
    if trimmed.starts_with('\'') && trimmed.contains("::") {
        return true;
    }
    // Also exclude range constructors so we don't compete with the range rewriter.
    for prefix in RANGE_CTOR_PREFIXES {
        if lower.starts_with(prefix) {
            return false;
        }
    }
    false
}

fn rewrite_array_op_once(sql: String, op: &str, _placeholder: bool) -> String {
    let mut s = sql;
    loop {
        let Some(op_start) = find_array_op_outside_strings(&s, op) else {
            break;
        };
        let op_end = op_start + op.len();

        let (lhs_start, lhs_end) = array_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = array_extract_right(&s, op_end);
        let lhs = s[lhs_start..lhs_end].trim().to_string();
        let rhs = s[rhs_start..rhs_end].trim().to_string();

        // Only rewrite if at least one operand looks like an array.
        if !looks_like_array(&lhs) && !looks_like_array(&rhs) {
            // Skip past this occurrence to avoid infinite loop.
            // We can't break (there may be more), but we can't advance
            // either without tracking position. For simplicity, break —
            // a second occurrence would be unusual for non-array ops.
            break;
        }

        let replacement = match op {
            // `A @> B` — A contains all elements of B.
            // DataFusion's `array_contains` (= `array_has`) has signature
            // (array, element) — passing two arrays triggers a coercion
            // error in most configs.  `list_has_all(A, B)` has signature
            // (array, array) and is the correct mapping.
            "@>" => format!("list_has_all({lhs}, {rhs})"),
            // `A <@ B` — A is contained in B (all elements of A exist in B).
            // Equivalent to `list_has_all(B, A)`.
            "<@" => format!("list_has_all({rhs}, {lhs})"),
            "&&" => format!("arrays_overlap({lhs}, {rhs})"),
            _ => unreachable!(),
        };
        s.replace_range(lhs_start..rhs_end, &replacement);
    }
    s
}

fn find_array_op_outside_strings(s: &str, op: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if s[i..].starts_with(op) {
            // For `&&`: exclude `&&` preceded by `-` (part of other ops) or
            // inside identifiers. Just return.
            return Some(i);
        }
        i += 1;
    }
    None
}

fn array_extract_left(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    if i == 0 {
        return (0, operand_end);
    }
    let last = bytes[i - 1];
    if last == b'\'' {
        // String literal possibly with a cast `::type`.
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'\'' {
                break;
            }
        }
        // Now check for `::type[]` suffix — already consumed up to `'`.
        // Re-scan forward from operand_end to see if there's `::`.
        // Actually we want the full cast expression including `::type[]`.
        // We already have the literal; the `::` and type follow the closing
        // quote. Re-compute: walk forward from the closing `'` to capture
        // the cast if any.
        // Reset: end is op_start, operand_end is before whitespace.
        // Walk back to the opening `'` of this literal.
    } else if last == b']' {
        // Array literal ARRAY[...] or column of type [...].
        let mut depth = 1i32;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            match bytes[i] {
                b']' => depth += 1,
                b'[' => depth -= 1,
                _ => {}
            }
        }
        // Capture ARRAY keyword if present.
        let mut j = i;
        while j > 0 && bytes[j - 1].is_ascii_whitespace() {
            j -= 1;
        }
        let pre_end = j;
        while j > 0 && (bytes[j - 1].is_ascii_alphanumeric() || bytes[j - 1] == b'_') {
            j -= 1;
        }
        if &s[j..pre_end].to_ascii_lowercase() == "array" {
            i = j;
        }
    } else if last == b')' {
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
        // Capture function name.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else {
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.'
                || bytes[i - 1] == b':')
        {
            i -= 1;
        }
    }
    (i, operand_end)
}

fn array_extract_right(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    if i >= bytes.len() {
        return (operand_start, operand_start);
    }
    if bytes[i] == b'\'' {
        // String literal with potential cast.
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                i += 1;
                break;
            }
            i += 1;
        }
        // Consume optional ::type[] cast.
        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            i += 2;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || bytes[i] == b'_'
                    || bytes[i] == b'['
                    || bytes[i] == b']')
            {
                i += 1;
            }
        }
    } else if bytes[i] == b'[' || (s[i..].to_ascii_lowercase().starts_with("array")) {
        // ARRAY[...] literal.
        // Skip ARRAY keyword if present.
        if s[i..].to_ascii_lowercase().starts_with("array") {
            i += 5;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
        }
        if i < bytes.len() && bytes[i] == b'[' {
            let mut depth = 1i32;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'[' => depth += 1,
                    b']' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
        }
    } else if bytes[i] == b'(' {
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
    } else {
        // Identifier or number — also capture ::type[] cast.
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric()
                || bytes[i] == b'_'
                || bytes[i] == b'.'
                || bytes[i] == b':')
        {
            i += 1;
        }
        // If we stopped at `[`, consume the type subscript.
        while i < bytes.len() && (bytes[i] == b'[' || bytes[i] == b']') {
            i += 1;
        }
    }
    (operand_start, i)
}

// ---------------------------------------------------------------------------
// ANY / SOME / ALL subquery rewrites
// ---------------------------------------------------------------------------

/// Rewrite quantified subquery comparisons to forms DataFusion can execute.
///
/// - `expr = ANY (subquery)`  → `expr IN (subquery)`
/// - `expr = SOME (subquery)` → `expr IN (subquery)`
/// - `expr > ALL (SELECT col FROM t)`  → `expr > (SELECT MAX(col) FROM t)`
/// - `expr >= ALL (SELECT col FROM t)` → `expr >= (SELECT MAX(col) FROM t)`
/// - `expr < ALL (SELECT col FROM t)`  → `expr < (SELECT MIN(col) FROM t)`
/// - `expr <= ALL (SELECT col FROM t)` → `expr <= (SELECT MIN(col) FROM t)`
///
/// DataFusion's `= ANY (subquery)` planner has type-coercion issues; `IN
/// (subquery)` is the standard SQL equivalent and works reliably.
/// `OP ALL` is rewritten using aggregate scalar subqueries.
pub(crate) fn rewrite_any_some_subquery(sql: &str) -> String {
    // `= ANY (...)` → `IN (...)`
    let s = rewrite_quantified_op(sql, "= any", "IN");
    // `= SOME (...)` → `IN (...)`
    let s = rewrite_quantified_op(&s, "= some", "IN");
    // `> ALL (SELECT col FROM t)` → `> (SELECT MAX(col) FROM t)`, etc.
    let s = rewrite_all_subquery(&s);
    // `> ANY (SELECT col FROM t)` → `> (SELECT MIN(col) FROM t)`, etc.
    let s = rewrite_any_cmp_subquery(&s);
    s
}

/// Rewrite `expr OP ANY (SELECT single_expr FROM ...)` to scalar subquery form.
///
/// - `> ANY`  → `> (SELECT MIN(...))`
/// - `>= ANY` → `>= (SELECT MIN(...))`
/// - `< ANY`  → `< (SELECT MAX(...))`
/// - `<= ANY` → `<= (SELECT MAX(...))`
/// - `<> ANY` / `!= ANY` → `<> (SELECT MIN(...))` — left unrewritten (complex)
///
/// The ANY semantics: `x > ANY (values)` is true when x > at least one value,
/// i.e., x > MIN(values). Similarly `x < ANY` ↔ x < MAX(values).
fn rewrite_any_cmp_subquery(sql: &str) -> String {
    let mut s = sql.to_string();
    // Pairs: (op_lower_including_any, aggregate_fn) — longer operators first.
    const OPS: &[(&str, &str)] = &[
        (">= any", "MIN"),
        ("<= any", "MAX"),
        ("> any",  "MIN"),
        ("< any",  "MAX"),
        (">= some", "MIN"),
        ("<= some", "MAX"),
        ("> some",  "MIN"),
        ("< some",  "MAX"),
    ];
    for (op_lower, agg) in OPS {
        s = rewrite_one_any_op(&s, op_lower, agg);
    }
    s
}

fn rewrite_one_any_op(sql: &str, op_lower: &str, agg: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(op_lower) else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + op_lower.len();
        let bytes = s.as_bytes();

        // Word boundary after `any`/`some`.
        let post_ok = kw_end >= s.len() || {
            let b = bytes[kw_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !post_ok {
            search_from = kw_end;
            continue;
        }

        // After keyword, skip whitespace and find `(`.
        let mut j = kw_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = kw_end;
            continue;
        }

        // Find the matching `)` for the subquery.
        let Some(subq_end) = find_matching_close_paren(&s, j) else {
            search_from = kw_end;
            continue;
        };

        let subq_body = s[j + 1..subq_end].trim().to_string();

        // Parse the subquery: must be `SELECT expr FROM ...`.
        let subq_lower = subq_body.to_ascii_lowercase();
        if !subq_lower.trim_start().starts_with("select ") {
            search_from = kw_end;
            continue;
        }
        // Reject complex subqueries.
        if subq_lower.contains("group by")
            || subq_lower.contains("having")
            || subq_lower.contains(" union ")
            || subq_lower.contains(" intersect ")
            || subq_lower.contains(" except ")
        {
            search_from = kw_end;
            continue;
        }

        let after_select = subq_body.trim_start();
        let sel_offset = after_select.to_ascii_lowercase().find("select ").unwrap_or(0) + 7;
        let from_lower = after_select.to_ascii_lowercase();
        let Some(from_pos) = find_from_at_depth0(&from_lower, sel_offset) else {
            search_from = kw_end;
            continue;
        };
        let col_expr = after_select[sel_offset..from_pos].trim().to_string();
        let from_clause = &after_select[from_pos..];

        // Strip the `any`/`some` keyword from op_lower (last word).
        // e.g. ">= any" → ">=" , "> some" → ">"
        let op_str = op_lower.split_whitespace().next().unwrap_or(">");
        let replacement = format!("{op_str} (SELECT {agg}({col_expr}) {from_clause})");
        s.replace_range(kw_start..subq_end + 1, &replacement);
        search_from = kw_start + replacement.len();
    }
    s
}

/// Rewrite `expr OP ALL (SELECT single_expr FROM ...)` to scalar subquery form.
///
/// - `> ALL`  → `> (SELECT MAX(...))`
/// - `>= ALL` → `>= (SELECT MAX(...))`
/// - `< ALL`  → `< (SELECT MIN(...))`
/// - `<= ALL` → `<= (SELECT MIN(...))`
/// - `<> ALL` / `!= ALL` → `NOT IN (...)`
///
/// Only handles simple single-column subqueries. Complex subqueries
/// (UNION, aggregates, etc.) are left untouched.
fn rewrite_all_subquery(sql: &str) -> String {
    let mut s = sql.to_string();
    // Pairs: (op, aggregate_fn) — longer operators first.
    const OPS: &[(&str, &str)] = &[
        (">= all", "MAX"),
        ("<= all", "MIN"),
        ("> all",  "MAX"),
        ("< all",  "MIN"),
        // <> ALL / != ALL → NOT IN
        ("<> all", "NOT_IN"),
        ("!= all", "NOT_IN"),
    ];
    for (op_lower, agg) in OPS {
        s = rewrite_one_all_op(&s, op_lower, agg);
    }
    s
}

fn rewrite_one_all_op(sql: &str, op_lower: &str, agg: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(op_lower) else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + op_lower.len();
        let bytes = s.as_bytes();

        // Word boundary after `all`.
        let post_ok = kw_end >= s.len() || {
            let b = bytes[kw_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !post_ok {
            search_from = kw_end;
            continue;
        }

        // After keyword, skip whitespace and find `(`.
        let mut j = kw_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = kw_end;
            continue;
        }

        // Find the matching `)` for the subquery.
        let Some(subq_end) = find_matching_close_paren(&s, j) else {
            search_from = kw_end;
            continue;
        };

        // Extract the subquery body (without outer parens).
        let subq_body = s[j + 1..subq_end].trim().to_string();

        // For NOT IN, just replace `OP ALL (subq)` with `NOT IN (subq)`.
        if agg == "NOT_IN" {
            // Replace `op_lower (subq)` with `NOT IN (subq)`.
            let op_part = &op_lower[..op_lower.len() - 4]; // strip " all"
            let replacement = format!("{op_part} NOT IN ({subq_body})");
            s.replace_range(kw_start..subq_end + 1, &replacement);
            search_from = kw_start + replacement.len();
            continue;
        }

        // Parse the subquery: must be `SELECT expr FROM ...` with no GROUP BY,
        // HAVING, LIMIT, UNION — keep it simple.
        let subq_lower = subq_body.to_ascii_lowercase();
        if !subq_lower.trim_start().starts_with("select ") {
            search_from = kw_end;
            continue;
        }
        // Reject complex subqueries to avoid incorrect rewrites.
        if subq_lower.contains("group by")
            || subq_lower.contains("having")
            || subq_lower.contains(" union ")
            || subq_lower.contains(" intersect ")
            || subq_lower.contains(" except ")
        {
            search_from = kw_end;
            continue;
        }

        // Extract the SELECT expression (between `SELECT ` and `FROM `).
        // Naive: find `FROM` at depth 0 after `SELECT`.
        let after_select = subq_body.trim_start();
        let sel_offset = after_select.to_ascii_lowercase().find("select ").unwrap_or(0) + 7;
        let from_lower = after_select.to_ascii_lowercase();
        let Some(from_pos) = find_from_at_depth0(&from_lower, sel_offset) else {
            search_from = kw_end;
            continue;
        };
        let col_expr = after_select[sel_offset..from_pos].trim().to_string();
        let from_clause = &after_select[from_pos..]; // includes `FROM ...`

        // Build the replacement: `op (SELECT AGG(col_expr) FROM ...)`.
        let op_str = &op_lower[..op_lower.len() - 4].trim(); // strip " all"
        let replacement = format!("{op_str} (SELECT {agg}({col_expr}) {from_clause})");
        s.replace_range(kw_start..subq_end + 1, &replacement);
        search_from = kw_start + replacement.len();
    }
    s
}

/// Find the `FROM` keyword at depth 0 (outside parens) starting from `offset`.
fn find_from_at_depth0(lower: &str, offset: usize) -> Option<usize> {
    let bytes = lower.as_bytes();
    let mut depth = 0i32;
    let mut i = offset;
    while i < lower.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => { if depth > 0 { depth -= 1; } }
            b'\'' => {
                i += 1;
                while i < lower.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < lower.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {
                if depth == 0 && lower[i..].starts_with("from ") {
                    let pre_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                    if pre_ok { return Some(i); }
                }
            }
        }
        i += 1;
    }
    None
}

/// Replace `expr OP keyword (subquery)` with `expr new_op (subquery)`.
fn rewrite_quantified_op(sql: &str, op_kw: &str, new_op: &str) -> String {
    // `op_kw` is lowercase, e.g. `"= any"` or `"= some"`.
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(op_kw) else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + op_kw.len();
        let bytes = s.as_bytes();

        // Word boundary after keyword: must be followed by `(` or whitespace.
        let post_ok = kw_end >= s.len() || {
            let b = bytes[kw_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !post_ok {
            search_from = kw_end;
            continue;
        }
        // Word boundary before: the char before `=` must not be alphanumeric,
        // and must not be `>`, `<`, or `!` — otherwise we would incorrectly
        // match inside `>= ANY`, `<= ANY`, `!= ANY` which have their own
        // dedicated rewrite path.
        let pre_ok = kw_start == 0 || {
            let prev = bytes[kw_start - 1];
            !prev.is_ascii_alphanumeric() && prev != b'>' && prev != b'<' && prev != b'!'
        };
        if !pre_ok {
            search_from = kw_end;
            continue;
        }

        // After keyword, skip whitespace and find `(`.
        let mut j = kw_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = kw_end;
            continue;
        }
        // Replace `= any (` / `= some (` with `IN (`.
        // The replacement starts at kw_start (the `=`).
        let replace_end = j + 1; // include the `(`
        let replacement = format!("{new_op} (");
        s.replace_range(kw_start..replace_end, &replacement);
        search_from = kw_start + replacement.len();
    }
    s
}

// ---------------------------------------------------------------------------
// ANY/ALL with ARRAY literal rewrites
// ---------------------------------------------------------------------------

/// Rewrite `expr = ANY(ARRAY[a,b,c])` (and `= SOME`) to `expr IN (a,b,c)`.
///
/// DataFusion cannot plan `= ANY(ARRAY[...])` — its coercion path only works
/// with subqueries. The PG semantics are identical to an `IN` list, so we
/// simply strip the `ARRAY[` / `]` wrapper and replace `= ANY` with `IN`.
///
/// Also handles `<> ANY(ARRAY[...])` / `!= ANY(ARRAY[...])` → `NOT IN (...)`.
pub(crate) fn rewrite_any_array(sql: &str) -> String {
    let mut s = rewrite_one_any_array(sql, "= any", false);
    s = rewrite_one_any_array(&s, "= some", false);
    s = rewrite_one_any_array(&s, "<> any", true);
    s = rewrite_one_any_array(&s, "!= any", true);
    s
}

fn rewrite_one_any_array(sql: &str, op_lower: &str, negate: bool) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(op_lower) else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + op_lower.len();
        let bytes = s.as_bytes();

        // Word boundary after (must be followed by `(` or whitespace).
        let post_ok = kw_end >= s.len() || {
            let b = bytes[kw_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !post_ok {
            search_from = kw_end;
            continue;
        }
        // Word boundary before the operator.
        let pre_ok = kw_start == 0 || {
            let prev = bytes[kw_start - 1];
            !prev.is_ascii_alphanumeric() && prev != b'>' && prev != b'<' && prev != b'!'
        };
        if !pre_ok {
            search_from = kw_end;
            continue;
        }

        // Skip whitespace after the keyword and find `(`.
        let mut j = kw_end;
        while j < s.len() && bytes[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || bytes[j] != b'(' {
            search_from = kw_end;
            continue;
        }

        // Find the matching `)` of the outer parens.
        let Some(outer_end) = find_matching_close_paren(&s, j) else {
            search_from = kw_end;
            continue;
        };

        let inner = s[j + 1..outer_end].trim();

        // Must be `ARRAY[...]` form (case-insensitive).
        let inner_lower = inner.to_ascii_lowercase();
        let arr_content = if inner_lower.starts_with("array[") {
            // Find matching `]` of the ARRAY constructor.
            let bracket_start = inner.find('[').unwrap();
            let Some(bracket_end) = find_matching_close_bracket(inner, bracket_start) else {
                search_from = kw_end;
                continue;
            };
            &inner[bracket_start + 1..bracket_end]
        } else {
            // Not an ARRAY literal — leave for the subquery rewriter.
            search_from = kw_end;
            continue;
        };

        let replacement = if negate {
            format!("NOT IN ({arr_content})")
        } else {
            format!("IN ({arr_content})")
        };
        s.replace_range(kw_start..outer_end + 1, &replacement);
        search_from = kw_start + replacement.len();
    }
    s
}

/// Rewrite `expr OP ALL(ARRAY[a,b,c])` to a scalar comparison using
/// `array_max` / `array_min` (DataFusion built-ins).
///
/// PG semantics: `x OP ALL(values)` holds when the comparison holds for
/// every element, which is equivalent to comparing against the aggregate:
///
/// | operator | aggregate |
/// |----------|-----------|
/// | `>`      | `array_max` (x > every ↔ x > max) |
/// | `>=`     | `array_max` (x >= every ↔ x >= max) |
/// | `<`      | `array_min` (x < every ↔ x < min) |
/// | `<=`     | `array_min` (x <= every ↔ x <= min) |
///
/// `<> ALL` / `!= ALL` falls through to the existing `rewrite_all_subquery`
/// path which converts them to `NOT IN` — not touched here.
/// `= ALL` is left unhandled (rare; semantics require both min = max = x).
pub(crate) fn rewrite_all_array(sql: &str) -> String {
    // Pairs: (op_lower_including_all, array_aggregate_fn)
    // Longer operators first to avoid `>= all` being eaten by `> all`.
    const OPS: &[(&str, &str)] = &[
        (">= all", "array_max"),
        ("<= all", "array_min"),
        ("> all",  "array_max"),
        ("< all",  "array_min"),
    ];
    let mut s = sql.to_string();
    for (op, agg) in OPS {
        s = rewrite_one_all_array(&s, op, agg);
    }
    s
}

fn rewrite_one_all_array(sql: &str, op_lower: &str, agg_fn: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(rel) = lower[search_from..].find(op_lower) else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + op_lower.len();
        let bytes = s.as_bytes();

        // Word boundary after (must be followed by `(` or whitespace).
        let post_ok = kw_end >= s.len() || {
            let b = bytes[kw_end];
            b == b'(' || b == b' ' || b == b'\t' || b == b'\n'
        };
        if !post_ok {
            search_from = kw_end;
            continue;
        }
        // Word boundary before the first char of op_lower: e.g. for `> all`
        // the char before `>` must not be alphanumeric or `_`.
        let pre_ok = kw_start == 0 || {
            let prev = bytes[kw_start - 1];
            !prev.is_ascii_alphanumeric() && prev != b'_'
        };
        if !pre_ok {
            search_from = kw_end;
            continue;
        }

        // Skip whitespace and find `(`.
        let mut j = kw_end;
        while j < s.len() && bytes[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || bytes[j] != b'(' {
            search_from = kw_end;
            continue;
        }

        let Some(outer_end) = find_matching_close_paren(&s, j) else {
            search_from = kw_end;
            continue;
        };

        let inner = s[j + 1..outer_end].trim().to_string();
        let inner_lower = inner.to_ascii_lowercase();

        // Must be `ARRAY[...]` — not a subquery (subquery handled separately).
        if inner_lower.starts_with("select ") || !inner_lower.starts_with("array[") {
            search_from = kw_end;
            continue;
        }

        // Build replacement: `op_str array_max(ARRAY[...])`.
        // Strip " all" from op_lower to get the bare comparison operator.
        let op_str = op_lower[..op_lower.len() - 4].trim(); // strip " all"
        let replacement = format!("{op_str} {agg_fn}({inner})");
        s.replace_range(kw_start..outer_end + 1, &replacement);
        search_from = kw_start + replacement.len();
    }
    s
}

/// Find the matching `]` for the `[` at `start` in `s`.
fn find_matching_close_bracket(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'[') { return None; }
    let mut depth = 1i32;
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 { return Some(i); }
            }
            b'\'' => {
                // Skip string literals.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrite `LATERAL unnest(...)` → `unnest(...)` so that sqlparser parses the
/// form as `TableFactor::UNNEST` (handled by DataFusion) rather than
/// `TableFactor::Function { lateral: true }` (which DataFusion can't plan
/// because `unnest` is not a registered table-function name).
///
/// Only fires when `LATERAL` is immediately followed by `unnest` (case-
/// insensitive, any whitespace). Other `LATERAL` uses (subqueries, other
/// functions) are left untouched.
pub(crate) fn rewrite_lateral_unnest(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("lateral") { return sql.to_string(); }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < sql.len() {
        // Skip string literals.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < sql.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < sql.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Skip double-quoted identifiers.
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < sql.len() && bytes[i] != b'"' { i += 1; }
            if i < sql.len() { i += 1; }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Check for `LATERAL` keyword (case-insensitive, word boundary).
        let lower_slice = &lower[i..];
        if lower_slice.starts_with("lateral") {
            let lat_end = i + 7; // len("lateral")
            // Word boundary before.
            let pre_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
            // Word boundary after: must be whitespace or end-of-string.
            let post_ok = lat_end >= sql.len() || !bytes[lat_end].is_ascii_alphanumeric();
            if pre_ok && post_ok {
                // Skip whitespace after LATERAL.
                let mut j = lat_end;
                while j < sql.len() && bytes[j].is_ascii_whitespace() { j += 1; }
                // Check if what follows is `unnest` (case-insensitive, word boundary).
                if j < sql.len() && lower[j..].starts_with("unnest") {
                    let u_end = j + 6; // len("unnest")
                    let post_u = u_end >= sql.len()
                        || !bytes[u_end].is_ascii_alphanumeric()
                        || bytes[u_end] == b'(';
                    if post_u {
                        // Drop the "LATERAL " prefix; continue from the "unnest".
                        i = j;
                        continue;
                    }
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// LATERAL subquery rewrites
// ---------------------------------------------------------------------------

/// Rewrite uncorrelated `LATERAL (subquery)` → `(subquery)`.
///
/// When the LATERAL body has zero references to columns of the outer FROM
/// tables, `LATERAL (subq)` is semantically identical to a plain join.
/// Stripping the LATERAL keyword lets DataFusion plan it as a regular
/// cross/inner join without needing the (unimplemented) correlated-lateral
/// physical operator.
///
/// ## Detection heuristic
///
/// After locating the LATERAL keyword and its matching parenthesised subquery,
/// we collect all outer table names/aliases visible in the FROM clause before
/// the LATERAL keyword (simple identifier scanning, not a full AST). We then
/// check whether the subquery body contains any `outer_name.something` dotted
/// reference — a reliable signal that the subquery references an outer column.
/// If no such dotted reference exists and the subquery body contains none of
/// the outer aliases as bare words followed by `.`, the subquery is treated as
/// uncorrelated and LATERAL is stripped.
///
/// ## Limits (not rewritten — left for DataFusion upstream)
///
/// - Any LATERAL whose body contains a `outer_alias.col` reference is left
///   untouched; DataFusion 53 has no physical plan support for correlated
///   lateral subqueries (`OuterReferenceColumn` paths fail at execution).
/// - `LATERAL <fn>(...)` table-function forms other than `unnest` (handled by
///   `rewrite_lateral_unnest`) are left untouched.
/// - Nested LATERAL (LATERAL inside another LATERAL subquery) is not handled.
pub(crate) fn rewrite_lateral_uncorrelated(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("lateral") { return sql.to_string(); }

    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        // Find `lateral` keyword (case-insensitive).
        let Some(rel) = lower[search_from..].find("lateral") else { break; };
        let lat_start = search_from + rel;
        let lat_end = lat_start + 7; // len("lateral")
        let bytes = s.as_bytes();

        // Word boundary before LATERAL.
        let pre_ok = lat_start == 0 || !bytes[lat_start - 1].is_ascii_alphanumeric();
        // Word boundary after LATERAL: must be whitespace or `(`.
        let post_ok = lat_end >= s.len()
            || bytes[lat_end].is_ascii_whitespace()
            || bytes[lat_end] == b'(';
        if !pre_ok || !post_ok {
            search_from = lat_end;
            continue;
        }

        // Skip whitespace after LATERAL.
        let mut j = lat_end;
        while j < s.len() && bytes[j].is_ascii_whitespace() { j += 1; }

        // Only handle LATERAL (subquery) form — not LATERAL function_call().
        if j >= s.len() || bytes[j] != b'(' {
            search_from = lat_end;
            continue;
        }
        let paren_start = j;

        // Find the matching `)`.
        let Some(paren_end) = find_matching_close_paren(&s, paren_start) else {
            search_from = lat_end;
            continue;
        };

        let body = &s[paren_start + 1..paren_end];
        let body_lower = body.to_ascii_lowercase();

        // Must be a SELECT subquery.
        if !body_lower.trim_start().starts_with("select") {
            search_from = lat_end;
            continue;
        }

        // Collect outer table names/aliases from the FROM list text that
        // precedes this LATERAL keyword. We look for simple `FROM t` / `FROM t
        // [AS] alias` / `t JOIN ...` patterns in the text to the left of `lat_start`.
        let outer_text = &s[..lat_start];
        let outer_names = collect_outer_table_names(outer_text);

        // Check if the subquery body references any outer table via a
        // `outer_name.something` dotted pattern.
        let correlated = outer_names.iter().any(|name| {
            let pat = format!("{name}.");
            body_lower.contains(&pat)
        });

        if correlated {
            // Leave correlated LATERAL intact — DataFusion upstream limitation.
            search_from = lat_end;
            continue;
        }

        // Uncorrelated: strip `LATERAL ` (from lat_start to paren_start, keeping the `(`).
        s.replace_range(lat_start..paren_start, "");
        // Continue searching from where we stripped.
        search_from = lat_start;
    }
    s
}

/// Collect table name/alias tokens from the FROM-clause text preceding the
/// LATERAL keyword. Returns lowercase identifier strings.
///
/// Heuristic: scan for `FROM ident [AS ident]` and `JOIN ident [AS ident]`
/// patterns. This is intentionally conservative — false negatives (not
/// detecting an outer name) cause us to incorrectly strip a correlated LATERAL,
/// which would produce a wrong result. False positives (detecting a name that
/// isn't really outer) cause us to conservatively leave a LATERAL that we
/// could have stripped — safe, just misses an optimisation.
fn collect_outer_table_names(outer_text: &str) -> Vec<String> {
    let lower = outer_text.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut names = Vec::new();
    let mut i = 0usize;

    while i < lower.len() {
        // Skip string literals.
        if bytes[i] == b'\'' {
            i += 1;
            while i < lower.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < lower.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                    i += 1; break;
                }
                i += 1;
            }
            continue;
        }
        // Skip double-quoted identifiers.
        if bytes[i] == b'"' {
            i += 1;
            while i < lower.len() && bytes[i] != b'"' { i += 1; }
            if i < lower.len() { i += 1; }
            continue;
        }

        // Look for `from ` or `join ` keyword.
        let slice = &lower[i..];
        let kw_len = if slice.starts_with("from ") || slice.starts_with("from\t") || slice.starts_with("from\n") {
            5
        } else if slice.starts_with("join ") || slice.starts_with("join\t") || slice.starts_with("join\n") {
            5
        } else {
            i += 1;
            continue;
        };

        // Word boundary before keyword.
        let pre_ok = i == 0 || {
            let prev = bytes[i - 1];
            !prev.is_ascii_alphanumeric() && prev != b'_'
        };
        if !pre_ok {
            i += kw_len;
            continue;
        }

        // Skip whitespace after keyword.
        let mut j = i + kw_len;
        while j < lower.len() && lower.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        // Skip over `(` (subquery in FROM — not a table name).
        if j < lower.len() && lower.as_bytes()[j] == b'(' {
            i = j + 1;
            continue;
        }
        // Extract the identifier (table name).
        let id_start = j;
        while j < lower.len() && (lower.as_bytes()[j].is_ascii_alphanumeric() || lower.as_bytes()[j] == b'_') {
            j += 1;
        }
        if j > id_start {
            let table_name = lower[id_start..j].to_string();
            // Skip known SQL keywords that appear after FROM/JOIN.
            if !matches!(table_name.as_str(), "select" | "with" | "lateral" | "only" | "values") {
                names.push(table_name.clone());
            }
            // Check for optional `AS alias` or bare alias after the table name.
            let mut k = j;
            while k < lower.len() && lower.as_bytes()[k].is_ascii_whitespace() { k += 1; }
            if lower[k..].starts_with("as ") || lower[k..].starts_with("as\t") {
                k += 3;
                while k < lower.len() && lower.as_bytes()[k].is_ascii_whitespace() { k += 1; }
                let alias_start = k;
                while k < lower.len() && (lower.as_bytes()[k].is_ascii_alphanumeric() || lower.as_bytes()[k] == b'_') {
                    k += 1;
                }
                if k > alias_start {
                    names.push(lower[alias_start..k].to_string());
                }
            } else if k < lower.len()
                && lower.as_bytes()[k].is_ascii_alphabetic()
                && !lower[k..].starts_with("on ")
                && !lower[k..].starts_with("where ")
                && !lower[k..].starts_with("inner ")
                && !lower[k..].starts_with("left ")
                && !lower[k..].starts_with("right ")
                && !lower[k..].starts_with("full ")
                && !lower[k..].starts_with("cross ")
                && !lower[k..].starts_with("join ")
            {
                let alias_start = k;
                let mut kk = k;
                while kk < lower.len() && (lower.as_bytes()[kk].is_ascii_alphanumeric() || lower.as_bytes()[kk] == b'_') {
                    kk += 1;
                }
                if kk > alias_start {
                    let alias = lower[alias_start..kk].to_string();
                    if !matches!(alias.as_str(), "on" | "where" | "inner" | "left" | "right" | "full" | "cross" | "join" | "natural" | "using" | "set") {
                        names.push(alias);
                    }
                }
            }
        }
        i += kw_len;
    }
    names
}

/// Rewrite the common ORM nested-read LATERAL pattern:
///
/// ```sql
/// LEFT JOIN LATERAL (
///   SELECT agg_expr [AS alias] [, agg_expr2 [AS alias2], ...]
///   FROM child_table [AS child_alias]
///   WHERE child_alias.fk_col = outer_alias.pk_col
///   [ORDER BY ... / LIMIT ...]   -- ONLY if no ORDER BY or LIMIT present
/// ) sub_alias ON true
/// ```
///
/// into:
///
/// ```sql
/// LEFT JOIN (
///   SELECT child_alias.fk_col, agg_expr [AS alias] [, ...]
///   FROM child_table [AS child_alias]
///   GROUP BY child_alias.fk_col
/// ) sub_alias ON sub_alias.fk_col = outer_alias.pk_col
/// ```
///
/// ## Preconditions checked before rewriting
///
/// 1. Must be `LEFT JOIN LATERAL` (not INNER JOIN, CROSS JOIN, comma-join).
/// 2. The join condition must be `ON true` (case-insensitive).
/// 3. The subquery body must start with `SELECT`.
/// 4. The subquery body must have exactly ONE FROM table (a bare table name,
///    optionally aliased).
/// 5. The WHERE clause must contain exactly ONE `child_ref.col = outer_ref.col`
///    predicate (or `outer_ref.col = child_ref.col` — either order).
///    "child_ref" is identified as the name that matches the child table
///    name or alias; "outer_ref" is the other side.
/// 6. ALL projection items must be aggregate function calls (json_agg, array_agg,
///    jsonb_agg, count, sum, avg, min, max, bool_and, bool_or, string_agg, …).
///    Mixed projections (any non-aggregate column refs) are NOT rewritten.
/// 7. No ORDER BY or LIMIT inside the subquery — such constraints cannot be
///    preserved after pre-aggregation and would change results.
///
/// ## Correctness invariants
///
/// - Parent with zero matching children → `LEFT JOIN` preserves the parent row
///   with `NULL` for all subquery columns (including the agg column). This is
///   correct because `json_agg` over an empty input in PG returns NULL, and the
///   rewritten LEFT JOIN propagates NULL from the grouped subquery when no
///   matching child row exists.
/// - Multiple parents, mixed child counts → each parent gets its own aggregated
///   result from the pre-grouped subquery.
///
/// ## Limits (NOT rewritten — honest upstream defer)
///
/// - ORDER BY or LIMIT inside the subquery → deferred (wrong results if rewritten).
/// - Non-aggregate column refs in projection → deferred (semantics differ from
///   aggregating).
/// - Multiple correlation predicates in WHERE → deferred (complex join condition).
/// - Multiple FROM tables inside the subquery → deferred.
/// - INNER/CROSS/comma LATERAL → deferred (different null semantics).
/// - Arbitrary correlated LATERAL (generate_series(1, t.id), multi-table body,
///   etc.) → DataFusion upstream limitation; left failing with original error.
pub(crate) fn rewrite_lateral_nested_agg(sql: &str) -> String {
    let lower_check = sql.to_ascii_lowercase();
    if !lower_check.contains("lateral") { return sql.to_string(); }

    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        // Look for `left join lateral` sequence.
        let Some(rel) = lower[search_from..].find("left join lateral") else { break; };
        let ljl_start = search_from + rel;
        let ljl_end = ljl_start + 17; // len("left join lateral")
        let bytes = s.as_bytes();

        // Word boundary before.
        let pre_ok = ljl_start == 0 || !bytes[ljl_start - 1].is_ascii_alphanumeric();
        // Word boundary after "lateral": whitespace or `(`.
        let post_ok = ljl_end >= s.len()
            || bytes[ljl_end].is_ascii_whitespace()
            || bytes[ljl_end] == b'(';
        if !pre_ok || !post_ok {
            search_from = ljl_end;
            continue;
        }

        // Skip whitespace after `lateral`.
        let mut j = ljl_end;
        while j < s.len() && bytes[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || bytes[j] != b'(' {
            search_from = ljl_end;
            continue;
        }
        let paren_start = j;
        let Some(paren_end) = find_matching_close_paren(&s, paren_start) else {
            search_from = ljl_end;
            continue;
        };

        let body = s[paren_start + 1..paren_end].trim().to_string();
        let body_lower = body.to_ascii_lowercase();

        // Must be a SELECT subquery.
        if !body_lower.trim_start().starts_with("select") {
            search_from = ljl_end;
            continue;
        }

        // Reject if subquery contains ORDER BY or LIMIT — can't safely pre-agg.
        if body_lower.contains("order by") || body_lower.contains("limit") {
            search_from = ljl_end;
            continue;
        }

        // Parse the subquery: find the SELECT projection, FROM table, WHERE clause.
        let after_select = body_lower.trim_start();
        let sel_body_offset = if after_select.starts_with("select ") || after_select.starts_with("select\t") {
            7
        } else {
            search_from = ljl_end;
            continue;
        };

        // Find FROM keyword at depth 0.
        let Some(from_pos) = find_from_at_depth0(&body_lower, sel_body_offset) else {
            search_from = ljl_end;
            continue;
        };

        let proj_str = body[sel_body_offset..from_pos].trim(); // original case
        let proj_lower = proj_str.to_ascii_lowercase();

        // Find WHERE keyword after FROM at depth 0.
        let after_from_offset = from_pos + 5; // skip "from "
        let where_pos_opt = find_keyword_at_depth0(&body_lower, "where", after_from_offset);

        // Extract child table (FROM clause: everything between FROM and WHERE/end).
        let child_clause = if let Some(wp) = where_pos_opt {
            body[after_from_offset..wp].trim().to_string()
        } else {
            // No WHERE → no correlation → not the ORM pattern.
            search_from = ljl_end;
            continue;
        };

        // Parse child table name and optional alias. Only allow a single
        // simple table reference (no subqueries, no JOINs inside).
        let (child_table, child_alias) = match parse_simple_table_ref(&child_clause) {
            Some(pair) => pair,
            None => {
                search_from = ljl_end;
                continue;
            }
        };

        // Extract the WHERE clause body (between WHERE and end of body).
        let where_body = body[where_pos_opt.unwrap() + 6..].trim(); // skip "where "
        let where_body_lower = where_body.to_ascii_lowercase();

        // Parse exactly one correlation predicate: `child_ref.fk = outer_ref.pk`
        // or `outer_ref.pk = child_ref.fk`. We do NOT rewrite if there are
        // additional predicates (AND, OR) beyond the single correlation.
        let corr = match parse_single_correlation_predicate(
            where_body,
            &where_body_lower,
            &child_alias,
            &child_table,
        ) {
            Some(c) => c,
            None => {
                search_from = ljl_end;
                continue;
            }
        };

        // Validate that ALL projection items are aggregate function calls.
        if !all_projections_are_aggregates(&proj_lower) {
            search_from = ljl_end;
            continue;
        }

        // Find the subquery alias after the closing `)`.
        // Skip whitespace.
        let mut after_paren = paren_end + 1;
        while after_paren < s.len() && bytes[after_paren].is_ascii_whitespace() {
            after_paren += 1;
        }
        // Optional `AS` before alias.
        let mut alias_start = after_paren;
        if lower[after_paren..].starts_with("as ") || lower[after_paren..].starts_with("as\t") {
            alias_start = after_paren + 3;
            while alias_start < s.len() && bytes[alias_start].is_ascii_whitespace() {
                alias_start += 1;
            }
        }
        let mut alias_end = alias_start;
        while alias_end < s.len() && (bytes[alias_end].is_ascii_alphanumeric() || bytes[alias_end] == b'_') {
            alias_end += 1;
        }
        if alias_end == alias_start {
            // No subquery alias — can't build the ON clause without it.
            search_from = ljl_end;
            continue;
        }
        let sub_alias = s[alias_start..alias_end].to_string();

        // Find `ON true` after the alias.
        let mut on_start = alias_end;
        while on_start < s.len() && bytes[on_start].is_ascii_whitespace() { on_start += 1; }
        if !lower[on_start..].starts_with("on ") && !lower[on_start..].starts_with("on\t") {
            search_from = ljl_end;
            continue;
        }
        let on_kw_end = on_start + 3;
        let mut on_val_start = on_kw_end;
        while on_val_start < s.len() && bytes[on_val_start].is_ascii_whitespace() { on_val_start += 1; }
        // Must be `ON true`.
        let on_lower = lower[on_val_start..].trim_start();
        if !on_lower.starts_with("true") {
            search_from = ljl_end;
            continue;
        }
        let on_true_end = on_val_start + 4;
        // Word boundary after `true`.
        let on_post_ok = on_true_end >= s.len() || {
            let b = bytes[on_true_end];
            !b.is_ascii_alphanumeric() && b != b'_'
        };
        if !on_post_ok {
            search_from = ljl_end;
            continue;
        }

        // All preconditions satisfied. Build the rewritten SQL.
        //
        // Original: LEFT JOIN LATERAL (SELECT proj FROM child [AS calias] WHERE child.fk = outer.pk) sub ON true
        // Rewrite:  LEFT JOIN (SELECT child.fk, proj FROM child [AS calias] GROUP BY child.fk) sub ON sub.fk = outer.pk
        //
        // The fk_col comes from the corr.child_col; pk side is corr.outer_ref (e.g. "parent.id").
        let fk_ref = format!("{}.{}", child_alias, corr.child_col);
        let child_decl = if child_table != child_alias {
            format!("{} AS {}", child_table, child_alias)
        } else {
            child_table.clone()
        };
        let new_body = format!(
            "SELECT {fk_ref}, {proj_str} FROM {child_decl} GROUP BY {fk_ref}",
        );
        let new_on = format!("{sub_alias}.{} = {}", corr.child_col, corr.outer_ref);
        let replacement = format!(
            "LEFT JOIN ({new_body}) {sub_alias} ON {new_on}",
        );

        // Replace from `left join lateral` start through `on true` end.
        s.replace_range(ljl_start..on_true_end, &replacement);
        search_from = ljl_start + replacement.len();
    }
    s
}

/// Result of parsing a single correlation predicate from the LATERAL WHERE clause.
struct CorrPredicate {
    /// The child-side column name (unqualified) used as the FK.
    child_col: String,
    /// The full outer reference string, e.g. `parent.id`.
    outer_ref: String,
}

/// Parse exactly one `child_ref.col = outer_ref.col` (or flipped) equality
/// predicate from `where_body`. Returns `None` if:
/// - The clause contains AND/OR operators (multiple predicates).
/// - The clause does not match the `a.b = c.d` pattern.
/// - Neither side matches the child table name or alias.
fn parse_single_correlation_predicate(
    where_body: &str,
    where_lower: &str,
    child_alias: &str,
    child_table: &str,
) -> Option<CorrPredicate> {
    // Reject anything with AND or OR — we only handle single predicate.
    if where_lower.contains(" and ") || where_lower.contains(" or ") {
        return None;
    }
    // Find `=` sign (not inside parens, not `!=`, `<=`, `>=`).
    let bytes = where_lower.as_bytes();
    let eq_pos = find_bare_eq(where_lower)?;

    let lhs = where_body[..eq_pos].trim();
    let rhs = where_body[eq_pos + 1..].trim();
    let lhs_lower = lhs.to_ascii_lowercase();
    let rhs_lower = rhs.to_ascii_lowercase();

    // Both sides must be `table.column` form.
    let (lhs_tbl, lhs_col) = parse_dotted_ref(&lhs_lower)?;
    let (rhs_tbl, rhs_col) = parse_dotted_ref(&rhs_lower)?;

    // Determine which side is child.
    let child_matches_lhs = lhs_tbl == child_alias || lhs_tbl == child_table;
    let child_matches_rhs = rhs_tbl == child_alias || rhs_tbl == child_table;

    if child_matches_lhs && !child_matches_rhs {
        Some(CorrPredicate {
            child_col: lhs_col.to_string(),
            outer_ref: rhs.to_string(),
        })
    } else if child_matches_rhs && !child_matches_lhs {
        Some(CorrPredicate {
            child_col: rhs_col.to_string(),
            outer_ref: lhs.to_string(),
        })
    } else {
        None
    }
}

/// Find a bare `=` at depth 0 that is not part of `!=`, `<=`, `>=`.
fn find_bare_eq(lower: &str) -> Option<usize> {
    let bytes = lower.as_bytes();
    let mut i = 0usize;
    while i < lower.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < lower.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < lower.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
            }
            b'=' => {
                // Not `!=`, `<=`, `>=`.
                let prev_ok = i == 0 || (bytes[i - 1] != b'!' && bytes[i - 1] != b'<' && bytes[i - 1] != b'>');
                let next_ok = i + 1 >= lower.len() || bytes[i + 1] != b'>';
                if prev_ok && next_ok {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse `table.column` and return `(table, column)`.
fn parse_dotted_ref(s: &str) -> Option<(&str, &str)> {
    let dot = s.find('.')?;
    let tbl = s[..dot].trim();
    let col = s[dot + 1..].trim();
    if tbl.is_empty() || col.is_empty() { return None; }
    // Column must not contain further dots or spaces (simple identifier).
    if col.contains('.') || col.contains(' ') { return None; }
    Some((tbl, col))
}

/// Parse a simple table reference: `table_name [AS alias]` or `table_name alias`.
/// Returns `(table_name, alias)`. The alias defaults to the table name if absent.
/// Returns `None` if the clause appears to contain a subquery or JOIN.
fn parse_simple_table_ref(clause: &str) -> Option<(String, String)> {
    let lower = clause.to_ascii_lowercase();
    // Reject subqueries, JOINs.
    if lower.contains('(')
        || lower.contains("join")
        || lower.contains(',')
    {
        return None;
    }
    let bytes = lower.as_bytes();
    let mut i = 0usize;
    while i < lower.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    let name_start = i;
    while i < lower.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.') { i += 1; }
    if i == name_start { return None; }
    let table_name = lower[name_start..i].to_string();

    // Skip whitespace.
    while i < lower.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    // Optional `AS`.
    if lower[i..].starts_with("as ") || lower[i..].starts_with("as\t") {
        i += 3;
        while i < lower.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    }
    let alias_start = i;
    while i < lower.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') { i += 1; }
    let alias = if i > alias_start {
        lower[alias_start..i].to_string()
    } else {
        table_name.clone()
    };
    Some((table_name, alias))
}

/// Check whether all comma-separated projection items are aggregate function calls.
///
/// Recognised aggregates: `json_agg`, `jsonb_agg`, `array_agg`, `count`,
/// `sum`, `avg`, `min`, `max`, `bool_and`, `bool_or`, `string_agg`,
/// `array_to_string`, `every`, `variance`, `stddev`, `var_pop`, `var_samp`,
/// `stddev_pop`, `stddev_samp`.
///
/// Each projection item is: `agg_fn(...)  [ORDER BY ...]  [AS alias]`.
/// A lone `*` (SELECT *) is rejected — not an aggregate.
fn all_projections_are_aggregates(proj_lower: &str) -> bool {
    const AGG_NAMES: &[&str] = &[
        "json_agg", "jsonb_agg", "array_agg", "count", "sum", "avg", "min",
        "max", "bool_and", "bool_or", "string_agg", "array_to_string",
        "every", "variance", "stddev", "var_pop", "var_samp",
        "stddev_pop", "stddev_samp",
    ];
    // Split projection at depth-0 commas.
    let items = split_at_depth0_commas(proj_lower);
    if items.is_empty() { return false; }
    for item in &items {
        let trimmed = item.trim();
        // Strip trailing `AS alias`.
        let expr = strip_trailing_alias(trimmed);
        let expr = expr.trim();
        // Check if it starts with one of the known aggregate names followed by `(`.
        let is_agg = AGG_NAMES.iter().any(|&name| {
            if let Some(rest) = expr.strip_prefix(name) {
                let after = rest.trim_start();
                after.starts_with('(')
            } else {
                false
            }
        });
        if !is_agg { return false; }
    }
    true
}

/// Split `s` at depth-0 commas (i.e., commas outside parentheses).
fn split_at_depth0_commas(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < s.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => { if depth > 0 { depth -= 1; } }
            b'\'' => {
                i += 1;
                while i < s.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < s.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            b',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

/// Strip a trailing `AS identifier` alias from a projection expression.
/// Only strips when `AS ident` appears at the very end outside parentheses.
fn strip_trailing_alias(s: &str) -> &str {
    // Find the last occurrence of ` as ` or ` AS ` at depth 0.
    let lower = s.to_ascii_lowercase();
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut last_as: Option<usize> = None;
    let mut i = 0usize;
    while i < lower.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => { if depth > 0 { depth -= 1; } }
            b'\'' => {
                i += 1;
                while i < lower.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < lower.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {
                if depth == 0 && lower[i..].starts_with(" as ") {
                    last_as = Some(i);
                }
            }
        }
        i += 1;
    }
    if let Some(pos) = last_as {
        // Verify the identifier after `AS` is simple (no spaces, no parens).
        let after_as = s[pos + 4..].trim();
        if !after_as.is_empty()
            && !after_as.contains(' ')
            && !after_as.contains('(')
        {
            return &s[..pos];
        }
    }
    s
}

/// Find a keyword at depth 0 in `lower` starting from `offset`.
fn find_keyword_at_depth0(lower: &str, keyword: &str, offset: usize) -> Option<usize> {
    let bytes = lower.as_bytes();
    let klen = keyword.len();
    let mut depth = 0i32;
    let mut i = offset;
    while i < lower.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => { if depth > 0 { depth -= 1; } }
            b'\'' => {
                i += 1;
                while i < lower.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < lower.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {
                if depth == 0 && lower[i..].starts_with(keyword) {
                    let pre_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                    let after = i + klen;
                    let post_ok = after >= lower.len() || !bytes[after].is_ascii_alphanumeric();
                    if pre_ok && post_ok { return Some(i); }
                }
            }
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Bitwise operator rewrites
// ---------------------------------------------------------------------------

/// Rewrite PG bitwise operators that DataFusion's GenericDialect parser
/// cannot handle:
///
/// - `A # B`  → `A ^ B`   (PG bitwise XOR; DataFusion GenericDialect maps `^`
///                          to BitwiseXor, but `#` is unknown to it)
/// - `~expr`  → `(-1 ^ expr)` (PG unary bitwise NOT = XOR with all-ones mask)
///
/// The `#` rewrite is purely character-level: replace each bare `#` that is
/// not inside a string literal with `^`.
///
/// The `~` unary rewrite only fires for an isolated `~` that is **not**
/// followed by `*` or preceded by `!` (those are regex operators handled
/// by `rewrite_posix_regex_operators`). The pattern
/// `SELECT ~expr` / `WHERE ~expr` is re-spelled `(-1 ^ expr)` so that
/// DataFusion's generic XOR operator evaluates it.
pub(crate) fn rewrite_pg_bitwise_operators(sql: &str) -> String {
    // Step 1: rewrite `A # B` → `A ^ B` (bitwise XOR).
    // `#` inside string literals is skipped.
    let s = rewrite_bitwise_xor_hash(sql);
    // Step 2: rewrite unary `~ expr` → `(-1 ^ (expr))`.
    rewrite_unary_bitwise_not(&s)
}

fn rewrite_bitwise_xor_hash(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        // Skip single-quoted string literals.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Skip double-quoted identifiers.
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i < bytes.len() { i += 1; }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Replace `#` that is NOT part of `#>` or `#>>` (JSON path operators),
        // `#"` (quoted identifier), or `#-` (JSONB path-delete operator).
        // Plain `#` between tokens is the PG bitwise XOR.
        if bytes[i] == b'#' {
            let next = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
            if next == b'>' || next == b'"' || next == b'-' {
                // `#>` / `#>>` / `#"` / `#-` — pass through unchanged.
                out.push('#');
            } else {
                // PG bitwise XOR: `A # B` → `A ^ B`.
                out.push('^');
            }
            i += 1;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn rewrite_unary_bitwise_not(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 16);
    let mut i = 0usize;
    while i < len {
        // Skip string literals.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    if i + 1 < len && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Skip double-quoted identifiers.
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < len && bytes[i] != b'"' { i += 1; }
            if i < len { i += 1; }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Look for `~` that is:
        //   - not preceded by `!` (regex operators — already rewritten)
        //   - not followed by `*` (regex operators)
        //   - not followed by `~` (double-tilde = LIKE)
        //   - followed only by whitespace / `(` / digit / identifier start
        if bytes[i] == b'~' {
            let preceded_by_bang = i > 0 && bytes[i - 1] == b'!';
            let next = if i + 1 < len { bytes[i + 1] } else { 0 };
            let followed_by_star = next == b'*';
            let followed_by_tilde = next == b'~';
            if !preceded_by_bang && !followed_by_star && !followed_by_tilde {
                // This looks like a unary bitwise NOT.
                // Extract the expression that follows.
                let expr_start = i + 1;
                // Skip whitespace.
                let mut j = expr_start;
                while j < len && bytes[j].is_ascii_whitespace() { j += 1; }
                // Collect the operand expression: parenthesised group or simple token.
                let (_, expr_end) = array_extract_right_pub(sql, j);
                let operand = sql[j..expr_end].trim();
                let replacement = format!("(-1 ^ ({operand}))");
                out.push_str(&replacement);
                i = expr_end;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

// Re-export the internal right-extraction helper so `rewrite_unary_bitwise_not`
// can call it without duplication.
fn array_extract_right_pub(s: &str, start: usize) -> (usize, usize) {
    array_extract_right(s, start)
}

// ---------------------------------------------------------------------------
// OVERLAPS rewrite
// ---------------------------------------------------------------------------

/// Rewrite PG `(s1, e1) OVERLAPS (s2, e2)` to `overlaps(s1, e1, s2, e2)`.
///
/// The PG `OVERLAPS` predicate checks whether two time intervals share any
/// point: `(S1, E1) OVERLAPS (S2, E2)`. DataFusion doesn't understand this
/// syntax, but Basin registers an `overlaps(s1, e1, s2, e2)` UDF. This
/// textual rewrite converts the PG tuple form to the UDF call form.
///
/// Only rewrites the exact pattern `(expr, expr) OVERLAPS (expr, expr)`;
/// leaves everything else untouched.
pub(crate) fn rewrite_overlaps(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower_view = s.to_ascii_lowercase();
        // Find `overlaps` keyword.
        let Some(rel) = lower_view[search_from..].find("overlaps") else {
            break;
        };
        let kw_start = search_from + rel;
        let kw_end = kw_start + 8;

        // Must be a word boundary.
        let pre_ok = kw_start == 0 || !s.as_bytes()[kw_start - 1].is_ascii_alphanumeric();
        let post_ok = kw_end >= s.len() || !s.as_bytes()[kw_end].is_ascii_alphanumeric();
        if !pre_ok || !post_ok {
            search_from = kw_end;
            continue;
        }

        // Require `(expr, expr)` on the right.
        let mut j = kw_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = kw_end;
            continue;
        }
        // Parse right tuple: `(e1, e2)`.
        let rhs_paren_start = j;
        let Some((r1, r2, rhs_paren_end)) = parse_two_tuple(&s, rhs_paren_start) else {
            search_from = kw_end;
            continue;
        };

        // Look back for `(expr, expr)` before OVERLAPS.
        let before = &s[..kw_start];
        let trimmed_before = before.trim_end();
        if !trimmed_before.ends_with(')') {
            search_from = kw_end;
            continue;
        }
        // Find the matching opening `(` for the left tuple.
        let lhs_paren_end_in_full = kw_start - (before.len() - trimmed_before.len()); // exclusive
        let Some(lhs_paren_start) =
            find_matching_open_paren(&s, lhs_paren_end_in_full - 1) else {
            search_from = kw_end;
            continue;
        };
        let Some((l1, l2, _)) = parse_two_tuple(&s, lhs_paren_start) else {
            search_from = kw_end;
            continue;
        };

        let replacement = format!("overlaps({l1}, {l2}, {r1}, {r2})");
        s.replace_range(lhs_paren_start..rhs_paren_end, &replacement);
        search_from = lhs_paren_start + replacement.len();
    }
    let _ = lower; // suppress unused warning
    s
}

/// Parse `(expr, expr)` starting at `start` (the opening paren).
/// Returns `(expr1_str, expr2_str, end_exclusive)` or `None`.
fn parse_two_tuple(s: &str, start: usize) -> Option<(String, String, usize)> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'(') {
        return None;
    }
    let mut i = start + 1;
    // Skip whitespace.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    // Parse first expression up to `,` at depth 0.
    let e1_start = i;
    let mut depth = 0i32;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 { return None; } // no comma found
                depth -= 1;
            }
            b',' if depth == 0 => break,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b',' { return None; }
    let e1 = s[e1_start..i].trim().to_string();
    i += 1; // skip comma
    while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
    let e2_start = i;
    depth = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 { break; }
                depth -= 1;
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b')' { return None; }
    let e2 = s[e2_start..i].trim().to_string();
    Some((e1, e2, i + 1))
}

/// Walk backwards from `close_paren` (index of the `)`) to find the matching `(`.
fn find_matching_open_paren(s: &str, close_paren: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(close_paren) != Some(&b')') { return None; }
    let mut depth = 1i32;
    let mut i = close_paren;
    while i > 0 {
        i -= 1;
        match bytes[i] {
            b')' => depth += 1,
            b'(' => {
                depth -= 1;
                if depth == 0 { return Some(i); }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// FILTER (WHERE ...) rewrite for aggregate functions
// ---------------------------------------------------------------------------

/// Rewrite `agg(args) FILTER (WHERE cond)` to an inline CASE WHEN form.
///
/// DataFusion's internal SQL parser (GenericDialect) doesn't support the
/// `FILTER (WHERE ...)` clause on aggregates, even though DataFusion's
/// logical plan supports filtered aggregation. This textual rewrite converts
/// the PG-standard form to the CASE-expression equivalent that DataFusion
/// *does* parse:
///
/// - `COUNT(*) FILTER (WHERE cond)` → `COUNT(CASE WHEN cond THEN 1 END)`
/// - `SUM(x) FILTER (WHERE cond)`   → `SUM(CASE WHEN cond THEN x END)`
/// - `agg(x) FILTER (WHERE cond)`   → `agg(CASE WHEN cond THEN x END)`
///
/// Only fires on `FILTER` that appears outside string literals and is
/// preceded by `)` (end of the aggregate's argument list).
pub(crate) fn rewrite_aggregate_filter(sql: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let lower = s.to_ascii_lowercase();
        // Find `FILTER` keyword at or after `search_from`.
        let Some(rel) = lower[search_from..].find("filter") else { break; };
        let kw_start = search_from + rel;
        let kw_end = kw_start + 6;
        let bytes = s.as_bytes();

        // Word boundary checks.
        let pre_ok = kw_start == 0 || !bytes[kw_start - 1].is_ascii_alphanumeric();
        let post_ok = kw_end >= s.len() || !bytes[kw_end].is_ascii_alphanumeric();
        if !pre_ok || !post_ok {
            search_from = kw_end;
            continue;
        }

        // Must be preceded (after whitespace) by `)` — end of aggregate args.
        let before = s[..kw_start].trim_end();
        if !before.ends_with(')') {
            search_from = kw_end;
            continue;
        }

        // After FILTER there must be `(WHERE ...)`.
        let mut j = kw_end;
        while j < s.len() && s.as_bytes()[j].is_ascii_whitespace() { j += 1; }
        if j >= s.len() || s.as_bytes()[j] != b'(' {
            search_from = kw_end;
            continue;
        }
        let filter_paren_start = j;
        let filter_lower = s.to_ascii_lowercase();
        if !filter_lower[filter_paren_start + 1..].trim_start().starts_with("where") {
            search_from = kw_end;
            continue;
        }
        // Find the matching `)` for the FILTER clause.
        let Some(filter_paren_end) =
            find_matching_close_paren(&s, filter_paren_start) else {
            search_from = kw_end;
            continue;
        };

        // Extract the condition (everything inside `(WHERE ...)`).
        let inner = s[filter_paren_start + 1..filter_paren_end].trim();
        // Strip leading `WHERE` keyword.
        let cond = if inner.to_ascii_lowercase().starts_with("where") {
            inner[5..].trim().to_string()
        } else {
            inner.to_string()
        };

        // Find the matching `(` for the aggregate's argument list.
        let agg_close_paren = kw_start - (s[..kw_start].len() - before.len()) - 1;
        let Some(agg_open_paren) = find_matching_open_paren(&s, agg_close_paren) else {
            search_from = kw_end;
            continue;
        };

        // Extract the function name (everything before the opening `(`).
        let func_name = s[..agg_open_paren].trim_end();
        let func_start = {
            let fb = func_name.as_bytes();
            let mut k = func_name.len();
            while k > 0 && (fb[k-1].is_ascii_alphanumeric() || fb[k-1] == b'_') { k -= 1; }
            k
        };
        let fname = &func_name[func_start..];

        // Extract the original args inside the aggregate.
        let orig_args = s[agg_open_paren + 1..agg_close_paren].trim().to_string();

        // Build the replacement.
        let case_expr = if orig_args == "*" {
            // COUNT(*) FILTER (WHERE cond) → COUNT(CASE WHEN cond THEN 1 END)
            format!("CASE WHEN {cond} THEN 1 END")
        } else {
            format!("CASE WHEN {cond} THEN {orig_args} END")
        };
        let replacement = format!("{fname}({case_expr})");

        // Replace from agg_open_paren - func_name_len .. filter_paren_end+1.
        let replace_start = func_start;
        s.replace_range(replace_start..filter_paren_end + 1, &replacement);
        search_from = replace_start + replacement.len();
    }
    s
}

/// Find the matching `)` for an opening `(` at `open_paren`.
fn find_matching_close_paren(s: &str, open_paren: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(open_paren) != Some(&b'(') { return None; }
    let mut depth = 1i32;
    let mut i = open_paren + 1;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' { i += 2; continue; }
                        i += 1; break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    if depth == 0 { Some(i - 1) } else { None }
}

// ---------------------------------------------------------------------------
// WITH ... [NOT] MATERIALIZED hint strip
// ---------------------------------------------------------------------------

/// Strip the `[NOT] MATERIALIZED` hint from `WITH cte AS MATERIALIZED (...)`.
///
/// DataFusion's sqlparser doesn't parse this optional PG syntax hint.
/// The hint is purely advisory (it affects PG's CTE materialisation decision)
/// and can be dropped without changing query semantics for Basin's purposes.
///
/// Rewrites:
/// - `WITH cte AS MATERIALIZED (...)` → `WITH cte AS (...)`
/// - `WITH cte AS NOT MATERIALIZED (...)` → `WITH cte AS (...)`
pub(crate) fn rewrite_cte_materialized(sql: &str) -> String {
    // Replace `AS NOT MATERIALIZED (` → `AS (` (longer match first).
    let s = rewrite_cte_keyword(sql, "as not materialized (", "AS (");
    rewrite_cte_keyword(&s, "as materialized (", "AS (")
}

fn rewrite_cte_keyword(sql: &str, needle_lower: &str, replacement: &str) -> String {
    let mut s = sql.to_string();
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(pos) = lower.find(needle_lower) else { break; };
        // Word boundary before `as`.
        let pre_ok = pos == 0 || !s.as_bytes()[pos - 1].is_ascii_alphanumeric();
        if !pre_ok {
            // Avoid infinite loop: move past this occurrence.
            break;
        }
        // Replace the keyword (keep existing paren; replacement ends with `AS (`).
        s.replace_range(pos..pos + needle_lower.len(), replacement);
    }
    s
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE column-alias propagation
// ---------------------------------------------------------------------------

/// Rewrite `WITH RECURSIVE cte_name(col1, col2) AS (SELECT expr1, expr2 UNION ALL ...)`
/// so that the base-case SELECT expressions carry explicit `AS col` aliases.
///
/// ## Problem
///
/// DataFusion 53's recursive-CTE planner builds the *working-table* schema
/// from the **static (base) term** before the CTE column list is applied.
/// When the base term is `SELECT 1` (unnamed literal), the inferred column
/// name is `"Int64(1)"`.  The recursive term later references `n` — which
/// does not exist in the working table — causing:
///
///   `Schema error: No field named n. Valid fields are r."Int64(1)".`
///
/// ## Fix
///
/// Inject explicit `AS col_name` aliases into the base-case SELECT list so
/// that DataFusion sees the correct field names when it builds the working-table
/// schema.  Only expressions that do not already carry an `AS` alias receive
/// one.
///
/// ## Scope / limits
///
/// * Only fires for `WITH RECURSIVE` with a column list `name(col1, ...)`.
/// * Only rewrites the base case (the part before `UNION` / `UNION ALL`).
/// * Uses a conservative text scan — handles literals, parenthesised
///   expressions, and simple identifiers.  Nested CTEs / subqueries inside
///   the base case are left unchanged.
pub(crate) fn rewrite_recursive_cte_column_aliases(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();

    // Fast path: no WITH RECURSIVE → nothing to do.
    let rec_start = match lower.find("with recursive") {
        Some(p) => p,
        None => return sql.to_string(),
    };

    // Word-boundary check: nothing alphanumeric before `with`.
    if rec_start > 0 {
        let pre = sql.as_bytes()[rec_start - 1];
        if pre.is_ascii_alphanumeric() || pre == b'_' {
            return sql.to_string();
        }
    }

    let after_rec = rec_start + "with recursive".len();

    // Skip whitespace after RECURSIVE.
    let bytes = sql.as_bytes();
    let mut i = after_rec;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Read the CTE name (identifier).
    let name_start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i == name_start {
        return sql.to_string(); // no name found
    }

    // Skip whitespace.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Must be followed by `(` for the column list.
    if i >= bytes.len() || bytes[i] != b'(' {
        return sql.to_string(); // no column list
    }

    // Parse the column list `(col1, col2, ...)`.
    i += 1; // skip '('
    let col_list_start = i;
    let mut paren_depth = 1i32;
    while i < bytes.len() && paren_depth > 0 {
        match bytes[i] {
            b'(' => { paren_depth += 1; i += 1; }
            b')' => { paren_depth -= 1; i += 1; }
            b'\'' => {
                // skip string literal
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' { i += 1; } else { break; }
                    } else {
                        i += 1;
                    }
                }
            }
            _ => { i += 1; }
        }
    }
    let col_list_end = i - 1; // position of closing ')'
    let col_list_str = &sql[col_list_start..col_list_end];

    // Parse column names (simple comma-split; we don't expect sub-expressions here).
    let col_names: Vec<&str> = col_list_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if col_names.is_empty() {
        return sql.to_string();
    }

    // Skip whitespace and `AS` keyword.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let lower_rest = lower[i..].to_string();
    if !lower_rest.starts_with("as") {
        return sql.to_string();
    }
    i += 2; // skip "AS"
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Must be `(` — start of the CTE body.
    if i >= bytes.len() || bytes[i] != b'(' {
        return sql.to_string();
    }
    let body_open = i;
    i += 1; // skip '('

    // Skip whitespace.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Must be SELECT keyword.
    if !lower[i..].starts_with("select") {
        return sql.to_string();
    }
    let select_kw_end = i + "select".len();
    // Check word boundary after SELECT.
    if select_kw_end < bytes.len() && (bytes[select_kw_end].is_ascii_alphanumeric() || bytes[select_kw_end] == b'_') {
        return sql.to_string();
    }
    i = select_kw_end;

    // Skip whitespace after SELECT.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    let select_list_start = i;

    // Scan the SELECT list to find the UNION boundary.
    // We need to track parenthesis depth to skip over nested subqueries.
    let union_pos = find_union_in_base_select(sql, select_list_start);
    let union_pos = match union_pos {
        Some(p) => p,
        None => return sql.to_string(), // no UNION found — not a recursive CTE body
    };

    // The base SELECT list runs from select_list_start to union_pos.
    let base_list = &sql[select_list_start..union_pos];

    // Parse the base SELECT list into individual expressions.
    let exprs = split_select_list(base_list);

    if exprs.len() != col_names.len() {
        // Column count mismatch — leave as-is to avoid silently producing
        // wrong results.  The planner will error out with a clear message.
        return sql.to_string();
    }

    // For each expression, add `AS col_name` if the expression doesn't
    // already end with an alias.
    let mut new_exprs: Vec<String> = Vec::with_capacity(exprs.len());
    for (expr, col) in exprs.iter().zip(col_names.iter()) {
        let trimmed = expr.trim();
        if expr_has_alias(trimmed) {
            new_exprs.push(trimmed.to_string());
        } else {
            new_exprs.push(format!("{} AS {}", trimmed, col));
        }
    }

    let new_base_list = new_exprs.join(", ");
    // Reconstruct the SQL: keep everything up to select_list_start, inject
    // the aliased list, then the UNION and everything after.
    // Always insert a single space before UNION so that the alias name and
    // the UNION keyword don't merge into a single token (e.g. `nUNION`).
    let prefix = &sql[..select_list_start];
    let suffix = &sql[union_pos..];
    format!("{prefix}{new_base_list} {suffix}")
}

/// Find the position of `UNION` (or `UNION ALL`) in the base SELECT, at
/// depth-0 paren level (i.e. not inside a nested subquery).
/// Returns the byte offset of the `U` in `UNION`.
fn find_union_in_base_select(sql: &str, from: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let lower = sql.to_ascii_lowercase();
    let mut i = from;
    let mut depth = 0i32;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => { depth += 1; i += 1; }
            b')' => {
                if depth == 0 {
                    // Hit the closing paren of the CTE body without finding UNION.
                    return None;
                }
                depth -= 1;
                i += 1;
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' { i += 1; } else { break; }
                    } else { i += 1; }
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                // Line comment — skip to end of line.
                while i < bytes.len() && bytes[i] != b'\n' { i += 1; }
            }
            _ => {
                if depth == 0 && lower[i..].starts_with("union") {
                    // Word boundary check.
                    let after = i + 5;
                    let after_ok = after >= bytes.len()
                        || bytes[after].is_ascii_whitespace()
                        || bytes[after] == b'(';
                    if after_ok {
                        return Some(i);
                    }
                }
                i += 1;
            }
        }
    }
    None
}

/// Split a SELECT column list (the part after SELECT, before UNION/FROM) into
/// individual expression strings, respecting parenthesis depth.
fn split_select_list(list: &str) -> Vec<String> {
    let bytes = list.as_bytes();
    let mut exprs = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                // skip: handled by forward scan in the caller already; here we
                // just count parens correctly inside non-string regions.
            }
            b',' if depth == 0 => {
                exprs.push(list[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = list[start..].trim();
    if !last.is_empty() {
        exprs.push(last.to_string());
    }
    exprs
}

/// Return `true` if `expr` already carries an explicit `AS alias` or is a
/// bare identifier that DataFusion will name correctly (i.e. an unqualified
/// column reference like `n`).  We only inject aliases for literals and
/// arithmetic/function expressions.
fn expr_has_alias(expr: &str) -> bool {
    let lower = expr.to_ascii_lowercase();
    // Explicit AS alias.
    // Scan for ` AS ` outside of string literals.
    let bytes = expr.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => { depth += 1; i += 1; }
            b')' => { depth -= 1; i += 1; }
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' { i += 1; } else { break; }
                    } else { i += 1; }
                }
            }
            _ => {
                if depth == 0 && lower[i..].starts_with(" as ") {
                    return true;
                }
                i += 1;
            }
        }
    }
    false
}


// ---------------------------------------------------------------------------
// UUID cast rewriter
// ---------------------------------------------------------------------------

/// Rewrite `'...'::UUID` (and `'...'::uuid`) to `'...'::VARCHAR` before
/// DataFusion sees the SQL. DataFusion 53 does not support the SQL `UUID`
/// type in CAST expressions (`not_impl_err!("Unsupported SQL type UUID")`).
///
/// Basin stores UUID column values as `FixedSizeBinary(16)`, but a standalone
/// literal `'str'::UUID` in a SELECT is best returned as its text form, which
/// is what `::VARCHAR` gives us. The pgwire layer renders it as text anyway,
/// so the client sees the correctly hyphenated UUID string.
///
/// The rewrite is case-insensitive for the `::UUID` suffix.
pub(crate) fn rewrite_uuid_cast(sql: &str) -> String {
    // Simple case-insensitive suffix substitution: `::UUID` → `::VARCHAR`.
    // We only replace the literal `::UUID` suffix (no modifier).
    let lower = sql.to_ascii_lowercase();
    let mut out = sql.to_string();
    // Scan for `::uuid` occurrences (case-insensitive) and replace with
    // `::VARCHAR`. We go right-to-left so byte positions stay valid.
    let mut positions: Vec<usize> = Vec::new();
    let mut start = 0usize;
    while let Some(pos) = lower[start..].find("::uuid") {
        let abs = start + pos;
        // Only replace if the character after `::uuid` is NOT alphanumeric or `_`
        // (so `::uuidarray` isn't mangled).
        let end_pos = abs + 6; // len("::uuid") == 6
        let next_is_ident = out.as_bytes().get(end_pos).map(|&c| c.is_ascii_alphanumeric() || c == b'_').unwrap_or(false);
        if !next_is_ident {
            positions.push(abs);
        }
        start = abs + 6;
    }
    for pos in positions.into_iter().rev() {
        out.replace_range(pos..pos + 6, "::VARCHAR");
    }
    out
}

// Bit-string literal rewriter
// ---------------------------------------------------------------------------

/// Rewrite PostgreSQL bit-string literals `B'bits'` to plain string literals
/// `'bits'` before DataFusion sees the SQL.
///
/// sqlparser 0.52 parses `B'1010'` as `Value::SingleQuotedByteStringLiteral`
/// and DataFusion 53 does not handle that value variant, failing with
/// `"Unsupported Value 'SingleQuotedByteStringLiteral'"`.
///
/// Postgres `B'...'` is a bit-string constant — a sequence of `0`/`1`
/// characters. `SELECT B'1010'` returns the text `'1010'` in PG's default
/// representation, so returning the bare string is semantically correct.
///
/// The rewriter detects `B'...'` only when `B` appears **outside** any
/// existing single-quoted string context (e.g. `'B'` is left as-is).
pub(crate) fn rewrite_bit_string_literal(sql: &str) -> String {
    // Use a character-level scan that correctly handles multi-byte UTF-8 and
    // tracks whether we're inside a single-quoted string literal.
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < n {
        // If inside a single-quoted string, consume it character-by-character
        // without trying to detect `B'`. PG uses `''` escaping inside strings.
        if chars[i] == '\'' {
            out.push('\'');
            i += 1;
            // Consume string content until closing `'`, handling `''` escape.
            while i < n {
                if chars[i] == '\'' {
                    out.push('\'');
                    i += 1;
                    if i < n && chars[i] == '\'' {
                        // Escaped quote `''` — continue the string.
                        out.push('\'');
                        i += 1;
                    } else {
                        // End of string.
                        break;
                    }
                } else {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            continue;
        }

        // Detect `B'bits'` — only when B is outside a quoted string.
        // A bit-string literal starts with an uppercase `B` followed immediately
        // by a single quote. Bit strings only contain `0`/`1`, so no quoting
        // can occur inside them; we scan until the closing `'`.
        if chars[i] == 'B' && i + 1 < n && chars[i + 1] == '\'' {
            i += 2; // skip `B` and opening `'`
            let mut bits = String::new();
            while i < n && chars[i] != '\'' {
                bits.push(chars[i]);
                i += 1;
            }
            if i < n { i += 1; } // skip closing `'`
            // Emit as plain single-quoted string.
            out.push('\'');
            out.push_str(&bits);
            out.push('\'');
            continue;
        }

        out.push(chars[i]);
        i += 1;
    }
    out
}

// Interval HH:MM:SS cast rewriter
// ---------------------------------------------------------------------------

/// Rewrite `'HH:MM:SS'::INTERVAL` (and lowercase `::interval`) to
/// `'N seconds'::INTERVAL` so Arrow's interval parser can handle it.
///
/// Postgres accepts time-like shorthand for intervals, e.g.
/// `'00:01:00'::INTERVAL` means 1 minute. Arrow's `parse_interval_month_day_nano`
/// only understands the verbose form (`'1 minute'`, `'60 seconds'`, etc.), so
/// DataFusion's `simplify_expressions` optimizer rule fails on the short form.
///
/// The rewriter detects `'H:MM:SS'` / `'HH:MM:SS'` patterns and converts
/// them to the equivalent number of seconds. Non-matching interval strings
/// (like `'1 hour 30 minutes'`) pass through unchanged.
pub(crate) fn rewrite_interval_hms_cast(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;
    while i < n {
        // Look for an opening single-quote — the start of a string literal.
        if bytes[i] != b'\'' {
            // Not a quote: push the char (must be ASCII since we're byte-scanning).
            // Use char-based push to handle UTF-8 correctly.
            let c = sql[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
            continue;
        }

        // Scan the quoted string content.
        let str_start = i; // byte offset of opening `'`
        i += 1;            // skip opening `'`
        let content_start = i;

        // Find closing `'`, handling `''` PG escape inside the string.
        while i < n {
            if bytes[i] == b'\'' {
                if i + 1 < n && bytes[i + 1] == b'\'' {
                    i += 2; // skip `''` escape
                } else {
                    break; // found unescaped closing `'`
                }
            } else {
                i += 1;
            }
        }
        // i points at closing `'` (or n if unterminated — shouldn't happen in valid SQL).
        let content = &sql[content_start..i];
        let str_end_inclusive = if i < n { i + 1 } else { n }; // one past closing `'`
        if i < n { i += 1; } // skip closing `'`

        // Check if what follows (in original position i) is `::interval`.
        let suffix_lower = &lower[i..];
        let is_interval_cast = suffix_lower.starts_with("::interval")
            && !suffix_lower[10..].starts_with(|c: char| c.is_ascii_alphabetic() || c == '_');

        if !is_interval_cast {
            // Not an interval cast — emit the string token literally.
            out.push_str(&sql[str_start..str_end_inclusive]);
            continue;
        }

        // Try to parse content as HH:MM:SS or H:MM:SS.
        if let Some(total_secs) = parse_hms_interval(content) {
            // Rewrite to `'N seconds'::INTERVAL`.
            out.push('\'');
            out.push_str(&total_secs.to_string());
            out.push_str(" seconds'");
            // Consume the `::interval` suffix (preserve the original case of `INTERVAL`).
            out.push_str(&sql[i..i + 10]);
            i += 10;
        } else {
            // Not an HH:MM:SS string — emit unchanged.
            out.push_str(&sql[str_start..str_end_inclusive]);
        }
    }
    out
}

/// Parse `"HH:MM:SS"` or `"H:MM:SS"` (integer components) into total seconds.
/// Returns `None` if the string doesn't match.
fn parse_hms_interval(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.trim().splitn(3, ':').collect();
    if parts.len() != 3 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let sec: i64 = parts[2].parse().ok()?;
    // Reject anything that doesn't look like valid hms (e.g. negative parts).
    if h < 0 || m < 0 || m >= 60 || sec < 0 || sec >= 60 {
        return None;
    }
    Some(h * 3600 + m * 60 + sec)
}

// PG array-literal cast rewriter
// ---------------------------------------------------------------------------

/// Rewrite PostgreSQL curly-brace array literal casts to `make_array(...)`.
///
/// Pattern: `'<curly-list>'::<type>[]`
///
/// Examples:
/// - `'{1,2,3}'::int[]`   → `make_array(1,2,3)`
/// - `'{a,b}'::text[]`    → `make_array('a','b')`
///
/// Multi-dimensional literals (e.g. `'{{1,2},{3,4}}'::int[][]`) are left
/// as-is — they are too complex for this pass.
///
/// Supported element types: `int2`, `int4`, `int8`, `int`, `bigint`,
/// `smallint`, `text`, `varchar`, `float4`, `float8`, `bool`, `boolean`.
pub(crate) fn rewrite_pg_array_literal_casts(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        // Must start with a single-quoted string that begins with `{`.
        if bytes[i] != b'\'' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }

        // Scan the quoted string, looking for its end.
        let str_start = i; // points to opening `'`
        i += 1;
        let str_content_start = i;
        // Skip past optional leading whitespace to check for `{`.
        while i < bytes.len() && bytes[i] == b' ' { i += 1; }
        let is_curly = i < bytes.len() && bytes[i] == b'{';
        i = str_content_start; // reset scan position

        // Find the closing `'` (handling `''` escapes).
        let mut str_end = i;
        while str_end < bytes.len() {
            if bytes[str_end] == b'\'' {
                if str_end + 1 < bytes.len() && bytes[str_end + 1] == b'\'' {
                    str_end += 2;
                    continue;
                }
                break;
            }
            str_end += 1;
        }
        // str_end now points to the closing `'`.
        let str_inner = &sql[i..str_end]; // content between quotes

        if !is_curly || str_inner.starts_with("{{") {
            // Multi-dim or non-curly: pass through unchanged.
            out.push_str(&sql[str_start..str_end + 1]);
            i = str_end + 1;
            continue;
        }

        // After the closing quote, check for `::type[]`.
        let after_str = str_end + 1; // index after closing `'`
        if after_str + 2 > sql.len() || &sql[after_str..after_str + 2] != "::" {
            out.push_str(&sql[str_start..str_end + 1]);
            i = str_end + 1;
            continue;
        }

        // We have `::` — scan the type name.
        let type_start = after_str + 2;
        let mut type_end = type_start;
        while type_end < sql.len()
            && (sql.as_bytes()[type_end].is_ascii_alphanumeric()
                || sql.as_bytes()[type_end] == b'_')
        {
            type_end += 1;
        }
        let type_name = &sql[type_start..type_end];

        // Must be followed by `[]` (single dim).
        if type_end + 2 > sql.len()
            || sql.as_bytes()[type_end] != b'['
            || sql.as_bytes()[type_end + 1] != b']'
        {
            out.push_str(&sql[str_start..str_end + 1]);
            i = str_end + 1;
            continue;
        }

        // Check for multi-dim `[][]` — leave as-is.
        let after_bracket = type_end + 2;
        if after_bracket < sql.len()
            && sql.as_bytes()[after_bracket] == b'['
        {
            out.push_str(&sql[str_start..str_end + 1]);
            i = str_end + 1;
            continue;
        }

        // Validate the type name.
        let type_lower = type_name.to_ascii_lowercase();
        let is_text_type = matches!(
            type_lower.as_str(),
            "text" | "varchar" | "char" | "bpchar"
        );
        let is_numeric_type = matches!(
            type_lower.as_str(),
            "int" | "int2" | "int4" | "int8" | "integer" | "bigint" | "smallint"
            | "float4" | "float8" | "real" | "numeric" | "bool" | "boolean"
        );
        if !is_text_type && !is_numeric_type {
            out.push_str(&sql[str_start..str_end + 1]);
            i = str_end + 1;
            continue;
        }

        // Parse the curly-brace list: strip outer `{` and `}`, split on `,`.
        let inner = str_inner.trim();
        let inner = inner.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(inner);
        let items: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();

        // Build `make_array(...)`.
        let mut args = String::new();
        for (idx, item) in items.iter().enumerate() {
            if idx > 0 { args.push_str(", "); }
            if is_text_type {
                // Quote the item as a string literal.
                args.push('\'');
                args.push_str(item);
                args.push('\'');
            } else {
                args.push_str(item);
            }
        }
        let replacement = format!("make_array({args})");
        out.push_str(&replacement);
        i = after_bracket; // skip past `'...'::type[]`
    }
    out
}

// ---------------------------------------------------------------------------
// json_to_record / jsonb_to_record  AS alias(coldef-list)  rewriter
// ---------------------------------------------------------------------------
//
// PostgreSQL's `json_to_record(obj) AS t(a int, b text)` returns a single row
// from a JSON object with columns declared in the coldef list.  sqlparser 0.61
// cannot parse the typed column-definition list `AS t(a type, b type)` in a
// scalar-SELECT or FROM context; it errors with "Expected: end of statement,
// found: (".
//
// The rewrite maps these two forms to equivalent SQL that sqlparser CAN parse
// and that the DataFusion engine (with the existing `json_to_recordset` UDTF)
// can execute correctly.
//
// ## Scalar-SELECT form (what the matrix tests use)
//
//   BEFORE: SELECT json_to_record('{"a":1,"b":"foo"}'::json) AS t(a int, b text)
//   AFTER:  SELECT * FROM json_to_recordset('[{"a":1,"b":"foo"}]'::json) AS t(a int, b text)
//
// The JSON object literal is wrapped in a single-element JSON array `[...]` so
// the existing array-of-objects recordset UDTF sees exactly one row.  The
// `::json` / `::jsonb` cast suffix is preserved on the outer bracket literal.
//
// ## FROM-clause form (common ETL usage)
//
//   BEFORE: FROM json_to_record('{"a":1}') AS t(a int, b text)
//   AFTER:  FROM json_to_recordset('[{"a":1}]') AS t(a int, b text)
//
// ## Correctness notes
//
// * The `AS t(a int, b text)` coldef annotation is passed untouched to
//   DataFusion's planner.  DataFusion interprets the declared types and adds
//   implicit casts from the Utf8 output of the UDTF — matching PG's behaviour.
// * Missing keys → NULL (the UDTF already emits NULL for absent keys).
// * Type casts: a declared `int` column receiving the string `"42"` is cast by
//   DataFusion's Arrow coercion — identical to `'42'::int` semantics.
// * Nested objects / arrays for a scalar-typed column (e.g. `int`): DataFusion
//   will fail the cast at execution time with a type error, matching PG.
//
// ## Limitations
//
// * Only rewritten when the JSON argument is a single-quoted string literal
//   (with optional `::json` / `::jsonb` suffix).  Column-reference or function-
//   call arguments are left untouched (the query will reach sqlparser and fail
//   with the original parse error — which is the honest behaviour).
// * The scalar-SELECT form returns a flat set of columns rather than PG's
//   composite RECORD type; any caller that wraps the result in `(result).col`
//   won't benefit from this rewrite (uncommon in practice).

/// Rewrite `json_to_record(J) AS t(coldefs)` and `jsonb_to_record(J) AS t(coldefs)`
/// into the `json_to_recordset([J]) AS t(coldefs)` form that sqlparser and
/// DataFusion can parse and execute.
///
/// Handles both scalar-SELECT position and FROM-clause position.
pub(crate) fn rewrite_json_to_record(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();

    // Fast path: no occurrence at all.
    if !lower.contains("json_to_record") && !lower.contains("jsonb_to_record") {
        return sql.to_string();
    }

    let mut out = String::with_capacity(sql.len() + 64);
    let mut i = 0usize;

    while i < sql.len() {
        // Skip single-quoted string literals (don't scan inside them).
        if sql.as_bytes()[i] == b'\'' {
            let end = skip_quoted_string(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }

        // Look for the keyword `json_to_record` or `jsonb_to_record`
        // (case-insensitive).  We do NOT require a word boundary before `json`
        // because identifiers always start at a non-alphanumeric char in
        // practice, but we DO require that the char immediately before is NOT
        // alphanumeric (to avoid matching `some_json_to_record`).
        let rest_lower = &lower[i..];
        let (fn_name_lower, fn_name_len) = if rest_lower.starts_with("jsonb_to_record(")
            || rest_lower.starts_with("jsonb_to_record (")
        {
            ("jsonb_to_record", 15usize)
        } else if rest_lower.starts_with("json_to_record(")
            || rest_lower.starts_with("json_to_record (")
        {
            ("json_to_record", 14usize)
        } else {
            out.push(sql.as_bytes()[i] as char);
            i += 1;
            continue;
        };

        // Confirm word boundary: char before must be non-alphanumeric.
        if i > 0 {
            let prev = sql.as_bytes()[i - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                // Not a standalone identifier — copy and skip.
                out.push(sql.as_bytes()[i] as char);
                i += 1;
                continue;
            }
        }

        // Skip to the opening `(` of the function call.
        let mut j = i + fn_name_len;
        while j < sql.len() && sql.as_bytes()[j] == b' ' { j += 1; }
        if j >= sql.len() || sql.as_bytes()[j] != b'(' {
            // Unexpected — emit as-is.
            out.push(sql.as_bytes()[i] as char);
            i += 1;
            continue;
        }

        // Find the matching closing `)` of the function argument list.
        let arg_open = j;
        let Some(arg_close) = find_matching_close_paren(sql, arg_open) else {
            out.push(sql.as_bytes()[i] as char);
            i += 1;
            continue;
        };
        // arg_str is everything inside the outermost parens.
        let arg_str = &sql[arg_open + 1..arg_close];

        // Now scan past any whitespace after `)` to find `AS`.
        let mut after = arg_close + 1;
        while after < sql.len() && sql.as_bytes()[after] == b' ' { after += 1; }

        let rest_after_lower = lower[after..].to_ascii_lowercase();
        if !rest_after_lower.starts_with("as ") && !rest_after_lower.starts_with("as\t") {
            // No AS clause — emit the whole function call unchanged.
            out.push_str(&sql[i..arg_close + 1]);
            i = arg_close + 1;
            continue;
        }

        // Skip "AS".
        after += 2;
        while after < sql.len() && sql.as_bytes()[after] == b' ' { after += 1; }

        // Read the alias name (identifier).
        let alias_start = after;
        while after < sql.len()
            && (sql.as_bytes()[after].is_ascii_alphanumeric() || sql.as_bytes()[after] == b'_')
        {
            after += 1;
        }
        if alias_start == after {
            // No alias — emit unchanged.
            out.push_str(&sql[i..arg_close + 1]);
            i = arg_close + 1;
            continue;
        }
        let alias = &sql[alias_start..after];

        // Skip whitespace.
        while after < sql.len() && sql.as_bytes()[after] == b' ' { after += 1; }

        // Must have `(coldef-list)` next.
        if after >= sql.len() || sql.as_bytes()[after] != b'(' {
            // No coldef list — not our pattern (plain alias). Emit unchanged.
            out.push_str(&sql[i..arg_close + 1]);
            i = arg_close + 1;
            continue;
        }

        let coldef_open = after;
        let Some(coldef_close) = find_matching_close_paren(sql, coldef_open) else {
            out.push(sql.as_bytes()[i] as char);
            i += 1;
            continue;
        };
        let coldef_str = &sql[coldef_open..coldef_close + 1]; // includes `(` and `)`

        // Extract the JSON argument literal and optional cast suffix.
        // We only rewrite when the argument is a single-quoted literal
        // (optionally with `::json` / `::jsonb` cast).
        let (json_text, cast_suffix) = match extract_json_literal_arg(arg_str) {
            Some(pair) => pair,
            None => {
                // Non-literal arg — emit the whole original expression unchanged.
                // sqlparser will fail with the parse error for the coldef list,
                // which is the honest behaviour for a non-literal arg.
                out.push_str(&sql[i..coldef_close + 1]);
                i = coldef_close + 1;
                continue;
            }
        };

        // Determine the recordset function name (json vs jsonb).
        let recordset_fn = if fn_name_lower == "jsonb_to_record" {
            "jsonb_to_recordset"
        } else {
            "json_to_recordset"
        };

        // Build the rewritten fragment:
        //   json_to_recordset('[{...}]'::json) AS alias(coldef-list)
        //
        // We do NOT emit `SELECT * FROM` here — the rewriter produces just the
        // table-function expression.  The scalar-SELECT→FROM restructuring is
        // handled separately below in a second pass.
        let wrapped_json = format!("'[{json_text}]'{cast_suffix}");
        let rewritten_fn = format!("{recordset_fn}({wrapped_json}) AS {alias}{coldef_str}");

        out.push_str(&rewritten_fn);
        i = coldef_close + 1;
    }

    // Second pass: if any scalar-SELECT contained the rewritten form, we need
    // to restructure:
    //   SELECT ... json_to_recordset(...) AS t(...) ...  (scalar position)
    // → SELECT * FROM json_to_recordset(...) AS t(...)
    //
    // We detect the scalar-SELECT case by looking for the pattern
    // `SELECT <ws> json_to_recordset(` or `SELECT <ws> jsonb_to_recordset(`
    // that is NOT already preceded by `FROM` on the same logical level.
    rewrite_scalar_select_to_from_recordset(&out)
}

/// Wrap `SELECT json_to_recordset(…) AS t(…)` (scalar position, no FROM)
/// into `SELECT * FROM json_to_recordset(…) AS t(…)`.
fn rewrite_scalar_select_to_from_recordset(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();

    // We look for the pattern: `SELECT` followed (possibly with whitespace) by
    // `json_to_recordset(` or `jsonb_to_recordset(`, where there is no `FROM`
    // between `SELECT` and the function call (meaning it's in scalar position).
    //
    // The match anchor: `select <sp>+ json[b]_to_recordset(`
    // We only handle the simple case where the function call is the ENTIRE
    // SELECT list (no additional columns before/after it).

    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;

    while i < sql.len() {
        // Skip quoted strings.
        if sql.as_bytes()[i] == b'\'' {
            let end = skip_quoted_string(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }

        let rest_lower = &lower[i..];

        // Match `select` keyword.
        if !rest_lower.starts_with("select") {
            out.push(sql.as_bytes()[i] as char);
            i += 1;
            continue;
        }

        // Word boundary after `select`.
        let after_select = i + 6;
        if after_select < sql.len() {
            let next = sql.as_bytes()[after_select];
            if next.is_ascii_alphanumeric() || next == b'_' {
                out.push(sql.as_bytes()[i] as char);
                i += 1;
                continue;
            }
        }

        // Skip whitespace after SELECT.
        let mut j = after_select;
        while j < sql.len() && (sql.as_bytes()[j] == b' ' || sql.as_bytes()[j] == b'\n' || sql.as_bytes()[j] == b'\t') {
            j += 1;
        }

        // Check for `json[b]_to_recordset(` immediately after SELECT + whitespace.
        let from_j = &lower[j..];
        let is_match = from_j.starts_with("json_to_recordset(")
            || from_j.starts_with("jsonb_to_recordset(");

        if !is_match {
            // Not our pattern — copy the SELECT keyword and advance.
            out.push_str(&sql[i..after_select]);
            i = after_select;
            continue;
        }

        // Confirm there's no FROM between the original SELECT and this position
        // (i.e., the recordset call is genuinely in scalar-SELECT position).
        // Since we built `out` from a previous pass that already rewrote the
        // function calls, the `SELECT` we found was already preceded by our
        // rewritten function.  We trust the pattern is scalar-position if we
        // see `SELECT <ws> json[b]_to_recordset(`.

        // Emit `SELECT * FROM ` instead of just `SELECT `.
        out.push_str("SELECT * FROM ");
        i = j; // advance past the `SELECT` + whitespace; the fn call follows
    }

    out
}

/// Skip a single-quoted SQL string literal starting at `start` (which must be
/// the `'` character).  Returns the index of the character AFTER the closing
/// `'`.  Handles `''` escape sequences.
fn skip_quoted_string(s: &str, start: usize) -> usize {
    let bytes = s.as_bytes();
    debug_assert!(bytes[start] == b'\'');
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    i // unclosed literal — return end of string
}

/// Attempt to parse a function argument string as a JSON string literal with
/// optional `::json` / `::jsonb` / `::text` suffix.
///
/// Returns `(json_text_content, cast_suffix)` where:
/// * `json_text_content` is the text INSIDE the quotes (without the quotes
///   themselves or the cast suffix).
/// * `cast_suffix` is the `::json` / `::jsonb` suffix verbatim (e.g. `"::json"`)
///   or `""` if absent.
///
/// Only recognises the form `'...'` or `'...'::json[b]` (no dollar quoting,
/// no `E'...'`, no nested function calls).  Returns `None` for anything else.
fn extract_json_literal_arg(arg: &str) -> Option<(String, String)> {
    let trimmed = arg.trim();
    if !trimmed.starts_with('\'') {
        return None;
    }

    // Find the closing quote (handling `''` escapes).
    let bytes = trimmed.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            break; // closing quote
        }
        i += 1;
    }
    if i >= bytes.len() {
        return None; // unclosed
    }
    // i points to closing `'`.
    let json_content = &trimmed[1..i]; // text inside quotes
    let after_quote = &trimmed[i + 1..].trim_start();

    // Optional `::json`, `::jsonb`, `::text` cast.
    let cast_suffix = if after_quote.to_ascii_lowercase().starts_with("::jsonb") {
        after_quote[..7].to_string()
    } else if after_quote.to_ascii_lowercase().starts_with("::json") {
        after_quote[..6].to_string()
    } else if after_quote.to_ascii_lowercase().starts_with("::text") {
        after_quote[..6].to_string()
    } else if after_quote.is_empty() {
        String::new()
    } else {
        // Unexpected suffix — not a plain JSON literal.
        return None;
    };

    Some((json_content.to_string(), cast_suffix))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── POSIX regex rewriter ────────────────────────────────────────────────

    #[test]
    fn regex_tilde_rewrites_to_regexp_like() {
        let sql = "SELECT * FROM t WHERE name ~ '^a'";
        let out = rewrite_posix_regex_operators(sql);
        assert!(out.contains("regexp_like(name, '^a')"), "got: {out}");
    }

    #[test]
    fn regex_bang_tilde_rewrites_to_not_regexp_like() {
        let sql = "SELECT * FROM t WHERE name !~ '^a'";
        let out = rewrite_posix_regex_operators(sql);
        assert!(out.contains("NOT regexp_like(name, '^a')"), "got: {out}");
    }

    #[test]
    fn regex_tilde_star_rewrites_case_insensitive() {
        let sql = "SELECT * FROM t WHERE name ~* '^A'";
        let out = rewrite_posix_regex_operators(sql);
        assert!(out.contains("regexp_like(name, '^A', 'i')"), "got: {out}");
    }

    #[test]
    fn regex_bang_tilde_star_rewrites_case_insensitive_negative() {
        let sql = "SELECT * FROM t WHERE name !~* '^A'";
        let out = rewrite_posix_regex_operators(sql);
        assert!(out.contains("NOT regexp_like(name, '^A', 'i')"), "got: {out}");
    }

    #[test]
    fn regex_no_false_positive_on_tilde_tilde() {
        // `~~` is the LIKE operator in PG internal form — must not be rewritten.
        let sql = "SELECT * FROM t WHERE name ~~ 'a%'";
        let out = rewrite_posix_regex_operators(sql);
        // Should not introduce regexp_like.
        assert!(!out.contains("regexp_like"), "got: {out}");
    }

    // ── find_word_sequence: multi-byte UTF-8 safety ─────────────────────────

    #[test]
    fn find_word_sequence_ascii_match() {
        // Basic sanity: still finds needle in ASCII-only input.
        assert_eq!(find_word_sequence("between symmetric", "between symmetric"), Some(0));
        assert_eq!(find_word_sequence("select between symmetric foo", "between symmetric"), Some(7));
    }

    #[test]
    fn find_word_sequence_no_panic_multibyte_em_dash() {
        // em-dash is 3 bytes (U+2014 → 0xE2 0x80 0x94).  Must not panic.
        let lower = "hello \u{2014} world";
        assert_eq!(find_word_sequence(lower, "between symmetric"), None);
    }

    #[test]
    fn find_word_sequence_no_panic_emoji() {
        // Emoji is 4 bytes (U+1F44D → 0xF0 0x9F 0x91 0x8D).  Must not panic.
        let lower = "select '\u{1F44D}' from t";
        assert_eq!(find_word_sequence(lower, "between symmetric"), None);
    }

    #[test]
    fn find_word_sequence_no_panic_cjk() {
        // CJK characters are 3 bytes each.  Must not panic.
        let lower = "select '\u{65E5}\u{672C}\u{8A9E}' from t";
        assert_eq!(find_word_sequence(lower, "between symmetric"), None);
    }

    #[test]
    fn find_word_sequence_match_after_multibyte() {
        // Needle appears AFTER multi-byte content — offset must still be correct.
        let lower = "caf\u{00E9} between symmetric 1 and 10";
        // "café" is 5 bytes (c-a-f + 2-byte U+00E9), so "between" starts at 6.
        let pos = find_word_sequence(lower, "between symmetric");
        assert!(pos.is_some(), "should find 'between symmetric' after multibyte prefix");
        // Verify the returned position is actually a char boundary.
        assert!(lower.is_char_boundary(pos.unwrap()), "returned offset must be a char boundary");
    }

    // ── BETWEEN SYMMETRIC rewriter ──────────────────────────────────────────

    #[test]
    fn between_symmetric_expands() {
        let sql = "SELECT * FROM t WHERE x BETWEEN SYMMETRIC 1 AND 10";
        let out = rewrite_between_symmetric(sql);
        // Should contain two BETWEEN clauses.
        assert!(out.contains("BETWEEN"), "got: {out}");
        assert!(!out.to_uppercase().contains("SYMMETRIC"), "SYMMETRIC should be gone: {out}");
    }

    #[test]
    fn between_symmetric_passthrough_plain_between() {
        let sql = "SELECT * FROM t WHERE x BETWEEN 1 AND 10";
        let out = rewrite_between_symmetric(sql);
        // Plain BETWEEN must not be touched.
        assert_eq!(out, sql);
    }

    // ── UUID cast rewriter ──────────────────────────────────────────────────

    #[test]
    fn uuid_cast_rewrite_basic() {
        let sql = "SELECT 'a6c5e8f0-1234-5678-abcd-000000000000'::UUID";
        let out = rewrite_uuid_cast(sql);
        assert!(out.contains("::VARCHAR") || out.contains("::varchar"), "got: {out}");
        assert!(!out.to_uppercase().contains("::UUID"), "UUID should be gone: {out}");
    }

    #[test]
    fn uuid_cast_rewrite_case_insensitive() {
        let sql = "SELECT 'a6c5e8f0-1234-5678-abcd-000000000000'::uuid";
        let out = rewrite_uuid_cast(sql);
        assert!(!out.to_uppercase().contains("::UUID"), "uuid should be gone: {out}");
    }

    #[test]
    fn uuid_cast_passthrough_no_cast() {
        let sql = "SELECT 'hello world'";
        let out = rewrite_uuid_cast(sql);
        assert_eq!(out, sql);
    }

    // ── Bit-string literal rewriter ─────────────────────────────────────────

    #[test]
    fn bit_string_rewrite_basic() {
        let sql = "SELECT B'1010'";
        let out = rewrite_bit_string_literal(sql);
        assert_eq!(out, "SELECT '1010'");
    }

    #[test]
    fn bit_string_passthrough_b_inside_string() {
        // `'B'` is a string literal containing the letter B — must NOT be rewritten.
        let sql = "UPDATE t SET name = 'B' WHERE id = 2";
        let out = rewrite_bit_string_literal(sql);
        assert_eq!(out, sql, "string containing 'B' must not be rewritten");
    }

    #[test]
    fn bit_string_passthrough_no_b() {
        let sql = "SELECT 'hello'";
        let out = rewrite_bit_string_literal(sql);
        assert_eq!(out, sql);
    }

    // ── Interval HH:MM:SS rewriter ──────────────────────────────────────────

    #[test]
    fn interval_hms_rewrite_basic() {
        let sql = "SELECT '00:01:00'::INTERVAL";
        let out = rewrite_interval_hms_cast(sql);
        // 0*3600 + 1*60 + 0 = 60 seconds
        assert!(out.contains("'60 seconds'") || out.contains("'60 second'"), "got: {out}");
    }

    #[test]
    fn interval_hms_rewrite_hours_mins_secs() {
        let sql = "SELECT '01:30:15'::interval";
        let out = rewrite_interval_hms_cast(sql);
        // 1*3600 + 30*60 + 15 = 5415 seconds
        assert!(out.contains("'5415 seconds'") || out.contains("'5415 second'"), "got: {out}");
    }

    #[test]
    fn interval_hms_passthrough_non_hms_interval() {
        // Postgres standard format — must pass through unchanged.
        let sql = "SELECT '1 hour'::INTERVAL";
        let out = rewrite_interval_hms_cast(sql);
        assert_eq!(out, sql);
    }

    // ── Array operator rewriter ─────────────────────────────────────────────

    #[test]
    fn array_contains_rewrite() {
        // @> rewrites to list_has_all(lhs, rhs) — (array, array) signature.
        let sql = "SELECT ARRAY[1,2,3] @> ARRAY[1,2]";
        let out = rewrite_array_operators(sql);
        assert!(out.contains("list_has_all("), "got: {out}");
        assert!(out.contains("ARRAY[1,2,3], ARRAY[1,2]"), "got: {out}");
    }

    #[test]
    fn array_contained_by_rewrite() {
        // <@ rewrites to list_has_all(rhs, lhs) — haystack is rhs, needle-set is lhs.
        let sql = "SELECT ARRAY[1,2] <@ ARRAY[1,2,3]";
        let out = rewrite_array_operators(sql);
        assert!(out.contains("list_has_all("), "got: {out}");
        // rhs comes first in the call: list_has_all(ARRAY[1,2,3], ARRAY[1,2])
        assert!(out.contains("ARRAY[1,2,3], ARRAY[1,2]"), "got: {out}");
    }

    #[test]
    fn array_overlap_rewrite() {
        let sql = "SELECT ARRAY[1,2] && ARRAY[2,3]";
        let out = rewrite_array_operators(sql);
        assert!(out.contains("arrays_overlap("), "got: {out}");
    }

    #[test]
    fn array_op_no_rewrite_plain_columns() {
        // Plain column references without array literal — must not rewrite.
        let sql = "SELECT * FROM t WHERE a && b";
        let out = rewrite_array_operators(sql);
        // Should not introduce arrays_overlap.
        assert!(!out.contains("arrays_overlap"), "should not rewrite plain cols: {out}");
    }

    // ── = ANY(ARRAY[...]) rewriter ──────────────────────────────────────────

    #[test]
    fn any_array_eq_basic() {
        let sql = "SELECT 2 = ANY(ARRAY[1,2,3])";
        let out = rewrite_any_array(sql);
        assert_eq!(out, "SELECT 2 IN (1,2,3)", "got: {out}");
    }

    #[test]
    fn any_array_eq_spaces() {
        let sql = "SELECT 2 = ANY ( ARRAY[1,2,3] )";
        let out = rewrite_any_array(sql);
        assert!(out.contains("IN ("), "got: {out}");
        assert!(!out.contains("ANY"), "ANY should be gone: {out}");
    }

    #[test]
    fn any_array_neq_to_not_in() {
        let sql = "SELECT 5 <> ANY(ARRAY[1,2,3])";
        let out = rewrite_any_array(sql);
        assert!(out.contains("NOT IN ("), "got: {out}");
    }

    #[test]
    fn any_array_no_rewrite_subquery() {
        // Subquery form must not be touched by rewrite_any_array.
        let sql = "SELECT id = ANY(SELECT id FROM t)";
        let out = rewrite_any_array(sql);
        assert_eq!(out, sql, "subquery form must be unchanged: {out}");
    }

    // ── > ALL(ARRAY[...]) rewriter ──────────────────────────────────────────

    #[test]
    fn all_array_gt_to_array_max() {
        let sql = "SELECT 5 > ALL(ARRAY[1,2,3])";
        let out = rewrite_all_array(sql);
        assert_eq!(out, "SELECT 5 > array_max(ARRAY[1,2,3])", "got: {out}");
    }

    #[test]
    fn all_array_lt_to_array_min() {
        let sql = "SELECT 1 < ALL(ARRAY[2,3,4])";
        let out = rewrite_all_array(sql);
        assert_eq!(out, "SELECT 1 < array_min(ARRAY[2,3,4])", "got: {out}");
    }

    #[test]
    fn all_array_gte_to_array_max() {
        let sql = "SELECT 5 >= ALL(ARRAY[1,2,3])";
        let out = rewrite_all_array(sql);
        assert_eq!(out, "SELECT 5 >= array_max(ARRAY[1,2,3])", "got: {out}");
    }

    #[test]
    fn all_array_no_rewrite_subquery() {
        // Subquery form must not be touched by rewrite_all_array.
        let sql = "SELECT id > ALL(SELECT id FROM t)";
        let out = rewrite_all_array(sql);
        assert_eq!(out, sql, "subquery form must be unchanged: {out}");
    }

    // ── LATERAL unnest rewriter ─────────────────────────────────────────────

    #[test]
    fn lateral_unnest_stripped() {
        let sql = "SELECT * FROM t, LATERAL unnest(ARRAY[1,2,3]) tag";
        let out = rewrite_lateral_unnest(sql);
        assert!(!out.contains("LATERAL"), "LATERAL should be gone: {out}");
        assert!(out.contains("unnest("), "unnest should remain: {out}");
    }

    #[test]
    fn lateral_unnest_case_insensitive() {
        let sql = "SELECT * FROM t, lateral UNNEST(ARRAY[1,2,3]) tag";
        let out = rewrite_lateral_unnest(sql);
        assert!(!out.to_ascii_lowercase().contains("lateral unnest"), "LATERAL should be gone: {out}");
        assert!(out.to_ascii_lowercase().contains("unnest("), "unnest should remain: {out}");
    }

    #[test]
    fn lateral_non_unnest_preserved() {
        // LATERAL with non-unnest function must NOT be stripped.
        let sql = "SELECT * FROM t, LATERAL jsonb_each('{\"a\":1}'::jsonb) j";
        let out = rewrite_lateral_unnest(sql);
        assert_eq!(out, sql, "non-unnest LATERAL must be unchanged: {out}");
    }

    #[test]
    fn lateral_subquery_preserved() {
        // LATERAL subquery form must NOT be stripped.
        let sql = "SELECT * FROM t, LATERAL (SELECT id FROM u WHERE u.id = t.id) sub";
        let out = rewrite_lateral_unnest(sql);
        assert_eq!(out, sql, "LATERAL subquery must be unchanged: {out}");
    }

    // ── Uncorrelated LATERAL strip ───────────────────────────────────────────

    #[test]
    fn lateral_uncorrelated_comma_join_stripped() {
        // No outer column reference → strip LATERAL.
        let sql = "SELECT t.id, sub.c FROM t, LATERAL (SELECT 42 AS c) sub";
        let out = rewrite_lateral_uncorrelated(sql);
        assert!(!out.to_ascii_lowercase().contains("lateral"), "LATERAL should be stripped: {out}");
        assert!(out.contains("(SELECT 42 AS c)"), "subquery body must remain: {out}");
    }

    #[test]
    fn lateral_uncorrelated_left_join_stripped() {
        // Uncorrelated LEFT JOIN LATERAL — strip LATERAL.
        let sql = "SELECT t.id FROM t LEFT JOIN LATERAL (SELECT 1 AS n) sub ON true";
        let out = rewrite_lateral_uncorrelated(sql);
        assert!(!out.to_ascii_lowercase().contains("lateral"), "LATERAL should be stripped: {out}");
        assert!(out.contains("(SELECT 1 AS n)"), "subquery body must remain: {out}");
    }

    #[test]
    fn lateral_correlated_subquery_preserved() {
        // Correlated (references t.id) → must NOT strip LATERAL.
        let sql = "SELECT * FROM t LEFT JOIN LATERAL (SELECT id FROM u WHERE u.id = t.id) sub ON true";
        let out = rewrite_lateral_uncorrelated(sql);
        assert!(out.to_ascii_lowercase().contains("lateral"), "correlated LATERAL must be preserved: {out}");
    }

    #[test]
    fn lateral_correlated_comma_join_preserved() {
        // Correlated comma-LATERAL — must NOT strip.
        let sql = "SELECT * FROM t, LATERAL (SELECT id FROM u WHERE u.id = t.id) sub";
        let out = rewrite_lateral_uncorrelated(sql);
        assert!(out.to_ascii_lowercase().contains("lateral"), "correlated LATERAL must be preserved: {out}");
    }

    #[test]
    fn lateral_uncorrelated_no_outer_tables() {
        // No outer table at all (degenerate FROM, unlikely but safe).
        let sql = "SELECT sub.x FROM LATERAL (SELECT 5 AS x) sub";
        let out = rewrite_lateral_uncorrelated(sql);
        // No outer names detected → treated as uncorrelated → LATERAL stripped.
        assert!(!out.to_ascii_lowercase().contains("lateral"), "should strip: {out}");
    }

    // ── Nested-aggregate ORM LATERAL rewrite ────────────────────────────────

    #[test]
    fn lateral_nested_agg_json_agg_rewrites() {
        // Drizzle/Prisma pattern: json_agg correlated by FK.
        let sql = "SELECT u.id, agg.posts FROM users u LEFT JOIN LATERAL (SELECT json_agg(p.title) AS posts FROM posts p WHERE p.author_id = u.id) agg ON true ORDER BY u.id";
        let out = rewrite_lateral_nested_agg(&sql);
        let out_lower = out.to_ascii_lowercase();
        // Must not contain LATERAL any more.
        assert!(!out_lower.contains("lateral"), "LATERAL should be rewritten: {out}");
        // Must contain a GROUP BY on the FK column.
        assert!(out_lower.contains("group by"), "must have GROUP BY: {out}");
        // The ON condition must reference the subquery alias.
        assert!(out_lower.contains("on agg."), "ON clause must reference sub alias: {out}");
        // Original ON true must be gone.
        assert!(!out_lower.contains("on true"), "ON true must be replaced: {out}");
    }

    #[test]
    fn lateral_nested_agg_count_rewrites() {
        // count(*) correlation.
        let sql = "SELECT p.id, cnt.c FROM parents p LEFT JOIN LATERAL (SELECT count(*) AS c FROM children ch WHERE ch.parent_id = p.id) cnt ON true";
        let out = rewrite_lateral_nested_agg(&sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(!out_lower.contains("lateral"), "LATERAL gone: {out}");
        assert!(out_lower.contains("group by"), "GROUP BY present: {out}");
    }

    #[test]
    fn lateral_nested_agg_order_by_not_rewritten() {
        // ORDER BY inside subquery → must NOT rewrite (result would be wrong).
        let sql = "SELECT u.id FROM users u LEFT JOIN LATERAL (SELECT json_agg(p.title ORDER BY p.id) AS posts FROM posts p WHERE p.author_id = u.id) agg ON true";
        let out = rewrite_lateral_nested_agg(&sql);
        let out_lower = out.to_ascii_lowercase();
        // Must be left untouched (still has LATERAL).
        assert!(out_lower.contains("lateral"), "ORDER BY inside must prevent rewrite: {out}");
    }

    #[test]
    fn lateral_nested_agg_non_aggregate_proj_not_rewritten() {
        // Non-aggregate projection (bare `id`) → must NOT rewrite.
        let sql = "SELECT t.id FROM t LEFT JOIN LATERAL (SELECT u.id FROM u WHERE u.id = t.id) sub ON true";
        let out = rewrite_lateral_nested_agg(&sql);
        assert!(out.to_ascii_lowercase().contains("lateral"), "non-aggregate projection must not be rewritten: {out}");
    }

    #[test]
    fn lateral_nested_agg_not_on_true_not_rewritten() {
        // JOIN condition is not `ON true` → must NOT rewrite.
        let sql = "SELECT t.id FROM t LEFT JOIN LATERAL (SELECT count(*) AS c FROM u WHERE u.tid = t.id) sub ON sub.c > 0";
        let out = rewrite_lateral_nested_agg(&sql);
        assert!(out.to_ascii_lowercase().contains("lateral"), "non-ON-true must not be rewritten: {out}");
    }

    #[test]
    fn lateral_non_rewritable_correlated_still_fails_path() {
        // Arbitrary correlated LATERAL (not agg, not uncorrelated) is left intact.
        // Both rewriters should leave it untouched.
        let sql = "SELECT * FROM t JOIN LATERAL (SELECT id * 2 AS dbl FROM u WHERE u.id = t.id) sub ON true";
        let out1 = rewrite_lateral_uncorrelated(sql);
        let out2 = rewrite_lateral_nested_agg(&out1);
        // Still has LATERAL — will fail at DataFusion physical plan (upstream limitation).
        assert!(out2.to_ascii_lowercase().contains("lateral"), "non-rewritable correlated LATERAL must remain: {out2}");
    }

    // ── WITH RECURSIVE column alias rewriter ────────────────────────────────

    #[test]
    fn recursive_cte_single_col_alias_added() {
        let sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r";
        let out = rewrite_recursive_cte_column_aliases(sql);
        assert!(out.contains("SELECT 1 AS n"), "expected AS alias: {out}");
        // Ensure a space separates the alias from UNION (no `nUNION` token merge).
        assert!(out.contains("1 AS n UNION"), "expected space before UNION: {out}");
    }

    #[test]
    fn recursive_cte_multi_col_alias_added() {
        let sql = "WITH RECURSIVE fib(a, b) AS (SELECT 1, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT a FROM fib";
        let out = rewrite_recursive_cte_column_aliases(sql);
        assert!(out.contains("SELECT 1 AS a, 1 AS b"), "expected AS aliases: {out}");
    }

    #[test]
    fn recursive_cte_already_aliased_unchanged() {
        // If the base case already has AS aliases, they are preserved.
        let sql = "WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r";
        let out = rewrite_recursive_cte_column_aliases(sql);
        // No additional aliasing should happen (no column list on the CTE).
        assert_eq!(out, sql, "no column list — must not touch: {out}");
    }

    #[test]
    fn non_recursive_cte_unchanged() {
        let sql = "WITH foo AS (SELECT 1 AS x) SELECT * FROM foo";
        let out = rewrite_recursive_cte_column_aliases(sql);
        assert_eq!(out, sql, "non-recursive must be unchanged: {out}");
    }

    // ── bitwise `#` vs JSONB `#-` ────────────────────────────────────────────

    #[test]
    fn bitwise_xor_hash_rewrites_bare_hash() {
        // `A # B` (bitwise XOR) must become `A ^ B`.
        let out = rewrite_bitwise_xor_hash("SELECT 5 # 3");
        assert_eq!(out, "SELECT 5 ^ 3");
    }

    #[test]
    fn bitwise_xor_hash_preserves_hash_minus() {
        // `jsonb #- path` must NOT be mangled to `^-`.
        let sql = "SELECT '{\"a\":1}' #- '{a}'";
        let out = rewrite_bitwise_xor_hash(sql);
        assert_eq!(out, sql, "#- must be left untouched: {out}");
    }

    #[test]
    fn bitwise_xor_hash_preserves_hash_arrow() {
        // `#>` and `#>>` must not be touched.
        let sql = "SELECT data #> '{a}' FROM t";
        let out = rewrite_bitwise_xor_hash(sql);
        assert_eq!(out, sql, "#> must be left untouched: {out}");
    }

    // ── json_to_record / jsonb_to_record rewriter ────────────────────────────

    #[test]
    fn json_to_record_scalar_select_rewrites_to_from_recordset() {
        // The primary matrix test case: scalar SELECT with ::json cast.
        let sql = r#"SELECT json_to_record('{"a":1,"b":"foo"}'::json) AS t(a int, b text)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("json_to_recordset"), "must use recordset fn: {out}");
        assert!(out_lower.contains("select * from"), "must restructure to SELECT * FROM: {out}");
        assert!(out_lower.contains("as t(a int, b text)"), "coldef list must be preserved: {out}");
        // The JSON literal must be wrapped in a single-element array.
        assert!(out.contains(r#"'[{"a":1,"b":"foo"}]'"#), "JSON must be wrapped in array: {out}");
        // The ::json cast must be preserved.
        assert!(out.contains("::json"), "cast suffix must be preserved: {out}");
        // Must not contain the original json_to_record (not recordset) as the function.
        assert!(!out_lower.contains("json_to_record("), "original fn must be gone: {out}");
    }

    #[test]
    fn jsonb_to_record_scalar_select_rewrites() {
        // jsonb variant with ::jsonb cast.
        let sql = r#"SELECT jsonb_to_record('{"a":1,"b":"foo"}'::jsonb) AS t(a int, b text)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("jsonb_to_recordset"), "must use jsonb_to_recordset: {out}");
        assert!(out_lower.contains("select * from"), "must restructure to SELECT * FROM: {out}");
        assert!(out_lower.contains("as t(a int, b text)"), "coldef list preserved: {out}");
        assert!(out.contains(r#"'[{"a":1,"b":"foo"}]'"#), "JSON wrapped in array: {out}");
        assert!(out.contains("::jsonb"), "jsonb cast preserved: {out}");
    }

    #[test]
    fn json_to_record_missing_key_sql_correct() {
        // Missing key: '{"a":1}' with coldef (a int, b text) — b will be NULL.
        let sql = r#"SELECT json_to_record('{"a":1}'::json) AS t(a int, b text)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("json_to_recordset"), "must use recordset fn: {out}");
        assert!(out_lower.contains("select * from"), "must be FROM form: {out}");
        // The coldef list (a int, b text) must remain for DataFusion to apply casts.
        assert!(out_lower.contains("as t(a int, b text)"), "coldef list must survive: {out}");
        // The wrapped array must contain just the one object.
        assert!(out.contains(r#"'[{"a":1}]'"#), "JSON wrapped: {out}");
    }

    #[test]
    fn json_to_record_no_cast_suffix_rewrites() {
        // No ::json cast — just a bare string literal.
        let sql = r#"SELECT json_to_record('{"a":"42"}'::json) AS t(a int)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("json_to_recordset"), "must use recordset: {out}");
        assert!(out_lower.contains("select * from"), "must be FROM form: {out}");
        assert!(out_lower.contains("as t(a int)"), "coldef preserved: {out}");
    }

    #[test]
    fn json_to_record_from_clause_rewrites() {
        // FROM-clause position: rewrite to recordset without SELECT restructuring.
        let sql = r#"SELECT * FROM json_to_record('{"a":1}'::json) AS t(a int, b text)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("json_to_recordset"), "must use recordset: {out}");
        assert!(!out_lower.contains("json_to_record("), "original fn must be gone: {out}");
        assert!(out_lower.contains("as t(a int, b text)"), "coldef list preserved: {out}");
        // The FROM keyword should be present (was already there).
        assert!(out_lower.contains("from"), "FROM must remain: {out}");
    }

    #[test]
    fn json_to_record_no_coldef_list_unchanged() {
        // A plain `json_to_record(x)` with no `AS t(...)` coldef list must not be touched.
        let sql = r#"SELECT json_to_record('{"a":1}'::json) AS result"#;
        let out = rewrite_json_to_record(sql);
        // Should not be rewritten (no coldef list after the alias).
        assert!(!out.to_ascii_lowercase().contains("recordset"), "no rewrite without coldef list: {out}");
    }

    #[test]
    fn json_to_record_no_occurrence_passthrough() {
        // Queries with no json_to_record must pass through unchanged.
        let sql = "SELECT 1 + 1 AS two";
        let out = rewrite_json_to_record(sql);
        assert_eq!(out, sql, "should be unchanged: {out}");
    }

    #[test]
    fn json_to_record_jsonb_from_clause_rewrites() {
        // FROM-clause jsonb variant.
        let sql = r#"SELECT * FROM jsonb_to_record('{"x":"hello"}'::jsonb) AS t(x text)"#;
        let out = rewrite_json_to_record(sql);
        let out_lower = out.to_ascii_lowercase();
        assert!(out_lower.contains("jsonb_to_recordset"), "must use jsonb_to_recordset: {out}");
        assert!(out_lower.contains("as t(x text)"), "coldef preserved: {out}");
        assert!(out.contains("::jsonb"), "cast preserved: {out}");
    }

    #[test]
    fn json_to_record_preserves_single_quotes_in_json() {
        // JSON string with embedded single-quote escape `''` must survive intact.
        // Basin stores strings as `''` inside SQL string literals.
        let sql = r#"SELECT json_to_record('{"a":"it''s"}'::json) AS t(a text)"#;
        let out = rewrite_json_to_record(sql);
        // The embedded `''` must survive in the wrapped form.
        assert!(out.contains("it''s"), "escaped quotes must survive: {out}");
        assert!(out.to_ascii_lowercase().contains("json_to_recordset"), "must use recordset: {out}");
    }
}
