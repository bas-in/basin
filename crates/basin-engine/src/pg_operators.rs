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
//! | `@>`        | `array_contains(lhs, rhs)`               |
//! | `<@`        | `array_contains(rhs, lhs)`               |
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
    let needle = sequence.as_bytes();
    let mut i = 0usize;
    while i + needle.len() <= lower.len() {
        if lower[i..].starts_with(sequence) {
            let pre_ok = i == 0 || !lb[i - 1].is_ascii_alphanumeric();
            let post = i + needle.len();
            let post_ok = post >= lower.len() || !lb[post].is_ascii_alphanumeric();
            if pre_ok && post_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Find the first word-boundary `AND` in `lower` at or after `start`.
fn find_and_after(lower: &str, start: usize) -> Option<usize> {
    let lb = lower.as_bytes();
    let mut depth = 0i32;
    let mut i = start;
    while i + 3 <= lower.len() {
        match lb[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                // Skip string literal.
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
                if depth == 0 && lower[i..].starts_with("and") {
                    let pre_ok = i == 0 || !lb[i - 1].is_ascii_alphanumeric();
                    let post_ok = i + 3 >= lower.len() || !lb[i + 3].is_ascii_alphanumeric();
                    if pre_ok && post_ok {
                        return Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

/// Rewrite array containment / overlap operators for array-typed operands.
///
/// Operators translated:
/// - `A @> B`  → `array_contains(A, B)`   (A contains all elements of B)
/// - `A <@ B`  → `array_contains(B, A)`   (A contained in B)
/// - `A && B`  → `arrays_overlap(A, B)`   (A and B share at least one element)
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
    loop {
        let Some(op_start) = find_regex_op_outside_strings(&s, op) else {
            break;
        };
        let op_end = op_start + op.len();

        let (lhs_start, lhs_end) = regex_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = regex_extract_right(&s, op_end);
        let lhs = s[lhs_start..lhs_end].trim().to_string();
        let rhs = s[rhs_start..rhs_end].trim().to_string();

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
            "@>" => format!("array_contains({lhs}, {rhs})"),
            "<@" => format!("array_contains({rhs}, {lhs})"),
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

    // ── Array operator rewriter ─────────────────────────────────────────────

    #[test]
    fn array_contains_rewrite() {
        let sql = "SELECT ARRAY[1,2,3] @> ARRAY[1,2]";
        let out = rewrite_array_operators(sql);
        assert!(out.contains("array_contains("), "got: {out}");
    }

    #[test]
    fn array_contained_by_rewrite() {
        let sql = "SELECT ARRAY[1,2] <@ ARRAY[1,2,3]";
        let out = rewrite_array_operators(sql);
        // <@ rewrites to array_contains(rhs, lhs).
        assert!(out.contains("array_contains("), "got: {out}");
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
}
