//! `CITEXT` — case-insensitive text type utilities shared across crates.
//!
//! Basin represents citext columns as Arrow `Utf8` with the field-metadata
//! marker `BASIN_TYPE=CITEXT` (defined in `basin-engine::types`). This module
//! provides the shared case-folding logic that UNIQUE enforcement, ORDER BY,
//! comparison operators, and LIKE/pattern matching all use to achieve
//! PostgreSQL-compatible citext semantics.
//!
//! PostgreSQL citext semantics:
//! - All comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`) are case-insensitive.
//! - LIKE / ILIKE pattern matching is case-insensitive.
//! - ORDER BY on a citext column sorts case-insensitively.
//! - UNIQUE constraints treat 'Foo' and 'foo' as duplicates.
//!
//! Implementation: case-fold both operands via Unicode lower-case mapping
//! (`str::to_lowercase()`) before comparing. This matches PG citext's use
//! of `pg_strncasecmp` which is locale-aware but folds via lower() in the
//! default C/POSIX locale Basin targets.

/// Normalise a citext value to its case-folded form for comparison purposes.
///
/// Applies Unicode lower-case mapping (`to_lowercase()`). Two citext values
/// that compare equal under PG citext semantics will produce identical strings
/// from this function.
///
/// # Examples
///
/// ```
/// use basin_common::types::citext::citext_fold;
/// assert_eq!(citext_fold("Hello"), citext_fold("hello"));
/// assert_eq!(citext_fold("FOO"), citext_fold("foo"));
/// ```
pub fn citext_fold(s: &str) -> String {
    s.to_lowercase()
}

/// Compare two citext values case-insensitively.
///
/// Returns the same ordering as comparing their case-folded forms.
pub fn citext_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    citext_fold(a).cmp(&citext_fold(b))
}

/// Return `true` if two citext values are equal under case-insensitive
/// comparison.
pub fn citext_eq(a: &str, b: &str) -> bool {
    citext_fold(a) == citext_fold(b)
}

/// Case-insensitive LIKE pattern matching for citext.
///
/// Converts both the value and the LIKE pattern to lower-case, then applies
/// standard SQL LIKE matching semantics:
/// - `%` matches any sequence of characters (including empty).
/// - `_` matches exactly one character.
/// - All other characters match themselves (case-insensitively via fold).
///
/// Returns `true` if `value` matches `pattern` under citext LIKE semantics.
pub fn citext_like(value: &str, pattern: &str) -> bool {
    let v = citext_fold(value);
    let p = citext_fold(pattern);
    sql_like_match(v.chars().collect::<Vec<_>>().as_slice(), p.as_str())
}

/// Recursive SQL LIKE matching on pre-folded Unicode chars.
fn sql_like_match(value: &[char], pattern: &str) -> bool {
    let pat_chars: Vec<char> = pattern.chars().collect();
    sql_like_match_inner(value, &pat_chars)
}

fn sql_like_match_inner(value: &[char], pattern: &[char]) -> bool {
    match (value, pattern) {
        ([], []) => true,
        (_, ['%', rest @ ..]) => {
            // '%' matches zero or more chars: try matching rest against every
            // suffix of value (including the empty suffix).
            if sql_like_match_inner(value, rest) {
                return true;
            }
            for i in 0..value.len() {
                if sql_like_match_inner(&value[i + 1..], rest) {
                    return true;
                }
            }
            false
        }
        ([_, v_rest @ ..], ['_', p_rest @ ..]) => sql_like_match_inner(v_rest, p_rest),
        ([v, v_rest @ ..], [p, p_rest @ ..]) if v == p => sql_like_match_inner(v_rest, p_rest),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_basic() {
        assert_eq!(citext_fold("Hello"), "hello");
        assert_eq!(citext_fold("FOO"), "foo");
        assert_eq!(citext_fold(""), "");
    }

    #[test]
    fn cmp_case_insensitive() {
        use std::cmp::Ordering;
        assert_eq!(citext_cmp("Foo", "foo"), Ordering::Equal);
        assert_eq!(citext_cmp("a", "B"), Ordering::Less);
        assert_eq!(citext_cmp("Z", "a"), Ordering::Greater);
    }

    #[test]
    fn eq_case_insensitive() {
        assert!(citext_eq("Foo", "FOO"));
        assert!(citext_eq("hello", "HELLO"));
        assert!(!citext_eq("foo", "bar"));
    }

    #[test]
    fn like_basic() {
        assert!(citext_like("Hello", "hello"));
        assert!(citext_like("Hello", "HELLO"));
        assert!(citext_like("Hello World", "hello%"));
        assert!(citext_like("foo", "%OO"));
        assert!(citext_like("bar", "b_r"));
        assert!(!citext_like("bar", "b__r"));
    }

    #[test]
    fn like_percent_only() {
        assert!(citext_like("anything", "%"));
        assert!(citext_like("", "%"));
    }

    #[test]
    fn like_no_match() {
        assert!(!citext_like("foo", "bar"));
        assert!(!citext_like("", "x"));
    }
}
