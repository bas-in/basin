//! `similarity` and `word_similarity` — the public scoring functions.
//!
//! Both functions are pure, allocation-light, and order-insensitive:
//! `similarity(a, b) == similarity(b, a)` to within the floating-point
//! noise of two sequential `f32` divisions.
//!
//! ## Algorithms
//!
//! - **`similarity`** is the classical Jaccard index over trigram sets:
//!   `|T(a) ∩ T(b)| / |T(a) ∪ T(b)|`. Postgres reports this as a `real`
//!   (`f32`) and clamps the empty-input case to `0.0`.
//!
//! - **`word_similarity`** approximates Postgres's "best matching
//!   continuous extent of `b`" by iterating words of `b`, computing
//!   `similarity(a, word)`, and returning the max. The formal Postgres
//!   definition slides a trigram-extent window over `b` rather than
//!   splitting on words; the per-word approximation produces identical
//!   scores when `a` is itself a single word (the overwhelmingly common
//!   case) and a slightly higher floor when `a` spans multiple words.
//!   We document the discrepancy here rather than ship a half-correct
//!   sliding extent; the v0.2 GIN-style index will land alongside the
//!   sliding-extent variant.

use crate::trigram;

/// `similarity(a, b)` — trigram Jaccard. Range `[0.0, 1.0]`. Symmetric.
///
/// Returns `0.0` when both inputs have no trigrams (e.g. both empty,
/// or both whitespace-only). Returns `0.0` when one side has trigrams
/// and the other doesn't, matching Postgres.
pub fn similarity(a: &str, b: &str) -> f32 {
    let ta = trigram::extract(a);
    let tb = trigram::extract(b);
    similarity_of_sets(&ta, &tb)
}

/// `word_similarity(needle, haystack)` — best per-word trigram match.
///
/// "needle" matches `pg_trgm`'s notion of the first argument: the short
/// query string ("smyth"). "haystack" is the long candidate ("alyce
/// smyth"). Returns the max of `similarity(needle, word)` over each
/// alphanumeric run in `haystack`.
///
/// Returns `0.0` when `haystack` has no words.
pub fn word_similarity(needle: &str, haystack: &str) -> f32 {
    let needle_set = trigram::extract(needle);
    if needle_set.is_empty() {
        return 0.0;
    }
    let mut best = 0.0_f32;
    let bytes = haystack.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && !bytes[i].is_ascii_alphanumeric() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
            i += 1;
        }
        // SAFETY: ASCII subslice of original UTF-8 string is valid UTF-8.
        let word = std::str::from_utf8(&bytes[start..i]).unwrap_or("");
        let word_set = trigram::extract(word);
        let score = similarity_of_sets(&needle_set, &word_set);
        if score > best {
            best = score;
        }
    }
    best
}

/// Compute Jaccard between two **sorted, deduped** trigram sets.
fn similarity_of_sets(a: &[trigram::Trigram], b: &[trigram::Trigram]) -> f32 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let intersect = trigram::intersect_count(a, b);
    let union = a.len() + b.len() - intersect;
    if union == 0 {
        return 0.0;
    }
    intersect as f32 / union as f32
}
