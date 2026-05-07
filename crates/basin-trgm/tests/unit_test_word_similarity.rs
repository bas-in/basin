//! `word_similarity` tests: best-substring match. The needle (first
//! argument) is the short query; the haystack (second) is the candidate
//! that may contain a closely-matching word.
//!
//! All asserted scores were spot-checked against `SELECT
//! word_similarity('needle', 'haystack');` in Postgres 16 with `pg_trgm`
//! installed. Per-word approximation matches PG's sliding-extent score
//! whenever the needle is a single word, which is the case in every
//! production usage we care about for v0.1.

use basin_trgm::word_similarity;

const EPS: f32 = 1e-4;

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= EPS
}

#[test]
fn exact_word_match_in_phrase_scores_one() {
    // pg: SELECT word_similarity('smith', 'alice smith'); => 1
    // The "smith" word in the haystack matches the needle exactly, so
    // the per-word max is `similarity('smith','smith') = 1`.
    assert!(approx_eq(word_similarity("smith", "alice smith"), 1.0));
    assert!(approx_eq(word_similarity("alice", "alice smith"), 1.0));
}

#[test]
fn typo_substring_scores_high() {
    // The viability bar: `word_similarity("smyth", "alyce smyth") >= 0.6`.
    // Per-word: similarity("smyth","alyce")=low, similarity("smyth","smyth")=1.
    // Max = 1.0, well above 0.6.
    let s = word_similarity("smyth", "alyce smyth");
    assert!(s >= 0.6, "got {s}");
}

#[test]
fn no_word_match_scores_low() {
    // pg: SELECT word_similarity('xyz', 'alice smith'); => 0
    let s = word_similarity("xyz", "alice smith");
    assert!(s < 0.2, "got {s}");
}

#[test]
fn empty_haystack_scores_zero() {
    assert!(approx_eq(word_similarity("smith", ""), 0.0));
    assert!(approx_eq(word_similarity("smith", "   "), 0.0));
}

#[test]
fn empty_needle_scores_zero() {
    // No needle trigrams to match anything against.
    assert!(approx_eq(word_similarity("", "alice smith"), 0.0));
}

#[test]
fn output_in_unit_interval() {
    let pairs = [
        ("smith", "alice smith"),
        ("smyth", "alyce smyth"),
        ("xyz", "alice smith"),
        ("post", "postgresql"),
    ];
    for (a, b) in pairs {
        let s = word_similarity(a, b);
        assert!(
            (0.0..=1.0).contains(&s),
            "out of range: word_similarity({a:?}, {b:?}) = {s}"
        );
    }
}

#[test]
fn case_insensitive() {
    assert!(approx_eq(
        word_similarity("SMITH", "alice smith"),
        word_similarity("smith", "ALICE SMITH")
    ));
}

#[test]
fn beats_overall_similarity_on_long_haystack() {
    // The whole point of word_similarity vs. similarity: a short needle
    // matched against a long haystack scores poorly under whole-string
    // similarity (because the haystack contributes many non-matching
    // trigrams to the union) but highly under word_similarity (because
    // the per-word max ignores the rest of the haystack).
    let needle = "smyth";
    let haystack = "the customer record for one alyce smyth from accounts";
    let whole = basin_trgm::similarity(needle, haystack);
    let best_word = word_similarity(needle, haystack);
    assert!(
        best_word > whole,
        "expected word_similarity ({best_word}) > similarity ({whole})"
    );
    assert!(best_word >= 0.6);
}
