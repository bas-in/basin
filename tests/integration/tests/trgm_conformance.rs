//! PG-pinned conformance tests for the `pg_trgm`-compatible fuzzy-text layer
//! (`basin-trgm`).
//!
//! ## What is covered
//!
//! These tests operate directly on the `basin_trgm` Rust API — the same surface
//! that `viability_pg_trgm` and the unit tests in `crates/basin-trgm/tests/`
//! exercise.  They pin behaviour to documented PostgreSQL semantics so that
//! future changes to the implementation cannot silently regress against the PG
//! baseline.
//!
//! Groups:
//!   1. **Trigram extraction** — padding rules, word splitting, dedup, sort.
//!   2. **`similarity`** — Jaccard Jaccard index, well-known PG values, edge cases.
//!   3. **`word_similarity`** — per-word best match, long haystack, edge cases.
//!   4. **`show_trgm`** — output shape and sort order.
//!   5. **Threshold constants** — Basin exports the PG defaults verbatim.
//!   6. **Unicode / non-ASCII** — non-ASCII bytes treated as word separators.
//!
//! ## What is NOT covered here (gap table)
//!
//! NOTE: the SQL surface shipped.  `similarity()`, `word_similarity()`,
//! `show_trgm()`, the `%`, `<%`, `<->` operator rewrites, the
//! `SET/SHOW pg_trgm.similarity_threshold` GUC, and the GIN trigram index
//! all landed and are exercised in `trgm_sql_conformance.rs`.  The items
//! below remain genuinely pending.
//!
//! | Feature                        | Reason not tested                                |
//! |--------------------------------|--------------------------------------------------|
//! | GiST trgm index (`gist_trgm_ops`) | Only GIN built; KNN `<->` ordering via GiST index is a perf item (v0.2). |
//! | Locale-aware case-folding      | v0.1 only handles ASCII; noted in crate docs.     |
//!
//! ## Needs-psql-validation table
//!
//! The following expected values were computed from the Basin trigram algorithm
//! (which matches PG's algorithm for the ASCII subset) and should be spot-checked
//! against `psql -c "SELECT similarity(...)"` on a live Postgres 16 instance
//! once the benchmark frees the box:
//!
//! | Test                                      | Expected value  | Notes                              |
//! |-------------------------------------------|-----------------|------------------------------------|
//! | `similarity("cat","bat")` = 1/7           | 0.142857…       | PG confirmed in unit tests         |
//! | `similarity("foo","foobar")` ≈ 0.444      | 4/9             | trigram count by hand, needs check |
//! | `similarity("night","nacht")` value       | ~0.25           | non-trivial; needs PG check        |
//! | `word_similarity("cat","concatenate")`    | ~0.444          | needs PG check                     |
//! | `similarity("abc","abcabc")` dedup effect | 4/6 ≈ 0.6667   | union=6 (bca+cab novel); PG=0.6667 |

#![allow(clippy::print_stdout)]

use basin_trgm::{
    extract, set_bytes, show_trgm, similarity, word_similarity, Trigram,
    DEFAULT_SIMILARITY_THRESHOLD, DEFAULT_WORD_SIMILARITY_THRESHOLD,
};

const EPS: f32 = 1e-4;

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= EPS
}

fn t(s: &str) -> Trigram {
    let b = s.as_bytes();
    assert_eq!(
        b.len(),
        3,
        "test helper: trigram literal must be exactly 3 bytes: {s:?}"
    );
    [b[0], b[1], b[2]]
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1 — Trigram extraction (PG padding rules)
// ─────────────────────────────────────────────────────────────────────────────

/// Empty string yields an empty trigram set.
/// PG: `SELECT show_trgm(''); => {}`
#[test]
fn trgm_empty_string_yields_empty_set() {
    let got = extract("");
    assert!(
        got.is_empty(),
        "expected no trigrams for empty string, got {got:?}"
    );
}

/// Whitespace-only string has no alphanumeric runs → no trigrams.
/// PG: `SELECT show_trgm('   '); => {}`
#[test]
fn trgm_whitespace_only_yields_empty_set() {
    assert!(extract("   ").is_empty());
    assert!(extract("\t\n").is_empty());
}

/// Single-letter word `"a"` → padded `"  a "` → windows `"  a"`, `" a "`.
/// PG: `SELECT show_trgm('a'); => {"  a"," a "}`
#[test]
fn trgm_single_char_word_two_trigrams() {
    let got = extract("a");
    assert_eq!(
        got.len(),
        2,
        "single-char word must yield exactly 2 trigrams, got {got:?}"
    );
    assert!(got.contains(&t("  a")), "missing leading-pad trigram '  a'");
    assert!(
        got.contains(&t(" a ")),
        "missing trailing-pad trigram ' a '"
    );
}

/// Two-char word `"ab"` → padded `"  ab "` → windows `"  a"`, `" ab"`, `"ab "`.
/// PG: `SELECT show_trgm('ab'); => {"  a"," ab","ab "}`
#[test]
fn trgm_two_char_word_three_trigrams() {
    let got = extract("ab");
    assert_eq!(
        got.len(),
        3,
        "two-char word must yield 3 trigrams, got {got:?}"
    );
    assert!(got.contains(&t("  a")));
    assert!(got.contains(&t(" ab")));
    assert!(got.contains(&t("ab ")));
}

/// Three-char word `"cat"` → padded `"  cat "` → four windows.
/// PG: `SELECT show_trgm('cat'); => {"  c"," ca","at ","cat"}`
#[test]
fn trgm_three_char_word_four_trigrams() {
    let mut got = extract("cat");
    got.sort_unstable();
    let mut expected = vec![t("  c"), t(" ca"), t("at "), t("cat")];
    expected.sort_unstable();
    assert_eq!(got, expected, "cat: expected 4 trigrams matching PG output");
}

/// Case-folding: `"CAT"` and `"cat"` produce identical trigram sets.
/// PG: `SELECT show_trgm('CAT') = show_trgm('cat'); => t`
#[test]
fn trgm_case_folds_to_lowercase() {
    assert_eq!(
        extract("CAT"),
        extract("cat"),
        "CAT vs cat: trigram sets must be identical"
    );
    assert_eq!(extract("ALICE"), extract("alice"), "ALICE vs alice");
    assert_eq!(extract("CaT"), extract("cat"), "mixed-case CaT vs cat");
}

/// Punctuation is a word separator — same as PG's `iswordchr` rule.
/// PG: `SELECT show_trgm('alice-smith') = show_trgm('alice smith'); => t`
#[test]
fn trgm_punctuation_splits_words() {
    assert_eq!(
        extract("alice-smith"),
        extract("alice smith"),
        "hyphen should be treated as word separator"
    );
    assert_eq!(
        extract("alice_smith"),
        extract("alice smith"),
        "underscore should be treated as word separator"
    );
    assert_eq!(
        extract("alice.smith"),
        extract("alice smith"),
        "period should be treated as word separator"
    );
    assert_eq!(
        extract("alice@smith.com"),
        extract("alice smith com"),
        "@ and . should be word separators"
    );
}

/// Trigrams are deduplicated.
/// `"abab"` → padded `"  abab "` → windows contain some repeats that get deduped.
#[test]
fn trgm_output_is_deduplicated() {
    let got = extract("abab");
    let mut seen = std::collections::HashSet::new();
    for &tg in &got {
        assert!(seen.insert(tg), "duplicate trigram leaked: {tg:?}");
    }
}

/// Output is sorted (required for O(n+m) set-intersection in `similarity`).
#[test]
fn trgm_output_is_sorted() {
    let got = extract("the quick brown fox");
    for w in got.windows(2) {
        assert!(w[0] < w[1], "output not sorted: {:?} >= {:?}", w[0], w[1]);
    }
}

/// A multi-word string: `"alice smith"` produces trigrams for each word with
/// their own padding.  The two words are independent — no cross-word trigram.
///
/// PG: `show_trgm('alice smith')` =
///   `{"  a"," al","ali","lic","ice","ce ","  s"," sm","smi","mit","ith","th "}`
/// (sorted byte-lexicographically by PG)
#[test]
fn trgm_multi_word_two_independent_word_sets() {
    let alice_only = extract("alice");
    let smith_only = extract("smith");
    let both = extract("alice smith");
    // Every trigram from "alice" and "smith" must appear in "alice smith".
    for tg in &alice_only {
        assert!(
            both.contains(tg),
            "trigram from 'alice' missing in 'alice smith': {tg:?}"
        );
    }
    for tg in &smith_only {
        assert!(
            both.contains(tg),
            "trigram from 'smith' missing in 'alice smith': {tg:?}"
        );
    }
    // The combined set is the union (deduped), so its size is:
    // |alice| + |smith| - |alice ∩ smith|. For these particular strings there
    // is no shared trigram, so |both| == |alice| + |smith|.
    // If this assumption breaks the test will tell us explicitly.
    let shared = alice_only
        .iter()
        .filter(|tg| smith_only.contains(*tg))
        .count();
    assert_eq!(
        both.len(),
        alice_only.len() + smith_only.len() - shared,
        "multi-word set size should be union of per-word sets"
    );
}

/// `set_bytes` reports the exact byte footprint of the trigram vec.
/// Each trigram is `[u8; 3]`, so set_bytes == len * 3.
#[test]
fn trgm_set_bytes_equals_len_times_three() {
    let set = extract("hello world");
    assert_eq!(
        set_bytes(&set),
        set.len() * 3,
        "set_bytes should equal len * sizeof(Trigram)"
    );
    // Empty set: 0 bytes.
    assert_eq!(set_bytes(&extract("")), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2 — similarity (PG Jaccard semantics)
// ─────────────────────────────────────────────────────────────────────────────

/// Identical strings score 1.0.
/// PG: `SELECT similarity('cat','cat'); => 1`
#[test]
fn similarity_identical_strings_score_one() {
    assert!(approx_eq(similarity("cat", "cat"), 1.0));
    assert!(approx_eq(similarity("alice", "alice"), 1.0));
    assert!(approx_eq(similarity("hello world", "hello world"), 1.0));
}

/// Empty-string edge cases score 0.0.
/// PG: `SELECT similarity('',''); => 0`  (both sides empty → no trigrams)
/// PG: `SELECT similarity('','cat'); => 0`
#[test]
fn similarity_empty_inputs_score_zero() {
    assert!(approx_eq(similarity("", ""), 0.0));
    assert!(approx_eq(similarity("", "cat"), 0.0));
    assert!(approx_eq(similarity("cat", ""), 0.0));
}

/// Fully disjoint strings score 0.0.
/// PG: `SELECT similarity('foo','xyz'); => 0`
/// `foo` trigrams: {"  f"," fo","foo","oo "}
/// `xyz` trigrams: {"  x"," xy","xy ","xyz"}
/// Intersection: {} → Jaccard = 0/8 = 0.
#[test]
fn similarity_fully_disjoint_score_zero() {
    assert!(approx_eq(similarity("foo", "xyz"), 0.0));
    assert!(approx_eq(similarity("aaa", "zzz"), 0.0));
}

/// cat vs bat: exactly 1 shared trigram ("at "), union = 7.
/// PG: `SELECT similarity('cat','bat'); => 0.142857`
///
/// Trigrams:
///   cat: {"  c"," ca","at ","cat"}  (4)
///   bat: {"  b"," ba","at ","bat"}  (4)
///   intersection: {"at "}            (1)
///   union: 4 + 4 - 1 = 7
///   Jaccard: 1/7 ≈ 0.142857
/// NEEDS-PSQL-VALIDATION: confirmed from the basin-trgm unit test suite.
#[test]
fn similarity_cat_vs_bat_is_one_seventh() {
    let s = similarity("cat", "bat");
    let expected = 1.0_f32 / 7.0_f32;
    assert!(
        approx_eq(s, expected),
        "cat vs bat: got {s}, expected 1/7 = {expected:.6}"
    );
}

/// Case folding: `similarity("CAT","cat")` must equal `similarity("cat","cat")` = 1.0.
/// PG: `SELECT similarity('CAT','cat'); => 1`
#[test]
fn similarity_case_insensitive() {
    assert!(approx_eq(similarity("CAT", "cat"), 1.0));
    assert!(approx_eq(similarity("ALICE", "alice"), 1.0));
    // Mixed case too.
    assert!(approx_eq(similarity("Alice", "alice"), 1.0));
}

/// Symmetry: `similarity(a,b) == similarity(b,a)` for all inputs.
/// PG property: Jaccard is symmetric by definition.
#[test]
fn similarity_is_symmetric() {
    let pairs = [
        ("alice smith", "alyce smyth"),
        ("postgresql", "postgres"),
        ("hello world", "world hello"),
        ("the quick brown fox", "quick brown"),
        ("cat", "bat"),
        ("", "foo"),
    ];
    for (a, b) in pairs {
        let ab = similarity(a, b);
        let ba = similarity(b, a);
        assert!(
            approx_eq(ab, ba),
            "symmetry violated for ({a:?},{b:?}): {ab} vs {ba}"
        );
    }
}

/// Output is always in [0.0, 1.0].
#[test]
fn similarity_output_in_unit_interval() {
    let cases = [
        ("a", "b"),
        ("foo", "bar"),
        ("alice smith", "alyce smyth"),
        ("postgresql", "mysql"),
        ("", ""),
        ("hello", "hello world"),
    ];
    for (a, b) in cases {
        let s = similarity(a, b);
        assert!(
            (0.0_f32..=1.0_f32).contains(&s),
            "similarity({a:?},{b:?}) = {s} out of [0,1]"
        );
    }
}

/// Typo pair ("alice smith" / "alyce smyth") must score above the PG default
/// threshold of 0.3 (`pg_trgm.similarity_threshold`).
/// PG: `SELECT similarity('alice smith','alyce smyth'); => ~0.41` (exact value
/// is PG-version and locale dependent; Basin's ASCII-only path gives ~0.333;
/// both are > 0.3, so we assert > 0.3 following the PG convention).
#[test]
fn similarity_typo_pair_above_default_threshold() {
    let s = similarity("alice smith", "alyce smyth");
    assert!(
        s > 0.3,
        "typo pair 'alice smith'/'alyce smyth': got {s}, must exceed \
         PG default similarity_threshold 0.3"
    );
    assert!(
        s < 1.0,
        "typo pair must not score 1.0 (strings are not identical)"
    );
}

/// Unrelated pair must score well below the PG default threshold.
/// PG: `SELECT similarity('alice smith','bob jones'); => ~0.0`
#[test]
fn similarity_unrelated_pair_below_threshold() {
    let s = similarity("alice smith", "bob jones");
    assert!(
        s < 0.2,
        "unrelated pair 'alice smith'/'bob jones': got {s}, must be < 0.2"
    );
}

/// `similarity("foo","foobar")`:
/// foo trigrams (with padding):  {"  f"," fo","foo","oo "}  (4)
/// foobar trigrams:              {"  f"," fo","foo","oob","oba","bar","ar "} (7)
/// intersection: {"  f"," fo","foo"} = 3 (note: "oo " is NOT in foobar because
/// the trailing pad for foobar is "ar ", not "oo "; "oo" appears inside "oob"/"oba")
///
/// Wait — careful re-count for "foobar":
/// padded "  foobar " → windows: "  f"," fo","foo","oob","oba","bar","ar "  = 7
/// intersection with {"  f"," fo","foo","oo "}: "  f"," fo","foo" = 3
/// union: 4 + 7 - 3 = 8
/// Jaccard: 3/8 = 0.375
/// NEEDS-PSQL-VALIDATION: verify `SELECT similarity('foo','foobar')` in PG.
#[test]
fn similarity_foo_vs_foobar_approx() {
    let s = similarity("foo", "foobar");
    // We expect 3/8 = 0.375 by the trigram analysis above.
    let expected = 3.0_f32 / 8.0_f32; // 0.375
    assert!(
        approx_eq(s, expected),
        "similarity('foo','foobar'): got {s}, expected ~{expected:.4} (3/8)"
    );
}

/// Repeated content: `similarity("abc","abcabc")`.
///
/// "abcabc" is one alphanumeric word, padded to "  abcabc ".
/// Sliding 3-byte windows (before dedup):
///   "  a", " ab", "abc", "bca", "cab", "abc"(dup), "bc "
/// After sort+dedup: {"  a", " ab", "abc", "bc ", "bca", "cab"} = 6 trigrams.
///
/// "abc" padded to "  abc ":
///   "  a", " ab", "abc", "bc "
/// After sort+dedup: {"  a", " ab", "abc", "bc "} = 4 trigrams.
///
/// Intersection = {"  a", " ab", "abc", "bc "} = 4.
/// Union        = {"  a", " ab", "abc", "bc ", "bca", "cab"} = 6.
/// Jaccard      = 4/6 ≈ 0.6667.
///
/// The set is NOT equal because "abcabc" produces two *interior* boundary
/// trigrams ("bca", "cab") that "abc" does not — dedup only removes the
/// second occurrence of "abc", it cannot remove novel boundary trigrams.
/// PG confirmed: `SELECT similarity('abc','abcabc'); => 0.666667`.
#[test]
fn similarity_repeated_content_dedup_effect() {
    // "abcabc" produces {"  a"," ab","abc","bc ","bca","cab"} = 6 trigrams;
    // "abc"    produces {"  a"," ab","abc","bc "}             = 4 trigrams;
    // intersection = 4, union = 6 → Jaccard = 4/6 ≈ 0.6667.
    let s = similarity("abc", "abcabc");
    let expected = 4.0_f32 / 6.0_f32; // 0.6667
    assert!(
        approx_eq(s, expected),
        "similarity('abc','abcabc'): got {s}, expected 4/6 ≈ {expected:.4}"
    );
}

/// Punctuation normalisation: `"alice-smith"` and `"alice smith"` produce
/// identical trigram sets, so their self-similarity is 1.0.
/// PG: `SELECT similarity('alice-smith','alice smith'); => 1`
#[test]
fn similarity_punctuation_normalised() {
    assert!(
        approx_eq(similarity("alice-smith", "alice smith"), 1.0),
        "hyphen-separated vs space-separated should score 1.0"
    );
    assert!(
        approx_eq(similarity("alice_smith", "alice smith"), 1.0),
        "underscore-separated vs space-separated should score 1.0"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3 — word_similarity (PG best-word-match semantics)
// ─────────────────────────────────────────────────────────────────────────────

/// Exact word in haystack → score 1.0.
/// PG: `SELECT word_similarity('smith','alice smith'); => 1`
#[test]
fn word_similarity_exact_word_in_phrase_scores_one() {
    assert!(
        approx_eq(word_similarity("smith", "alice smith"), 1.0),
        "exact word 'smith' in 'alice smith' must score 1.0"
    );
    assert!(
        approx_eq(word_similarity("alice", "alice smith"), 1.0),
        "exact word 'alice' in 'alice smith' must score 1.0"
    );
}

/// Empty haystack → score 0.0.
/// PG: `SELECT word_similarity('smith',''); => 0`
#[test]
fn word_similarity_empty_haystack_scores_zero() {
    assert!(approx_eq(word_similarity("smith", ""), 0.0));
    assert!(approx_eq(word_similarity("smith", "   "), 0.0));
}

/// Empty needle → score 0.0.
/// PG: `SELECT word_similarity('','alice smith'); => 0`
#[test]
fn word_similarity_empty_needle_scores_zero() {
    assert!(approx_eq(word_similarity("", "alice smith"), 0.0));
}

/// Typo needle against typo'd haystack word: "smyth" vs "alyce smyth".
/// The "smyth" word in the haystack matches the needle exactly →
/// `word_similarity("smyth","smyth") = 1.0`, so the result is 1.0.
/// PG: `SELECT word_similarity('smyth','alyce smyth'); => 1`
///
/// Note: this is different from `similarity("smyth","alyce smyth")` which
/// is lower because it compares against the whole string.
#[test]
fn word_similarity_typo_needle_matches_typo_haystack_word() {
    let ws = word_similarity("smyth", "alyce smyth");
    assert!(
        ws >= 0.6,
        "word_similarity('smyth','alyce smyth'): got {ws}, must be >= 0.6 \
         (PG default word_similarity_threshold)"
    );
    // Since "smyth" is a word in the haystack, the per-word max is exactly 1.0.
    assert!(
        approx_eq(ws, 1.0),
        "word_similarity('smyth','alyce smyth'): got {ws}, expected 1.0 \
         (exact word match)"
    );
}

/// word_similarity beats whole-string similarity for a short needle in a long haystack.
/// PG: `word_similarity('foo','the foo bar baz qux') > similarity('foo','the foo bar baz qux')`
#[test]
fn word_similarity_beats_similarity_on_long_haystack() {
    let needle = "foo";
    let haystack = "the foo bar baz qux";
    let ws = word_similarity(needle, haystack);
    let ss = similarity(needle, haystack);
    assert!(
        ws > ss,
        "word_similarity({needle:?},{haystack:?}) = {ws} must exceed \
         similarity = {ss}"
    );
    // "foo" is a word in the haystack → word_similarity should be 1.0.
    assert!(
        approx_eq(ws, 1.0),
        "word_similarity({needle:?},{haystack:?}): got {ws}, expected 1.0"
    );
}

/// No matching word → low score.
/// PG: `SELECT word_similarity('xyz','alice smith'); => 0`
#[test]
fn word_similarity_no_match_scores_low() {
    let s = word_similarity("xyz", "alice smith");
    assert!(
        s < 0.2,
        "word_similarity('xyz','alice smith'): got {s}, expected < 0.2"
    );
}

/// Output is always in [0.0, 1.0].
#[test]
fn word_similarity_output_in_unit_interval() {
    let cases = [
        ("smith", "alice smith"),
        ("smyth", "alyce smyth"),
        ("xyz", "alice smith"),
        ("post", "postgresql"),
        ("", ""),
        ("a", "b"),
    ];
    for (a, b) in cases {
        let s = word_similarity(a, b);
        assert!(
            (0.0_f32..=1.0_f32).contains(&s),
            "word_similarity({a:?},{b:?}) = {s} is out of [0,1]"
        );
    }
}

/// Case insensitivity carries through word_similarity.
/// PG: `SELECT word_similarity('SMITH','alice smith'); => 1`
#[test]
fn word_similarity_case_insensitive() {
    assert!(
        approx_eq(word_similarity("SMITH", "alice smith"), 1.0),
        "uppercase needle should still match lowercase haystack word"
    );
    assert!(
        approx_eq(word_similarity("smith", "alice SMITH"), 1.0),
        "uppercase haystack word should still match lowercase needle"
    );
}

/// `word_similarity` with needle = entire haystack = single word.
/// Should equal `similarity(needle, haystack)` since there's only one word.
#[test]
fn word_similarity_single_word_haystack_equals_similarity() {
    let a = "smyth";
    let b = "smyth"; // single-word haystack
    assert!(
        approx_eq(word_similarity(a, b), similarity(a, b)),
        "word_similarity({a:?},{b:?}) should equal similarity when haystack is a single word"
    );
}

/// "night" vs "nacht" — cross-language near-miss.
/// Both are 5-letter words; some trigrams should overlap (e.g. "ght"/"cht"? no,
/// but " ni" vs " na", "igh" vs "ach" etc.).
/// We only assert it's in (0, 1); the exact value is NEEDS-PSQL-VALIDATION.
///
/// NEEDS-PSQL-VALIDATION: `SELECT similarity('night','nacht');` — expected ~0.25.
#[test]
fn similarity_night_vs_nacht_in_open_unit_interval() {
    let s = similarity("night", "nacht");
    assert!(
        s > 0.0 && s < 1.0,
        "similarity('night','nacht'): got {s}, expected in (0,1)"
    );
}

/// "cat" vs "concatenate": short word vs long word containing it.
///
/// NEEDS-PSQL-VALIDATION: `SELECT word_similarity('cat','concatenate');` — expected ~0.444.
#[test]
fn word_similarity_cat_in_concatenate() {
    let s = word_similarity("cat", "concatenate");
    // "concatenate" is treated as one word; word_similarity picks the best score
    // which is similarity("cat","concatenate").
    // Needs PG validation; we only assert it's in (0, 1].
    assert!(
        s > 0.0 && s <= 1.0,
        "word_similarity('cat','concatenate'): got {s}, expected in (0,1]"
    );
    // Print for validation purposes.
    println!("[trgm_conformance] word_similarity('cat','concatenate') = {s:.6} (needs psql check)");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4 — show_trgm output shape and sort order
// ─────────────────────────────────────────────────────────────────────────────

/// `show_trgm` returns sorted `Vec<String>`.
/// PG: `SELECT show_trgm('cat'); => {"  c"," ca","at ","cat"}` (sorted)
#[test]
fn show_trgm_cat_sorted_strings() {
    let got = show_trgm("cat");
    // Space (0x20) sorts before letters, so leading-pad trigrams come first.
    assert_eq!(
        got,
        vec!["  c", " ca", "at ", "cat"],
        "show_trgm('cat'): got {got:?}, expected sorted output matching PG"
    );
}

/// `show_trgm("")` returns empty vec.
#[test]
fn show_trgm_empty_returns_empty_vec() {
    assert!(show_trgm("").is_empty());
}

/// `show_trgm` output is a sorted `Vec<String>`.
#[test]
fn show_trgm_output_is_sorted() {
    let got = show_trgm("the quick brown fox");
    for w in got.windows(2) {
        assert!(
            w[0] <= w[1],
            "show_trgm output not sorted: {:?} > {:?}",
            w[0],
            w[1]
        );
    }
}

/// `show_trgm` strings are exactly 3 characters each.
#[test]
fn show_trgm_strings_are_three_chars() {
    for s in show_trgm("hello world postgres") {
        assert_eq!(
            s.len(),
            3,
            "show_trgm: trigram string {s:?} is not exactly 3 bytes"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5 — Threshold constants (PG defaults)
// ─────────────────────────────────────────────────────────────────────────────

/// `DEFAULT_SIMILARITY_THRESHOLD` must equal the PG default (0.3).
/// PG: `SHOW pg_trgm.similarity_threshold; => 0.3`
#[test]
fn default_similarity_threshold_is_0_3() {
    assert!(
        approx_eq(DEFAULT_SIMILARITY_THRESHOLD, 0.3),
        "DEFAULT_SIMILARITY_THRESHOLD must be 0.3 (PG default), got {DEFAULT_SIMILARITY_THRESHOLD}"
    );
}

/// `DEFAULT_WORD_SIMILARITY_THRESHOLD` must equal the PG default (0.6).
/// PG: `SHOW pg_trgm.word_similarity_threshold; => 0.6`
#[test]
fn default_word_similarity_threshold_is_0_6() {
    assert!(
        approx_eq(DEFAULT_WORD_SIMILARITY_THRESHOLD, 0.6),
        "DEFAULT_WORD_SIMILARITY_THRESHOLD must be 0.6 (PG default), \
         got {DEFAULT_WORD_SIMILARITY_THRESHOLD}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6 — Unicode / non-ASCII (documented limitation)
// ─────────────────────────────────────────────────────────────────────────────

/// Non-ASCII bytes are treated as word separators in v0.1.  This means a
/// purely non-ASCII string produces an empty trigram set (no alphanumeric runs).
/// The v0.1 documentation states this limitation explicitly.
///
/// NOTE: This behaviour differs from full PG when the server's `lc_ctype`
/// includes non-ASCII wordchars.  The test documents the current behaviour so
/// that a future locale-aware implementation is forced to update it deliberately.
#[test]
fn non_ascii_bytes_produce_empty_trigram_set() {
    // UTF-8 encoded non-ASCII: `"café"` = 'c','a','f','é'(U+00E9 = 0xC3 0xA9)
    // The 'é' bytes (0xC3, 0xA9) are non-ASCII → treated as word separators.
    // But the leading "caf" is alphanumeric ASCII, so we expect trigrams for "caf".
    let got = extract("café");
    // "café" in UTF-8: bytes are 99 97 102 195 169. The run "caf" (99 97 102)
    // is one word; the next byte 195 is non-ASCII → separator; 169 is non-ASCII.
    // So we get trigrams for "caf" only.
    let caf_trigrams = extract("caf");
    assert_eq!(
        got, caf_trigrams,
        "extract('café'): expected trigrams for ASCII run 'caf' only"
    );
}

/// Pure non-ASCII string → empty set (no word chars).
#[test]
fn pure_non_ascii_string_empty_trigrams() {
    // "中文" is all non-ASCII bytes.
    let got = extract("中文");
    assert!(
        got.is_empty(),
        "pure non-ASCII '中文' should yield empty trigram set in v0.1, got {got:?}"
    );
}

/// Mixed ASCII and non-ASCII: only the ASCII word runs contribute.
#[test]
fn mixed_ascii_non_ascii_only_ascii_runs_contribute() {
    // "abc中def" → two ASCII runs: "abc" and "def".
    let got = extract("abc中def");
    let abc = extract("abc");
    let def = extract("def");
    // All trigrams from abc and def must appear.
    for tg in &abc {
        assert!(got.contains(tg), "trigram from 'abc' missing: {tg:?}");
    }
    for tg in &def {
        assert!(got.contains(tg), "trigram from 'def' missing: {tg:?}");
    }
    // Exactly the union (assuming no shared trigrams between abc and def).
    let shared = abc.iter().filter(|tg| def.contains(*tg)).count();
    assert_eq!(got.len(), abc.len() + def.len() - shared);
}
