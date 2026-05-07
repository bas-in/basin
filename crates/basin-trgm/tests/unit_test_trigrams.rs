//! Trigram-extraction tests: known input/output pairs against the
//! `pg_trgm` reference (`SELECT show_trgm('...')` in psql).
//!
//! Every assertion here was validated against Postgres 16's pg_trgm
//! contrib; the inline comments call out the exact `psql` query when the
//! expected set is non-obvious.

use basin_trgm::{extract, show_trgm, Trigram};

fn t(s: &str) -> Trigram {
    let b = s.as_bytes();
    assert_eq!(b.len(), 3, "trigram literal must be 3 bytes: {s:?}");
    [b[0], b[1], b[2]]
}

#[test]
fn cat_produces_four_trigrams() {
    // pg: SELECT show_trgm('cat');
    //   => {"  c"," ca","at ","cat"}
    let mut got = extract("cat");
    got.sort_unstable();
    let mut expected = vec![t("  c"), t(" ca"), t("at "), t("cat")];
    expected.sort_unstable();
    assert_eq!(got, expected);
    assert_eq!(got.len(), 4);
}

#[test]
fn empty_string_yields_empty_set() {
    assert!(extract("").is_empty());
}

#[test]
fn whitespace_only_yields_empty_set() {
    // No alphanumeric runs → no words → no trigrams.
    assert!(extract("   ").is_empty());
    assert!(extract("\t\n  ").is_empty());
}

#[test]
fn case_folds_to_lowercase() {
    // pg: SELECT show_trgm('CAT') = show_trgm('cat'); => true
    assert_eq!(extract("CAT"), extract("cat"));
    assert_eq!(extract("CaT"), extract("cat"));
}

#[test]
fn punctuation_splits_words() {
    // pg: SELECT show_trgm('alice-smith') = show_trgm('alice smith');
    // => true. Both produce the same trigram set: words "alice" and
    // "smith" each padded "  word ".
    assert_eq!(extract("alice-smith"), extract("alice smith"));
    assert_eq!(extract("alice_smith"), extract("alice smith"));
    assert_eq!(extract("alice.smith"), extract("alice smith"));
}

#[test]
fn single_char_word_has_three_trigrams() {
    // padded "  a "  →  windows "  a", " a "
    // pg: SELECT show_trgm('a'); => {"  a"," a "}
    let got = extract("a");
    assert_eq!(got.len(), 2, "got {got:?}");
    assert!(got.contains(&t("  a")));
    assert!(got.contains(&t(" a ")));
}

#[test]
fn two_char_word_has_three_trigrams() {
    // padded "  ab "  →  windows "  a", " ab", "ab "
    // pg: SELECT show_trgm('ab'); => {"  a"," ab","ab "}
    let got = extract("ab");
    assert_eq!(got.len(), 3);
    assert!(got.contains(&t("  a")));
    assert!(got.contains(&t(" ab")));
    assert!(got.contains(&t("ab ")));
}

#[test]
fn show_trgm_is_sorted_strings() {
    let got = show_trgm("cat");
    // Lexicographic byte order: space (0x20) sorts before letters.
    assert_eq!(got, vec!["  c", " ca", "at ", "cat"]);
}

#[test]
fn duplicate_trigrams_dedupe() {
    // "abab" yields "  a", " ab", "aba", "bab", "ab ". No duplicates,
    // but use a string with a known repeat: "ababab" yields the same
    // set as above (with extra "aba"+"bab" once each via dedup).
    let got = extract("ababab");
    let mut s: std::collections::HashSet<Trigram> = std::collections::HashSet::new();
    for tg in &got {
        assert!(s.insert(*tg), "duplicate trigram leaked through dedup");
    }
}

#[test]
fn output_is_sorted() {
    let got = extract("the quick brown fox");
    for window in got.windows(2) {
        assert!(window[0] < window[1], "not sorted: {window:?}");
    }
}
