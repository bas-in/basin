//! `(I)LIKE` candidate-pruning tests for [`TrigramGinIndex`].
//!
//! The load-bearing invariant is *no false negatives*: every row that
//! truly matches the pattern must appear in `candidates`. The tests pin
//! this against a brute-force `LIKE` matcher over the same corpus for a
//! battery of pattern shapes (exact, leading-`%`, trailing-`%`, middle,
//! short-pattern fallback, no-match) under both `LIKE` and `ILIKE`.

use std::collections::HashSet;

use basin_trgm::gin_like::{trigrams_for_pattern, Candidates, RowId, TrigramGinIndex};

/// Minimal SQL `LIKE` matcher (no escapes) for the brute-force baseline.
/// `case_insensitive = true` is the `ILIKE` behaviour.
fn like_matches(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let (p, t) = if case_insensitive {
        (pattern.to_ascii_lowercase(), text.to_ascii_lowercase())
    } else {
        (pattern.to_string(), text.to_string())
    };
    like_inner(p.as_bytes(), t.as_bytes())
}

fn like_inner(pat: &[u8], txt: &[u8]) -> bool {
    // Classic recursive `%`/`_` matcher.
    match pat.first() {
        None => txt.is_empty(),
        Some(b'%') => {
            // `%` matches zero or more chars: try every split.
            if like_inner(&pat[1..], txt) {
                return true;
            }
            (0..txt.len()).any(|i| like_inner(pat, &txt[i + 1..]))
        }
        Some(b'_') => !txt.is_empty() && like_inner(&pat[1..], &txt[1..]),
        Some(&c) => !txt.is_empty() && txt[0] == c && like_inner(&pat[1..], &txt[1..]),
    }
}

const CORPUS: &[(RowId, &str)] = &[
    (1, "alice@gmail.com"),
    (2, "bob@gmail.com"),
    (3, "carol@yahoo.com"),
    (4, "dave@GMAIL.COM"),
    (5, "Eve@Gmail.Com"),
    (6, "frank@hotmail.com"),
    (7, "grace@gmail.org"),
    (8, "heidi@protonmail.com"),
    (9, "ivan@gmail.com.evil.net"),
    (10, "judy"),
    (11, "mallory@example.com"),
    (12, "GMAILER@gmail.com"),
];

fn build_idx() -> TrigramGinIndex {
    TrigramGinIndex::build(CORPUS.iter().map(|(id, t)| (*id, *t)))
}

/// Materialise the brute-force matching row-set over `corpus`.
fn brute_force(corpus: &[(RowId, &str)], pattern: &str, case_insensitive: bool) -> HashSet<RowId> {
    corpus
        .iter()
        .filter(|(_, t)| like_matches(pattern, t, case_insensitive))
        .map(|(id, _)| *id)
        .collect()
}

/// Assert the candidate set is a *superset* of the true matches (the
/// core no-false-negatives invariant), resolving `All` against the
/// universe. `corpus` must be the exact set of rows in `idx`.
fn assert_superset(
    idx: &TrigramGinIndex,
    corpus: &[(RowId, &str)],
    pattern: &str,
    case_insensitive: bool,
) {
    let truth = brute_force(corpus, pattern, case_insensitive);
    let cands = idx
        .candidates(pattern, case_insensitive)
        .resolve(idx.universe());
    for row in &truth {
        assert!(
            cands.contains(row),
            "pattern {pattern:?} (ci={case_insensitive}): row {row} truly matches but was pruned. \
             candidates={cands:?}, truth={truth:?}"
        );
    }
}

// ---------------------------------------------------------------------
// trigrams_for_pattern
// ---------------------------------------------------------------------

fn tg(s: &str) -> [u8; 3] {
    let b = s.as_bytes();
    assert_eq!(b.len(), 3, "trigram literal must be 3 bytes: {s:?}");
    [b[0], b[1], b[2]]
}

#[test]
fn pattern_trigrams_exact_run() {
    // '%@gmail.com' → run "@gmail.com" → contiguous 3-windows, no padding.
    let got = trigrams_for_pattern("%@gmail.com", true);
    assert!(got.contains(&tg("@gm")));
    assert!(got.contains(&tg("gma")));
    assert!(got.contains(&tg("mai")));
    assert!(got.contains(&tg("ail")));
    assert!(got.contains(&tg("il.")));
    assert!(got.contains(&tg("l.c")));
    assert!(got.contains(&tg(".co")));
    assert!(got.contains(&tg("com")));
    // No word-boundary padding trigrams (those would start with a space).
    assert!(
        got.iter().all(|t| t[0] != b' ' && t[2] != b' '),
        "pattern trigrams must not be space-padded: {got:?}"
    );
}

#[test]
fn pattern_trigrams_lowercase_for_ilike() {
    // ILIKE lowercases; LIKE keeps case.
    let ci = trigrams_for_pattern("%GMAIL%", true);
    let cs = trigrams_for_pattern("%GMAIL%", false);
    assert!(ci.contains(&tg("gma")));
    assert!(!ci.contains(&tg("GMA")));
    assert!(cs.contains(&tg("GMA")));
    assert!(!cs.contains(&tg("gma")));
}

#[test]
fn pattern_trigrams_multiple_runs() {
    // '%foo%bar%' → two runs, each ≥3 → trigrams from both.
    let got = trigrams_for_pattern("%foo%bar%", false);
    assert!(got.contains(&tg("foo")));
    assert!(got.contains(&tg("bar")));
    // Sorted + deduped.
    for w in got.windows(2) {
        assert!(w[0] < w[1], "not strictly sorted/deduped: {got:?}");
    }
}

#[test]
fn pattern_trigrams_short_runs_yield_nothing() {
    // Runs shorter than 3 chars give no trigrams.
    assert!(trigrams_for_pattern("%a%", false).is_empty());
    assert!(trigrams_for_pattern("%ab%", false).is_empty());
    assert!(trigrams_for_pattern("_b_", false).is_empty());
    assert!(trigrams_for_pattern("%", false).is_empty());
    assert!(trigrams_for_pattern("", false).is_empty());
}

#[test]
fn pattern_trigrams_underscore_splits_runs() {
    // '_' is a single-char wildcard and breaks the literal run, so
    // "ab_cd" has runs "ab" and "cd" — both too short → nothing.
    assert!(trigrams_for_pattern("ab_cd", false).is_empty());
    // But "abc_def" has two runs of length 3 each.
    let got = trigrams_for_pattern("abc_def", false);
    assert!(got.contains(&tg("abc")));
    assert!(got.contains(&tg("def")));
}

#[test]
fn pattern_trigrams_escape_treats_wildcard_as_literal() {
    // '\%' is a literal percent; "a\%bc" is the run "a%bc".
    let got = trigrams_for_pattern("a\\%bc", false);
    assert!(got.contains(&tg("a%b")));
    assert!(got.contains(&tg("%bc")));
    // Trailing backslash escapes nothing and must not panic.
    let _ = trigrams_for_pattern("abc\\", false);
}

// ---------------------------------------------------------------------
// TrigramGinIndex::candidates
// ---------------------------------------------------------------------

#[test]
fn candidates_trailing_wildcard_anchored_prefix() {
    // 'alice@%' — prefix match. Only row 1 truly matches.
    let idx = build_idx();
    assert_superset(&idx, CORPUS, "alice@%", true);
    let cands = idx.candidates("alice@gmail%", true).resolve(idx.universe());
    assert!(cands.contains(&1));
    // Yahoo/hotmail rows must be pruned out.
    assert!(!cands.contains(&3));
    assert!(!cands.contains(&6));
}

#[test]
fn candidates_leading_wildcard_suffix() {
    // '%@gmail.com' — the canonical case. ILIKE catches mixed-case rows.
    let idx = build_idx();
    assert_superset(&idx, CORPUS, "%@gmail.com", true);
    let cands = idx.candidates("%@gmail.com", true).resolve(idx.universe());
    // True ILIKE matches: rows 1,2,4,5,12 (mixed case, exact suffix).
    for r in [1u64, 2, 4, 5, 12] {
        assert!(
            cands.contains(&r),
            "row {r} should be a candidate: {cands:?}"
        );
    }
    // yahoo / hotmail / proton / gmail.org / judy are not matches; the
    // index should prune at least the clearly-distinct ones.
    assert!(!cands.contains(&3), "yahoo pruned");
    assert!(!cands.contains(&6), "hotmail pruned");
    assert!(!cands.contains(&10), "judy pruned");
}

#[test]
fn candidates_middle_wildcard() {
    // '%@gmail%' — contains "@gmail" anywhere.
    let idx = build_idx();
    assert_superset(&idx, CORPUS, "%@gmail%", true);
    let cands = idx.candidates("%@gmail%", true).resolve(idx.universe());
    // Rows containing "@gmail" (case-insensitive): 1,2,4,5,7,9,12.
    for r in [1u64, 2, 4, 5, 7, 9, 12] {
        assert!(
            cands.contains(&r),
            "row {r} should be a candidate: {cands:?}"
        );
    }
    assert!(!cands.contains(&3));
    assert!(!cands.contains(&6));
    assert!(!cands.contains(&8));
}

#[test]
fn candidates_exact_no_wildcards() {
    // Exact literal pattern (no wildcards) is one big run.
    let idx = build_idx();
    assert_superset(&idx, CORPUS, "alice@gmail.com", true);
    let cands = idx
        .candidates("alice@gmail.com", true)
        .resolve(idx.universe());
    assert!(cands.contains(&1));
    assert!(!cands.contains(&2));
}

#[test]
fn candidates_case_insensitivity() {
    // ILIKE must catch dave@GMAIL.COM and Eve@Gmail.Com for '%@gmail.com'.
    let idx = build_idx();
    let ci = idx.candidates("%@gmail.com", true).resolve(idx.universe());
    assert!(ci.contains(&4), "ILIKE should catch uppercase GMAIL.COM");
    assert!(ci.contains(&5), "ILIKE should catch mixed-case Gmail.Com");
}

#[test]
fn candidates_short_pattern_falls_back_to_all() {
    // No literal run ≥ 3 → cannot prune → All.
    let idx = build_idx();
    assert_eq!(idx.candidates("%a%", true), Candidates::All);
    assert_eq!(idx.candidates("_b_", false), Candidates::All);
    assert_eq!(idx.candidates("%", true), Candidates::All);
    assert_eq!(idx.candidates("", false), Candidates::All);
    // Resolving All yields the whole universe.
    let all = idx.candidates("%a%", true).resolve(idx.universe());
    assert_eq!(all.len(), idx.len());
}

#[test]
fn candidates_no_match_returns_empty_set() {
    // A pattern whose required trigram never appears → empty (not All).
    let idx = build_idx();
    let cands = idx.candidates("%zzzqqq%", true);
    match cands {
        Candidates::Some(set) => assert!(set.is_empty(), "expected empty, got {set:?}"),
        Candidates::All => panic!("a present trigram constraint must not fall back to All"),
    }
}

#[test]
fn candidates_empty_index() {
    let idx = TrigramGinIndex::new();
    assert!(idx.is_empty());
    // Prunable pattern over empty index → empty set.
    match idx.candidates("%@gmail.com%", true) {
        Candidates::Some(s) => assert!(s.is_empty()),
        Candidates::All => panic!("empty index should not say All for a prunable pattern"),
    }
    // Non-prunable still says All.
    assert_eq!(idx.candidates("%a%", true), Candidates::All);
}

#[test]
fn candidates_brute_force_superset_battery() {
    // Sweep many patterns and pin the no-false-negatives invariant for
    // both LIKE and ILIKE.
    let idx = build_idx();
    let patterns = [
        "%@gmail.com",
        "%@gmail%",
        "alice@%",
        "%gmail.com",
        "%@gmail.com%",
        "GMAIL%",
        "%mail%",
        "%.com",
        "alice@gmail.com",
        "%example%",
    ];
    for p in patterns {
        assert_superset(&idx, CORPUS, p, true);
        assert_superset(&idx, CORPUS, p, false);
    }
}

#[test]
fn candidates_actually_prunes_not_just_all() {
    // Guard against a trivially-correct-but-useless implementation that
    // returns the whole universe: the canonical query must prune strictly
    // below the corpus size.
    let idx = build_idx();
    match idx.candidates("%@gmail.com", true) {
        Candidates::Some(set) => {
            assert!(
                set.len() < idx.len(),
                "expected real pruning, got all {} rows",
                set.len()
            );
            assert!(!set.is_empty(), "but should still have the true matches");
        }
        Candidates::All => panic!("a long literal run must prune, not fall back to All"),
    }
}

#[test]
fn remove_drops_row_from_candidates() {
    let mut idx = build_idx();
    let before = idx.candidates("%@gmail.com", true).resolve(idx.universe());
    assert!(before.contains(&1));
    idx.remove(1);
    let after = idx.candidates("%@gmail.com", true).resolve(idx.universe());
    assert!(!after.contains(&1), "removed row must not be a candidate");
    // Removing a non-existent row is a no-op.
    idx.remove(9999);
}

#[test]
fn candidates_handles_unicode_without_panic() {
    // Non-ASCII bytes flow through as raw windows; must not panic and
    // must not drop true matches on the ASCII portion.
    let corpus: &[(RowId, &str)] = &[
        (1, "café@gmail.com"),
        (2, "日本語@gmail.com"),
        (3, "plain@yahoo.com"),
    ];
    let idx = TrigramGinIndex::build(corpus.iter().map(|(id, t)| (*id, *t)));
    assert_superset(&idx, corpus, "%@gmail.com", true);
    let cands = idx.candidates("%@gmail.com", true).resolve(idx.universe());
    assert!(cands.contains(&1));
    assert!(cands.contains(&2));
    assert!(!cands.contains(&3));
}
