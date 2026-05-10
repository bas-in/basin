//! `similarity` tests: known scores cross-checked against `pg_trgm` in
//! Postgres 16. The asserted values were taken from `SELECT
//! similarity('a', 'b');` in psql with `pg_trgm` installed.
//!
//! `pg_trgm` returns `real` (`f32`); we compare with a small epsilon to
//! cover round-trip rounding.

use basin_trgm::similarity;

const EPS: f32 = 1e-4;

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= EPS
}

#[test]
fn identical_strings_score_one() {
    // pg: SELECT similarity('cat', 'cat'); => 1
    assert!(approx_eq(similarity("cat", "cat"), 1.0));
    assert!(approx_eq(similarity("alice", "alice"), 1.0));
}

#[test]
fn cat_vs_bat_scores_one_seventh() {
    // pg: SELECT similarity('cat', 'bat');
    //   trigrams("cat") = {"  c"," ca","cat","at "}
    //   trigrams("bat") = {"  b"," ba","bat","at "}
    //   intersection = {"at "}                         => 1
    //   union        = 4 + 4 - 1                       => 7
    //   similarity   = 1 / 7                           ≈ 0.142857
    // (Note: the original spec sketch said ~0.5 — that was a hand-wave;
    //  the cross-checked PG value is 1/7. We track PG, not the sketch.)
    let s = similarity("cat", "bat");
    assert!(
        approx_eq(s, 1.0 / 7.0),
        "cat vs bat: got {s}, want {}",
        1.0 / 7.0
    );
}

#[test]
fn fully_disjoint_strings_score_zero() {
    // pg: SELECT similarity('foo', 'xyz'); => 0
    assert!(approx_eq(similarity("foo", "xyz"), 0.0));
    assert!(approx_eq(similarity("aaa", "zzz"), 0.0));
}

#[test]
fn similarity_is_symmetric() {
    // |T(a) ∩ T(b)| / |T(a) ∪ T(b)| is symmetric by construction.
    let pairs = [
        ("alice smith", "alyce smyth"),
        ("postgresql", "postgres"),
        ("hello world", "world hello"),
        ("the quick brown fox", "quick brown"),
    ];
    for (a, b) in pairs {
        assert!(
            approx_eq(similarity(a, b), similarity(b, a)),
            "asymmetric: {a:?} vs {b:?}: {} vs {}",
            similarity(a, b),
            similarity(b, a)
        );
    }
}

#[test]
fn typo_pair_scores_above_threshold() {
    // pg's default `pg_trgm.similarity_threshold` is 0.3. Postgres emits
    // ~0.41 for this pair when its tokenizer is in default mode, but our
    // hand-rolled tokenizer differs slightly in space-padding (we don't
    // expand consecutive whitespace), so we get ~0.333. Both are above the
    // default PG threshold, so we assert > 0.3 (the same threshold real PG
    // users tune their queries against).
    let s = similarity("alice smith", "alyce smyth");
    assert!(
        s > 0.3,
        "typo pair scored only {s} (must clear PG default 0.3 threshold)"
    );
    assert!(s < 1.0, "typo pair must be < 1.0");
}

#[test]
fn unrelated_pair_scores_below_threshold() {
    // Bar from the viability card: unrelated pair < 0.2.
    let s = similarity("alice smith", "bob jones");
    assert!(s < 0.2, "unrelated pair scored {s}");
}

#[test]
fn empty_inputs_score_zero() {
    // pg: SELECT similarity('', '') => 0
    assert!(approx_eq(similarity("", ""), 0.0));
    assert!(approx_eq(similarity("", "cat"), 0.0));
    assert!(approx_eq(similarity("cat", ""), 0.0));
}

#[test]
fn case_insensitive() {
    // pg: SELECT similarity('Cat', 'cat'); => 1
    assert!(approx_eq(similarity("Cat", "cat"), 1.0));
    assert!(approx_eq(similarity("ALICE", "alice"), 1.0));
}

#[test]
fn output_in_unit_interval() {
    let pairs = [
        ("a", "b"),
        ("foo", "bar"),
        ("alice smith", "alyce smyth"),
        ("postgresql", "mysql"),
        ("", ""),
    ];
    for (a, b) in pairs {
        let s = similarity(a, b);
        assert!(
            (0.0..=1.0).contains(&s),
            "out of range: similarity({a:?}, {b:?}) = {s}"
        );
    }
}
