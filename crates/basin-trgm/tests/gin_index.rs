//! `TrigramIndex` integration tests.
//!
//! The load-bearing test is `gin_results_match_brute_force`: it pins the
//! correctness invariant that the GIN-indexed search returns identical
//! `(row_id, score)` tuples to running brute-force `similarity` over the
//! corpus. If that diverges, the index is wrong, full stop.

use std::sync::{Arc, Mutex};
use std::thread;

use basin_trgm::{similarity, TrigramIndex};

const EPS: f32 = 1e-5;

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= EPS
}

#[test]
fn insert_and_search_basic() {
    let mut idx = TrigramIndex::new();
    idx.insert(1, "alice");
    idx.insert(2, "alyce");
    idx.insert(3, "bob");
    let hits = idx.search("alice", 0.0, 10);
    // Row 1 (exact match) should dominate; row 2 should outrank row 3.
    assert!(!hits.is_empty(), "expected non-empty results");
    assert_eq!(hits[0].0, 1, "exact match should be first");
    assert!(approx_eq(hits[0].1, 1.0), "exact match scores 1.0");
    let pos2 = hits.iter().position(|(rid, _)| *rid == 2);
    let pos3 = hits.iter().position(|(rid, _)| *rid == 3);
    if let (Some(p2), Some(p3)) = (pos2, pos3) {
        assert!(p2 < p3, "alyce should outrank bob");
    } else {
        // bob may be filtered if it has zero overlap — alyce must still appear
        assert!(pos2.is_some(), "alyce should always be a candidate");
    }
}

#[test]
fn search_threshold_filters() {
    let mut idx = TrigramIndex::new();
    idx.insert(1, "alice");
    idx.insert(2, "alyce");
    idx.insert(3, "totally unrelated");
    let high = idx.search("alice", 0.9, 10);
    assert_eq!(high.len(), 1, "only the exact match passes 0.9");
    assert_eq!(high[0].0, 1);
    let low = idx.search("alice", 0.0, 10);
    assert!(low.len() >= 2, "low threshold catches more rows");
}

#[test]
fn search_limit_respected() {
    let mut idx = TrigramIndex::new();
    for i in 0..20 {
        idx.insert(i, "alice");
    }
    let hits = idx.search("alice", 0.0, 5);
    assert_eq!(hits.len(), 5, "limit caps result count");
}

#[test]
fn remove_row_id_disappears_from_results() {
    let mut idx = TrigramIndex::new();
    idx.insert(1, "alice");
    idx.insert(2, "alyce");
    let before = idx.search("alice", 0.0, 10);
    assert!(before.iter().any(|(rid, _)| *rid == 2));
    idx.remove(2);
    let after = idx.search("alice", 0.0, 10);
    assert!(
        after.iter().all(|(rid, _)| *rid != 2),
        "removed row must not appear"
    );
    // Removing a non-existent row is a no-op.
    idx.remove(999);
}

#[test]
fn search_returns_empty_when_query_unknown() {
    let mut idx = TrigramIndex::new();
    idx.insert(1, "alice");
    idx.insert(2, "bob");
    // Query whose trigrams don't appear in the corpus — should return empty.
    let hits = idx.search("xyzzy", 0.0, 10);
    assert!(
        hits.is_empty(),
        "novel-trigram query returns empty: {hits:?}"
    );
    // Query that's all whitespace produces no trigrams → empty.
    let hits = idx.search("   ", 0.0, 10);
    assert!(hits.is_empty());
    // Empty query → empty.
    let hits = idx.search("", 0.0, 10);
    assert!(hits.is_empty());
    // Zero limit → empty even if the query has matches.
    let hits = idx.search("alice", 0.0, 0);
    assert!(hits.is_empty());
}

#[test]
fn gin_results_match_brute_force() {
    // Load-bearing correctness test: same set + same scores as
    // brute-force `similarity` over the full corpus.
    let corpus: Vec<&str> = vec![
        "alice",
        "alyce",
        "alyssa",
        "bob",
        "robert",
        "smith",
        "smyth",
        "smithson",
        "johnson",
        "jonson",
        "jonsen",
        "anderson",
        "andersen",
        "michael",
        "micheal",
        "mikael",
        "mike",
        "mick",
        "mickael",
        "mick mick",
        "elizabeth",
        "elisabeth",
        "lizbeth",
        "liz",
        "beth",
        "betsy",
        "betty",
        "william",
        "willem",
        "wilhelm",
        "will",
        "bill",
        "billy",
        "wm",
        "katherine",
        "katharine",
        "kathryn",
        "catherine",
        "cathy",
        "kathy",
        "richard",
        "rickard",
        "rick",
        "dick",
        "ricky",
        "rich",
        "richie",
        "thomas",
        "tomas",
        "tom",
        "tommy",
        "thompson",
        "tomson",
        "edward",
        "edmund",
        "ed",
        "eddie",
        "ted",
        "teddy",
        "ned",
        "henry",
        "henri",
        "hank",
        "harry",
        "harold",
        "harriet",
        "henrietta",
        "george",
        "georg",
        "georges",
        "geo",
        "georgie",
        "charles",
        "charlie",
        "carl",
        "karl",
        "chuck",
        "chas",
        "joseph",
        "josef",
        "joe",
        "joey",
        "jose",
        "jodie",
        "yossi",
        "francis",
        "francisco",
        "frank",
        "frankie",
        "francisca",
        "patricia",
        "patrick",
        "patty",
        "pat",
        "patrice",
        "patsy",
        "barbara",
        "barb",
        "babs",
        "barbra",
        "barbie",
        "jennifer",
        "jenny",
        "jenn",
        "jen",
        "ginny",
    ];
    let mut idx = TrigramIndex::new();
    for (i, text) in corpus.iter().enumerate() {
        idx.insert(i as u64, text);
    }

    let queries = ["smith", "alice", "rich", "henry", "katherine", "qzqz"];
    for q in &queries {
        // Brute-force baseline: every row, score, filter, sort.
        let mut bf: Vec<(u64, f32)> = corpus
            .iter()
            .enumerate()
            .map(|(i, text)| (i as u64, similarity(q, text)))
            .filter(|(_, s)| *s >= 0.3)
            .collect();
        bf.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        bf.truncate(10);

        let gin = idx.search(q, 0.3, 10);

        assert_eq!(
            gin.len(),
            bf.len(),
            "query {q:?}: GIN returned {} rows, brute-force {}; gin={gin:?}, bf={bf:?}",
            gin.len(),
            bf.len()
        );
        for (g, b) in gin.iter().zip(bf.iter()) {
            assert_eq!(
                g.0, b.0,
                "query {q:?}: row_id mismatch gin={gin:?}, bf={bf:?}"
            );
            assert!(
                approx_eq(g.1, b.1),
                "query {q:?} row {}: gin score {}, brute-force {}",
                g.0,
                g.1,
                b.1
            );
        }
    }
}

#[test]
fn gin_search_handles_unicode() {
    let mut idx = TrigramIndex::new();
    // Multibyte chars: the underlying extractor treats non-ASCII bytes
    // as word separators, so these should not panic and should produce
    // sensible results based on the ASCII portion.
    idx.insert(1, "café");
    idx.insert(2, "naïve");
    idx.insert(3, "日本語");
    idx.insert(4, "smörgåsbord");
    idx.insert(5, "alice");
    // Should not panic.
    let _ = idx.search("café", 0.0, 10);
    let _ = idx.search("日本語", 0.0, 10);
    let _ = idx.search("smörgåsbord", 0.0, 10);
    // ASCII-only query continues to work.
    let hits = idx.search("alice", 0.5, 10);
    assert!(hits.iter().any(|(rid, _)| *rid == 5));
}

#[test]
fn word_similarity_search_basic() {
    let mut idx = TrigramIndex::new();
    idx.insert(1, "alice smith");
    idx.insert(2, "bob jones");
    idx.insert(3, "alice");
    let hits = idx.search_word_similarity("alice", 0.5, 10);
    // Row 3 (single-word "alice") should hit. Row 1 contains "alice"
    // among multiple words but the row's full trigram set dilutes the
    // score; depending on threshold either may appear.
    assert!(hits.iter().any(|(rid, _)| *rid == 3));
    let high = idx.search_word_similarity("alice", 0.99, 10);
    // Single exact word match should still produce ~1.0 against row 3
    // since the row text "alice" tokenises to a single word too.
    assert!(high.iter().any(|(rid, _)| *rid == 3));
    // Multi-word query: best per-word score should drive results.
    let multi = idx.search_word_similarity("alice xyzzy", 0.5, 10);
    assert!(multi.iter().any(|(rid, _)| *rid == 3));
    // Zero limit → empty.
    assert!(idx.search_word_similarity("alice", 0.0, 0).is_empty());
}

#[test]
fn gin_index_concurrent_inserts_safe() {
    let idx = Arc::new(Mutex::new(TrigramIndex::new()));
    let mut handles = Vec::new();
    for t in 0..10u64 {
        let idx = Arc::clone(&idx);
        handles.push(thread::spawn(move || {
            for i in 0..50u64 {
                let row_id = t * 1000 + i;
                let text = format!("row-{}-{}", t, i);
                idx.lock().unwrap().insert(row_id, &text);
                // Interleave a search to exercise read-while-write.
                let _ = idx.lock().unwrap().search("row", 0.0, 5);
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panicked");
    }
    let final_idx = idx.lock().unwrap();
    assert_eq!(final_idx.len(), 10 * 50, "all rows indexed");
    let hits = final_idx.search("row", 0.0, 1000);
    assert!(
        !hits.is_empty(),
        "search returns rows after concurrent inserts"
    );
}

#[test]
fn gin_speedup_over_brute_force() {
    // Observability test — passes regardless of speedup, but prints
    // timings so we can eyeball the GIN win on a 1000-row corpus.
    use std::time::Instant;

    let n = 1000usize;
    let bases = [
        "alice",
        "smith",
        "johnson",
        "anderson",
        "michael",
        "elizabeth",
        "william",
        "katherine",
        "richard",
        "thomas",
    ];
    let corpus: Vec<String> = (0..n)
        .map(|i| format!("{}-{}", bases[i % bases.len()], i))
        .collect();

    let mut idx = TrigramIndex::new();
    for (i, text) in corpus.iter().enumerate() {
        idx.insert(i as u64, text);
    }

    let query = "smith";

    let t0 = Instant::now();
    let mut bf: Vec<(u64, f32)> = corpus
        .iter()
        .enumerate()
        .map(|(i, text)| (i as u64, similarity(query, text)))
        .filter(|(_, s)| *s >= 0.3)
        .collect();
    bf.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    bf.truncate(10);
    let brute_t = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    let gin = idx.search(query, 0.3, 10);
    let gin_t = t1.elapsed().as_secs_f64() * 1000.0;

    eprintln!("brute-force search: {:.3} ms", brute_t);
    eprintln!("GIN search: {:.3} ms", gin_t);
    if gin_t > 0.0 {
        eprintln!("speedup: {:.1}x", brute_t / gin_t);
    }

    // Equivalence sanity-check on the speedup test (cheap insurance).
    assert_eq!(gin.len(), bf.len(), "result-count mismatch");
}
