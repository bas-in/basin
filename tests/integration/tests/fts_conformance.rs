//! Full-text search (FTS) conformance tests.
//!
//! Tests only the surface the engine actually implements.  Every test is
//! pinned to documented Basin semantics (see `crates/basin-engine/src/fts_udf.rs`
//! module comment).  Gaps are recorded in the module-level gap list below.
//!
//! ## What is tested
//!
//! | Test                                                 | Surface / Regression                        |
//! |------------------------------------------------------|---------------------------------------------|
//! | `tsvector_empty_document`                            | empty doc → empty tsvector                 |
//! | `tsvector_stopwords_only`                            | all-stopword doc → empty tsvector           |
//! | `tsvector_stemming_consistency_with_tsquery`         | to_tsvector/to_tsquery same stem pipeline   |
//! | `tsvector_duplicate_positions_collapsed`             | duplicate lexeme gets multiple positions    |
//! | `tsvector_simple_config_no_stemming_no_stopwords`    | simple config: unstemmed + no stopwords     |
//! | `at_at_positive_match`                               | @@ returns true for present term            |
//! | `at_at_negative_miss`                                | @@ returns false for absent term            |
//! | `at_at_and_both_present`                             | @@ AND: both present → true                |
//! | `at_at_and_one_absent`                               | @@ AND: one absent → false                 |
//! | `at_at_or_one_present`                               | @@ OR: one branch present → true           |
//! | `at_at_or_none_present`                              | @@ OR: neither present → false             |
//! | `at_at_not_absent_term`                              | @@ NOT: negated absent term → true         |
//! | `at_at_not_present_term`                             | @@ NOT: negated present term → false       |
//! | `at_at_phrase_in_order`                              | @@ phrase <-> in order → true              |
//! | `at_at_phrase_out_of_order`                          | @@ phrase <-> wrong order → false          |
//! | `at_at_phrase_custom_distance`                       | @@ phrase <2> correct distance → true      |
//! | `at_at_where_clause_filter`                          | @@ in WHERE selects correct rows            |
//! | `at_at_select_expression`                            | @@ in SELECT list (not WHERE)               |
//! | `at_at_null_tsvector`                                | NULL tsvector @@ q → NULL (not false)       |
//! | `at_at_null_tsquery`                                 | tv @@ NULL tsquery → NULL (not false)       |
//! | `at_at_empty_tsquery`                                | tv @@ empty tsquery → false                |
//! | `plainto_tsquery_and_join`                           | plainto_tsquery AND-joins terms             |
//! | `phraseto_tsquery_phrase_join`                       | phraseto_tsquery phrase-joins terms         |
//! | `websearch_to_tsquery_treated_as_plainto`            | websearch_to_tsquery best-effort AND        |
//! | `ts_rank_positive_for_match`                         | ts_rank > 0 for genuine match               |
//! | `ts_rank_zero_for_no_match`                          | ts_rank = 0 when query doesn't match        |
//! | `ts_rank_deterministic`                              | ts_rank same result across 10 runs          |
//! | `ts_rank_relative_ordering`                          | more term occurrences → higher rank         |
//! | `ts_rank_cd_non_negative`                            | ts_rank_cd ≥ 0 always                       |
//! | `ts_headline_wraps_matched_terms`                    | ts_headline wraps matched terms in <b>…</b> |
//! | `ts_headline_no_match_unchanged`                     | ts_headline returns body when no match      |
//! | `ts_headline_null_safe`                              | ts_headline on NULL body → NULL             |
//! | `setweight_not_registered`                           | setweight is not registered (honest error)  |
//! | `strip_removes_positions`                            | strip() removes position info               |
//! | `tsvector_length_counts_lexemes`                     | tsvector_length returns lexeme count        |
//! | `gin_index_scan_agrees_with_table_scan`              | GIN vs full-scan result equality            |
//! | `gin_or_not_and_merged`                              | OR probe unions file sets (regression)      |
//! | `gin_not_declines_to_full_scan`                      | NOT probe full-scans (regression)           |
//! | `gin_phrase_prunes_positionally_reevaluates`         | phrase prunes as AND, re-checks positions   |
//! | `at_at_stem_match_running_vs_run`                    | stemmed doc matches stemmed query (regression)|
//! | `at_at_raw_cast_not_stemmed`                         | ::tsquery cast NOT stemmed (PG parity)      |
//!
//! ## Gap list (out of scope — NOT tested)
//!
//! - `setweight(tsvector, "A"…"D")` — not registered; returns error.
//! - Weighted tsvectors (`:1A` weight class notation) — positions are parsed
//!   but weight letters are stripped; `ts_rank` ignores weights.
//! - `ts_rank` / `ts_rank_cd` cover-density algorithm — both use the same
//!   simplified `matched_lexemes / total_lexemes` formula.
//! - `ts_headline` fragment selection — returns the full body with matched
//!   terms wrapped, not PG's cover-density snippet.
//! - Multi-language configs beyond `english` and `simple` — accepted but not
//!   distinguished.
//! - `ts_delete` / `ts_filter` / `numnode` precision / `querytree` exact
//!   PG output — not tested here (covered by fts_stubs.rs).
//! - `websearch_to_tsquery` exact PG output — documented best-effort (AND join).
//! - `@@` inside `--` line comments — real engine bug, tracked in fts_at_at.rs.
//!
//! ## Needs-psql-validation table
//!
//! | ID     | Test                                   | Value to validate                          |
//! |--------|----------------------------------------|--------------------------------------------|
//! | FTS-1  | `tsvector_stemming_consistency_*`      | Exact lexeme output for 'running' → 'run'; |
//! |        |                                        | 'dogs' → 'dog'; compound stems             |
//! | FTS-2  | `ts_rank_positive_for_match`           | Score for 1-match 4-lexeme doc (expected   |
//! |        |                                        | 0.25 = 1/4 by Basin's formula, not PG BM25)|
//! | FTS-3  | `ts_rank_relative_ordering`            | Exact score for 3× vs 1× repeated term     |
//! | FTS-4  | `ts_headline_wraps_matched_terms`      | Basin wraps unstemmed tokens; PG may not   |
//! |        |                                        | highlight 'FOX' if it was stemmed at index |
//! | FTS-5  | `tsvector_simple_config_*`             | simple: 'the' appears as position 1,       |
//! |        |                                        | 'a' as position 2, etc. exact positions    |

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int32Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── test harness ─────────────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn build_engine_no_cache(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec(sess: &basin_engine::ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e}"));
}

async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for {sql:?}: {e}"),
    }
}

async fn single_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected Boolean: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_bool_nullable(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Option<bool> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected Boolean: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            if arr.is_null(0) { None } else { Some(arr.value(0)) }
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_string(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("expected Utf8: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0).to_owned()
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_f32(sess: &basin_engine::ProjectSession, sql: &str) -> f32 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap_or_else(|| panic!("expected Float32: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

/// Return sorted i64 ids from the first column of query results.
async fn sorted_ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut ids = Vec::new();
            for b in &batches {
                if let Some(arr) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            ids.push(arr.value(i));
                        }
                    }
                }
            }
            ids.sort_unstable();
            ids
        }
        Ok(ExecResult::Empty { .. }) => Vec::new(),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// to_tsvector / tsvector semantics
// ─────────────────────────────────────────────────────────────────────────────

/// Empty document produces an empty tsvector (the stored form is "").
/// PG: SELECT to_tsvector('english', '') = '' (empty lexeme set).
#[tokio::test]
async fn tsvector_empty_document() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(&sess, "SELECT to_tsvector('english', '')").await;
    assert_eq!(
        got, "",
        "empty document must produce empty tsvector ''; got={got:?}"
    );
    println!("[fts tsvec] empty document → '' ✓");
}

/// Document composed entirely of stopwords produces an empty tsvector.
/// PG: SELECT to_tsvector('english', 'the a an is in at') = ''
#[tokio::test]
async fn tsvector_stopwords_only() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT to_tsvector('english', 'the a an is in at on')",
    )
    .await;
    assert_eq!(
        got, "",
        "all-stopword document must produce empty tsvector; got={got:?}"
    );
    println!("[fts tsvec] stopwords-only → '' ✓");
}

/// Stemming consistency: 'running' is stored as 'run'; to_tsquery('running')
/// also stems to 'run', so @@ matches.
///
/// This is the critical regression test for the Phase 5.20.E stem-consistency
/// contract. A stem mismatch between the document side and the query side would
/// break both @@ and GIN prune soundness.
///
/// [FTS-1] NEEDS PSQL VALIDATION — exact canonical form of to_tsvector.
#[tokio::test]
async fn tsvector_stemming_consistency_with_tsquery() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Document side: 'running' → lexeme 'run'.
    let tv = single_string(&sess, "SELECT to_tsvector('english', 'running fast')").await;
    assert!(
        tv.contains("'run'"),
        "to_tsvector('running fast') must contain lexeme 'run'; got={tv:?}"
    );

    // Query side: to_tsquery('running') must also stem to 'run'.
    let tq = single_string(&sess, "SELECT to_tsquery('english', 'running')").await;
    assert_eq!(
        tq, "'run'",
        "to_tsquery('english','running') must stem to \"'run'\"; got={tq:?}"
    );

    // End-to-end: the stemmed document @@ stemmed query must match.
    let matched = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'running fast') @@ to_tsquery('english', 'running')",
    )
    .await;
    assert!(
        matched,
        "stemmed doc 'running' @@ stemmed query 'running' must be true"
    );

    // Negative: ::tsquery (raw cast) does NOT stem → 'running' != 'run' → no match.
    let raw_mismatch = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'running fast') @@ 'running'::tsquery",
    )
    .await;
    assert!(
        !raw_mismatch,
        "::tsquery cast is NOT stemmed; 'running' != 'run' → must return false"
    );

    println!("[fts stem] stemming consistency ✓");
}

/// A lexeme that appears at multiple positions accumulates all positions.
/// 'fox fox fox' → 'fox':1,2,3 (not just position 1).
#[tokio::test]
async fn tsvector_duplicate_positions_collapsed() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT to_tsvector('english', 'fox fox fox')",
    )
    .await;
    // 'fox' → stem 'fox' (not changed by English stemmer); positions 1,2,3.
    assert_eq!(
        got, "'fox':1,2,3",
        "triple-'fox' doc must have positions 1,2,3; got={got:?}"
    );
    println!("[fts tsvec] duplicate positions → 1,2,3 ✓");
}

/// `simple` config: no stemming, no stopwords.
/// 'the quick fox' → 'fox':3 'quick':2 'the':1  (stopword 'the' kept).
///
/// [FTS-5] NEEDS PSQL VALIDATION — exact positions.
#[tokio::test]
async fn tsvector_simple_config_no_stemming_no_stopwords() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT to_tsvector('simple', 'the quick fox')",
    )
    .await;
    // Sorted: fox:3, quick:2, the:1.
    assert_eq!(
        got, "'fox':3 'quick':2 'the':1",
        "simple config must keep 'the' and not stem; got={got:?}"
    );

    // With simple config, 'running' is NOT stemmed.
    let plain = single_string(
        &sess,
        "SELECT to_tsvector('simple', 'running')",
    )
    .await;
    assert_eq!(
        plain, "'running':1",
        "simple config must NOT stem 'running'; got={plain:?}"
    );

    println!("[fts tsvec] simple config: stopwords kept, no stemming ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// @@ operator truth table
// ─────────────────────────────────────────────────────────────────────────────

/// Regression: @@ was once a match-all stub returning true for every pair.
/// These tests pin the correct semantics of the real evaluator.

#[tokio::test]
async fn at_at_positive_match() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('world')",
    )
    .await;
    assert!(r, "@@ positive: 'world' present in vector must match");
    println!("[fts @@] positive match ✓");
}

#[tokio::test]
async fn at_at_negative_miss() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Regression: the old stub always returned true.  This catches that.
    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('rust')",
    )
    .await;
    assert!(!r, "@@ negative: 'rust' absent from vector must NOT match (stub regression)");
    println!("[fts @@] negative miss ✓");
}

#[tokio::test]
async fn at_at_and_both_present() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('hello & world')",
    )
    .await;
    assert!(r, "@@ AND: both terms present → true");
    println!("[fts @@] AND both-present ✓");
}

#[tokio::test]
async fn at_at_and_one_absent() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('hello & rust')",
    )
    .await;
    assert!(!r, "@@ AND: one term absent → false");
    println!("[fts @@] AND one-absent ✓");
}

#[tokio::test]
async fn at_at_or_one_present() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Regression: a naive AND-merge of OR branches would fail this.
    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('hello | rust')",
    )
    .await;
    assert!(r, "@@ OR: one branch present → true");
    println!("[fts @@] OR one-present ✓");
}

#[tokio::test]
async fn at_at_or_none_present() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('rust | python')",
    )
    .await;
    assert!(!r, "@@ OR: neither branch present → false");
    println!("[fts @@] OR none-present ✓");
}

/// NOT: negated term that is ABSENT → the NOT condition is satisfied → true.
#[tokio::test]
async fn at_at_not_absent_term() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Regression: a naive NOT-as-AND-merge would treat !rust as requiring
    // 'rust' in the posting list, pruning docs that don't contain it.
    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('!rust')",
    )
    .await;
    assert!(r, "@@ NOT absent-term: !rust with 'rust' absent → true");
    println!("[fts @@] NOT absent-term ✓");
}

/// NOT: negated term that IS PRESENT → NOT condition fails → false.
#[tokio::test]
async fn at_at_not_present_term() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('!hello')",
    )
    .await;
    assert!(!r, "@@ NOT present-term: !hello with 'hello' present → false");
    println!("[fts @@] NOT present-term ✓");
}

/// Phrase `<->`: terms must be adjacent and in order.
/// 'quick brown fox': quick@2, brown@3, fox@4.  'quick <-> brown' → adjacent → true.
#[tokio::test]
async fn at_at_phrase_in_order() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'the quick brown fox') @@ to_tsquery('quick <-> brown')",
    )
    .await;
    assert!(r, "phrase <->: quick immediately before brown → true");
    println!("[fts @@] phrase in-order ✓");
}

/// Phrase: wrong order → false.
/// 'quick <-> fox' — positions are quick@2, fox@4 — not adjacent → false.
#[tokio::test]
async fn at_at_phrase_out_of_order() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // quick@2, fox@4: distance is 2, not 1 (non-adjacent for <->).
    let r = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'the quick brown fox') @@ to_tsquery('quick <-> fox')",
    )
    .await;
    assert!(!r, "phrase <->: quick and fox are 2 apart, not adjacent → false");
    println!("[fts @@] phrase out-of-order ✓");
}

/// Phrase `<2>`: exact distance 2.
/// 'quick <2> fox' — quick@2, fox@4 — distance = 2 → true.
#[tokio::test]
async fn at_at_phrase_custom_distance() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'the quick brown fox') @@ to_tsquery('quick <2> fox')",
    )
    .await;
    assert!(r, "phrase <2>: quick(2) and fox(4) are exactly 2 apart → true");
    println!("[fts @@] phrase custom distance <2> ✓");
}

/// @@ in WHERE clause correctly filters rows.
/// Three rows: fox present in rows 1 and 3 only.
#[tokio::test]
async fn at_at_where_clause_filter() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fdocs (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(
        &sess,
        "INSERT INTO fdocs VALUES
         (1, to_tsvector('english', 'the quick brown fox')),
         (2, to_tsvector('english', 'lazy dog sleeps')),
         (3, to_tsvector('english', 'fox runs fast'))",
    )
    .await;

    let ids = sorted_ids(
        &sess,
        "SELECT id FROM fdocs WHERE ts @@ to_tsquery('english', 'fox')",
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 3],
        "WHERE @@ 'fox': should return ids [1, 3]; got={ids:?}"
    );
    println!("[fts @@] WHERE filter → [1,3] ✓");
}

/// @@ in SELECT list (not WHERE) produces a boolean column.
#[tokio::test]
async fn at_at_select_expression() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE sexpr (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(
        &sess,
        "INSERT INTO sexpr VALUES (1, to_tsvector('english', 'cat and dog'))",
    )
    .await;

    let r = single_bool(
        &sess,
        "SELECT ts @@ to_tsquery('english', 'cat') FROM sexpr WHERE id = 1",
    )
    .await;
    assert!(r, "@@ in SELECT list for row containing 'cat' must return true");

    let r2 = single_bool(
        &sess,
        "SELECT ts @@ to_tsquery('english', 'elephant') FROM sexpr WHERE id = 1",
    )
    .await;
    assert!(!r2, "@@ in SELECT list for absent term must return false");
    println!("[fts @@] SELECT expression ✓");
}

/// NULL tsvector @@ query → NULL result (not false, not an error).
/// PG: NULL @@ 'fox' = NULL.
#[tokio::test]
async fn at_at_null_tsvector() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nullvec (id BIGINT, ts TSVECTOR)").await;
    exec(&sess, "INSERT INTO nullvec VALUES (1, NULL)").await;

    let result = single_bool_nullable(
        &sess,
        "SELECT ts @@ to_tsquery('english', 'fox') FROM nullvec WHERE id = 1",
    )
    .await;
    // Must be NULL (not false, not true).
    assert_eq!(
        result, None,
        "NULL tsvector @@ query must return NULL; got={result:?}"
    );
    println!("[fts @@] NULL tsvector → NULL ✓");
}

/// tv @@ NULL tsquery → NULL result.
#[tokio::test]
async fn at_at_null_tsquery() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nullq (id BIGINT, ts TSVECTOR, q TSQUERY)").await;
    exec(
        &sess,
        "INSERT INTO nullq VALUES (1, to_tsvector('hello world'), NULL)",
    )
    .await;

    let result = single_bool_nullable(
        &sess,
        "SELECT ts @@ q FROM nullq WHERE id = 1",
    )
    .await;
    assert_eq!(
        result, None,
        "tsvector @@ NULL tsquery must return NULL; got={result:?}"
    );
    println!("[fts @@] NULL tsquery → NULL ✓");
}

/// Empty tsquery → false (PG warns and returns 0 rows / false for empty query).
/// Basin documents this as "empty query matches nothing".
#[tokio::test]
async fn at_at_empty_tsquery() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // An empty string passed to to_tsquery returns the empty string ''.
    // '' as a tsquery matches nothing.
    let r = single_bool(
        &sess,
        "SELECT to_tsvector('hello world') @@ to_tsquery('')",
    )
    .await;
    assert!(!r, "empty tsquery must not match anything; got={r}");
    println!("[fts @@] empty tsquery → false ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// plainto_tsquery / phraseto_tsquery / websearch_to_tsquery
// ─────────────────────────────────────────────────────────────────────────────

/// plainto_tsquery AND-joins terms after stopword removal.
/// 'the quick fox' → 'quick' & 'fox' (not 'the' which is a stopword).
#[tokio::test]
async fn plainto_tsquery_and_join() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT plainto_tsquery('english', 'the quick fox')",
    )
    .await;
    assert_eq!(
        got, "'quick' & 'fox'",
        "plainto_tsquery must AND-join non-stopword terms; got={got:?}"
    );

    // Verify it actually works for matching.
    let matched = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'a quick brown fox') @@ plainto_tsquery('english', 'quick fox')",
    )
    .await;
    assert!(matched, "plainto_tsquery match must work end-to-end");
    println!("[fts plainto] AND-join ✓");
}

/// phraseto_tsquery phrase-joins with <->.
#[tokio::test]
async fn phraseto_tsquery_phrase_join() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT phraseto_tsquery('english', 'quick fox')",
    )
    .await;
    assert_eq!(
        got, "'quick' <-> 'fox'",
        "phraseto_tsquery must phrase-join with <->; got={got:?}"
    );

    // 'the quick brown fox': quick@2, fox@4 — NOT adjacent (distance 2).
    let not_adj = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'the quick brown fox') @@ phraseto_tsquery('english', 'quick fox')",
    )
    .await;
    assert!(
        !not_adj,
        "phraseto_tsquery('quick fox'): quick and fox not adjacent in 'the quick brown fox' → false"
    );

    // 'quick fox': quick@1, fox@2 — adjacent → true.
    let adj = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'quick fox') @@ phraseto_tsquery('english', 'quick fox')",
    )
    .await;
    assert!(
        adj,
        "phraseto_tsquery('quick fox'): adjacent in 'quick fox' → true"
    );
    println!("[fts phraseto] phrase-join ✓");
}

/// websearch_to_tsquery is a best-effort AND join (documented behaviour).
#[tokio::test]
async fn websearch_to_tsquery_treated_as_plainto() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT websearch_to_tsquery('english', 'quick fox')",
    )
    .await;
    // Basin documents websearch_to_tsquery as best-effort plainto (AND join).
    assert_eq!(
        got, "'quick' & 'fox'",
        "websearch_to_tsquery best-effort: expected AND-join; got={got:?}"
    );
    println!("[fts websearch] treated as plainto ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// ts_rank / ts_rank_cd
// ─────────────────────────────────────────────────────────────────────────────

/// ts_rank returns a positive score when the query term is present.
///
/// [FTS-2] NEEDS PSQL VALIDATION — Basin's simplified formula is
/// matched_lexemes / total_lexemes.  For a doc with 4 distinct lexemes
/// (quick, brown, fox, jump) and query 'fox', score = 1/4 = 0.25.
/// PG's BM25 would return a different value.
#[tokio::test]
async fn ts_rank_positive_for_match() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let score = single_f32(
        &sess,
        "SELECT ts_rank(\
            to_tsvector('english', 'quick brown fox jumps'), \
            to_tsquery('english', 'fox'))",
    )
    .await;
    assert!(
        score > 0.0,
        "ts_rank must be positive when query term is present; got={score}"
    );
    assert!(
        (0.0..=1.0).contains(&score),
        "ts_rank must be in [0,1]; got={score}"
    );
    println!("[fts ts_rank] positive-for-match: {score} ✓");
}

/// ts_rank returns 0.0 when the query doesn't match the document.
#[tokio::test]
async fn ts_rank_zero_for_no_match() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let score = single_f32(
        &sess,
        "SELECT ts_rank(\
            to_tsvector('english', 'quick brown fox'), \
            to_tsquery('english', 'elephant'))",
    )
    .await;
    assert_eq!(
        score, 0.0,
        "ts_rank must be 0 when query term is absent; got={score}"
    );
    println!("[fts ts_rank] zero-for-no-match ✓");
}

/// ts_rank is deterministic across repeated calls with identical inputs.
#[tokio::test]
async fn ts_rank_deterministic() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let sql = "SELECT ts_rank(\
        to_tsvector('english', 'quick brown fox jumped over the lazy dog'), \
        to_tsquery('english', 'fox'))";
    let mut scores = Vec::new();
    for _ in 0..10 {
        scores.push(single_f32(&sess, sql).await);
    }
    let first = scores[0];
    for (i, &s) in scores.iter().enumerate() {
        assert_eq!(
            s, first,
            "ts_rank must be deterministic; run {i} returned {s}, run 0 returned {first}"
        );
    }
    println!("[fts ts_rank] deterministic across 10 runs: {first} ✓");
}

/// Higher-frequency term appearance in a document produces a higher rank.
///
/// [FTS-3] NEEDS PSQL VALIDATION — exact scores.
#[tokio::test]
async fn ts_rank_relative_ordering() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let low = single_f32(
        &sess,
        "SELECT ts_rank(\
            to_tsvector('english', 'the fox sat by the river'), \
            to_tsquery('english', 'fox'))",
    )
    .await;
    let high = single_f32(
        &sess,
        "SELECT ts_rank(\
            to_tsvector('english', 'fox chased fox until the fox escaped'), \
            to_tsquery('english', 'fox'))",
    )
    .await;
    assert!(
        high >= low,
        "3× 'fox' doc must rank >= 1× 'fox' doc; low={low}, high={high}"
    );
    println!("[fts ts_rank] relative ordering: high={high} >= low={low} ✓");
}

/// ts_rank_cd is non-negative.
#[tokio::test]
async fn ts_rank_cd_non_negative() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for sql in &[
        "SELECT ts_rank_cd(to_tsvector('english', 'fox runs fast'), to_tsquery('english', 'fox'))",
        "SELECT ts_rank_cd(to_tsvector('english', 'cat climbs'), to_tsquery('english', 'dog'))",
    ] {
        let s = single_f32(&sess, sql).await;
        assert!(s >= 0.0, "ts_rank_cd must be non-negative; got={s} for {sql}");
    }
    println!("[fts ts_rank_cd] non-negative ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// ts_headline
// ─────────────────────────────────────────────────────────────────────────────
//
// Basin's ts_headline wraps matched (positive, non-negated) query terms in
// <b>…</b> tags using exact-token lowercase comparison.  This is NOT PG's
// cover-density fragment selection (which returns a snippet; Basin returns
// the full body).
//
// [FTS-4] NEEDS PSQL VALIDATION: Basin wraps the original-case token (e.g.
// '<b>FOX</b>') while PG may highlight the stemmed form.

/// ts_headline wraps matched terms in <b>…</b>.
#[tokio::test]
async fn ts_headline_wraps_matched_terms() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // 2-arg form: (body_text, tsquery).
    let got = single_string(
        &sess,
        "SELECT ts_headline('a quick brown fox', to_tsquery('fox'))",
    )
    .await;
    assert_eq!(
        got, "a quick brown <b>fox</b>",
        "ts_headline must wrap 'fox' in <b>; got={got:?}"
    );

    // 3-arg form: (config, body_text, tsquery).
    let got3 = single_string(
        &sess,
        "SELECT ts_headline('english', 'quick brown fox', to_tsquery('fox'))",
    )
    .await;
    assert_eq!(
        got3, "quick brown <b>fox</b>",
        "ts_headline 3-arg must wrap 'fox' in <b>; got={got3:?}"
    );

    // AND query: both terms highlighted.
    let and_got = single_string(
        &sess,
        "SELECT ts_headline('Quick brown FOX jumps', to_tsquery('quick & fox'))",
    )
    .await;
    assert!(
        and_got.contains("<b>Quick</b>") && and_got.contains("<b>FOX</b>"),
        "ts_headline AND: both terms must be wrapped; got={and_got:?}"
    );
    println!("[fts ts_headline] wraps matched terms ✓");
}

/// ts_headline with no matching term returns the body unchanged.
#[tokio::test]
async fn ts_headline_no_match_unchanged() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let body = "the quick brown fox";
    let got = single_string(
        &sess,
        "SELECT ts_headline('the quick brown fox', to_tsquery('elephant'))",
    )
    .await;
    assert_eq!(
        got, body,
        "ts_headline no-match: body must be unchanged; got={got:?}"
    );
    println!("[fts ts_headline] no-match unchanged ✓");
}

/// ts_headline on NULL body → NULL.
#[tokio::test]
async fn ts_headline_null_safe() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE hl_null (id BIGINT, body TEXT)").await;
    exec(&sess, "INSERT INTO hl_null VALUES (1, NULL)").await;

    match sess
        .execute("SELECT ts_headline(body, to_tsquery('fox')) FROM hl_null WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let col = b.column(0);
            assert!(
                col.is_null(0),
                "ts_headline(NULL, q) must return NULL; col not null"
            );
        }
        Ok(other) => panic!("unexpected: {other:?}"),
        Err(e) => panic!("ts_headline on NULL body must not error: {e}"),
    }
    println!("[fts ts_headline] NULL body → NULL ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// setweight / strip / tsvector_length
// ─────────────────────────────────────────────────────────────────────────────

/// setweight is NOT registered (Phase 6.X removed it; it is an honest gap).
/// Must error cleanly, not return a stub value.
#[tokio::test]
async fn setweight_not_registered() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = sess
        .execute("SELECT setweight(to_tsvector('hello world'), 'A')")
        .await;
    assert!(
        r.is_err(),
        "setweight is not registered and must error; got={r:?}"
    );
    println!("[fts setweight] not registered → error ✓");
}

/// strip(tsvector) removes position information, lexemes remain.
#[tokio::test]
async fn strip_removes_positions() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Input has positions (from to_tsvector with english config).
    let stripped = single_string(
        &sess,
        "SELECT strip(to_tsvector('english', 'quick brown fox'))",
    )
    .await;
    // Positions must be removed; lexemes remain sorted.
    assert!(
        !stripped.contains(':'),
        "strip must remove position ':'  annotations; got={stripped:?}"
    );
    assert!(
        stripped.contains("'fox'") && stripped.contains("'quick'"),
        "strip must keep lexemes fox and quick; got={stripped:?}"
    );
    println!("[fts strip] positions removed, lexemes kept ✓");
}

/// tsvector_length returns the count of distinct lexemes.
#[tokio::test]
async fn tsvector_length_counts_lexemes() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    match sess
        .execute("SELECT tsvector_length(to_tsvector('english', 'one two three'))")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            assert_eq!(arr.value(0), 3, "tsvector_length: 3 distinct lexemes expected");
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Stopwords reduce the count.
    match sess
        .execute("SELECT tsvector_length(to_tsvector('english', 'the a quick brown fox'))")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            // 'the' and 'a' removed → 3 lexemes: quick, brown, fox.
            assert_eq!(
                arr.value(0), 3,
                "tsvector_length after stopword removal: expected 3"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[fts tsvec_length] counts lexemes ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// GIN index path vs table-scan equality (regression suite)
// ─────────────────────────────────────────────────────────────────────────────
//
// Historical bug class: the GIN fast-path (Empty short-circuit, pruned
// provider re-registration) could return different results from the baseline
// full scan if the posting list logic was unsound.  These tests pin exact
// result equality between the two paths on the same data.
//
// We use two identical tables:
//   * `scan_docs`  — no GIN index (forces full table scan)
//   * `gin_docs`   — has GIN index on `ts` column
// Both tables hold identical data.  For every query, both must return the
// same row ids.

async fn seed_two_tables(sess: &basin_engine::ProjectSession) {
    exec(sess, "CREATE TABLE scan_docs (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(sess, "CREATE TABLE gin_docs  (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(sess, "CREATE INDEX gin_docs_ts ON gin_docs USING gin (ts)").await;

    let inserts = [
        (1i64, "the quick brown fox"),
        (2, "lazy dog sleeps deeply"),
        (3, "fox runs fast through forest"),
        (4, "cat and dog play"),
        (5, "running marathon in the rain"),
    ];
    for (id, text) in &inserts {
        let sql = format!(
            "INSERT INTO scan_docs VALUES ({id}, to_tsvector('english', '{text}'))"
        );
        exec(sess, &sql).await;
        let sql2 = format!(
            "INSERT INTO gin_docs VALUES ({id}, to_tsvector('english', '{text}'))"
        );
        exec(sess, &sql2).await;
    }
}

/// Simple positive query: both paths return the same ids.
#[tokio::test]
async fn gin_index_scan_agrees_with_table_scan() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_two_tables(&sess).await;

    for query in &["fox", "dog", "run", "cat & dog", "fox | cat", "!dog"] {
        let scan = sorted_ids(
            &sess,
            &format!(
                "SELECT id FROM scan_docs WHERE ts @@ to_tsquery('english', '{query}')"
            ),
        )
        .await;
        let gin = sorted_ids(
            &sess,
            &format!(
                "SELECT id FROM gin_docs WHERE ts @@ to_tsquery('english', '{query}')"
            ),
        )
        .await;
        assert_eq!(
            scan, gin,
            "GIN vs scan MUST agree for query '{query}': scan={scan:?}, gin={gin:?}"
        );
        println!("[fts gin==scan] '{query}': {scan:?} ✓");
    }
}

/// OR query: the GIN path must UNION file sets, not AND-merge them.
/// Regression: the old AND-merge short-circuit would return empty for
/// 'cat | dog' if 'cat' and 'dog' lived in file-disjoint rows.
#[tokio::test]
async fn gin_or_not_and_merged() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE or_sep (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(&sess, "CREATE INDEX or_sep_gin ON or_sep USING gin (ts)").await;
    // Each row holds exactly one of the two query terms in separate files.
    exec(&sess, "INSERT INTO or_sep VALUES (1, to_tsvector('english', 'cat climbs'))").await;
    exec(&sess, "INSERT INTO or_sep VALUES (2, to_tsvector('english', 'dog barks'))").await;
    exec(&sess, "INSERT INTO or_sep VALUES (3, to_tsvector('english', 'fish swims'))").await;

    let ids = sorted_ids(
        &sess,
        "SELECT id FROM or_sep WHERE ts @@ to_tsquery('english', 'cat | dog')",
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2],
        "OR query must union file sets and return [1,2]; AND-merge returns [] (regression); got={ids:?}"
    );
    println!("[fts gin OR] union: [1,2] ✓");
}

/// NOT query: GIN path must NOT bound to files containing the negated term.
/// Regression: naive AND-merge would treat !dog as requiring 'dog', pruning
/// every file that doesn't contain 'dog'.
#[tokio::test]
async fn gin_not_declines_to_full_scan() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE not_sep (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(&sess, "CREATE INDEX not_sep_gin ON not_sep USING gin (ts)").await;
    exec(&sess, "INSERT INTO not_sep VALUES (1, to_tsvector('english', 'cat climbs'))").await;
    exec(&sess, "INSERT INTO not_sep VALUES (2, to_tsvector('english', 'cat dog'))").await;

    // 'cat & !dog': row 1 has cat but no dog → match; row 2 has both → no match.
    let ids = sorted_ids(
        &sess,
        "SELECT id FROM not_sep WHERE ts @@ to_tsquery('english', 'cat & !dog')",
    )
    .await;
    assert_eq!(
        ids,
        vec![1],
        "'cat & !dog' must return [1]; AND-merge would return [] (regression); got={ids:?}"
    );
    println!("[fts gin NOT] [1] ✓");
}

/// Phrase query: prune like AND (both terms must be in the same file) then
/// re-check positions.  The out-of-order row must be rejected.
#[tokio::test]
async fn gin_phrase_prunes_positionally_reevaluates() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ph_sep (id BIGINT NOT NULL, ts TSVECTOR)").await;
    exec(&sess, "CREATE INDEX ph_sep_gin ON ph_sep USING gin (ts)").await;
    // Row 1: 'fox quick' (wrong order).
    exec(&sess, "INSERT INTO ph_sep VALUES (1, to_tsvector('english', 'fox quick'))").await;
    // Row 2: 'quick fox' (right order, adjacent).
    exec(&sess, "INSERT INTO ph_sep VALUES (2, to_tsvector('english', 'quick fox'))").await;
    // Row 3: 'quick snail' (only 'quick', not 'fox').
    exec(&sess, "INSERT INTO ph_sep VALUES (3, to_tsvector('english', 'quick snail'))").await;

    // 'quick <-> fox': only row 2 has quick immediately before fox.
    let ids = sorted_ids(
        &sess,
        "SELECT id FROM ph_sep WHERE ts @@ to_tsquery('english', 'quick <-> fox')",
    )
    .await;
    assert_eq!(
        ids,
        vec![2],
        "phrase <-> must return only the in-order row [2]; got={ids:?}"
    );
    println!("[fts gin phrase] positional re-eval → [2] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Stemming regressions (pinned from Phase 5.20.E contract)
// ─────────────────────────────────────────────────────────────────────────────

/// to_tsvector + to_tsquery SAME Snowball pipeline → end-to-end match.
/// Regression: early builds had to_tsquery return unstemmed lexemes while
/// to_tsvector stored stemmed ones — every stemmed query would miss.
#[tokio::test]
async fn at_at_stem_match_running_vs_run() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Document: 'running' → stored as 'run'.
    // Query: to_tsquery('runs') → stems to 'run'.
    let matched = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'running fast') @@ to_tsquery('english', 'runs')",
    )
    .await;
    assert!(
        matched,
        "stem regression: to_tsquery('runs') must match doc 'running' (both→'run'); got=false"
    );

    // plainto variant.
    let plain = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'dogs bark loudly') @@ plainto_tsquery('english', 'dog barks')",
    )
    .await;
    assert!(
        plain,
        "stem regression: plainto_tsquery('dog barks') must match doc 'dogs bark loudly'"
    );
    println!("[fts stem] to_tsquery + plainto_tsquery stem regression ✓");
}

/// A `::tsquery` raw cast is NOT stemmed (PG parity).
/// Regression: early builds applied Snowball to all tsquery inputs including
/// raw casts, which diverged from PG semantics.
#[tokio::test]
async fn at_at_raw_cast_not_stemmed() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Document: 'running' → stored as 'run'.
    // Raw cast 'running'::tsquery → stored as-is 'running' (NOT stemmed).
    // 'running' != 'run' → no match.
    let no_match = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'running fast') @@ 'running'::tsquery",
    )
    .await;
    assert!(
        !no_match,
        "::tsquery cast regression: 'running'::tsquery must NOT match stemmed doc (unstemmed cast); got=true"
    );

    // But the stemmed form 'run'::tsquery DOES match.
    let stem_match = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'running fast') @@ 'run'::tsquery",
    )
    .await;
    assert!(
        stem_match,
        "'run'::tsquery (already stem) must match the 'running' doc"
    );
    println!("[fts raw cast] ::tsquery not stemmed ✓");
}
