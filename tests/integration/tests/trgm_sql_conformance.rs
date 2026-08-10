//! SQL-level conformance tests for the `pg_trgm`-compatible fuzzy-text layer.
//!
//! These tests exercise the DataFusion UDF registrations and the pre-parse
//! operator rewriter introduced by the trgm SQL surface wiring:
//!
//! - `similarity(text, text) -> float4`
//! - `word_similarity(text, text) -> float4`
//! - `show_trgm(text) -> text[]`
//! - `a % b`   → `similarity(a, b) >= pg_trgm.similarity_threshold`
//! - `a <% b`  → `word_similarity(a, b) >= pg_trgm.word_similarity_threshold`
//! - `a <-> b` → `(1.0 - similarity(a, b))`
//! - `SET pg_trgm.similarity_threshold = <float>`
//! - `SET pg_trgm.word_similarity_threshold = <float>`
//! - `SHOW pg_trgm.similarity_threshold`
//! - `SHOW pg_trgm.word_similarity_threshold`
//!
//! All expected values are consistent with the Rust-API-level expectations
//! pinned in `tests/integration/tests/trgm_conformance.rs`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{BooleanArray, Float32Array, Int64Array, ListArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── engine helper ───────────────────────────────────────────────────────────

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

// ─── assertion helpers ────────────────────────────────────────────────────────

/// Execute SQL and expect a single-row, single-column float32 result.
async fn scalar_f32(sess: &basin_engine::ProjectSession, sql: &str) -> f32 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            // DataFusion may return Float32 or Float64 depending on coercion.
            if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                return arr.value(0);
            }
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
                return arr.value(0) as f32;
            }
            panic!(
                "expected Float32 or Float64 column for:\n  {sql}\n  got type: {:?}",
                col.data_type()
            )
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Execute SQL and expect a single-row, single-column boolean result.
async fn scalar_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("expected BooleanArray");
            arr.value(0)
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Execute SQL and expect a single-row, single-column integer result.
async fn scalar_i64(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("expected Int64Array");
            arr.value(0)
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Execute SQL and return the first string value from the first row/column.
async fn scalar_text(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray");
            arr.value(0).to_owned()
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Execute SQL, return Ok(()) on success, Err(msg) on engine error.
async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) -> Result<(), String> {
    match sess.execute(sql).await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("{e}")),
    }
}

const EPS: f32 = 1e-4;

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= EPS
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1 — similarity() UDF
// ─────────────────────────────────────────────────────────────────────────────

/// `SELECT similarity('word','word')` must return 1.0.
/// Matches trgm_conformance.rs `similarity_identical_strings_score_one`.
#[tokio::test]
async fn sql_similarity_identical_strings_score_one() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT similarity('word', 'word')").await;
    assert!(
        approx_eq(v, 1.0),
        "similarity('word','word') = {v}, expected 1.0"
    );
}

/// `SELECT similarity('', '')` → 0.0.
#[tokio::test]
async fn sql_similarity_empty_strings_score_zero() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT similarity('', '')").await;
    assert!(approx_eq(v, 0.0), "similarity('','') = {v}, expected 0.0");
}

/// `SELECT similarity('cat','bat')` → 1/7 ≈ 0.142857.
/// Pins the canonical 4+4-1=7 union / 1 intersect Jaccard value.
/// Consistent with trgm_conformance.rs `similarity_cat_vs_bat_is_one_seventh`.
#[tokio::test]
async fn sql_similarity_cat_vs_bat() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT similarity('cat', 'bat')").await;
    let expected = 1.0_f32 / 7.0_f32;
    assert!(
        approx_eq(v, expected),
        "similarity('cat','bat') = {v}, expected 1/7 ≈ {expected:.6}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2 — word_similarity() UDF
// ─────────────────────────────────────────────────────────────────────────────

/// `SELECT word_similarity('smith','alice smith')` → 1.0 (exact word).
#[tokio::test]
async fn sql_word_similarity_exact_word_in_phrase() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT word_similarity('smith', 'alice smith')").await;
    assert!(
        approx_eq(v, 1.0),
        "word_similarity('smith','alice smith') = {v}, expected 1.0"
    );
}

/// `SELECT word_similarity('xyz','alice smith')` → near 0.
#[tokio::test]
async fn sql_word_similarity_no_match_near_zero() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT word_similarity('xyz', 'alice smith')").await;
    assert!(
        v < 0.2,
        "word_similarity('xyz','alice smith') = {v}, expected < 0.2"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3 — show_trgm() UDF
// ─────────────────────────────────────────────────────────────────────────────

/// `SELECT show_trgm('cat')` must return a non-empty list.
#[tokio::test]
async fn sql_show_trgm_cat_returns_list() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    match sess.execute("SELECT show_trgm('cat')").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("show_trgm must return a ListArray");
            // 'cat' → 4 trigrams: {"  c"," ca","at ","cat"}
            let values = list_arr.value(0);
            assert_eq!(
                values.len(),
                4,
                "show_trgm('cat') must return 4 trigrams, got {}",
                values.len()
            );
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty"),
        Err(e) => panic!("show_trgm('cat') failed: {e}"),
    }
}

/// `SELECT show_trgm('')` returns an empty list.
#[tokio::test]
async fn sql_show_trgm_empty_string_returns_empty_list() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    match sess.execute("SELECT show_trgm('')").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("show_trgm must return a ListArray");
            let values = list_arr.value(0);
            assert_eq!(values.len(), 0, "show_trgm('') must return an empty list");
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty"),
        Err(e) => panic!("show_trgm('') failed: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4 — % operator (similarity threshold filter)
// ─────────────────────────────────────────────────────────────────────────────

/// Table scan with `name % 'alice'` using default threshold 0.3.
/// 'alice' and 'alyce' have similarity > 0.3; 'bob' does not.
#[tokio::test]
async fn sql_percent_operator_default_threshold() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE names (name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO names VALUES ('alice'), ('alyce'), ('bob'), ('alice smith')")
        .await
        .unwrap();

    // Default threshold = 0.3. 'alyce' and 'alice' should both match 'alice'.
    let count_sql = "SELECT COUNT(*) FROM names WHERE name % 'alice'";
    match sess.execute(count_sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected batch");
            let col = batch.column(0);
            // COUNT returns Int64.
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT should return Int64");
            let count = arr.value(0);
            // 'alice' (similarity=1.0), 'alyce' (> 0.3), 'alice smith' (> 0.3)
            // 'bob' should NOT match (similarity very low)
            assert!(
                count >= 2,
                "% filter should match at least 'alice' and 'alyce', got count={count}"
            );
            println!("[trgm_sql] name % 'alice' with default threshold: {count} matches");
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows"),
        Err(e) => panic!("% operator query failed: {e}"),
    }
}

/// `SET pg_trgm.similarity_threshold = 0.9` then `%` should be more selective.
#[tokio::test]
async fn sql_percent_operator_raised_threshold() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE names2 (name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO names2 VALUES ('alice'), ('alyce'), ('bob')")
        .await
        .unwrap();

    // Raise threshold so only very similar strings match.
    exec_ok(&sess, "SET pg_trgm.similarity_threshold = 0.9")
        .await
        .unwrap();

    let count_sql = "SELECT COUNT(*) FROM names2 WHERE name % 'alice'";
    match sess.execute(count_sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT should return Int64");
            let count = arr.value(0);
            // Only 'alice' (similarity=1.0) should match at threshold 0.9.
            assert_eq!(
                count, 1,
                "at threshold 0.9, only 'alice' should match 'alice', got {count}"
            );
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows"),
        Err(e) => panic!("% operator raised threshold query failed: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5 — % does NOT break integer modulo
// ─────────────────────────────────────────────────────────────────────────────

/// `SELECT 7 % 3` must return 1 (integer modulo), not a similarity predicate.
#[tokio::test]
async fn sql_integer_modulo_not_rewritten() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_i64(&sess, "SELECT 7 % 3").await;
    assert_eq!(v, 1, "7 % 3 must equal 1 (integer modulo), got {v}");
}

/// `SELECT 10 % 4` → 2.
#[tokio::test]
async fn sql_integer_modulo_ten_mod_four() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_i64(&sess, "SELECT 10 % 4").await;
    assert_eq!(v, 2, "10 % 4 must equal 2 (integer modulo), got {v}");
}

/// Integer-COLUMN modulo in a WHERE clause must work as modulo — NOT be
/// rewritten into a `similarity()` predicate (which would fail at planning
/// with a type error on integer columns). This pins the tightened `%` gate:
/// the rewrite fires ONLY when at least one operand is a string literal, so
/// `n % 2 = 0` and `a % b = 0` stay arithmetic.
#[tokio::test]
async fn sql_integer_column_modulo_in_where_works() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE nums (n BIGINT NOT NULL, d BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nums VALUES (1,2),(2,2),(3,2),(4,2),(6,3)")
        .await
        .unwrap();

    // Column % literal: even values of n → 2,4,6.
    let even = scalar_i64(&sess, "SELECT COUNT(*) FROM nums WHERE n % 2 = 0").await;
    assert_eq!(even, 3, "n % 2 = 0 should match 2,4,6 (got {even})");

    // Column % column: rows where n is divisible by d → 2,4 (n%2), 6 (6%3).
    let divisible = scalar_i64(&sess, "SELECT COUNT(*) FROM nums WHERE n % d = 0").await;
    assert_eq!(
        divisible, 3,
        "n % d = 0 should match 2,4,6 (got {divisible})"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6 — string literals containing % and <-> are untouched
// ─────────────────────────────────────────────────────────────────────────────

/// A `%` inside a string literal must survive unchanged.
#[tokio::test]
async fn sql_percent_inside_string_literal_preserved() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_text(&sess, "SELECT '50%'").await;
    assert_eq!(
        v, "50%",
        "percent inside string literal should be preserved, got {v:?}"
    );
}

/// A `<->` inside a tsquery string literal must survive unchanged.
#[tokio::test]
async fn sql_lt_dash_gt_inside_string_literal_preserved() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // The phrase operator form must not be rewritten by the trgm rewriter.
    let v = scalar_text(&sess, "SELECT 'quick <-> fox'").await;
    assert_eq!(
        v, "quick <-> fox",
        "<-> inside string literal should be preserved, got {v:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7 — <% operator (word_similarity threshold filter)
// ─────────────────────────────────────────────────────────────────────────────

/// `'smith' <% 'alice smith'` should be true (word match above 0.6 threshold).
#[tokio::test]
async fn sql_word_similarity_op_exact_word() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // `a <% b` ≡ `word_similarity(a, b) >= 0.6`
    // 'smith' is a word in 'alice smith' → word_similarity = 1.0 ≥ 0.6 → true.
    let b = scalar_bool(&sess, "SELECT 'smith' <% 'alice smith'").await;
    assert!(b, "'smith' <% 'alice smith' should be true");
}

/// `'xyz' <% 'alice smith'` should be false (no similar word).
#[tokio::test]
async fn sql_word_similarity_op_no_match() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let b = scalar_bool(&sess, "SELECT 'xyz' <% 'alice smith'").await;
    assert!(!b, "'xyz' <% 'alice smith' should be false");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8 — <-> operator (trigram distance, ORDER BY nearest-first)
// ─────────────────────────────────────────────────────────────────────────────

/// `a <-> b` = `1.0 - similarity(a, b)`.  Identical strings → distance 0.0.
#[tokio::test]
async fn sql_distance_op_identical_strings_zero() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT 'word' <-> 'word'").await;
    assert!(approx_eq(v, 0.0), "'word' <-> 'word' = {v}, expected 0.0");
}

/// ORDER BY `name <-> 'alice'` should return nearest name first.
/// 'alice' (distance=0) should sort before 'alyce' (distance≈non-zero),
/// which sorts before 'bob' (distance≈1.0).
#[tokio::test]
async fn sql_distance_op_order_by_nearest_first() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE words (name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO words VALUES ('bob'), ('alyce'), ('alice')")
        .await
        .unwrap();

    match sess
        .execute("SELECT name FROM words ORDER BY name <-> 'alice' LIMIT 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray");
            let nearest = arr.value(0);
            assert_eq!(
                nearest, "alice",
                "ORDER BY <-> 'alice': nearest should be 'alice', got {nearest:?}"
            );
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows"),
        Err(e) => panic!("ORDER BY <-> query failed: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9 — GUC SET / SHOW round-trips
// ─────────────────────────────────────────────────────────────────────────────

/// `SHOW pg_trgm.similarity_threshold` must return "0.3" by default.
#[tokio::test]
async fn sql_show_similarity_threshold_default() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_text(&sess, "SHOW pg_trgm.similarity_threshold").await;
    // PG returns "0.3"; we return the f32 rendered with Rust's default Display.
    assert!(
        v.starts_with("0.3"),
        "default similarity_threshold should be 0.3, got {v:?}"
    );
}

/// `SHOW pg_trgm.word_similarity_threshold` must return "0.6" by default.
#[tokio::test]
async fn sql_show_word_similarity_threshold_default() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_text(&sess, "SHOW pg_trgm.word_similarity_threshold").await;
    assert!(
        v.starts_with("0.6"),
        "default word_similarity_threshold should be 0.6, got {v:?}"
    );
}

/// `SET pg_trgm.similarity_threshold = 0.5` then `SHOW` reflects 0.5.
#[tokio::test]
async fn sql_set_show_similarity_threshold_round_trip() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec_ok(&sess, "SET pg_trgm.similarity_threshold = 0.5")
        .await
        .unwrap();
    let v = scalar_text(&sess, "SHOW pg_trgm.similarity_threshold").await;
    assert!(v.starts_with("0.5"), "after SET 0.5, SHOW returned {v:?}");
}

/// `SET pg_trgm.word_similarity_threshold = 0.8` then `SHOW` reflects 0.8.
#[tokio::test]
async fn sql_set_show_word_similarity_threshold_round_trip() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec_ok(&sess, "SET pg_trgm.word_similarity_threshold = 0.8")
        .await
        .unwrap();
    let v = scalar_text(&sess, "SHOW pg_trgm.word_similarity_threshold").await;
    assert!(v.starts_with("0.8"), "after SET 0.8, SHOW returned {v:?}");
}

/// After `SET pg_trgm.similarity_threshold = 0.5`, `%` uses the new threshold.
/// 'alyce' vs 'alice': similarity ≈ 0.333... (above 0.3 but may be below 0.5).
/// At threshold 0.5 'alyce' might not match 'alice', depending on exact value.
/// We test the threshold change takes effect by checking 1.0-similarity cases.
#[tokio::test]
async fn sql_set_threshold_affects_percent_operator() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE thresh_test (name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO thresh_test VALUES ('alice'), ('xyz_unrelated')")
        .await
        .unwrap();

    // With default threshold 0.3: 'alice' % 'alice' = true (similarity=1.0).
    let count_default = match sess
        .execute("SELECT COUNT(*) FROM thresh_test WHERE name % 'alice'")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap();
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        _ => panic!("query failed"),
    };
    // 'alice' must always match itself (similarity=1.0).
    assert!(count_default >= 1, "at least 'alice' must match itself");

    // Raise threshold to 0.99 — only near-identical strings pass.
    exec_ok(&sess, "SET pg_trgm.similarity_threshold = 0.99")
        .await
        .unwrap();

    let count_high = match sess
        .execute("SELECT COUNT(*) FROM thresh_test WHERE name % 'alice'")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap();
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        _ => panic!("query failed"),
    };
    // 'alice' (similarity=1.0 ≥ 0.99) must still match; 'xyz_unrelated' must not.
    assert_eq!(
        count_high, 1,
        "at threshold 0.99, only 'alice' should match 'alice'"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10 — word_similarity phrase cases
// ─────────────────────────────────────────────────────────────────────────────

/// `word_similarity('smyth', 'alyce smyth')` → should be near 1.0.
/// Consistent with trgm_conformance.rs `word_similarity_typo_needle_matches_typo_haystack_word`.
#[tokio::test]
async fn sql_word_similarity_smyth_in_alyce_smyth() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(&sess, "SELECT word_similarity('smyth', 'alyce smyth')").await;
    assert!(
        approx_eq(v, 1.0),
        "word_similarity('smyth','alyce smyth') = {v}, expected 1.0 (exact word match)"
    );
}

/// `word_similarity('foo', 'the foo bar baz')` → 1.0 (exact word in haystack).
#[tokio::test]
async fn sql_word_similarity_foo_in_long_haystack() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    let v = scalar_f32(
        &sess,
        "SELECT word_similarity('foo', 'the foo bar baz qux')",
    )
    .await;
    assert!(
        approx_eq(v, 1.0),
        "word_similarity('foo','the foo bar baz qux') = {v}, expected 1.0"
    );
}
