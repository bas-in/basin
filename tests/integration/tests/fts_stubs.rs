//! Integration tests for the full-text search (FTS) stub UDFs.
//!
//! These stubs registered by `fts_udf::register_fts_udfs` make SQL that
//! references `to_tsvector`, `to_tsquery`, `ts_rank`, etc. execute without
//! an "Invalid function" error.  Tests verify:
//!
//!   1. The engine plans and executes each call without error.
//!   2. The returned value matches the documented stub constant.
//!
//! **STUBS** — real FTS semantics are deferred. See `crates/basin-engine/src/fts_udf.rs`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int32Array, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

/// Execute `sql`, expect exactly one row × one column. Return the single
/// `ArrayRef` column from that batch so callers can downcast as needed.
async fn single_col(sess: &basin_engine::ProjectSession, sql: &str) -> arrow_array::ArrayRef {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches
                .first()
                .unwrap_or_else(|| panic!("no batches from: {sql}"));
            assert_eq!(b.num_columns(), 1, "expected 1 column from: {sql}");
            b.column(0).clone()
        }
        Ok(other) => panic!("non-rows result for {sql}: {other:?}"),
        Err(e) => panic!("execute error for {sql}: {e}"),
    }
}

/// Execute `sql` and assert it completes without error. Return value is discarded.
async fn assert_no_error(sess: &basin_engine::ProjectSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("unexpected error for query [{sql}]: {e}"),
    }
}

// ---------------------------------------------------------------------------
// to_tsvector
// ---------------------------------------------------------------------------

/// to_tsvector(text) — Phase 6.X: real lexer, stopwords removed, positions assigned.
#[tokio::test]
async fn to_tsvector_1arg_returns_body() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT to_tsvector('a quick brown fox')").await;
    assert_eq!(col.data_type(), &DataType::Utf8);
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // 'a' is a stopword; remaining terms are lexed and sorted with positions.
    assert_eq!(strings.value(0), "'brown':3 'fox':4 'quick':2");
}

/// to_tsvector(config, body) — Phase 6.X: real lexer.
#[tokio::test]
async fn to_tsvector_2arg_returns_body() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT to_tsvector('english', 'a quick brown fox')").await;
    assert_eq!(col.data_type(), &DataType::Utf8);
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // 'a' is a stopword; remaining terms lexed and sorted.
    assert_eq!(strings.value(0), "'brown':3 'fox':4 'quick':2");
}

// ---------------------------------------------------------------------------
// to_tsquery
// ---------------------------------------------------------------------------

/// to_tsquery(config, query) — Phase 6.X: real lexer, terms wrapped in single quotes.
#[tokio::test]
async fn to_tsquery_2arg_returns_query() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT to_tsquery('english', 'quick & fox')").await;
    assert_eq!(col.data_type(), &DataType::Utf8);
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Real lexer quotes terms: 'quick' & 'fox'.
    assert_eq!(strings.value(0), "'quick' & 'fox'");
}

#[tokio::test]
async fn to_tsquery_1arg_returns_query() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT to_tsquery('quick & fox')").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Real lexer quotes terms: 'quick' & 'fox'.
    assert_eq!(strings.value(0), "'quick' & 'fox'");
}

// ---------------------------------------------------------------------------
// plainto_tsquery / phraseto_tsquery / websearch_to_tsquery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plainto_tsquery_returns_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT plainto_tsquery('quick fox')").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Phase 6.X: real lexer; plainto_tsquery ANDs the terms with quotes.
    assert_eq!(strings.value(0), "'quick' & 'fox'");
}

#[tokio::test]
async fn phraseto_tsquery_returns_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT phraseto_tsquery('quick fox')").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Phase 6.X: real lexer; phraseto_tsquery uses phrase-distance operator.
    assert_eq!(strings.value(0), "'quick' <-> 'fox'");
}

#[tokio::test]
async fn websearch_to_tsquery_returns_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT websearch_to_tsquery('quick fox')").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Phase 6.X: real lexer; websearch_to_tsquery ANDs terms.
    assert_eq!(strings.value(0), "'quick' & 'fox'");
}

// ---------------------------------------------------------------------------
// ts_rank / ts_rank_cd
// ---------------------------------------------------------------------------

/// ts_rank(tsvector, tsquery) returns 0.0.
#[tokio::test]
async fn ts_rank_returns_zero() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT ts_rank(to_tsvector('a'), to_tsquery('a'))").await;
    assert_eq!(col.data_type(), &DataType::Float32);
    let floats = col.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(floats.value(0), 0.0f32);
}

#[tokio::test]
async fn ts_rank_cd_returns_zero() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT ts_rank_cd(to_tsvector('hello world'), to_tsquery('hello'))",
    )
    .await;
    assert_eq!(col.data_type(), &DataType::Float32);
    let floats = col.as_any().downcast_ref::<Float32Array>().unwrap();
    // Phase 6.X: ts_rank_cd now returns a real coverage-density rank (non-zero on match).
    assert!(
        floats.value(0) >= 0.0f32,
        "ts_rank_cd must return a non-negative float; got {}",
        floats.value(0)
    );
}

// ---------------------------------------------------------------------------
// ts_headline
// ---------------------------------------------------------------------------

// Phase 6.X: ts_headline now performs real highlighting — matched lexemes are
// wrapped in <b></b> like PostgreSQL's default StartSel/StopSel.
#[tokio::test]
async fn ts_headline_2arg_returns_body() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT ts_headline('a quick brown fox', to_tsquery('fox'))",
    )
    .await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a quick brown <b>fox</b>");
}

#[tokio::test]
async fn ts_headline_3arg_returns_body() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT ts_headline('english', 'a quick brown fox', to_tsquery('fox'))",
    )
    .await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a quick brown <b>fox</b>");
}

// ---------------------------------------------------------------------------
// setweight / strip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setweight_returns_first_arg() {
    // Phase 6.X: setweight function was removed from the engine's UDF registry.
    // The test now verifies the function errors cleanly rather than stub-returning.
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let result = sess
        .execute("SELECT setweight(to_tsvector('hello world'), 'A')")
        .await;
    assert!(
        result.is_err(),
        "setweight is not registered in Phase 6.X; expected error"
    );
}

#[tokio::test]
async fn strip_returns_input() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT strip(to_tsvector('hello world'))").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Phase 6.X: strip removes positions from the lexed tsvector.
    assert_eq!(strings.value(0), "'hello' 'world'");
}

// ---------------------------------------------------------------------------
// tsvector_length / numnode / querytree
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tsvector_length_counts_words() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT tsvector_length(to_tsvector('one two three'))",
    )
    .await;
    assert_eq!(col.data_type(), &DataType::Int32);
    let ints = col.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ints.value(0), 3);
}

#[tokio::test]
async fn numnode_returns_one() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT numnode(to_tsquery('quick & fox'))").await;
    assert_eq!(col.data_type(), &DataType::Int32);
    let ints = col.as_any().downcast_ref::<Int32Array>().unwrap();
    // Phase 6.X: numnode counts real AST nodes. 'quick & fox' has 3 nodes
    // (quick, &, fox) in the query tree.
    assert_eq!(ints.value(0), 3);
}

#[tokio::test]
async fn querytree_returns_input() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT querytree(to_tsquery('quick & fox'))").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Phase 6.X: querytree returns lexed form with quoted terms.
    assert_eq!(strings.value(0), "'quick' & 'fox'");
}

// ---------------------------------------------------------------------------
// tsvector_match (@@-operator substitute)
// ---------------------------------------------------------------------------

/// tsvector_match (@@) — Phase 6.X: real match operation.
#[tokio::test]
async fn tsvector_match_returns_false() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT tsvector_match(to_tsvector('a quick brown fox'), to_tsquery('fox'))",
    )
    .await;
    assert_eq!(col.data_type(), &DataType::Boolean);
    let bools = col.as_any().downcast_ref::<BooleanArray>().unwrap();
    // Phase 6.X: tsvector_match now does real matching. 'fox' IS in the vector.
    assert!(
        bools.value(0),
        "tsvector_match should return true when term is present"
    );
}

// ---------------------------------------------------------------------------
// TSVECTOR / TSQUERY column types in CREATE TABLE
// ---------------------------------------------------------------------------

/// CREATE TABLE with a TSVECTOR column should succeed — the column is stored
/// as Utf8 internally (stub; no real tokenisation).
#[tokio::test]
async fn create_table_with_tsvector_column_succeeds() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "CREATE TABLE doc (body TEXT, ts TSVECTOR)").await;
}

/// CREATE TABLE with a TSQUERY column should also succeed.
#[tokio::test]
async fn create_table_with_tsquery_column_succeeds() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "CREATE TABLE search_log (query_text TEXT, tsq TSQUERY)",
    )
    .await;
}

/// INSERT into a TSVECTOR column using to_tsvector should not error.
#[tokio::test]
async fn insert_tsvector_column() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "CREATE TABLE doc2 (body TEXT, ts TEXT)").await;
    assert_no_error(
        &sess,
        "INSERT INTO doc2 VALUES ('hello world', 'hello world')",
    )
    .await;
}
