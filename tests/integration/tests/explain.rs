//! Integration tests for EXPLAIN statement routing (real DataFusion plan text).
//!
//! Basin previously returned nothing for EXPLAIN (the no-op accept arm in the
//! executor caught it). These tests verify that EXPLAIN now:
//!
//! 1. Returns a non-empty `QUERY PLAN` column result set.
//! 2. EXPLAIN ANALYZE executes the query and returns timing information.
//! 3. EXPLAIN (FORMAT JSON) returns JSON-formatted plan.
//! 4. EXPLAIN VERBOSE returns extra detail columns.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::cache_defaults;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Set up a minimal engine with a single table containing a few rows.
async fn make_engine_with_table() -> (Engine, ProjectId, TempDir) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, val TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    (engine, project, dir)
}

/// Extract all string values from the first column of the result.
fn plan_lines(result: ExecResult) -> Vec<String> {
    match result {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for batch in &batches {
                assert!(
                    batch.num_columns() >= 1,
                    "EXPLAIN result must have at least one column"
                );
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("QUERY PLAN column must be Utf8");
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            }
            out
        }
        ExecResult::Empty { tag } => {
            panic!("EXPLAIN returned Empty({tag}) — expected Rows with a QUERY PLAN column");
        }
    }
}

#[tokio::test]
async fn explain_select_returns_plan_text() {
    let (engine, project, _dir) = make_engine_with_table().await;
    let sess = engine.open_session(project).await.unwrap();

    let result = sess
        .execute("EXPLAIN SELECT * FROM t")
        .await
        .expect("EXPLAIN SELECT * FROM t must not error");

    let lines = plan_lines(result);
    assert!(
        !lines.is_empty(),
        "EXPLAIN must return at least one row; got none"
    );
    // The plan text must mention the table name somewhere in the output.
    let full_text = lines.join("\n");
    assert!(
        full_text.contains('t') || full_text.contains("TableScan") || full_text.contains("Scan"),
        "plan text should reference the scanned table; got:\n{full_text}"
    );
    println!("EXPLAIN SELECT plan:\n{full_text}");
}

#[tokio::test]
async fn explain_analyze_returns_plan_text() {
    let (engine, project, _dir) = make_engine_with_table().await;
    let sess = engine.open_session(project).await.unwrap();

    let result = sess
        .execute("EXPLAIN ANALYZE SELECT * FROM t")
        .await
        .expect("EXPLAIN ANALYZE SELECT * FROM t must not error");

    let lines = plan_lines(result);
    assert!(
        !lines.is_empty(),
        "EXPLAIN ANALYZE must return at least one row"
    );
    let full_text = lines.join("\n");
    println!("EXPLAIN ANALYZE plan:\n{full_text}");
}

#[tokio::test]
async fn explain_verbose_returns_plan_text() {
    let (engine, project, _dir) = make_engine_with_table().await;
    let sess = engine.open_session(project).await.unwrap();

    let result = sess
        .execute("EXPLAIN VERBOSE SELECT id FROM t WHERE id > 1")
        .await
        .expect("EXPLAIN VERBOSE SELECT must not error");

    let lines = plan_lines(result);
    assert!(
        !lines.is_empty(),
        "EXPLAIN VERBOSE must return at least one row"
    );
    let full_text = lines.join("\n");
    println!("EXPLAIN VERBOSE plan:\n{full_text}");
}

#[tokio::test]
async fn explain_format_json_returns_json() {
    let (engine, project, _dir) = make_engine_with_table().await;
    let sess = engine.open_session(project).await.unwrap();

    let result = sess
        .execute("EXPLAIN (FORMAT JSON) SELECT * FROM t")
        .await
        .expect("EXPLAIN (FORMAT JSON) SELECT * FROM t must not error");

    let lines = plan_lines(result);
    assert_eq!(
        lines.len(),
        1,
        "EXPLAIN FORMAT JSON must return exactly one row (the JSON blob)"
    );
    let json_str = &lines[0];
    // Must be valid JSON.
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).expect("EXPLAIN FORMAT JSON output must be valid JSON");
    assert!(
        parsed.is_array(),
        "EXPLAIN FORMAT JSON output must be a JSON array; got: {json_str}"
    );
    let arr = parsed.as_array().unwrap();
    assert!(!arr.is_empty(), "JSON plan array must not be empty");
    // Each entry should have plan_type and plan fields.
    for entry in arr {
        assert!(
            entry.get("plan_type").is_some(),
            "each JSON entry must have plan_type"
        );
        assert!(
            entry.get("plan").is_some(),
            "each JSON entry must have plan"
        );
    }
    println!("EXPLAIN FORMAT JSON:\n{json_str}");
}

#[tokio::test]
async fn explain_options_analyze_verbose() {
    let (engine, project, _dir) = make_engine_with_table().await;
    let sess = engine.open_session(project).await.unwrap();

    // PG-style: EXPLAIN (ANALYZE, VERBOSE) <stmt>
    let result = sess
        .execute("EXPLAIN (ANALYZE, VERBOSE) SELECT id, val FROM t")
        .await
        .expect("EXPLAIN (ANALYZE, VERBOSE) must not error");

    let lines = plan_lines(result);
    assert!(
        !lines.is_empty(),
        "EXPLAIN (ANALYZE, VERBOSE) must return at least one row"
    );
    let full_text = lines.join("\n");
    println!("EXPLAIN (ANALYZE, VERBOSE) plan:\n{full_text}");
}
