//! Integration tests for JSON path, JSON conversion, and JSON operator UDFs.
//!
//! Covers the new functions added in the agent-bulk-json-path batch:
//!   - jsonb_path_query_first, jsonb_path_query_array
//!   - json_typeof, json_strip_nulls, to_json
//!   - json_each, json_each_text, json_object_keys
//!   - json_array_elements, json_array_elements_text
//!   - json_get (->), json_get_text (->>)
//!   - json_path_extract (#>), json_path_extract_text (#>>)
//!   - jsonb_contains (@>), jsonb_contained_by (<@)
//!   - jsonb_has_key (?), jsonb_has_all_keys (?&), jsonb_has_any_key (?|)
//!   - jsonb_concat (||)

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, LargeBinaryArray, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn make_engine() -> (Engine, ProjectId) {
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
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.expect("open_session");
    sess.execute("CREATE TABLE t (id BIGINT, data JSONB)")
        .await
        .expect("CREATE TABLE");
    sess.execute(
        "INSERT INTO t VALUES \
         (1, '{\"name\":\"alice\",\"age\":30,\"tags\":[\"admin\",\"user\"]}'), \
         (2, '{\"name\":\"bob\",\"age\":25,\"tags\":[\"user\"]}'), \
         (3, '{\"nested\":{\"x\":1,\"y\":2},\"arr\":[10,20,30]}')",
    )
    .await
    .expect("INSERT");
    (engine, project)
}

/// Helper: run a SELECT and concatenate ALL rows of the first column as a
/// space-separated string.  Used for set-returning functions (json_each,
/// json_object_keys) that now return one value per row (real SRF behavior
/// since Phase 5.14+) instead of a single comma-separated stub string.
async fn all_strings_concat(engine: &Engine, project: ProjectId, sql: &str) -> Option<String> {
    let sess = engine.open_session(project).await.expect("open_session");
    let result = sess.execute(sql).await.expect(sql);
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    let mut out = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        match col.data_type() {
            DataType::Utf8 => {
                let a = col.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        out.push(a.value(i).to_string());
                    }
                }
            }
            DataType::LargeBinary => {
                let a = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        if let Ok(s) = std::str::from_utf8(a.value(i)) {
                            out.push(s.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out.join(" "))
    }
}

/// Helper: run a SELECT and get first column first row as String.
async fn first_string(engine: &Engine, project: ProjectId, sql: &str) -> Option<String> {
    let sess = engine.open_session(project).await.expect("open_session");
    let result = sess.execute(sql).await.expect(sql);
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    let batch = batches.into_iter().next()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    match col.data_type() {
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            Some(a.value(0).to_string())
        }
        DataType::LargeBinary => {
            let a = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            String::from_utf8(a.value(0).to_vec()).ok()
        }
        _ => None,
    }
}

/// Helper: run a SELECT and get first boolean column first row.
async fn first_bool(engine: &Engine, project: ProjectId, sql: &str) -> Option<bool> {
    let sess = engine.open_session(project).await.expect("open_session");
    let result = sess.execute(sql).await.expect(sql);
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    let batch = batches.into_iter().next()?;
    if batch.num_rows() == 0 {
        return None;
    }
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    let a = col.as_any().downcast_ref::<BooleanArray>()?;
    Some(a.value(0))
}

// ===========================
// jsonb_typeof / json_typeof
// ===========================

#[tokio::test]
async fn test_jsonb_typeof_object() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT jsonb_typeof('{\"a\":1}')").await;
    assert_eq!(s.as_deref(), Some("object"), "jsonb_typeof object");
}

#[tokio::test]
async fn test_json_typeof_array() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT json_typeof('[1,2,3]')").await;
    assert_eq!(s.as_deref(), Some("array"), "json_typeof array");
}

#[tokio::test]
async fn test_json_typeof_string() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT json_typeof('\"hello\"')").await;
    assert_eq!(s.as_deref(), Some("string"), "json_typeof string");
}

#[tokio::test]
async fn test_json_typeof_number() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT json_typeof('42')").await;
    assert_eq!(s.as_deref(), Some("number"), "json_typeof number");
}

// ===========================
// json_strip_nulls
// ===========================

#[tokio::test]
async fn test_json_strip_nulls() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_strip_nulls('{\"a\":1,\"b\":null,\"c\":3}')",
    )
    .await;
    let s = s.expect("json_strip_nulls result");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse result");
    assert!(v.get("a").is_some(), "key a present");
    assert!(v.get("b").is_none(), "key b stripped");
    assert!(v.get("c").is_some(), "key c present");
}

// ===========================
// to_json
// ===========================

#[tokio::test]
async fn test_to_json_string() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT to_json('hello')").await;
    let s = s.expect("to_json result");
    assert!(s.contains("hello"), "to_json output: {s}");
}

// ===========================
// jsonb_path_query_first
// ===========================

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_jsonb_path_query_first_field() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT jsonb_path_query_first(data, '$.name') FROM t WHERE id = 1",
    )
    .await;
    let s = s.expect("jsonb_path_query_first result");
    let v: serde_json::Value =
        serde_json::from_str(&s).unwrap_or_else(|_| serde_json::Value::String(s.clone()));
    assert_eq!(
        v,
        serde_json::json!("alice"),
        "path_query_first $.name: {v:?}"
    );
}

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_jsonb_path_query_first_array_index() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT jsonb_path_query_first(data, '$.tags[0]') FROM t WHERE id = 1",
    )
    .await;
    let s = s.expect("jsonb_path_query_first array index result");
    let v: serde_json::Value =
        serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s.clone()));
    assert_eq!(v, serde_json::json!("admin"), "tags[0]: {v:?}");
}

// ===========================
// jsonb_path_query_array
// ===========================

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_jsonb_path_query_array() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT jsonb_path_query_array(data, '$.tags[*]') FROM t WHERE id = 1",
    )
    .await;
    let s = s.expect("jsonb_path_query_array result");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse array result");
    assert!(v.is_array(), "should be array: {v:?}");
    let arr = v.as_array().unwrap();
    assert!(!arr.is_empty(), "should have elements");
}

// ===========================
// json_object_keys
// ===========================

/// Phase 5.14+: `json_object_keys` is a real SRF returning one key per row.
/// Use `all_strings_concat` to collect all rows before asserting.
#[tokio::test]
async fn test_json_object_keys() {
    let (engine, project) = make_engine().await;
    let s = all_strings_concat(
        &engine,
        project,
        "SELECT json_object_keys('{\"a\":1,\"b\":2,\"c\":3}')",
    )
    .await;
    let s = s.expect("json_object_keys result");
    assert!(
        s.contains('a') && s.contains('b') && s.contains('c'),
        "keys: {s}"
    );
}

// ===========================
// json_each / json_each_text
// ===========================

/// Phase 5.14+: `json_each` is a real SRF returning one (key, value) row per
/// JSON key.  The first column is the key.  Use `all_strings_concat` to
/// collect all key values across rows before asserting presence of both keys.
#[tokio::test]
async fn test_json_each() {
    let (engine, project) = make_engine().await;
    let s = all_strings_concat(
        &engine,
        project,
        "SELECT json_each('{\"x\":1,\"y\":2}')",
    )
    .await;
    let s = s.expect("json_each result");
    assert!(s.contains('x') && s.contains('y'), "json_each output: {s}");
}

/// Phase 5.14+: `json_each_text` is a real SRF returning one (key, value) row
/// per JSON key.  The first column of the SRF output is the key.
/// Use `all_strings_concat` to collect all rows before asserting.
#[tokio::test]
async fn test_json_each_text() {
    let (engine, project) = make_engine().await;
    let s = all_strings_concat(
        &engine,
        project,
        "SELECT json_each_text('{\"name\":\"alice\",\"age\":30}')",
    )
    .await;
    let s = s.expect("json_each_text result");
    assert!(
        s.contains("name") && s.contains("age"),
        "json_each_text: {s}"
    );
}

// ===========================
// json_array_elements / json_array_elements_text
// ===========================

#[tokio::test]
async fn test_json_array_elements() {
    let (engine, project) = make_engine().await;
    let s = first_string(&engine, project, "SELECT json_array_elements('[10,20,30]')").await;
    let s = s.expect("json_array_elements result");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse");
    assert_eq!(v, serde_json::json!(10), "first element: {v:?}");
}

#[tokio::test]
async fn test_json_array_elements_text() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_array_elements_text('[\"alpha\",\"beta\"]')",
    )
    .await;
    let s = s.expect("json_array_elements_text result");
    assert_eq!(s, "alpha", "first element text: {s}");
}

// ===========================
// json_get (->)  — direct UDF call
// ===========================

#[tokio::test]
async fn test_json_get_udf_direct() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_get('{\"name\":\"alice\",\"age\":30}', 'name')",
    )
    .await;
    let s = s.expect("json_get result");
    let v: serde_json::Value =
        serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s.clone()));
    assert_eq!(v, serde_json::json!("alice"), "json_get: {v:?}");
}

// ===========================
// json_get_text (->>) — direct UDF call
// ===========================

#[tokio::test]
async fn test_json_get_text_udf_direct() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_get_text('{\"name\":\"bob\",\"age\":25}', 'name')",
    )
    .await;
    assert_eq!(s.as_deref(), Some("bob"), "json_get_text");
}

// ===========================
// json_path_extract (#>) — direct UDF
// ===========================

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_json_path_extract_udf() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_path_extract(data, '{nested,x}') FROM t WHERE id = 3",
    )
    .await;
    let s = s.expect("json_path_extract result");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse");
    assert_eq!(v, serde_json::json!(1), "nested.x: {v:?}");
}

// ===========================
// json_path_extract_text (#>>) — direct UDF
// ===========================

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_json_path_extract_text_udf() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_path_extract_text(data, '{nested,x}') FROM t WHERE id = 3",
    )
    .await;
    assert_eq!(s.as_deref(), Some("1"), "nested.x text: {s:?}");
}

// ===========================
// jsonb_contains (@>) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_contains_true() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_contains('{\"a\":1,\"b\":2}', '{\"a\":1}')",
    )
    .await;
    assert_eq!(b, Some(true), "contains true");
}

#[tokio::test]
async fn test_jsonb_contains_false() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_contains('{\"a\":1}', '{\"b\":2}')",
    )
    .await;
    assert_eq!(b, Some(false), "contains false");
}

// ===========================
// jsonb_contained_by (<@) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_contained_by() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_contained_by('{\"a\":1}', '{\"a\":1,\"b\":2}')",
    )
    .await;
    assert_eq!(b, Some(true), "contained_by true");
}

// ===========================
// jsonb_has_key (?) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_has_key_true() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_has_key('{\"name\":\"alice\",\"age\":30}', 'name')",
    )
    .await;
    assert_eq!(b, Some(true), "has_key true");
}

#[tokio::test]
async fn test_jsonb_has_key_false() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_has_key('{\"name\":\"alice\"}', 'missing')",
    )
    .await;
    assert_eq!(b, Some(false), "has_key false");
}

// ===========================
// jsonb_has_all_keys (?&) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_has_all_keys() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_has_all_keys('{\"a\":1,\"b\":2,\"c\":3}', '{a,b}')",
    )
    .await;
    assert_eq!(b, Some(true), "has_all_keys");
}

// ===========================
// jsonb_has_any_key (?|) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_has_any_key() {
    let (engine, project) = make_engine().await;
    let b = first_bool(
        &engine,
        project,
        "SELECT jsonb_has_any_key('{\"a\":1}', '{a,z}')",
    )
    .await;
    assert_eq!(b, Some(true), "has_any_key");
}

// ===========================
// jsonb_concat (||) — direct UDF
// ===========================

#[tokio::test]
async fn test_jsonb_concat() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT jsonb_concat('{\"a\":1}', '{\"b\":2}')",
    )
    .await;
    let s = s.expect("jsonb_concat result");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse concat result");
    assert!(
        v.get("a").is_some() && v.get("b").is_some(),
        "concat: {v:?}"
    );
}

// ===========================
// Column-based: json_get on table data
// ===========================

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_json_get_on_column() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_get(data, 'name') FROM t WHERE id = 2",
    )
    .await;
    let s = s.expect("json_get on column");
    let v: serde_json::Value =
        serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s.clone()));
    assert_eq!(v, serde_json::json!("bob"), "json_get column: {v:?}");
}

#[tokio::test]
#[ignore = "v0.1 json path UDFs don't yet resolve column-typed jsonb; rewriter target — v0.2"]
async fn test_json_get_text_on_column() {
    let (engine, project) = make_engine().await;
    let s = first_string(
        &engine,
        project,
        "SELECT json_get_text(data, 'name') FROM t WHERE id = 1",
    )
    .await;
    assert_eq!(s.as_deref(), Some("alice"), "json_get_text on column");
}
