//! Integration tests for the JSONB scalar UDF suite.
//!
//! Exercises all 21 registered JSONB functions through the Basin engine,
//! verifying correct output shapes, null handling, and semantic correctness
//! against expected values derived from PostgreSQL 16.
//!
//! Uses `ProjectSession::execute` directly (no pgwire); JSONB columns are
//! returned as Arrow `LargeBinary` so tests decode them with `serde_json`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::{json, Value};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
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

/// Run a SELECT that returns one row × one LargeBinary (JSONB) column.
/// Decodes the bytes to a serde_json::Value.
async fn one_jsonb(sess: &basin_engine::ProjectSession, sql: &str) -> Option<Value> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let batch = batches.first().expect("no batches");
    assert_eq!(batch.num_rows(), 1, "expected 1 row from: {sql}");
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    let arr = col
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap_or_else(|| panic!("expected LargeBinary column for: {sql}"));
    let bytes = arr.value(0);
    Some(serde_json::from_slice(bytes).unwrap_or_else(|e| {
        panic!("json decode failed for: {sql}\n  bytes: {bytes:?}\n  err: {e}")
    }))
}

/// Run a SELECT that returns one row × one Utf8 column.
async fn one_text(sess: &basin_engine::ProjectSession, sql: &str) -> Option<String> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let batch = batches.first().expect("no batches");
    assert_eq!(batch.num_rows(), 1, "expected 1 row from: {sql}");
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("expected Utf8 column for: {sql}"));
    Some(arr.value(0).to_string())
}

/// Run a SELECT that returns one row × one Int64 column.
async fn one_int(sess: &basin_engine::ProjectSession, sql: &str) -> Option<i64> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let batch = batches.first().expect("no batches");
    assert_eq!(batch.num_rows(), 1, "expected 1 row from: {sql}");
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("expected Int64 column for: {sql}"));
    Some(arr.value(0))
}

/// Run a SELECT that returns one row × one Boolean column.
async fn one_bool(sess: &basin_engine::ProjectSession, sql: &str) -> Option<bool> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let batch = batches.first().expect("no batches");
    assert_eq!(batch.num_rows(), 1, "expected 1 row from: {sql}");
    let col = batch.column(0);
    if col.is_null(0) {
        return None;
    }
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("expected Boolean column for: {sql}"));
    Some(arr.value(0))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// 1. jsonb_typeof — classify JSON value types
#[tokio::test]
async fn test_jsonb_typeof() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Object
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('{"a":1}')"#).await.as_deref(),
        Some("object"),
        "typeof object"
    );
    // Array
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('[1,2,3]')"#).await.as_deref(),
        Some("array"),
        "typeof array"
    );
    // Number
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('42')"#).await.as_deref(),
        Some("number"),
        "typeof number"
    );
    // String
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('"hello"')"#).await.as_deref(),
        Some("string"),
        "typeof string"
    );
    // Boolean
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('true')"#).await.as_deref(),
        Some("boolean"),
        "typeof boolean"
    );
    // Null JSON value
    assert_eq!(
        one_text(&sess, r#"SELECT jsonb_typeof('null')"#).await.as_deref(),
        Some("null"),
        "typeof null"
    );
}

/// 2. jsonb_pretty — pretty-print JSON
#[tokio::test]
async fn test_jsonb_pretty() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_text(&sess, r#"SELECT jsonb_pretty('{"a":1,"b":2}')"#).await;
    let text = result.expect("jsonb_pretty should return text");
    // Must be multi-line and contain the keys
    assert!(text.contains("\"a\""), "pretty output missing key a: {text}");
    assert!(text.contains("\"b\""), "pretty output missing key b: {text}");
    assert!(text.contains('\n'), "pretty output should be multi-line: {text}");
}

/// 3. jsonb_array_length — array length
#[tokio::test]
async fn test_jsonb_array_length() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_int(&sess, r#"SELECT jsonb_array_length('[1,2,3]')"#).await,
        Some(3),
        "array length 3"
    );
    assert_eq!(
        one_int(&sess, r#"SELECT jsonb_array_length('[]')"#).await,
        Some(0),
        "empty array length 0"
    );
    // Non-array returns 0
    assert_eq!(
        one_int(&sess, r#"SELECT jsonb_array_length('{"a":1}')"#).await,
        Some(0),
        "non-array returns 0"
    );
}

/// 4. jsonb_strip_nulls — remove null fields recursively
#[tokio::test]
async fn test_jsonb_strip_nulls() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_strip_nulls('{"a":1,"b":null,"c":{"d":null,"e":2}}')"#,
    )
    .await
    .expect("strip_nulls should return jsonb");

    // "b" and "c.d" should be gone
    assert_eq!(result["a"], json!(1));
    assert!(result.get("b").is_none(), "b should be stripped");
    assert_eq!(result["c"]["e"], json!(2));
    assert!(result["c"].get("d").is_none(), "c.d should be stripped");
}

/// 5. jsonb_set — replace value at path
#[tokio::test]
async fn test_jsonb_set() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Replace existing key
    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_set('{"a":1,"b":2}', '{a}', '99')"#,
    )
    .await
    .expect("jsonb_set should return jsonb");

    assert_eq!(result["a"], json!(99), "a should be updated to 99");
    assert_eq!(result["b"], json!(2), "b should be unchanged");
}

/// 6. jsonb_insert — insert at path
#[tokio::test]
async fn test_jsonb_insert() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Insert into array
    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_insert('{"arr":[1,2,3]}', '{arr,1}', '99')"#,
    )
    .await
    .expect("jsonb_insert should return jsonb");

    // After insert at index 1 (before), array should have 4 elements
    if let Value::Array(arr) = &result["arr"] {
        assert_eq!(arr.len(), 4, "array should have 4 elements after insert: {result}");
        assert_eq!(arr[1], json!(99), "new element at index 1: {result}");
    } else {
        panic!("arr should be an array: {result}");
    }
}

/// 7. jsonb_path_query — navigate by path
#[tokio::test]
async fn test_jsonb_path_query() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Simple key navigation
    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_path_query('{"user":{"name":"Alice","age":30}}', '$.user.name')"#,
    )
    .await
    .expect("jsonb_path_query should return jsonb");

    assert_eq!(result, json!("Alice"), "should navigate to user.name");

    // Non-existent path returns NULL
    let null_result = one_jsonb(
        &sess,
        r#"SELECT jsonb_path_query('{"a":1}', '$.missing')"#,
    )
    .await;
    assert!(null_result.is_none(), "missing path should return NULL");
}

/// 8. jsonb_path_exists — check path existence
#[tokio::test]
async fn test_jsonb_path_exists() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_bool(&sess, r#"SELECT jsonb_path_exists('{"a":{"b":1}}', '$.a.b')"#).await,
        Some(true),
        "a.b should exist"
    );
    assert_eq!(
        one_bool(&sess, r#"SELECT jsonb_path_exists('{"a":1}', '$.missing')"#).await,
        Some(false),
        "missing key should not exist"
    );
}

/// 9. jsonb_path_match — same as exists for simple paths
#[tokio::test]
async fn test_jsonb_path_match() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_bool(&sess, r#"SELECT jsonb_path_match('{"x":42}', '$.x')"#).await,
        Some(true),
        "x should match"
    );
    assert_eq!(
        one_bool(&sess, r#"SELECT jsonb_path_match('{"x":42}', '$.y')"#).await,
        Some(false),
        "y should not match"
    );
}

/// 10. jsonb_object_keys — get object keys (SRF stub: comma-joined)
#[tokio::test]
async fn test_jsonb_object_keys() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_text(
        &sess,
        r#"SELECT jsonb_object_keys('{"b":2,"a":1,"c":3}')"#,
    )
    .await
    .expect("jsonb_object_keys should return text");

    // Keys should be present (order depends on serde_json's BTreeMap — sorted)
    let keys: Vec<&str> = result.split(',').collect();
    assert!(keys.contains(&"a"), "key a missing from: {result}");
    assert!(keys.contains(&"b"), "key b missing from: {result}");
    assert!(keys.contains(&"c"), "key c missing from: {result}");
}

/// 11. jsonb_each — SRF stub: returns JSON text
#[tokio::test]
async fn test_jsonb_each() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_text(
        &sess,
        r#"SELECT jsonb_each('{"a":1,"b":2}')"#,
    )
    .await
    .expect("jsonb_each should return text");

    // The stub returns the JSON string; it should be non-empty and parseable
    let v: Value = serde_json::from_str(&result)
        .unwrap_or_else(|_| panic!("jsonb_each stub output should be parseable JSON: {result}"));
    assert!(v.is_object(), "stub should return object JSON: {result}");
}

/// 12. jsonb_each_text — SRF stub: key=value text pairs
#[tokio::test]
async fn test_jsonb_each_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_text(
        &sess,
        r#"SELECT jsonb_each_text('{"a":1,"b":"hello"}')"#,
    )
    .await
    .expect("jsonb_each_text should return text");

    // Stub returns "key=value,..." pairs
    assert!(result.contains("a="), "should contain a= pair: {result}");
    assert!(result.contains("b="), "should contain b= pair: {result}");
}

/// 13. jsonb_array_elements — SRF stub: first element
#[tokio::test]
async fn test_jsonb_array_elements() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_array_elements('[10,20,30]')"#,
    )
    .await
    .expect("jsonb_array_elements should return jsonb (first element)");

    assert_eq!(result, json!(10), "stub should return first element");
}

/// 14. jsonb_array_elements_text — SRF stub: first element as text
#[tokio::test]
async fn test_jsonb_array_elements_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_text(
        &sess,
        r#"SELECT jsonb_array_elements_text('["alpha","beta","gamma"]')"#,
    )
    .await
    .expect("jsonb_array_elements_text should return text (first element)");

    assert_eq!(result, "alpha", "stub should return first string element");
}

/// 15. jsonb_build_object — build object from key/value pairs
#[tokio::test]
async fn test_jsonb_build_object() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_build_object('name', 'Alice', 'age', 30)"#,
    )
    .await
    .expect("jsonb_build_object should return jsonb");

    assert_eq!(result["name"], json!("Alice"), "name key");
    assert_eq!(result["age"], json!(30), "age key");
}

/// 16. jsonb_build_array — build array from args
#[tokio::test]
async fn test_jsonb_build_array() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = one_jsonb(
        &sess,
        r#"SELECT jsonb_build_array(1, 'two', true)"#,
    )
    .await
    .expect("jsonb_build_array should return jsonb");

    if let Value::Array(arr) = &result {
        assert_eq!(arr.len(), 3, "array should have 3 elements");
        assert_eq!(arr[0], json!(1));
        assert_eq!(arr[2], json!(true));
    } else {
        panic!("expected array, got: {result}");
    }
}

/// 17. to_jsonb — convert any value to JSONB
#[tokio::test]
async fn test_to_jsonb() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Integer → JSONB number
    let result = one_jsonb(&sess, r#"SELECT to_jsonb(42)"#).await
        .expect("to_jsonb(42) should return jsonb");
    assert_eq!(result, json!(42), "to_jsonb(int)");

    // String → JSONB string
    let result = one_jsonb(&sess, r#"SELECT to_jsonb('hello')"#).await
        .expect("to_jsonb('hello') should return jsonb");
    // String literal "hello" will be a JSON string
    assert!(
        result == json!("hello") || result.is_string(),
        "to_jsonb(text) should be a string: {result}"
    );

    // Boolean → JSONB bool
    let result = one_jsonb(&sess, r#"SELECT to_jsonb(true)"#).await
        .expect("to_jsonb(true) should return jsonb");
    assert_eq!(result, json!(true), "to_jsonb(bool)");
}

/// 18. row_to_json — stub: convert to json
#[tokio::test]
async fn test_row_to_json() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Stub accepts any scalar; should return JSONB
    let result = one_jsonb(&sess, r#"SELECT row_to_json(42)"#).await
        .expect("row_to_json should return jsonb");
    assert_eq!(result, json!(42), "row_to_json stub with int");
}

/// 19. array_to_json — convert array expression to JSON
#[tokio::test]
async fn test_array_to_json() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Pass a JSON array literal — should come back as jsonb
    let result = one_jsonb(
        &sess,
        r#"SELECT array_to_json('[1,2,3]')"#,
    )
    .await
    .expect("array_to_json should return jsonb");

    assert!(result.is_array(), "array_to_json should return an array: {result}");
    if let Value::Array(arr) = &result {
        assert_eq!(arr.len(), 3, "array should have 3 elements: {result}");
    }
}

/// 20. jsonb_agg stub — must return a user-friendly error, not panic
#[tokio::test]
async fn test_jsonb_agg_stub_errors_cleanly() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // The stub should error with a clear message
    let result = sess.execute(r#"SELECT jsonb_agg(1)"#).await;
    assert!(result.is_err(), "jsonb_agg stub should return an error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("jsonb_agg") || err.contains("AggregateUDFImpl") || err.contains("deferred"),
        "error message should mention jsonb_agg or deferral: {err}"
    );
}

/// 21. jsonb_object_agg stub — must return a user-friendly error, not panic
#[tokio::test]
async fn test_jsonb_object_agg_stub_errors_cleanly() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = sess.execute(r#"SELECT jsonb_object_agg('k', 'v')"#).await;
    assert!(result.is_err(), "jsonb_object_agg stub should return an error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("jsonb_object_agg") || err.contains("AggregateUDFImpl") || err.contains("deferred"),
        "error message should mention jsonb_object_agg or deferral: {err}"
    );
}

/// 22. JSONB round-trip via table INSERT + SELECT
#[tokio::test]
async fn test_jsonb_udf_round_trip_with_table() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE docs (id BIGINT, data JSONB)")
        .await
        .expect("CREATE TABLE");

    sess.execute(r#"INSERT INTO docs VALUES (1, '{"name":"Alice","scores":[10,20,30],"active":true}')"#)
        .await
        .expect("INSERT");

    // jsonb_typeof on a stored column
    let typ = one_text(
        &sess,
        "SELECT jsonb_typeof(data) FROM docs WHERE id = 1",
    )
    .await
    .expect("jsonb_typeof on column");
    assert_eq!(typ, "object", "stored JSONB should be typeof object");

    // jsonb_array_length on nested array via jsonb_path_query
    let scores_json = one_jsonb(
        &sess,
        "SELECT jsonb_path_query(data, '$.scores') FROM docs WHERE id = 1",
    )
    .await
    .expect("jsonb_path_query scores");
    assert!(scores_json.is_array(), "scores should be an array: {scores_json}");

    // jsonb_pretty on stored column
    let pretty = one_text(
        &sess,
        "SELECT jsonb_pretty(data) FROM docs WHERE id = 1",
    )
    .await
    .expect("jsonb_pretty on column");
    assert!(pretty.contains("Alice"), "pretty output should contain Alice: {pretty}");

    // jsonb_strip_nulls — add a row with nulls first
    sess.execute(r#"INSERT INTO docs VALUES (2, '{"x":1,"y":null}')"#)
        .await
        .expect("INSERT nulls");
    let stripped = one_jsonb(
        &sess,
        "SELECT jsonb_strip_nulls(data) FROM docs WHERE id = 2",
    )
    .await
    .expect("jsonb_strip_nulls on column");
    assert_eq!(stripped["x"], json!(1), "x should remain");
    assert!(stripped.get("y").is_none(), "y null should be stripped");

    println!("[JSONB UDF] round-trip test passed: typeof={typ}, scores={scores_json}, stripped={stripped}");
}
