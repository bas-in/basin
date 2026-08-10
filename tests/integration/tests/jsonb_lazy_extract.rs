//! End-to-end correctness tests for the LAZY / targeted JSONB extraction path.
//!
//! The `->`, `->>`, `#>`, `#>>`, `jsonb_typeof`, `jsonb_array_length`, and `?`
//! operators were re-implemented to walk the raw stored JSONB bytes and pull
//! ONLY the requested key/path, instead of parsing the whole document into a
//! `serde_json::Value` tree (see `crates/basin-engine/src/jsonb_udf.rs`,
//! `RawJson`). These tests drive those operators through the real engine over
//! a table of arbitrary, structurally-diverse payloads — nested objects,
//! arrays, nulls, unicode/escape-heavy strings, duplicate keys, and the full
//! number spectrum — and assert results identical to PostgreSQL 16.
//!
//! Fairness: the payloads and keys here are arbitrary fixtures for correctness;
//! the optimization itself special-cases no key name or value.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
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
    if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        return Some(serde_json::from_slice(arr.value(0)).unwrap());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Some(serde_json::from_str(arr.value(0)).unwrap());
    }
    panic!("unexpected column type for {sql}: {:?}", col.data_type());
}

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
        .unwrap_or_else(|| panic!("expected Utf8 column for: {sql}, got {:?}", col.data_type()));
    Some(arr.value(0).to_string())
}

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
// -> (json_get) : returns the sub-value as JSONB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arrow_get_object_and_array() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // object key present -> sub-object
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '{"a":{"b":1},"c":2}'::jsonb -> 'a'"#).await,
        Some(serde_json::json!({"b": 1}))
    );
    // object key present -> scalar
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '{"a":{"b":1},"c":2}'::jsonb -> 'c'"#).await,
        Some(serde_json::json!(2))
    );
    // missing key -> NULL
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '{"a":1}'::jsonb -> 'zzz'"#).await,
        None
    );
    // array index (string-keyed form, which the operator rewriter passes as a
    // Utf8 literal — the path `extract_string` + the integer parse handle).
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '[10,20,30]'::jsonb -> '1'"#).await,
        Some(serde_json::json!(20))
    );
    // array negative index (from end)
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '[10,20,30]'::jsonb -> '-1'"#).await,
        Some(serde_json::json!(30))
    );
    // array index out of range -> NULL
    assert_eq!(
        one_jsonb(&sess, r#"SELECT '[10,20,30]'::jsonb -> '9'"#).await,
        None
    );
}

// ---------------------------------------------------------------------------
// ->> (json_get_text) : returns the sub-value as text
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arrow_text_get() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // string value -> unquoted text
    assert_eq!(
        one_text(&sess, r#"SELECT '{"k":"hello"}'::jsonb ->> 'k'"#).await,
        Some("hello".to_string())
    );
    // number value -> its text form
    assert_eq!(
        one_text(&sess, r#"SELECT '{"k":42}'::jsonb ->> 'k'"#).await,
        Some("42".to_string())
    );
    // object value -> canonical JSON text
    assert_eq!(
        one_text(&sess, r#"SELECT '{"k":{"x":1}}'::jsonb ->> 'k'"#)
            .await
            .as_deref(),
        Some(r#"{"x":1}"#)
    );
    // JSON null value -> SQL NULL
    assert_eq!(
        one_text(&sess, r#"SELECT '{"k":null}'::jsonb ->> 'k'"#).await,
        None
    );
    // missing key -> NULL
    assert_eq!(
        one_text(&sess, r#"SELECT '{"k":1}'::jsonb ->> 'nope'"#).await,
        None
    );
}

// ---------------------------------------------------------------------------
// #> / #>> : path extraction (nested)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hash_path_extract() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let doc = r#"{"a":{"b":{"c":"deep"}},"arr":[{"x":1},{"x":2}]}"#;

    // #> nested object path -> JSONB
    assert_eq!(
        one_jsonb(&sess, &format!("SELECT '{doc}'::jsonb #> '{{a,b}}'")).await,
        Some(serde_json::json!({"c": "deep"}))
    );
    // #>> leaf string -> text
    assert_eq!(
        one_text(&sess, &format!("SELECT '{doc}'::jsonb #>> '{{a,b,c}}'")).await,
        Some("deep".to_string())
    );
    // #> path into array element by index
    assert_eq!(
        one_text(&sess, &format!("SELECT '{doc}'::jsonb #>> '{{arr,1,x}}'")).await,
        Some("2".to_string())
    );
    // missing path -> NULL
    assert_eq!(
        one_jsonb(&sess, &format!("SELECT '{doc}'::jsonb #> '{{a,nope}}'")).await,
        None
    );
    // type-mismatch mid-path (descend a scalar) -> NULL
    assert_eq!(
        one_jsonb(&sess, &format!("SELECT '{doc}'::jsonb #> '{{a,b,c,d}}'")).await,
        None
    );
}

// ---------------------------------------------------------------------------
// jsonb_typeof / jsonb_array_length / ?  (top-level structure only)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn typeof_arraylen_haskey() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for (lit, ty) in [
        (r#"{"a":1}"#, "object"),
        (r#"[1,2]"#, "array"),
        (r#""s""#, "string"),
        (r#"3.14"#, "number"),
        (r#"true"#, "boolean"),
        (r#"null"#, "null"),
    ] {
        assert_eq!(
            one_text(&sess, &format!("SELECT jsonb_typeof('{lit}'::jsonb)"))
                .await
                .as_deref(),
            Some(ty),
            "typeof {lit}"
        );
    }

    // jsonb_array_length
    assert_eq!(
        one_int(&sess, r#"SELECT jsonb_array_length('[1,2,3,4]'::jsonb)"#).await,
        Some(4)
    );
    assert_eq!(
        one_int(&sess, r#"SELECT jsonb_array_length('[]'::jsonb)"#).await,
        Some(0)
    );
    // nested arrays counted at top level only
    assert_eq!(
        one_int(
            &sess,
            r#"SELECT jsonb_array_length('[[1,2],[3,4],[5]]'::jsonb)"#
        )
        .await,
        Some(3)
    );

    // ? key exists
    assert_eq!(
        one_bool(&sess, r#"SELECT '{"a":1,"b":2}'::jsonb ? 'b'"#).await,
        Some(true)
    );
    assert_eq!(
        one_bool(&sess, r#"SELECT '{"a":1,"b":2}'::jsonb ? 'z'"#).await,
        Some(false)
    );
    // ? on an array tests string membership
    assert_eq!(
        one_bool(&sess, r#"SELECT '["x","y","z"]'::jsonb ? 'y'"#).await,
        Some(true)
    );
    assert_eq!(
        one_bool(&sess, r#"SELECT '["x","y","z"]'::jsonb ? 'q'"#).await,
        Some(false)
    );
}

// ---------------------------------------------------------------------------
// Edge payloads: unicode, escapes, duplicate keys, number spectrum.
// These all flow through the byte-scanner; results must equal PG semantics.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn edge_payloads_over_column() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE docs (id BIGINT NOT NULL, p JSONB)")
        .await
        .expect("CREATE TABLE");

    // Row payloads. SQL string literals: single quotes doubled.
    let rows = [
        // 1: unicode + emoji string values
        (1, r#"{"name":"café ☃","emoji":"😀","note":"naïve résumé"}"#),
        // 2: escaped chars inside string values
        (
            2,
            r#"{"path":"a\\b\\c","quote":"he said \"hi\"","tab":"x\ty"}"#,
        ),
        // 3: duplicate keys — last wins (serde / PG jsonb)
        (3, r#"{"k":1,"k":2,"k":3}"#),
        // 4: number spectrum
        (
            4,
            r#"{"neg":-42,"flt":3.5,"exp":6e2,"big":9223372036854775807,"zero":0}"#,
        ),
        // 5: nested + arrays + nulls
        (
            5,
            r#"{"outer":{"inner":[true,null,{"leaf":"v"}]},"empty":{}}"#,
        ),
    ];
    for (id, p) in rows {
        let esc = p.replace('\'', "''");
        sess.execute(&format!("INSERT INTO docs VALUES ({id}, '{esc}')"))
            .await
            .unwrap_or_else(|e| panic!("INSERT id={id}: {e}"));
    }

    // 1: unicode string extraction via ->>
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'name' FROM docs WHERE id = 1").await,
        Some("café ☃".to_string())
    );
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'emoji' FROM docs WHERE id = 1").await,
        Some("😀".to_string())
    );

    // 2: escaped chars decode correctly
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'path' FROM docs WHERE id = 2").await,
        Some(r"a\b\c".to_string())
    );
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'quote' FROM docs WHERE id = 2").await,
        Some(r#"he said "hi""#.to_string())
    );
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'tab' FROM docs WHERE id = 2").await,
        Some("x\ty".to_string())
    );

    // 3: duplicate key — last value wins
    assert_eq!(
        one_int(&sess, "SELECT (p ->> 'k')::bigint FROM docs WHERE id = 3").await,
        Some(3)
    );

    // 4: number spectrum round-trips through ->> as text
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'neg' FROM docs WHERE id = 4").await,
        Some("-42".to_string())
    );
    assert_eq!(
        one_text(&sess, "SELECT p ->> 'big' FROM docs WHERE id = 4").await,
        Some("9223372036854775807".to_string())
    );

    // 5: nested path #>> into array element object leaf
    assert_eq!(
        one_text(
            &sess,
            "SELECT p #>> '{outer,inner,2,leaf}' FROM docs WHERE id = 5"
        )
        .await,
        Some("v".to_string())
    );
    // typeof of an empty object
    assert_eq!(
        one_text(
            &sess,
            "SELECT jsonb_typeof(p -> 'empty') FROM docs WHERE id = 5"
        )
        .await
        .as_deref(),
        Some("object")
    );
    // ? key presence over a column
    assert_eq!(
        one_bool(&sess, "SELECT p ? 'outer' FROM docs WHERE id = 5").await,
        Some(true)
    );
    assert_eq!(
        one_bool(&sess, "SELECT p ? 'missing' FROM docs WHERE id = 5").await,
        Some(false)
    );
}

// ---------------------------------------------------------------------------
// Chained extraction over a column (the bench-shape pattern, arbitrary keys).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chained_extract_over_column() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, doc JSONB)")
        .await
        .expect("CREATE TABLE");
    sess.execute(
        r#"INSERT INTO t VALUES (1, '{"level1":{"level2":{"value":"found"}},"list":[9,8,7]}')"#,
    )
    .await
    .unwrap();

    // chained -> ... ->>
    assert_eq!(
        one_text(
            &sess,
            "SELECT doc -> 'level1' -> 'level2' ->> 'value' FROM t WHERE id = 1"
        )
        .await,
        Some("found".to_string())
    );
    // array element via chained -> (string-keyed index form)
    assert_eq!(
        one_text(&sess, "SELECT doc -> 'list' ->> '0' FROM t WHERE id = 1").await,
        Some("9".to_string())
    );
    // missing intermediate -> NULL (no panic)
    assert_eq!(
        one_text(
            &sess,
            "SELECT doc -> 'nope' ->> 'value' FROM t WHERE id = 1"
        )
        .await,
        None
    );
}
