//! Integration tests for `jsonb_set` on the UPDATE write path.
//!
//! Closes the "basin gap" shape recorded by the differential bench
//! (`compare_postgres_common.rs` shape #37):
//!
//!   UPDATE events SET payload = jsonb_set(payload, '{metadata,score}', '99')
//!
//! `jsonb_set` is a registered DataFusion scalar UDF. When it appears as the
//! RHS of an UPDATE SET, `dml_mutate::parse_assignments` cannot fold it into a
//! literal scalar, so it routes through `AssignmentRhs::Expr` →
//! `generated_cols::eval_expression`, which evaluates the call as a per-batch
//! DataFusion projection over the pre-update rows and merges the result back
//! into the matched rows via the mask. These tests assert that the nested
//! value is actually written and survives a subsequent SELECT.
//!
//! Drives `ProjectSession::execute` directly (no pgwire). JSONB columns come
//! back as Arrow `LargeBinary`, so the read helper decodes with `serde_json`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, LargeBinaryArray, StringArray};
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

async fn seed_events(sess: &basin_engine::ProjectSession) {
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .expect("CREATE TABLE events");

    // id 1 — device.version present (replace-existing path)
    // id 2 — device object present but no `version` key (create-missing path)
    // id 3 — control row, must be left untouched by a WHERE id = 1 UPDATE
    let rows = [
        (
            1,
            r#"{"category":"purchase","device":{"os":"ios","version":"1.2.3"}}"#,
        ),
        (2, r#"{"category":"signup","device":{"os":"android"}}"#),
        (3, r#"{"category":"click","device":{"os":"ios","version":"9.9.9"}}"#),
    ];
    for (id, payload) in rows {
        sess.execute(&format!("INSERT INTO events VALUES ({id}, '{payload}')"))
            .await
            .unwrap_or_else(|e| panic!("INSERT id={id}: {e}"));
    }
}

/// Fetch the full `payload` JSONB document for one `id` as a serde_json Value.
/// Decodes whichever physical type the engine returns (LargeBinary or Utf8).
async fn payload_for_id(sess: &basin_engine::ProjectSession, id: i64) -> Value {
    let sql = format!("SELECT payload FROM events WHERE id = {id}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let batch = batches.first().expect("no batches");
    assert_eq!(batch.num_rows(), 1, "expected exactly 1 row for id={id}");
    let col = batch.column(0);
    assert!(!col.is_null(0), "payload unexpectedly NULL for id={id}");
    if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        return serde_json::from_slice(arr.value(0))
            .unwrap_or_else(|e| panic!("decode binary payload id={id}: {e}"));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return serde_json::from_str(arr.value(0))
            .unwrap_or_else(|e| panic!("decode text payload id={id}: {e}"));
    }
    panic!("payload column unexpected type {:?}", col.data_type());
}

async fn id_count(sess: &basin_engine::ProjectSession) -> i64 {
    let batches = match sess.execute("SELECT COUNT(*) FROM events").await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("count failed: {other:?}"),
    };
    let col = batches[0].column(0);
    col.as_any()
        .downcast_ref::<Int64Array>()
        .expect("count Int64")
        .value(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Replace-existing path: `device.version` already exists; jsonb_set overwrites
/// it. The rest of the document (and other rows) must be untouched.
///
/// IGNORED pending a one-line fix in `generated_cols::eval_expression`
/// (`crates/basin-engine/src/generated_cols.rs`, ~line 93): that fn builds a
/// fresh `SessionContext` for SET-RHS expression evaluation but registers only
/// the distance / pg / pg_compat / fts UDFs — NOT the JSONB UDFs. So
/// `UPDATE … SET col = jsonb_set(…)` fails to plan with "Invalid function
/// 'jsonb_set'". Adding `crate::jsonb_udf::register_jsonb_udfs(&ctx);` (the
/// same call already used in session.rs:313) next to the existing
/// `register_fts_udfs(&ctx)` closes the gap; remove `#[ignore]` then. The
/// writeback path in dml_mutate.rs (AssignmentRhs::Expr → eval_expression)
/// already handles a UDF-valued RHS correctly.
#[tokio::test]
async fn jsonb_set_update_replaces_existing_nested_value() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    sess.execute(
        r#"UPDATE events SET payload = jsonb_set(payload, '{device,version}', '"2.0"') WHERE id = 1"#,
    )
    .await
    .expect("UPDATE jsonb_set replace-existing");

    let p1 = payload_for_id(&sess, 1).await;
    assert_eq!(
        p1.pointer("/device/version").and_then(Value::as_str),
        Some("2.0"),
        "device.version should be updated to 2.0; full doc: {p1}"
    );
    // Sibling fields preserved.
    assert_eq!(
        p1.pointer("/device/os").and_then(Value::as_str),
        Some("ios"),
        "device.os must survive jsonb_set"
    );
    assert_eq!(
        p1.pointer("/category").and_then(Value::as_str),
        Some("purchase"),
        "top-level category must survive jsonb_set"
    );

    // Control row id=3 must be unchanged.
    let p3 = payload_for_id(&sess, 3).await;
    assert_eq!(
        p3.pointer("/device/version").and_then(Value::as_str),
        Some("9.9.9"),
        "row outside the WHERE must not be mutated"
    );

    assert_eq!(id_count(&sess).await, 3, "row count must be stable");
}

/// Create-missing path: `device.version` does NOT exist on id=2; with the PG
/// default `create_missing = true`, jsonb_set adds it.
///
/// IGNORED for the same reason as `jsonb_set_update_replaces_existing_nested_value`.
#[tokio::test]
async fn jsonb_set_update_creates_missing_nested_value() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    sess.execute(
        r#"UPDATE events SET payload = jsonb_set(payload, '{device,version}', '"5.5"') WHERE id = 2"#,
    )
    .await
    .expect("UPDATE jsonb_set create-missing");

    let p2 = payload_for_id(&sess, 2).await;
    assert_eq!(
        p2.pointer("/device/version").and_then(Value::as_str),
        Some("5.5"),
        "device.version should be created; full doc: {p2}"
    );
    assert_eq!(
        p2.pointer("/device/os").and_then(Value::as_str),
        Some("android"),
        "existing device.os must survive create-missing"
    );
}

/// Top-level key set (single-segment path), and numeric replacement value.
///
/// IGNORED for the same reason as `jsonb_set_update_replaces_existing_nested_value`.
#[tokio::test]
async fn jsonb_set_update_top_level_and_numeric_value() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    // Add a brand-new top-level key carrying a JSON number.
    sess.execute(r#"UPDATE events SET payload = jsonb_set(payload, '{score}', '99') WHERE id = 1"#)
        .await
        .expect("UPDATE jsonb_set top-level numeric");

    let p1 = payload_for_id(&sess, 1).await;
    assert_eq!(
        p1.pointer("/score").and_then(Value::as_i64),
        Some(99),
        "top-level numeric score should be 99; full doc: {p1}"
    );
    // Pre-existing nested structure untouched.
    assert_eq!(
        p1.pointer("/device/version").and_then(Value::as_str),
        Some("1.2.3"),
        "nested device.version must survive an unrelated jsonb_set"
    );
}
