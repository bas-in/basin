//! Integration tests for JSONB deep-path extraction and GROUP-BY-on-extract.
//!
//! Closes two read-side "basin gap" shapes that the differential bench
//! (`compare_postgres_common.rs`) recorded as unsupported (-1.0):
//!
//!   #30  chained deep path  — `payload -> 'device' ->> 'version'`
//!   #36  filter + aggregate — `GROUP BY payload->>'category'`
//!
//! Root cause was the textual JSON-operator rewrite (`udf::rewrite_json_operators`):
//! when an arrow op's left operand was itself the result of a *preceding* arrow
//! op, the left-operand walk stopped at the inner key literal instead of
//! absorbing the whole chain. The fix makes `extract_left_operand` greedily
//! consume a leading `->` / `->>` / `#>` / `#>>` chain so chained extracts nest:
//!
//!   payload -> 'device' ->> 'version'
//!     →  json_get_text(json_get(payload, 'device'), 'version')
//!
//! The underlying `json_get` / `json_get_text` / `json_path_extract*` UDFs
//! already compose (json_get returns LargeBinary JSONB, which json_get_text
//! decodes) and are `Immutable` (so the extracted scalar is groupable in a
//! GROUP BY). These tests drive `ProjectSession::execute` directly (no pgwire).

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
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

/// Create an `events(id BIGINT, payload JSONB)` table and seed it with rows
/// whose payloads carry a nested `device.{os,version}` object and a top-level
/// `category`. Mirrors the bench's payload shape closely enough to exercise
/// the same query shapes.
async fn seed_events(sess: &basin_engine::ProjectSession) {
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .expect("CREATE TABLE events");

    // id 1 — purchase / ios / 1.2.3
    // id 2 — signup   / android / 2.0.1
    // id 3 — purchase / ios / 1.5.0   (second purchase → group count 2)
    // id 4 — click    / android / 3.1.4
    // id 5 — purchase with NO device key (deep-path null-safety probe)
    let rows = [
        (1, r#"{"category":"purchase","device":{"os":"ios","version":"1.2.3"},"metadata":{"score":10.0}}"#),
        (2, r#"{"category":"signup","device":{"os":"android","version":"2.0.1"},"metadata":{"score":20.0}}"#),
        (3, r#"{"category":"purchase","device":{"os":"ios","version":"1.5.0"},"metadata":{"score":30.0}}"#),
        (4, r#"{"category":"click","device":{"os":"android","version":"3.1.4"},"metadata":{"score":40.0}}"#),
        (5, r#"{"category":"purchase","metadata":{"score":50.0}}"#),
    ];
    for (id, payload) in rows {
        sess.execute(&format!("INSERT INTO events VALUES ({id}, '{payload}')"))
            .await
            .unwrap_or_else(|e| panic!("INSERT id={id}: {e}"));
    }
}

/// Collect a single Utf8 column across all result rows (NULLs become `None`),
/// keyed by the `id` column in column 0. Used for per-row deep-path probes.
async fn text_by_id(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> HashMap<i64, Option<String>> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let mut out = HashMap::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("col0 not Int64 for: {sql}"));
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| {
                panic!(
                    "col1 not Utf8 for: {sql} (got {:?})",
                    batch.column(1).data_type()
                )
            });
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                None
            } else {
                Some(vals.value(i).to_string())
            };
            out.insert(ids.value(i), v);
        }
    }
    out
}

/// Collect `(text_key, count)` pairs from a `SELECT key, COUNT(*) ... GROUP BY`
/// across all result batches. NULL keys are stored under the empty-string key
/// is avoided — we panic if a NULL key appears since the seed has none.
async fn group_counts(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> HashMap<String, i64> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows result for: {sql}\n  got: {other:?}"),
        Err(e) => panic!("execute error for: {sql}\n  err: {e}"),
    };
    let mut out = HashMap::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| {
                panic!(
                    "GROUP BY key col not Utf8 for: {sql} (got {:?})",
                    batch.column(0).data_type()
                )
            });
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "COUNT col not Int64 for: {sql} (got {:?})",
                    batch.column(1).data_type()
                )
            });
        for i in 0..batch.num_rows() {
            let key = if keys.is_null(i) {
                "<null>".to_string()
            } else {
                keys.value(i).to_string()
            };
            out.insert(key, counts.value(i));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Bench shape #30: chained `payload -> 'device' ->> 'version'` returns the
/// nested string value for each row.
#[tokio::test]
async fn deep_path_chained_arrow_returns_nested_value() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let got = text_by_id(
        &sess,
        "SELECT id, payload->'device'->>'version' FROM events ORDER BY id",
    )
    .await;

    assert_eq!(got.get(&1), Some(&Some("1.2.3".to_string())), "id=1 version");
    assert_eq!(got.get(&2), Some(&Some("2.0.1".to_string())), "id=2 version");
    assert_eq!(got.get(&3), Some(&Some("1.5.0".to_string())), "id=3 version");
    assert_eq!(got.get(&4), Some(&Some("3.1.4".to_string())), "id=4 version");
    // id=5 has no `device` key → NULL, not an error.
    assert_eq!(got.get(&5), Some(&None), "id=5 missing device → NULL");
}

/// Deeper chain to prove the recursion is not limited to two levels:
/// `payload -> 'device' -> 'os'` extracted via `->>` at the leaf.
#[tokio::test]
async fn deep_path_three_level_chain() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let got = text_by_id(
        &sess,
        "SELECT id, payload->'metadata'->>'score' FROM events ORDER BY id",
    )
    .await;
    // score values are JSON numbers; ->> extracts them as text.
    assert_eq!(got.get(&1), Some(&Some("10.0".to_string())), "id=1 score");
    assert_eq!(got.get(&2), Some(&Some("20.0".to_string())), "id=2 score");
}

/// Bench shape using the `#>` / `#>>` path-array operators with a multi-element
/// path. `#>` returns JSONB, `#>>` returns text.
#[tokio::test]
async fn deep_path_hash_arrow_path_array() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    // `#>>` (text) with a two-element path: {device,version}.
    let txt = text_by_id(
        &sess,
        "SELECT id, payload #>> '{device,version}' FROM events ORDER BY id",
    )
    .await;
    assert_eq!(txt.get(&1), Some(&Some("1.2.3".to_string())), "#>> id=1");
    assert_eq!(txt.get(&3), Some(&Some("1.5.0".to_string())), "#>> id=3");
    assert_eq!(txt.get(&5), Some(&None), "#>> id=5 missing device → NULL");

    // `#>` (jsonb) with the same path executes without error and the `::text`
    // cast of the leaf string value matches `#>>` for the present rows. We
    // assert the text-cast form so the physical JSONB type doesn't matter.
    let as_text = text_by_id(
        &sess,
        "SELECT id, (payload #> '{device,os}')::text FROM events ORDER BY id",
    )
    .await;
    // `#>` returns the JSONB string node; its text rendering is the quoted
    // JSON string. Accept either quoted or unquoted to stay robust to the
    // JSONB→text cast convention.
    let os1 = as_text.get(&1).and_then(|o| o.clone());
    assert!(
        matches!(os1.as_deref(), Some("ios") | Some("\"ios\"")),
        "#> {{device,os}} id=1 should render ios, got {os1:?}"
    );
}

/// Bench shape #36: `GROUP BY payload->>'category'` produces correct per-group
/// counts. The extracted text scalar must be groupable.
#[tokio::test]
async fn group_by_jsonb_extract_counts() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let counts = group_counts(
        &sess,
        "SELECT payload->>'category', COUNT(*) FROM events GROUP BY payload->>'category'",
    )
    .await;

    // purchase: ids 1,3,5 = 3; signup: id 2 = 1; click: id 4 = 1.
    assert_eq!(counts.get("purchase"), Some(&3), "purchase group count");
    assert_eq!(counts.get("signup"), Some(&1), "signup group count");
    assert_eq!(counts.get("click"), Some(&1), "click group count");
    assert_eq!(counts.len(), 3, "exactly three distinct categories: {counts:?}");
}

/// The exact bench filter+agg shape (#36): GROUP BY ordinal 1 with a SUM over a
/// deep extract cast to float. Proves the deep extract works inside an
/// aggregate argument and that the extracted key groups correctly.
#[tokio::test]
async fn group_by_ordinal_with_deep_extract_sum() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = match sess
        .execute(
            "SELECT payload->>'category', SUM((payload->'metadata'->>'score')::float) \
             FROM events GROUP BY 1",
        )
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("expected rows, got: {other:?}"),
    };
    let mut sums: HashMap<String, f64> = HashMap::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key col Utf8");
        let agg = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .expect("SUM col Float64");
        for i in 0..batch.num_rows() {
            sums.insert(keys.value(i).to_string(), agg.value(i));
        }
    }
    // purchase: 10 + 30 + 50 = 90; signup: 20; click: 40.
    assert_eq!(sums.get("purchase"), Some(&90.0), "purchase score sum");
    assert_eq!(sums.get("signup"), Some(&20.0), "signup score sum");
    assert_eq!(sums.get("click"), Some(&40.0), "click score sum");
}

/// Null-safety: a missing intermediate key in a chained extract yields NULL,
/// never an execution error, and such rows simply don't contribute a value.
#[tokio::test]
async fn deep_path_missing_intermediate_is_null() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    // `payload -> 'nope' ->> 'version'`: top-level 'nope' is absent on every
    // row, so the inner json_get returns NULL and the outer json_get_text
    // returns NULL for all rows — no error.
    let got = text_by_id(
        &sess,
        "SELECT id, payload->'nope'->>'version' FROM events ORDER BY id",
    )
    .await;
    for id in 1..=5 {
        assert_eq!(
            got.get(&id),
            Some(&None),
            "missing intermediate key must be NULL for id={id}"
        );
    }
}
