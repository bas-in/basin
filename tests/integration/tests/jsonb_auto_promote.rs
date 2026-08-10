//! Integration tests for ADR 0027 Phase 4 automatic JSONB path promotion.
//!
//! Verifies that the engine promotes a JSON path to a shadow column when
//! that path is queried at or above [`AUTO_PROMOTE_MIN_HITS`] times, and
//! that paths queried fewer times are NOT promoted.
//!
//! Design principle: no key name (`category`, `device`, etc.) is special-
//! cased anywhere in the promotion logic; the threshold fires purely on
//! observed query frequency.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{jsonb_promotion::AUTO_PROMOTE_MIN_HITS, Engine, EngineConfig, ExecResult};
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

/// Seed a small table with arbitrary JSONB payloads.
async fn seed_table(sess: &basin_engine::ProjectSession, table: &str) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, payload JSONB)"
    ))
    .await
    .unwrap_or_else(|e| panic!("CREATE TABLE {table}: {e}"));

    // Insert rows with three independent keys so we can track them separately.
    sess.execute(&format!(
        "INSERT INTO {table} VALUES \
         (1, '{{\"color\":\"red\",\"weight\":10,\"tag\":\"a\"}}'), \
         (2, '{{\"color\":\"blue\",\"weight\":20,\"tag\":\"b\"}}'), \
         (3, '{{\"color\":\"green\",\"weight\":30,\"tag\":\"c\"}}')"
    ))
    .await
    .unwrap_or_else(|e| panic!("INSERT INTO {table}: {e}"));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Run `SELECT payload->>'<key>' FROM <table>` exactly N times and return
/// the results so the compiler doesn't optimise away the calls.
async fn run_extract_queries(sess: &basin_engine::ProjectSession, table: &str, key: &str, n: u64) {
    let sql = format!("SELECT payload->>'{}' FROM {}", key, table);
    for _ in 0..n {
        let res = sess.execute(&sql).await.unwrap();
        // Touch the result so the call can't be elided.
        match res {
            ExecResult::Rows { batches, .. } => {
                let _ = batches.len();
            }
            ExecResult::Empty { .. } => {}
        }
    }
}

/// After crossing the threshold, the path should be marked promoted in the
/// registry AND the catalog's `promoted_jsonb_paths` should include it.
#[tokio::test]
async fn promoted_at_threshold() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_table(&sess, "items").await;

    let registry = engine.jsonb_promotion_registry_for_test();
    let table = basin_common::TableName::new("items").unwrap();

    // Before threshold: not promoted.
    assert!(!registry.is_promoted(&project, &table, "payload", "color"));

    // Run exactly AUTO_PROMOTE_MIN_HITS queries.
    run_extract_queries(&sess, "items", "color", AUTO_PROMOTE_MIN_HITS).await;

    // Give the fire-and-forget tokio::spawn a moment to complete.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Registry must show promoted.
    assert!(
        registry.is_promoted(&project, &table, "payload", "color"),
        "path should be auto-promoted after {} queries",
        AUTO_PROMOTE_MIN_HITS,
    );

    // Hit count must be >= AUTO_PROMOTE_MIN_HITS.
    assert!(
        registry.hit_count(&project, &table, "payload", "color") >= AUTO_PROMOTE_MIN_HITS,
        "hit count too low: {}",
        registry.hit_count(&project, &table, "payload", "color"),
    );

    // The catalog must also reflect the promotion (InMemoryCatalog implements it).
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .expect("load_table");
    let promoted = meta
        .promoted_jsonb_paths
        .iter()
        .any(|p| p.source_col == "payload" && p.json_key == "color");
    assert!(
        promoted,
        "catalog should record the promoted path; got: {:?}",
        meta.promoted_jsonb_paths
    );
}

/// A path queried below the threshold must NOT be promoted.
#[tokio::test]
async fn not_promoted_below_threshold() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_table(&sess, "orders").await;

    let registry = engine.jsonb_promotion_registry_for_test();
    let table = basin_common::TableName::new("orders").unwrap();

    // Run strictly fewer queries than the threshold.
    let below = AUTO_PROMOTE_MIN_HITS - 1;
    run_extract_queries(&sess, "orders", "weight", below).await;

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert!(
        !registry.is_promoted(&project, &table, "payload", "weight"),
        "path should NOT be promoted after only {below} queries"
    );
    assert_eq!(
        registry.hit_count(&project, &table, "payload", "weight"),
        below,
        "hit count should be exactly {below}"
    );
}

/// Two different keys on the same column are tracked and promoted independently.
#[tokio::test]
async fn two_keys_tracked_independently() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_table(&sess, "logs").await;

    let registry = engine.jsonb_promotion_registry_for_test();
    let table = basin_common::TableName::new("logs").unwrap();

    // Key "tag" reaches threshold.
    run_extract_queries(&sess, "logs", "tag", AUTO_PROMOTE_MIN_HITS).await;
    // Key "weight" stays below threshold.
    run_extract_queries(&sess, "logs", "weight", AUTO_PROMOTE_MIN_HITS - 1).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        registry.is_promoted(&project, &table, "payload", "tag"),
        "'tag' should be promoted"
    );
    assert!(
        !registry.is_promoted(&project, &table, "payload", "weight"),
        "'weight' should NOT be promoted"
    );
}

/// A query that does NOT reference any JSON operator records zero JSON-path hits.
#[tokio::test]
async fn non_json_query_records_no_hits() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_table(&sess, "plain").await;

    let registry = engine.jsonb_promotion_registry_for_test();
    let table = basin_common::TableName::new("plain").unwrap();

    // Run many plain id-column queries — no JSONB operator.
    for _ in 0..AUTO_PROMOTE_MIN_HITS * 2 {
        let _ = sess.execute("SELECT id FROM plain").await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert_eq!(
        registry.hit_count(&project, &table, "payload", "color"),
        0,
        "plain query must not record JSON-path hits"
    );
    assert!(
        !registry.is_promoted(&project, &table, "payload", "color"),
        "no JSON key should be promoted from plain queries"
    );
}

/// Once promoted, repeated queries beyond the threshold do not fire the
/// catalog call a second time (idempotency guard in the registry).
#[tokio::test]
async fn promotion_fires_exactly_once() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_table(&sess, "events2").await;

    let registry = engine.jsonb_promotion_registry_for_test();
    let table = basin_common::TableName::new("events2").unwrap();

    // Far more queries than needed.
    run_extract_queries(&sess, "events2", "color", AUTO_PROMOTE_MIN_HITS * 5).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Promoted flag set.
    assert!(registry.is_promoted(&project, &table, "payload", "color"));

    // The catalog has exactly one entry for this path (not duplicated).
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let count = meta
        .promoted_jsonb_paths
        .iter()
        .filter(|p| p.source_col == "payload" && p.json_key == "color")
        .count();
    assert_eq!(
        count, 1,
        "promote_jsonb_path should be called exactly once per path"
    );
}
