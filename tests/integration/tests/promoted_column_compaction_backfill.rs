//! Integration test: ADR 0027 Phase 4 — promoted JSONB shadow column backfill
//! at compaction time.
//!
//! ## What this verifies
//!
//! When a JSONB path is promoted AFTER rows have already been written to the
//! shard tail (simulating rows written before the promotion), the compactor must
//! backfill the shadow column for those pre-promotion rows.  Without the
//! backfill, DataFusion's missing-column fallback returns NULL for old rows,
//! which is incorrect.
//!
//! ## Test plan
//!
//! 1. Build a shard + engine pair with an in-memory catalog.
//! 2. CREATE TABLE with a JSONB column and a small `basin.row_block_size`.
//! 3. INSERT rows via the engine SQL path (they land in the shard tail without
//!    any shadow column — the promotion has not happened yet).
//! 4. Call `catalog.promote_jsonb_path(...)` to register the promotion.
//! 5. Call `shard.flush_to_parquet()` (synchronous compaction).
//! 6. Run `SELECT payload->>'event_type' FROM events` and assert:
//!    (a) Pre-promotion rows return their correct values (not NULL).
//!    (b) The values match what `json_get_text` returns over the raw payload.
//!    (c) Rows whose key is absent return NULL.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Execute SQL and collect all string values from the first column.
async fn query_string_col(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Vec<Option<String>> {
    let result = sess.execute(sql).await.unwrap();
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows result, got {:?}", other),
    };
    let mut out: Vec<Option<String>> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let arr = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("expected Utf8 column");
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_column_compaction_backfill() {
    basin_common::telemetry::try_init_for_tests();

    // ── 1. Build shard + engine ──────────────────────────────────────────────
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(
            LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
        ),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap(),
            ),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );

    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));

    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });

    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    // ── 2. CREATE TABLE with a JSONB column ──────────────────────────────────
    // Use a small row_block_size to keep the test file small.
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id      BIGINT NOT NULL,\
            payload JSONB\
         ) WITH (basin.row_block_size = '256')",
    )
    .await
    .unwrap();

    // ── 3. INSERT rows BEFORE the path is promoted ───────────────────────────
    // Row A: has the key we will promote.
    // Row B: has the key with a non-string value (number).
    // Row C: does NOT have the key at all (absent-key → NULL).
    // Row D: has the key but its value is JSON null → NULL.
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (1, '{\"event_type\": \"login\", \"user\": \"alice\"}'),\
           (2, '{\"event_type\": 42, \"user\": \"bob\"}'),\
           (3, '{\"user\": \"carol\"}'),\
           (4, '{\"event_type\": null, \"user\": \"dave\"}')",
    )
    .await
    .unwrap();

    // Verify rows are in the shard tail (not yet flushed).
    // We deliberately do NOT call flush_to_parquet yet.

    // ── 4. Promote the JSONB path AFTER insertion ────────────────────────────
    // This simulates the real-world scenario where the path is promoted after
    // historical data exists.
    let table = TableName::new("events").unwrap();
    catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();

    // Verify the promotion is visible in table metadata.
    let meta = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(meta.promoted_jsonb_paths.len(), 1);
    assert_eq!(meta.promoted_jsonb_paths[0].source_col, "payload");
    assert_eq!(meta.promoted_jsonb_paths[0].json_key, "event_type");
    assert_eq!(
        meta.promoted_jsonb_paths[0].shadow_col_name(),
        "__promoted$payload$event_type"
    );

    // ── 5. Compact (flush tail → Parquet) ────────────────────────────────────
    // The compactor reads the promoted_jsonb_paths from the catalog and runs
    // backfill_promoted_columns on the merged batch before writing to storage.
    shard.flush_to_parquet().await.unwrap();

    // ── 6a. Assert pre-promotion rows return correct values ──────────────────
    // The planner is not yet wired to rewrite payload->>'event_type' to the
    // shadow column (that's the parallel agent's work). So we read the shadow
    // column directly by name to verify the backfill populated it correctly.
    let shadow_vals = query_string_col(
        &sess,
        "SELECT \"__promoted$payload$event_type\" FROM events ORDER BY id",
    )
    .await;

    assert_eq!(
        shadow_vals.len(),
        4,
        "expected 4 rows, got {}",
        shadow_vals.len()
    );

    // Row 1: event_type = "login" → Some("login")
    assert_eq!(
        shadow_vals[0],
        Some("login".to_string()),
        "row 1 (string value) should be 'login', got {:?}",
        shadow_vals[0]
    );

    // Row 2: event_type = 42 (number) → Some("42") (canonical JSON text)
    assert_eq!(
        shadow_vals[1],
        Some("42".to_string()),
        "row 2 (number value) should be '42', got {:?}",
        shadow_vals[1]
    );

    // Row 3: event_type absent → None (NULL)
    assert_eq!(
        shadow_vals[2],
        None,
        "row 3 (absent key) should be NULL, got {:?}",
        shadow_vals[2]
    );

    // Row 4: event_type = null → None (NULL)
    assert_eq!(
        shadow_vals[3],
        None,
        "row 4 (JSON null value) should be NULL, got {:?}",
        shadow_vals[3]
    );

    // ── 6b. Parity: shadow column == json_get_text(payload, 'event_type') ────
    // Both paths must return identical values for correctness.
    let jsonb_vals = query_string_col(
        &sess,
        "SELECT json_get_text(payload, 'event_type') FROM events ORDER BY id",
    )
    .await;

    assert_eq!(
        shadow_vals, jsonb_vals,
        "shadow column values must be byte-identical to json_get_text results.\n\
         shadow: {shadow_vals:?}\n\
         jsonb:  {jsonb_vals:?}"
    );

    // ── 6c. Idempotency: re-promoting the same path is a no-op ───────────────
    catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();
    let meta2 = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(
        meta2.promoted_jsonb_paths.len(),
        1,
        "re-registering the same path should be idempotent (still 1 entry)"
    );

    wal.close().await.unwrap();
}
