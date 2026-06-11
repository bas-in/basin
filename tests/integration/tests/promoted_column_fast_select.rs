//! Integration test: ADR 0027 Phase 4 — promoted JSONB shadow column served
//! by the `fast_select` read path (bypassing DataFusion).
//!
//! ## What this verifies
//!
//! Once a JSONB path is promoted and every live data file carries the
//! materialised `__promoted$col$key` shadow column, a query of the shape
//! `SELECT payload->>'key' FROM t [WHERE <simple predicate>]` is served by
//! `fast_select` reading the shadow column directly — no per-row UDF dispatch,
//! no DataFusion plan.
//!
//! Three properties, all expressed against the always-correct DataFusion / UDF
//! answer (`json_get_text(payload, 'key')`):
//!
//! * **parity** — when all files are backfilled the fast path returns
//!   row-identical results to the UDF path;
//! * **mixed-file correctness guard** — when SOME files predate the promotion
//!   (shadow column absent) the fast path must NOT null-fill the missing column;
//!   it falls back to DataFusion and returns the correct extracted values;
//! * **routing proof** — the engine's `promoted_fast_select_count()` advances
//!   only when the shadow-column fast path actually engages (and stays put when
//!   the guard forces a fallback).

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

/// Build a shard + engine pair backed by a temp local filesystem.
async fn open_shard_engine() -> (TempDir, TempDir, Arc<dyn Wal>, Storage, Shard, Engine, ProjectId)
{
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );

    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });

    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    (storage_dir, wal_dir, wal, storage, shard, engine, project)
}

/// Execute `SELECT id, <expr> FROM ...` (no ORDER BY, so the query qualifies
/// for the `fast_select` path) and return the `<expr>` string values ordered by
/// `id` in the harness.  Sorting in Rust (rather than via SQL `ORDER BY`, which
/// would force the DataFusion path) keeps the result deterministic while
/// allowing the fast path to engage.
async fn query_id_string_col(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Vec<Option<String>> {
    let result = sess.execute(sql).await.unwrap();
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows result, got {:?}", other),
    };
    let mut rows: Vec<(i64, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("expected Int64 id column");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("expected Utf8 value column");
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                None
            } else {
                Some(vals.value(i).to_string())
            };
            rows.push((ids.value(i), v));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, v)| v).collect()
}

/// Parity + routing: after promotion AND a flush that backfills every file with
/// the shadow column, `SELECT payload->>'event_type' FROM events ORDER BY id`
/// returns row-identical results to the `json_get_text` UDF path AND is served
/// by the fast path (the promoted-fast-select counter advances).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_fast_select_parity_and_routing() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();

    // Insert rows, THEN promote, THEN flush so the flush materialises the shadow
    // column into the (only) cold-tier file via backfill.  After this every live
    // file carries `__promoted$payload$event_type`.
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (1, '{\"event_type\": \"login\",  \"user\": \"alice\"}'),\
           (2, '{\"event_type\": 42,        \"user\": \"bob\"}'),\
           (3, '{\"user\": \"carol\"}'),\
           (4, '{\"event_type\": null,      \"user\": \"dave\"}'),\
           (5, '{\"event_type\": \"logout\", \"user\": \"eve\"}')",
    )
    .await
    .unwrap();

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // The expected answer (== what json_get_text computes from the payloads).
    let expected = vec![
        Some("login".to_string()),
        Some("42".to_string()),
        None,
        None,
        Some("logout".to_string()),
    ];

    // Snapshot the counter, run the promoted `->>` query (no ORDER BY → eligible
    // for fast_select), and assert it advanced — i.e. the shadow-column fast
    // path served the query, not DataFusion.
    let before = engine.promoted_fast_select_count();
    let promoted_vals =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after = engine.promoted_fast_select_count();

    assert!(
        after > before,
        "promoted fast-select counter must advance (before={before}, after={after}) — \
         the shadow-column fast path should have served `payload->>'event_type'`"
    );
    assert_eq!(
        promoted_vals, expected,
        "promoted fast-select results must equal the json_get_text answer.\n\
         promoted: {promoted_vals:?}\n\
         expected: {expected:?}"
    );

    // Same query with a simple WHERE predicate on a real column also fast-paths.
    let before2 = engine.promoted_fast_select_count();
    let filtered =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events WHERE id = 1")
            .await;
    let after2 = engine.promoted_fast_select_count();
    assert!(after2 > before2, "filtered promoted `->>` should also fast-path");
    assert_eq!(filtered, vec![Some("login".to_string())]);

    wal.close().await.unwrap();
}

/// Mixed-file correctness guard: some files predate the promotion (no shadow
/// column) and some are written after.  Reading the shadow column from a
/// pre-promotion file would null-fill — a WRONG answer.  The guard must detect
/// the missing column on at least one file and fall back to the DataFusion /
/// UDF path, which returns the correct extracted values for ALL rows (no
/// spurious NULLs), and must NOT bump the promoted-fast-select counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_fast_select_mixed_files_guard() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();

    let table = TableName::new("events").unwrap();

    // ── File #1: written + flushed BEFORE promotion → no shadow column. ──────
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (1, '{\"event_type\": \"login\",  \"user\": \"alice\"}'),\
           (2, '{\"user\": \"bob\"}'),\
           (3, '{\"event_type\": \"signup\", \"user\": \"carol\"}')",
    )
    .await
    .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Promote the path now — file #1 already exists without the shadow column.
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();

    // ── File #2: written + flushed AFTER promotion → carries shadow column. ──
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (4, '{\"event_type\": \"purchase\", \"user\": \"dave\"}'),\
           (5, '{\"event_type\": null,        \"user\": \"eve\"}'),\
           (6, '{\"event_type\": 99,          \"user\": \"frank\"}')",
    )
    .await
    .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Sanity: confirm the file set is genuinely MIXED — at least one file lacks
    // the shadow column in its catalog `column_stats`, at least one has it.
    let shadow = "__promoted$payload$event_type";
    let meta = engine.config().catalog.load_table(&project, &table).await.unwrap();
    let files = meta.live_data_files();
    let with = files.iter().filter(|f| f.column_stats.contains_key(shadow)).count();
    let without = files.iter().filter(|f| !f.column_stats.contains_key(shadow)).count();
    assert!(
        with >= 1 && without >= 1,
        "test requires a MIXED file set: with_shadow={with}, without_shadow={without}, \
         files={}",
        files.len()
    );
    // Belt-and-braces: list_data_files agrees there are ≥2 files.
    let raw_files = storage.list_data_files(&project, &table).await.unwrap();
    assert!(raw_files.len() >= 2, "expected ≥2 files, got {}", raw_files.len());

    // Expected answer over ALL rows (== json_get_text from the payloads).
    let expected = vec![
        Some("login".to_string()),
        None,                          // row 2: absent key
        Some("signup".to_string()),
        Some("purchase".to_string()),
        None,                          // row 5: JSON null
        Some("99".to_string()),
    ];

    // The promoted `->>` query against the MIXED file set: the guard must NOT
    // rewrite to the shadow column (a missing column on file #1 would null-fill
    // rows 1/3), so the query is served via the UDF / DataFusion path and the
    // promoted fast-select counter must NOT advance.
    let before = engine.promoted_fast_select_count();
    let promoted_vals =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after = engine.promoted_fast_select_count();

    // Correctness: identical to the expected answer — NO spurious NULLs.
    assert_eq!(
        promoted_vals, expected,
        "mixed-file promoted `->>` must equal the json_get_text answer (no null-fill).\n\
         promoted: {promoted_vals:?}\n\
         expected: {expected:?}"
    );
    // Routing: the shadow-column fast path must NOT have served this query.
    assert_eq!(
        after, before,
        "promoted fast-select counter must NOT advance when the file set is mixed \
         (guard must leave the value as a json_get_text UDF call)"
    );

    // After backfilling the pre-promotion file, every file carries the shadow
    // column and the SAME query now DOES fast-path — still with correct values.
    let rewritten = shard
        .run_promoted_column_backfill_sweep(&project, &table)
        .await
        .unwrap();
    assert!(rewritten >= 1, "backfill sweep should rewrite ≥1 file, got {rewritten}");

    let before3 = engine.promoted_fast_select_count();
    let promoted_after_backfill =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after3 = engine.promoted_fast_select_count();
    assert!(
        after3 > before3,
        "after backfill the promoted `->>` query should fast-path (counter must advance)"
    );
    assert_eq!(
        promoted_after_backfill, expected,
        "post-backfill fast-path results must still equal the expected answer"
    );

    wal.close().await.unwrap();
}
