//! Integration test: GIN row-group re-indexing after compaction.
//!
//! Verifies that when a table with a JSONB GIN index is written via the
//! shard INSERT path (WAL+tail) and then compacted into a Parquet file,
//! the compactor re-indexes the GIN row-group bloom summaries so that
//! subsequent `@>` containment queries can prune at row-group granularity.
//!
//! Test plan:
//!   1. Build a shard+engine pair with an in-memory catalog.
//!   2. CREATE TABLE with `WITH (basin.row_block_size = 256)` and a GIN
//!      index on a JSONB column so compacted files span multiple row-groups.
//!   3. INSERT rows via SQL (shard path: WAL+tail) in multiple batches so
//!      the merged compacted file will exceed one row-group.
//!   4. Trigger compaction via `shard.flush_to_parquet()`.
//!   5. Assert that the row-group bloom registry has a sealed entry for the
//!      compacted file's path (proves re-indexing ran).
//!   6. Run `@>` queries:
//!        a. A needle whose matching row lands in a NON-ZERO row-group of
//!           the compacted file — must return the correct row (no false
//!           negative from incorrect row-group assignment).
//!        b. A needle that matches NO row — must return 0 rows.
//!        c. A common-key needle that matches ALL rows sharing that key —
//!           must return the full matching set.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a shard+engine pair using local filesystem storage.
/// Engine::new wires the GIN row-group registry and query-history adapter
/// into the shard automatically.
async fn build_engine_with_shard() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    Arc<dyn Wal>,
) {
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
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    // Engine::new wires GIN row-group registry and query history into the shard.
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (storage_dir, wal_dir, engine, shard, wal)
}

/// Collect all rows returned by a SELECT, returning them as a vec of
/// JSON-formatted strings of the `doc` column (for easy assertion).
async fn collect_doc_rows(engine: &Engine, project: ProjectId, sql: &str) -> Vec<String> {
    use arrow_array::{Array, LargeBinaryArray};

    let sess = engine.open_session(project).await.unwrap();
    let result = sess.execute(sql).await.unwrap();
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        _ => return Vec::new(),
    };

    let mut rows = Vec::new();
    for batch in &batches {
        let col_idx = batch.schema().index_of("doc").unwrap_or(0);
        let col = batch.column(col_idx);
        if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    continue;
                }
                let bytes = arr.value(i);
                let s = String::from_utf8_lossy(bytes).to_string();
                rows.push(s);
            }
        }
    }
    rows
}

/// Run a `SELECT COUNT(*) …` and return the single i64 scalar.
/// Panics if the result is not a single-row, single-column Int64 batch.
async fn scalar_count(engine: &Engine, project: ProjectId, sql: &str) -> i64 {
    use arrow_array::{Array, Int64Array};

    let sess = engine.open_session(project).await.unwrap();
    let result = sess.execute(sql).await.unwrap_or_else(|e| {
        panic!("execute failed for:\n  {sql}\n  error: {e}");
    });
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows for COUNT(*), got {other:?} for:\n  {sql}"),
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "COUNT(*) must return exactly one row, got {total_rows} for:\n  {sql}"
    );
    let batch = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    let col = batch.column(0);
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("COUNT(*) column not Int64 for:\n  {sql}"));
    arr.value(0)
}

/// Regression: `COUNT(*) … WHERE col @> '…'` over a GIN-indexed, shard-written
/// + compacted JSONB column must:
///   1. route through the `GinRowGroupPrunedTable` scan path WITHOUT panicking
///      on a `Binary` vs `LargeBinary` schema mismatch (the cold Vortex decode
///      narrows JSONB `LargeBinary` → `Binary`; the scan now normalizes), and
///   2. return the correct count for matching, non-matching, and common-key
///      needles — including `0` (a single row, not an empty result) for a
///      no-match needle — while a fast-path UPDATE overlay is present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_count_star_at_gt_rowgroup_pruned() {
    basin_common::telemetry::try_init_for_tests();

    let (_storage_dir, _wal_dir, engine, shard, wal) = build_engine_with_shard().await;

    let project = ProjectId::new();
    engine.config().catalog.create_namespace(&project).await.unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE events (\
            id      BIGINT NOT NULL, \
            payload JSONB  NOT NULL \
         ) WITH (basin.row_block_size = 256)",
    )
    .await
    .unwrap();
    sess.execute("CREATE INDEX events_gin ON events USING gin (payload)")
        .await
        .unwrap();

    // Seed via the shard INSERT path (WAL + tail). 600 rows: every 3rd row is a
    // "purchase" (200 rows), the rest are "view" (400 rows). Spreads across
    // multiple row-groups with row_block_size=256.
    let mut expected_purchase = 0i64;
    for i in 0..600i64 {
        let category = if i % 3 == 0 {
            expected_purchase += 1;
            "purchase"
        } else {
            "view"
        };
        sess.execute(&format!(
            "INSERT INTO events (id, payload) VALUES ({i}, '{{\"category\":\"{category}\",\"id\":{i}}}')"
        ))
        .await
        .unwrap();
    }

    // Compact into a single Parquet file → GIN row-group re-index runs.
    shard.flush_to_parquet().await.unwrap();

    let table = TableName::new("events").unwrap();
    let meta = engine.config().catalog.load_table(&project, &table).await.unwrap();
    let live_files = meta.live_data_files();
    let compacted_path = live_files[0].path.to_string();
    let rg_registry = engine.gin_rowgroup_registry_for_test();
    assert!(
        rg_registry.is_file_indexed(&project, &table, "payload", &compacted_path),
        "GIN row-group registry must have a sealed entry for the compacted file"
    );

    // Introduce a fast-path UPDATE overlay: flip one "view" row to "purchase".
    // id=1 is a "view" row (1 % 3 != 0). After the update it becomes a
    // "purchase", so the purchase count must increase by exactly one. This puts
    // a hot overlay row (writer's `LargeBinary` JSONB) into the read path that
    // unions with the cold (`Binary`) compacted batches.
    sess.execute(
        "UPDATE events SET payload = '{\"category\":\"purchase\",\"id\":1}' WHERE id = 1",
    )
    .await
    .unwrap();
    expected_purchase += 1;

    // ── COUNT(*) matching needle — must equal the purchase count, NOT panic. ──
    let n_purchase = scalar_count(
        &engine,
        project,
        r#"SELECT COUNT(*) FROM events WHERE payload @> '{"category":"purchase"}'::jsonb"#,
    )
    .await;
    assert_eq!(
        n_purchase, expected_purchase,
        "COUNT(*) @> purchase must equal {expected_purchase} (got {n_purchase})"
    );

    // ── COUNT(*) non-matching needle — must be a single `0` row. ──
    let n_absent = scalar_count(
        &engine,
        project,
        r#"SELECT COUNT(*) FROM events WHERE payload @> '{"category":"nonexistent"}'::jsonb"#,
    )
    .await;
    assert_eq!(
        n_absent, 0,
        "COUNT(*) @> non-matching must be 0 (a single 0 row, not an empty result)"
    );

    // ── SELECT id variant — row-emitting path with the same prune. ──
    let purchase_ids = {
        use arrow_array::{Array, Int64Array};
        let s = engine.open_session(project).await.unwrap();
        let result = s
            .execute(r#"SELECT id FROM events WHERE payload @> '{"category":"purchase"}'::jsonb"#)
            .await
            .unwrap();
        let batches = match result {
            basin_engine::ExecResult::Rows { batches, .. } => batches,
            other => panic!("expected Rows, got {other:?}"),
        };
        let mut total = 0usize;
        for b in &batches {
            let col = b.column(b.schema().index_of("id").unwrap());
            assert!(
                col.as_any().downcast_ref::<Int64Array>().is_some(),
                "id column must be Int64"
            );
            total += b.num_rows();
        }
        total
    };
    assert_eq!(
        purchase_ids as i64, expected_purchase,
        "SELECT id @> purchase must return {expected_purchase} rows (got {purchase_ids})"
    );

    // SELECT id non-matching → 0 rows (the row-emitting Empty path is still
    // allowed to short-circuit; just verify correctness here).
    let absent_ids = {
        let s = engine.open_session(project).await.unwrap();
        let result = s
            .execute(r#"SELECT id FROM events WHERE payload @> '{"category":"nonexistent"}'::jsonb"#)
            .await
            .unwrap();
        match result {
            basin_engine::ExecResult::Rows { batches, .. } => {
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    };
    assert_eq!(absent_ids, 0, "SELECT id @> non-matching must be empty");

    wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_compaction_reindex_fires_rowgroup_prune() {
    basin_common::telemetry::try_init_for_tests();

    // ── 1. Build shard + engine ──────────────────────────────────────────────
    let (_storage_dir, _wal_dir, engine, shard, wal) = build_engine_with_shard().await;

    let project = ProjectId::new();
    engine.config().catalog.create_namespace(&project).await.unwrap();
    let sess = engine.open_session(project).await.unwrap();

    // ── 2. CREATE TABLE with small row_block_size + GIN index ────────────────
    // row_block_size=256: the Parquet writer emits a new row-group every 256
    // rows. Inserting 300+ rows total ensures the compacted file spans at
    // least 2 row-groups (rg0: rows 0-255, rg1: rows 256+).
    sess.execute(
        "CREATE TABLE docs (\
            id   BIGINT NOT NULL, \
            doc  JSONB  NOT NULL \
         ) WITH (basin.row_block_size = 256)",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE INDEX docs_gin ON docs USING gin (doc)",
    )
    .await
    .unwrap();

    // ── 3. INSERT rows via SQL (shard path: WAL + tail) ──────────────────────
    // First batch: 200 rows all with tag="batch_a".
    // These will land in row-group 0 of the compacted file (rows 0-199).
    for i in 0..200i64 {
        sess.execute(&format!(
            "INSERT INTO docs (id, doc) VALUES ({i}, '{{\"tag\":\"batch_a\",\"id\":{i}}}')"
        ))
        .await
        .unwrap();
    }

    // Second batch: 150 rows with tag="batch_b" and a unique sentinel key.
    // Rows 200-349 in the compacted file.  With row_block_size=256, rows
    // 200-255 land in rg0 and rows 256-349 land in rg1.
    // The rows in rg1 have id in [256, 349].  We pick id=300 as our sentinel.
    for i in 200..350i64 {
        let extra = if i == 300 {
            // This specific row gets a unique key "sentinel_row" — it will
            // land in rg1 of the compacted file.
            ",\"sentinel_row\":true".to_string()
        } else {
            String::new()
        };
        sess.execute(&format!(
            "INSERT INTO docs (id, doc) VALUES ({i}, '{{\"tag\":\"batch_b\",\"id\":{i}{extra}}}')"
        ))
        .await
        .unwrap();
    }

    // ── 4. Trigger compaction ────────────────────────────────────────────────
    // flush_to_parquet drains all in-memory tail batches (350 rows total)
    // into a single compacted Parquet file with row_block_size=256 →
    // row-group 0: rows 0-255, row-group 1: rows 256-349.
    shard.flush_to_parquet().await.unwrap();

    // ── 5. Assert the row-group registry has a sealed entry for the file ─────
    // load_table gives us the live file set; there should be exactly one file.
    let table = TableName::new("docs").unwrap();
    let meta = engine.config().catalog.load_table(&project, &table).await.unwrap();
    let live_files = meta.live_data_files();
    assert_eq!(
        live_files.len(),
        1,
        "expected exactly one compacted Parquet file, got {}",
        live_files.len()
    );
    let compacted_path = live_files[0].path.to_string();

    // The compactor should have called index_batch_jsonb_gin for each GIN
    // column → mark_file_indexed → sealed = true.
    let rg_registry = engine.gin_rowgroup_registry_for_test();
    assert!(
        rg_registry.is_file_indexed(&project, &table, "doc", &compacted_path),
        "GIN row-group registry must have a sealed entry for the compacted file '{compacted_path}'"
    );

    // ── 6a. Row in non-zero row-group ─────────────────────────────────────────
    // The sentinel_row=true key only exists in the row at id=300.  id=300 is
    // row index 300 in the merged batch; with row_block_size=256 it lands in
    // row-group 1 (index 300 / 256 = 1).  The @> query must return exactly
    // one row — proving the re-indexing recorded the CORRECT row-group (if
    // the indexer had used rg=0 for all rows, the bloom for rg1 would lack
    // "sentinel_row" and prune it away, causing a false negative).
    let rows = collect_doc_rows(
        &engine,
        project,
        "SELECT id, doc FROM docs WHERE doc @> '{\"sentinel_row\":true}'",
    )
    .await;
    assert_eq!(
        rows.len(),
        1,
        "expected exactly 1 row matching sentinel_row=true, got {} (false negative = re-index bug)",
        rows.len()
    );
    // Verify the returned row really is id=300.
    assert!(
        rows[0].contains("\"id\":300"),
        "expected id=300 in result, got: {}",
        rows[0]
    );

    // ── 6b. Absent key returns 0 rows ─────────────────────────────────────────
    let absent = collect_doc_rows(
        &engine,
        project,
        "SELECT id, doc FROM docs WHERE doc @> '{\"nonexistent_key\":true}'",
    )
    .await;
    assert_eq!(
        absent.len(),
        0,
        "needle with absent key must return 0 rows, got {}",
        absent.len()
    );

    // ── 6c. Common key returns all matching rows ───────────────────────────────
    // tag="batch_a" appears in 200 rows (ids 0-199); tag="batch_b" in 150 rows.
    let batch_a_rows = collect_doc_rows(
        &engine,
        project,
        "SELECT id, doc FROM docs WHERE doc @> '{\"tag\":\"batch_a\"}'",
    )
    .await;
    assert_eq!(
        batch_a_rows.len(),
        200,
        "tag=batch_a must match 200 rows, got {} (possible false negatives from prune)",
        batch_a_rows.len()
    );

    let batch_b_rows = collect_doc_rows(
        &engine,
        project,
        "SELECT id, doc FROM docs WHERE doc @> '{\"tag\":\"batch_b\"}'",
    )
    .await;
    assert_eq!(
        batch_b_rows.len(),
        150,
        "tag=batch_b must match 150 rows, got {} (possible false negatives from prune)",
        batch_b_rows.len()
    );

    wal.close().await.unwrap();
}
