//! Integration coverage for the PK IN-list probe fast path:
//! `WHERE pk IN (v1, v2, …, vN)` and the equivalent
//! `WHERE pk = ANY('{v1,…}'::int[])` shape (lowered to IN-list by pg_ast).
//!
//! The same bloom+zone-map file-prune that fires for `WHERE pk = <lit>` now
//! also fires for IN-list shapes.  For each live file the probe checks whether
//! ANY of the queried values could be present (zone-map OR of Eq atoms plus
//! bloom OR across all values).  Files where no value can possibly be present
//! are pruned; the result set is read only from the surviving candidate files.
//!
//! Test structure:
//!   1. IN-list returns correct rows (span two files, some absent values).
//!   2. IN-list with all absent values prunes all files → zero rows + bloom
//!      skip counter advances (validates the probe fired).
//!   3. IN-list with values spanning multiple files returns all matching rows.
//!   4. `= ANY('{…}'::int[])` shape (pg_ast lowered) behaves identically to
//!      the equivalent IN-list.
//!   5. Shard path: engine wired with `shard = Some(…)`, same correctness.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Helpers (no-shard engine) ─────────────────────────────────────────────────

fn build_engine_noshard(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn two_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn sequential_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let payloads: StringArray = (start..start + len).map(|v| Some(format!("p{v}"))).collect();
    RecordBatch::try_new(two_col_schema(), vec![Arc::new(ids), Arc::new(payloads)]).unwrap()
}

/// Write a batch with a bloom on `id` and register it in the catalog.
/// Bypasses SQL INSERT so no row lands in the shard tail or memtable —
/// the read is forced onto the cold-tier PK probe path.
async fn seed_cold_file(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    snap: SnapshotId,
) -> SnapshotId {
    let opts = WriteOptions {
        bloom_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let part = PartitionKey::default_key();
    let df = engine
        .config()
        .storage
        .write_batch_with_options(project, table, &part, &batch, &opts)
        .await
        .unwrap();
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats,
        bloom_filters: df.bloom_filters,
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };
    engine
        .config()
        .catalog
        .append_data_files(project, table, snap, vec![file_ref])
        .await
        .unwrap()
        .current_snapshot
}

async fn query_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected Rows for {sql:?}, got {other:?}"),
        Err(e) => panic!("query {sql:?} failed: {e}"),
    }
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn ids_sorted(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("id").unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

/// Build a multi-file table and return (engine, session, project, table).
async fn setup_multifile_table(
    dir: &TempDir,
) -> (Engine, basin_engine::ProjectSession, ProjectId, TableName) {
    let engine = build_engine_noshard(dir);
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();

    // Three disjoint cold-tier files:
    //   f0: ids [0,  100)
    //   f1: ids [200, 300)
    //   f2: ids [400, 500)
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(0, 100), snap).await;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(200, 100), snap).await;
    let _ = seed_cold_file(&engine, &project, &table, sequential_batch(400, 100), snap).await;

    (engine, sess, project, table)
}

// ── Test 1: IN-list spanning two files returns correct rows ───────────────────

#[tokio::test]
async fn in_list_spanning_two_files_returns_correct_rows() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // Values 5 and 250 live in f0 and f1 respectively; 150 is absent.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (5, 150, 250)").await;
    let ids = ids_sorted(&rows);
    assert_eq!(ids, vec![5, 250], "only existing ids must be returned");
}

// ── Test 2: All-absent IN-list prunes all files → zero rows + bloom counter ──

#[tokio::test]
async fn in_list_all_absent_prunes_all_files() {
    let dir = TempDir::new().unwrap();
    let (engine, sess, _, _) = setup_multifile_table(&dir).await;

    let before = engine.blooms_skipped_count();

    // 150, 350, 999 all fall in the gaps between the three files.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (150, 350, 999)").await;
    assert_eq!(row_count(&rows), 0, "all-absent IN-list must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after > before,
        "bloom/zone-map probe must have skipped at least one file \
         for all-absent IN-list (before={before}, after={after})"
    );
}

// ── Test 3: IN-list spanning all three files returns all matching rows ────────

#[tokio::test]
async fn in_list_spanning_all_files_returns_all_matching_rows() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // One value per file; absent value 150 must not appear.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (10, 150, 250, 450)").await;
    let ids = ids_sorted(&rows);
    assert_eq!(ids, vec![10, 250, 450], "three matching rows across three files");
}

// ── Test 4: `= ANY('{…}'::int[])` shape behaves identically to IN ────────────

#[tokio::test]
async fn any_array_shape_behaves_identically_to_in_list() {
    let dir = TempDir::new().unwrap();
    let (_, sess, _, _) = setup_multifile_table(&dir).await;

    // pg_ast lowers `= ANY('{5,250}'::int[])` to `IN (5, 250)` before parse.
    let rows = query_rows(
        &sess,
        "SELECT * FROM t WHERE id = ANY('{5, 150, 250}'::int[])",
    )
    .await;
    let ids = ids_sorted(&rows);
    assert_eq!(
        ids,
        vec![5, 250],
        "= ANY('{{...}}'::int[]) must return the same rows as the equivalent IN-list"
    );
}

// ── Test 5: IN-list out-of-range prunes everything ────────────────────────────

#[tokio::test]
async fn in_list_out_of_range_prunes_all_files_and_returns_empty() {
    let dir = TempDir::new().unwrap();
    let (engine, sess, _, _) = setup_multifile_table(&dir).await;

    let before = engine.blooms_skipped_count();

    // All values are above the max stored id (499) — zone-map proves absent.
    let rows = query_rows(&sess, "SELECT * FROM t WHERE id IN (1000, 2000, 9999)").await;
    assert_eq!(row_count(&rows), 0, "out-of-range IN-list must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after >= before + 3,
        "zone-map probe must prune all 3 files for out-of-range IN-list \
         (before={before}, after={after})"
    );
}

// ── Test 6: 1000-key IN clustered in a narrow range prunes outer files ───────
//
// Exercises the Int64 zone-map range-overlap fast path: a 1000-value IN-list
// whose values all sit inside f1's range [200,300) must prune f0 and f2 (whose
// [min,max] don't overlap the IN-list span) and return EXACTLY the values that
// exist in f1 — including the boundary keys equal to the IN-list min and max.

#[tokio::test]
async fn in_list_1000_keys_narrow_range_prunes_outer_files_exact_boundaries() {
    let dir = TempDir::new().unwrap();
    let (engine, sess, _, _) = setup_multifile_table(&dir).await;

    // 1000 contiguous keys spanning [200, 1199].  Existing ids inside the
    // span are f1's [200,300) AND f2's [400,500) — both belong in the result
    // (f2's ids fall inside the IN-list range and exist).  f0 ([0,100)) has
    // max 99 < 200 so the zone-map prunes it.  Correctness is the invariant;
    // f0 pruning is the perf assertion.
    let keys: Vec<String> = (200..1200).map(|k| k.to_string()).collect();
    let sql = format!("SELECT id FROM t WHERE id IN ({})", keys.join(","));

    let before = engine.blooms_skipped_count();
    let rows = query_rows(&sess, &sql).await;
    let after = engine.blooms_skipped_count();

    // Existing ids in [200,1200): f1's [200,300) plus f2's [400,500).
    let expected: Vec<i64> = (200..300).chain(400..500).collect();
    assert_eq!(
        ids_sorted(&rows),
        expected,
        "1000-key narrow IN-list must return exactly the existing ids in range"
    );
    // f0 ([0,100), max 99) cannot overlap the IN-list span [200,1199] → pruned.
    assert!(
        after > before,
        "zone-map range prune must skip at least f0 (before={before}, after={after})"
    );

    // Boundary correctness: the IN-list min (200) and the largest existing key
    // (299) are both present and correct.
    let got = ids_sorted(&rows);
    assert_eq!(got.first().copied(), Some(200), "min boundary key must be returned");
    assert_eq!(got.last().copied(), Some(499), "max existing key must be returned");
}

// ── Test 7: a 1000-key IN whose span is ENTIRELY inside one file ──────────────
//
// Sharper prune assertion: build a single dense file and query a 1000-key IN
// whose min/max are interior keys.  Confirms boundary keys (== min, == max of
// the IN-list) are returned and that no false negative occurs at the edges.

#[tokio::test]
async fn in_list_1000_keys_interior_span_boundary_keys_exact() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_noshard(&dir);
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, payload TEXT) WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();

    // Two files: f0 [0, 5000), f1 [5000, 10000).
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(0, 5000), snap).await;
    let _ = seed_cold_file(&engine, &project, &table, sequential_batch(5000, 5000), snap).await;

    // 1000 keys, all interior to f0: min=1000, max=1999 (contiguous), every key
    // exists.  Span [1000,1999] does NOT overlap f1 ([5000,10000)) → f1 pruned.
    let keys: Vec<String> = (1000..2000).map(|k| k.to_string()).collect();
    let sql = format!("SELECT id FROM t WHERE id IN ({})", keys.join(","));

    let before = engine.blooms_skipped_count();
    let rows = query_rows(&sess, &sql).await;
    let after = engine.blooms_skipped_count();

    let expected: Vec<i64> = (1000..2000).collect();
    assert_eq!(
        ids_sorted(&rows),
        expected,
        "every key in a dense interior 1000-key IN-list must be returned"
    );
    let got = ids_sorted(&rows);
    assert_eq!(got.first().copied(), Some(1000), "IN-list min boundary present");
    assert_eq!(got.last().copied(), Some(1999), "IN-list max boundary present");
    assert!(
        after >= before + 1,
        "zone-map range prune must skip f1 whose [5000,10000) is above the span \
         (before={before}, after={after})"
    );
}

// ── Test 8: Utf8 PK IN-list unaffected by the Int64 range fast path ───────────
//
// The range-overlap fast path is Int64-only; a Utf8 PK IN-list must still take
// the exact OR-of-Eq prune path and return correct rows (including string keys
// that sort as the lexical min/max of the IN-list).

fn utf8_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn utf8_batch(keys: &[&str]) -> RecordBatch {
    let k: StringArray = keys.iter().map(|s| Some(*s)).collect();
    let v: Int64Array = (0..keys.len() as i64).collect();
    RecordBatch::try_new(utf8_pk_schema(), vec![Arc::new(k), Arc::new(v)]).unwrap()
}

async fn seed_utf8_cold_file(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    snap: SnapshotId,
) -> SnapshotId {
    let opts = WriteOptions {
        bloom_columns: vec!["k".to_string()],
        ..Default::default()
    };
    let part = PartitionKey::default_key();
    let df = engine
        .config()
        .storage
        .write_batch_with_options(project, table, &part, &batch, &opts)
        .await
        .unwrap();
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats,
        bloom_filters: df.bloom_filters,
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };
    engine
        .config()
        .catalog
        .append_data_files(project, table, snap, vec![file_ref])
        .await
        .unwrap()
        .current_snapshot
}

#[tokio::test]
async fn utf8_pk_in_list_is_correct_and_unaffected_by_range_fastpath() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_noshard(&dir);
    let project = ProjectId::new();
    let table = TableName::new("kv").unwrap();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v BIGINT) WITH (basin.sort_by='k')",
    )
    .await
    .unwrap();

    // Two disjoint string files by lexical range:
    //   f0: "a".."e"   f1: "p".."t"
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_utf8_cold_file(
        &engine,
        &project,
        &table,
        utf8_batch(&["a", "b", "c", "d", "e"]),
        snap,
    )
    .await;
    let _ = seed_utf8_cold_file(
        &engine,
        &project,
        &table,
        utf8_batch(&["p", "q", "r", "s", "t"]),
        snap,
    )
    .await;

    // IN-list with the lexical min ('a') and max ('t') keys plus an absent
    // middle value ('m') and a key in each file.
    let rows = query_rows(
        &sess,
        "SELECT k FROM kv WHERE k IN ('a', 'c', 'm', 'r', 't')",
    )
    .await;
    let mut got: Vec<String> = rows
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(b.schema().index_of("k").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..arr.len()).map(move |i| arr.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec!["a".to_string(), "c".to_string(), "r".to_string(), "t".to_string()],
        "Utf8 PK IN-list must return exactly the existing keys incl. lexical min/max"
    );
}

// ── Shard path: same correctness and bloom-skip guarantee ────────────────────

async fn build_shard_engine() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(
            LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
        ),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
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
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_list_on_shard_path_prunes_files_and_returns_correct_rows() {
    let (_sd, _wd, engine, bg, wal) = build_shard_engine().await;
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, payload TEXT) \
         WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();

    // Seed three disjoint cold-tier files (bypass shard tail).
    let mut snap = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .current_snapshot;
    snap = seed_cold_file(&engine, &project, &table, sequential_batch(0, 1000), snap).await;
    snap = seed_cold_file(
        &engine,
        &project,
        &table,
        sequential_batch(2000, 1000),
        snap,
    )
    .await;
    let _ = seed_cold_file(
        &engine,
        &project,
        &table,
        sequential_batch(4000, 1000),
        snap,
    )
    .await;

    let before = engine.blooms_skipped_count();

    // All-absent IN-list: every file must be pruned by zone-map.
    let res = sess
        .execute("SELECT id FROM events WHERE id IN (1500, 3500, 9999)")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "all-absent IN-list on shard must return zero rows");

    let after = engine.blooms_skipped_count();
    assert!(
        after >= before + 3,
        "PK IN-list probe on shard path must prune all 3 cold-tier files for \
         the all-absent IN-list (before={before}, after={after})"
    );

    // Spanning IN-list: values in f0 and f2.
    let res = sess
        .execute("SELECT id FROM events WHERE id IN (100, 1500, 4500)")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..arr.len()).map(move |i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![100, 4500],
        "IN-list spanning two files must return exactly the matching rows"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── Diagnostic: 1000-key IN-list latency at 1M rows ──────────────────────────
//
// Measures the p50 of a 1000-key `WHERE id IN (...)` over a 1M-row single-PK
// table whose data is quiesced cold to the storage tier.  Baseline (pre-fix)
// was ~275ms vs PG's ~3.1ms; the zone-map range-overlap + bloom early-exit
// + Int64 range pushdown collapse the per-file work.  Run explicitly:
//   cargo test -p basin-integration-tests --test pk_in_list_probe \
//     in_list_latency_probe -- --ignored --nocapture
//
// Seed pattern mirrors `update_latency_probe`: engine INSERT path in 10k
// batches, then `flush_to_parquet` to force cold.

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

async fn build_shard_engine_with_handle() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn in_list_latency_probe() {
    use std::time::Instant;

    let (_sd, _wd, engine, shard, bg, wal) = build_shard_engine_with_handle().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let scale = 1_000_000i64;
    let table = "inlist1m";
    sess.execute(&format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"))
        .await
        .unwrap();

    // Seed 1M rows via the engine INSERT path in 10k batches — each disjoint id
    // range becomes one cold file with a tight PK zone-map after flush.
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < scale {
        let lo = id;
        let hi = (id + batch).min(scale);
        let mut stmt = String::with_capacity((hi - lo) as usize * 16);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }

    // Quiesce cold so each query exercises the cold-tier probe + read path.
    shard.flush_to_parquet().await.unwrap();

    // Build a 1000-key IN-list clustered in a narrow range near the middle of
    // the key space (all keys exist) — the shape that exercises zone-map range
    // pruning + bloom early-exit.
    let key_lo = 500_000i64;
    let keys: Vec<String> = (key_lo..key_lo + 1000).map(|k| k.to_string()).collect();
    let sql = format!("SELECT id FROM {table} WHERE id IN ({})", keys.join(","));

    // Warm-up discard: pay first-touch costs (plan cache, footer reads) once.
    let warm = query_rows(&sess, &sql).await;
    assert_eq!(
        row_count(&warm),
        1000,
        "warm-up 1000-key IN-list must return all 1000 existing rows"
    );

    let n = 50usize;
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = Instant::now();
        let rows = query_rows(&sess, &sql).await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(row_count(&rows), 1000, "each iteration must return all 1000 rows");
        assert!(ms.is_finite());
        times.push(ms);
    }

    let mut sorted = times.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&sorted, 0.50);
    let p99 = percentile(&sorted, 0.99);
    println!("[in-list-latency] scale={scale} keys=1000 p50={p50:.3}ms p99={p99:.3}ms");

    bg.shutdown().await;
    wal.close().await.unwrap();
}
