//! Shard-path IN-list row-group pruning integration test.
//!
//! The previous agent's `pk_in_list_probe` test seeded tables via direct
//! catalog writes (bypassing the WAL+shard tail path), so files had full
//! catalog `column_stats` and the test passed even though the shard path
//! was broken.
//!
//! This test seeds a large table via **SQL INSERT through the shard** — the
//! path that the bench and production use.  Files written by the shard's WAL
//! flush have EMPTY catalog `column_stats` (see `commit_new_file`), so the
//! catalog-level zone-map prune cannot fire.  Row-group pruning must happen
//! inside the Parquet reader instead, driven by the bounding-range predicates
//! synthesised from the IN-list in `execute_simple_select`.
//!
//! Assertion strategy: time the IN-list query whose values are clustered in
//! the first row-group of the data file.  Without row-group pruning the query
//! scans all ~3 row-groups (200k rows); with pruning it touches only 1.  We
//! assert the query completes in well under what a full scan would cost and
//! verify the correct 100 rows are returned.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

/// Seed 200k rows via SQL INSERT so files are written by the shard WAL flush
/// path (which produces Parquet files with row-group stats but empty catalog
/// `column_stats`).  This is the real production path.
///
/// Returns the number of rows inserted.
async fn seed_via_sql_insert(
    sess: &basin_engine::ProjectSession,
    n: i64,
    batch_size: i64,
) -> i64 {
    let mut id = 0i64;
    while id < n {
        let hi = (id + batch_size).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 40);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k}, {})", k as f64 * 0.5));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    n
}

/// IN-list query on a shard-written table where catalog `column_stats` are
/// absent.  Row-group pruning must fire via the bounding-range predicates
/// synthesised from the IN-list so only the first row-group is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_list_on_shard_written_table_row_group_prunes() {
    let (_sd, _wd, engine, bg, wal) = build_shard_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Simple 2-column table — no JSONB, no extra complexity.
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, amount DOUBLE PRECISION)",
    )
    .await
    .unwrap();

    // Seed 200k rows (20 batches of 10k) via SQL INSERT through the shard.
    // IDs are sequential 0..200k so the data is naturally sorted by id.
    let n = 200_000i64;
    let batch = 10_000i64;
    seed_via_sql_insert(&sess, n, batch).await;

    // Query 1: exact bench shape — 100 ids in [1, 694], all in the first
    // row-group of the single compacted file.
    //
    // Expected row-group layout (default 65k rows/group):
    //   rg0: ids [0,   65535]  ← contains all 100 target values
    //   rg1: ids [65536, 131071]
    //   rg2: ids [131072, 196607]
    //   rg3: ids [196608, 199999]
    //
    // The bounding-range for id IN (1,8,...,694) is Gt(id,0) AND Lt(id,695).
    // Row-groups 1-3 have min > 694, so they are pruned.  Only rg0 is read.
    let in_ids: Vec<String> = (0..100i64).map(|k| (k * 7 + 1).to_string()).collect();
    let in_list_sql = format!(
        "SELECT id, amount FROM events WHERE id IN ({})",
        in_ids.join(",")
    );

    // Warm-up run (also triggers flush_to_parquet so subsequent reads hit cold files).
    let warm = sess.execute(&in_list_sql).await.unwrap();
    let warm_rows: usize = match warm {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        _ => panic!("expected Rows"),
    };
    assert_eq!(warm_rows, 100, "warm-up: must return 100 matching rows");

    // Timed run.
    let t0 = Instant::now();
    let res = sess.execute(&in_list_sql).await.unwrap();
    let elapsed = t0.elapsed();
    let rows: usize = match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        _ => panic!("expected Rows"),
    };
    assert_eq!(rows, 100, "must return exactly 100 matching rows");

    // Query 2: full scan (no WHERE) to establish a reference timing.
    let t_full0 = Instant::now();
    let full = sess
        .execute("SELECT id, amount FROM events")
        .await
        .unwrap();
    let full_elapsed = t_full0.elapsed();
    let full_rows: usize = match full {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        _ => panic!("expected Rows"),
    };
    assert_eq!(full_rows, n as usize, "full scan must return all {n} rows");

    println!(
        "[in-list-shard-prune] IN-list(100) time={:.2}ms  full-scan time={:.2}ms  ratio={:.1}x",
        elapsed.as_secs_f64() * 1000.0,
        full_elapsed.as_secs_f64() * 1000.0,
        full_elapsed.as_secs_f64() / elapsed.as_secs_f64(),
    );

    // The IN-list should be at least 3x faster than the full scan because the
    // bounding-range prunes 3 of the 4 row-groups (values all in [1,694];
    // row-groups 1-3 have min > 694).  Even a conservative 2x threshold
    // would fail without row-group pruning (full scan == IN-list scan when
    // no filters reach the Parquet reader).
    //
    // We use a generous 2x threshold to avoid flakiness on slow CI boxes:
    // the theoretical speedup is ~4x (1/4 of row-groups) but I/O setup
    // and per-row filter overhead narrow that in practice.
    assert!(
        elapsed < full_elapsed / 2,
        "IN-list on shard-written data must be >2x faster than a full scan \
         (IN-list={:.2}ms, full-scan={:.2}ms). Row-group pruning is not firing.",
        elapsed.as_secs_f64() * 1000.0,
        full_elapsed.as_secs_f64() * 1000.0,
    );

    // Query 3: verify result correctness — all returned ids must be in the
    // queried set and the values must match.
    let expected_ids: std::collections::BTreeSet<i64> =
        (0..100i64).map(|k| k * 7 + 1).collect();
    let res = sess.execute(&in_list_sql).await.unwrap();
    let mut returned_ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut ids = Vec::new();
            for b in &batches {
                let id_col = b.column(b.schema().index_of("id").unwrap());
                let arr = id_col
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    ids.push(arr.value(i));
                }
            }
            ids
        }
        _ => panic!("expected Rows"),
    };
    returned_ids.sort_unstable();
    let returned_set: std::collections::BTreeSet<i64> = returned_ids.into_iter().collect();
    assert_eq!(
        returned_set, expected_ids,
        "must return exactly the queried 100 ids, no more, no less"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// OUT-OF-RANGE IN-list: all 100 values are above the max id in the table.
/// The bounding range Gt(id, min-1) AND Lt(id, max+1) prunes ALL row-groups
/// in every file → 0 rows returned, very fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_list_out_of_range_on_shard_written_table_returns_empty_fast() {
    let (_sd, _wd, engine, bg, wal) = build_shard_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val DOUBLE PRECISION)",
    )
    .await
    .unwrap();

    // Insert 50k rows (ids 0..50000).
    let n = 50_000i64;
    let mut id = 0i64;
    while id < n {
        let hi = (id + 5000).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 30);
        stmt.push_str("INSERT INTO t VALUES ");
        for k in id..hi {
            if k > id { stmt.push(','); }
            stmt.push_str(&format!("({k}, {})", k as f64));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }

    // All IN-list values are above max_id → all row-groups pruned.
    let in_ids: String = (0..50i64).map(|k| (100_000 + k * 7).to_string()).collect::<Vec<_>>().join(",");
    let sql = format!("SELECT id FROM t WHERE id IN ({in_ids})");

    // Warm up.
    let _ = sess.execute(&sql).await.unwrap();

    let t0 = Instant::now();
    let res = sess.execute(&sql).await.unwrap();
    let elapsed = t0.elapsed();
    let rows: usize = match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        _ => panic!("expected Rows"),
    };
    assert_eq!(rows, 0, "out-of-range IN-list must return 0 rows");

    // Should complete very quickly (row-groups pruned immediately by the
    // Gt/Lt bounding range: max(stored) = 49999 < min(queried) = 100000).
    // 500ms is a very generous upper bound even on slow hardware.
    assert!(
        elapsed < Duration::from_millis(500),
        "out-of-range IN-list on shard-written data must return quickly \
         (elapsed={:.2}ms). If this fails, row-group pruning is not firing.",
        elapsed.as_secs_f64() * 1000.0,
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
