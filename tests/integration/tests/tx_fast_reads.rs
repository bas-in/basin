//! In-transaction fast-path SELECT, re-enabled SAFELY against a pinned
//! read-view (the snapshot_isolation work's hot+cold pin).
//!
//! History (see `executor.rs`, the in-tx fast-path gate): an in-tx gate that
//! consulted the TTL meta cache leaked a concurrent commit past the pinned
//! snapshot, so the gate was hardened to a blanket `!in_txn` bail — every in-tx
//! read paid full DataFusion. This suite guards the re-enabled fast path: it
//! engages ONLY when the table is untouched by this tx AND both pins (cold
//! snapshot + hot-seq watermark) already exist, and it then reads AT the pin
//! with fresh state (historical metadata via `load_table_at_snapshot`, hot
//! probes filtered by the watermark, PK row cache bypassed).
//!
//! Mirrors `snapshot_isolation.rs`'s two-session patterns. The point reads here
//! exercise `match_simple_select` → `execute_simple_select_pinned`; the leading
//! in-tx `COUNT(*)` forces a DataFusion read whose `load_table_for_read` CAPTURES
//! both pins for the table (the fast-path gate only ever PEEKS), so the
//! subsequent point read of the SAME table is eligible for the fast path.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Shard + WAL engine, matching `compare_postgres_common.rs::build_basin_engine`
/// (and `snapshot_isolation.rs`). A committed INSERT lands in the shard tail and
/// is flushed to a Parquet file (a new catalog snapshot) on the next reader's
/// `shard.flush_to_parquet()`.
async fn open_engine_with_shard() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
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
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    assert!(!batches.is_empty(), "expected at least one batch");
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1, "expected single-row batch");
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array")
        .value(0)
}

/// Run a COUNT(*)-shaped query and return the scalar. Inside a tx this routes
/// to DataFusion (aggregates-in-tx bail), which captures both read pins for the
/// referenced table — priming the table for a subsequent fast point read.
async fn count(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => scalar_i64(&batches),
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

/// Point read of `SELECT v FROM t WHERE id = <k>`. Returns `Some(v)` for the
/// single matched row, `None` when no row matches (deleted / absent). Asserts
/// at most one row (PK lookup).
async fn point_v(sess: &ProjectSession, table: &str, k: i64) -> Option<i64> {
    let sql = format!("SELECT v FROM {table} WHERE id = {k}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total <= 1, "PK point read returned {total} rows for {sql:?}");
            if total == 0 {
                return None;
            }
            let b = batches.iter().find(|b| b.num_rows() == 1).unwrap();
            Some(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("expected Int64Array v")
                    .value(0),
            )
        }
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)"
    ))
    .await
    .unwrap();
    let mut stmt = format!("INSERT INTO {table} (id, v) VALUES ");
    for i in 0..n {
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({i}, {})", i * 10));
    }
    sess.execute(&stmt).await.unwrap();
}

/// A pins its read-view (in-tx COUNT primes the table), point-reads `k` via the
/// fast path; B fast-path-UPDATEs `k` and commits; A re-point-reads `k` and MUST
/// still see the OLD value (the pin's hot watermark hides B's post-pin override).
/// After A COMMITs, A sees the new value.
#[tokio::test]
async fn in_tx_point_read_repeatable_across_concurrent_update() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 50).await;
    // Force a flush so rows live in cold Parquet (the fast path reads cold +
    // the hot overlay; B's UPDATE will be the only hot entry).
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;

    a.execute("BEGIN").await.unwrap();
    // Pin t's read-view (DataFusion COUNT captures both pins).
    let c1 = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(c1, 50);
    // First point read, now eligible for the in-tx fast path.
    let v0 = point_v(&a, "t", 7).await;
    assert_eq!(v0, Some(70), "A's pinned read of k=7");

    // B (auto-commit) UPDATEs k=7 via the hot-tier fast path and commits.
    b.execute("UPDATE t SET v = 99999 WHERE id = 7")
        .await
        .unwrap();
    assert_eq!(
        point_v(&b, "t", 7).await,
        Some(99999),
        "B sees its own committed update"
    );

    // A's re-read MUST still be the pinned OLD value — the override's seq is
    // past A's hot watermark, so the probe + post-read overlay hide it.
    let v1 = point_v(&a, "t", 7).await;
    assert_eq!(
        v1,
        Some(70),
        "A's repeated in-tx point read must equal the first (snapshot stable); \
         saw {v1:?} — B's concurrent UPDATE leaked"
    );

    a.execute("COMMIT").await.unwrap();
    // Post-COMMIT the pin is released: A sees B's committed value.
    assert_eq!(
        point_v(&a, "t", 7).await,
        Some(99999),
        "post-COMMIT A sees the live head"
    );
}

/// Same as above but B DELETEs the row: the tombstone is newer than A's
/// watermark and MUST NOT hide the row from A's pinned read.
#[tokio::test]
async fn in_tx_point_read_survives_concurrent_delete() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 50).await;
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;

    a.execute("BEGIN").await.unwrap();
    let c1 = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(c1, 50);
    assert_eq!(point_v(&a, "t", 12).await, Some(120), "A pinned read of k=12");

    // B deletes k=12 (hot-tier tombstone) and commits.
    b.execute("DELETE FROM t WHERE id = 12").await.unwrap();
    assert_eq!(point_v(&b, "t", 12).await, None, "B sees its own delete");

    // A's pinned read must STILL see the row: the tombstone's seq is past A's
    // watermark, so it must not suppress the pinned cold row.
    let v1 = point_v(&a, "t", 12).await;
    assert_eq!(
        v1,
        Some(120),
        "A's pinned point read must survive B's concurrent DELETE; saw {v1:?}"
    );

    a.execute("COMMIT").await.unwrap();
    // Post-COMMIT the deletion is visible.
    assert_eq!(
        point_v(&a, "t", 12).await,
        None,
        "post-COMMIT A sees B's delete"
    );
}

/// A's own in-tx write to ANOTHER table must not disable the fast read on the
/// untouched table: the gate's untouched-by-this-tx check is per-table.
#[tokio::test]
async fn own_write_to_other_table_keeps_fast_reads_on_untouched_table() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 30).await;
    seed(&a, "other", 30).await;
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;
    let _ = count(&a, "SELECT COUNT(*) FROM other").await;

    a.execute("BEGIN").await.unwrap();
    // Pin both tables' read-views.
    assert_eq!(count(&a, "SELECT COUNT(*) FROM t").await, 30);
    assert_eq!(count(&a, "SELECT COUNT(*) FROM other").await, 30);
    assert_eq!(point_v(&a, "t", 5).await, Some(50), "A pinned read of t.k=5");

    // A writes to `other` (this tx now has an overlay/pending entry for
    // `other`, but `t` is still untouched).
    a.execute("UPDATE other SET v = 1 WHERE id = 0").await.unwrap();

    // B commits a concurrent UPDATE to t.k=5; A must NOT see it (fast read on
    // the untouched `t` still honours the pin).
    b.execute("UPDATE t SET v = 7777 WHERE id = 5").await.unwrap();

    let v1 = point_v(&a, "t", 5).await;
    assert_eq!(
        v1,
        Some(50),
        "fast read on untouched table t must stay pinned despite A's write to `other`; \
         saw {v1:?}"
    );
    // Read-your-own-writes on the touched table still holds (routes to DataFusion).
    assert_eq!(point_v(&a, "other", 0).await, Some(1), "A sees its own write to other");

    a.execute("COMMIT").await.unwrap();
}

/// First touch inside a tx now PINS-AND-SERVES on the fast path: the gate
/// passes a `FirstTouch` request, the fast path flushes the shard tail, captures
/// both pins (cold snapshot + hot watermark) at the post-flush head — the SAME
/// moment `load_table_for_read` would — and serves the read at that pin. So the
/// FIRST in-tx read of a table no longer pays the DataFusion tax, yet stays
/// snapshot-stable: a concurrent committer's later UPDATE must not leak in.
#[tokio::test]
async fn first_touch_pins_and_serves_fast() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 40).await;
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;

    a.execute("BEGIN").await.unwrap();
    // FIRST touch of t is the point read itself — no leading COUNT to prime the
    // pin. The fast path captures the pin on this very read (flush → pin → serve)
    // and the answer must be correct.
    let v0 = point_v(&a, "t", 3).await;
    assert_eq!(v0, Some(30), "first-touch point read (fast-path-pinned)");

    // B commits a concurrent UPDATE after A pinned on first touch.
    b.execute("UPDATE t SET v = 555 WHERE id = 3").await.unwrap();
    assert_eq!(point_v(&b, "t", 3).await, Some(555), "B sees its own update");

    // SECOND read of the SAME row: now `AlreadyPinned`, still fast, and must
    // STILL return the OLD value captured at first touch (snapshot stable).
    let v1 = point_v(&a, "t", 3).await;
    assert_eq!(
        v1,
        Some(30),
        "second (fast) point read must equal the first-touch pin; saw {v1:?} — B's UPDATE leaked"
    );

    // A different row read at the SAME pin must also see the pre-update value
    // (the pin covers the whole table, not just the first-read key).
    assert_eq!(point_v(&a, "t", 4).await, Some(40), "sibling row at the pin");

    a.execute("COMMIT").await.unwrap();
    assert_eq!(point_v(&a, "t", 3).await, Some(555), "post-COMMIT sees live head");
}

/// First touch where ANOTHER session committed an UPDATE BEFORE A's tx begins:
/// the first-touch capture pins the post-flush head, so the read sees that
/// committed value (read-committed at pin time). A concurrent INSERT after the
/// pin (which flushes to a new cold snapshot, advancing the head) is hidden by
/// the cold pin — the realistic, fully-closed isolation case.
#[tokio::test]
async fn first_touch_pins_at_post_flush_head() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 40).await;
    // B commits an UPDATE BEFORE A's tx begins — A must see this committed value.
    b.execute("UPDATE t SET v = 111 WHERE id = 9").await.unwrap();
    assert_eq!(point_v(&b, "t", 9).await, Some(111), "B sees its own update");

    a.execute("BEGIN").await.unwrap();
    // First touch pins at the current (post-flush) head — sees B's committed 111.
    let v0 = point_v(&a, "t", 9).await;
    assert_eq!(v0, Some(111), "first-touch sees pre-BEGIN committed value");

    // B inserts a NEW row after A pinned, and reads it back (flushing the tail to
    // a new cold snapshot that advances the head past A's pin).
    b.execute("INSERT INTO t (id, v) VALUES (10001, 7)")
        .await
        .unwrap();
    let _ = count(&b, "SELECT COUNT(*) FROM t").await;

    // A's pinned read of the existing key stays stable, and B's new row is hidden.
    assert_eq!(point_v(&a, "t", 9).await, Some(111), "pinned read stable across concurrent INSERT");
    assert_eq!(point_v(&a, "t", 10001).await, None, "B's post-pin row hidden from A");

    a.execute("COMMIT").await.unwrap();
    assert_eq!(point_v(&a, "t", 10001).await, Some(7), "post-COMMIT sees B's row");
}

/// A concurrent INSERT (flushed to a new cold snapshot, advancing the head)
/// mid-tx must not disturb A's pinned point read.
#[tokio::test]
async fn concurrent_insert_flush_does_not_move_pinned_point_read() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    seed(&a, "t", 20).await;
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;

    a.execute("BEGIN").await.unwrap();
    let c1 = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(c1, 20);
    assert_eq!(point_v(&a, "t", 9).await, Some(90), "A pinned read of k=9");

    // B inserts a NEW row and reads it back — the read flushes the shard tail to
    // Parquet, advancing the catalog head past A's pin.
    b.execute("INSERT INTO t (id, v) VALUES (10001, 1)")
        .await
        .unwrap();
    let _ = count(&b, "SELECT COUNT(*) FROM t").await;

    // A's pinned point read of an EXISTING key stays stable (reads at the pin's
    // historical snapshot, not the advanced head).
    let v1 = point_v(&a, "t", 9).await;
    assert_eq!(v1, Some(90), "pinned point read stable across concurrent INSERT; saw {v1:?}");
    // The new row is invisible to A's pinned tx.
    assert_eq!(point_v(&a, "t", 10001).await, None, "B's new row hidden from A's pin");

    a.execute("COMMIT").await.unwrap();
    assert_eq!(point_v(&a, "t", 10001).await, Some(1), "post-COMMIT A sees B's row");
}

/// Smoke timing: print the in-tx point-read latency. Not asserted (no baseline
/// is available in-test), but proves the path runs and surfaces a number for
/// eyeballing the fast-path engagement.
#[tokio::test]
async fn in_tx_point_read_timing_smoke() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();

    seed(&a, "t", 200).await;
    let _ = count(&a, "SELECT COUNT(*) FROM t").await;

    a.execute("BEGIN").await.unwrap();
    // Prime the pin (first-touch DataFusion).
    let _ = point_v(&a, "t", 0).await;

    let iters = 50u32;
    let start = Instant::now();
    for i in 0..iters {
        let k = (i as i64) % 200;
        let _ = point_v(&a, "t", k).await;
    }
    let elapsed = start.elapsed();
    let p_avg = elapsed / iters;
    println!(
        "in_tx fast point read: {iters} iters in {elapsed:?} (avg {p_avg:?}/read)"
    );

    a.execute("COMMIT").await.unwrap();
}
