//! ACID correctness — BEGIN; INSERT; ROLLBACK; SELECT must return 0 rows.
//!
//! Reproduces the bench-shape "ROLLBACK drops rows (txn correctness)" failure
//! from `compare_postgres_common.rs::rollback_drops_rows` (#42 in the 25-shape
//! audit). Postgres semantics: a rolled-back transaction must leave the table
//! visible to the same session indistinguishable from its pre-BEGIN state.
//!
//! The bug being guarded: in-transaction INSERT/UPDATE/DELETE that smuggle
//! their writes into the process-wide MemTableRegistry (or any other shared
//! visibility surface) escape ROLLBACK, because ROLLBACK only drains the
//! per-session `TxState` (pending_files + htap_rows). Any path that touches
//! the shared registry while `tx_is_active()` is true would leak.
//!
//! These tests also lock in the slow-path tx-buffer contract for UPDATE and
//! DELETE, mirror PG's read-your-own-writes-then-rollback semantics, and
//! catch regressions if a future fastpath gate is loosened.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

/// Open an engine wired to a real shard + WAL (matching the bench harness
/// `build_basin_engine()` in `compare_postgres_common.rs`). The shard write
/// path is a separate INSERT codepath from the legacy synchronous one; the
/// bench-shape `-1.0` (ROLLBACK leak) was reported only on this path.
async fn open_engine_with_shard() -> (TempDir, TempDir, Engine, basin_shard::ShardBackgroundHandle, Arc<dyn Wal>) {
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

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    assert!(!batches.is_empty(), "expected at least one batch");
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1, "expected single-row batch");
    let arr = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array");
    arr.value(0)
}

async fn count(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => scalar_i64(&batches),
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

// ─── 1. INSERT inside a rolled-back txn must not be visible after ROLLBACK ──

#[tokio::test]
async fn rollback_drops_inserted_rows() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    let n = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        n, 0,
        "ROLLBACK must drop all rows from the aborted transaction; saw {n}"
    );
}

// ─── 2. Mixed committed + rolled-back txns: only committed rows survive ────

#[tokio::test]
async fn rollback_preserves_earlier_committed_inserts() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    // First txn: commit two rows.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();
    sess.execute("COMMIT").await.unwrap();

    // Second txn: insert two more, then roll back.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t VALUES (3), (4)").await.unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    let n = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        n, 2,
        "post-ROLLBACK count must only reflect the committed pair; saw {n}"
    );

    // Also verify per-id presence — guards against ROLLBACK eating committed
    // rows (the other way the same bug could manifest).
    let n1 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 1").await;
    let n3 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 3").await;
    assert_eq!(n1, 1, "id=1 was committed and must survive ROLLBACK of a later txn");
    assert_eq!(n3, 0, "id=3 was inserted then rolled back — must be gone");
}

// ─── 3. Read-your-own-writes inside the txn, then ROLLBACK drops them ──────

#[tokio::test]
async fn within_tx_reads_see_uncommitted_then_rollback_clears() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t VALUES (10), (20), (30)")
        .await
        .unwrap();
    // Read-your-own-writes within the txn — PG semantics.
    let in_tx = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(in_tx, 3, "within-tx SELECT must see uncommitted rows; saw {in_tx}");

    sess.execute("ROLLBACK").await.unwrap();

    let after = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        after, 0,
        "ROLLBACK must drop the three uncommitted rows; saw {after}"
    );
}

// ─── 4. ROLLBACK after UPDATE: the original values must be restored ────────
//
// UPDATE inside a tx is implemented via copy-on-write; if the rewritten
// Parquet file or any memtable override escapes ROLLBACK, a SELECT after
// rollback will return the new value instead of the committed one.

#[tokio::test]
async fn rollback_undoes_update() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("UPDATE t SET v = 999 WHERE id = 1").await.unwrap();
    // Within-tx read sees the override.
    let in_tx = count(&sess, "SELECT COUNT(*) FROM t WHERE v = 999").await;
    assert_eq!(in_tx, 1, "within-tx SELECT must see the UPDATE; saw {in_tx}");
    sess.execute("ROLLBACK").await.unwrap();

    // Post-rollback: original 100 must be back, 999 must be gone.
    let still_100 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 1 AND v = 100").await;
    let leaked_999 = count(&sess, "SELECT COUNT(*) FROM t WHERE v = 999").await;
    assert_eq!(still_100, 1, "ROLLBACK must restore v=100 for id=1");
    assert_eq!(leaked_999, 0, "ROLLBACK must drop v=999; saw {leaked_999} survivors");
}

// ─── 5. ROLLBACK after DELETE: deleted rows must reappear ─────────────────
//
// Mirrors test #4 for the DELETE direction. A tombstone or file-drop that
// is not gated on `tx_is_active` would cause the row to stay gone.

#[tokio::test]
async fn rollback_undoes_delete() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("DELETE FROM t WHERE id = 2").await.unwrap();
    let in_tx = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(in_tx, 2, "within-tx SELECT must reflect the DELETE; saw {in_tx}");
    sess.execute("ROLLBACK").await.unwrap();

    let after = count(&sess, "SELECT COUNT(*) FROM t").await;
    assert_eq!(after, 3, "ROLLBACK must restore the deleted row; saw {after}");
    let id2 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 2").await;
    assert_eq!(id2, 1, "id=2 must reappear after ROLLBACK of the DELETE");
}

// ─── 6. Hot-tier UPDATE fastpath must remain gated off in-tx ───────────────
//
// Regression guard for ADR-style gating: the hot-tier UPDATE fastpath writes
// a `MemRowValue::Update` straight into the process-wide MemTableRegistry —
// no per-session buffer, no ROLLBACK hook. If the `tx_is_active` gate is
// ever weakened (or the env var defaulted on) without a tx-scoped registry,
// this test will catch the regression.
//
// We force the fastpath on via env var and assert ROLLBACK still works.

#[tokio::test]
async fn rollback_undoes_update_even_with_hottier_fastpath_envvar_on() {
    // Set env var only for the duration of this test.  Safe because each
    // test has its own engine + tempdir; the env-var is read on each UPDATE.
    // SAFETY: tests in this file are independent processes per #[tokio::test]
    // but env vars are process-global. Set, run, restore.
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    // SAFETY: setting env vars across threads is unsound in general but this
    // test file does not spawn additional threads that read this var.
    unsafe { std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1") };

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 100)").await.unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("UPDATE t SET v = 999 WHERE id = 1").await.unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    let still_100 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 1 AND v = 100").await;
    let leaked_999 = count(&sess, "SELECT COUNT(*) FROM t WHERE v = 999").await;

    // Restore env var before asserting (so a failure doesn't leak state).
    match prev {
        Some(v) => unsafe { std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v) },
        None => unsafe { std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH") },
    }

    assert_eq!(
        still_100, 1,
        "ROLLBACK must restore v=100 even when the UPDATE fastpath env var is set \
         — the fastpath must be disabled inside an explicit transaction"
    );
    assert_eq!(
        leaked_999, 0,
        "no v=999 row may survive ROLLBACK; saw {leaked_999}"
    );
}

// ─── 9. Bench-harness reproduction (shard + WAL) ──────────────────────────
//
// The `compare_postgres_common.rs::build_basin_engine` wires `shard:
// Some(shard)` with a real WAL.  The shard-enabled INSERT path in
// `executor.rs::exec_insert` (around line 3742) calls
// `shard.get().write_batch()` straight through — there is NO `tx_is_active`
// gate, so an INSERT issued inside a `BEGIN` block lands in the shard's
// WAL/memtable and is visible to subsequent SELECTs on this session and
// every other. ROLLBACK only drains `TxState::pending_files` /
// `htap_rows`, which the shard path never populated.
//
// This is the actual root cause of bench shape #42's `-1.0`. Test will
// FAIL until either (a) the shard write path checks `tx_is_active` and
// falls through to the synchronous catalog path, or (b) the shard writes
// are buffered in TxState and only flushed on COMMIT.

#[tokio::test]
async fn shard_path_rollback_drops_inserted_rows() {
    let (_dir, _wal_dir, eng, _bg, _wal) = open_engine_with_shard().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // Warmup: commit a few rows so the table is non-empty (mirrors the
    // bench's #41 → #42 ordering).
    sess.execute("BEGIN").await.unwrap();
    for k in 0..10i64 {
        sess.execute(&format!("INSERT INTO t (id, v) VALUES ({k}, {k})"))
            .await
            .unwrap();
    }
    sess.execute("COMMIT").await.unwrap();

    // The bench-shape: BEGIN; INSERT (sentinel); ROLLBACK.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t (id, v) VALUES (999999, 1)")
        .await
        .unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    let surviving = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 999999").await;
    assert_eq!(
        surviving, 0,
        "shard-path: ROLLBACK must drop the row; saw {surviving} \
         (this is the bench #42 `-1.0` failure)"
    );
}

// ─── 8. Bench-shape reproduction (exact compare_postgres_common #42) ──────
//
// Mirrors `compare_postgres_common.rs::rollback_drops_rows` end-to-end:
//
//   CREATE TABLE txnins (id BIGINT NOT NULL PRIMARY KEY, v BIGINT);
//   BEGIN; INSERT VALUES (k, k) for k in 0..100; COMMIT;     -- #41 warmup
//   BEGIN; INSERT VALUES (999999, 1); ROLLBACK;              -- #42 the test
//   SELECT COUNT(*) FROM txnins WHERE id = 999999;           -- expect 0
//
// The 100-row warmup commit populates the same HTAP memtable / shared
// registry surfaces that #42 then writes to inside a rolled-back txn.  If
// the rollback path forgets to discard rows that landed in the registry,
// id=999999 stays visible.

#[tokio::test]
async fn bench_shape_rollback_drops_rows_exact_repro() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE txnins (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // #41 warmup: BEGIN; INSERT x100; COMMIT.
    sess.execute("BEGIN").await.unwrap();
    for k in 0..100i64 {
        sess.execute(&format!("INSERT INTO txnins (id, v) VALUES ({k}, {k})"))
            .await
            .unwrap();
    }
    sess.execute("COMMIT").await.unwrap();

    // Sanity: warmup committed 100 rows.
    let warm = count(&sess, "SELECT COUNT(*) FROM txnins").await;
    assert_eq!(warm, 100, "warmup should have committed 100 rows; saw {warm}");

    // #42 the test: BEGIN; INSERT (999999); ROLLBACK.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO txnins (id, v) VALUES (999999, 1)")
        .await
        .unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    // Bench expectation: id=999999 must not exist.
    let surviving = count(&sess, "SELECT COUNT(*) FROM txnins WHERE id = 999999").await;
    assert_eq!(
        surviving, 0,
        "ROLLBACK must drop the inserted row; saw {surviving} (bench-shape #42)"
    );
    // And the committed 100 must still be intact.
    let total = count(&sess, "SELECT COUNT(*) FROM txnins").await;
    assert_eq!(total, 100, "committed 100 rows must survive ROLLBACK; saw {total}");
}

// ─── 10. SAVEPOINT + ROLLBACK TO on shard-path INSERT ─────────────────────
//
// Mirrors the bench-shape #43 (`savepoint_rollback`). Now that in-tx INSERTs
// are routed through the legacy synchronous path (which buffers writes in
// `TxState::pending_files`), `tx_rollback_to_savepoint` truncates the tail
// past the savepoint watermark and deletes the abandoned files. This test
// proves the shard-path fix carries through to the SAVEPOINT codepath too.

#[tokio::test]
async fn shard_path_savepoint_rollback_to_partial() {
    let (_dir, _wal_dir, eng, _bg, _wal) = open_engine_with_shard().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // BEGIN; INSERT a; SAVEPOINT sp1; INSERT b; ROLLBACK TO sp1; COMMIT.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("INSERT INTO t (id, v) VALUES (888001, 1)")
        .await
        .unwrap();
    sess.execute("SAVEPOINT sp1").await.unwrap();
    sess.execute("INSERT INTO t (id, v) VALUES (888002, 2)")
        .await
        .unwrap();
    sess.execute("ROLLBACK TO SAVEPOINT sp1").await.unwrap();
    sess.execute("COMMIT").await.unwrap();

    let a = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 888001").await;
    let b = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 888002").await;
    assert_eq!(a, 1, "row inserted before SAVEPOINT must survive COMMIT; saw {a}");
    assert_eq!(
        b, 0,
        "row inserted after SAVEPOINT must be dropped by ROLLBACK TO; saw {b}"
    );
}

// ─── 7. Hot-tier DELETE fastpath must remain gated off in-tx ───────────────

#[tokio::test]
async fn rollback_undoes_delete_even_with_hottier_fastpath_envvar_on() {
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    unsafe { std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1") };

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("DELETE FROM t WHERE id = 2").await.unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    let n = count(&sess, "SELECT COUNT(*) FROM t").await;
    let id2 = count(&sess, "SELECT COUNT(*) FROM t WHERE id = 2").await;

    match prev {
        Some(v) => unsafe { std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v) },
        None => unsafe { std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH") },
    }

    assert_eq!(
        n, 3,
        "ROLLBACK must restore the row deleted via fastpath; saw {n}"
    );
    assert_eq!(id2, 1, "id=2 must reappear after ROLLBACK");
}
