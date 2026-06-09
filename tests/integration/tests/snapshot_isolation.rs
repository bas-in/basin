//! Transaction snapshot-stable reads (REPEATABLE-READ-ish).
//!
//! Guards the bench/harness shape:
//!
//!   session A: BEGIN
//!   session A: SELECT COUNT(*)        -- N
//!   session B: INSERT ...; COMMIT     -- advances the catalog head
//!   session A: SELECT COUNT(*)        -- MUST still be N (snapshot stable)
//!   session A: COMMIT
//!
//! Implementation under test (see `basin-engine::session`):
//!   * `tx_begin` seeds `TxState::read_snapshots` from the session's observed
//!     heads; tables first touched *inside* the tx are pinned on first read.
//!   * `load_table_for_read` reconstructs the catalog-live file set at the
//!     pinned snapshot via `Catalog::load_table_at_snapshot`, so another
//!     session's later commit (a *higher* catalog snapshot id) is invisible.
//!   * The auto-commit reader (session B between/after these steps, and any
//!     session with no open tx) always sees the live head.
//!
//! ## What is covered
//!
//! The guarantee here is **cold-snapshot (catalog) stability**: a concurrent
//! INSERT by another session flushes to Parquet and advances the catalog
//! snapshot, which the pinning hides. This is the exact harness shape, and it
//! is exercised both with a real shard+WAL (matching
//! `compare_postgres_common.rs::build_basin_engine`) and with the plain
//! in-memory engine.
//!
//! ## Known limitation (NOT covered, by design)
//!
//! Rows another session writes via the hot-tier DELETE/UPDATE fast path land
//! in the shared `MemTableRegistry` as tombstones / overrides that carry no
//! sequence/LSN, so the read-path overlay union cannot filter them by a
//! transaction watermark. A concurrent hot-tier UPDATE/DELETE of the same rows
//! can therefore still leak into an open transaction's reads. Closing that gap
//! needs a sequence field on `MemRowValue` (basin-hottier). Concurrent
//! INSERTs are *not* affected (they advance the catalog snapshot, which is
//! pinned correctly), so the harness shape is fully isolated.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Plain in-memory engine (no shard): in-tx INSERTs buffer in `TxState` and
/// commit to the catalog on COMMIT.
async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

/// Shard + WAL engine, matching `compare_postgres_common.rs::build_basin_engine`.
/// In this configuration a committed INSERT lands in the shard tail and is
/// flushed to a Parquet file (a new catalog snapshot) on the next reader's
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

async fn count(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => scalar_i64(&batches),
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

/// Core harness shape on the plain in-memory engine.
///
/// A's two in-tx COUNT(*) reads must be equal even though B commits a row in
/// between. B's own (auto-commit) read must see its row immediately.
#[tokio::test]
async fn two_session_snapshot_stable_count_in_memory() {
    let (_dir, eng) = open_engine().await;
    // Both sessions share one project so they see the same catalog table.
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    a.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    a.execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    // A opens a transaction and pins its read-view at first SELECT.
    a.execute("BEGIN").await.unwrap();
    let first = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(first, 3, "A's first in-tx count; saw {first}");

    // B commits a new row (auto-commit) — advances the catalog head.
    b.execute("INSERT INTO t VALUES (4)").await.unwrap();
    let b_sees = count(&b, "SELECT COUNT(*) FROM t").await;
    assert_eq!(b_sees, 4, "B (auto-commit) must see its own committed row; saw {b_sees}");

    // A's second read MUST still be the pinned snapshot value.
    let second = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        second, first,
        "A's repeated in-tx count must equal the first (snapshot stable); \
         saw {second} vs {first} — B's concurrent insert leaked"
    );

    a.execute("COMMIT").await.unwrap();

    // After COMMIT the snapshot pin is released: A now sees B's row too.
    let after = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(after, 4, "post-COMMIT A must see the live head (4 rows); saw {after}");
}

/// Same shape on the shard+WAL engine — the configuration the bench harness
/// (`build_basin_engine`) actually uses. B's committed INSERT flows through the
/// shard tail and is flushed to a new Parquet snapshot on the next reader's
/// `flush_to_parquet()`; the cold-snapshot pin hides it from A's open tx.
#[tokio::test]
async fn two_session_snapshot_stable_count_shard() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    a.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    for k in 0..5i64 {
        a.execute(&format!("INSERT INTO t (id, v) VALUES ({k}, {k})"))
            .await
            .unwrap();
    }

    a.execute("BEGIN").await.unwrap();
    let first = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(first, 5, "A's first in-tx count; saw {first}");

    // B inserts and commits (auto-commit). This forces a shard flush to
    // Parquet → a new catalog snapshot on the next read.
    b.execute("INSERT INTO t (id, v) VALUES (999, 1)")
        .await
        .unwrap();
    let b_sees = count(&b, "SELECT COUNT(*) FROM t").await;
    assert_eq!(b_sees, 6, "B must see its own committed row; saw {b_sees}");

    // A's second read must remain pinned.
    let second = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        second, first,
        "shard-path: A's repeated in-tx count must equal the first; \
         saw {second} vs {first}"
    );

    a.execute("COMMIT").await.unwrap();
    let after = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(after, 6, "post-COMMIT A sees the live head; saw {after}");
}

/// Cold-only variant: B's INSERT is committed AND a fresh reader has already
/// forced it to a Parquet snapshot *before* A's second read. This isolates the
/// pure catalog-snapshot reconstruction path (`load_table_at_snapshot`) from
/// any shard-tail timing: the row is unambiguously in a higher cold snapshot
/// when A reads, and must still be hidden.
#[tokio::test]
async fn snapshot_stable_when_other_commit_already_flushed_cold() {
    let (_sd, _wd, eng, _bg, _wal) = open_engine_with_shard().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    a.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    a.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();

    a.execute("BEGIN").await.unwrap();
    let first = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(first, 2, "A first in-tx count; saw {first}");

    // B commits, then B reads — the read flushes the shard tail to Parquet, so
    // the catalog head is now strictly ahead of A's pin via a cold snapshot.
    b.execute("INSERT INTO t VALUES (3)").await.unwrap();
    let _ = count(&b, "SELECT COUNT(*) FROM t").await;

    let second = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        second, first,
        "A must read its pinned cold snapshot (B's committed+flushed row hidden); \
         saw {second} vs {first}"
    );
    a.execute("COMMIT").await.unwrap();
}

/// Read-your-own-writes must still hold *inside* the pinned transaction: an
/// INSERT by A after BEGIN is visible to A's later reads, even though the
/// catalog-live half is pinned. This guards that snapshot pinning does not
/// hide the transaction's own buffered writes.
#[tokio::test]
async fn own_writes_visible_under_snapshot_pin() {
    let (_dir, eng) = open_engine().await;
    let project = ProjectId::new();
    let a = eng.open_session(project).await.unwrap();
    let b = eng.open_session(project).await.unwrap();

    a.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    a.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();

    a.execute("BEGIN").await.unwrap();
    let base = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(base, 2, "A baseline in-tx count; saw {base}");

    // A's own in-tx insert (read-your-own-writes).
    a.execute("INSERT INTO t VALUES (10)").await.unwrap();
    // B commits concurrently — must stay hidden from A.
    b.execute("INSERT INTO t VALUES (99)").await.unwrap();

    let a_view = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        a_view, 3,
        "A must see its own uncommitted row (3 = 2 pinned + 1 own) but NOT B's; \
         saw {a_view}"
    );

    a.execute("COMMIT").await.unwrap();
    // After commit both A's row and B's row are live: 4 total.
    let after = count(&a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(after, 4, "post-COMMIT all four rows visible; saw {after}");
}
