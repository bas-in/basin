//! WAL durability contract — group-commit fsync + per-session
//! `basin.synchronous_commit` end-to-end tests.
//!
//! The contract under test:
//!
//! * **async** (`synchronous_commit = off`, the default): INSERT acks once
//!   the WAL entry is in RAM (ack-before-durable). Loss window =
//!   `min(flush_interval, buffer-pressure)`. Unchanged behavior.
//! * **sync** (`SET basin.synchronous_commit = on`): INSERT only acks after
//!   the WAL group-commits the entry's segment to the backing store —
//!   ack-after-durable. Concurrent sync writers coalesce into one PUT.
//!
//! "Kill" is simulated the same way `wal_tx_commit_marker.rs` does: a second
//! `LocalWal::open` over the same directory sees exactly what a crash
//! recovery would see — closed segments only, never the first process's RAM
//! buffer. The first WAL is deliberately **not** flushed or closed before
//! the reopen, and its background tick is parked at one hour so durability
//! can only come from the path under test.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId};
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Lsn, Wal, WalConfig};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// WAL config with the background tick parked (1 h) and pressure disabled,
/// so nothing flushes behind the test's back: durability observed through a
/// reopen is attributable solely to the synchronous-commit path (or to an
/// explicit `flush()` the test issues itself).
fn quiet_wal_cfg(dir: &std::path::Path) -> WalConfig {
    WalConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
        root_prefix: None,
        flush_interval: Duration::from_secs(3600),
        flush_max_bytes: u64::MAX,
        commit_delay: Duration::from_millis(1),
    }
}

/// Crash-recovery view: a fresh `LocalWal` over the same directory. Sees
/// only closed segments — exactly what a restarted process would replay.
async fn reopen_wal(dir: &std::path::Path) -> LocalWal {
    LocalWal::open(quiet_wal_cfg(dir)).await.unwrap()
}

// ──────────────────────────────────────────────────────────────────────────────
// Pure WAL level
// ──────────────────────────────────────────────────────────────────────────────

/// `append_fenced_durable` must be on the backing store the moment it
/// returns: a reopen with NO flush/close on the writer sees the entry.
#[tokio::test]
async fn durable_append_survives_immediate_reopen() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let wal = LocalWal::open(quiet_wal_cfg(dir.path())).await.unwrap();
    let lsn = wal
        .append_fenced_durable(&project, &part, Bytes::from_static(b"sync-row"), None)
        .await
        .unwrap();
    assert_eq!(lsn, Lsn(1));

    // No flush(), no close() — simulate the writer being killed right after
    // the ack.
    let recovered = reopen_wal(dir.path()).await;
    let entries = recovered.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    assert_eq!(
        entries.len(),
        1,
        "a durable-acked append must survive an immediate kill/reopen"
    );
    assert_eq!(&entries[0].payload[..], b"sync-row");
    recovered.close().await.unwrap();
    wal.close().await.unwrap();
}

/// Async-path contract pinned: a plain `append` ack is RAM-only — invisible
/// to a reopen — until something flushes. After an explicit flush the same
/// reopen sees it. (This is the documented loss window, unchanged.)
#[tokio::test]
async fn async_append_is_ram_only_until_flush() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let wal = LocalWal::open(quiet_wal_cfg(dir.path())).await.unwrap();
    wal.append(&project, &part, Bytes::from_static(b"async-row"))
        .await
        .unwrap();

    {
        let crash_view = reopen_wal(dir.path()).await;
        let entries = crash_view.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert!(
            entries.is_empty(),
            "async append is ack-before-durable: a kill inside the flush \
             window must lose it (documented contract)"
        );
        crash_view.close().await.unwrap();
    }

    wal.flush().await.unwrap();
    let recovered = reopen_wal(dir.path()).await;
    let entries = recovered.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    assert_eq!(entries.len(), 1, "flushed async append must survive reopen");
    recovered.close().await.unwrap();
    wal.close().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// Engine level — SET basin.synchronous_commit through the shard INSERT path
// ──────────────────────────────────────────────────────────────────────────────

/// Engine + shard wired over temp dirs, WAL ticks parked. Background loops
/// are intentionally NOT spawned: nothing may flush except the path under
/// test.
async fn build_engine(wal_dir: &TempDir, storage_dir: &TempDir) -> (Engine, Arc<dyn Wal>) {
    basin_common::telemetry::try_init_for_tests();
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
        LocalWal::open(quiet_wal_cfg(wal_dir.path())).await.unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (engine, wal)
}

/// Does any WAL entry under `(project, default partition)` embed the table
/// name? Shard WAL payloads are framed as `u32 table-name length || table
/// name || Arrow IPC`, so a plain byte scan is a sufficient row-presence
/// probe without re-implementing the framing.
async fn wal_has_entry_for(wal: &LocalWal, project: &ProjectId, table: &str) -> bool {
    let part = PartitionKey::default_key();
    let entries = wal.read_from(project, &part, Lsn::ZERO).await.unwrap();
    entries.iter().any(|e| {
        e.payload
            .windows(table.len())
            .any(|w| w == table.as_bytes())
    })
}

/// `SET basin.synchronous_commit = on` → INSERT → kill/reopen the WAL with
/// no flush: the row's WAL entry must already be on disk when the INSERT
/// acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_commit_on_insert_survives_wal_reopen() {
    let wal_dir = TempDir::new().unwrap();
    let storage_dir = TempDir::new().unwrap();
    let (engine, _wal) = build_engine(&wal_dir, &storage_dir).await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT, payload TEXT)")
        .await
        .unwrap();
    sess.execute("SET basin.synchronous_commit = on")
        .await
        .unwrap();
    assert!(
        sess.synchronous_commit(),
        "SET basin.synchronous_commit = on must flip the session GUC"
    );
    sess.execute("INSERT INTO events VALUES (1, 'durable-row')")
        .await
        .unwrap();

    // Kill/reopen: no flush, no close. The synchronous INSERT must already
    // have group-committed its WAL entry.
    let recovered = reopen_wal(wal_dir.path()).await;
    assert!(
        wal_has_entry_for(&recovered, &project, "events").await,
        "synchronous_commit=on INSERT must be in a closed WAL segment \
         before the statement acks"
    );
    recovered.close().await.unwrap();
}

/// Default / `off` path unchanged: the INSERT acks from RAM (nothing on
/// disk at the reopen), and an explicit WAL flush makes it durable exactly
/// as before this feature existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_commit_off_insert_behavior_unchanged() {
    let wal_dir = TempDir::new().unwrap();
    let storage_dir = TempDir::new().unwrap();
    let (engine, wal) = build_engine(&wal_dir, &storage_dir).await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT, payload TEXT)")
        .await
        .unwrap();
    // Explicit off (also the default — and the engine-wide default unless
    // BASIN_SYNCHRONOUS_COMMIT says otherwise).
    sess.execute("SET basin.synchronous_commit = off")
        .await
        .unwrap();
    assert!(!sess.synchronous_commit());
    sess.execute("INSERT INTO events VALUES (1, 'fast-row')")
        .await
        .unwrap();

    {
        let crash_view = reopen_wal(wal_dir.path()).await;
        assert!(
            !wal_has_entry_for(&crash_view, &project, "events").await,
            "synchronous_commit=off INSERT acks before durability: a kill \
             inside the flush window loses the row (unchanged contract)"
        );
        crash_view.close().await.unwrap();
    }

    // The pre-existing durability gate still works.
    wal.flush().await.unwrap();
    let recovered = reopen_wal(wal_dir.path()).await;
    assert!(
        wal_has_entry_for(&recovered, &project, "events").await,
        "flushed async INSERT must survive reopen"
    );
    recovered.close().await.unwrap();
}
