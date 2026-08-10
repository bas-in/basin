//! ADR 0020 §6 — WAL TxCommit marker end-to-end tests.
//!
//! Verifies the four behaviour variants described in the ADR:
//!
//! 1. Crash-mid-tx (no Commit, no Rollback at EOF) — entries must be discarded.
//! 2. Explicit commit present — entries must be visible after replay.
//! 3. Explicit rollback present — entries must be discarded (regression guard).
//! 4. Auto-commit (no BEGIN at all) — entries must replay verbatim.
//!
//! Each test writes directly to a `LocalWal`, then calls `read_events` +
//! `replay_wal` to simulate what a crash-recovery path would do.  No engine
//! or catalog involvement is needed — this is a pure WAL-layer test.

use std::sync::Arc;
use std::time::Duration;

use basin_common::{PartitionKey, ProjectId};
use basin_wal::{replay_wal, LocalWal, Lsn, Wal, WalConfig, WalEvent, WalReplayConfig};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn local_cfg(dir: &TempDir) -> WalConfig {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    WalConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        flush_interval: Duration::from_millis(50),
        flush_max_bytes: 1024 * 1024,
        commit_delay: Duration::from_millis(2),
    }
}

fn row(i: u64) -> Bytes {
    Bytes::from(format!("row-{i:08}"))
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 1: crash-mid-tx — no Commit, no Rollback.
//
// Simulates a writer that called BEGIN, wrote some rows, then crashed before
// calling COMMIT or ROLLBACK.  Replay must discard the orphaned rows.
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wal_replay_buffers_until_explicit_commit() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // Write a TxBegin + INSERT rows, then flush — simulating a crash before
    // COMMIT.  We manually build the event stream rather than close-and-reopen
    // because a real crash leaves the segment on disk exactly as flushed.
    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
        let tx_id: u64 = 42;
        wal.append_tx_begin(&project, &part, tx_id).await.unwrap();
        for i in 0..10u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }
        // No append_tx_commit — simulates crash before COMMIT.
        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    // Re-open (crash recovery path).
    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // Sanity: we should see Begin + 10 entries, but no Commit.
    let begin_count = events
        .iter()
        .filter(|e| matches!(e, WalEvent::Begin { .. }))
        .count();
    let commit_count = events
        .iter()
        .filter(|e| matches!(e, WalEvent::Commit { .. }))
        .count();
    let entry_count = events
        .iter()
        .filter(|e| matches!(e, WalEvent::Entry(_)))
        .count();
    assert_eq!(begin_count, 1, "expected 1 Begin marker");
    assert_eq!(commit_count, 0, "expected no Commit markers (crash case)");
    assert_eq!(entry_count, 10, "expected 10 raw entry events");

    // With require_explicit_commit = true AND has_any_commit = false, the
    // replay falls through to legacy implicit-commit because no Commit markers
    // were observed — this is by design for back-compat with old segments.
    //
    // To test the *new* crash-mid-tx discard path, we inject an artificial
    // Commit for a *different* tx_id so that `has_any_commit` is true, then
    // verify tx 42's entries are dropped.
    let mut events_with_stale_commit = events.clone();
    // Inject a Commit for a different tx_id (tx 99 — never started) to
    // flip `has_any_commit = true` without affecting committed entries.
    events_with_stale_commit.push(WalEvent::Commit { tx_id: 99 });

    let cfg = WalReplayConfig {
        suppress_rolled_back: true,
        require_explicit_commit: true,
    };
    let replayed = replay_wal(events_with_stale_commit, &cfg);

    assert_eq!(
        replayed.len(),
        0,
        "crash-mid-tx entries must be discarded when a post-§6 segment \
         (has Commit markers) is replayed; got {} entries",
        replayed.len()
    );

    wal.close().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2: explicit COMMIT present — entries must replay.
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wal_replay_honors_explicit_commit() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
        let tx_id: u64 = 7;
        wal.append_tx_begin(&project, &part, tx_id).await.unwrap();
        for i in 0..5u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }
        // Explicit commit: the new code path.
        wal.append_tx_commit(&project, &part, tx_id).await.unwrap();
        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // Verify the Commit marker round-tripped through the segment format.
    let commit_count = events
        .iter()
        .filter(|e| matches!(e, WalEvent::Commit { tx_id: 7 }))
        .count();
    assert_eq!(
        commit_count, 1,
        "expected exactly 1 Commit marker for tx_id=7 after flush+reopen; \
         got commit_count={commit_count}, events={events:?}"
    );

    let cfg = WalReplayConfig::default();
    let replayed = replay_wal(events, &cfg);

    assert_eq!(
        replayed.len(),
        5,
        "all 5 explicitly-committed rows must replay; got {}",
        replayed.len()
    );

    wal.close().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 3: explicit ROLLBACK — entries must be discarded (regression guard).
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wal_replay_discards_explicit_rollback() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
        let tx_id: u64 = 13;
        wal.append_tx_begin(&project, &part, tx_id).await.unwrap();
        for i in 0..8u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }
        // Explicit rollback: rows must be suppressed.
        wal.append_tx_rollback(&project, &part, tx_id)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    let cfg = WalReplayConfig::default();
    let replayed = replay_wal(events, &cfg);

    assert_eq!(
        replayed.len(),
        0,
        "rolled-back rows must not appear in replay; got {}",
        replayed.len()
    );

    wal.close().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 4: auto-commit — standalone INSERT with no BEGIN.
//
// Entries outside any transaction are committed immediately on replay;
// this back-compat path must be unchanged.
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wal_replay_auto_commit_unchanged() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
        // No BEGIN — raw appends are "auto-commit" in WAL terms.
        for i in 0..20u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // No markers at all in a raw-append segment.
    let marker_count = events
        .iter()
        .filter(|e| !matches!(e, WalEvent::Entry(_)))
        .count();
    assert_eq!(marker_count, 0, "no markers expected in auto-commit WAL");

    let cfg = WalReplayConfig::default();
    let replayed = replay_wal(events, &cfg);

    assert_eq!(
        replayed.len(),
        20,
        "all 20 auto-commit rows must replay verbatim; got {}",
        replayed.len()
    );
    for (i, entry) in replayed.iter().enumerate() {
        assert_eq!(
            entry.lsn,
            Lsn((i + 1) as u64),
            "LSN ordering must be preserved"
        );
    }

    wal.close().await.unwrap()
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 5: mixed session — committed tx + auto-commit rows in same segment.
//
// Verifies that `TxCommit` correctly separates the committed transaction
// from surrounding auto-commit rows, and that all expected rows appear.
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wal_replay_mixed_explicit_commit_and_auto_commit() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();

        // 3 auto-commit rows before the transaction.
        for i in 0..3u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }

        // Explicit transaction: 5 rows + explicit commit.
        let tx_id: u64 = 100;
        wal.append_tx_begin(&project, &part, tx_id).await.unwrap();
        for i in 10..15u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }
        wal.append_tx_commit(&project, &part, tx_id).await.unwrap();

        // 2 more auto-commit rows after the transaction.
        for i in 20..22u64 {
            wal.append(&project, &part, row(i)).await.unwrap();
        }

        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    let cfg = WalReplayConfig::default();
    let replayed = replay_wal(events, &cfg);

    // 3 pre-tx auto-commit + 5 in-tx committed + 2 post-tx auto-commit = 10.
    assert_eq!(
        replayed.len(),
        10,
        "expected 10 total rows (3 + 5 + 2); got {}",
        replayed.len()
    );

    wal.close().await.unwrap();
}
