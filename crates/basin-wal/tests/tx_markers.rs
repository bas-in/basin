//! Integration tests for WAL transaction markers (BEGIN / ROLLBACK) and
//! replay suppression.
//!
//! Acceptance gates from Phase 5.14.C2 prep:
//!
//! 1. Rollback suppression: `Begin` + 100 inserts + `Rollback` for tx A and
//!    `Begin` + 50 inserts + (end-of-WAL, implicit commit) for tx B — replay
//!    must emit only the 50 from B.
//!
//! 2. Interleaved tx: `Begin A`, `Insert A1`, `Begin B`, `Insert B1`,
//!    `Rollback A`, `Insert B2`, end — replay must emit `B1` + `B2`, not `A1`.

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

fn payload(i: u64) -> Bytes {
    Bytes::from(format!("row-{i:08}"))
}

// ---------------------------------------------------------------------------
// Gate 1: Rollback suppression + implicit commit at end-of-WAL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_suppresses_100_inserts_implicit_commit_emits_50() {
    let dir = TempDir::new().unwrap();
    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // Tx A: 100 inserts followed by an explicit rollback.
    let tx_a: u64 = 1;
    wal.append_tx_begin(&project, &part, tx_a).await.unwrap();
    for i in 0..100u64 {
        wal.append(&project, &part, payload(i)).await.unwrap();
    }
    wal.append_tx_rollback(&project, &part, tx_a).await.unwrap();

    // Tx B: 50 inserts with no explicit commit (implicit commit at end-of-WAL).
    let tx_b: u64 = 2;
    wal.append_tx_begin(&project, &part, tx_b).await.unwrap();
    for i in 0..50u64 {
        wal.append(&project, &part, payload(200 + i)).await.unwrap();
    }
    // No Rollback for B — implicit commit.

    wal.flush().await.unwrap();

    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // Sanity-check raw event counts before replay.
    let begin_count = events
        .iter()
        .filter(|ev| matches!(ev, WalEvent::Begin { .. }))
        .count();
    let rollback_count = events
        .iter()
        .filter(|ev| matches!(ev, WalEvent::Rollback { .. }))
        .count();
    let entry_count = events
        .iter()
        .filter(|ev| matches!(ev, WalEvent::Entry(_)))
        .count();
    assert_eq!(begin_count, 2, "expected 2 Begin markers");
    assert_eq!(rollback_count, 1, "expected 1 Rollback marker");
    assert_eq!(entry_count, 150, "expected 150 raw entry events");

    // After replay, only tx B's 50 entries should survive.
    let cfg = WalReplayConfig::default();
    let committed = replay_wal(events, &cfg);
    assert_eq!(
        committed.len(),
        50,
        "expected 50 committed entries, got {}",
        committed.len()
    );
    // All surviving entries must have been from tx B (payloads 200..249).
    for entry in &committed {
        let text = std::str::from_utf8(&entry.payload).expect("utf8 payload");
        assert!(
            text.starts_with("row-000002"),
            "unexpected payload in committed set: {text}"
        );
    }

    wal.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Gate 2: Interleaved transactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn interleaved_txs_rollback_a_emits_b1_and_b2() {
    let dir = TempDir::new().unwrap();
    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let tx_a: u64 = 10;
    let tx_b: u64 = 20;

    // Sequence: Begin A, Insert A1, Begin B, Insert B1, Rollback A, Insert B2, end
    wal.append_tx_begin(&project, &part, tx_a).await.unwrap();
    wal.append(&project, &part, Bytes::from("A1"))
        .await
        .unwrap();
    wal.append_tx_begin(&project, &part, tx_b).await.unwrap();
    wal.append(&project, &part, Bytes::from("B1"))
        .await
        .unwrap();
    wal.append_tx_rollback(&project, &part, tx_a).await.unwrap();
    wal.append(&project, &part, Bytes::from("B2"))
        .await
        .unwrap();
    // End of WAL — B is implicitly committed.

    wal.flush().await.unwrap();

    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    let cfg = WalReplayConfig::default();
    let committed = replay_wal(events, &cfg);

    assert_eq!(
        committed.len(),
        2,
        "expected B1 + B2 only, got {} entries",
        committed.len()
    );
    let payloads: Vec<&str> = committed
        .iter()
        .map(|e| std::str::from_utf8(&e.payload).unwrap())
        .collect();
    assert!(
        payloads.contains(&"B1"),
        "B1 missing from committed set: {payloads:?}"
    );
    assert!(
        payloads.contains(&"B2"),
        "B2 missing from committed set: {payloads:?}"
    );
    assert!(
        !payloads.contains(&"A1"),
        "A1 must not appear in committed set: {payloads:?}"
    );

    wal.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Back-compat: WAL with no markers replays identically
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_entries_without_markers_replay_verbatim() {
    let dir = TempDir::new().unwrap();
    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    for i in 0..30u64 {
        wal.append(&project, &part, payload(i)).await.unwrap();
    }
    wal.flush().await.unwrap();

    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // No markers in this WAL.
    let marker_count = events
        .iter()
        .filter(|ev| !matches!(ev, WalEvent::Entry(_)))
        .count();
    assert_eq!(marker_count, 0, "no markers expected in legacy WAL");

    let cfg = WalReplayConfig::default();
    let committed = replay_wal(events, &cfg);
    assert_eq!(committed.len(), 30, "all 30 legacy entries must replay");
    for (i, entry) in committed.iter().enumerate() {
        assert_eq!(entry.lsn, Lsn((i + 1) as u64));
    }

    wal.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// WalReplayConfig::suppress_rolled_back = false (v0.1-compat mode)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn suppress_false_emits_all_entries_including_rolled_back() {
    let dir = TempDir::new().unwrap();
    let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let tx: u64 = 99;
    wal.append_tx_begin(&project, &part, tx).await.unwrap();
    for i in 0..10u64 {
        wal.append(&project, &part, payload(i)).await.unwrap();
    }
    wal.append_tx_rollback(&project, &part, tx).await.unwrap();

    wal.flush().await.unwrap();

    let events = wal.read_events(&project, &part, Lsn::ZERO).await.unwrap();

    // With suppression ON, the 10 rolled-back entries are discarded.
    let suppress_on = replay_wal(
        events.clone(),
        &WalReplayConfig {
            suppress_rolled_back: true,
            require_explicit_commit: true,
        },
    );
    assert_eq!(
        suppress_on.len(),
        0,
        "all entries should be suppressed when rollback present"
    );

    // With suppression OFF, all 10 entries are emitted regardless.
    let suppress_off = replay_wal(
        events,
        &WalReplayConfig {
            suppress_rolled_back: false,
            require_explicit_commit: false,
        },
    );
    assert_eq!(
        suppress_off.len(),
        10,
        "all entries must pass through in compat mode"
    );

    wal.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// read_events + replay round-trip: flushed-to-disk then re-read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn roundtrip_flush_then_reread_events() {
    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // Write markers + data, flush, close, reopen.
    {
        let wal = LocalWal::open(local_cfg(&dir)).await.unwrap();
        let tx: u64 = 7;
        wal.append_tx_begin(&project, &part, tx).await.unwrap();
        for i in 0..5u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.append_tx_rollback(&project, &part, tx).await.unwrap();
        // 3 more entries outside any tx.
        for i in 100..103u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        wal.close().await.unwrap();
    }

    // Reopen and replay.
    let wal2 = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let events = wal2.read_events(&project, &part, Lsn::ZERO).await.unwrap();
    let committed = replay_wal(events, &WalReplayConfig::default());

    // The 5 rolled-back entries are gone; only the 3 outside the tx survive.
    assert_eq!(
        committed.len(),
        3,
        "only the 3 non-tx entries should survive replay, got {}",
        committed.len()
    );
    wal2.close().await.unwrap();
}
