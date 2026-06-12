//! Phase 1 CI-blocking regression harness (ADR 0028 §3, `cdc-design.md` §4).
//!
//! This is the standing gate proving the HTAP hot-tier fast-path UPDATE/DELETE
//! and in-transaction mutations are captured by CDC. If a future fast path is
//! added without wiring event dispatch, these tests go red and block merge.
//!
//! It is a NORMAL test (no `#[ignore]`): it must run in CI.
//!
//! The harness drives the real `basin-engine` with a real `CdcRingWriter`
//! attached as a post-commit sink — exactly how production wires it — so it
//! exercises the true capture seam, not a mock.

use std::sync::Arc;

use basin_cdc::{CdcConfig, CdcRingWriter};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_and_writer(dir: &TempDir) -> (Engine, CdcRingWriter) {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: None,
    });
    // Flush every event immediately so assertions don't race the time window.
    let cfg = CdcConfig {
        flush_interval: std::time::Duration::from_millis(5),
        ..CdcConfig::default()
    };
    let writer = CdcRingWriter::with_config(storage, cfg);
    engine.attach_post_commit_sink(Arc::new(writer.clone()));
    (engine, writer)
}

/// Drain the durable ring for a project until it has at least `want` records or
/// we run out of patience. Post-commit sinks dispatch on a spawned task, so we
/// poll instead of assuming synchronous delivery.
async fn drain_ring(writer: &CdcRingWriter, project: &ProjectId, want: usize) -> Vec<basin_cdc::CdcRecord> {
    for _ in 0..200 {
        writer.flush_all_for_test().await;
        let recs = writer.ring_for(project).replay_after(0).await.unwrap();
        if recs.len() >= want {
            return recs;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    writer.ring_for(project).replay_after(0).await.unwrap()
}

/// Test 1 (cdc-design.md §4): hot-tier UPDATE emits a CDC event with the
/// correct before/after images. Single-column int PK + single-PK-equality
/// WHERE → takes `hot_tier_update_by_pk`, which now builds before/after
/// `ChangeEvent`s from the pre-image (`current`) + post-image rows and
/// `dispatch_post_commit`s them after the overlay write (gated on
/// `registry_has_any` so the zero-sink hot path stays allocation-free).
#[tokio::test]
async fn hot_tier_update_captured() {
    let dir = TempDir::new().unwrap();
    let (engine, writer) = engine_and_writer(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").await.unwrap();
    sess.execute("INSERT INTO t VALUES (1, 0)").await.unwrap();
    sess.execute("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();

    let recs = drain_ring(&writer, &project, 2).await;
    let upd = recs
        .iter()
        .find(|r| r.op_name() == "update")
        .expect("an update event must be captured (hot-tier fast path)");
    assert_eq!(upd.before.as_ref().unwrap()["v"].as_i64(), Some(0), "before.v");
    assert_eq!(upd.after.as_ref().unwrap()["v"].as_i64(), Some(1), "after.v");
}

/// Test 2: hot-tier DELETE emits a CDC event carrying the before image and a
/// null after. → `hot_tier_delete_by_pk`. The fast path now reads each matched
/// PK's pre-image (lazily, only when a sink is attached) before writing the
/// tombstone and `dispatch_post_commit`s a DELETE event (`after = None`).
#[tokio::test]
async fn hot_tier_delete_captured() {
    let dir = TempDir::new().unwrap();
    let (engine, writer) = engine_and_writer(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").await.unwrap();
    sess.execute("INSERT INTO t VALUES (2, 'bob')").await.unwrap();
    sess.execute("DELETE FROM t WHERE id = 2").await.unwrap();

    let recs = drain_ring(&writer, &project, 2).await;
    let del = recs
        .iter()
        .find(|r| r.op_name() == "delete")
        .expect("a delete event must be captured (hot-tier fast path)");
    assert_eq!(del.before.as_ref().unwrap()["name"].as_str(), Some("bob"), "before.name");
    assert!(del.after.is_none(), "delete after image is null");
}

/// Test 3: hot-tier UPDATE inside an explicit transaction is captured as one
/// tx group in seq order, committed-only — and ROLLBACK emits nothing.
///
/// In-transaction hot-tier UPDATEs write to `TxState::tx_overlay` and buffer a
/// `TxChangeEvent` (no seq yet) in `TxState::tx_change_events`. The executor's
/// COMMIT drain assigns each a commit-ordered `(project, table)` seq and
/// `dispatch_post_commit`s them AFTER the overlay drains into the shared
/// memtable; ROLLBACK drops the buffer, so the rolled-back v=999 update never
/// reaches the ring.
#[tokio::test]
async fn in_tx_multi_statement_committed_in_order_and_rollback_emits_nothing() {
    let dir = TempDir::new().unwrap();
    let (engine, writer) = engine_and_writer(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").await.unwrap();
    sess.execute("INSERT INTO t VALUES (3, 10), (4, 100)").await.unwrap();

    // --- ROLLBACK path emits nothing -------------------------------------
    sess.execute("BEGIN").await.unwrap();
    sess.execute("UPDATE t SET v = 999 WHERE id = 3").await.unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    // --- committed multi-statement tx ------------------------------------
    sess.execute("BEGIN").await.unwrap();
    sess.execute("UPDATE t SET v = 20 WHERE id = 3").await.unwrap();
    sess.execute("UPDATE t SET v = 200 WHERE id = 4").await.unwrap();
    sess.execute("COMMIT").await.unwrap();

    // 2 inserts + 2 committed updates = 4. The rolled-back update (v=999)
    // must be absent.
    let recs = drain_ring(&writer, &project, 4).await;

    let updates: Vec<&basin_cdc::CdcRecord> =
        recs.iter().filter(|r| r.op_name() == "update").collect();
    assert_eq!(updates.len(), 2, "exactly the two committed updates");
    assert!(
        recs.iter().all(|r| r.after.as_ref().and_then(|a| a["v"].as_i64()) != Some(999)),
        "rolled-back mutation (v=999) must never appear in the CDC ring",
    );

    // Seq strictly monotonic per project, and the tx updates are in order.
    for w in recs.windows(2) {
        assert!(w[0].seq < w[1].seq, "seq strictly monotonic: {recs:?}");
    }
    let upd_for = |id: i64| -> i64 {
        updates
            .iter()
            .find(|r| r.before.as_ref().unwrap()["id"].as_i64() == Some(id))
            .unwrap()
            .after
            .as_ref()
            .unwrap()["v"]
            .as_i64()
            .unwrap()
    };
    assert_eq!(upd_for(3), 20);
    assert_eq!(upd_for(4), 200);
    // The two tx updates carry contiguous, in-order seqs.
    let id3_seq = updates
        .iter()
        .find(|r| r.before.as_ref().unwrap()["id"].as_i64() == Some(3))
        .unwrap()
        .seq;
    let id4_seq = updates
        .iter()
        .find(|r| r.before.as_ref().unwrap()["id"].as_i64() == Some(4))
        .unwrap()
        .seq;
    assert!(id3_seq < id4_seq, "tx statements captured in execution order");
}

/// Replay-from-cursor returns exactly the missed events.
#[tokio::test]
async fn replay_from_cursor_exact() {
    let dir = TempDir::new().unwrap();
    let (engine, writer) = engine_and_writer(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").await.unwrap();
    for i in 1..=5 {
        sess.execute(&format!("INSERT INTO t VALUES ({i}, {i})")).await.unwrap();
    }
    let all = drain_ring(&writer, &project, 5).await;
    assert!(all.len() >= 5);

    // Resume after the 2nd inserted row's seq → exactly the later events.
    let cursor = all[1].seq;
    let missed = writer.ring_for(&project).replay_after(cursor).await.unwrap();
    assert!(
        missed.iter().all(|r| r.seq > cursor),
        "replay returns only seq > cursor",
    );
    assert_eq!(
        missed.len(),
        all.len() - 2,
        "exactly the events after the cursor",
    );
}

/// Cross-project isolation: project B's ring never contains A's events.
#[tokio::test]
async fn cross_project_isolation_end_to_end() {
    let dir = TempDir::new().unwrap();
    let (engine, writer) = engine_and_writer(&dir);
    let a = ProjectId::new();
    let b = ProjectId::new();
    let sa = engine.open_session(a).await.unwrap();
    let sb = engine.open_session(b).await.unwrap();
    for s in [&sa, &sb] {
        s.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").await.unwrap();
    }
    sa.execute("INSERT INTO t VALUES (1, 11)").await.unwrap();
    sa.execute("INSERT INTO t VALUES (2, 22)").await.unwrap();
    sb.execute("INSERT INTO t VALUES (1, 99)").await.unwrap();

    let a_recs = drain_ring(&writer, &a, 2).await;
    let b_recs = drain_ring(&writer, &b, 1).await;

    assert!(a_recs.iter().all(|r| r.project == a), "A ring is A-only");
    assert!(b_recs.iter().all(|r| r.project == b), "B ring never sees A");
    // B never observes A's v=11/v=22 payloads.
    assert!(
        b_recs
            .iter()
            .all(|r| r.after.as_ref().and_then(|x| x["v"].as_i64()) == Some(99)),
        "project B only sees its own committed rows",
    );
}
