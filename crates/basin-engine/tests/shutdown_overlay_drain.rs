//! Graceful-shutdown overlay drain (durability bug 1).
//!
//! The auto-commit point fast paths (`hot_tier_update_by_pk` /
//! `hot_tier_delete_by_pk`) park their row images / tombstones in the
//! process-wide `MemTableRegistry` overlay with NO WAL record — durability
//! comes only when the overlay is materialized into cold Parquet/Vortex and the
//! catalog commits. On a graceful restart the background overlay reconciler may
//! not have run since the last point mutation, so without an explicit drain the
//! last reconcile-window of acked point mutations would be lost on reopen.
//!
//! `Engine::drain_overlays_for_shutdown` (invoked by basin-server's SIGINT
//! handler before `wal.close()`) forces every pending overlay to durable cold
//! storage. These tests prove the contract: write via the point fast path,
//! drain, reopen a fresh engine over the SAME (persistent) catalog + storage,
//! and assert the mutation survived.
//!
//! The background reconciler is disabled (`BASIN_OVERLAY_RECONCILE_SECS=0`) so
//! durability across the reopen is attributable SOLELY to the shutdown drain —
//! making the drain load-bearing. Without the drain (or with the bug), the
//! reopen would observe the pre-mutation cold state.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Disable the background overlay reconciler exactly once for this test binary
/// so the only path that can drain the overlay is the shutdown drain under
/// test.
fn disable_background_reconciler() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| std::env::set_var("BASIN_OVERLAY_RECONCILE_SECS", "0"));
}

/// Build an engine over a SHARED catalog + storage dir so a second engine can
/// be opened to model a process restart.
fn engine_over(
    catalog: Arc<dyn basin_catalog::Catalog>,
    dir: &std::path::Path,
) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected result for {sql:?}: {other:?}"),
    }
}

fn i64s(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// An acked point UPDATE must survive a graceful shutdown drain + reopen.
#[tokio::test]
async fn point_update_survives_shutdown_drain_and_reopen() {
    basin_common::telemetry::try_init_for_tests();
    disable_background_reconciler();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());

    // ── Lifetime 1: create, seed, point-UPDATE, then drain on "shutdown". ──
    {
        let eng = engine_over(catalog.clone(), dir.path());
        let sess = eng.open_session(project).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t (id, n) VALUES (1, 100), (2, 200), (3, 300)")
            .await
            .unwrap();
        // Make the seed rows durable in cold (so the reopen has a base to read),
        // mirroring how the INSERT path's own flush eventually lands them.
        eng.drain_overlays_for_shutdown().await.unwrap();

        // Point UPDATE on the PK — the auto-commit overlay fast-path shape.
        sess.execute("UPDATE t SET n = 999 WHERE id = 2")
            .await
            .unwrap();

        // The graceful-shutdown drain: force the overlay to durable cold
        // storage before the (modeled) wal.close().
        eng.drain_overlays_for_shutdown().await.unwrap();
        // engine + session dropped here = process exit.
    }

    // ── Lifetime 2: fresh engine over the SAME catalog + storage = restart. ──
    {
        let eng = engine_over(catalog.clone(), dir.path());
        let sess = eng.open_session(project).await.unwrap();
        let got = rows(&sess, "SELECT n FROM t WHERE id = 2").await;
        assert_eq!(
            i64s(&got, "n"),
            vec![999],
            "the acked point UPDATE must survive the shutdown drain + reopen",
        );
        // The untouched rows are intact too.
        let all = rows(&sess, "SELECT n FROM t ORDER BY id").await;
        assert_eq!(i64s(&all, "n"), vec![100, 999, 300]);
    }
}

/// An acked point DELETE must survive a graceful shutdown drain + reopen — the
/// tombstone has to be materialized into cold, not just held in the registry.
#[tokio::test]
async fn point_delete_survives_shutdown_drain_and_reopen() {
    basin_common::telemetry::try_init_for_tests();
    disable_background_reconciler();

    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());

    {
        let eng = engine_over(catalog.clone(), dir.path());
        let sess = eng.open_session(project).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t (id, n) VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();
        eng.drain_overlays_for_shutdown().await.unwrap();

        sess.execute("DELETE FROM t WHERE id = 2").await.unwrap();
        eng.drain_overlays_for_shutdown().await.unwrap();
    }

    {
        let eng = engine_over(catalog.clone(), dir.path());
        let sess = eng.open_session(project).await.unwrap();
        let all = rows(&sess, "SELECT id FROM t ORDER BY id").await;
        assert_eq!(
            i64s(&all, "id"),
            vec![1, 3],
            "the acked point DELETE tombstone must survive the shutdown drain + reopen",
        );
    }
}
