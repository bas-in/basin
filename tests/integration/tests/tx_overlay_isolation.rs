//! Hot-tier overlay transaction-snapshot isolation (the documented gap, now
//! closed).
//!
//! Closes the leak called out in `snapshot_isolation.rs`'s "Known limitation"
//! note: another session's hot-tier UPDATE override / DELETE tombstone written
//! by the AUTO-COMMIT fast path used to leak into an open transaction's pinned
//! snapshot, because `MemRowValue` carried no sequence and the overlay union
//! (`snapshot_tombstones` / `snapshot_updates` in `hot_tombstone.rs`, consumed
//! by `HtapUnionTable` / `register_cold_with_overlay`) could not filter by
//! recency.
//!
//! Fix under test:
//!   * `basin-hottier::MemTable` now stamps every insert/delete with a
//!     monotonic per-table MVCC sequence (`current_seq` / `snapshot_with_seq`).
//!   * On its first in-tx touch of a table, a transaction pins
//!     `TxState::hot_seq_watermark` = registry `hot_tier_seq` (alongside the
//!     cold `read_snapshots` pin, in `load_table_for_read`).
//!   * The overlay read path drops any registry entry whose `seq` exceeds the
//!     pinned watermark — a post-snapshot write by another session.
//!   * The transaction's OWN `tx_overlay` entries are merged on top and always
//!     win (read-your-own-writes).
//!   * Auto-commit reads pass `None` for the watermark → no filtering, no cost.
//!
//! These tests force the auto-commit UPDATE/DELETE fast paths ON (the writers
//! that populate the shared registry overlay), exactly the case that leaked.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across parallel tests in this binary (the
// `BASIN_HOTTIER_*_FASTPATH` gates are process-wide; same convention as
// `tx_fastpath_dml.rs`).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ── Engine / session builders ─────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn open(eng: &Engine, project: ProjectId) -> ProjectSession {
    eng.open_session(project).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Run `fut` with BOTH auto-commit fast paths forced ON for its duration,
/// under the env lock so other tests never observe the flipped gate.
async fn with_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev_u = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    let prev_d = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    let prev_kill = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE");
    let out = fut.await;
    restore("BASIN_HOTTIER_UPDATE_FASTPATH", prev_u);
    restore("BASIN_HOTTIER_DELETE_FASTPATH", prev_d);
    restore("BASIN_HOTTIER_FASTPATH_DISABLE", prev_kill);
    out
}

fn restore(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

// ── Read helpers ───────────────────────────────────────────────────────────────

fn int_value(c: &Arc<dyn Array>, row: usize) -> i64 {
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        a.value(row)
    } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
        a.value(row) as i64
    } else {
        panic!("column is not an int array: {:?}", c.data_type());
    }
}

/// `SELECT val FROM t WHERE id = <id>` → single value (None when the row is
/// absent / deleted). Uses a WHERE-pk shape so it goes through the same overlay
/// read path inside the tx.
async fn val_at(sess: &ProjectSession, table: &str, id: i64) -> Option<i64> {
    let sql = format!("SELECT val FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() > 0 {
            return Some(int_value(b.column(0), 0));
        }
    }
    None
}

async fn count_rows(sess: &ProjectSession, table: &str) -> i64 {
    match sess
        .execute(&format!("SELECT count(*) FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows from count(*), got {other:?}"),
    }
}

/// Seed `(id INT PRIMARY KEY, val INT)` with rows `(i, i*10)`, committed cold.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id INT PRIMARY KEY, val INT)"),
    )
    .await;
    for i in 1..=n {
        exec(
            sess,
            &format!("INSERT INTO {table} (id, val) VALUES ({i}, {})", i * 10),
        )
        .await;
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// Session B's auto-commit fast-path UPDATE on a row A already read inside an
/// open transaction must NOT leak into A's pinned snapshot. After A COMMITs,
/// A sees B's value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_session_update_hidden_until_commit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;

        // A opens a tx and reads row 3 — this pins A's cold snapshot AND its
        // hot-tier MVCC watermark for table `t`.
        exec(&a, "BEGIN").await;
        assert_eq!(val_at(&a, "t", 3).await, Some(30), "A's pinned pre-B value");

        // B (auto-commit) does a fast-path UPDATE on the same row → writes an
        // Update override into the SHARED registry with seq > A's watermark.
        exec(&b, "UPDATE t SET val = 999 WHERE id = 3").await;
        // C (a fresh auto-commit reader / B itself) sees B's write immediately.
        assert_eq!(
            val_at(&b, "t", 3).await,
            Some(999),
            "auto-commit reader must see the fast-path UPDATE immediately"
        );

        // A re-reads inside the still-open tx: MUST see the OLD value. The
        // override's seq exceeds A's pinned watermark, so it is filtered out.
        assert_eq!(
            val_at(&a, "t", 3).await,
            Some(30),
            "B's post-snapshot UPDATE leaked into A's open transaction"
        );

        exec(&a, "COMMIT").await;

        // Post-COMMIT the pin is released: A now sees B's committed value.
        assert_eq!(
            val_at(&a, "t", 3).await,
            Some(999),
            "post-COMMIT A must see B's fast-path UPDATE value"
        );
    })
    .await;
}

/// Session B's auto-commit fast-path DELETE (tombstone) must NOT hide the row
/// from A's open transaction. After A COMMITs, the row is gone for A too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_session_delete_does_not_hide_row_in_open_tx() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;

        exec(&a, "BEGIN").await;
        let c1 = count_rows(&a, "t").await;
        assert_eq!(c1, 5, "A's first in-tx count; saw {c1}");
        assert_eq!(val_at(&a, "t", 2).await, Some(20));

        // B fast-path DELETEs row 2 (auto-commit) → tombstone, seq > watermark.
        exec(&b, "DELETE FROM t WHERE id = 2").await;
        assert_eq!(
            count_rows(&b, "t").await,
            4,
            "auto-commit reader must see the fast-path DELETE immediately"
        );

        // A's open tx must still see row 2 and the original count.
        assert_eq!(
            val_at(&a, "t", 2).await,
            Some(20),
            "B's tombstone leaked: it hid row 2 from A's open transaction"
        );
        let c2 = count_rows(&a, "t").await;
        assert_eq!(c2, c1, "A's in-tx count must stay stable; saw {c2} vs {c1}");

        exec(&a, "COMMIT").await;

        // Post-COMMIT the pin is released: A sees the deletion.
        assert_eq!(val_at(&a, "t", 2).await, None, "post-COMMIT row 2 is gone");
        assert_eq!(
            count_rows(&a, "t").await,
            4,
            "post-COMMIT count reflects DELETE"
        );
    })
    .await;
}

/// A's OWN in-tx fast-path UPDATE remains visible to A (read-your-own-writes)
/// even while another session's concurrent UPDATE is filtered out. The tx
/// overlay is layered on top of the (watermark-filtered) shared registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn own_in_tx_update_visible_while_other_filtered() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;

        exec(&a, "BEGIN").await;
        // Pin the snapshot/watermark by touching the table.
        assert_eq!(val_at(&a, "t", 4).await, Some(40));

        // A's own in-tx fast-path UPDATE on row 4 (lands in tx_overlay).
        exec(&a, "UPDATE t SET val = 444 WHERE id = 4").await;
        // B's concurrent auto-commit UPDATE on the SAME row (shared registry).
        exec(&b, "UPDATE t SET val = 888 WHERE id = 4").await;

        // A must see ITS OWN value (444), not B's (888) and not the cold (40).
        assert_eq!(
            val_at(&a, "t", 4).await,
            Some(444),
            "A must read-its-own-write (444), not B's leaked 888 nor stale 40"
        );

        exec(&a, "COMMIT").await;
    })
    .await;
}

/// Repeat-read COUNT(*) stability under concurrent fast-path DML — the exact
/// `snapshot_isolation` harness shape, but with hot-tier UPDATE/DELETE writes
/// (the overlay) by another session instead of catalog-advancing INSERTs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeat_read_count_stable_under_concurrent_fastpath_dml() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 20).await;

        exec(&a, "BEGIN").await;
        let c1 = count_rows(&a, "t").await;
        assert_eq!(c1, 20, "A's first in-tx count; saw {c1}");

        // B fires a burst of auto-commit fast-path UPDATEs AND DELETEs.
        for id in 1..=5i64 {
            exec(
                &b,
                &format!("UPDATE t SET val = val + 1000 WHERE id = {id}"),
            )
            .await;
        }
        for id in 6..=8i64 {
            exec(&b, &format!("DELETE FROM t WHERE id = {id}")).await;
        }
        assert_eq!(
            count_rows(&b, "t").await,
            17,
            "auto-commit reader sees the 3 deletes immediately"
        );

        // A's repeated count must remain the pinned value — none of B's
        // post-snapshot overlay writes leak in (neither the updates nor the
        // tombstones change A's count, but the tombstones especially must not
        // drop rows from A's view).
        for _ in 0..3 {
            let c = count_rows(&a, "t").await;
            assert_eq!(c, c1, "A's in-tx count must stay {c1}; saw {c}");
            // The updated rows must still show their pre-snapshot value for A.
            assert_eq!(
                val_at(&a, "t", 1).await,
                Some(10),
                "row 1 stale-stable for A"
            );
            // A deleted-by-B row must still be visible to A.
            assert_eq!(
                val_at(&a, "t", 6).await,
                Some(60),
                "row 6 must survive for A"
            );
        }

        exec(&a, "COMMIT").await;

        // Post-COMMIT A sees the full effect of B's overlay writes.
        assert_eq!(
            count_rows(&a, "t").await,
            17,
            "post-COMMIT count reflects deletes"
        );
        assert_eq!(
            val_at(&a, "t", 1).await,
            Some(1010),
            "post-COMMIT sees B's update"
        );
        assert_eq!(
            val_at(&a, "t", 6).await,
            None,
            "post-COMMIT sees B's delete"
        );
    })
    .await;
}
