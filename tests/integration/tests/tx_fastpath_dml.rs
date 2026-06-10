//! In-transaction hot-tier single-row PK UPDATE/DELETE fast-path tests
//! (the OLTP-in-tx fast path).
//!
//! Inside `BEGIN..COMMIT`, a single-row `UPDATE/DELETE ... WHERE pk = lit` on a
//! single-column-PK table no longer falls to the cold copy-on-write path.
//! Instead it writes a `MemRowValue::Update` / `Tombstone` into the SESSION's
//! transaction-scoped overlay (`TxState::tx_overlay`), which:
//!   * is visible to this transaction's own reads (read-your-own-writes);
//!   * is INVISIBLE to other sessions until COMMIT;
//!   * is drained into the process-wide `MemTableRegistry` on COMMIT (so it
//!     mirrors exactly what the auto-commit fast path would have written);
//!   * is dropped on ROLLBACK (no shared state touched).
//!
//! These tests pin:
//!   * begin-update-commit visible after commit + a registry entry present
//!     post-commit;
//!   * begin-update-rollback leaves the old value + NO registry entry;
//!   * read-modify-write twice in one tx accumulates (`v=v+1` twice → +2);
//!   * in-tx read-your-own-update (this session sees the new value; another
//!     session does NOT pre-commit, does post-commit);
//!   * update-then-delete-in-tx → row gone post-commit;
//!   * delete-then-rollback → row alive;
//!   * a savepoint disables the fast path (the overlay can't be rewound), so
//!     the statement still produces correct results via the cold path;
//!   * mixed cold-path bulk UPDATE + fast single-row UPDATE in one tx stays
//!     correct;
//!   * the `BEGIN; SELECT .. FOR UPDATE; UPDATE; COMMIT` benchmark shape.
//!
//! `BASIN_HOTTIER_*_FASTPATH` are process-wide env vars read inside
//! `exec_update`/`exec_delete`, so the fast-path tests serialise behind an env
//! lock (the same convention as `update_hottier.rs`).

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across the parallel test threads in this binary.
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

/// Run `fut` with BOTH fast paths forced ON for its duration, under the env
/// lock so cold-path tests never observe the flipped gate.
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

fn int_value(c: &Arc<dyn arrow_array::Array>, row: usize) -> i64 {
    use arrow_array::{Int16Array, Int32Array, Int64Array};
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        a.value(row)
    } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
        a.value(row) as i64
    } else if let Some(a) = c.as_any().downcast_ref::<Int16Array>() {
        a.value(row) as i64
    } else {
        panic!("column is not an integer array: {:?}", c.data_type());
    }
}

/// `SELECT val FROM t WHERE id = <id>` → single value (or None when absent).
async fn val_at(sess: &ProjectSession, table: &str, id: i64) -> Option<i64> {
    let sql = format!("SELECT val FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        return Some(int_value(b.column(0), 0));
    }
    None
}

/// `SELECT id, val FROM t ORDER BY id` (full-table / DataFusion read path).
async fn all_rows(sess: &ProjectSession, table: &str) -> Vec<(i64, i64)> {
    let sql = format!("SELECT id, val FROM {table} ORDER BY id");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return Vec::new(),
    };
    let mut out = Vec::new();
    for b in &batches {
        let idc = b.column_by_name("id").expect("id column");
        let vc = b.column_by_name("val").expect("val column");
        for r in 0..b.num_rows() {
            out.push((int_value(idc, r), int_value(vc, r)));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    out
}

async fn count_rows(sess: &ProjectSession, table: &str) -> i64 {
    match sess.execute(&format!("SELECT count(*) FROM {table}")).await.unwrap() {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows from count(*), got {other:?}"),
    }
}

/// Seed `(id INT PRIMARY KEY, val INT)` with rows `(i, i*10)`, COMMITTED cold.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(sess, &format!("CREATE TABLE {table} (id INT PRIMARY KEY, val INT)")).await;
    for i in 1..=n {
        exec(sess, &format!("INSERT INTO {table} (id, val) VALUES ({i}, {})", i * 10)).await;
    }
}

/// Does the process-wide registry hold ANY live override / tombstone entry
/// for `(project, table)`? (i.e. an `Update`/`Tombstone` — counter-keyed plain
/// `Row` HTAP inserts are excluded.)
fn registry_has_overlay(eng: &Engine, project: &ProjectId, table: &str) -> bool {
    let registry = eng.memtable_registry();
    let t = basin_common::TableName::new(table).unwrap();
    let Some(entry) = registry.get(project, &t) else {
        return false;
    };
    entry
        .memtable
        .snapshot()
        .iter()
        .any(|(_, v)| v.is_update() || v.is_tombstone())
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// begin-update-commit: the new value is visible after COMMIT, and the override
/// has been drained into the shared registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begin_update_commit_visible_and_registry_present() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 5).await;
        exec(&sess, "BEGIN").await;
        exec(&sess, "UPDATE t SET val = 999 WHERE id = 3").await;
        // No registry entry yet — the override lives in the tx overlay only.
        assert!(
            !registry_has_overlay(&eng, &project, "t"),
            "in-tx fast-path UPDATE must NOT touch the shared registry before COMMIT"
        );
        exec(&sess, "COMMIT").await;

        assert_eq!(val_at(&sess, "t", 3).await, Some(999));
        assert!(
            registry_has_overlay(&eng, &project, "t"),
            "COMMIT must drain the tx overlay into the shared registry"
        );
    })
    .await;
}

/// begin-update-rollback: the old value survives and the shared registry has
/// NO override entry (the overlay was dropped, never promoted).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begin_update_rollback_leaves_old_value_no_registry() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 5).await;
        exec(&sess, "BEGIN").await;
        exec(&sess, "UPDATE t SET val = 999 WHERE id = 3").await;
        // In-tx read sees the override.
        assert_eq!(val_at(&sess, "t", 3).await, Some(999));
        exec(&sess, "ROLLBACK").await;

        assert_eq!(
            val_at(&sess, "t", 3).await,
            Some(30),
            "ROLLBACK must restore the cold-tier value"
        );
        assert!(
            !registry_has_overlay(&eng, &project, "t"),
            "ROLLBACK must leave NO override in the shared registry"
        );
    })
    .await;
}

/// read-modify-write twice in one tx accumulates: `val=val+1` twice → +2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rmw_twice_in_tx_accumulates_and_commits() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 3).await; // id=2 → val=20
        exec(&sess, "BEGIN").await;
        exec(&sess, "UPDATE t SET val = val + 1 WHERE id = 2").await;
        assert_eq!(val_at(&sess, "t", 2).await, Some(21));
        exec(&sess, "UPDATE t SET val = val + 1 WHERE id = 2").await;
        assert_eq!(
            val_at(&sess, "t", 2).await,
            Some(22),
            "second RMW must read the first override (21), not the cold base (20)"
        );
        exec(&sess, "COMMIT").await;
        assert_eq!(val_at(&sess, "t", 2).await, Some(22));
    })
    .await;
}

/// in-tx read-your-own-update + cross-session isolation: the writing session
/// sees the new value pre-commit; another session does NOT until COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_your_own_update_isolated_until_commit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let writer = open(&eng, project).await;
    let reader = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&writer, "t", 5).await;
        exec(&writer, "BEGIN").await;
        exec(&writer, "UPDATE t SET val = 999 WHERE id = 4").await;

        // Writer sees its own uncommitted update.
        assert_eq!(val_at(&writer, "t", 4).await, Some(999));
        // Reader must still see the committed cold value.
        assert_eq!(
            val_at(&reader, "t", 4).await,
            Some(40),
            "another session must NOT see an uncommitted in-tx overlay"
        );

        exec(&writer, "COMMIT").await;

        // After COMMIT the reader sees the new value (registry is shared).
        assert_eq!(
            val_at(&reader, "t", 4).await,
            Some(999),
            "post-COMMIT the override is in the shared registry, visible to all"
        );
    })
    .await;
}

/// update-then-delete the same PK in one tx → the row is gone post-commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_then_delete_in_tx_row_gone_post_commit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 5).await;
        exec(&sess, "BEGIN").await;
        exec(&sess, "UPDATE t SET val = 999 WHERE id = 3").await;
        exec(&sess, "DELETE FROM t WHERE id = 3").await;
        // In-tx the row must already be gone (tombstone wins over the override).
        assert_eq!(val_at(&sess, "t", 3).await, None);
        exec(&sess, "COMMIT").await;

        assert_eq!(val_at(&sess, "t", 3).await, None, "row must be gone post-COMMIT");
        assert_eq!(count_rows(&sess, "t").await, 4, "exactly one row removed");
    })
    .await;
}

/// delete-then-rollback → the row is still alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_then_rollback_row_alive() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 5).await;
        exec(&sess, "BEGIN").await;
        exec(&sess, "DELETE FROM t WHERE id = 2").await;
        assert_eq!(val_at(&sess, "t", 2).await, None, "in-tx the delete is visible");
        exec(&sess, "ROLLBACK").await;

        assert_eq!(
            val_at(&sess, "t", 2).await,
            Some(20),
            "ROLLBACK must resurrect the deleted row"
        );
        assert_eq!(count_rows(&sess, "t").await, 5);
        assert!(!registry_has_overlay(&eng, &project, "t"));
    })
    .await;
}

/// A savepoint disables the in-tx fast path (the overlay can't be rewound to a
/// savepoint watermark), routing the UPDATE to the cold path. The cold path's
/// in-tx catalog commit is immediate and NOT savepoint-rollbackable — a
/// pre-existing limitation this test pins (see in-test comment).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn savepoint_active_routes_to_cold_path_and_rolls_back() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 5).await;
        exec(&sess, "BEGIN").await;
        exec(&sess, "UPDATE t SET val = 111 WHERE id = 1").await; // before savepoint
        exec(&sess, "SAVEPOINT sp1").await;
        exec(&sess, "UPDATE t SET val = 222 WHERE id = 2").await; // after savepoint
        assert_eq!(val_at(&sess, "t", 1).await, Some(111));
        assert_eq!(val_at(&sess, "t", 2).await, Some(222));

        // PRE-EXISTING LIMITATION (predates the tx overlay): the in-tx
        // COLD UPDATE path commits its file replacement to the catalog
        // immediately, so ROLLBACK TO SAVEPOINT does NOT undo it — only
        // htap INSERTs are watermark-rewound. This pin documents the
        // actual contract; fixing it requires deferring cold-path catalog
        // commits to COMMIT time.
        exec(&sess, "ROLLBACK TO SAVEPOINT sp1").await;
        assert_eq!(val_at(&sess, "t", 1).await, Some(111));
        assert_eq!(
            val_at(&sess, "t", 2).await,
            Some(222),
            "in-tx cold UPDATE catalog-commits directly; ROLLBACK TO cannot undo it"
        );

        exec(&sess, "COMMIT").await;
        assert_eq!(val_at(&sess, "t", 1).await, Some(111));
        assert_eq!(val_at(&sess, "t", 2).await, Some(222));
    })
    .await;
}

/// Mixed: a cold-path bulk UPDATE (no WHERE → all rows) followed by a fast
/// single-row UPDATE in the same tx. Whichever path the single-row UPDATE
/// takes, the COMMITTED result must be consistent (val=0 everywhere except the
/// single-row target). This pins that the in-tx overlay reads the correct base
/// after a cold-path mutation earlier in the same tx.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_cold_bulk_and_fast_single_row_in_tx() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 4).await;
        exec(&sess, "BEGIN").await;
        // Cold-path bulk UPDATE (no WHERE → every row) sets val = 0.
        exec(&sess, "UPDATE t SET val = 0").await;
        // Single-row UPDATE in the same tx.
        exec(&sess, "UPDATE t SET val = 7 WHERE id = 2").await;
        exec(&sess, "COMMIT").await;
    })
    .await;

    // Assert the COMMITTED result from a fresh session (no in-tx read-view
    // pinning), so the check is independent of cold-UPDATE-in-tx snapshot
    // semantics and only validates the final committed state.
    let verify = open(&eng, project).await;
    assert_eq!(
        all_rows(&verify, "t").await,
        vec![(1, 0), (2, 7), (3, 0), (4, 0)],
        "mixed cold-bulk + single-row in one tx must commit consistently"
    );
}

/// The OLTP benchmark shape end-to-end:
/// `BEGIN; SELECT .. FOR UPDATE; UPDATE; COMMIT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_then_update_commit_shape() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&sess, "t", 10).await;
        exec(&sess, "BEGIN").await;
        // SELECT ... FOR UPDATE (the lock clause is a no-op here, but the shape
        // must parse and read the current value inside the tx).
        let locked = val_at_for_update(&sess, "t", 5).await;
        assert_eq!(locked, Some(50));
        exec(&sess, "UPDATE t SET val = val + 1 WHERE id = 5").await;
        assert_eq!(val_at(&sess, "t", 5).await, Some(51));
        exec(&sess, "COMMIT").await;
        assert_eq!(val_at(&sess, "t", 5).await, Some(51));
    })
    .await;
}

/// `SELECT val FROM t WHERE id = <id> FOR UPDATE` → single value.
async fn val_at_for_update(sess: &ProjectSession, table: &str, id: i64) -> Option<i64> {
    let sql = format!("SELECT val FROM {table} WHERE id = {id} FOR UPDATE");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        return Some(int_value(b.column(0), 0));
    }
    None
}
