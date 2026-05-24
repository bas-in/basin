//! Hot-tier UPDATE fast-path integration tests
//! (`BASIN_HOTTIER_UPDATE_FASTPATH`).
//!
//! Mirrors the DELETE fast path: for `SET col = lit WHERE pk = lit / pk IN
//! (lits)` on a single-column-PK table the engine writes a
//! `MemRowValue::Update` override into the process-wide `MemTableRegistry`
//! instead of rewriting cold-tier Parquet files. These tests assert the
//! merge-on-read overlay surfaces the new value on every read path and never
//! returns the stale cold-tier row.
//!
//! ## Coverage
//!
//! * UPDATE single row by PK → SELECT returns the new value, not the old.
//! * UPDATE then COUNT(*) unchanged (UPDATE adds/removes no rows).
//! * UPDATE row A, SELECT row B → B unchanged.
//! * UPDATE the same row twice → the latest value wins.
//! * UPDATE then DELETE the same PK → the row is gone (tombstone interaction).
//! * UPDATE with the env unset → still correct (cold copy-on-write path).
//! * Bulk UPDATE WHERE id IN (...) → all matched rows show the new value.
//!
//! NOTE: `BASIN_HOTTIER_UPDATE_FASTPATH` is a process-wide env var. The env
//! gate is read inside `exec_update`, so tests that need it set must set it
//! before issuing the UPDATE. Because Rust test binaries run tests in parallel
//! threads of ONE process, we serialise the fast-path tests behind a mutex and
//! set/clear the env around the UPDATE so a cold-path test never races a
//! fast-path test's env. Run with `--test-threads=1` for belt-and-braces.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across the parallel test threads in this binary.
// A tokio mutex so the guard can be held across `.await` points (the env var
// must stay set while the UPDATE future runs).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ── Engine builders ───────────────────────────────────────────────────────────

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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Run `fut` with `BASIN_HOTTIER_UPDATE_FASTPATH=1` set for its duration, under
/// the env lock so cold-path tests never observe the flipped gate. The lock is
/// held across the await so the env var stays set while the UPDATE future runs.
async fn with_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
    out
}

async fn with_fastpath_off<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH");
    let out = fut.await;
    if let Some(v) = prev {
        std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v);
    }
    out
}

// ── Read helpers ───────────────────────────────────────────────────────────────

/// `SELECT <col> FROM <table> WHERE id = <id>` → single i64 value (or None).
async fn scalar_at(sess: &ProjectSession, table: &str, col: &str, id: i64) -> Option<i64> {
    let sql = format!("SELECT {col} FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let c = b.column(0);
        return Some(int_value(c, 0));
    }
    None
}

/// Read all `(id, val)` rows ordered by id via a full-table SELECT (the
/// DataFusion / fast-select bulk read path — not the point-lookup probe).
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
    let sql = format!("SELECT count(*) FROM {table}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows from count(*), got {other:?}"),
    }
}

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

/// Seed a `(id INT PRIMARY KEY, val INT)` table with `n` rows
/// `(i, i * 10)` committed to the cold tier.
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

// ── Tests: fast path ON ─────────────────────────────────────────────────────────

/// UPDATE a single row by PK → SELECT returns the new value (point lookup AND
/// full-table read), not the old cold-tier value.
#[tokio::test]
async fn update_single_row_by_pk_returns_new_value() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    with_fastpath_on(exec(&sess, "UPDATE t SET val = 999 WHERE id = 3")).await;

    // Point-lookup path.
    assert_eq!(
        scalar_at(&sess, "t", "val", 3).await,
        Some(999),
        "point lookup must see the updated value"
    );
    // Full-table read path (fast-select bulk / DataFusion overlay).
    let rows = all_rows(&sess, "t").await;
    assert_eq!(
        rows,
        vec![(1, 10), (2, 20), (3, 999), (4, 40), (5, 50)],
        "full-table read must surface the override and suppress the stale cold row"
    );
}

/// UPDATE must not change the row count.
#[tokio::test]
async fn update_does_not_change_count() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    let before = count_rows(&sess, "t").await;
    with_fastpath_on(exec(&sess, "UPDATE t SET val = 777 WHERE id = 2")).await;
    let after = count_rows(&sess, "t").await;
    assert_eq!(before, 5);
    assert_eq!(after, 5, "UPDATE must not add or remove rows");
}

/// UPDATE row A, then SELECT row B → B is unchanged.
#[tokio::test]
async fn update_row_a_leaves_row_b_unchanged() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    with_fastpath_on(exec(&sess, "UPDATE t SET val = 111 WHERE id = 1")).await;
    assert_eq!(scalar_at(&sess, "t", "val", 1).await, Some(111));
    assert_eq!(
        scalar_at(&sess, "t", "val", 4).await,
        Some(40),
        "unrelated row must keep its cold-tier value"
    );
}

/// UPDATE the same row twice → the latest value wins.
#[tokio::test]
async fn update_same_row_twice_latest_wins() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 3).await;

    with_fastpath_on(async {
        exec(&sess, "UPDATE t SET val = 100 WHERE id = 2").await;
        exec(&sess, "UPDATE t SET val = 200 WHERE id = 2").await;
    })
    .await;
    assert_eq!(
        scalar_at(&sess, "t", "val", 2).await,
        Some(200),
        "second UPDATE must overwrite the first override"
    );
    assert_eq!(
        all_rows(&sess, "t").await,
        vec![(1, 10), (2, 200), (3, 30)]
    );
}

/// UPDATE then DELETE the same PK → the row is gone (override then tombstone).
#[tokio::test]
async fn update_then_delete_same_pk_row_gone() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 4).await;

    with_fastpath_on(async {
        exec(&sess, "UPDATE t SET val = 555 WHERE id = 2").await;
        // DELETE fast path also keyed by PK; tombstone overwrites the
        // Update override in the memtable.
        std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
        exec(&sess, "DELETE FROM t WHERE id = 2").await;
        std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH");
    })
    .await;
    assert_eq!(
        scalar_at(&sess, "t", "val", 2).await,
        None,
        "deleted row must not be visible even after a prior UPDATE"
    );
    assert_eq!(count_rows(&sess, "t").await, 3, "one row removed");
    assert_eq!(all_rows(&sess, "t").await, vec![(1, 10), (3, 30), (4, 40)]);
}

/// Bulk UPDATE WHERE id IN (...) → every matched row shows the new value;
/// unmatched rows are unchanged.
#[tokio::test]
async fn bulk_update_where_in_all_matched() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 6).await;

    with_fastpath_on(exec(&sess, "UPDATE t SET val = 0 WHERE id IN (2, 4, 6)")).await;
    assert_eq!(
        all_rows(&sess, "t").await,
        vec![(1, 10), (2, 0), (3, 30), (4, 0), (5, 50), (6, 0)],
        "all WHERE-IN matched rows must show the new value; others unchanged"
    );
    assert_eq!(count_rows(&sess, "t").await, 6);
}

// ── Tests: fast path OFF (cold copy-on-write path) ──────────────────────────────

/// With the env unset the UPDATE goes through the cold copy-on-write rewrite
/// and is still correct.
#[tokio::test]
async fn update_with_env_unset_cold_path_correct() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    with_fastpath_off(exec(&sess, "UPDATE t SET val = 4242 WHERE id = 3")).await;
    assert_eq!(scalar_at(&sess, "t", "val", 3).await, Some(4242));
    assert_eq!(
        all_rows(&sess, "t").await,
        vec![(1, 10), (2, 20), (3, 4242), (4, 40), (5, 50)]
    );
    assert_eq!(count_rows(&sess, "t").await, 5);
}
