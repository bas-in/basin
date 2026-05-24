//! Regression tests for `HtapUnionTable::scan` — UpdateOverlayExec wiring.
//!
//! `HtapUnionTable` is the DataFusion table provider registered by
//! `session::refresh_table_with_htap` when a transaction has both cold-tier
//! Parquet files AND in-memory (in-tx) hot batches. Before the fix, its `scan`
//! applied `TombstoneFilterExec` to the cold plan but NOT `UpdateOverlayExec`,
//! so a fast-path UPDATE that preceded the transaction was invisible: the stale
//! cold row appeared alongside the new in-memory row image (duplicate).
//!
//! ## Test scenario
//!
//! 1. CREATE TABLE + INSERT N rows outside a transaction (cold Parquet files).
//! 2. Issue a fast-path UPDATE on one cold row (outside tx, writes to the
//!    process-wide `MemTableRegistry`). The UPDATE fast path is gated OFF inside
//!    a transaction, so this must happen before BEGIN.
//! 3. BEGIN a transaction and INSERT a new row. This triggers
//!    `refresh_table_with_htap` → `HtapUnionTable` is registered in the
//!    session's DataFusion context with both cold Parquet and in-tx MemTable.
//! 4. SELECT * FROM t inside the transaction. DataFusion calls
//!    `HtapUnionTable::scan`, which must now apply the UpdateOverlayExec so
//!    the updated row shows the NEW value (not duplicated with the stale one).
//! 5. SELECT COUNT(*) FROM t inside the transaction. Must equal N+1 (the N
//!    cold rows plus the one in-tx INSERT), NOT N+2 (which would indicate the
//!    stale cold row leaked through alongside the override).
//! 6. ROLLBACK.
//!
//! ## Env-var serialisation
//!
//! `BASIN_HOTTIER_UPDATE_FASTPATH` is process-wide. Tests that need it set
//! serialise behind `ENV_LOCK` (a tokio Mutex held across await points) so a
//! cold-path test never observes the flipped gate. Run with `--test-threads=1`
//! for belt-and-braces isolation.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialise env-var mutation across test threads in this binary.
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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

// ── Env-gate helpers ──────────────────────────────────────────────────────────

/// Run `fut` with `BASIN_HOTTIER_UPDATE_FASTPATH=1` for its duration.
async fn with_update_fastpath_on<F, R>(fut: F) -> R
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

// ── Read helpers ──────────────────────────────────────────────────────────────

/// `SELECT id, val FROM t ORDER BY id` → collected rows.
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

/// `SELECT COUNT(*) FROM t` → row count.
async fn count_rows(sess: &ProjectSession, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows from COUNT(*), got {other:?}"),
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
        panic!("column is not an integer array: {:?}", c.data_type())
    }
}

/// Seed `(id INT PRIMARY KEY, val INT)` with N rows `(i, i*10)` into cold
/// Parquet (non-transactional path).
async fn seed_cold(sess: &ProjectSession, table: &str, n: i64) {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Core correctness test: after a fast-path UPDATE on a cold row, a SELECT
/// routed through the HtapUnionTable path (inside a transaction that has
/// in-memory hot batches) must:
///   - show the updated value for the modified row (not the stale cold value),
///   - show each row exactly once (no duplicate of the updated row),
///   - return a row count equal to cold_rows + in_tx_inserts.
#[tokio::test]
async fn htap_union_update_overlay_no_duplicate_updated_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    // Step 1: Seed cold Parquet with 5 rows.
    seed_cold(&sess, "t", 5).await;

    // Step 2: Fast-path UPDATE row id=3 (val 30 → 999) outside any transaction.
    // The update fast path exits when tx_is_active, so we do this before BEGIN.
    with_update_fastpath_on(exec(&sess, "UPDATE t SET val = 999 WHERE id = 3")).await;

    // Step 3: BEGIN and INSERT a new row inside the transaction. This triggers
    // refresh_table_with_htap → HtapUnionTable is registered for this session.
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, val) VALUES (6, 60)").await;

    // Step 4: SELECT id, val inside the transaction. DataFusion routes through
    // HtapUnionTable::scan. The UpdateOverlayExec must suppress the stale cold
    // row for id=3 and surface the override value 999 exactly once.
    let rows = all_rows(&sess, "t").await;
    assert_eq!(
        rows,
        vec![(1, 10), (2, 20), (3, 999), (4, 40), (5, 50), (6, 60)],
        "HtapUnionTable::scan must show the updated val and no stale duplicate"
    );

    // Step 5: COUNT(*) must be 6 (5 cold + 1 in-tx), not 7 (which would mean
    // the stale cold row for id=3 leaked through alongside the override).
    let cnt = count_rows(&sess, "t").await;
    assert_eq!(
        cnt, 6,
        "row count must be cold_rows + in_tx_inserts, stale row must not appear"
    );

    exec(&sess, "ROLLBACK").await;
}

/// The non-PK-projected case: `SELECT val FROM t` (PK not in projection) after
/// a fast-path UPDATE. The PK-augment / projection-strip path in
/// HtapUnionTable::scan must correctly strip the injected PK column so the
/// outer plan sees only the originally-requested schema.
#[tokio::test]
async fn htap_union_update_overlay_non_pk_projection() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    seed_cold(&sess, "t", 3).await;

    // Fast-path UPDATE row id=2 (val 20 → 777) before the transaction.
    with_update_fastpath_on(exec(&sess, "UPDATE t SET val = 777 WHERE id = 2")).await;

    // Open transaction with a hot INSERT to force HtapUnionTable registration.
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, val) VALUES (4, 40)").await;

    // SELECT only `val` (PK column not in projection). The overlay must still
    // apply correctly via the PK-augment / strip mechanism.
    let sql = "SELECT val FROM t ORDER BY val";
    let batches = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => panic!("expected rows"),
    };
    let mut vals: Vec<i64> = Vec::new();
    for b in &batches {
        let c = b.column(0);
        for r in 0..b.num_rows() {
            vals.push(int_value(c, r));
        }
    }
    vals.sort();

    // Expected: [10, 40, 777, 30] sorted = [10, 30, 40, 777].
    // (old val=20 for id=2 must NOT appear; new 777 appears exactly once)
    assert_eq!(
        vals,
        vec![10, 30, 40, 777],
        "non-PK projection must see updated val without stale duplicate"
    );
    assert_eq!(vals.len(), 4, "row count must be 4 (3 cold + 1 in-tx)");

    exec(&sess, "ROLLBACK").await;
}

/// UPDATE then COUNT(*) — verifies no duplicate rows inflate the count even
/// when the fast-path UPDATE overlay is active in HtapUnionTable.
#[tokio::test]
async fn htap_union_update_overlay_count_unchanged() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    seed_cold(&sess, "t", 4).await;

    with_update_fastpath_on(exec(&sess, "UPDATE t SET val = 555 WHERE id = 1")).await;

    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, val) VALUES (5, 50)").await;

    let cnt = count_rows(&sess, "t").await;
    assert_eq!(cnt, 5, "UPDATE must not inflate COUNT(*); expected cold+in_tx=5");

    exec(&sess, "ROLLBACK").await;
}
