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

use arrow_array::Array;
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

/// Run `fut` with the hot-tier fast path genuinely DISABLED, so the UPDATE
/// takes the cold copy-on-write rewrite. The UPDATE fast-path gate **defaults
/// ON** when `BASIN_HOTTIER_UPDATE_FASTPATH` is unset, so merely removing that
/// var does NOT force the cold path — we must assert the global kill-switch
/// `BASIN_HOTTIER_FASTPATH_DISABLE=1` (read by `dml_mutate`).
async fn with_fastpath_off<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev_disable = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_FASTPATH_DISABLE", "1");
    let out = fut.await;
    match prev_disable {
        Some(v) => std::env::set_var("BASIN_HOTTIER_FASTPATH_DISABLE", v),
        None => std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE"),
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

// ── Tests: RETURNING on the fast path ──────────────────────────────────────────

/// `UPDATE … RETURNING id, val` with the fast path ON must return the
/// post-update column values — not the stale cold-tier values.
#[tokio::test]
async fn update_returning_fastpath_returns_new_values() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    let result = with_fastpath_on(
        sess.execute("UPDATE t SET val = 888 WHERE id = 3 RETURNING id, val"),
    )
    .await
    .unwrap_or_else(|e| panic!("UPDATE … RETURNING failed: {e:?}"));

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected Rows from RETURNING, got Empty({tag})"),
    };
    // Exactly one row must be returned.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "RETURNING must return the one updated row");

    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let id_col = b.column_by_name("id").expect("id column in RETURNING");
    let val_col = b.column_by_name("val").expect("val column in RETURNING");
    assert_eq!(int_value(id_col, 0), 3, "RETURNING id must be 3");
    assert_eq!(int_value(val_col, 0), 888, "RETURNING val must be the new value 888");

    // Confirm the overlay is still correct on a subsequent read.
    assert_eq!(scalar_at(&sess, "t", "val", 3).await, Some(888));
}

/// `UPDATE … RETURNING *` on the fast path must return all columns of the
/// post-update row.
#[tokio::test]
async fn update_returning_star_fastpath() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 3).await;

    let result = with_fastpath_on(
        sess.execute("UPDATE t SET val = 777 WHERE id = 2 RETURNING *"),
    )
    .await
    .unwrap_or_else(|e| panic!("UPDATE … RETURNING * failed: {e:?}"));

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected Rows from RETURNING *, got Empty({tag})"),
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "RETURNING * must return exactly one row");

    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    // Schema must include both id and val (the full table schema).
    let id_col = b.column_by_name("id").expect("id column in RETURNING *");
    let val_col = b.column_by_name("val").expect("val column in RETURNING *");
    assert_eq!(int_value(id_col, 0), 2, "id must be 2");
    assert_eq!(int_value(val_col, 0), 777, "val must be the new value 777");

    // The overlay is correct on a subsequent point lookup.
    assert_eq!(scalar_at(&sess, "t", "val", 2).await, Some(777));
}

/// `UPDATE … RETURNING val + 1` contains an expression, not a plain column
/// ref, so the fast path MUST fall back to the cold path. The result must
/// still be correct (the expression evaluated against the post-update value).
#[tokio::test]
async fn update_returning_expression_falls_back_cold() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 4).await;

    // Even with the fast-path env set, `val + 1` is not a plain column ref
    // so routing falls through to the cold copy-on-write path.
    let result = with_fastpath_on(
        sess.execute("UPDATE t SET val = 50 WHERE id = 1 RETURNING val + 1"),
    )
    .await
    .unwrap_or_else(|e| panic!("UPDATE … RETURNING expr failed: {e:?}"));

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected Rows from RETURNING expr, got Empty({tag})"),
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "RETURNING expr must return one row");

    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    // val was set to 50, so val + 1 = 51.
    let expr_col = b.column(0);
    assert_eq!(
        int_value(expr_col, 0),
        51,
        "val + 1 must be 51 (new val=50, +1)"
    );

    // The underlying value is also correct after the cold rewrite.
    assert_eq!(scalar_at(&sess, "t", "val", 1).await, Some(50));
}

// ── Tests: read-modify-write (computed SET RHS) on the fast path ─────────────────
//
// These exercise the RMW allowlist (`rmw_rhs_is_fast_path_eligible`): with the
// fast path ON, `SET col = <allowlisted expr>` routes through the hot-tier
// overlay (reading the LATEST value: memtable Update > PK cache > cold) and the
// post-image is byte-identical to the cold DataFusion path. A non-allowlisted
// expression still falls to the cold path and must remain correct.

/// `SELECT <col> FROM <table> WHERE id = <id>` → `Some(true)` when the single
/// matched cell is SQL NULL, `Some(false)` when it holds a value, `None` when
/// no row matched. Distinguishes a NULL cell from a missing row (which
/// `scalar_at` cannot, since it returns the array's default for a null cell).
async fn is_null_at(sess: &ProjectSession, table: &str, col: &str, id: i64) -> Option<bool> {
    let sql = format!("SELECT {col} FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        return Some(b.column(0).is_null(0));
    }
    None
}

/// 10 consecutive `SET v = v + 1` on the same cold-seeded PK must accumulate
/// (each RMW reads the latest overlay, not the stale cold base), and COUNT(*)
/// must be unchanged (UPDATE replaces, never inserts).
#[tokio::test]
async fn rmw_increment_fastpath_accumulates() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    let before = count_rows(&sess, "t").await;
    with_fastpath_on(async {
        for _ in 0..10 {
            exec(&sess, "UPDATE t SET val = val + 1 WHERE id = 3").await;
        }
    })
    .await;

    // Seed value for id=3 is 3*10 = 30; ten +1 increments → 40.
    assert_eq!(
        scalar_at(&sess, "t", "val", 3).await,
        Some(40),
        "ten v=v+1 increments must accumulate from the cold base (30+10=40)"
    );
    // Full-table read must agree (overlay suppresses the stale cold row).
    assert_eq!(
        all_rows(&sess, "t").await,
        vec![(1, 10), (2, 20), (3, 40), (4, 40), (5, 50)],
        "only id=3 changed; the accumulated overlay is the value seen on bulk read"
    );
    assert_eq!(
        count_rows(&sess, "t").await,
        before,
        "RMW UPDATE must not add or remove rows"
    );
}

/// `SET val = CASE WHEN val > <lit> THEN <lit> ELSE val END` on the fast path
/// must produce the same result as the cold DataFusion path.
#[tokio::test]
async fn rmw_case_when_fastpath() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await; // vals 10,20,30,40,50

    with_fastpath_on(async {
        // id=2 (val 20): 20 > 25 is false → ELSE keeps 20.
        exec(
            &sess,
            "UPDATE t SET val = CASE WHEN val > 25 THEN 0 ELSE val END WHERE id = 2",
        )
        .await;
        // id=4 (val 40): 40 > 25 is true → 0.
        exec(
            &sess,
            "UPDATE t SET val = CASE WHEN val > 25 THEN 0 ELSE val END WHERE id = 4",
        )
        .await;
    })
    .await;

    assert_eq!(scalar_at(&sess, "t", "val", 2).await, Some(20), "20>25 false → unchanged");
    assert_eq!(scalar_at(&sess, "t", "val", 4).await, Some(0), "40>25 true → 0");
    assert_eq!(count_rows(&sess, "t").await, 5);
}

/// Regression for the Int32-PK crossed-value race (was ~60% flaky):
///
/// On an `INT PRIMARY KEY` (physical Int32) table, two fast-path CASE UPDATEs
/// write PK-keyed overlay entries for id=2 (→20) and id=4 (→0). Before the fix,
/// `batch_matches_predicates` did a strict `downcast::<Int64Array>` so the
/// Int32-decoded overlay row failed re-validation in the point-lookup probe —
/// dropping the SELECT onto the cold path, where `apply_update_overlay_to_batches`
/// appended BOTH override rows (id=2 AND id=4) regardless of the `WHERE id = k`
/// filter. The result batches were `[(2,20),(4,0)]` in nondeterministic HashMap
/// order, so a point SELECT returned the wrong key's row.
///
/// Two fixes are exercised here: (1) numeric coercion in
/// `batch_matches_predicates` so the overlay direct-get hit is served, and (2)
/// `apply_update_overlay_to_batches` re-applies the query predicate to appended
/// override rows. The point SELECTs must return EXACTLY one row with the correct
/// value, and a single batch from each point lookup must hold at most one row.
/// Run the assert loop several times to flush out any residual nondeterminism.
#[tokio::test]
async fn rmw_case_when_int32_pk_no_crossed_values() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await; // INT PK (Int32), vals 10,20,30,40,50

    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE t SET val = CASE WHEN val > 25 THEN 0 ELSE val END WHERE id = 2",
        )
        .await;
        exec(
            &sess,
            "UPDATE t SET val = CASE WHEN val > 25 THEN 0 ELSE val END WHERE id = 4",
        )
        .await;
    })
    .await;

    // Repeat the point-SELECT battery to catch any HashMap-order nondeterminism.
    for _ in 0..20 {
        // id=2: 20 > 25 false → unchanged 20. Must NOT cross to id=4's 0.
        let v2 = point_select_exactly_one(&sess, "t", "val", 2).await;
        assert_eq!(v2, 20, "id=2 must read its own value, never id=4's post-image");
        // id=4: 40 > 25 true → 0. Must NOT cross to id=2's 20.
        let v4 = point_select_exactly_one(&sess, "t", "val", 4).await;
        assert_eq!(v4, 0, "id=4 must read its own value, never id=2's value");
        // Untouched rows.
        assert_eq!(point_select_exactly_one(&sess, "t", "val", 1).await, 10);
        assert_eq!(point_select_exactly_one(&sess, "t", "val", 3).await, 30);
        assert_eq!(point_select_exactly_one(&sess, "t", "val", 5).await, 50);
    }
    assert_eq!(count_rows(&sess, "t").await, 5);
    // Full-table read must also reflect exactly the post-update state.
    assert_eq!(
        all_rows(&sess, "t").await,
        vec![(1, 10), (2, 20), (3, 30), (4, 0), (5, 50)],
    );
}

/// `SELECT <col> FROM <table> WHERE id = <id>` asserting the result has EXACTLY
/// one row across all batches (a crossed-value bug surfaced as a SECOND appended
/// override row for the non-matching key). Returns the single value.
async fn point_select_exactly_one(
    sess: &ProjectSession,
    table: &str,
    col: &str,
    id: i64,
) -> i64 {
    let sql = format!("SELECT {col} FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => panic!("id={id} returned no rows"),
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "WHERE id={id} must match exactly one row, got {total} (crossed-value overlay leak)"
    );
    for b in &batches {
        if b.num_rows() == 1 {
            return int_value(b.column(0), 0);
        }
    }
    unreachable!("total==1 guarantees a single-row batch");
}

/// NULL propagation: `SET v = v + 1` where `v` is SQL NULL must stay NULL
/// (NULL + 1 = NULL), identical to the cold DataFusion path. Uses a table with
/// a nullable column seeded NULL on the cold tier.
#[tokio::test]
async fn rmw_null_propagation() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    exec(&sess, "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await;
    exec(&sess, "INSERT INTO n (id, v) VALUES (1, NULL), (2, 7)").await;

    with_fastpath_on(async {
        exec(&sess, "UPDATE n SET v = v + 1 WHERE id = 1").await; // NULL + 1 = NULL
        exec(&sess, "UPDATE n SET v = v + 1 WHERE id = 2").await; // 7 + 1 = 8
    })
    .await;

    assert_eq!(
        is_null_at(&sess, "n", "v", 1).await,
        Some(true),
        "NULL + 1 must stay NULL (matching DataFusion null propagation)"
    );
    assert_eq!(
        is_null_at(&sess, "n", "v", 2).await,
        Some(false),
        "7 + 1 = 8 is non-null"
    );
    assert_eq!(scalar_at(&sess, "n", "v", 2).await, Some(8));
    assert_eq!(count_rows(&sess, "n").await, 2);
}

/// Cross-column RMW: `SET a = b + 1` must read the pre-image of column `b`
/// (the other column), not the post-image of `a`.
#[tokio::test]
async fn rmw_cross_column() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    exec(
        &sess,
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
    )
    .await;
    exec(&sess, "INSERT INTO c (id, a, b) VALUES (1, 5, 100), (2, 6, 200)").await;

    with_fastpath_on(exec(&sess, "UPDATE c SET a = b + 1 WHERE id = 1")).await;

    let a = match sess.execute("SELECT a FROM c WHERE id = 1").await.unwrap() {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(a, 101, "a must be b(100)+1 = 101 (reads pre-image b)");
    // b is untouched; the other row is untouched.
    let b = match sess.execute("SELECT b FROM c WHERE id = 1").await.unwrap() {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(b, 100, "b must be unchanged");
    assert_eq!(count_rows(&sess, "c").await, 2);
}

// ── Tests: batched multi-key expression eval (write-time batching) ────────────
//
// The hot-tier UPDATE fast path evaluates a multi-key expression SET in ONE
// `apply_assignments` call over the concatenated pre-images (instead of one
// fresh DataFusion SessionContext per key). These tests pin that the batched
// post-images are identical to the slow (cold copy-on-write) path — the
// semantics oracle — across CASE/arithmetic, jsonb_set, and NULL pre-images,
// at the full `SMALL_BULK_FASTPATH_CAP`-sized key count the IN-list shape
// routes through the overlay.

/// 64-key expression UPDATE (CASE + arithmetic) through the fast path must
/// produce row-for-row identical results to the same UPDATE forced down the
/// slow path (`BASIN_HOTTIER_FASTPATH_DISABLE=1`) on an identically seeded
/// twin table.
#[tokio::test]
async fn rmw_bulk_64_keys_expr_matches_slow_path() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "tf", 70).await;
    seed(&sess, "ts", 70).await;

    let in_list = (1..=64).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
    let set = "val = CASE WHEN val > 320 THEN val * 2 ELSE val + 1 END";
    with_fastpath_on(exec(
        &sess,
        &format!("UPDATE tf SET {set} WHERE id IN ({in_list})"),
    ))
    .await;
    with_fastpath_off(exec(
        &sess,
        &format!("UPDATE ts SET {set} WHERE id IN ({in_list})"),
    ))
    .await;

    let fast = all_rows(&sess, "tf").await;
    let slow = all_rows(&sess, "ts").await;
    assert_eq!(
        fast, slow,
        "64-key batched expr eval must match the cold (slow) path row-for-row"
    );
    // Independent expectation so a shared bug can't hide: seed val is i*10;
    // i ≤ 32 → 320-or-less → +1; i ≥ 33 → ×2; i > 64 untouched.
    let expected: Vec<(i64, i64)> = (1..=70)
        .map(|i| {
            let v = i * 10;
            let nv = if i > 64 {
                v
            } else if v > 320 {
                v * 2
            } else {
                v + 1
            };
            (i, nv)
        })
        .collect();
    assert_eq!(fast, expected, "batched CASE/arithmetic post-images");
    assert_eq!(count_rows(&sess, "tf").await, 70);
}

/// Multi-key UPDATE assigning TWO expression columns in one statement —
/// `CASE`+arithmetic on an integer column AND `jsonb_set` on a JSONB column —
/// must match the slow path exactly (values and JSON documents).
#[tokio::test]
async fn rmw_bulk_multi_expr_jsonb_case_arith_matches_slow_path() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    for t in ["jf", "js"] {
        exec(
            &sess,
            &format!("CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT, payload JSONB)"),
        )
        .await;
        for i in 1..=8 {
            exec(
                &sess,
                &format!(
                    "INSERT INTO {t} (id, val, payload) VALUES ({i}, {}, '{{\"a\":{i},\"tag\":\"x\"}}')",
                    i * 10
                ),
            )
            .await;
        }
    }

    let set = "val = CASE WHEN val > 45 THEN val * 2 ELSE val + 1 END, \
               payload = jsonb_set(payload, '{a}', '99')";
    with_fastpath_on(exec(
        &sess,
        &format!("UPDATE jf SET {set} WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8)"),
    ))
    .await;
    with_fastpath_off(exec(
        &sess,
        &format!("UPDATE js SET {set} WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8)"),
    ))
    .await;

    for i in 1..=8 {
        let vf = scalar_at(&sess, "jf", "val", i).await;
        let vs = scalar_at(&sess, "js", "val", i).await;
        assert_eq!(vf, vs, "id={i}: fast val must match slow val");
        let expected = if i * 10 > 45 { i * 20 } else { i * 10 + 1 };
        assert_eq!(vf, Some(expected), "id={i}: CASE/arith post-image");

        let pf = payload_json(&sess, "jf", i).await;
        let ps = payload_json(&sess, "js", i).await;
        assert_eq!(pf, ps, "id={i}: fast payload must match slow payload");
        assert_eq!(
            pf,
            serde_json::json!({"a": 99, "tag": "x"}),
            "id={i}: jsonb_set must replace a and keep tag"
        );
    }
    assert_eq!(count_rows(&sess, "jf").await, 8);
}

/// Fetch the full `payload` JSONB document for one row as a serde_json value.
/// Decodes whichever physical type the engine returns (LargeBinary / Binary /
/// Utf8).
async fn payload_json(sess: &ProjectSession, table: &str, id: i64) -> serde_json::Value {
    use arrow_array::{BinaryArray, LargeBinaryArray, StringArray};
    let sql = format!("SELECT payload FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    };
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no row for {sql:?}"));
    assert_eq!(b.num_rows(), 1, "exactly one row for id={id}");
    let col = b.column(0);
    if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        return serde_json::from_slice(arr.value(0)).expect("decode LargeBinary payload");
    }
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        return serde_json::from_slice(arr.value(0)).expect("decode Binary payload");
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return serde_json::from_str(arr.value(0)).expect("decode Utf8 payload");
    }
    panic!("payload column unexpected type {:?}", col.data_type());
}

/// Mixed NULL pre-images in one multi-key expression UPDATE: `v = v + 1` over
/// keys where some pre-images are NULL must keep NULL NULL (DataFusion null
/// propagation) and increment the rest — identical to the slow path.
#[tokio::test]
async fn rmw_bulk_mixed_null_preimages_match_slow_path() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    for t in ["nf", "ns"] {
        exec(
            &sess,
            &format!("CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"),
        )
        .await;
        exec(
            &sess,
            &format!("INSERT INTO {t} (id, val) VALUES (1, NULL), (2, 7), (3, NULL), (4, 9), (5, 11)"),
        )
        .await;
    }

    with_fastpath_on(exec(&sess, "UPDATE nf SET val = val + 1 WHERE id IN (1, 2, 3, 4)")).await;
    with_fastpath_off(exec(&sess, "UPDATE ns SET val = val + 1 WHERE id IN (1, 2, 3, 4)")).await;

    for i in 1..=5 {
        assert_eq!(
            is_null_at(&sess, "nf", "val", i).await,
            is_null_at(&sess, "ns", "val", i).await,
            "id={i}: fast NULL-ness must match slow path"
        );
    }
    assert_eq!(is_null_at(&sess, "nf", "val", 1).await, Some(true), "NULL+1 stays NULL");
    assert_eq!(scalar_at(&sess, "nf", "val", 2).await, Some(8), "7+1");
    assert_eq!(is_null_at(&sess, "nf", "val", 3).await, Some(true), "NULL+1 stays NULL");
    assert_eq!(scalar_at(&sess, "nf", "val", 4).await, Some(10), "9+1");
    assert_eq!(scalar_at(&sess, "nf", "val", 5).await, Some(11), "id=5 untouched");
    assert_eq!(count_rows(&sess, "nf").await, 5);
}

// ── Tests: decoded-overlay memo invalidation ──────────────────────────────────

/// UPDATE → SELECT (warms the per-table decoded-overlay memo) → UPDATE the
/// SAME key → SELECT must see the new value: the second UPDATE bumps the
/// memtable epoch while `update_count` stays at 1, so this exercises exactly
/// the epoch component of the memo's `(epoch, update_count)` validity key.
/// A third UPDATE on another key then moves `update_count` 1→2.
#[tokio::test]
async fn overlay_memo_epoch_invalidation_update_select_update() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    with_fastpath_on(async {
        exec(&sess, "UPDATE t SET val = 100 WHERE id = 2").await;
        // Bulk SELECT decodes + memoizes the overlay (auto-commit path).
        assert_eq!(
            all_rows(&sess, "t").await,
            vec![(1, 10), (2, 100), (3, 30), (4, 40), (5, 50)],
            "first SELECT after the first override"
        );
        // Same-key overwrite: epoch bumps, update_count stays 1 — a stale
        // epoch-insensitive memo would keep serving 100.
        exec(&sess, "UPDATE t SET val = 200 WHERE id = 2").await;
        assert_eq!(
            all_rows(&sess, "t").await,
            vec![(1, 10), (2, 200), (3, 30), (4, 40), (5, 50)],
            "memo must invalidate on the same-key overwrite (epoch bump)"
        );
        assert_eq!(scalar_at(&sess, "t", "val", 2).await, Some(200));
        // New-key override: update_count 1→2.
        exec(&sess, "UPDATE t SET val = 300 WHERE id = 4").await;
        assert_eq!(
            all_rows(&sess, "t").await,
            vec![(1, 10), (2, 200), (3, 30), (4, 300), (5, 50)],
            "memo must invalidate when a second override is added"
        );
    })
    .await;
    assert_eq!(count_rows(&sess, "t").await, 5);
}

/// A non-allowlisted RMW expression (`SET v = abs(v)` — a function call) is NOT
/// on the fast-path allowlist, so it falls through to the cold copy-on-write
/// path. The result must still be correct.
#[tokio::test]
async fn rmw_unsupported_falls_back_cold() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    exec(&sess, "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)").await;
    exec(&sess, "INSERT INTO u (id, v) VALUES (1, -5), (2, 9)").await;

    // `abs(v)` is a function call → excluded from the allowlist → cold path,
    // even with the fast-path env on. Must still be correct.
    let val = with_fastpath_on(async {
        exec(&sess, "UPDATE u SET v = abs(v) WHERE id = 1").await;
        match sess.execute("SELECT v FROM u WHERE id = 1").await.unwrap() {
            ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
            other => panic!("expected rows, got {other:?}"),
        }
    })
    .await;
    assert_eq!(val, 5, "abs(-5) = 5 via the cold path");
    assert_eq!(count_rows(&sess, "u").await, 2, "no rows added/removed");
}
