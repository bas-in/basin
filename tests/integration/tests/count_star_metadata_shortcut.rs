//! `SELECT COUNT(*) FROM t` metadata-shortcut tests for the hot-tier
//! overlay gate in `basin_engine::fast_aggregate::execute_metadata_aggregate`.
//!
//! Background (commit `3a52efc` / PR2). The metadata-only COUNT(*) fast
//! path answers `SELECT COUNT(*) FROM t` from `catalog.live_data_files()`
//! row-count statistics without touching storage. PR2 (hot-tier UPDATE
//! fast path) added a correctness gate: when the process-wide
//! `MemTableRegistry` holds a tombstone (fast-path DELETE) or an
//! `Update` (fast-path UPDATE) for the table, the catalog row counts no
//! longer reflect the merged hot+cold logical row set — a tombstone
//! removes a counted cold row, an update shadows one.
//!
//! The original gate placement (in `executor.rs`) walked the entire
//! memtable BTreeMap via `snapshot()` on every COUNT(*), then bailed on
//! any non-`Row` variant — including `Update`, which is row-count-neutral.
//! On a freshly bulk-loaded table that path allocated a multi-million-row
//! `Vec` per query and produced a ~1100x regression on the perf bench
//! (92x faster than PG → 12x slower at 1M, commit `ddfd8a8`).
//!
//! The replacement gate in `fast_aggregate.rs` is cheap (O(1) when the
//! memtable is empty; otherwise short-circuits at the first tombstone)
//! and recognises that `Row`/`Update` entries don't change a count.
//! These tests lock that contract:
//!
//! 1. **Fresh cold-only table** → COUNT(*) returns the cold count.
//!    Sub-millisecond timing is asserted as a behavioural proxy for the
//!    metadata shortcut firing (a real scan over even 100k rows is many
//!    ms on debug builds).
//! 2. **After a fast-path DELETE** → COUNT(*) returns
//!    `cold_count − deleted` (the shortcut bails; the DataFusion scan
//!    runs the tombstone-suppression overlay).
//! 3. **After a fast-path UPDATE** → COUNT(*) is unchanged AND fast (the
//!    shortcut still fires because `Update` is row-count-neutral).
//!
//! ## Test isolation
//!
//! `BASIN_HOTTIER_DELETE_FASTPATH` / `BASIN_HOTTIER_UPDATE_FASTPATH` are
//! process-wide env vars. Tests that depend on them run under an
//! `ENV_LOCK` mutex held across the await so a parallel test can't
//! observe a flipped gate. Each test creates a fresh `(TempDir, Engine,
//! Session, ProjectId)` so the `MemTableRegistry` and catalog are
//! isolated per-test.

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Serialises env-var mutation across the parallel test threads in this
/// binary. Tokio mutex so the guard can be held across `.await` points
/// (matches the `update_hottier.rs` / `dml_extras.rs` pattern).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ─── harness ─────────────────────────────────────────────────────────────────

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

/// `SELECT COUNT(*) FROM t` → returns the i64 count from the first cell of
/// the first batch. Panics if the query result shape is not a single i64
/// (which would indicate the metadata-aggregate path returned something
/// schema-incompatible — itself a regression).
async fn count_star(sess: &ProjectSession, table: &str) -> i64 {
    use arrow_array::Int64Array;
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let res = sess
        .execute(&sql)
        .await
        .unwrap_or_else(|e| panic!("COUNT(*) failed: {e:?}"));
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("COUNT(*) expected Rows, got {other:?}"),
    };
    let b = batches.first().expect("COUNT(*) returned no batches");
    let arr = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) first column must be Int64");
    arr.value(0)
}

/// Seed `(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)` with `n` rows. Uses
/// multi-row INSERT batches so the test stays fast (~100k rows still
/// takes seconds on debug builds, but is well under the per-test budget).
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
    )
    .await;
    let batch = 1000_i64;
    let mut i = 1_i64;
    while i <= n {
        let lo = i;
        let hi = (i + batch - 1).min(n);
        let mut stmt = format!("INSERT INTO {table} (id, v) VALUES ");
        let mut first = true;
        for k in lo..=hi {
            if !first {
                stmt.push(',');
            }
            first = false;
            stmt.push_str(&format!("({k}, {})", k * 2));
        }
        exec(sess, &stmt).await;
        i = hi + 1;
    }
}

/// Time `count_star` once. Returned in milliseconds (f64).
async fn timed_count(sess: &ProjectSession, table: &str) -> (i64, f64) {
    let started = Instant::now();
    let n = count_star(sess, table).await;
    (n, started.elapsed().as_secs_f64() * 1000.0)
}

// ─── tests ────────────────────────────────────────────────────────────────────

/// Fresh table, no fast-path DML, no hot-tier overlay. COUNT(*) must
/// return the cold-tier row count via the metadata shortcut — i.e. fast.
///
/// We use a moderate seed size (10 000 rows) so the cold scan path
/// would be measurably slow but the test still finishes quickly. The
/// timing assertion is loose (< 50 ms p95-ish) — the shortcut typically
/// returns in well under 1 ms but debug-mode noise on shared CI hardware
/// is real, so we just need a number low enough to rule out the
/// 100k-row full-scan-and-aggregate path.
#[tokio::test]
async fn count_star_fresh_table_uses_metadata_shortcut() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "ev", 10_000).await;

    // Warm any one-time codegen / catalog lookup so the timing on the
    // measured call reflects the steady-state path, not first-touch
    // costs that are the same on both fast and slow paths.
    let _ = count_star(&sess, "ev").await;

    let (n, ms) = timed_count(&sess, "ev").await;
    assert_eq!(n, 10_000, "COUNT(*) must match seeded row count");
    assert!(
        ms < 50.0,
        "freshly-loaded COUNT(*) took {ms:.2} ms — expected < 50 ms via metadata shortcut; \
         a slow value here means the executor's overlay gate is forcing a full scan \
         even though the memtable holds no tombstones or updates"
    );
}

/// After a fast-path DELETE, COUNT(*) reflects the tombstone-suppressed live
/// count via the metadata shortcut's tombstone adjustment
/// (`COUNT(*) = Σrow_count − tombstone_count`) — NOT a full scan. Asserts both
/// correctness AND that it stays fast: this is the perf win that turns a
/// post-DELETE COUNT(*) from a full-table scan back into O(files).
#[tokio::test]
async fn count_star_after_fast_path_delete_adjusts_via_metadata_shortcut() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "ev", 10_000).await;
    let _ = count_star(&sess, "ev").await; // warm first-touch costs

    let res = sess
        .execute("DELETE FROM ev WHERE id IN (1, 2, 3)")
        .await
        .expect("fast-path DELETE must succeed");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 3"),
        other => panic!("expected Empty from DELETE, got {other:?}"),
    }

    let (n, ms) = timed_count(&sess, "ev").await;
    assert_eq!(
        n, 9_997,
        "COUNT(*) must subtract the 3 tombstones (Σrow_count − tombstone_count)"
    );
    assert!(
        ms < 50.0,
        "post-DELETE COUNT(*) took {ms:.2} ms — expected < 50 ms via the \
         tombstone-adjusted metadata shortcut, not a full scan"
    );

    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
}

/// The benchmark's exact shape: a fast-path UPDATE *and* a fast-path DELETE in
/// the same session, then COUNT(*). Both overlays present → the shortcut must
/// still fire (UPDATE is count-neutral; the DELETE's tombstones are subtracted)
/// and stay fast. Previously the executor gate bailed on *any* tombstone OR
/// update, forcing a full scan (the ~30× loss this fixes).
#[tokio::test]
async fn count_star_after_update_and_delete_uses_shortcut() {
    let _g = ENV_LOCK.lock().await;
    let pd = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    let pu = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "ev", 10_000).await;
    let _ = count_star(&sess, "ev").await;

    exec(&sess, "UPDATE ev SET v = 7 WHERE id = 42").await;
    let res = sess
        .execute("DELETE FROM ev WHERE id IN (1, 2, 3, 4, 5)")
        .await
        .unwrap();
    assert!(matches!(res, ExecResult::Empty { .. }));

    let (n, ms) = timed_count(&sess, "ev").await;
    assert_eq!(
        n, 9_995,
        "COUNT(*) = 10000 − 5 deleted (update is count-neutral)"
    );
    assert!(
        ms < 50.0,
        "COUNT(*) under a combined update+tombstone overlay took {ms:.2} ms — \
         expected fast via the metadata shortcut (the gate must not bail on \
         update/tombstone presence)"
    );

    match pd {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
    match pu {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
}

/// A value aggregate (MAX) with a live tombstone must NOT use the (counter-only)
/// shortcut — it can't be derived from counts — so it falls back to the scan and
/// still returns the correct tombstone-suppressed value.
#[tokio::test]
async fn max_with_tombstone_declines_shortcut_and_is_correct() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "ev", 100).await; // seed sets id 1..=100, v = id

    // Delete the top ids so MAX(id) must drop if tombstones are honored.
    sess.execute("DELETE FROM ev WHERE id IN (99, 100)")
        .await
        .unwrap();

    let res = sess.execute("SELECT MAX(id) FROM ev").await.unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            use arrow_array::Int64Array;
            let v = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(
                v, 98,
                "MAX(id) must reflect the deleted top rows (scan, not stale metadata)"
            );
        }
        other => panic!("expected Rows, got {other:?}"),
    }

    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
}

/// After a fast-path UPDATE of 1 row, COUNT(*) is unchanged (UPDATE is
/// row-count-neutral). The cheap gate is allowed to still fire the
/// metadata shortcut in this case, because `MemRowValue::Update` is a
/// same-PK replacement — the cold row that backs it is already counted
/// exactly once in `live_data_files()` and the override does not add or
/// remove any rows.
#[tokio::test]
async fn count_star_after_fast_path_update_unchanged_and_fast() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "ev", 10_000).await;

    // Warm one COUNT to amortise first-touch costs.
    let _ = count_star(&sess, "ev").await;

    // Fast-path UPDATE — single PK match, scalar RHS, no expressions.
    exec(&sess, "UPDATE ev SET v = 99999 WHERE id = 42").await;

    // Sanity: the row really did update (point lookup goes through the
    // overlay; if this is wrong the test isn't actually exercising the
    // update gate path).
    {
        let res = sess
            .execute("SELECT v FROM ev WHERE id = 42")
            .await
            .unwrap();
        if let ExecResult::Rows { batches, .. } = res {
            use arrow_array::Int64Array;
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v must be Int64");
            assert_eq!(arr.value(0), 99999, "fast-path UPDATE did not apply");
        } else {
            panic!("expected Rows from SELECT after UPDATE");
        }
    }

    let (n, ms) = timed_count(&sess, "ev").await;
    assert_eq!(
        n, 10_000,
        "UPDATE must not change COUNT(*); a different value means the \
         scan path double-counted or under-counted the overridden row"
    );
    assert!(
        ms < 100.0,
        "COUNT(*) after UPDATE took {ms:.2} ms — expected < 100 ms via metadata \
         shortcut (UPDATE is row-count-neutral so the gate should allow it). \
         A slow value here means the gate over-bails on Update entries"
    );

    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
}
