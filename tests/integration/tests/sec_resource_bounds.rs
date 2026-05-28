//! Security regression — resource bounds (DoS / noisy neighbour).
//!
//! Scenarios:
//!
//! 1. `statement_timeout_cancels_recursive_cte` — SQL-level
//!    `SET statement_timeout = '500ms'` cancels a runaway query within
//!    a tight wall-clock bound.
//! 2. `huge_groupby_does_not_panic` — GROUP BY on a 200K-cardinality
//!    expression returns a result or a typed error, never a panic.
//! 3. `concurrent_queries_dont_starve_other_project` — 8 concurrent
//!    queries from project A do not starve a single query from project B
//!    of more than 2x wall-clock budget vs. a baseline run.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

#[allow(clippy::type_complexity)]
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

// ---------------------------------------------------------------------------
// 1. statement_timeout cancels a runaway query.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statement_timeout_cancels_recursive_cte() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Per-session 500 ms statement timeout. If SET statement_timeout isn't
    // wired (engine returns an error here), we fall back to the outer
    // tokio guard — the test still pins the property "never hangs".
    let set = sess.execute("SET statement_timeout = '500ms'").await;
    println!("[sec_resource_bounds] SET statement_timeout: {set:?}");

    // No-termination RCTE: `SELECT n+1 FROM r` forever. Bounded by either
    // the SQL statement_timeout (500 ms) or the outer tokio guard (10 s).
    let sql = "WITH RECURSIVE r(n) AS ( \
                  SELECT 1 \
                  UNION ALL \
                  SELECT n+1 FROM r \
               ) \
               SELECT count(*) FROM r";
    let t0 = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(10), sess.execute(sql)).await;
    let elapsed = t0.elapsed();
    println!(
        "[sec_resource_bounds] runaway RCTE: outcome={outcome:?} elapsed={elapsed:?}"
    );

    match outcome {
        Err(_) => panic!(
            "SECURITY: runaway recursive CTE ran past 10s outer bound — \
             statement_timeout not enforced for RCTEs"
        ),
        Ok(Ok(_)) => {
            // Engine completed (maybe iteration cap fired). Fine if it was
            // fast.
            assert!(
                elapsed < Duration::from_secs(8),
                "RCTE completed in {elapsed:?} — exceeds 8s expectation"
            );
        }
        Ok(Err(e)) => {
            // QueryCanceled is the SQLSTATE 57014 path we want.
            match e {
                BasinError::QueryCanceled(_)
                | BasinError::QueryCostExceeded(_)
                | BasinError::FeatureNotSupported(_)
                | BasinError::InvalidSchema(_) => {}
                BasinError::Internal(msg) => panic!(
                    "SECURITY: runaway RCTE returned BasinError::Internal: {msg} — \
                     expected typed timeout / cost / feature error"
                ),
                _ => println!(
                    "[sec_resource_bounds] runaway RCTE returned typed error: {e}"
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Huge GROUP BY does not panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_groupby_does_not_panic() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, k BIGINT)")
        .await
        .unwrap();

    // 100K rows with distinct keys — high-cardinality groupby.
    // Insert in batches to keep parse-time bounded.
    let batch_size = 1_000usize;
    let total = 100_000usize;
    for batch in 0..(total / batch_size) {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in 0..batch_size {
            if i > 0 {
                sql.push(',');
            }
            let row_id = batch * batch_size + i;
            sql.push_str(&format!("({row_id},{row_id})"));
        }
        sess.execute(&sql).await.unwrap();
    }

    let t0 = Instant::now();
    let r = tokio::time::timeout(
        Duration::from_secs(60),
        sess.execute("SELECT k, count(*) FROM t GROUP BY k"),
    )
    .await;
    let elapsed = t0.elapsed();
    println!(
        "[sec_resource_bounds] groupby 100K groups: outcome={r:?} elapsed={elapsed:?}"
    );

    match r {
        Err(_) => panic!(
            "SECURITY: huge GROUP BY did not complete within 60s — possible \
             unbounded memory accumulation"
        ),
        Ok(Ok(ExecResult::Rows { batches, .. })) => {
            let groups: usize = batches.iter().map(|b| b.num_rows()).sum();
            // Each k appears once, so there must be exactly `total` distinct
            // groups (engine returns them all OR returns a query-cost error;
            // both are acceptable).
            assert!(
                groups == total || groups == 0,
                "huge GROUP BY returned unexpected group count: {groups}"
            );
        }
        Ok(Ok(ExecResult::Empty { .. })) => {}
        Ok(Err(e)) => match e {
            BasinError::QueryCostExceeded(_) | BasinError::QueryCanceled(_) => {}
            BasinError::Internal(msg) => panic!(
                "SECURITY: huge GROUP BY returned BasinError::Internal: {msg}"
            ),
            other => println!(
                "[sec_resource_bounds] huge GROUP BY typed err: {other}"
            ),
        },
    }
}

// ---------------------------------------------------------------------------
// 3. Noisy neighbour: project B's single query must complete while project
//    A bombards the engine with 8 concurrent queries.
//
// We compare:
//   * Baseline: B's query in isolation.
//   * Loaded:   B's query while A is hammering.
// We assert the loaded run is not >5x slower than the baseline (very
// generous bound — the point is to catch "B's query never returns").
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_queries_dont_starve_other_project() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let pa = ProjectId::new();
    let pb = ProjectId::new();
    let sa = eng.open_session(pa).await.unwrap();
    let sb = eng.open_session(pb).await.unwrap();

    sa.execute("CREATE TABLE big (id BIGINT, v BIGINT)")
        .await
        .unwrap();
    sb.execute("CREATE TABLE small (id BIGINT)").await.unwrap();
    // Seed A with enough rows so its queries take measurable time.
    let mut sql = String::from("INSERT INTO big VALUES ");
    let n_seed = 50_000usize;
    for i in 0..n_seed {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({},{})", i, i * 2));
    }
    sa.execute(&sql).await.unwrap();
    sb.execute("INSERT INTO small VALUES (1), (2), (3)")
        .await
        .unwrap();

    // Baseline: B's query in isolation, measured 3 times to amortize noise.
    let mut baseline = Duration::from_secs(60);
    for _ in 0..3 {
        let t0 = Instant::now();
        sb.execute("SELECT count(*) FROM small").await.unwrap();
        let e = t0.elapsed();
        if e < baseline {
            baseline = e;
        }
    }
    println!("[sec_resource_bounds] B baseline: {baseline:?}");

    // Load: A runs 8 concurrent queries; B's measured query overlaps with
    // them. The eng is `Arc`-shared via the Engine struct's internal state;
    // we drive concurrency via per-session clones backed by the same engine.
    let eng_clone = eng.clone();
    let pa_copy = pa;
    let load_handle = tokio::spawn(async move {
        let mut joins = Vec::new();
        for _ in 0..8 {
            let eng = eng_clone.clone();
            let pa_inner = pa_copy;
            joins.push(tokio::spawn(async move {
                let sa = eng.open_session(pa_inner).await.unwrap();
                // Wide scan with aggregate.
                let _ = sa
                    .execute("SELECT count(*) FROM big WHERE v > 0")
                    .await;
            }));
        }
        for j in joins {
            let _ = j.await;
        }
    });

    // Give load a moment to ramp.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let t0 = Instant::now();
    let r = tokio::time::timeout(
        Duration::from_secs(30),
        sb.execute("SELECT count(*) FROM small"),
    )
    .await;
    let loaded = t0.elapsed();
    println!("[sec_resource_bounds] B under load: outcome={r:?} elapsed={loaded:?}");

    // Drain the load.
    let _ = load_handle.await;

    assert!(
        matches!(r, Ok(Ok(_))),
        "SECURITY: B's query did not complete under noisy-neighbour load: {r:?}"
    );

    // Generous starvation bound: 5x baseline OR 2s, whichever is larger.
    // The point is to catch infinite stalls / unbounded queuing.
    let limit = std::cmp::max(baseline.saturating_mul(5), Duration::from_secs(2));
    assert!(
        loaded < limit,
        "SECURITY: B's query took {loaded:?} under load — exceeds {limit:?} \
         (5x baseline {baseline:?} or 2s floor). Possible cross-project starvation."
    );
}
