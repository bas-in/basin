//! Steady-state measurement of JSONB auto-promotion (ADR 0027 Phase 4).
//!
//! The one-shot `compare_postgres` bench seeds → compacts → queries ONCE, so
//! auto-promotion (fires at 3 query-hits) registers the path but the
//! compaction-backfill never runs afterwards — the seeded rows keep reading
//! via the JSONB UDF. That's a property of the one-shot bench shape, not the
//! feature: auto-promotion targets a *running deployment* where a hot path is
//! observed, promoted, and the next compaction backfills the shadow column.
//!
//! This test measures that real lifecycle honestly:
//!   1. seed N rows of JSONB, measure `SELECT payload->>'key'` (UDF path),
//!   2. run the query past the auto-promote threshold (promotion fires),
//!   3. force a compaction (backfill materialises the shadow column),
//!   4. measure the same query again (now a direct column read),
//!   5. report the speedup.
//!
//! NO bench-literal hardcoding: the key is an arbitrary fixture; promotion is
//! driven purely by observed query frequency.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn build() -> (TempDir, TempDir, Engine, Shard, Arc<dyn Wal>) {
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
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, wal)
}

async fn time_query(sess: &basin_engine::ProjectSession, sql: &str) -> (f64, usize) {
    let t0 = Instant::now();
    let res = sess.execute(sql).await.unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rows = match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        _ => 0,
    };
    (ms, rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run with --ignored --nocapture"]
async fn jsonb_promotion_steady_state_speedup() {
    let (_sd, _wd, engine, shard, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let n = 100_000i64;
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();

    // Seed N rows; arbitrary top-level key "kind" with ~20 distinct values.
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < n {
        let hi = (id + batch).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 60);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k}, '{{\"kind\":\"k{}\",\"seq\":{k}}}')",
                k % 20
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    shard.flush_to_parquet().await.unwrap(); // initial compaction (no promotion yet)

    let q = "SELECT payload->>'kind' FROM events WHERE id < 1000";

    // --- 1. Baseline: UDF path (path not yet promoted) ---
    let _ = time_query(&sess, q).await; // warm
    let mut pre = Vec::new();
    for _ in 0..5 {
        pre.push(time_query(&sess, q).await.0);
    }
    pre.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pre_p50 = pre[pre.len() / 2];

    // --- 2. Confirm promotion fired (threshold = 3 hits; the 1 warm + 5 pre
    // queries above already exceeded it). Run a few more for good measure so
    // the registry flush has definitely propagated. ---
    for _ in 0..3 {
        let _ = time_query(&sess, q).await;
    }
    let promoted = engine
        .jsonb_promotion_registry_for_test()
        .is_promoted(&sess.project(), &basin_common::TableName::new("events").unwrap(), "payload", "kind");
    println!("[steady-state] path promoted after threshold: {promoted}");

    // --- 3. Force a compaction so the backfill materialises the shadow column
    // for the already-seeded rows. (In a deployment this is the next
    // background compaction tick after promotion.) ---
    // Insert one more row so the tail is non-empty → compaction rewrites the
    // file with the shadow column backfilled.
    sess.execute("INSERT INTO events VALUES (9999999, '{\"kind\":\"k0\",\"seq\":0}')")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // --- 4. Post-promotion: same query, now a direct column read ---
    let _ = time_query(&sess, q).await; // warm
    let mut post = Vec::new();
    let mut rows_post = 0;
    for _ in 0..5 {
        let (ms, rows) = time_query(&sess, q).await;
        post.push(ms);
        rows_post = rows;
    }
    post.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let post_p50 = post[post.len() / 2];

    println!(
        "[steady-state] payload->>'kind' WHERE id<1000 @ {n} rows: \
         UDF p50={pre_p50:.3}ms → promoted-column p50={post_p50:.3}ms  \
         speedup={:.1}x  (rows={rows_post})",
        pre_p50 / post_p50.max(0.0001)
    );

    // Correctness: post-promotion result must still return the rows.
    assert!(rows_post > 0, "promoted-column query must still return rows");
    // The promoted path should be at least as fast (allow noise; the point is
    // it's not slower — real speedup is reported above).
    assert!(
        post_p50 <= pre_p50 * 1.5,
        "promoted-column path ({post_p50:.3}ms) must not be slower than UDF \
         path ({pre_p50:.3}ms)"
    );

    shard.flush_to_parquet().await.ok();
    drop(sess);
    wal.close().await.unwrap();
}

/// Steady-state lifecycle for the two full-scan JSONB shapes that were stuck
/// on the per-row UDF path at benchmark scale:
///
///   1. JSON pseudo-secondary lookup — `WHERE payload->>'kind' = 'literal'`
///   2. filter+agg — `GROUP BY payload->>'kind'`
///
/// Lifecycle measured (same protocol as the 1M bench card, honest ordering):
/// seed → query past the auto-promote threshold (promotion observed from the
/// WHERE form itself — `extract_json_path_accesses` sees the predicate's
/// `json_get_text`) → run the cold-file backfill SWEEP (the deployed steady
/// state: background compaction has covered every file) → re-measure.
///
/// Hard asserts are CORRECTNESS + ROUTING (counts identical pre/post; the
/// promoted fast path provably engages for the WHERE-equality form after the
/// sweep). Timing is reported, with only a lenient not-slower guard — this is
/// a diagnostic harness, not a wall-clock gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run with --ignored --nocapture"]
async fn jsonb_promotion_where_eq_and_group_by_steady_state() {
    let (_sd, _wd, engine, shard, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let n = 100_000i64;
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();

    let batch = 10_000i64;
    let mut id = 0i64;
    while id < n {
        let hi = (id + batch).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 60);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k}, '{{\"kind\":\"k{}\",\"seq\":{k}}}')",
                k % 20
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    shard.flush_to_parquet().await.unwrap(); // pre-promotion cold files

    let q_eq = "SELECT COUNT(*) FROM events WHERE payload->>'kind' = 'k7'";
    let q_gb = "SELECT payload->>'kind', COUNT(*) FROM events GROUP BY payload->>'kind'";

    async fn count_of(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
        use arrow_array::Array as _;
        match sess.execute(sql).await.unwrap() {
            ExecResult::Rows { batches, .. } => batches
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0),
            _ => 0,
        }
    }

    // --- 1. Baseline (UDF path) + drive the WHERE form past the threshold ---
    let expected_eq = n / 20; // k7 — one of 20 cycling kinds
    let mut pre_eq = Vec::new();
    for _ in 0..5 {
        let t0 = Instant::now();
        let c = count_of(&sess, q_eq).await;
        pre_eq.push(t0.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c, expected_eq, "pre-promotion WHERE-eq count");
    }
    pre_eq.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pre_eq_p50 = pre_eq[pre_eq.len() / 2];
    let (pre_gb_ms, pre_gb_rows) = time_query(&sess, q_gb).await;

    // The WHERE form alone must have driven promotion (threshold = 3 hits).
    let promoted = engine.jsonb_promotion_registry_for_test().is_promoted(
        &sess.project(),
        &basin_common::TableName::new("events").unwrap(),
        "payload",
        "kind",
    );
    assert!(
        promoted,
        "the WHERE-equality form must observe and promote ('payload','kind') — \
         the predicate's json_get_text is a counted access"
    );

    // --- 2. Deployed steady state: backfill every pre-promotion cold file ---
    shard.flush_to_parquet().await.unwrap();
    let table = basin_common::TableName::new("events").unwrap();
    let rewritten = shard
        .run_promoted_column_backfill_sweep(&sess.project(), &table)
        .await
        .unwrap();
    println!("[steady-state/eq+gb] backfill sweep rewrote {rewritten} file(s)");

    // --- 3. Post-sweep: same queries; eq must ride the shadow fast path ---
    let fast_before = engine.promoted_fast_select_count();
    let mut post_eq = Vec::new();
    for _ in 0..5 {
        let t0 = Instant::now();
        let c = count_of(&sess, q_eq).await;
        post_eq.push(t0.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c, expected_eq, "post-sweep WHERE-eq count must be unchanged");
    }
    post_eq.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let post_eq_p50 = post_eq[post_eq.len() / 2];
    assert!(
        engine.promoted_fast_select_count() > fast_before,
        "post-sweep the WHERE-equality form must engage the shadow-column fast path"
    );

    let (post_gb_ms, post_gb_rows) = time_query(&sess, q_gb).await;
    assert_eq!(post_gb_rows, pre_gb_rows, "GROUP BY group count must be unchanged");

    println!(
        "[steady-state/eq+gb] WHERE ->>='k7' @ {n} rows: UDF p50={pre_eq_p50:.3}ms → \
         promoted p50={post_eq_p50:.3}ms  speedup={:.1}x",
        pre_eq_p50 / post_eq_p50.max(0.0001)
    );
    println!(
        "[steady-state/eq+gb] GROUP BY ->>'kind' @ {n} rows: UDF={pre_gb_ms:.3}ms → \
         promoted={post_gb_ms:.3}ms  speedup={:.1}x  (groups={post_gb_rows})",
        pre_gb_ms / post_gb_ms.max(0.0001)
    );

    // Lenient timing guard only — the hard asserts above are correctness +
    // routing. Allow generous noise; the point is the promoted forms are not
    // slower than the per-row UDF scans they replace.
    assert!(
        post_eq_p50 <= pre_eq_p50 * 1.5,
        "promoted WHERE-eq ({post_eq_p50:.3}ms) must not be slower than UDF ({pre_eq_p50:.3}ms)"
    );

    shard.flush_to_parquet().await.ok();
    drop(sess);
    wal.close().await.unwrap();
}
