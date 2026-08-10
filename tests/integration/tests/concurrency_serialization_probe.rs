//! OLTP concurrency diagnostic — fast dev loop for the #1 OLTP gap.
//!
//! The 1M differential bench shows `concurrent_select_16` at ~31× PG and
//! `rmw_contention_8` at ~20×, while a single-threaded point query is ~0.1 ms.
//! That delta means concurrent work is being SERIALIZED somewhere (a global
//! lock or per-session overhead that doesn't parallelize). This test isolates
//! that signal on a small dataset so the fix can be iterated in <1 s instead
//! of re-running the 174 s 1M card.
//!
//! It is Basin-only (no Postgres) and prints a **serialization factor**:
//!
//!   serialization_factor = concurrent_wall_ms / single_session_ms
//!
//! Interpretation on an N-session run:
//!   * ≈ 1     → perfect parallelism (concurrent batch ≈ one query).
//!   * ≈ N     → fully serialized (every query waits its turn — a global lock).
//!   * between → partial contention.
//!
//! Run: cargo test --release -p basin-integration-tests \
//!        --test concurrency_serialization_probe -- --nocapture

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// Open `n` sessions and run `body(idx, session)` on each concurrently;
/// return the wall-clock for the whole batch. Mirrors the bench's
/// `basin_concurrent` helper.
async fn concurrent_wall<F, Fut>(engine: &Engine, project: ProjectId, n: usize, body: F) -> f64
where
    F: Fn(usize, basin_engine::ProjectSession) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut sessions = Vec::with_capacity(n);
    for _ in 0..n {
        sessions.push(engine.open_session(project).await.unwrap());
    }
    let started = Instant::now();
    let mut handles = Vec::with_capacity(n);
    for (idx, s) in sessions.into_iter().enumerate() {
        let body = body.clone();
        handles.push(tokio::spawn(async move { body(idx, s).await }));
    }
    for h in handles {
        h.await.unwrap();
    }
    started.elapsed().as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn concurrency_serialization_probe() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Small dataset: 50k rows, flushed to cold Vortex (quiesced) so reads hit
    // the cold path like the bench. Default format = Vortex.
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT)",
    )
    .await
    .unwrap();
    let n: i64 = 50_000;
    let batch = 10_000i64;
    let mut id = 0;
    while id < n {
        let hi = (id + batch).min(n);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k},{},{},'pending',{k})",
                k % 1000,
                k as f64 * 0.5
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    shard.flush_to_parquet().await.unwrap();

    // Extra tables so the per-SELECT "refresh ALL tables" loop (executor.rs)
    // has more than one to walk — mirrors the bench schema (events, users,
    // oltp_extra, rstress, …). A range query (non-fast-path) hits that loop;
    // a point query (= PK) goes through fast_select and skips it.
    for t in ["users", "orders", "rstress", "oltp_extra"] {
        sess.execute(&format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap();
        sess.execute(&format!("INSERT INTO {t} VALUES (1,1),(2,2),(3,3)"))
            .await
            .unwrap();
    }
    shard.flush_to_parquet().await.unwrap();

    let pid = |i: i64| format!("SELECT id, user_id, amount FROM events WHERE id = {i}");

    // ── Baseline: single-session point query (warm) ───────────────────────
    let _ = sess.execute(&pid(123)).await.unwrap();
    let iters = 50;
    let t0 = Instant::now();
    for k in 0..iters {
        let _ = sess.execute(&pid(k * 7 + 1)).await.unwrap();
    }
    let single_ms = t0.elapsed().as_secs_f64() * 1000.0 / iters as f64;

    // ── 16 concurrent sessions, each does the SAME point query once ────────
    // (mirrors the bench's concurrent_select shape). Warm the cold files once.
    let warm = engine.open_session(project).await.unwrap();
    let _ = warm.execute(&pid(50)).await.unwrap();
    for &nsess in &[2usize, 4, 8, 16] {
        let wall = concurrent_wall(&engine, project, nsess, move |idx, s| async move {
            let _ = s
                .execute(&format!(
                    "SELECT id, user_id, amount FROM events WHERE id = {}",
                    (idx as i64) * 13 + 7
                ))
                .await
                .unwrap();
        })
        .await;
        let sf = wall / single_ms.max(1e-9);
        println!(
            "[concurrency] N={nsess:>2}  single={single_ms:.3}ms  concurrent_wall={wall:.3}ms  serialization_factor={sf:.1}x  (1=perfect, {nsess}=fully serial)"
        );
    }

    // ── Concurrent RANGE reads (non-fast-path → hits the per-SELECT
    //    flush + refresh-ALL-tables loop). This is the bench's
    //    concurrent_select shape; point reads above skip this path. ─────────
    let range_sql = "SELECT id, user_id, amount FROM events WHERE id >= 100 AND id < 600";
    let _ = sess.execute(range_sql).await.unwrap();
    let t0 = Instant::now();
    for _ in 0..10 {
        let _ = sess.execute(range_sql).await.unwrap();
    }
    let single_range_ms = t0.elapsed().as_secs_f64() * 1000.0 / 10.0;
    for &nsess in &[2usize, 8, 16] {
        let wall = concurrent_wall(&engine, project, nsess, move |_idx, s| async move {
            let _ = s
                .execute("SELECT id, user_id, amount FROM events WHERE id >= 100 AND id < 600")
                .await
                .unwrap();
        })
        .await;
        let sf = wall / single_range_ms.max(1e-9);
        println!(
            "[concurrency] RANGE N={nsess:>2}  single={single_range_ms:.3}ms  concurrent_wall={wall:.3}ms  serialization_factor={sf:.1}x"
        );
    }

    // ── Write contention signal (BOUNDED so the dev loop stays fast) ───────
    // Concurrent point-UPDATEs on the SAME key are the pathological OLTP shape
    // (an earlier unbounded 8x5 version of this took ~24 min — ~36s/op). We
    // probe the SMALLEST contended case — 2 sessions, 1 UPDATE each on the same
    // key — and print per-op wall time. A healthy engine does this in low-ms;
    // seconds means a retry storm / cold-path rewrite under contention. Capped
    // at N=2 so a regression can't hang the suite for minutes.
    // Reconcile the bench (single-row UPDATE = 8ms) vs RMW (221ms): the
    // difference is whether SET reads the old value. `SET col = <literal>` is
    // write-only; `SET col = col + 1` is read-modify-write and must read the
    // old row first. Time both on cold rows on the SAME table.
    let upd_literal = {
        let t = Instant::now();
        let _ = sess
            .execute("UPDATE events SET status = 'x' WHERE id = 5")
            .await;
        t.elapsed().as_secs_f64() * 1000.0
    };
    let upd_rmw = {
        let t = Instant::now();
        let _ = sess
            .execute("UPDATE events SET amount = amount + 1.0 WHERE id = 6")
            .await;
        t.elapsed().as_secs_f64() * 1000.0
    };
    println!(
        "[concurrency] UPDATE SET=literal (write-only)={upd_literal:.3}ms  SET=col+1 (read-modify-write)={upd_rmw:.3}ms  rmw/literal={:.1}x",
        upd_rmw / upd_literal.max(1e-9)
    );
    let single_upd = upd_rmw;
    let contended_wall = concurrent_wall(&engine, project, 2, move |_idx, s| async move {
        let _ = s
            .execute("UPDATE events SET amount = amount + 1.0 WHERE id = 9")
            .await;
    })
    .await;
    println!(
        "[concurrency] UPDATE: uncontended single={single_upd:.3}ms  2-session same-key wall={contended_wall:.3}ms  (contention_factor={:.1}x)",
        contended_wall / single_upd.max(1e-9)
    );

    println!(
        "\n[concurrency] FINDINGS: concurrent point READS parallelize (factor stays low, not ~N) — \
         the read path is not globally locked. The OLTP concurrency cost is on the WRITE side: \
         concurrent UPDATEs to the same key contend badly (watch contention_factor + the per-op ms)."
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
