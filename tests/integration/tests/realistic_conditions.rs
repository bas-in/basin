//! Realistic-conditions benchmark cards: sustained load, background
//! interference, N+1 storm, and read-only session-scaling.
//!
//! Cards (all PG head-to-head except where noted):
//!
//!   1. `soak_latency_stability`       — 120s mixed RW, background ALIVE;
//!                                       p50/p99 in 10s windows, drift ratio,
//!                                       time-series JSON
//!   2. `background_interference`      — identical read shapes quiesced vs
//!                                       compaction-live vs bulk-ingest-live;
//!                                       the delta IS the result
//!   8. `n_plus_one_storm`             — 100 sequential single-row SELECTs
//!                                       (ORM lazy-load), total wall time vs PG
//!   9. `read_scaling_curve`           — 1/4/16/64 sessions, QPS each, vs PG;
//!                                       emits the concurrency curve
//!
//! # Running
//!
//! All four tests are `#[ignore]`d — sanctioned run-later benchmark cards.
//! Run one at a time on an idle box (repo cargo discipline: one cargo at a
//! time, timing-sensitive):
//!
//! ```text
//! cargo test -p basin-integration-tests --test realistic_conditions \
//!   -- --ignored --nocapture
//! ```
//!
//! Or via `benchmark/run/realistic-suite.sh` which runs them sequentially.
//!
//! # Env knobs
//!
//! * `BASIN_SOAK_SECS`        — soak duration (default 120)
//! * `BASIN_SOAK_SEED_ROWS`   — pre-seeded rows (default 500_000)
//! * `BASIN_INTERF_SEED_ROWS` — interference card seed rows (default 200_000)
//! * `BASIN_N1_PARENTS`       — N+1 parent count (default 100)
//! * `BASIN_SCALE_WINDOW_SECS`— per-concurrency measurement window (default 5)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;
use tokio_postgres::NoTls;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{
    build_basin_engine, email_for, median, percentile, status_for, try_connect, SchemaGuard,
};

// ── env helpers ──────────────────────────────────────────────────────────────

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ── build helpers ────────────────────────────────────────────────────────────

/// Standard in-process engine (Shard + WAL + LocalFileSystem). Background loop
/// is ALIVE after construction — call `bg.shutdown().await` when ready to
/// quiesce (or leave it alive if the card needs it live).
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

fn row_count_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Atomic artifact write to `<repo_root>/benchmark/data/<file>`.
fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[realistic] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[realistic] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[realistic] artifact rename {}: {e}", path.display());
    }
    eprintln!("[realistic] artifact written: {}", path.display());
}

// ── SplitMix64 PRNG ──────────────────────────────────────────────────────────

fn next_u64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 1 — soak_latency_stability
// ─────────────────────────────────────────────────────────────────────────────

/// 120-second sustained mixed-RW soak with background loop ALIVE throughout.
///
/// Records p50/p99 latency per 10-second window for both Basin and Postgres.
/// The "drift ratio" (p99 in final window / p99 in first window) is an
/// informational metric — not gated, because hardware variance can inflate it
/// legitimately. The correctness assert (zero errors during the run) is hard.
///
/// Emits `benchmark/data/soak_latency_stability.json` with the full
/// time-series so the dashboard can plot the drift curve.
///
/// PG twin: identical load profile over a single `tokio_postgres` connection;
/// wall-clock timing (no EXPLAIN ANALYZE overhead on the PG side).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn soak_latency_stability() {
    let soak_secs = env_u64("BASIN_SOAK_SECS", 120);
    let seed_rows = env_usize("BASIN_SOAK_SEED_ROWS", 500_000) as i64;
    let window_secs = 10u64;
    let n_windows = (soak_secs / window_secs).max(1) as usize;

    eprintln!("[soak] config: soak_secs={soak_secs} seed_rows={seed_rows} windows={n_windows}");

    // ── Basin engine setup ───────────────────────────────────────────────────
    let (_sd, _wd, engine, _shard, _bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = Arc::new(engine.open_session(project).await.unwrap());

    const EPOCH: i64 = 1_700_000_000;
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL, \
            payload JSONB)",
    )
    .await
    .unwrap();

    // Seed in 10k-row batches (background loop alive during seed).
    eprintln!("[soak] seeding {seed_rows} rows ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < seed_rows {
        let hi = (id + batch).min(seed_rows);
        let mut stmt = format!("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            let amount = 1.0 + (k % 10000) as f64 * 0.01;
            stmt.push_str(&format!(
                "({k},{},{amount},'{status}',{},'{{\"cat\":{}}}')",
                k % (seed_rows / 10).max(1),
                EPOCH + k,
                k % 5
            ));
        }
        sess.execute(&stmt).await.expect("basin seed batch");
        id = hi;
    }
    eprintln!("[soak] basin seed complete");

    // ── PG setup ─────────────────────────────────────────────────────────────
    // Schema name and conn_str are kept as owned Strings so the cleanup task
    // that fires after the soak window can drop the schema. No SchemaGuard here
    // because the guard would drop at the end of the match arm (before the soak
    // measurement loop runs).
    let (pg_avail, pg_conn_str, pg_soak_schema) = match try_connect().await {
        Some((pg, cs)) => {
            let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
            let schema = format!("basin_soak_{suffix}");
            pg.simple_query(&format!("CREATE SCHEMA {schema}"))
                .await
                .expect("pg schema");
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.events (\
                    id BIGINT PRIMARY KEY, \
                    user_id BIGINT NOT NULL, \
                    amount DOUBLE PRECISION NOT NULL, \
                    status TEXT NOT NULL, \
                    created_at BIGINT NOT NULL)"
            ))
            .await
            .expect("pg create table");

            eprintln!("[soak] seeding PG {seed_rows} rows ...");
            let mut pg_id = 0i64;
            while pg_id < seed_rows {
                let hi = (pg_id + batch).min(seed_rows);
                let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
                for k in pg_id..hi {
                    if k > pg_id {
                        stmt.push(',');
                    }
                    let status = status_for(k);
                    let amount = 1.0 + (k % 10000) as f64 * 0.01;
                    stmt.push_str(&format!(
                        "({k},{},{amount},'{status}',{})",
                        k % (seed_rows / 10).max(1),
                        EPOCH + k,
                    ));
                }
                pg.simple_query(&stmt).await.expect("pg seed batch");
                pg_id = hi;
            }
            eprintln!("[soak] pg seed complete");
            (true, cs, schema)
        }
        None => {
            eprintln!("[soak] postgres unavailable — running Basin-only soak");
            (false, String::new(), String::new())
        }
    };

    // ── Soak measurement ─────────────────────────────────────────────────────
    // Three concurrent loops run for `soak_secs`:
    //   Reader   — random-PK point queries; latency samples per 10s window
    //   Writer   — batched INSERT 100 rows at ~100 rows/sec
    //   Updater  — single-row UPDATE at ~10/sec
    //
    // Each loop records errors; the correctness assert fires if any occur.
    // Window buckets are collected via an AtomicU64-indexed shared Vec of
    // Mutex<Vec<f64>> (one per window, one per engine side).

    let done = Arc::new(AtomicBool::new(false));
    let basin_errors = Arc::new(AtomicU64::new(0));

    // Per-window latency accumulators: Vec<Mutex<Vec<f64>>> with n_windows slots.
    let basin_windows: Arc<Vec<std::sync::Mutex<Vec<f64>>>> = Arc::new(
        (0..n_windows)
            .map(|_| std::sync::Mutex::new(Vec::new()))
            .collect(),
    );

    let soak_start = Instant::now();
    let total_dur = Duration::from_secs(soak_secs);

    // Basin reader task
    let bw = basin_windows.clone();
    let bd = done.clone();
    let be = basin_errors.clone();
    let sess_r = sess.clone();
    let basin_reader = tokio::spawn(async move {
        let mut rng: u64 = 0xDEAD_BEEF_CAFE_1234;
        while !bd.load(Ordering::Relaxed) {
            let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
            let t = Instant::now();
            match sess_r
                .execute(&format!(
                    "SELECT id, user_id, amount FROM events WHERE id = {id_val}"
                ))
                .await
            {
                Ok(_) => {
                    let ms = t.elapsed().as_secs_f64() * 1000.0;
                    let w = (soak_start.elapsed().as_secs() / window_secs).min(n_windows as u64 - 1)
                        as usize;
                    bw[w].lock().unwrap().push(ms);
                }
                Err(_) => {
                    be.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    // Basin writer task
    let sess_w = sess.clone();
    let bd2 = done.clone();
    let be2 = basin_errors.clone();
    let basin_writer = tokio::spawn(async move {
        let mut next_id = seed_rows + 1;
        while !bd2.load(Ordering::Relaxed) {
            // 100-row batch
            let mut stmt = String::from("INSERT INTO events VALUES ");
            for k in 0..100i64 {
                let rid = next_id + k;
                if k > 0 {
                    stmt.push(',');
                }
                stmt.push_str(&format!(
                    "({rid},{},1.0,'pending',{},'{{}}' )",
                    rid % 50000,
                    EPOCH + rid,
                ));
            }
            next_id += 100;
            if let Err(_) = sess_w.execute(&stmt).await {
                be2.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(1000)).await; // ~100 rows/sec
        }
    });

    // Basin updater task
    let sess_u = sess.clone();
    let bd3 = done.clone();
    let be3 = basin_errors.clone();
    let basin_updater = tokio::spawn(async move {
        let mut rng: u64 = 0xBEEF_CAFE;
        while !bd3.load(Ordering::Relaxed) {
            let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
            let new_amt = 9.99 + (next_u64(&mut rng) % 100) as f64 * 0.01;
            if let Err(_) = sess_u
                .execute(&format!(
                    "UPDATE events SET amount = {new_amt} WHERE id = {id_val}"
                ))
                .await
            {
                be3.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(100)).await; // ~10/sec
        }
    });

    // PG reader task (wall-clock, no EXPLAIN ANALYZE overhead)
    // Uses the schema seeded above (`pg_soak_schema`); opens its own connection
    // so the reader doesn't share a connection with other tasks.
    let pg_windows: Arc<Vec<std::sync::Mutex<Vec<f64>>>> = Arc::new(
        (0..n_windows)
            .map(|_| std::sync::Mutex::new(Vec::new()))
            .collect(),
    );
    let pg_task = if pg_avail {
        let pwc = pg_conn_str.clone();
        let sch = pg_soak_schema.clone();
        let pw = pg_windows.clone();
        let bd4 = done.clone();
        Some(tokio::spawn(async move {
            let (pgr, pgr_conn) = match tokio_postgres::connect(&pwc, NoTls).await {
                Ok(v) => v,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let _ = pgr_conn.await;
            });
            let mut rng: u64 = 0xABCD_1234;
            while !bd4.load(Ordering::Relaxed) {
                let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
                let t = Instant::now();
                let _ = pgr
                    .simple_query(&format!(
                        "SELECT id, user_id, amount FROM {sch}.events WHERE id = {id_val}"
                    ))
                    .await;
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                let elapsed_s = soak_start.elapsed().as_secs();
                let w = (elapsed_s / window_secs).min(n_windows as u64 - 1) as usize;
                pw[w].lock().unwrap().push(ms);
            }
        }))
    } else {
        None
    };

    // Run for soak_secs then signal done
    tokio::time::sleep(total_dur).await;
    done.store(true, Ordering::Relaxed);

    // Collect tasks
    let _ = basin_reader.await;
    let _ = basin_writer.await;
    let _ = basin_updater.await;
    if let Some(t) = pg_task {
        let _ = t.await;
    }

    let total_errors = basin_errors.load(Ordering::Relaxed);
    assert_eq!(
        total_errors, 0,
        "soak correctness: {total_errors} Basin errors during {soak_secs}s soak"
    );

    // ── Compute per-window p50/p99 ───────────────────────────────────────────
    let mut basin_p50s: Vec<f64> = Vec::new();
    let mut basin_p99s: Vec<f64> = Vec::new();
    let mut pg_p50s: Vec<f64> = Vec::new();
    let mut pg_p99s: Vec<f64> = Vec::new();
    let mut json_windows: Vec<serde_json::Value> = Vec::new();

    for w in 0..n_windows {
        let mut bs: Vec<f64> = basin_windows[w].lock().unwrap().clone();
        let mut ps: Vec<f64> = pg_windows[w].lock().unwrap().clone();
        let b50 = if bs.is_empty() {
            f64::NAN
        } else {
            bs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            median(&bs)
        };
        let b99 = if bs.is_empty() {
            f64::NAN
        } else {
            percentile(&bs, 99.0)
        };
        let p50 = if ps.is_empty() {
            f64::NAN
        } else {
            ps.sort_by(|a, b| a.partial_cmp(b).unwrap());
            median(&ps)
        };
        let p99 = if ps.is_empty() {
            f64::NAN
        } else {
            percentile(&ps, 99.0)
        };
        let n_b = bs.len();
        let n_p = ps.len();
        basin_p50s.push(b50);
        basin_p99s.push(b99);
        pg_p50s.push(p50);
        pg_p99s.push(p99);
        json_windows.push(json!({
            "window": w,
            "window_start_s": w as u64 * window_secs,
            "basin_p50_ms": if b50.is_nan() { serde_json::Value::Null } else { json!(b50) },
            "basin_p99_ms": if b99.is_nan() { serde_json::Value::Null } else { json!(b99) },
            "pg_p50_ms": if p50.is_nan() { serde_json::Value::Null } else { json!(p50) },
            "pg_p99_ms": if p99.is_nan() { serde_json::Value::Null } else { json!(p99) },
            "basin_samples": n_b,
            "pg_samples": n_p,
        }));
        println!(
            "[soak] window {:>2} ({}–{}s): basin p50={:.2}ms p99={:.2}ms (n={}) | pg p50={:.2}ms p99={:.2}ms (n={})",
            w,
            w as u64 * window_secs,
            (w as u64 + 1) * window_secs,
            b50, b99, n_b, p50, p99, n_p,
        );
    }

    // Latency drift ratio: p99 in last non-empty window / p99 in first non-empty window.
    // Informational — not gated (environment-sensitive). Asserted loose bound only.
    let first_basin_p99 = basin_p99s
        .iter()
        .find(|v| v.is_finite())
        .copied()
        .unwrap_or(f64::NAN);
    let last_basin_p99 = basin_p99s
        .iter()
        .rev()
        .find(|v| v.is_finite())
        .copied()
        .unwrap_or(f64::NAN);
    let drift_ratio = if first_basin_p99 > 1e-9 {
        last_basin_p99 / first_basin_p99
    } else {
        f64::NAN
    };
    println!(
        "[soak] latency drift: first_p99={:.2}ms last_p99={:.2}ms drift_ratio={:.2}x",
        first_basin_p99, last_basin_p99, drift_ratio
    );

    // Stability assert: p99 in any window must not exceed 3× the first window p99.
    // The bar is intentionally loose — the first measurement goal is to surface
    // catastrophic tail blowouts (OOM-killer, compaction deadlock, cascade panic).
    if first_basin_p99.is_finite() {
        let cap = first_basin_p99 * 3.0;
        for (w, &p99) in basin_p99s.iter().enumerate() {
            if p99.is_finite() {
                assert!(
                    p99 <= cap,
                    "soak stability: window {w} p99={p99:.2}ms exceeds 3× first-window p99 cap {cap:.2}ms"
                );
            }
        }
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    write_artifact(
        "soak_latency_stability.json",
        &json!({
            "card": "soak_latency_stability",
            "generated_at": format!("@{ts}"),
            "config": {
                "soak_secs": soak_secs,
                "seed_rows": seed_rows,
                "window_secs": window_secs,
                "n_windows": n_windows,
            },
            "drift_ratio_informational": if drift_ratio.is_nan() { serde_json::Value::Null } else { json!(drift_ratio) },
            "total_basin_errors": total_errors,
            "pg_available": pg_avail,
            "windows": json_windows,
        }),
    );

    // Cleanup PG schema now that measurement is complete.
    if pg_avail && !pg_soak_schema.is_empty() {
        if let Ok((pgc, f)) = tokio_postgres::connect(&pg_conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = f.await;
            });
            let _ = pgc
                .simple_query(&format!("DROP SCHEMA IF EXISTS {pg_soak_schema} CASCADE"))
                .await;
        }
    }

    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 2 — background_interference
// ─────────────────────────────────────────────────────────────────────────────

/// Identical read shapes measured in three conditions:
///   Phase A — quiesced (bg.shutdown() called; compactor idle)
///   Phase B — during active compaction (bg alive, no shutdown)
///   Phase C — during bulk ingest (bg alive + concurrent 1k-row INSERT loop)
///
/// The p50/p99 delta between phases IS the result. The test asserts only that
/// p99 during compaction stays below 10× the quiesced p99 — a catastrophic-
/// regression guard, not a performance target.
///
/// PG twin: same point-query shape run during autovacuum (autovacuum is NOT
/// disabled, letting natural background activity from the fresh-insert workload
/// serve as the PG interference signal).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn background_interference() {
    let seed_rows = env_usize("BASIN_INTERF_SEED_ROWS", 200_000) as i64;
    let measure_secs = 30u64; // measurement window per phase
    let samples = 200usize;

    eprintln!("[interf] config: seed_rows={seed_rows} measure_secs={measure_secs}");

    const EPOCH: i64 = 1_700_000_000;

    // ── Phase A: quiesced ────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess_a = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess_a
        .execute(
            "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
        )
        .await
        .unwrap();

    eprintln!("[interf] seeding phase A ({seed_rows} rows) ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < seed_rows {
        let hi = (id + batch).min(seed_rows);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            let amount = 0.5 + (k % 99) as f64;
            stmt.push_str(&format!(
                "({k},{},{amount},'{status}',{})",
                k % (seed_rows / 10).max(1),
                EPOCH + k,
            ));
        }
        sess_a.execute(&stmt).await.expect("interf seed");
        id = hi;
    }

    // Quiesce: flush + background shutdown (mirrors the compare_postgres cards).
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    let mut rng: u64 = 0xA1B2C3D4;
    let mut phase_a_samples: Vec<f64> = Vec::with_capacity(samples);
    // One warm-up pass
    let _ = sess_a
        .execute(&format!("SELECT id, amount FROM events WHERE id = 42"))
        .await;
    for _ in 0..samples {
        let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
        let t = Instant::now();
        let res = sess_a
            .execute(&format!(
                "SELECT id, user_id, amount FROM events WHERE id = {id_val}"
            ))
            .await
            .expect("phase A query");
        phase_a_samples.push(t.elapsed().as_secs_f64() * 1000.0);
        // correctness: row must exist
        assert!(
            row_count_of(&res) <= 1,
            "phase A: point query returned multiple rows for id={id_val}"
        );
    }
    let a_p50 = median(&phase_a_samples);
    let a_p99 = percentile(&phase_a_samples, 99.0);
    println!("[interf] Phase A (quiesced):  p50={a_p50:.3}ms  p99={a_p99:.3}ms  n={samples}");

    // ── Phase B: during compaction ───────────────────────────────────────────
    // Re-open a fresh engine with background alive; seed same rows so the
    // compactor has work (large WAL tail to flush).
    let (_sd_b, _wd_b, engine_b, shard_b, bg_b, wal_b) = build().await;
    let project_b = ProjectId::new();
    let sess_b = engine_b.open_session(project_b).await.unwrap();
    sess_b
        .execute(
            "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    eprintln!("[interf] seeding phase B ({seed_rows} rows, bg ALIVE) ...");
    id = 0;
    while id < seed_rows {
        let hi = (id + batch).min(seed_rows);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            let amount = 0.5 + (k % 99) as f64;
            stmt.push_str(&format!(
                "({k},{},{amount},'{status}',{})",
                k % (seed_rows / 10).max(1),
                EPOCH + k,
            ));
        }
        sess_b.execute(&stmt).await.expect("interf seed B");
        id = hi;
    }
    // NO flush/shutdown — background loop is alive and doing compaction work.

    rng = 0xB1C2D3E4;
    let mut phase_b_samples: Vec<f64> = Vec::with_capacity(samples);
    let _ = sess_b
        .execute("SELECT id, amount FROM events WHERE id = 42")
        .await;
    for _ in 0..samples {
        let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
        let t = Instant::now();
        let res = sess_b
            .execute(&format!(
                "SELECT id, user_id, amount FROM events WHERE id = {id_val}"
            ))
            .await
            .expect("phase B query");
        phase_b_samples.push(t.elapsed().as_secs_f64() * 1000.0);
        assert!(
            row_count_of(&res) <= 1,
            "phase B: point query correctness violation for id={id_val}"
        );
    }
    let b_p50 = median(&phase_b_samples);
    let b_p99 = percentile(&phase_b_samples, 99.0);
    println!("[interf] Phase B (compacting): p50={b_p50:.3}ms  p99={b_p99:.3}ms  n={samples}");

    // ── Phase C: during bulk ingest ──────────────────────────────────────────
    // Background alive + concurrent INSERT loop at 1000-row batches every 2s.
    let (_sd_c, _wd_c, engine_c, _shard_c, _bg_c, wal_c) = build().await;
    let project_c = ProjectId::new();
    let sess_c = Arc::new(engine_c.open_session(project_c).await.unwrap());
    sess_c
        .execute(
            "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    // Seed same baseline rows
    eprintln!("[interf] seeding phase C ({seed_rows} rows, bg ALIVE) ...");
    id = 0;
    while id < seed_rows {
        let hi = (id + batch).min(seed_rows);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            let amount = 0.5 + (k % 99) as f64;
            stmt.push_str(&format!(
                "({k},{},{amount},'{status}',{})",
                k % (seed_rows / 10).max(1),
                EPOCH + k,
            ));
        }
        sess_c.execute(&stmt).await.expect("interf seed C");
        id = hi;
    }

    // Launch background bulk-ingest loop
    let done_c = Arc::new(AtomicBool::new(false));
    let sess_c_ingest = sess_c.clone();
    let dc = done_c.clone();
    let ingest_task = tokio::spawn(async move {
        let mut next_id = seed_rows + 1;
        while !dc.load(Ordering::Relaxed) {
            let mut stmt = String::from("INSERT INTO events VALUES ");
            for k in 0..1000i64 {
                let rid = next_id + k;
                if k > 0 {
                    stmt.push(',');
                }
                stmt.push_str(&format!(
                    "({rid},{},1.0,'pending',{})",
                    rid % 50000,
                    EPOCH + rid,
                ));
            }
            next_id += 1000;
            let _ = sess_c_ingest.execute(&stmt).await;
            tokio::time::sleep(Duration::from_millis(2000)).await;
        }
    });

    rng = 0xC1D2E3F4;
    let mut phase_c_samples: Vec<f64> = Vec::with_capacity(samples);
    let _ = sess_c
        .execute("SELECT id, amount FROM events WHERE id = 42")
        .await;
    for _ in 0..samples {
        let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
        let t = Instant::now();
        let res = sess_c
            .execute(&format!(
                "SELECT id, user_id, amount FROM events WHERE id = {id_val}"
            ))
            .await
            .expect("phase C query");
        phase_c_samples.push(t.elapsed().as_secs_f64() * 1000.0);
        assert!(
            row_count_of(&res) <= 1,
            "phase C: point query correctness violation for id={id_val}"
        );
    }
    done_c.store(true, Ordering::Relaxed);
    let _ = ingest_task.await;

    let c_p50 = median(&phase_c_samples);
    let c_p99 = percentile(&phase_c_samples, 99.0);
    println!("[interf] Phase C (bulk-ingest): p50={c_p50:.3}ms  p99={c_p99:.3}ms  n={samples}");

    // ── PG twin ───────────────────────────────────────────────────────────────
    let (pg_b_p50, pg_b_p99) = if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_interf_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("pg schema");
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.events (\
                id BIGINT PRIMARY KEY, user_id BIGINT, \
                amount DOUBLE PRECISION, status TEXT, created_at BIGINT)"
        ))
        .await
        .expect("pg create");
        // Seed PG identically
        let mut pg_id = 0i64;
        while pg_id < seed_rows {
            let hi = (pg_id + batch).min(seed_rows);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                let status = status_for(k);
                let amount = 0.5 + (k % 99) as f64;
                stmt.push_str(&format!(
                    "({k},{},{amount},'{status}',{})",
                    k % (seed_rows / 10).max(1),
                    EPOCH + k,
                ));
            }
            pg.simple_query(&stmt).await.expect("pg seed interf");
            pg_id = hi;
        }
        // Measure PG with autovacuum running (no explicit quiesce)
        let mut pg_samples: Vec<f64> = Vec::with_capacity(samples);
        rng = 0xD1E2F3A4;
        let _ = pg
            .simple_query(&format!("SELECT id FROM {schema}.events WHERE id = 42"))
            .await;
        for _ in 0..samples {
            let id_val = (next_u64(&mut rng) as i64).abs() % seed_rows;
            let t = Instant::now();
            let _ = pg
                .simple_query(&format!(
                    "SELECT id, user_id, amount FROM {schema}.events WHERE id = {id_val}"
                ))
                .await;
            pg_samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let pp50 = median(&pg_samples);
        let pp99 = percentile(&pg_samples, 99.0);
        println!("[interf] PG (autovacuum-live): p50={pp50:.3}ms p99={pp99:.3}ms n={samples}");
        std::mem::forget(_guard);
        (pp50, pp99)
    } else {
        eprintln!("[interf] PG unavailable — recording Basin-only interference deltas");
        (f64::NAN, f64::NAN)
    };

    // ── Interference assertion ────────────────────────────────────────────────
    // p99 during compaction must be < 10× quiesced p99. Bar is loose — this
    // is a catastrophic-regression guard, not a performance target.
    if a_p99 > 1e-9 {
        assert!(
            b_p99 < a_p99 * 10.0,
            "background_interference: phase B p99 {b_p99:.2}ms exceeds 10× quiesced p99 {:.2}ms",
            a_p99 * 10.0
        );
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    write_artifact(
        "background_interference.json",
        &json!({
            "card": "background_interference",
            "generated_at": format!("@{ts}"),
            "config": { "seed_rows": seed_rows, "samples_per_phase": samples },
            "basin": {
                "phase_a_quiesced":  { "p50_ms": a_p50, "p99_ms": a_p99 },
                "phase_b_compacting": { "p50_ms": b_p50, "p99_ms": b_p99 },
                "phase_c_bulk_ingest": { "p50_ms": c_p50, "p99_ms": c_p99 },
                "b_over_a_p99_ratio": if a_p99 > 1e-9 { json!(b_p99 / a_p99) } else { serde_json::Value::Null },
                "c_over_a_p99_ratio": if a_p99 > 1e-9 { json!(c_p99 / a_p99) } else { serde_json::Value::Null },
            },
            "postgres": {
                "autovacuum_live": {
                    "p50_ms": if pg_b_p50.is_nan() { serde_json::Value::Null } else { json!(pg_b_p50) },
                    "p99_ms": if pg_b_p99.is_nan() { serde_json::Value::Null } else { json!(pg_b_p99) },
                }
            }
        }),
    );

    bg_b.shutdown().await;
    wal_b.close().await.unwrap();
    wal_c.close().await.unwrap();
    instance.wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 8 — n_plus_one_storm
// ─────────────────────────────────────────────────────────────────────────────

/// ORM lazy-load anti-pattern: fetch N parent rows, then issue N individual
/// child lookups (one query per parent, no join). Measures total wall-clock vs
/// PG over the same sequential round-trip count.
///
/// Also measures the single JOIN equivalent so callers see the N+1 vs join
/// speedup ratio. The catastrophic-regression assert: Basin N+1 time ≤ 10×
/// PG N+1 time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn n_plus_one_storm() {
    let n_parents = env_usize("BASIN_N1_PARENTS", 100);
    let seed_rows = 100_000i64;
    let n_users = 1_000i64;

    eprintln!("[n+1] config: n_parents={n_parents} seed_rows={seed_rows}");

    const EPOCH: i64 = 1_700_000_000;

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();

    sess.execute(&format!(
        "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email TEXT NOT NULL)"
    ))
    .await
    .unwrap();
    sess.execute(&format!(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            created_at BIGINT NOT NULL)"
    ))
    .await
    .unwrap();

    // Seed users
    {
        let mut stmt = String::from("INSERT INTO users VALUES ");
        for u in 0..n_users {
            if u > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({u},'{}')", email_for(u)));
        }
        sess.execute(&stmt).await.unwrap();
    }

    // Seed events (10k-row batches)
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < seed_rows {
        let hi = (id + batch).min(seed_rows);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let uid = k % n_users;
            stmt.push_str(&format!(
                "({k},{uid},{:.2},{})",
                1.0 + (k % 99) as f64,
                EPOCH + k
            ));
        }
        sess.execute(&stmt).await.expect("basin seed events");
        id = hi;
    }

    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // Warm-up
    let _ = sess.execute("SELECT id FROM users LIMIT 5").await;

    // Step 1: fetch n_parents parent rows
    let fetch_parents_sql = format!("SELECT id FROM users LIMIT {n_parents}");
    let parent_ids: Vec<i64> = match sess.execute(&fetch_parents_sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            use arrow_array::{Array, Int64Array};
            batches
                .iter()
                .flat_map(|b| {
                    let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
                })
                .collect()
        }
        _ => panic!("expected rows from parent fetch"),
    };
    assert_eq!(
        parent_ids.len(),
        n_parents,
        "must fetch exactly {n_parents} parent rows"
    );

    // Step 2: N individual child SELECTs (ORM lazy-load pattern)
    let basin_n1_start = Instant::now();
    let mut total_child_rows = 0usize;
    for &uid in &parent_ids {
        let res = sess
            .execute(&format!(
                "SELECT id, amount FROM events WHERE user_id = {uid} LIMIT 10"
            ))
            .await
            .expect("basin child select");
        total_child_rows += row_count_of(&res);
    }
    let basin_n1_ms = basin_n1_start.elapsed().as_secs_f64() * 1000.0;
    assert!(
        total_child_rows > 0,
        "basin N+1: expected some child rows across {n_parents} parents"
    );

    // JOIN equivalent
    let basin_join_start = Instant::now();
    let join_res = sess
        .execute(
            "SELECT u.id, e.id, e.amount \
         FROM users u JOIN events e ON e.user_id = u.id \
         LIMIT 1000",
        )
        .await
        .expect("basin join");
    let basin_join_ms = basin_join_start.elapsed().as_secs_f64() * 1000.0;
    let join_rows = row_count_of(&join_res);
    assert!(join_rows > 0, "basin join must return rows");

    println!(
        "[n+1] Basin: n+1 total={:.2}ms ({} parents × child selects, {} child rows) | join={:.2}ms ({} rows)",
        basin_n1_ms, n_parents, total_child_rows, basin_join_ms, join_rows
    );

    // ── PG twin ───────────────────────────────────────────────────────────────
    let (pg_n1_ms, pg_join_ms) = if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_n1_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.users (id BIGINT PRIMARY KEY, email TEXT)"
        ))
        .await
        .unwrap();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.events (\
                id BIGINT PRIMARY KEY, user_id BIGINT, \
                amount DOUBLE PRECISION, created_at BIGINT)"
        ))
        .await
        .unwrap();
        // Seed PG users
        {
            let mut stmt = format!("INSERT INTO {schema}.users VALUES ");
            for u in 0..n_users {
                if u > 0 {
                    stmt.push(',');
                }
                stmt.push_str(&format!("({u},'{}')", email_for(u)));
            }
            pg.simple_query(&stmt).await.unwrap();
        }
        // Seed PG events
        let mut pg_id = 0i64;
        while pg_id < seed_rows {
            let hi = (pg_id + batch).min(seed_rows);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                let uid = k % n_users;
                stmt.push_str(&format!(
                    "({k},{uid},{:.2},{})",
                    1.0 + (k % 99) as f64,
                    EPOCH + k
                ));
            }
            pg.simple_query(&stmt).await.unwrap();
            pg_id = hi;
        }

        // Warm-up
        let _ = pg
            .simple_query(&format!("SELECT id FROM {schema}.users LIMIT 5"))
            .await;

        // PG N+1 loop (same parent ids)
        let pg_n1_start = Instant::now();
        for &uid in &parent_ids {
            let _ = pg
                .simple_query(&format!(
                    "SELECT id, amount FROM {schema}.events WHERE user_id = {uid} LIMIT 10"
                ))
                .await
                .expect("pg child select");
        }
        let pn1 = pg_n1_start.elapsed().as_secs_f64() * 1000.0;

        // PG JOIN equivalent
        let pg_join_start = Instant::now();
        let _ = pg
            .simple_query(&format!(
                "SELECT u.id, e.id, e.amount \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             LIMIT 1000"
            ))
            .await
            .expect("pg join");
        let pj = pg_join_start.elapsed().as_secs_f64() * 1000.0;

        println!("[n+1] PG:    n+1 total={:.2}ms | join={:.2}ms", pn1, pj);
        std::mem::forget(_guard);
        (pn1, pj)
    } else {
        eprintln!("[n+1] PG unavailable — Basin-only result");
        (f64::NAN, f64::NAN)
    };

    // Catastrophic-regression guard: Basin N+1 must not exceed 10× PG N+1.
    if pg_n1_ms.is_finite() && pg_n1_ms > 1e-9 {
        let ratio = basin_n1_ms / pg_n1_ms;
        assert!(
            ratio < 10.0,
            "n_plus_one_storm: Basin {basin_n1_ms:.2}ms = {ratio:.2}× PG {pg_n1_ms:.2}ms; \
             catastrophic-regression bar is <10×"
        );
        println!("[n+1] Basin/PG ratio = {ratio:.2}×");
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    write_artifact(
        "n_plus_one_storm.json",
        &json!({
            "card": "n_plus_one_storm",
            "generated_at": format!("@{ts}"),
            "config": { "n_parents": n_parents, "seed_rows": seed_rows },
            "basin": {
                "n_plus_one_total_ms": basin_n1_ms,
                "join_equivalent_ms": basin_join_ms,
                "n_plus_one_over_join": basin_n1_ms / basin_join_ms.max(1e-9),
            },
            "postgres": {
                "n_plus_one_total_ms": if pg_n1_ms.is_nan() { serde_json::Value::Null } else { json!(pg_n1_ms) },
                "join_equivalent_ms": if pg_join_ms.is_nan() { serde_json::Value::Null } else { json!(pg_join_ms) },
            },
            "basin_over_pg_n1_ratio": if pg_n1_ms.is_finite() && pg_n1_ms > 1e-9 {
                json!(basin_n1_ms / pg_n1_ms)
            } else {
                serde_json::Value::Null
            },
        }),
    );

    instance.wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 9 — read_scaling_curve
// ─────────────────────────────────────────────────────────────────────────────

/// Read-only N-session scaling curve, Basin vs Postgres.
///
/// For C in [1, 4, 16, 64]: spawn C concurrent tasks, each running random-PK
/// point queries for `window_secs` seconds. Measure total QPS and per-task QPS.
/// Asserts: peak Basin QPS ≥ 3× QPS(C=1); Basin/PG QPS ratio does not collapse
/// below 0.1 at any C (the concurrency-floor guard).
///
/// PG twin: identical C concurrent `tokio_postgres` connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn read_scaling_curve() {
    const ROWS: i64 = 1_000_000;
    const CONCURRENCIES: [usize; 4] = [1, 4, 16, 64];
    let window_secs = env_u64("BASIN_SCALE_WINDOW_SECS", 5);

    eprintln!("[scale] config: rows={ROWS} window={window_secs}s concurrencies={CONCURRENCIES:?}");

    const EPOCH: i64 = 1_700_000_000;

    // ── Basin engine setup ───────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let engine = Arc::new(instance.engine);
    let project = instance.project;
    let sess_seed = engine.open_session(project).await.unwrap();

    sess_seed
        .execute(
            "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
        )
        .await
        .unwrap();

    eprintln!("[scale] seeding {ROWS} rows ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < ROWS {
        let hi = (id + batch).min(ROWS);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k},{},{},'{}',{})",
                k % 100_000,
                0.5 + (k % 100) as f64,
                status_for(k),
                EPOCH + k,
            ));
        }
        sess_seed.execute(&stmt).await.expect("seed");
        id = hi;
    }

    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    drop(sess_seed);

    // ── PG setup ─────────────────────────────────────────────────────────────
    let (pg_avail, pg_schema, pg_cs) = if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_scale_{suffix}");
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.events (\
                id BIGINT PRIMARY KEY, user_id BIGINT, \
                amount DOUBLE PRECISION, status TEXT, created_at BIGINT)"
        ))
        .await
        .unwrap();
        pg.simple_query("SET work_mem = '4MB'").await.unwrap();
        // Seed PG
        eprintln!("[scale] seeding PG {ROWS} rows ...");
        let mut pg_id = 0i64;
        while pg_id < ROWS {
            let hi = (pg_id + batch).min(ROWS);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                stmt.push_str(&format!(
                    "({k},{},{},'{}',{})",
                    k % 100_000,
                    0.5 + (k % 100) as f64,
                    status_for(k),
                    EPOCH + k,
                ));
            }
            pg.simple_query(&stmt).await.expect("pg seed");
            pg_id = hi;
        }
        eprintln!("[scale] PG seed complete");
        (true, schema, cs)
    } else {
        eprintln!("[scale] PG unavailable — Basin-only scaling curve");
        (false, String::new(), String::new())
    };

    // ── Measurement loop ─────────────────────────────────────────────────────
    struct CurvePoint {
        c: usize,
        basin_qps: f64,
        pg_qps: f64,
    }
    let mut curve: Vec<CurvePoint> = Vec::new();

    for &c in CONCURRENCIES.iter() {
        let engine = engine.clone();
        let deadline = Duration::from_secs(window_secs);

        // Basin: C concurrent sessions, random PK point queries
        let b_total: f64 = {
            let mut set: JoinSet<u64> = JoinSet::new();
            for task_idx in 0..c {
                let eng = engine.clone();
                set.spawn(async move {
                    let sess = eng.open_session(project).await.unwrap();
                    let mut rng: u64 = (task_idx as u64).wrapping_mul(0xA5A5A5A5).wrapping_add(1);
                    let mut count: u64 = 0;
                    let until = Instant::now() + deadline;
                    while Instant::now() < until {
                        let id_val = (next_u64(&mut rng) as i64).abs() % ROWS;
                        let _ = sess
                            .execute(&format!(
                                "SELECT id, user_id, amount FROM events WHERE id = {id_val}"
                            ))
                            .await;
                        count += 1;
                    }
                    count
                });
            }
            let mut total: u64 = 0;
            while let Some(r) = set.join_next().await {
                total += r.unwrap();
            }
            total as f64 / window_secs as f64
        };

        // PG: C concurrent connections, same query shape
        let p_total: f64 = if pg_avail {
            let cs = pg_cs.clone();
            let sch = pg_schema.clone();
            let mut set: JoinSet<u64> = JoinSet::new();
            for task_idx in 0..c {
                let cs = cs.clone();
                let sch = sch.clone();
                set.spawn(async move {
                    let (pgc, pgconn) = match tokio_postgres::connect(&cs, NoTls).await {
                        Ok(v) => v,
                        Err(_) => return 0u64,
                    };
                    tokio::spawn(async move {
                        let _ = pgconn.await;
                    });
                    let mut rng: u64 = (task_idx as u64).wrapping_mul(0xB6B6B6B6).wrapping_add(1);
                    let mut count: u64 = 0;
                    let until = Instant::now() + deadline;
                    while Instant::now() < until {
                        let id_val = ((next_u64(&mut rng) % ROWS as u64) + 1) as i64;
                        let _ = pgc
                            .simple_query(&format!(
                                "SELECT id, user_id, amount FROM {sch}.events WHERE id = {id_val}"
                            ))
                            .await;
                        count += 1;
                    }
                    count
                });
            }
            let mut total: u64 = 0;
            while let Some(r) = set.join_next().await {
                total += r.unwrap();
            }
            total as f64 / window_secs as f64
        } else {
            f64::NAN
        };

        println!(
            "[scale] C={:>2}: basin={:.1} qps | pg={:.1} qps | ratio={:.2}x",
            c,
            b_total,
            p_total,
            if p_total.is_finite() && p_total > 1e-9 {
                b_total / p_total
            } else {
                f64::NAN
            },
        );
        curve.push(CurvePoint {
            c,
            basin_qps: b_total,
            pg_qps: p_total,
        });
    }

    // ── Assertions ────────────────────────────────────────────────────────────
    let qps_c1 = curve.first().map(|r| r.basin_qps).unwrap_or(0.0);
    let peak_qps = curve
        .iter()
        .map(|r| r.basin_qps)
        .fold(f64::NEG_INFINITY, f64::max);
    let peak_ratio = if qps_c1 > 1e-9 {
        peak_qps / qps_c1
    } else {
        0.0
    };
    assert!(
        peak_ratio >= 3.0,
        "read_scaling_curve: peak Basin QPS {peak_qps:.1} = {peak_ratio:.2}× QPS(C=1) {qps_c1:.1}; expected ≥3×"
    );

    // Concurrency-floor guard: Basin/PG ratio must not collapse below 0.1 at any C.
    if pg_avail {
        for pt in &curve {
            if pt.pg_qps.is_finite() && pt.pg_qps > 1e-9 {
                let ratio = pt.basin_qps / pt.pg_qps;
                assert!(
                    ratio >= 0.1,
                    "read_scaling_curve: Basin/PG QPS ratio {ratio:.3}× at C={} below floor 0.1×",
                    pt.c
                );
            }
        }
    }

    let json_curve: Vec<serde_json::Value> = curve.iter().map(|pt| json!({
        "concurrency": pt.c,
        "basin_qps": pt.basin_qps,
        "pg_qps": if pt.pg_qps.is_nan() { serde_json::Value::Null } else { json!(pt.pg_qps) },
        "basin_over_pg": if pt.pg_qps.is_finite() && pt.pg_qps > 1e-9 {
            json!(pt.basin_qps / pt.pg_qps)
        } else {
            serde_json::Value::Null
        },
    })).collect();

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    write_artifact(
        "read_scaling_curve.json",
        &json!({
            "card": "read_scaling_curve",
            "generated_at": format!("@{ts}"),
            "config": {
                "seed_rows": ROWS,
                "window_secs": window_secs,
                "concurrencies": CONCURRENCIES.as_slice(),
            },
            "peak_speedup_vs_c1": peak_ratio,
            "curve": json_curve,
        }),
    );

    // Clean up PG schema (async-safe path: open a fresh connection)
    if pg_avail && !pg_schema.is_empty() {
        if let Ok((pgc, f)) = tokio_postgres::connect(&pg_cs, NoTls).await {
            tokio::spawn(async move {
                let _ = f.await;
            });
            let _ = pgc
                .simple_query(&format!("DROP SCHEMA IF EXISTS {pg_schema} CASCADE"))
                .await;
        }
    }

    instance.wal.close().await.unwrap();
}
